// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// WALMagic is a 4 byte number every WAL segment file starts with.
	WALMagic = uint32(0x43AF00EF)

	// WALFormatDefault is the version flag for the default outer segment file format.
	WALFormatDefault = byte(1)
)

type walMetrics struct {
	fsyncDuration prometheus.Summary
	corruptions   prometheus.Counter
}

func newWALMetrics(wal *WAL, r prometheus.Registerer) *walMetrics {
	m := &walMetrics{}

	m.fsyncDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "tsdb_wal_fsync_duration_seconds",
		Help: "Duration of WAL fsync.",
	})
	m.corruptions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tsdb_wal_corruptions_total",
		Help: "Total number of WAL corruptions.",
	})

	if r != nil {
		r.MustRegister(
			m.fsyncDuration,
			m.corruptions,
		)
	}
	return m
}

// Interface is a write ahead log that can log new series labels and samples.
// It must be completely read before new entries are logged.
type Interface interface {
	// Log adds an entry to the WAL and returns once it was written to disk.
	Log(b []byte) (lsn uint64, err error)

	// Log adds an entry to the WAL and returns before it was written to disk.
	// Callers may call LogAsync multiple times followed by a call to Log to
	// ensure that all records have been written to disk.
	LogAsync(b []byte) (lsn uint64, err error)

	// ReadAll consumes the WAL from the beginning and calls the function
	// for every entry. If an error is encountered the WAL automatically gets
	// truncated after the last good record.
	// Must be called exactly once initially.
	ReadAll(func(b []byte) error) error

	// Trim removes data from the front of the WAL.
	Trim(lsn uint64) error

	// Close the WAL and return after all pending writes have been written
	// and synced.
	Close() error
}

const (
	walSegmentSizeBytes = 128 * 1024 * 1024
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// WAL is a write ahead log for series data.
type WAL struct {
	metrics       *walMetrics
	logger        log.Logger
	dir           string
	fsyncInterval time.Duration
	segmentSize   int64

	mtx         sync.Mutex
	initialized bool
	file        *File // segments being written to

	stopc         chan struct{}
	donec         chan struct{}
	closec        chan *os.File
	queue         chan *block // persistence queue for blocks
	activeWriters sync.WaitGroup

	curBlock      unsafe.Pointer
	buffers       sync.Pool
	lastBufferSeq uint64
}

// Open or create a write ahead log in the given directory.
// The WAL must be read completely before new data is written.
func Open(dir string, fsyncInterval time.Duration, logger log.Logger, r prometheus.Registerer) (*WAL, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}

	w := &WAL{
		dir:           dir,
		logger:        logger,
		fsyncInterval: fsyncInterval,
		donec:         make(chan struct{}),
		stopc:         make(chan struct{}),
		queue:         make(chan *block, 10),
		closec:        make(chan *os.File),
		segmentSize:   walSegmentSizeBytes,
	}
	w.curBlock = unsafe.Pointer(w.newBlock())
	w.metrics = newWALMetrics(w, r)

	go w.processQueue()
	go w.run()

	return w, nil
}

func (w *WAL) run() {
	if w.fsyncInterval == 0 {
		return
	}
	tick := time.NewTicker(w.fsyncInterval)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:

		case <-w.stopc:
			return
		}
	}
}

func (w *WAL) processBlock(blk *block, lastSeq uint64, pending []*block) {
	// Enqueue if the block is out of order.
	if blk.seq != lastSeq+1 {
		pending = append(pending, blk)

		sort.Slice(pending, func(i, j int) bool {
			return pending[i].seq > pending[j].seq
		})
		return
	}

	// Write current in-order block.
	_, blk.err = w.file.Write(blk.buf[:])
	close(blk.done)
	lastSeq = blk.seq

	// We might be able to work down the pending queue now.
	for i := len(pending) - 1; i >= 0; i-- {
		blk = pending[i]

		if blk.seq != lastSeq+1 {
			break
		}
		_, blk.err = w.file.Write(blk.buf[:])
		close(blk.done)
		lastSeq = blk.seq
		pending = pending[:len(pending)-1]
	}
}

// processQueue persists enqueued blocks to disk in the order of their sequence number.
func (w *WAL) processQueue() {
	defer close(w.donec)

	// Pending writes that cannot be written yet because they are out of order.
	var lastSeq uint64
	var pending []*block

	c := 0

	for {
		c++
		if c%100000 == 0 {
			fmt.Println("processed", c)
		}
		select {
		case blk := <-w.queue:
			w.processBlock(blk, lastSeq, pending)
		}
	}
}

func (w *WAL) getBuffer() *[blockSize]byte {
	if b := w.buffers.Get(); b != nil {
		return b.(*[blockSize]byte)
	}
	return &[blockSize]byte{}
}

func (w *WAL) putBuffer(b *[blockSize]byte) {
	w.buffers.Put(b)
}

const blockSize = 64 * 1024

type block struct {
	seq   uint64
	state uint64
	buf   *[blockSize]byte
	done  chan struct{}
	err   error
}

func (b *block) String() string {
	return fmt.Sprintf("%d", b.seq)
}

func (b *block) getState() blockState {
	return blockState(atomic.LoadUint64(&b.state))
}

func (b *block) setDone(s blockState) bool {
	return atomic.CompareAndSwapUint64(&b.state, uint64(s), uint64(s)|(1<<63))
}

func (b *block) reserve(s blockState, x int) bool {
	return atomic.CompareAndSwapUint64(&b.state, uint64(s), uint64(s)+uint64(x<<32))
}

func (b *block) release(x int) blockState {
	return blockState(atomic.AddUint64(&b.state, uint64(x)))
}

type blockState uint64

func (s blockState) done() bool {
	return s>>63 == 1
}

func (s blockState) reserved() int {
	return int(s << 1 >> 33)
}

func (s blockState) released() int {
	return int(s << 32 >> 32)
}

func (w *WAL) newBlock() *block {
	w.lastBufferSeq++

	return &block{
		seq:   w.lastBufferSeq,
		state: 0,
		buf:   w.getBuffer(),
		done:  make(chan struct{}),
	}
}

// enqueue a block for disk persistence.
func (w *WAL) enqueue(blk *block) {
	w.queue <- blk
}

// log the byte slice. It returns a function that blocks until the data was persisted
// to disk and returns any errors.
func (w *WAL) log(b []byte) func() error {
	var blk *block
	var state blockState
	var reserved int

	req := len(b) + 8

	for k := 0; ; k++ {
		blk = (*block)(atomic.LoadPointer(&w.curBlock))
		state = blk.getState()
		reserved = state.reserved()

		// Already marked as done and in the process of being swapped. Spin!
		if state.done() {
			goto RETRY
		}
		if reserved+req <= blockSize {
			// Reserve space for ourselves and exit if successful.
			if blk.reserve(state, req) {
				break
			}
			goto RETRY
		}
		// We are exceeding the buffer size; swap the next one in.
		if len(b) > blockSize/2 {
			panic("record too big") // TODO(fabxc): implement record splitting.
		}

		// Mark as done before swapping so no other goroutine attempts adding another entry.
		if !blk.setDone(state) {
			goto RETRY
		}
		// We only set the done field if the previous state was not done. Thus, if we succeed
		// we are the first ones and exclusive in updating the current buffer.
		atomic.StorePointer(&w.curBlock, unsafe.Pointer(w.newBlock()))

		// If everything was already released when we marked it as done,
		// we are responsible for shipping the buffer off.
		if state.released() == reserved {
			w.enqueue(blk)
		}

	RETRY:
		// This increases the average cycles per call but is still faster by saving
		// a lot of context switches into the runtime.
		if k > 2 {
			runtime.Gosched()
		}
	}

	// Assemble the record entry and copy it into the reserved space.
	var lb [5]byte
	binary.BigEndian.PutUint32(lb[:4], uint32(len(b)))
	lb[4] = byte(flagFull)
	off := reserved + 4

	copy(blk.buf[off:], lb[1:])
	copy(blk.buf[off+4:], b)

	crc := crc32.Checksum(blk.buf[off:off+len(b)], castagnoliTable)
	binary.BigEndian.PutUint32(blk.buf[reserved:], crc)

	state = blk.release(req)

	// If we are the last one to release, ship the buffer off. But only if
	// it was marked as done.
	if state.released() == state.reserved() && state.done() {
		w.enqueue(blk)
	}
	return func() error {
		<-blk.done
		return blk.err
	}
}

// Log writes a record to the WAL and returns once it was written to disk.
func (w *WAL) Log(b []byte) error {
	w.activeWriters.Add(1)
	wait := w.log(b)
	w.activeWriters.Done()
	return wait()
}

// LogAsync logs stuff.
func (w *WAL) LogAsync(b []byte) error {
	w.activeWriters.Add(1)
	w.log(b)
	w.activeWriters.Done()
	return nil
}

// Close syncs all data and closes the underlying resources.
func (w *WAL) Close() error {
	// Terminate background flusher/fsyncer.
	close(w.stopc)

	// Wait for pending Log*s to complete, close queue and wait for persistence to exit.
	w.activeWriters.Done()
	close(w.queue)
	<-w.donec

	return nil
}

func (w *WAL) initialize(f *File) error {
	w.initialized = true
	w.file = f
	return nil
}

// ReadAll consumes the WAL from the beginning and calls the function
// for every entry. If an error is encountered the WAL automatically gets
// truncated after the last good record.
// Must be called once initially before writing any new data.
func (w *WAL) ReadAll(h func(b []byte) error) error {
	sf, err := OpenFile(w.dir, w.segmentSize, os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	var (
		blk  [blockSize]byte
		blkr blockReader
		buf  = make([]byte, 4*blockSize)
	)
	for {
		if _, e := sf.Read(blk[:]); e != nil {
			err = e
			break
		}
		blkr.reset(blk[:])

		for {
			flag, rec, e := blkr.read()
			if e != nil {
				err = e
				break
			}
			switch flag {
			case flagNull:
				continue
			case flagFull:
				err = h(rec)
			case flagStart:
				buf = append(buf[:0], rec...)
			case flagCont:
				buf = append(buf, rec...)
			case flagEnd:
				err = h(buf)
			default:
				panic("unknown flag")
			}
		}
	}
	w.mtx.Lock()
	defer w.mtx.Unlock()

	if err == io.EOF {
		err = nil
	}
	if w.initialized {
		return err
	}
	// In case of an error, truncate the segment file after the last valid record
	// we read from it. The write offset remains at the next block.
	if err != nil {
		off, err := sf.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}
		if err := sf.Truncate(off - blockSize + int64(blkr.offset())); err != nil {
			return err
		}
	}
	return w.initialize(sf)
}

const (
	flagNull  = 0
	flagFull  = 1
	flagStart = 2
	flagCont  = 3
	flagEnd   = 4
)

func packRecordHeader(crc uint32, length int, flag uint8) uint64 {
	return (uint64(crc) << 32) | uint64(uint32(length)<<8) | uint64(flag)
}

func unpackRecordHeader(h uint64) (crc uint32, len int, flag uint8) {
	return uint32(h >> 32), int(uint32(h) >> 8), uint8(h)
}

// blockReader reads records from an arbitrarily sized byte block.
type blockReader struct {
	b []byte
	i int
}

// read retrieves the next record and validates it checksum.
func (r *blockReader) read() (uint8, []byte, error) {
	// Once we hit a zero header, the block is done. Either because there was no space
	// left, it was intentionally zerod out during truncation, or intentional padding.
	hdr := binary.BigEndian.Uint64(r.b[r.i:])
	if hdr == 0 {
		return flagNull, nil, io.EOF
	}
	crc, length, flag := unpackRecordHeader(hdr)

	if len(r.b[r.i:]) < length+8 {
		return flagNull, nil, errInvalidSize
	}
	r.i += length + 8

	if got := crc32.Checksum(r.b[r.i+4:], castagnoliTable); got != crc {
		return flag, nil, errors.Errorf("expected CRC32 %d but got %d", crc, got)
	}
	return flag, r.b[r.i+8:], nil
}

// offset returns the position of the beginning of the record that was last attempted to be read.
func (r *blockReader) offset() int {
	return r.i
}

// reset to block reader to a new block.
func (r *blockReader) reset(b []byte) {
	r.b = b
	r.i = 0
}
