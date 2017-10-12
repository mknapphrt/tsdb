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

package tsdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/labels"
)

// WALEntryType indicates what data a WAL entry contains.
type WALEntryType uint8

const (
	// WALMagic is a 4 byte number every WAL segment file starts with.
	WALMagic = uint32(0x43AF00EF)

	// WALFormatDefault is the version flag for the default outer segment file format.
	WALFormatDefault = byte(1)
)

// Entry types in a segment file.
const (
	WALEntrySymbols WALEntryType = 1
	WALEntrySeries  WALEntryType = 2
	WALEntrySamples WALEntryType = 3
	WALEntryDeletes WALEntryType = 4
)

type walMetrics struct {
	fsyncDuration prometheus.Summary
	corruptions   prometheus.Counter
}

func newWalMetrics(wal *SegmentWAL, r prometheus.Registerer) *walMetrics {
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

// WAL is a write ahead log that can log new series labels and samples.
// It must be completely read before new entries are logged.
type WAL interface {
	Reader() WALReader
	LogSeries([]RefSeries) error
	LogSamples([]RefSample) error
	LogDeletes([]Stone) error
	Truncate(mint int64, keep func(uint64) bool) error
	Close() error
}

// NopWAL is a WAL that does nothing.
func NopWAL() WAL {
	return nopWAL{}
}

type nopWAL struct{}

func (nopWAL) Read(
	seriesf func([]RefSeries),
	samplesf func([]RefSample),
	deletesf func([]Stone),
) error {
	return nil
}
func (w nopWAL) Reader() WALReader                     { return w }
func (nopWAL) LogSeries([]RefSeries) error             { return nil }
func (nopWAL) LogSamples([]RefSample) error            { return nil }
func (nopWAL) LogDeletes([]Stone) error                { return nil }
func (nopWAL) Truncate(int64, func(uint64) bool) error { return nil }
func (nopWAL) Close() error                            { return nil }

// WALReader reads entries from a WAL.
type WALReader interface {
	Read(
		seriesf func([]RefSeries),
		samplesf func([]RefSample),
		deletesf func([]Stone),
	) error
}

// RefSeries is the series labels with the series ID.
type RefSeries struct {
	Ref    uint64
	Labels labels.Labels
}

// RefSample is a timestamp/value pair associated with a reference to a series.
type RefSample struct {
	Ref uint64
	T   int64
	V   float64

	series *memSeries
}

// segmentFile wraps a file object of a segment and tracks the highest timestamp
// it contains. During WAL truncating, all segments with no higher timestamp than
// the truncation threshold can be compacted.
type segmentFile struct {
	*os.File
	maxTime int64 // highest tombstone or sample timpstamp in segment
}

func newSegmentFile(f *os.File) *segmentFile {
	return &segmentFile{
		File:    f,
		maxTime: math.MinInt64,
	}
}

const (
	walSegmentSizeBytes = 256 * 1024 * 1024 // 256 MB
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

// SegmentWAL is a write ahead log for series data.
type SegmentWAL struct {
	mtx     sync.Mutex
	metrics *walMetrics

	dirFile *os.File
	files   []*segmentFile

	logger        log.Logger
	flushInterval time.Duration
	async         bool
	segmentSize   int64

	cur  *bufio.Writer
	curN int64

	stopc       chan struct{}
	donec       chan struct{}
	queue       chan *walBuffer
	byteBuffers sync.Pool

	curBuf unsafe.Pointer
	// nextBuf unsafe.Pointer
	buffers sync.Pool
}

// OpenSegmentWAL opens or creates a write ahead log in the given directory.
// The WAL must be read completely before new data is written.
func OpenSegmentWAL(dir string, logger log.Logger, async bool, flushInterval time.Duration, r prometheus.Registerer) (*SegmentWAL, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	df, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}

	curBuf := &walBuffer{maxTime: math.MinInt64, buf: &[walBufferSize]byte{}}
	// nextBuf :=

	w := &SegmentWAL{
		dirFile:       df,
		logger:        logger,
		async:         async,
		flushInterval: flushInterval,
		donec:         make(chan struct{}),
		stopc:         make(chan struct{}),
		queue:         make(chan *walBuffer, 100),
		segmentSize:   walSegmentSizeBytes,
		curBuf:        (unsafe.Pointer)(curBuf),
	}
	w.metrics = newWalMetrics(w, r)

	fns, err := sequenceFiles(w.dirFile.Name())
	if err != nil {
		return nil, err
	}
	for _, fn := range fns {
		f, err := w.openSegmentFile(fn)
		if err != nil {
			return nil, err
		}
		w.files = append(w.files, newSegmentFile(f))
	}

	// go w.run()

	return w, nil
}

func (w *SegmentWAL) getBuffer() *[walBufferSize]byte {
	if b := w.buffers.Get(); b != nil {
		return b.(*[walBufferSize]byte)
	}
	return &[walBufferSize]byte{}
}

func (w *SegmentWAL) putBuffer(b *[walBufferSize]byte) {
	w.buffers.Put(b)
}

const walBufferSize = 128 * 1024

type walBuffer struct {
	state   uint64
	buf     *[walBufferSize]byte
	maxTime int64
}

func (b *walBuffer) getState() walBufferState {
	return walBufferState(atomic.LoadUint64(&b.state))
}

func (b *walBuffer) setDone(s walBufferState) bool {
	return atomic.CompareAndSwapUint64(&b.state, uint64(s), uint64(s)|(1<<63))
}

func (b *walBuffer) reserve(s walBufferState, x int) bool {
	return atomic.CompareAndSwapUint64(&b.state, uint64(s), uint64(s)+uint64(x<<32))
}

func (b *walBuffer) release(x int) walBufferState {
	return walBufferState(atomic.AddUint64(&b.state, uint64(x)))
}

type walBufferState uint64

func (s walBufferState) done() bool {
	return s>>63 == 1
}

func (s walBufferState) reserved() int {
	return int(s << 1 >> 33)
}

func (s walBufferState) released() int {
	return int(s << 32 >> 32)
}

func (w *SegmentWAL) enqueue(i int, maxt int64, b []byte) func() error {
	var buf *walBuffer
	var state walBufferState

	for {
		buf = (*walBuffer)(atomic.LoadPointer(&w.curBuf))
		state = buf.getState()

		// Already marked as done and in the process of being swapped. Spin!
		if state.done() {
			goto END
		}
		// We are exceeding the buffer size; swap the next one in.
		if res := state.reserved(); res+len(b) > walBufferSize {
			// If the buffer is still empty, the record will never fit.
			// XXX(fabxc): handle large records properly.
			if res == 0 {
				panic("record too big")
			}
			// Mark as done before swapping so no other goroutine attempts adding another entry.
			if !buf.setDone(state) {
				goto END
			}
			// We only set the done field if the previous state was not done. Thus, if we succeed
			// we are the first ones and exclusive in updating the current buffer.
			newBuf := &walBuffer{maxTime: math.MinInt64, buf: w.getBuffer()}
			atomic.StorePointer(&w.curBuf, unsafe.Pointer(newBuf))

			// If everything was already released when we marked it as done,
			// we are responsible for shipping the buffer off.
			if state.released() == state.reserved() {
				w.queue <- buf
			}
			goto END
		}
		// Reserve space for ourselves and exit if successful.
		if buf.reserve(state, len(b)) {
			break
		}
	END:
		runtime.Gosched()
	}

	// fmt.Println("copy")
	copy(buf.buf[state.reserved():], b)
	state = buf.release(len(b))

	// If we are the last one to release, ship the buffer off. But only if
	// it was marked as done.
	if state.released() == state.reserved() && state.done() {
		w.queue <- buf
	}
	return func() error { return nil }
}

// repairingWALReader wraps a WAL reader and truncates its underlying SegmentWAL after the last
// valid entry if it encounters corruption.
type repairingWALReader struct {
	wal *SegmentWAL
	r   WALReader
}

func (r *repairingWALReader) Read(
	seriesf func([]RefSeries),
	samplesf func([]RefSample),
	deletesf func([]Stone),
) error {
	err := r.r.Read(seriesf, samplesf, deletesf)
	if err == nil {
		return nil
	}
	cerr, ok := errors.Cause(err).(walCorruptionErr)
	if !ok {
		return err
	}
	r.wal.metrics.corruptions.Inc()
	return r.wal.truncate(cerr.err, cerr.file, cerr.lastOffset)
}

// truncate the WAL after the last valid entry.
func (w *SegmentWAL) truncate(err error, file int, lastOffset int64) error {
	level.Error(w.logger).Log("msg", "WAL corruption detected; truncating",
		"err", err, "file", w.files[file].Name(), "pos", lastOffset)

	// Close and delete all files after the current one.
	for _, f := range w.files[file+1:] {
		if err := f.Close(); err != nil {
			return err
		}
		if err := os.Remove(f.Name()); err != nil {
			return err
		}
	}
	w.mtx.Lock()
	defer w.mtx.Unlock()

	w.files = w.files[:file+1]

	// Seek the current file to the last valid offset where we continue writing from.
	_, err = w.files[file].Seek(lastOffset, os.SEEK_SET)
	return err
}

// Reader returns a new reader over the the write ahead log data.
// It must be completely consumed before writing to the WAL.
func (w *SegmentWAL) Reader() WALReader {
	return &repairingWALReader{
		wal: w,
		r:   newWALReader(w.files, w.logger),
	}
}

// openSegmentFile opens the given segment file and consumes and validates header.
func (w *SegmentWAL) openSegmentFile(name string) (*os.File, error) {
	// We must open all files in read/write mode as we may have to truncate along
	// the way and any file may become the head.
	f, err := os.OpenFile(name, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	metab := make([]byte, 8)

	if n, err := f.Read(metab); err != nil {
		return nil, errors.Wrapf(err, "validate meta %q", f.Name())
	} else if n != 8 {
		return nil, errors.Errorf("invalid header size %d in %q", n, f.Name())
	}

	if m := binary.BigEndian.Uint32(metab[:4]); m != WALMagic {
		return nil, errors.Errorf("invalid magic header %x in %q", m, f.Name())
	}
	if metab[4] != WALFormatDefault {
		return nil, errors.Errorf("unknown WAL segment format %d in %q", metab[4], f.Name())
	}
	return f, nil
}

// createSegmentFile creates a new segment file with the given name. It preallocates
// the standard segment size if possible and writes the header.
func (w *SegmentWAL) createSegmentFile(name string) (*os.File, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	if err = fileutil.Preallocate(f, w.segmentSize, true); err != nil {
		return nil, err
	}
	// Write header metadata for new file.
	metab := make([]byte, 8)
	binary.BigEndian.PutUint32(metab[:4], WALMagic)
	metab[4] = WALFormatDefault

	if _, err := f.Write(metab); err != nil {
		return nil, err
	}
	return f, err
}

// cut finishes the currently active segments and opens the next one.
// The encoder is reset to point to the new segment.
func (w *SegmentWAL) cut() error {
	// Sync current head to disk and close.
	if hf := w.head(); hf != nil {
		if err := w.flush(); err != nil {
			return err
		}
		// Finish last segment asynchronously to not block the WAL moving along
		// in the new segment.
		go func() {
			off, err := hf.Seek(0, os.SEEK_CUR)
			if err != nil {
				level.Error(w.logger).Log("msg", "finish old segment", "segment", hf.Name(), "err", err)
			}
			if err := hf.Truncate(off); err != nil {
				level.Error(w.logger).Log("msg", "finish old segment", "segment", hf.Name(), "err", err)
			}
			if err := hf.Sync(); err != nil {
				level.Error(w.logger).Log("msg", "finish old segment", "segment", hf.Name(), "err", err)
			}
			if err := hf.Close(); err != nil {
				level.Error(w.logger).Log("msg", "finish old segment", "segment", hf.Name(), "err", err)
			}
		}()
	}

	p, _, err := nextSequenceFile(w.dirFile.Name())
	if err != nil {
		return err
	}
	f, err := w.createSegmentFile(p)
	if err != nil {
		return err
	}

	go func() {
		if err = w.dirFile.Sync(); err != nil {
			level.Error(w.logger).Log("msg", "sync WAL directory", "err", err)
		}
	}()

	w.files = append(w.files, newSegmentFile(f))

	// TODO(gouthamve): make the buffer size a constant.
	w.cur = bufio.NewWriterSize(f, 8*1024*1024)
	w.curN = 8

	return nil
}

func (w *SegmentWAL) head() *segmentFile {
	if len(w.files) == 0 {
		return nil
	}
	return w.files[len(w.files)-1]
}

// Sync flushes the changes to disk.
func (w *SegmentWAL) Sync() error {
	var head *segmentFile
	var err error

	// Flush the writer and retrieve the reference to the head segment under mutex lock.
	func() {
		w.mtx.Lock()
		defer w.mtx.Unlock()
		if err = w.flush(); err != nil {
			return
		}
		head = w.head()
	}()
	if err != nil {
		return errors.Wrap(err, "flush buffer")
	}
	if head != nil {
		// But only fsync the head segment after releasing the mutex as it will block on disk I/O.
		start := time.Now()
		err := fileutil.Fdatasync(head.File)
		w.metrics.fsyncDuration.Observe(time.Since(start).Seconds())
		return err
	}
	return nil
}

func (w *SegmentWAL) sync() error {
	if err := w.flush(); err != nil {
		return err
	}
	if w.head() == nil {
		return nil
	}

	start := time.Now()
	err := fileutil.Fdatasync(w.head().File)
	w.metrics.fsyncDuration.Observe(time.Since(start).Seconds())
	return err
}

func (w *SegmentWAL) flush() error {
	if w.cur == nil {
		return nil
	}
	return w.cur.Flush()
}

type walByteBuffer struct {
	crc32   hash.Hash
	buf     encbuf
	maxTime int64
}

func (w *SegmentWAL) getByteBuffer() *walByteBuffer {
	if b := w.byteBuffers.Get(); b != nil {
		return b.(*walByteBuffer)
	}
	return &walByteBuffer{crc32: newCRC32(), maxTime: math.MinInt64}
}

func (w *SegmentWAL) putByteBuffer(b *walByteBuffer) {
	b.crc32.Reset()
	b.buf.reset()
	b.maxTime = math.MinInt64
	w.byteBuffers.Put(b)
}

func (w *SegmentWAL) run(interval time.Duration) {
	var tick <-chan time.Time

	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		tick = ticker.C
	}
	defer close(w.donec)

	for {
		select {
		case <-w.stopc:
			return
		case <-tick:
			if err := w.Sync(); err != nil {
				level.Error(w.logger).Log("msg", "sync failed", "err", err)
			}
			// case buf := <-w.queue:

		}
	}
}

const (
	walSeriesSimple  = 1
	walSamplesSimple = 1
	walDeletesSimple = 1
)

func (w *SegmentWAL) encodeSeries(b *walByteBuffer, series []RefSeries) {
	b.buf.putBE32(0)
	b.buf.putByte(byte(WALEntrySeries))
	b.buf.putByte(walSeriesSimple)

	for _, s := range series {
		b.buf.putBE64(s.Ref)
		b.buf.putUvarint(len(s.Labels))

		for _, l := range s.Labels {
			b.buf.putUvarintStr(l.Name)
			b.buf.putUvarintStr(l.Value)
		}
	}

	binary.BigEndian.PutUint32(b.buf.b, uint32(b.buf.len()-4))
	b.buf.putHash(b.crc32)
}

func (w *SegmentWAL) encodeSamples(b *walByteBuffer, samples []RefSample) {
	b.buf.putBE32(0)
	b.buf.putByte(byte(WALEntrySamples))
	b.buf.putByte(walSamplesSimple)

	// Store base timestamp and base reference number of first sample.
	// All samples encode their timestamp and ref as delta to those.
	//
	// TODO(fabxc): optimize for all samples having the same timestamp.
	first := samples[0]

	b.buf.putBE64(first.Ref)
	b.buf.putBE64int64(first.T)

	for _, s := range samples {
		b.buf.putVarint64(int64(s.Ref) - int64(first.Ref))
		b.buf.putVarint64(s.T - first.T)
		b.buf.putBE64(math.Float64bits(s.V))

		if s.T > b.maxTime {
			b.maxTime = s.T
		}
	}
	binary.BigEndian.PutUint32(b.buf.b, uint32(b.buf.len()-4))
	b.buf.putHash(b.crc32)
}

func (w *SegmentWAL) encodeDeletes(b *walByteBuffer, stones []Stone) {
	b.buf.putBE32(0)
	b.buf.putByte(byte(WALEntryDeletes))
	b.buf.putByte(walDeletesSimple)

	for _, s := range stones {
		for _, iv := range s.intervals {
			b.buf.putBE64(s.ref)
			b.buf.putVarint64(iv.Mint)
			b.buf.putVarint64(iv.Maxt)

			if iv.Maxt > b.maxTime {
				b.maxTime = iv.Maxt
			}
		}
	}
	binary.BigEndian.PutUint32(b.buf.b, uint32(b.buf.len()-4))
	b.buf.putHash(b.crc32)
}

// LogSeries writes a batch of new series labels to the log.
// The series have to be ordered.
func (w *SegmentWAL) LogSeries(series []RefSeries) error {
	b := w.getByteBuffer()
	w.encodeSeries(b, series)

	wait := w.enqueue(0, b.maxTime, b.buf.get())
	w.putByteBuffer(b)

	if w.async {
		return nil
	}
	return wait()
}

// LogSamples writes a batch of new samples to the log.
func (w *SegmentWAL) LogSamples(samples []RefSample) error {
	b := w.getByteBuffer()
	w.encodeSamples(b, samples)

	wait := w.enqueue(0, b.maxTime, b.buf.get())
	w.putByteBuffer(b)

	if w.async {
		return nil
	}
	return wait()
}

// LogDeletes write a batch of new deletes to the log.
func (w *SegmentWAL) LogDeletes(stones []Stone) error {
	b := w.getByteBuffer()
	w.encodeDeletes(b, stones)

	wait := w.enqueue(0, b.maxTime, b.buf.get())
	w.putByteBuffer(b)

	if w.async {
		return nil
	}
	return wait()
}

// Truncate deletes the values prior to mint and the series which the keep function
// does not indiciate to preserve.
func (w *SegmentWAL) Truncate(mint int64, keep func(uint64) bool) error {
	// The last segment is always active.
	if len(w.files) < 2 {
		return nil
	}
	var candidates []*segmentFile

	// All files have to be traversed as there could be two segments for a block
	// with first block having times (10000, 20000) and SECOND one having (0, 10000).
	for _, sf := range w.files[:len(w.files)-1] {
		if sf.maxTime >= mint {
			break
		}
		// Past WAL files are closed. We have to reopen them for another read.
		f, err := w.openSegmentFile(sf.Name())
		if err != nil {
			return errors.Wrap(err, "open old WAL segment for read")
		}
		candidates = append(candidates, &segmentFile{
			File:    f,
			maxTime: sf.maxTime,
		})
	}
	if len(candidates) == 0 {
		return nil
	}

	r := newWALReader(candidates, w.logger)

	// Create a new tmp file.
	f, err := w.createSegmentFile(filepath.Join(w.dirFile.Name(), "compact.tmp"))
	if err != nil {
		return errors.Wrap(err, "create compaction segment")
	}
	var (
		csf          = newSegmentFile(f)
		decSeries    = []RefSeries{}
		activeSeries = []RefSeries{}
	)

	for r.next() {
		rt, flag, byt := r.at()

		if rt != WALEntrySeries {
			continue
		}
		decSeries = decSeries[:0]
		activeSeries = activeSeries[:0]

		err := r.decodeSeries(flag, byt, &decSeries)
		if err != nil {
			return errors.Wrap(err, "decode samples while truncating")
		}
		for _, s := range decSeries {
			if keep(s.Ref) {
				activeSeries = append(activeSeries, s)
			}
		}

		b := w.getByteBuffer()
		w.encodeSeries(b, activeSeries)

		_, err = csf.Write(b.buf.get())
		w.putByteBuffer(b)

		if err != nil {
			return err
		}
	}
	if r.Err() != nil {
		return errors.Wrap(r.Err(), "read candidate WAL files")
	}

	off, err := csf.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	if err := csf.Truncate(off); err != nil {
		return err
	}
	csf.Sync()
	csf.Close()

	if err := renameFile(csf.Name(), candidates[0].Name()); err != nil {
		return err
	}
	for _, f := range candidates[1:] {
		if err := os.RemoveAll(f.Name()); err != nil {
			return errors.Wrap(err, "delete WAL segment file")
		}
		f.Close()
	}
	if err := w.dirFile.Sync(); err != nil {
		return err
	}

	// The file object of csf still holds the name before rename. Recreate it so
	// subsequent truncations do not look at a non-existant file name.
	csf.File, err = w.openSegmentFile(candidates[0].Name())
	if err != nil {
		return err
	}
	// We don't need it to be open.
	csf.Close()

	w.mtx.Lock()
	w.files = append([]*segmentFile{csf}, w.files[len(candidates):]...)
	w.mtx.Unlock()

	return nil
}

// Close syncs all data and closes the underlying resources.
func (w *SegmentWAL) Close() error {
	close(w.stopc)
	<-w.donec

	w.mtx.Lock()
	defer w.mtx.Unlock()

	if err := w.sync(); err != nil {
		return err
	}
	// On opening, a WAL must be fully consumed once. Afterwards
	// only the current segment will still be open.
	if hf := w.head(); hf != nil {
		return errors.Wrapf(hf.Close(), "closing WAL head %s", hf.Name())
	}
	return nil
}

const (
	minSectorSize = 512

	// walPageBytes is the alignment for flushing records to the backing Writer.
	// It should be a multiple of the minimum sector size so that WAL can safely
	// distinguish between torn writes and ordinary data corruption.
	walPageBytes = 16 * minSectorSize
)

func (w *SegmentWAL) write(buf []byte) error {
	// Cut to the next segment if the entry exceeds the file size unless it would also
	// exceed the size of a new segment.
	// TODO(gouthamve): Add a test for this case where the commit is greater than segmentSize.
	var (
		sz    = int64(len(buf)) + 6
		newsz = w.curN + sz
	)
	// XXX(fabxc): this currently cuts a new file whenever the WAL was newly opened.
	// Probably fine in general but may yield a lot of short files in some cases.
	if w.cur == nil || w.curN > w.segmentSize || newsz > w.segmentSize && sz <= w.segmentSize {
		if err := w.cut(); err != nil {
			return err
		}
	}
	n, err := w.cur.Write(buf)

	w.curN += int64(n)

	return err
}

// walReader decodes and emits write ahead log entries.
type walReader struct {
	logger log.Logger

	files []*segmentFile
	cur   int
	buf   []byte
	crc32 hash.Hash32

	curType    WALEntryType
	curFlag    byte
	curBuf     []byte
	lastOffset int64 // offset after last successfully read entry

	err error
}

func newWALReader(files []*segmentFile, l log.Logger) *walReader {
	if l == nil {
		l = log.NewNopLogger()
	}
	return &walReader{
		logger: l,
		files:  files,
		buf:    make([]byte, 0, 128*4096),
		crc32:  newCRC32(),
	}
}

// Err returns the last error the reader encountered.
func (r *walReader) Err() error {
	return r.err
}

func (r *walReader) Read(
	seriesf func([]RefSeries),
	samplesf func([]RefSample),
	deletesf func([]Stone),
) error {
	// Concurrency for replaying the WAL is very limited. We at least split out decoding and
	// processing into separate threads.
	// Historically, the processing is the bottleneck with reading and decoding using only
	// 15% of the CPU.
	var (
		seriesPool sync.Pool
		samplePool sync.Pool
		deletePool sync.Pool
	)
	donec := make(chan struct{})
	datac := make(chan interface{}, 100)

	go func() {
		defer close(donec)

		for x := range datac {
			switch v := x.(type) {
			case []RefSeries:
				if seriesf != nil {
					seriesf(v)
				}
				seriesPool.Put(v[:0])
			case []RefSample:
				if samplesf != nil {
					samplesf(v)
				}
				samplePool.Put(v[:0])
			case []Stone:
				if deletesf != nil {
					deletesf(v)
				}
				deletePool.Put(v[:0])
			default:
				level.Error(r.logger).Log("msg", "unexpected data type")
			}
		}
	}()

	var err error

	for r.next() {
		et, flag, b := r.at()

		// In decoding below we never return a walCorruptionErr for now.
		// Those should generally be catched by entry decoding before.
		switch et {
		case WALEntrySeries:
			var series []RefSeries
			if v := seriesPool.Get(); v == nil {
				series = make([]RefSeries, 0, 512)
			} else {
				series = v.([]RefSeries)
			}

			err := r.decodeSeries(flag, b, &series)
			if err != nil {
				err = errors.Wrap(err, "decode series entry")
				break
			}
			datac <- series

		case WALEntrySamples:
			var samples []RefSample
			if v := samplePool.Get(); v == nil {
				samples = make([]RefSample, 0, 512)
			} else {
				samples = v.([]RefSample)
			}

			err := r.decodeSamples(flag, b, &samples)
			if err != nil {
				err = errors.Wrap(err, "decode samples entry")
				break
			}
			datac <- samples

			// Update the times for the WAL segment file.
			cf := r.current()
			for _, s := range samples {
				if cf.maxTime < s.T {
					cf.maxTime = s.T
				}
			}
		case WALEntryDeletes:
			var deletes []Stone
			if v := deletePool.Get(); v == nil {
				deletes = make([]Stone, 0, 512)
			} else {
				deletes = v.([]Stone)
			}

			err := r.decodeDeletes(flag, b, &deletes)
			if err != nil {
				err = errors.Wrap(err, "decode delete entry")
				break
			}
			datac <- deletes

			// Update the times for the WAL segment file.
			cf := r.current()
			for _, s := range deletes {
				for _, iv := range s.intervals {
					if cf.maxTime < iv.Maxt {
						cf.maxTime = iv.Maxt
					}
				}
			}
		}
	}
	close(datac)
	<-donec

	if err != nil {
		return err
	}
	if r.Err() != nil {
		return errors.Wrap(r.Err(), "read entry")
	}
	return nil
}

func (r *walReader) at() (WALEntryType, byte, []byte) {
	return r.curType, r.curFlag, r.curBuf
}

// next returns decodes the next entry pair and returns true
// if it was succesful.
func (r *walReader) next() bool {
	if r.cur >= len(r.files) {
		return false
	}
	cf := r.files[r.cur]

	// Remember the offset after the last correctly read entry. If the next one
	// is corrupted, this is where we can safely truncate.
	r.lastOffset, r.err = cf.Seek(0, os.SEEK_CUR)
	if r.err != nil {
		return false
	}

	et, flag, b, err := r.entry(cf)
	// If we reached the end of the reader, advance to the next one
	// and close.
	// Do not close on the last one as it will still be appended to.
	if err == io.EOF {
		if r.cur == len(r.files)-1 {
			return false
		}
		// Current reader completed, close and move to the next one.
		if err := cf.Close(); err != nil {
			r.err = err
			return false
		}
		r.cur++
		return r.next()
	}
	if err != nil {
		r.err = err
		return false
	}

	r.curType = et
	r.curFlag = flag
	r.curBuf = b
	return r.err == nil
}

func (r *walReader) current() *segmentFile {
	return r.files[r.cur]
}

// walCorruptionErr is a type wrapper for errors that indicate WAL corruption
// and trigger a truncation.
type walCorruptionErr struct {
	err        error
	file       int
	lastOffset int64
}

func (e walCorruptionErr) Error() string {
	return fmt.Sprintf("%s <file: %d, lastOffset: %d>", e.err, e.file, e.lastOffset)
}

func (r *walReader) corruptionErr(s string, args ...interface{}) error {
	return walCorruptionErr{
		err:        errors.Errorf(s, args...),
		file:       r.cur,
		lastOffset: r.lastOffset,
	}
}

func (r *walReader) entry(cr io.Reader) (WALEntryType, byte, []byte, error) {
	r.crc32.Reset()
	tr := io.TeeReader(cr, r.crc32)

	b := make([]byte, 6)
	if n, err := tr.Read(b); err != nil {
		return 0, 0, nil, err
	} else if n != 6 {
		return 0, 0, nil, r.corruptionErr("invalid entry header size %d", n)
	}

	var (
		etype  = WALEntryType(b[0])
		flag   = b[1]
		length = int(binary.BigEndian.Uint32(b[2:]))
	)
	// Exit if we reached pre-allocated space.
	if etype == 0 {
		return 0, 0, nil, io.EOF
	}
	if etype != WALEntrySeries && etype != WALEntrySamples && etype != WALEntryDeletes {
		return 0, 0, nil, r.corruptionErr("invalid entry type %d", etype)
	}

	if length > len(r.buf) {
		r.buf = make([]byte, length)
	}
	buf := r.buf[:length]

	if n, err := tr.Read(buf); err != nil {
		return 0, 0, nil, err
	} else if n != length {
		return 0, 0, nil, r.corruptionErr("invalid entry body size %d", n)
	}

	if n, err := cr.Read(b[:4]); err != nil {
		return 0, 0, nil, err
	} else if n != 4 {
		return 0, 0, nil, r.corruptionErr("invalid checksum length %d", n)
	}
	if exp, has := binary.BigEndian.Uint32(b[:4]), r.crc32.Sum32(); has != exp {
		return 0, 0, nil, r.corruptionErr("unexpected CRC32 checksum %x, want %x", has, exp)
	}

	return etype, flag, buf, nil
}

func (r *walReader) decodeSeries(flag byte, b []byte, res *[]RefSeries) error {
	dec := decbuf{b: b}

	for len(dec.b) > 0 && dec.err() == nil {
		ref := dec.be64()

		lset := make(labels.Labels, dec.uvarint())

		for i := range lset {
			lset[i].Name = dec.uvarintStr()
			lset[i].Value = dec.uvarintStr()
		}
		sort.Sort(lset)

		*res = append(*res, RefSeries{
			Ref:    ref,
			Labels: lset,
		})
	}
	if dec.err() != nil {
		return dec.err()
	}
	if len(dec.b) > 0 {
		return errors.Errorf("unexpected %d bytes left in entry", len(dec.b))
	}
	return nil
}

func (r *walReader) decodeSamples(flag byte, b []byte, res *[]RefSample) error {
	if len(b) == 0 {
		return nil
	}
	dec := decbuf{b: b}

	var (
		baseRef  = dec.be64()
		baseTime = dec.be64int64()
	)

	for len(dec.b) > 0 && dec.err() == nil {
		dref := dec.varint64()
		dtime := dec.varint64()
		val := dec.be64()

		*res = append(*res, RefSample{
			Ref: uint64(int64(baseRef) + dref),
			T:   baseTime + dtime,
			V:   math.Float64frombits(val),
		})
	}

	if dec.err() != nil {
		return errors.Wrapf(dec.err(), "decode error after %d samples", len(*res))
	}
	if len(dec.b) > 0 {
		return errors.Errorf("unexpected %d bytes left in entry", len(dec.b))
	}
	return nil
}

func (r *walReader) decodeDeletes(flag byte, b []byte, res *[]Stone) error {
	dec := &decbuf{b: b}

	for dec.len() > 0 && dec.err() == nil {
		*res = append(*res, Stone{
			ref: dec.be64(),
			intervals: Intervals{
				{Mint: dec.varint64(), Maxt: dec.varint64()},
			},
		})
	}
	if dec.err() != nil {
		return dec.err()
	}
	if len(dec.b) > 0 {
		return errors.Errorf("unexpected %d bytes left in entry", len(dec.b))
	}
	return nil
}
