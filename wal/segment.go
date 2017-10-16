package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/fileutil"
)

// File implements a regular file interface but is backed by a directory containing
// sequential segment files of a fixed size.
// It always grows by full segments that are zerod initially. Truncating it only ever
// removes entire and zeros data in partially truncated segments.
//
// Reads and writes across segment boundaries will fail. File is intended to be used in
// a way for this case to not occur rather than callers explicitly handling it.
type File struct {
	dirFile     *os.File
	segments    []*segmentFile
	segmentSize int64
	curFile     int
}

// OpenFile opens a new segmented file. The flag and permissions are used to
// open segment files. A dir with name must already exist.
func OpenFile(name string, segmentSize int64, flag int, perm os.FileMode) (*File, error) {
	if segmentSize%blockSize != 0 {
		return nil, errors.Wrapf(errInvalidSize, "segment size must be multiple of %d", blockSize)
	}
	var f File

	df, err := fileutil.OpenDir(name)
	if err != nil {
		return nil, err
	}
	f.dirFile = df
	f.segmentSize = segmentSize

	files, err := ioutil.ReadDir(name)
	if err != nil {
		return nil, err
	}
	for _, fi := range files {
		seq, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			continue
		}
		if fi.Size() != segmentSize {
			return nil, errInvalidSize
		}
		path := filepath.Join(name, fi.Name())
		sf, err := os.OpenFile(path, flag, perm)
		if err != nil {
			return nil, err
		}
		f.segments = append(f.segments, &segmentFile{file: sf, seq: seq})
	}
	if len(files) == 0 {
		_, err := createSegmentFile(name, 0, segmentSize)
		if err != nil {
			return nil, err
		}
		return OpenFile(name, segmentSize, flag, perm)
	}
	return &f, nil
}

// Fdatasync does an fdatasync on all segments.
func (f *File) Fdatasync() error {
	for _, s := range f.segments {
		if err := fileutil.Fdatasync(s.file); err != nil {
			return err
		}
	}
	return nil
}

func (f *File) current() *segmentFile {
	return f.segments[f.curFile]
}

var errSegmentFull = errors.New("segment full")

// Seek to in the segment file. Only os.SEEK_CUR with 0 offset is currently supported.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case os.SEEK_CUR:
		if offset != 0 {
			return 0, errors.New("non-zero offset not supported")
		}
		off, err := f.current().Seek(0, os.SEEK_CUR)
		if err != nil {
			return 0, err
		}
		return (int64(f.current().seq) * f.segmentSize) + off, nil
	case os.SEEK_END:
		return 0, errors.New("not supported")
	case os.SEEK_SET:
		return 0, errors.New("not supported")
	default:
		return 0, errors.Errorf("unknown whence %d", whence)
	}
}

// Write b to the segment file. It will return an error if b attempts
// to write across a segment boundary.
// If the current segment is exactly full, a new segment will be created
// into which b is written.
func (f *File) Write(b []byte) (int, error) {
	sf := f.current()

	if sf.off == f.segmentSize {
		nsf, err := createSegmentFile(f.dirFile.Name(), sf.seq+1, f.segmentSize)
		if err != nil {
			return 0, err
		}
		f.segments = append(f.segments, nsf)
		f.curFile++
		sf = nsf
	}
	if sf.off+int64(len(b)) > f.segmentSize {
		return 0, errors.New("write across segment boundary not allowed")
	}
	return sf.Write(b)
}

// Read into b at the current position. It will return an error if b attempts
// to read across a segment boundary.
func (f *File) Read(b []byte) (int, error) {
	sf := f.current()

	if sf.off+int64(len(b)) > f.segmentSize {
		return 0, errors.New("write across segment boundary not allowed")
	}
	if sf.off == f.segmentSize {
		f.curFile++
		sf = f.current()
	}
	return sf.Read(b)
}

// Truncate the segmented file after the given position. All pre-allocated space
// in the segment offset points to will be zerod out after offset. Subsequent segments
// will be deleted.
func (f *File) Truncate(offset int64) error {
	seq := uint64(offset / f.segmentSize)

	var sf *segmentFile
	var pos int
	for i, x := range f.segments {
		if x.seq == seq {
			sf = x
			pos = i
		}
		if x.seq >= seq {
			break
		}
	}
	if sf == nil {
		return errors.New("offset does not resolve to valid segment")
	}

	// Delete all following sequence files.
	for _, x := range f.segments[pos+1:] {
		if err := os.Remove(x.file.Name()); err != nil {
			return err
		}
	}
	f.segments = f.segments[:pos+1]

	return sf.Truncate(offset % f.segmentSize)
}

// Close all underlying segment files and return the last encountered error.
func (f *File) Close() (err error) {
	for _, sf := range f.segments {
		if e := sf.file.Close(); e != nil {
			e = err
		}
	}
	return err
}

var errInvalidSize = errors.New("invalid size")

// segmentFile wraps a regular file and only allows writes in full block sizes
// and within its size limits.
// It generally requires the first block to adhere to the segment format.
type segmentFile struct {
	file *os.File
	seq  uint64
	off  int64
}

// createSegmentFile creates a new segment file with the given name. It preallocates
// the standard segment size if possible and writes the header.
func createSegmentFile(dir string, seq uint64, size int64) (*segmentFile, error) {
	path := filepath.Join(dir, fmt.Sprintf("%.6d", seq))

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err = fileutil.Preallocate(f, size, true); err != nil {
		return nil, err
	}
	return &segmentFile{seq: seq, file: f}, nil
}

func (sf *segmentFile) Write(b []byte) (int, error) {
	n, err := sf.file.Write(b)
	sf.off += int64(n)
	return n, err
}

func (sf *segmentFile) Read(b []byte) (int, error) {
	n, err := sf.file.Read(b)
	sf.off += int64(n)
	return n, err
}

func (sf *segmentFile) Seek(off int64, whence int) (int64, error) {
	if off != 0 && whence != os.SEEK_CUR {
		return 0, errors.New("invalid seek arguments")
	}
	return sf.file.Seek(0, os.SEEK_CUR)
}

// Truncate zeros out the file after offset.
func (sf *segmentFile) Truncate(offset int64) error {
	var zero [blockSize]byte

	// Zero remainding bytes in current block.
	_, err := sf.file.WriteAt(zero[offset%blockSize:], offset)
	if err != nil {
		return err
	}
	fi, err := sf.file.Stat()
	if err != nil {
		return err
	}
	// Zero out all remaining blocks.
	for i := (offset / blockSize) + 1; i < fi.Size()/blockSize; i++ {
		if _, err := sf.file.WriteAt(zero[:], int64(i)*blockSize); err != nil {
			return err
		}
	}
	return nil
}
