package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/igrmk/treemap/v2"
	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"

	"github.com/octu0/wal/codec"
)

type position struct {
	offset int
	size   int
}

// implements
var (
	_ io.WriterTo   = (*segment)(nil)
	_ io.ReaderFrom = (*segment)(nil)
)

type segment struct {
	mutex          *sync.RWMutex
	fileID         FileID
	file           *os.File
	buf            *bufio.Writer
	enc            *codec.Encoder
	index          *treemap.TreeMap[Index, position]
	lastPos        position
	autoSync       bool
	dataloadFunc   DataLoadFunc
	lastWriteTime  time.Time
	lastSyncTime   time.Time
	dataSize       uint64
	reclaimable    uint64
	needCompaction bool
}

func (s *segment) FileID() FileID {
	return s.fileID
}

func (s *segment) Size() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.dataSize
}

func (s *segment) ReclaimableSpace() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.reclaimable
}

func (s *segment) NeedCompaction() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.needCompaction
}

func (s *segment) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.syncLocked(); err != nil {
		return errors.WithStack(err)
	}
	if err := s.file.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *segment) Sync() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.syncLocked(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *segment) syncLocked() error {
	if s.lastWriteTime.IsZero() {
		// No Sync needed since not yet data written.
		return nil
	}

	if s.lastWriteTime.UnixNano() < s.lastSyncTime.UnixNano() {
		// No Sync needed since data has not been written after Sync()
		return nil
	}

	if err := s.buf.Flush(); err != nil {
		return errors.WithStack(err)
	}
	if err := s.file.Sync(); err != nil {
		return errors.WithStack(err)
	}
	s.lastSyncTime = time.Now()
	return nil
}

func (s *segment) flushAndAutoSyncLocked() error {
	if err := s.buf.Flush(); err != nil {
		return errors.WithStack(err)
	}
	if s.autoSync {
		if err := s.file.Sync(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *segment) Write(idx Index, data []byte, sync bool) (position, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	pos, err := s.writeLocked(idx, data, true, sync)
	if err != nil {
		return position{}, errors.WithStack(err)
	}
	return pos, nil
}

func (s *segment) writeLocked(idx Index, data []byte, flush, sync bool) (position, error) {
	currentPos := s.lastPos
	size, err := s.enc.Encode(codec.ID(idx), data)
	if err != nil {
		return position{}, errors.WithStack(err)
	}

	if flush {
		if err := s.buf.Flush(); err != nil {
			return position{}, errors.WithStack(err)
		}
	}
	if sync {
		if err := s.file.Sync(); err != nil {
			return position{}, errors.WithStack(err)
		}
	}

	newPos := position{
		offset: currentPos.offset + currentPos.size,
		size:   size,
	}
	s.index.Set(idx, newPos)
	s.dataSize += uint64(size)
	s.lastPos = newPos
	s.lastWriteTime = time.Now()
	return newPos, nil
}

func (s *segment) Read(idx Index) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	pos, ok := s.index.Get(idx)
	if ok != true {
		return []byte{}, errors.WithStack(ErrNotFound)
	}

	data, err := s.decodeAtLocked(pos)
	if err != nil {
		return []byte{}, errors.WithStack(err)
	}
	return data, nil
}

func (s *segment) decodeAtLocked(pos position) ([]byte, error) {
	r, err := openRead(s.file.Name())
	if err != nil {
		return []byte{}, errors.WithStack(err)
	}
	defer r.Close()

	lim := io.NewSectionReader(r, int64(pos.offset), int64(pos.size))
	_, data, err := codec.NewDecoder(lim).Decode()
	if err != nil {
		return []byte{}, errors.WithStack(err)
	}
	return data, nil
}

func (s *segment) Delete(idx Index) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if pos, ok := s.index.Get(idx); ok {
		s.index.Del(idx)
		s.reclaimable += uint64(pos.size)
		s.needCompaction = true
	}
}

func (s *segment) CopyTo(dst *segment) ([]Index, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	dst.mutex.Lock()
	defer dst.mutex.Unlock()

	indexes := make([]Index, 0, s.index.Len())
	for it := s.index.Iterator(); it.Valid(); it.Next() {
		idx := it.Key()
		pos := it.Value()

		data, err := s.decodeAtLocked(pos)
		if err != nil {
			return []Index{}, errors.WithStack(err)
		}
		if _, err := dst.writeLocked(idx, data, false, false); err != nil {
			return []Index{}, errors.WithStack(err)
		}
		indexes = append(indexes, idx)
	}
	if err := dst.flushAndAutoSyncLocked(); err != nil {
		return []Index{}, errors.WithStack(err)
	}
	return indexes, nil
}

func (s *segment) WriteTo(w io.Writer) (int64, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	enc := codec.NewEncoder(w)
	written := int64(0)
	for it := s.index.Iterator(); it.Valid(); it.Next() {
		idx := it.Key()
		pos := it.Value()
		data, err := s.decodeAtLocked(pos)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		size, err := enc.Encode(codec.ID(idx), data)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		written += int64(size)
	}
	return written, nil
}

func (s *segment) ReadFrom(r io.Reader) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	dec := codec.NewDecoder(r)
	readed := int64(0)
	for {
		head, data, err := dec.Decode()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, errors.WithStack(err)
		}
		pos, err := s.writeLocked(Index(head.ID), data, false, false)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if s.dataloadFunc != nil {
			if err := s.dataloadFunc(Index(head.ID), data); err != nil {
				return 0, errors.WithStack(err)
			}
		}
		readed += int64(pos.size)
	}
	if err := s.flushAndAutoSyncLocked(); err != nil {
		return 0, errors.WithStack(err)
	}
	return readed, nil
}

func newSegment(dir string, fileID FileID, file *os.File, index *treemap.TreeMap[Index, position], lastPos position, writeBufferSize int, autoSync bool, dataloadFunc DataLoadFunc) *segment {
	buf := bufio.NewWriterSize(file, writeBufferSize)
	enc := codec.NewEncoder(buf)
	return &segment{
		mutex:          new(sync.RWMutex),
		fileID:         fileID,
		file:           file,
		buf:            buf,
		enc:            enc,
		index:          index,
		lastPos:        lastPos,
		autoSync:       autoSync,
		dataloadFunc:   dataloadFunc,
		lastWriteTime:  time.Time{},
		lastSyncTime:   time.Time{},
		dataSize:       0,
		reclaimable:    0,
		needCompaction: false,
	}
}

func loadSegment(dir, fileName string, writeBufferSize int, autoSync bool, dataloadFunc DataLoadFunc) (*segment, []Index, error) {
	if strings.HasSuffix(fileName, segmentFileExt) != true {
		return nil, nil, errors.Errorf("%s does not segment file pattern(%s)", fileName, segmentFileExt)
	}
	base := fileName[0 : len(fileName)-len(segmentFileExt)]
	fileID, ok := ParseFileID(base)
	if ok != true {
		return nil, nil, errors.Errorf("file %s(%s) does not FileID pattern", base, fileName)
	}

	path := filepath.Join(dir, fileName)
	f, err := os.OpenFile(path, os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "file %s", fileName)
	}
	defer f.Close()

	dataSize := uint64(0)
	indexes := make([]Index, 0, 64)
	lastPos := position{}
	index := treemap.New[Index, position]()
	dec := codec.NewDecoder(f)
	for {
		var head codec.Header
		var data []byte
		var err error

		if dataloadFunc != nil {
			head, data, err = dec.Decode()
		} else {
			head, err = dec.DecodeHeader()
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, nil, errors.WithStack(err)
		}

		idx := Index(head.ID)
		newPos := position{
			offset: lastPos.offset + lastPos.size,
			size:   codec.HeaderSize() + int(head.DataSize),
		}
		lastPos = newPos
		indexes = append(indexes, idx)
		index.Set(idx, newPos)
		dataSize += uint64(newPos.size)

		if dataloadFunc != nil {
			if err := dataloadFunc(idx, data); err != nil {
				return nil, nil, errors.WithStack(err)
			}
		}
	}

	w, err := openWrite(path)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	seg := newSegment(dir, fileID, w, index, lastPos, writeBufferSize, autoSync, dataloadFunc)
	seg.dataSize = dataSize
	return seg, indexes, nil
}

func createSegment(dir string, writeBufferSize int, autoSync bool, dataloadFunc DataLoadFunc) (*segment, error) {
	fileID := NextFileID()
	segmentName := fmt.Sprintf(segmentFileNameFormat, fileID.String())
	path := filepath.Join(dir, segmentName)
	file, err := openWrite(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return newSegment(dir, fileID, file, treemap.New[Index, position](), position{}, writeBufferSize, autoSync, dataloadFunc), nil
}

func openWrite(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0600))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return f, nil
}

func openRead(path string) (*mmap.ReaderAt, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return f, nil
}
