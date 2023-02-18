package wal

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"

	"github.com/octu0/wal/codec"
)

const (
	walFileName     string = "data.wal"
	walTempFileName string = "data.waltemp"
	walLockFileName string = "data.lock"
)

var (
	ErrClosed         = errors.New("closed")
	ErrCompactRunning = errors.New("compat already in progress")
	ErrNotFound       = errors.New("not found")
	ErrLogLocked      = errors.New("cloed")
)

type Index codec.ID

type position struct {
	offset int
	size   int
}

type Log struct {
	mutex          *sync.RWMutex
	opt            *logOpt
	dir            string
	lastPos        position
	lastIndex      Index
	indexes        map[Index]position
	wfile          *os.File
	wbuf           *bufio.Writer
	enc            *codec.Encoder
	reclaimable    uint64
	needCompaction bool
	compacting     bool
	closed         bool
}

func (l *Log) LastIndex() Index {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.lastIndex
}

func (l *Log) ReclaimableSpace() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.reclaimable
}

func (l *Log) NeedCompaction() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.needCompaction
}

func (l *Log) Len() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return len(l.indexes)
}

func (l *Log) Close() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if err := l.closeLocked(true); err != nil {
		return errors.WithStack(err)
	}
	if l.opt.closeCompaction {
		if l.needCompaction {
			newLog, _, err := l.copyLatestLocked()
			if err != nil {
				return errors.WithStack(err)
			}
			if err := newLog.closeLocked(false); err != nil {
				return errors.WithStack(err)
			}
			if err := os.Rename(newLog.wfile.Name(), l.wfile.Name()); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func (l *Log) closeLocked(removeLockFile bool) error {
	if l.closed {
		return nil
	}

	if err := l.wbuf.Flush(); err != nil {
		return errors.WithStack(err)
	}
	if err := l.wfile.Sync(); err != nil {
		return errors.WithStack(err)
	}
	if err := l.wfile.Close(); err != nil {
		return errors.WithStack(err)
	}
	if removeLockFile {
		if err := os.Remove(filepath.Join(l.dir, walLockFileName)); err != nil {
			return errors.WithStack(err)
		}
	}
	l.closed = true
	return nil
}

func (l *Log) Sync() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return errors.WithStack(ErrClosed)
	}

	if err := l.wbuf.Flush(); err != nil {
		return errors.WithStack(err)
	}
	if err := l.wfile.Sync(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (l *Log) Write(data []byte) (Index, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return Index(0), errors.WithStack(ErrClosed)
	}

	index := l.lastIndex
	_, err := l.writeLocked(index, data, true)
	if err != nil {
		return Index(0), errors.WithStack(err)
	}
	return index, nil
}

func (l *Log) WriteAt(idx Index, data []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return errors.WithStack(ErrClosed)
	}

	_, err := l.writeLocked(idx, data, true)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (l *Log) writeLocked(idx Index, data []byte, flush bool) (Index, error) {
	currentPos := l.lastPos
	size, err := l.enc.Encode(codec.ID(idx), data)
	if err != nil {
		return Index(0), errors.WithStack(err)
	}
	if flush {
		if err := l.wbuf.Flush(); err != nil {
			return Index(0), errors.WithStack(err)
		}
	}
	if l.opt.sync {
		if err := l.wfile.Sync(); err != nil {
			return Index(0), errors.WithStack(err)
		}
	}

	newIndex := idx + 1
	newPos := position{
		offset: currentPos.offset + currentPos.size,
		size:   size,
	}

	l.lastPos = newPos
	l.indexes[idx] = newPos
	l.lastIndex = newIndex
	return newIndex, nil
}

func (l *Log) Read(idx Index) ([]byte, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.closed {
		return []byte{}, errors.WithStack(ErrClosed)
	}

	pos, ok := l.indexes[idx]
	if ok != true {
		return []byte{}, errors.WithStack(ErrNotFound)
	}

	data, err := l.decodeAtLocked(pos)
	if err != nil {
		return []byte{}, errors.WithStack(err)
	}
	return data, nil
}

func (l *Log) decodeAtLocked(pos position) ([]byte, error) {
	r, err := openRead(l.wfile.Name())
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

func (l *Log) Delete(idxs ...Index) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return errors.WithStack(ErrClosed)
	}

	currReclaimable := l.reclaimable
	for _, id := range idxs {
		if pos, ok := l.indexes[id]; ok {
			currReclaimable += uint64(pos.size)
			delete(l.indexes, id)
		}
	}
	l.reclaimable = currReclaimable
	l.needCompaction = true
	return nil
}

func (l *Log) compactRunning() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.compacting
}

func (l *Log) copyLatest() (*Log, position, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.copyLatestLocked()
}

func (l *Log) copyLatestLocked() (*Log, position, error) {
	prevPos := l.lastPos
	newLog, err := openLog(l.dir, walTempFileName, WithSync(false), WithWriteBufferSize(l.opt.writeBufferSize))
	if err != nil {
		return nil, position{}, errors.WithStack(err)
	}

	// sequencial read (reduce seek)
	indexes := make([]Index, 0, len(l.indexes))
	for idx, _ := range l.indexes {
		indexes = append(indexes, idx)
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})

	for _, idx := range indexes {
		pos := l.indexes[idx]

		data, err := l.decodeAtLocked(pos)
		if err != nil {
			return nil, position{}, errors.WithStack(err)
		}
		if _, err := newLog.writeLocked(idx, data, false); err != nil {
			return nil, position{}, errors.WithStack(err)
		}
	}

	return newLog, prevPos, nil
}

func (l *Log) copyBehindLocked(newLog *Log, prevPos position, targetPos position) error {
	f, err := os.OpenFile(l.wfile.Name(), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	lim := io.NewSectionReader(f, int64(prevPos.offset+prevPos.size), int64(l.lastPos.offset+l.lastPos.size))
	dec := codec.NewDecoder(lim)
	for {
		head, data, err := dec.Decode()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.WithStack(err)
		}
		if _, err := newLog.writeLocked(Index(head.ID), data, false); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (l *Log) Compact() error {
	if l.compactRunning() {
		return errors.WithStack(ErrCompactRunning)
	}

	l.mutex.Lock()
	closed := l.closed
	l.compacting = true
	l.mutex.Unlock()
	defer func() {
		l.mutex.Lock()
		l.compacting = false
		l.mutex.Unlock()
	}()

	if closed {
		return errors.WithStack(ErrClosed)
	}

	// flush buffer and sync
	if err := l.Sync(); err != nil {
		return errors.WithStack(err)
	}

	newLog, prevPos, err := l.copyLatest()
	if err != nil {
		return errors.WithStack(err)
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if err := l.closeLocked(false); err != nil {
		return errors.WithStack(err)
	}
	if prevPos.offset < l.lastPos.offset {
		if err := l.copyBehindLocked(newLog, prevPos, l.lastPos); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := newLog.closeLocked(false); err != nil {
		return errors.WithStack(err)
	}
	if err := os.Rename(newLog.wfile.Name(), l.wfile.Name()); err != nil {
		return errors.WithStack(err)
	}

	wf, err := openWrite(l.wfile.Name())
	if err != nil {
		return errors.WithStack(err)
	}
	l.wbuf.Reset(wf)
	enc := codec.NewEncoder(l.wbuf)

	l.lastPos = newLog.lastPos
	l.lastIndex = newLog.lastIndex
	l.indexes = newLog.indexes
	l.wfile = wf
	l.enc = enc
	l.reclaimable = 0
	l.needCompaction = false
	l.closed = false

	if l.opt.compactFunc != nil {
		l.opt.compactFunc()
	}
	return nil
}

func Open(dir string, funcs ...OptionFunc) (*Log, error) {
	stat, err := os.Stat(dir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if stat.IsDir() != true {
		return nil, errors.Errorf("%s is not directory", dir)
	}

	if err := tryLock(dir, walLockFileName); err != nil {
		return nil, errors.WithStack(err)
	}

	log, err := openLog(dir, walFileName, funcs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return log, nil
}

func tryLock(dir, name string) error {
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.FileMode(0600))
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	pid := os.Getpid()
	pidBuf := make([]byte, 8)
	if _, err := f.Read(pidBuf); err != nil {
		if errors.Is(err, io.EOF) {
			// empty, write locked pid
			binary.BigEndian.PutUint64(pidBuf, uint64(pid))

			if _, err := f.Write(pidBuf); err != nil {
				return errors.WithStack(err)
			}
			if err := f.Sync(); err != nil {
				return errors.WithStack(err)
			}
			return nil
		}
		return errors.WithStack(err)
	}

	lockedPid := binary.BigEndian.Uint64(pidBuf)
	return errors.Wrapf(ErrLogLocked, "another process locked=%d", lockedPid)
}

func openLog(dir, name string, funcs ...OptionFunc) (*Log, error) {
	opt := newLogOpt(funcs...)

	path := filepath.Join(dir, name)
	lastPos, lastIndex, indexes, err := loadLog(path, opt)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	wf, err := openWrite(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	wbuf := bufio.NewWriterSize(wf, opt.writeBufferSize)
	enc := codec.NewEncoder(wbuf)

	return &Log{
		mutex:       new(sync.RWMutex),
		opt:         opt,
		dir:         dir,
		lastPos:     lastPos,
		lastIndex:   lastIndex,
		indexes:     indexes,
		wfile:       wf,
		wbuf:        wbuf,
		enc:         enc,
		reclaimable: 0,
		compacting:  false,
		closed:      false,
	}, nil
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

func loadLog(path string, opt *logOpt) (position, Index, map[Index]position, error) {
	stat, err := os.Stat(path)
	if err != nil {
		// no file
		return position{}, Index(0), make(map[Index]position, 64), nil
	}
	if stat.IsDir() {
		return position{}, Index(0), nil, errors.Errorf("%s is directory", path)
	}

	f, err := os.OpenFile(path, os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return position{}, Index(0), nil, errors.WithStack(err)
	}
	defer f.Close()

	lastPos := position{}
	lastIndex := Index(0)
	indexes := make(map[Index]position, 64)
	dec := codec.NewDecoder(f)
	for {
		head, err := dec.DecodeHeader()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return position{}, Index(0), nil, errors.WithStack(err)
		}
		newPos := position{
			offset: lastPos.offset + lastPos.size,
			size:   codec.HeaderSize() + int(head.DataSize),
		}
		lastPos = newPos
		lastIndex = Index(head.ID) + 1
		indexes[Index(head.ID)] = newPos
	}
	return lastPos, lastIndex, indexes, nil
}
