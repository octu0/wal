package wal

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/pkg/errors"
)

var (
	ErrLocked = errors.Errorf("locked")
)

type Locker interface {
	TryLock() error
	Unlock() error
}

var (
	_ Locker = (*noopLocker)(nil)
	_ Locker = (*FileLocker)(nil)
)

type noopLocker struct{}

func (lock *noopLocker) TryLock() error {
	return nil
}

func (lock *noopLocker) Unlock() error {
	return nil
}

func newNoopLocker() *noopLocker {
	return new(noopLocker)
}

type FileLocker struct {
	path string
}

func (lock *FileLocker) TryLock() error {
	f, err := os.OpenFile(lock.path, os.O_RDWR|os.O_CREATE, os.FileMode(0600))
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
			return nil // ok
		}
		return errors.WithStack(err)
	}

	lockedPid := binary.BigEndian.Uint64(pidBuf)
	return errors.Wrapf(ErrLocked, "another process locked pid=%d", lockedPid)
}

func (lock *FileLocker) Unlock() error {
	if err := os.Remove(lock.path); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func NewFileLocker(path string) *FileLocker {
	return &FileLocker{path}
}
