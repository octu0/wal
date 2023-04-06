package wal

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/igrmk/treemap/v2"
	"github.com/pkg/errors"

	"github.com/octu0/wal/codec"
)

const (
	walLockFileName       string = "LOCK"
	segmentFileExt        string = ".wal"
	segmentFilePattern    string = "*" + segmentFileExt  // *.wal
	segmentFileNameFormat string = "%s" + segmentFileExt // %s.wal
)

type Index codec.ID

type DataLoadFunc func(Index, []byte) error

// implements
var (
	_ io.WriterTo   = (*Log)(nil)
	_ io.ReaderFrom = (*Log)(nil)
)

type Log struct {
	mutex          *sync.RWMutex
	opt            *logOpt
	dir            string
	indexes        *treemap.TreeMap[Index, FileID]
	segments       map[FileID]*segment
	currentSegment *segment
	lastIndex      Index
	locker         Locker
	compacting     bool
	closed         bool
}

func (l *Log) LastIndex() Index {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.lastIndex
}

func (l *Log) ReclaimableSpace() uint64 {
	reclaimable := uint64(0)
	for _, s := range l.segments {
		reclaimable += s.ReclaimableSpace()
	}
	return reclaimable
}

func (l *Log) NeedCompaction() bool {
	needCompaction := false
	for _, s := range l.segments {
		if s.NeedCompaction() {
			needCompaction = true
		}
	}
	return needCompaction
}

func (l *Log) Size() uint64 {
	size := uint64(0)
	for _, s := range l.segments {
		size += s.Size()
	}
	return size
}

func (l *Log) Len() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.indexes.Len()
}

func (l *Log) Close() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if err := l.closeLocked(true); err != nil {
		return errors.WithStack(err)
	}
	if l.opt.closeCompaction {
		if l.NeedCompaction() {
			if err := l.rotateSegmentLocked(); err != nil {
				return errors.WithStack(err)
			}

			_, _, mergedSegments, err := l.copyLatestLocked()
			if err != nil {
				return errors.WithStack(err)
			}
			for _, merged := range mergedSegments {
				if err := os.Remove(merged.file.Name()); err != nil {
					return errors.WithStack(err)
				}
			}
		}
	}
	return nil
}

func (l *Log) closeLocked(removeLockFile bool) error {
	if l.closed {
		return nil
	}

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return errors.WithStack(err)
		}
	}

	if removeLockFile {
		if err := l.locker.Unlock(); err != nil {
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

	for _, s := range l.segments {
		if err := s.Sync(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (l *Log) nextIndexLocked() Index {
	l.lastIndex += 1
	return l.lastIndex
}

func (l *Log) Write(data []byte) (Index, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return Index(0), errors.WithStack(ErrClosed)
	}

	idx := l.nextIndexLocked()
	if err := l.writeAtLocked(idx, data); err != nil {
		return Index(0), errors.WithStack(err)
	}
	return idx, nil
}

func (l *Log) WriteAt(idx Index, data []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return errors.WithStack(ErrClosed)
	}

	if err := l.writeAtLocked(idx, data); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (l *Log) writeAtLocked(idx Index, data []byte) error {
	fileID := l.currentSegment.FileID()
	if _, err := l.currentSegment.Write(idx, data, l.opt.sync); err != nil {
		return errors.WithStack(err)
	}
	l.indexes.Set(idx, fileID)
	return nil
}

func (l *Log) Read(idx Index) ([]byte, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.closed {
		return []byte{}, errors.WithStack(ErrClosed)
	}

	fileID, ok := l.indexes.Get(idx)
	if ok != true {
		return []byte{}, errors.WithStack(ErrNotFound)
	}
	seg, ok := l.segments[fileID]
	if ok != true {
		return []byte{}, errors.WithStack(ErrSegmentNotOpen)
	}
	data, err := seg.Read(idx)
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

	for _, idx := range idxs {
		l.deleteLocked(idx)
	}
	return nil
}

func (l *Log) deleteLocked(idx Index) error {
	if fileID, ok := l.indexes.Get(idx); ok {
		if seg, exists := l.segments[fileID]; exists {
			seg.Delete(idx)
			l.indexes.Del(idx)
		}
	}
	return nil
}

func (l *Log) rotateSegment() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if err := l.rotateSegmentLocked(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (l *Log) rotateSegmentLocked() error {
	if err := l.currentSegment.Sync(); err != nil {
		return errors.WithStack(err)
	}

	newSegment, err := createSegment(l.dir, l.opt.writeBufferSize, l.opt.sync, l.opt.dataloadFunc)
	if err != nil {
		return errors.Wrapf(err, "failed to create segment(rotate)")
	}
	l.segments[newSegment.FileID()] = newSegment
	l.currentSegment = newSegment
	return nil
}

func (l *Log) compactRunning() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.compacting
}

func (l *Log) copyLatest() (*treemap.TreeMap[Index, FileID], map[FileID]*segment, []*segment, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.copyLatestLocked()
}

func (l *Log) copyLatestLocked() (*treemap.TreeMap[Index, FileID], map[FileID]*segment, []*segment, error) {
	currentFileID := l.currentSegment.FileID()
	targetSegments := make([]*segment, 0, len(l.segments))
	for _, s := range l.segments {
		if s.FileID().Equal(currentFileID) {
			continue
		}
		targetSegments = append(targetSegments, s)
	}

	needMergeSegments := make([]*segment, 0, len(targetSegments))
	passThorughSegments := make([]*segment, 0, len(targetSegments))
	for _, target := range targetSegments {
		if target.NeedCompaction() {
			needMergeSegments = append(needMergeSegments, target)
		} else {
			passThorughSegments = append(passThorughSegments, target)
		}
	}

	newSegment, err := createSegment(l.dir, l.opt.writeBufferSize, l.opt.sync, l.opt.dataloadFunc)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed to create segment(copyLatest)")
	}

	newIndexes := make([]Index, 0, l.indexes.Len())
	for _, merge := range needMergeSegments {
		idxs, err := merge.CopyTo(newSegment)
		if err != nil {
			return nil, nil, nil, errors.Wrapf(err, "failed to Copy(new)")
		}
		newIndexes = append(newIndexes, idxs...)
	}

	newLogIndexes := treemap.New[Index, FileID]()
	for _, idx := range newIndexes {
		newLogIndexes.Set(idx, newSegment.FileID())
	}

	newSegments := make(map[FileID]*segment, len(passThorughSegments)+1)
	for _, p := range passThorughSegments {
		newSegments[p.FileID()] = p
	}
	newSegments[newSegment.FileID()] = newSegment
	newSegments[l.currentSegment.FileID()] = l.currentSegment

	return newLogIndexes, newSegments, needMergeSegments, nil
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

	if err := l.rotateSegment(); err != nil {
		return errors.WithStack(err)
	}

	newIndexes, newSegments, mergedSegments, err := l.copyLatest()
	if err != nil {
		return errors.WithStack(err)
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	for it := l.indexes.Iterator(); it.Valid(); it.Next() {
		oldIndex := it.Key()
		oldFileID := it.Value()
		if newIndexes.Contains(oldIndex) {
			continue
		}
		newIndexes.Set(oldIndex, oldFileID)
	}
	newSegments[l.currentSegment.FileID()] = l.currentSegment

	l.indexes = newIndexes
	l.segments = newSegments

	for _, merged := range mergedSegments {
		if err := os.Remove(merged.file.Name()); err != nil {
			return errors.WithStack(err)
		}
	}

	if l.opt.compactFunc != nil {
		l.opt.compactFunc()
	}
	return nil
}

func (l *Log) WriteTo(w io.Writer) (int64, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	fileIds := make([]FileID, 0, len(l.segments))
	for fileID, _ := range l.segments {
		fileIds = append(fileIds, fileID)
	}
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i].Newer(fileIds[j])
	})

	bfw := bufio.NewWriterSize(w, l.opt.writeBufferSize)
	written := int64(0)
	for _, fileID := range fileIds {
		seg := l.segments[fileID]
		size, err := seg.WriteTo(bfw)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		written += size
	}
	if err := bfw.Flush(); err != nil {
		return 0, errors.WithStack(err)
	}
	return written, nil
}

func (l *Log) ReadFrom(r io.Reader) (int64, error) {
	seg, err := createSegment(l.dir, l.opt.writeBufferSize, l.opt.sync, l.opt.dataloadFunc)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create segment(ReadFrom)")
	}
	readed, err := seg.ReadFrom(r)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	newSegmentFileID := seg.FileID()
	for it := seg.index.Iterator(); it.Valid(); it.Next() {
		newIndex := it.Key()
		// delete old index + update new index and new segment
		l.deleteLocked(newIndex)
		l.indexes.Set(newIndex, newSegmentFileID)
	}
	l.segments[newSegmentFileID] = seg

	return readed, nil
}

func Open(dir string, funcs ...OptionFunc) (*Log, error) {
	stat, err := os.Stat(dir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if stat.IsDir() != true {
		return nil, errors.Errorf("%s is not directory", dir)
	}

	flock := NewFileLocker(filepath.Join(dir, walLockFileName))
	if err := flock.TryLock(); err != nil {
		return nil, errors.WithStack(err)
	}

	log, err := openLog(flock, dir, funcs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return log, nil
}

func openLog(locker Locker, dir string, funcs ...OptionFunc) (*Log, error) {
	opt := newLogOpt(funcs...)

	matches, err := filepath.Glob(filepath.Join(dir, segmentFilePattern))
	if err != nil {
		return nil, errors.Wrapf(err, "glob(%s/%s)", dir, segmentFilePattern)
	}
	fileNames := make([]string, len(matches))
	for i, m := range matches {
		fileNames[i] = filepath.Base(m)
	}
	sort.Strings(fileNames)

	lastIndex := Index(0)
	indexes := treemap.New[Index, FileID]()
	segments := make(map[FileID]*segment, len(fileNames))
	for _, f := range fileNames {
		seg, idxs, err := loadSegment(dir, f, opt.writeBufferSize, opt.sync, opt.dataloadFunc)
		if err != nil {
			return nil, errors.Wrapf(err, "loadSegment(%s)", f)
		}
		fileID := seg.FileID()
		for _, idx := range idxs {
			if lastIndex < idx {
				lastIndex = idx
			}
			indexes.Set(idx, fileID)
		}
		segments[fileID] = seg
	}

	newSegment, err := createSegment(dir, opt.writeBufferSize, opt.sync, opt.dataloadFunc)
	if err != nil {
		return nil, errors.Wrapf(err, "createSegment(open)")
	}
	segments[newSegment.FileID()] = newSegment

	return &Log{
		mutex:          new(sync.RWMutex),
		opt:            opt,
		dir:            dir,
		indexes:        indexes,
		segments:       segments,
		currentSegment: newSegment,
		lastIndex:      lastIndex,
		locker:         locker,
		compacting:     false,
		closed:         false,
	}, nil
}
