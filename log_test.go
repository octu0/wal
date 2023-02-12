package wal

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/pkg/errors"

	"github.com/octu0/wal/codec"
)

func TestLog(t *testing.T) {
	t.Run("Write/Len/Read", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "waltest-*")
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		tt.Cleanup(func() {
			os.RemoveAll(dir)
		})

		log, err := Open(dir)
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		defer log.Close()

		id1, err := log.Write([]byte("test1"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		id2, err := log.Write([]byte("testtest2"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		id3, err := log.Write([]byte("t3"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		tt.Logf("ids=%v", []Index{id1, id2, id3})

		if log.Len() != 3 {
			tt.Errorf("3 item written: %d %+v", log.Len(), log)
		}

		data1, err := log.Read(id1)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if bytes.Equal(data1, []byte("test1")) != true {
			tt.Errorf("data1 actual=%s", data1)
		}
		data2, err := log.Read(id2)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if bytes.Equal(data2, []byte("testtest2")) != true {
			tt.Errorf("data2 actual=%s", data2)
		}
		data3, err := log.Read(id3)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if bytes.Equal(data3, []byte("t3")) != true {
			tt.Errorf("data3 actual=%s", data3)
		}
		tt.Logf("data=%s %s %s", data1, data2, data3)
	})

	t.Run("WriteAt/Len/Read", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "waltest-*")
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		tt.Cleanup(func() {
			os.RemoveAll(dir)
		})

		log, err := Open(dir)
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		defer log.Close()

		id1, err := log.Write([]byte("d1"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if err := log.WriteAt(Index(100), []byte("d100")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		newID, err := log.Write([]byte("d101"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		tt.Logf("ids=%v", []Index{id1, Index(100), newID})

		if log.Len() != 3 {
			tt.Errorf("3 item written: %d %+v", log.Len(), log)
		}

		data1, err := log.Read(id1)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if bytes.Equal(data1, []byte("d1")) != true {
			tt.Errorf("data1 actual=%s", data1)
		}

		data100, err := log.Read(Index(100))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if bytes.Equal(data100, []byte("d100")) != true {
			tt.Errorf("data100 actual=%s", data100)
		}

		dataNew, err := log.Read(newID)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if bytes.Equal(dataNew, []byte("d101")) != true {
			tt.Errorf("dataNew actual=%s", dataNew)
		}
		tt.Logf("data=%s %s %s", data1, data100, dataNew)
	})

	t.Run("NotFound", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "waltest-*")
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		tt.Cleanup(func() {
			os.RemoveAll(dir)
		})

		log, err := Open(dir)
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		defer log.Close()

		if _, err := log.Read(Index(1000)); errors.Is(err, ErrNotFound) != true {
			tt.Errorf("actual=%+v", err)
		}
		if err := log.WriteAt(Index(1000), []byte("test")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		data, err := log.Read(Index(1000))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if bytes.Equal(data, []byte("test")) != true {
			tt.Errorf("actual %s", data)
		}
	})

	t.Run("Delete/ReclaimableSpace", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "waltest-*")
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		tt.Cleanup(func() {
			os.RemoveAll(dir)
		})

		log, err := Open(dir)
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		defer log.Close()

		if 0 != log.ReclaimableSpace() {
			tt.Errorf("no delete")
		}

		id1, err := log.Write([]byte("test1"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}

		if 0 != log.ReclaimableSpace() {
			tt.Errorf("no delete")
		}

		id2, err := log.Write([]byte("test12345"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if 0 != log.ReclaimableSpace() {
			tt.Errorf("no delete")
		}
		id3, err := log.Write([]byte("t3"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}

		if err := log.Delete(id1, id3); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		data1Size := len([]byte("test1")) + codec.HeaderSize()
		data3Size := len([]byte("t3")) + codec.HeaderSize()

		if uint64(data1Size+data3Size) != log.ReclaimableSpace() {
			tt.Errorf("actual reclaimable=%d expect=%d+%d", log.ReclaimableSpace(), data1Size, data3Size)
		}

		if _, err := log.Read(id1); errors.Is(err, ErrNotFound) != true {
			tt.Errorf("actual err=%+v", err)
		}
		if _, err := log.Read(id3); errors.Is(err, ErrNotFound) != true {
			tt.Errorf("actual err=%+v", err)
		}

		data2, err := log.Read(id2)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if bytes.Equal(data2, []byte("test12345")) != true {
			tt.Errorf("actual data=%s", data2)
		}
	})
	t.Run("Close/Read/Write", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "waltest-*")
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		tt.Cleanup(func() {
			os.RemoveAll(dir)
		})

		log, err := Open(dir)
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}

		if _, err := log.Write([]byte("1")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if _, err := log.Write([]byte("2")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if _, err := log.Read(Index(0)); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if _, err := log.Read(Index(1)); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if err := log.Delete(Index(0)); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		if err := log.Close(); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		if _, err := log.Write([]byte("3")); errors.Is(err, ErrClosed) != true {
			tt.Errorf("actual err=%+v", err)
		}
		if err := log.WriteAt(Index(100), []byte("4")); errors.Is(err, ErrClosed) != true {
			tt.Errorf("actual err=%+v", err)
		}
		if _, err := log.Read(Index(1)); errors.Is(err, ErrClosed) != true {
			tt.Errorf("actual err=%+v", err)
		}
		if err := log.Delete(Index(1)); errors.Is(err, ErrClosed) != true {
			tt.Errorf("actual err=%+v", err)
		}
		if err := log.Compact(); errors.Is(err, ErrClosed) != true {
			tt.Errorf("actual err=%+v", err)
		}
	})
	t.Run("Compact/Read/Write", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "waltest-*")
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		tt.Cleanup(func() {
			os.RemoveAll(dir)
		})

		log, err := Open(dir)
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		defer log.Close()

		ids := make([]Index, 100)
		for i := 0; i < 100; i += 1 {
			data := fmt.Sprintf("test%03d", i)
			id, err := log.Write([]byte(data))
			if err != nil {
				tt.Errorf("no error: %+v", err)
			}
			ids[i] = id
		}
		stPrev, err := log.wfile.Stat()
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		fileSize := stPrev.Size()

		if err := log.Delete(ids[0:50]...); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		reclaimable := int64(log.ReclaimableSpace())
		prevLastIndex := log.LastIndex()

		if err := log.Compact(); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		stNew, err := log.wfile.Stat()
		expectNewSize := fileSize - reclaimable
		actualSize := stNew.Size()
		if expectNewSize != actualSize {
			tt.Errorf("compact prev=%d after=%d reclaimable=%d expect=%d", fileSize, actualSize, reclaimable, expectNewSize)
		}
		newLastIndex := log.LastIndex()

		if prevLastIndex != newLastIndex {
			tt.Errorf("same LastIndex")
		}
		id, err := log.Write([]byte("test"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if id <= ids[len(ids)-1] {
			tt.Errorf("newID is grater")
		}
		data, err := log.Read(id)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if bytes.Equal(data, []byte("test")) != true {
			tt.Errorf("actual=%s", data)
		}
		for i, id := range ids[51:] {
			expectData := []byte(fmt.Sprintf("test%03d", 51+i))
			actualData, err := log.Read(id)
			if err != nil {
				tt.Errorf("no error: %+v", err)
			}
			if bytes.Equal(expectData, actualData) != true {
				tt.Errorf("expect=%s actual=%s", expectData, actualData)
			}
		}
	})
}

func TestLogOpen(t *testing.T) {
	dir, err := os.MkdirTemp("", "waltest-*")
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	log, err := Open(dir)
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(5)
	for i := 0; i < 5; i += 1 {
		go func(w *sync.WaitGroup) {
			defer w.Done()

			if _, err := Open(dir); errors.Is(err, ErrLogLocked) != true {
				t.Errorf("must locked error: %+v", err)
			}
		}(wg)
	}
	wg.Wait()

	if err := log.Close(); err != nil {
		t.Errorf("no error: %+v", err)
	}

	log2, err := Open(dir)
	if err != nil {
		t.Errorf("no error")
	}
	wg2 := new(sync.WaitGroup)
	wg2.Add(5)
	for i := 0; i < 5; i += 1 {
		go func(w *sync.WaitGroup) {
			defer w.Done()

			if _, err := Open(dir); errors.Is(err, ErrLogLocked) != true {
				t.Errorf("must locked error: %+v", err)
			}
		}(wg2)
	}
	wg2.Wait()

	if err := log2.Close(); err != nil {
		t.Errorf("no error: %+v", err)
	}
}

func TestLogCompact(t *testing.T) {
	dir, err := os.MkdirTemp("", "waltest-*")
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	log, err := Open(dir)
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}
	lock := make(chan struct{})
	log.opt.compactFunc = func() {
		<-lock
	}

	ids := make([]Index, 1000)
	for i := 0; i < 1000; i += 1 {
		data := fmt.Sprintf("test%04d", i)
		id, err := log.Write([]byte(data))
		if err != nil {
			t.Errorf("no error: %+v", err)
		}
		ids[i] = id
	}

	if err := log.Delete(ids[10:]...); err != nil {
		t.Errorf("no error: %+v", err)
	}

	done := make(chan struct{})
	run := make(chan struct{})
	go func(r, d chan struct{}) {
		r <- struct{}{}

		// pass compact first process
		if err := log.Compact(); err != nil {
			t.Errorf("no error: %+v", err)
		}
		d <- struct{}{}
	}(run, done)

	<-run

	wg := new(sync.WaitGroup)
	wg.Add(3)
	for i := 0; i < 3; i += 1 {
		go func(w *sync.WaitGroup) {
			defer w.Done()

			// block compact, when compacting
			if err := log.Compact(); errors.Is(err, ErrCompactRunning) != true {
				t.Errorf("actual err=%+v", err)
			}
		}(wg)
	}
	wg.Wait()

	lock <- struct{}{}

	<-done
}
