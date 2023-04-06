package wal

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

const (
	defaultIdFormat    string = "%016x-%016x"
	defaultIdFormatLen int    = 33 // 16 + 16 + 1
)

var (
	idGen = newIdGenerator(rand.NewSource(time.Now().UnixNano()))
)

type defaultIdGenerator struct {
	mutex    *sync.Mutex
	rnd      *rand.Rand
	lastTime int64
	lastRand int64
}

func (g *defaultIdGenerator) Next() (int64, int64) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	return g.nextIdLocked()
}

func (g *defaultIdGenerator) nextIdLocked() (int64, int64) {
	now := g.now()
	r := g.rand(now)

	g.lastTime = now
	g.lastRand = r

	return now, r
}

func (g *defaultIdGenerator) now() int64 {
	return time.Now().UTC().UnixNano()
}

func (g *defaultIdGenerator) rand(now int64) int64 {
	if g.lastTime < now {
		return g.rnd.Int63()
	}
	return g.lastRand + 1
}

func newIdGenerator(src rand.Source) *defaultIdGenerator {
	return &defaultIdGenerator{
		mutex:    new(sync.Mutex),
		rnd:      rand.New(src),
		lastTime: 0,
		lastRand: 0,
	}
}

const (
	FileIDByteSize int = 8 + 8
)

type FileID struct {
	Time int64
	Rand int64
}

func (f FileID) IsZero() bool {
	return f.Time == 0 && f.Rand == 0
}

func (f FileID) Equal(target FileID) bool {
	return target.Time == f.Time && target.Rand == f.Rand
}

func (f FileID) Newer(target FileID) bool {
	if f.Equal(target) {
		return false
	}
	if f.Time < target.Time {
		return true
	}
	if f.Time == target.Time {
		if f.Rand < target.Rand {
			return true
		}
	}
	return false
}

func (f FileID) String() string {
	return fmt.Sprintf(defaultIdFormat, f.Time, f.Rand)
}

func CreateFileID(t, r int64) FileID {
	return FileID{
		Time: t,
		Rand: r,
	}
}

func NextFileID() FileID {
	return CreateFileID(idGen.Next())
}

func ParseFileID(fileID string) (FileID, bool) {
	if len(fileID) != defaultIdFormatLen {
		return FileID{}, false
	}
	timeField := fileID[0:16]
	separator := fileID[16 : 16+1]
	randField := fileID[16+1:]
	if separator != "-" {
		return FileID{}, false
	}

	t, err := strconv.ParseInt(timeField, 16, 64)
	if err != nil {
		return FileID{}, false
	}
	r, err := strconv.ParseInt(randField, 16, 64)
	if err != nil {
		return FileID{}, false
	}
	return CreateFileID(t, r), true
}
