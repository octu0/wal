# `wal`

[![MIT License](https://img.shields.io/github/license/octu0/wal)](https://github.com/octu0/wal/blob/master/LICENSE)
[![GoDoc](https://godoc.org/github.com/octu0/wal?status.svg)](https://godoc.org/github.com/octu0/wal)
[![Go Report Card](https://goreportcard.com/badge/github.com/octu0/wal)](https://goreportcard.com/report/github.com/octu0/wal)
[![Releases](https://img.shields.io/github/v/release/octu0/wal)](https://github.com/octu0/wal/releases)

**simple/small** write ahead log.

## Installation

```
$ go get github.com/octu0/wal
```

## Example

```go
import "github.com/octu0/wal"

func main() {
	log, err := Open("/path/to/dir", wal.WithSync(true))
	if err != nil {
		panic(err)
	}
  defer log.Close()

	i1, err := log.Write([]byte("data1"))
	i2, err := log.Write([]byte("data2"))
	err := log.WriteAt(Index(100), []byte("data3"))

	data1, _ := log.Read(i1)
	println(string(data1)) // => "data1"

	data3, _ := log.Read(Index(100))
	println(string(data3)) // => "data3"

	// delete logs on memory
	if err := log.Delete(i1, i2); err != nil {
		panic(err)
	}

	// compaction of deleted logs to free disk space
	if err := log.Compact(); err != nil {
		panic(err)
	}
}
```

# License

MIT, see LICENSE file for details.
