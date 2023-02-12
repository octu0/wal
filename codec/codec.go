package codec

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/pkg/errors"
)

//
// DataFormat
// +-----------------------------------+----------+---------+-------------+
// | Header                            | Payload  | Header  | Payload ... |
// +----------------+------------------+----------+---------+-------------+
// |  ID(8 byte)    | DataSize(8 byte) | .....    | 16 byte | ...         |
// |----------------+------------------+----------+---------+-------------+
// \---------------- Encode() --------------------/
//
//

const (
	headerIDSize   int = 8 // uint64
	headerDataSize int = 8 // uint64

	headerSize int = headerIDSize + headerDataSize
)

type ID uint64

type Header struct {
	ID       ID
	DataSize uint64
}

var (
	codecSizePool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, headerSize)
		},
	}
)

func HeaderSize() int {
	return headerSize
}

func encodeHeader(w io.Writer, h Header) (int, error) {
	buf := codecSizePool.Get().([]byte)
	defer codecSizePool.Put(buf)

	binary.BigEndian.PutUint64(buf[0:headerIDSize], uint64(h.ID))
	binary.BigEndian.PutUint64(buf[headerIDSize:headerSize], h.DataSize)

	size, err := w.Write(buf[0:headerSize])
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return size, nil
}

func decodeHeader(r io.Reader) (Header, error) {
	buf := codecSizePool.Get().([]byte)
	defer codecSizePool.Put(buf)

	if _, err := r.Read(buf[0:headerSize]); err != nil {
		return Header{}, errors.WithStack(err)
	}

	id := binary.BigEndian.Uint64(buf[0:headerIDSize])
	dataSize := binary.BigEndian.Uint64(buf[headerIDSize:headerSize])
	return Header{ID(id), dataSize}, nil
}
