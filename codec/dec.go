package codec

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

var (
	discardBuffer = &sync.Pool{
		New: func() interface{} {
			return make([]byte, 32*1024)
		},
	}
)

type Decoder struct {
	r io.Reader
}

// DecodeHeader is Read Header only, discard Payload
func (d *Decoder) DecodeHeader() (Header, error) {
	h, err := decodeHeader(d.r)
	if err != nil {
		return Header{}, errors.WithStack(err)
	}
	buf := discardBuffer.Get().([]byte)
	defer discardBuffer.Put(buf)

	lim := io.LimitReader(d.r, int64(h.DataSize))
	if _, err := io.CopyBuffer(io.Discard, lim, buf); err != nil {
		return Header{}, errors.WithStack(err)
	}
	return h, nil
}

// Decode is Read Header and Payload
func (d *Decoder) Decode() (Header, []byte, error) {
	h, err := decodeHeader(d.r)
	if err != nil {
		return Header{}, []byte{}, errors.WithStack(err)
	}
	data := make([]byte, h.DataSize)
	if _, err := d.r.Read(data); err != nil {
		return Header{}, []byte{}, errors.WithStack(err)
	}
	return h, data, nil
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r}
}
