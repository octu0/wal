package codec

import (
	"io"

	"github.com/pkg/errors"
)

type Encoder struct {
	w io.Writer
}

// Encode is Writes Index and Payload
func (e *Encoder) Encode(id ID, data []byte) (int, error) {
	dataSize := uint64(len(data))
	headerWritten, err := encodeHeader(e.w, Header{id, dataSize})
	if err != nil {
		return 0, errors.WithStack(err)
	}
	payloadWritten, err := e.w.Write(data)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return headerWritten + payloadWritten, nil
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w}
}
