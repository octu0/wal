package codec

import (
	"bytes"
	"testing"
)

func TestHeader(t *testing.T) {
	t.Run("encode", func(tt *testing.T) {
		buf1 := bytes.NewBuffer(nil)
		size1, err := encodeHeader(buf1, Header{ID(0), 0})
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if size1 != headerSize {
			tt.Errorf("corrupt written size: %d", size1)
		}
		b1 := buf1.Bytes()
		if len(b1) != headerSize {
			tt.Errorf("corrupt header size: %d", len(b1))
		}

		buf2 := bytes.NewBuffer(make([]byte, 0, 1024))
		size2, err := encodeHeader(buf2, Header{ID(100), 123})
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if size2 != headerSize {
			tt.Errorf("corrupt written size: %d", size2)
		}
		b2 := buf2.Bytes()
		if len(b2) != headerSize {
			tt.Errorf("corrupt header size: %d", len(b2))
		}
	})
	t.Run("decode", func(tt *testing.T) {
		buf1 := bytes.NewBuffer(nil)
		size1, err := encodeHeader(buf1, Header{ID(123), 456})
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if size1 != headerSize {
			tt.Errorf("corrupt written size: %d", size1)
		}
		h1, err := decodeHeader(bytes.NewReader(buf1.Bytes()))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if h1.ID != ID(123) {
			tt.Errorf("ID=123 %d", h1.ID)
		}
		if h1.DataSize != 456 {
			tt.Errorf("datasize=456 %d", h1.DataSize)
		}

		buf2 := bytes.NewBuffer(make([]byte, 0, 1024))
		size2, err := encodeHeader(buf2, Header{ID(123456), 123456789})
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if size2 != headerSize {
			tt.Errorf("corrupt written size: %d", size2)
		}
		h2, err := decodeHeader(bytes.NewReader(buf2.Bytes()))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if h2.ID != ID(123456) {
			tt.Errorf("ID=123456 %d", h2.ID)
		}
		if h2.DataSize != 123456789 {
			tt.Errorf("datasize=123456789 %d", h2.DataSize)
		}

		buf3 := bytes.NewBuffer(nil)
		size3, err := encodeHeader(buf3, Header{ID(0), 0})
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if size3 != headerSize {
			tt.Errorf("corrupt written size: %d", size3)
		}
		h3, err := decodeHeader(bytes.NewReader(buf3.Bytes()))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if h3.ID != ID(0) {
			tt.Errorf("ID=0 %d", h3.ID)
		}
		if h3.DataSize != 0 {
			tt.Errorf("datasize=0 %d", h3.DataSize)
		}
	})
}
