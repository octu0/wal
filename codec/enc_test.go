package codec

import (
	"bytes"
	"testing"
)

func TestEncode(t *testing.T) {
	t.Run("checksize", func(tt *testing.T) {
		data1 := []byte("test1")
		data2 := []byte("testtest2")

		buf := bytes.NewBuffer(nil)

		enc := NewEncoder(buf)
		size1, err := enc.Encode(ID(0), data1)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if size1 != len(data1)+headerSize {
			tt.Errorf("corrupt written size %d", size1)
		}
		b1 := buf.Bytes()
		if (headerSize + len(data1)) != len(b1) {
			tt.Errorf("corrupt binary size: %d(initial)", len(b1))
		}

		size2, err := enc.Encode(ID(1), data2)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if size2 != len(data2)+headerSize {
			tt.Errorf("corrupt written size %d", size2)
		}
		b2 := buf.Bytes()
		if ((headerSize * 2) + (len(data1) + len(data2))) != len(b2) {
			tt.Errorf("corrupt size: %d(append)", len(b2))
		}
	})
	t.Run("write/100", func(tt *testing.T) {
		buf := bytes.NewBuffer(nil)
		enc := NewEncoder(buf)
		for i := ID(0); i < ID(100); i += 1 {
			size, err := enc.Encode(i, []byte{})
			if err != nil {
				tt.Fatalf("no error: %+v", err)
			}
			if size != headerSize {
				tt.Errorf("corrupt written size: %d", size)
			}
		}
		if (headerSize * 100) != buf.Len() {
			tt.Errorf("corrupt size: %d(large)", buf.Len())
		}
	})
	t.Run("write/large", func(tt *testing.T) {
		data := bytes.Repeat([]byte("@"), 1000)
		buf := bytes.NewBuffer(nil)
		enc := NewEncoder(buf)
		size, err := enc.Encode(ID(123456789), data)
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if size != (headerSize + len(data)) {
			tt.Errorf("corrupt written size: %d", size)
		}
		if (headerSize + len(data)) != buf.Len() {
			tt.Errorf("corrupt size: %d(large)", buf.Len())
		}
	})
}
