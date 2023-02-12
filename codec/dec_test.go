package codec

import (
	"bytes"
	"fmt"
	"testing"
)

func TestDecode(t *testing.T) {
	t.Run("enc/dec", func(tt *testing.T) {
		buf := bytes.NewBuffer(nil)

		enc := NewEncoder(buf)
		if _, err := enc.Encode(ID(100), []byte("test1")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if _, err := enc.Encode(ID(101), []byte("testtest2")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}

		dec := NewDecoder(bytes.NewReader(buf.Bytes()))
		h1, data1, err := dec.Decode()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if h1.ID != ID(100) {
			tt.Errorf("ID=100 %d", h1.ID)
		}
		if h1.DataSize != uint64(len([]byte("test1"))) {
			tt.Errorf("corrupt size: %d", h1.DataSize)
		}
		if bytes.Equal(data1, []byte("test1")) != true {
			tt.Errorf("test1 %s", data1)
		}

		h2, data2, err := dec.Decode()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if h2.ID != ID(101) {
			tt.Errorf("ID=101 %d", h2.ID)
		}
		if h2.DataSize != uint64(len([]byte("testtest2"))) {
			tt.Errorf("corrupt size: %d", h2.DataSize)
		}
		if bytes.Equal(data2, []byte("testtest2")) != true {
			tt.Errorf("payload=testtest2 %s", data2)
		}
	})
	t.Run("decodeHeader", func(tt *testing.T) {
		buf := bytes.NewBuffer(nil)
		enc := NewEncoder(buf)
		if _, err := enc.Encode(ID(0), []byte{}); err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if _, err := enc.Encode(ID(123), []byte("test123")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if _, err := enc.Encode(ID(1), []byte("test1")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if _, err := enc.Encode(ID(456), []byte("t456456456")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if _, err := enc.Encode(ID(123456), []byte("123456")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}

		dec := NewDecoder(bytes.NewReader(buf.Bytes()))
		h1, err := dec.DecodeHeader()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		h2, err := dec.DecodeHeader()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		h3, err := dec.DecodeHeader()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		h4, err := dec.DecodeHeader()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		h5, err := dec.DecodeHeader()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}

		if h1.ID != ID(0) {
			tt.Errorf("actual: %d", h1.ID)
		}
		if h2.ID != ID(123) {
			tt.Errorf("actual: %d", h2.ID)
		}
		if h3.ID != ID(1) {
			tt.Errorf("actual: %d", h3.ID)
		}
		if h4.ID != ID(456) {
			tt.Errorf("actual: %d", h4.ID)
		}
		if h5.ID != ID(123456) {
			tt.Errorf("actual: %d", h5.ID)
		}

		if h1.DataSize != uint64(len([]byte{})) {
			tt.Errorf("actual: %d", h1.DataSize)
		}
		if h2.DataSize != uint64(len([]byte("test123"))) {
			tt.Errorf("actual: %d", h2.DataSize)
		}
		if h3.DataSize != uint64(len([]byte("test1"))) {
			tt.Errorf("actual: %d", h3.DataSize)
		}
		if h4.DataSize != uint64(len([]byte("t456456456"))) {
			tt.Errorf("actual: %d", h4.DataSize)
		}
		if h5.DataSize != uint64(len([]byte("123456"))) {
			tt.Errorf("actual: %d", h5.DataSize)
		}
	})
	t.Run("decodeHeader+decode", func(tt *testing.T) {
		buf := bytes.NewBuffer(nil)
		enc := NewEncoder(buf)
		if _, err := enc.Encode(ID(0), []byte{}); err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if _, err := enc.Encode(ID(123), []byte("test123")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if _, err := enc.Encode(ID(1), []byte("test1")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if _, err := enc.Encode(ID(456), []byte("t456456456")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if _, err := enc.Encode(ID(123456), []byte("123456")); err != nil {
			tt.Fatalf("no error: %+v", err)
		}

		dec := NewDecoder(bytes.NewReader(buf.Bytes()))
		h1, err := dec.DecodeHeader()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		h2, data2, err := dec.Decode()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		h3, data3, err := dec.Decode()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		h4, err := dec.DecodeHeader()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		h5, data5, err := dec.Decode()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}

		if h1.ID != ID(0) {
			tt.Errorf("actual: %d", h1.ID)
		}
		if h2.ID != ID(123) {
			tt.Errorf("actual: %d", h2.ID)
		}
		if h3.ID != ID(1) {
			tt.Errorf("actual: %d", h3.ID)
		}
		if h4.ID != ID(456) {
			tt.Errorf("actual: %d", h4.ID)
		}
		if h5.ID != ID(123456) {
			tt.Errorf("actual: %d", h5.ID)
		}

		if h1.DataSize != uint64(len([]byte{})) {
			tt.Errorf("actual: %d", h1.DataSize)
		}
		if h2.DataSize != uint64(len([]byte("test123"))) {
			tt.Errorf("actual: %d", h2.DataSize)
		}
		if h3.DataSize != uint64(len([]byte("test1"))) {
			tt.Errorf("actual: %d", h3.DataSize)
		}
		if h4.DataSize != uint64(len([]byte("t456456456"))) {
			tt.Errorf("actual: %d", h4.DataSize)
		}
		if h5.DataSize != uint64(len([]byte("123456"))) {
			tt.Errorf("actual: %d", h5.DataSize)
		}

		if bytes.Equal(data2, []byte("test123")) != true {
			tt.Errorf("actual %s", data2)
		}
		if bytes.Equal(data3, []byte("test1")) != true {
			tt.Errorf("actual %s", data3)
		}
		if bytes.Equal(data5, []byte("123456")) != true {
			tt.Errorf("actual %s", data5)
		}
	})
	t.Run("enc100/dec50", func(tt *testing.T) {
		buf := bytes.NewBuffer(nil)
		enc := NewEncoder(buf)
		for i := ID(0); i < ID(100); i += 1 {
			data := fmt.Sprintf("test%05d", uint64(i))
			if _, err := enc.Encode(i, []byte(data)); err != nil {
				tt.Fatalf("no error: %+v", err)
			}
		}
		dec := NewDecoder(bytes.NewReader(buf.Bytes()))
		for i := ID(0); i < ID(50); i += 1 {
			expect := []byte(fmt.Sprintf("test%05d", uint64(i)))
			h, data, err := dec.Decode()
			if err != nil {
				tt.Fatalf("no error: %+v", err)
			}
			if h.ID != i {
				tt.Errorf("expect=%d actual=%d", i, h.ID)
			}
			if h.DataSize != uint64(len(expect)) {
				tt.Errorf("expect-size=%d actual-size=%d", len(expect), h.DataSize)
			}
			if bytes.Equal(expect, data) != true {
				tt.Errorf("expect-data=%s actual-data=%s", expect, data)
			}
		}
	})
}
