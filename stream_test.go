package post

import (
	"bytes"
	"testing"
	"net"
	"time"
)

type FakeConn struct {
	*bytes.Buffer
}

func newFakeConn() *FakeConn {
	var buf bytes.Buffer
	return &FakeConn{&buf}
}

func newFakeConnBytes(data []byte) *FakeConn {
	buf := bytes.NewBuffer(data)
	return &FakeConn{buf}
}

func (f *FakeConn) LocalAddr() net.Addr {
	return nil
}

func (f *FakeConn) RemoteAddr() net.Addr {
	return nil
}

func (f *FakeConn) Close() error {
	return nil
}

func (f *FakeConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (f *FakeConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (f *FakeConn) SetDeadline(t time.Time) error {
	return nil
}

var byteTests = []struct {
	value byte
	bytes []byte
}{
	{0x00, []byte{0x00}},
	{0x01, []byte{0x01}},
	{0xFF, []byte{0xFF}},
}

var uint16Tests = []struct {
	value int16
	bytes []byte
}{
	{0x00, []byte{0x00, 0x00}},
	{0x01, []byte{0x00, 0x01}},
	{0xFF, []byte{0x00, 0xFF}},
	{0x0100, []byte{0x01, 0x00}},
	{0x7FFF, []byte{0x7F, 0xFF}},
	{-0x01, []byte{0xFF, 0xFF}},
}

var uint32Tests = []struct {
	value int32
	bytes []byte
}{
	{0x00, []byte{0x00, 0x00, 0x00, 0x00}},
	{0x01, []byte{0x00, 0x00, 0x00, 0x01}},
	{0xFF, []byte{0x00, 0x00, 0x00, 0xFF}},
	{0x01000000, []byte{0x01, 0x00, 0x00, 0x00}},
	{0x7FFFFFFF, []byte{0x7F, 0xFF, 0xFF, 0xFF}},
	{-0x01, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
}

var cStringTests = []struct {
	value string
	bytes []byte
}{
	{"", []byte{0x00}},
	{"x", []byte{0x78, 0x00}},
	{"hello", []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0}},
	{"déjà vu", []byte{0x64, 0xc3, 0xa9, 0x6a, 0xc3, 0xa0, 0x20, 0x76, 0x75, 0x0}},
}

var bytesTests = []struct {
	value []byte
	bytes []byte
}{
	{[]byte{}, []byte{}},
	{[]byte{0x1}, []byte{0x1}},
	{[]byte{0x1, 0x2}, []byte{0x1, 0x2}},
}

func TestWriteByte(t *testing.T) {
	for i, tt := range byteTests {
		b := newFakeConn()
		s := NewStream(b)
		n, err := s.WriteByte(tt.value)
		if n != 1 {
			t.Errorf("%d: want 1 byte written; got %d", i, n)
		}
		if err != nil {
			t.Errorf("%d: want nil err on write; got %#v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err on flush; got %#v", i, err)
		}
		result := b.Bytes()
		if !bytes.Equal(tt.bytes, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.bytes, result)
		}
	}
}

func TestWriteInt16(t *testing.T) {
	for i, tt := range uint16Tests {
		b := newFakeConn()
		s := NewStream(b)
		n, err := s.WriteInt16(tt.value)
		if n != 2 {
			t.Errorf("%d: want 2 bytes written; got %d", i, n)
		}
		if err != nil {
			t.Errorf("%d: want nil err on write; got %#v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err on flush; got %#v", i, err)
		}
		result := b.Bytes()
		if !bytes.Equal(tt.bytes, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.bytes, result)
		}
	}
}

func TestWriteInt32(t *testing.T) {
	for i, tt := range uint32Tests {
		b := newFakeConn()
		s := NewStream(b)
		n, err := s.WriteInt32(tt.value)
		if n != 4 {
			t.Errorf("%d: want 4 bytes written; got %d", i, n)
		}
		if err != nil {
			t.Errorf("%d: want nil err on write; got %#v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err on flush; got %#v", i, err)
		}
		result := b.Bytes()
		if !bytes.Equal(tt.bytes, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.bytes, result)
		}
	}
}

func TestWriteCString(t *testing.T) {
	for i, tt := range cStringTests {
		b := newFakeConn()
		s := NewStream(b)
		n, err := s.WriteCString(tt.value)
		if expected := len(tt.value) + 1; n != expected {
			t.Errorf("%d: want %d bytes written; got %d", i, expected, n)
		}
		if err != nil {
			t.Errorf("%d: want nil err on write; got %#v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err on flush; got %#v", i, err)
		}
		result := b.Bytes()
		if !bytes.Equal(tt.bytes, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.bytes, result)
		}
	}
}

func TestWrite(t *testing.T) {
	for i, tt := range bytesTests {
		b := newFakeConn()
		s := NewStream(b)
		n, err := s.Write(tt.value)
		if expected := len(tt.value); n != expected {
			t.Errorf("%d: want %d bytes written; got %d", i, expected, n)
		}
		if err != nil {
			t.Errorf("%d: want nil err on write; got %#v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err on flush; got %#v", i, err)
		}
		result := b.Bytes()
		if !bytes.Equal(tt.bytes, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.bytes, result)
		}
	}
}

func TestReadByte(t *testing.T) {
	for i, tt := range byteTests {
		s := NewStream(newFakeConnBytes(tt.bytes))
		result, err := s.ReadByte()
		if err != nil {
			t.Errorf("%d: want nil error; got %v", err)
		}
		if result != tt.value {
			t.Errorf("%d: want %#v; got %#v", i, tt.value, result)
		}
	}
}

func TestReadInt16(t *testing.T) {
	for i, tt := range uint16Tests {
		s := NewStream(newFakeConnBytes(tt.bytes))
		result, err := s.ReadInt16()
		if err != nil {
			t.Errorf("%d: want nil error; got %v", err)
		}
		if result != tt.value {
			t.Errorf("%d: want %#v; got %#v", i, tt.value, result)
		}
	}
}

func TestReadInt32(t *testing.T) {
	for i, tt := range uint32Tests {
		s := NewStream(newFakeConnBytes(tt.bytes))
		result, err := s.ReadInt32()
		if err != nil {
			t.Errorf("%d: want nil error; got %v", err)
		}
		if result != tt.value {
			t.Errorf("%d: want %#v; got %#v", i, tt.value, result)
		}
	}
}

func TestReadCString(t *testing.T) {
	for i, tt := range cStringTests {
		s := NewStream(newFakeConnBytes(tt.bytes))
		result, err := s.ReadCString()
		if err != nil {
			t.Errorf("%d: want nil error; got %v", err)
		}
		if result != tt.value {
			t.Errorf("%d: want %#v; got %#v", i, tt.value, result)
		}
	}
}

func TestRead(t *testing.T) {
	for i, tt := range bytesTests {
		s := NewStream(newFakeConnBytes(tt.bytes))
		result := make([]byte, len(tt.bytes))
		n, err := s.Read(result)
		if err != nil {
			t.Errorf("%d: want nil error; got %v", err)
		}
		if n != len(tt.bytes) {
			t.Errorf("%d: want %#v bytes read; got %#v", i, len(tt.bytes), n)
		}
		if !bytes.Equal(tt.bytes, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.bytes, result)
		}
	}
}
