package post

import (
	"bytes"
	"testing"
)

type FakeBufferedStreamer struct {
	bytes.Buffer
}

func (f *FakeBufferedStreamer) Flush() error {
	return nil
}

func (f *FakeBufferedStreamer) Close() error {
	return nil
}

var uint16Tests = []struct{
	value int16
	expected []byte
}{
	{ 0x00, []byte{0x00, 0x00} },
	{ 0x01, []byte{0x00, 0x01} },
	{ 0xFF, []byte{0x00, 0xFF} },
	{ 0x0100, []byte{0x01, 0x00} },
	{ 0x7FFF, []byte{0x7F, 0xFF} },
	{ -0x01, []byte{0xFF, 0xFF} },
}

func TestWriteInt16(t *testing.T) {
	for i, tt := range uint16Tests {
		var b FakeBufferedStreamer
		s := NewStream(&b)
		s.WriteInt16(tt.value)
		// N.B.: due to the implementation of
		// FakeBufferedStream, we do *not* need to call Flush
		// before reading the bytes here
		result := b.Bytes()
		if !bytes.Equal(tt.expected, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.expected, result)
		}
	}
}

var uint32Tests = []struct{
	value int32
	expected []byte
}{
	{ 0x00, []byte{0x00, 0x00, 0x00, 0x00} },
	{ 0x01, []byte{0x00, 0x00, 0x00, 0x01} },
	{ 0xFF, []byte{0x00, 0x00, 0x00, 0xFF} },
	{ 0x01000000, []byte{0x01, 0x00, 0x00, 0x00} },
	{ 0x7FFFFFFF, []byte{0x7F, 0xFF, 0xFF, 0xFF} },
	{ -0x01, []byte{0xFF, 0xFF, 0xFF, 0xFF} },
}

func TestWriteInt32(t *testing.T) {
	for i, tt := range uint32Tests {
		var b FakeBufferedStreamer
		s := NewStream(&b)
		s.WriteInt32(tt.value)
		result := b.Bytes()
		if !bytes.Equal(tt.expected, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.expected, result)
		}
	}
}

var byteTests = []struct{
	value byte
	expected []byte
}{
	{ 0x00, []byte{0x00} },
	{ 0x01, []byte{0x01} },
	{ 0xFF, []byte{0xFF} },
}

func TestWriteByte(t *testing.T) {
	for i, tt := range byteTests {
		var b FakeBufferedStreamer
		s := NewStream(&b)
		s.WriteByte(tt.value)
		result := b.Bytes()
		if !bytes.Equal(tt.expected, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.expected, result)
		}
	}
}

var cStringTests = []struct{
	value string
	expected []byte
}{
	{ "", []byte{0x00} },
	{ "x", []byte{0x78, 0x00} },
	{ "hello", []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0} },
	{ "déjà vu", []byte{0x64, 0xc3, 0xa9, 0x6a, 0xc3, 0xa0, 0x20, 0x76, 0x75, 0x0} },
}

func TestWriteCString(t *testing.T) {
	for i, tt := range cStringTests {
		var b FakeBufferedStreamer
		s := NewStream(&b)
		s.WriteCString(tt.value)
		result := b.Bytes()
		if !bytes.Equal(tt.expected, result) {
			t.Errorf("%d: want %#v; got %#v", i, tt.expected, result)
		}
	}
}

