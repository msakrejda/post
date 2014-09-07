package post

import (
	"encoding/binary"
	"io"
)

type BufferedStreamer interface {
	io.ReadWriteCloser
	Flush() error
}

type Stream struct {
	str BufferedStreamer
	buf [4]byte
	buf1 []byte
	buf2 []byte
	buf4 []byte
}

var be = binary.BigEndian

func NewStream(inner BufferedStreamer) *Stream {
	var s = Stream{str: inner}
	s.buf1 = s.buf[0:1]
	s.buf2 = s.buf[0:2]
	s.buf4 = s.buf[0:4]
	return &s
}

func (s *Stream) WriteInt16(val int16) (n int, err error) {
	be.PutUint16(s.buf2, uint16(val))
	return s.str.Write(s.buf2)
}

func (s *Stream) WriteInt32(val int32) (n int, err error) {
	be.PutUint32(s.buf4, uint32(val))
	return s.str.Write(s.buf4)
}

func (s *Stream) WriteByte(val byte) (n int, err error) {
	s.buf1[0] = val
	return s.str.Write(s.buf1)
}

func (s *Stream) WriteCString(val string) (n int, err error) {
	n, err = s.str.Write([]byte(val))
	if err != nil {
		return n, err
	}
	s.buf1[0] = 0
	return s.str.Write(s.buf1)
}
