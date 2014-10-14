package post

import (
	"bytes"
	"encoding/binary"
	"net"
)

type Stream struct {
	str  net.Conn
	buf  [4]byte
	buf1 []byte
	buf2 []byte
	buf4 []byte
}

var be = binary.BigEndian

func NewStream(conn net.Conn) *Stream {
	var s = Stream{str: conn}
	s.buf1 = s.buf[0:1]
	s.buf2 = s.buf[0:2]
	s.buf4 = s.buf[0:4]
	return &s
}

func (s *Stream) WriteByte(val byte) (n int, err error) {
	s.buf1[0] = val
	return s.str.Write(s.buf1)
}

func (s *Stream) WriteInt16(val int16) (n int, err error) {
	be.PutUint16(s.buf2, uint16(val))
	return s.str.Write(s.buf2)
}

func (s *Stream) WriteInt32(val int32) (n int, err error) {
	be.PutUint32(s.buf4, uint32(val))
	return s.str.Write(s.buf4)
}

func (s *Stream) WriteCString(val string) (n int, err error) {
	n, err = s.str.Write([]byte(val))
	if err != nil {
		return n, err
	}
	s.buf1[0] = 0
	return s.str.Write(s.buf1)
}

func (s *Stream) Write(val []byte) (n int, err error) {
	return s.str.Write(val)
}

func (s *Stream) ReadByte() (b byte, err error) {
	_, err = s.str.Read(s.buf1)
	if err != nil {
		return 0, err
	}
	return s.buf1[0], nil
}

func (s *Stream) ReadInt16() (val int16, err error) {
	_, err = s.str.Read(s.buf2)
	if err != nil {
		return 0, err
	}
	return int16(be.Uint16(s.buf2)), nil
}

func (s *Stream) ReadInt32() (val int32, err error) {
	_, err = s.str.Read(s.buf4)
	if err != nil {
		return 0, err
	}
	return int32(be.Uint32(s.buf4)), nil
}

func (s *Stream) ReadCString() (val string, err error) {
	var buf bytes.Buffer
	for {
		// TODO: read into larger temporary buffer and check
		// buffer contents rather than doing individual reads?
		if n, err := s.str.Read(s.buf1); err != nil {
			return "", err
		} else if n < 1 {
			// N.B.: io.Reader does not guarantee that n
			// is greater than zero just because err is
			// nil
			continue
		}

		switch s.buf1[0] {
		case '\000':
			return string(buf.Bytes()), nil
		default:
			buf.Write(s.buf1)
		}
	}
}

func (s *Stream) Read(buf []byte) (n int, err error) {
	return s.str.Read(buf)
}
