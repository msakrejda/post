package post

import (
	"bufio"
	"encoding/binary"
	"net"
)

type Stream struct {
	str  *bufio.ReadWriter
	conn net.Conn
	buf  [4]byte
	buf1 []byte
	buf2 []byte
	buf4 []byte
}

var be = binary.BigEndian

func NewStream(conn net.Conn) *Stream {
	var buf = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	var s = Stream{conn: conn, str: buf}
	s.buf2 = s.buf[0:1]
	s.buf2 = s.buf[0:2]
	s.buf4 = s.buf[0:4]
	return &s
}

func (s *Stream) WriteByte(val byte) (n int, err error) {
	return 1, s.str.WriteByte(val)
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
	n, err = s.str.WriteString(val)
	if err != nil {
		return n, err
	}
	return n + 1 /* for the zero byte */, s.str.WriteByte(0)
}

func (s *Stream) Write(val []byte) (n int, err error) {
	return s.str.Write(val)
}

func (s *Stream) Flush() (err error) {
	return s.str.Flush()
}

func (s *Stream) ReadByte() (b byte, err error) {
	return s.str.ReadByte()
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
	str, err := s.str.ReadString(0)
	return str[:len(str)-1], err
}

func (s *Stream) Read(buf []byte) (n int, err error) {
	return s.str.Read(buf)
}

