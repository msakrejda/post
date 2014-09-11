package post

import (
	"bytes"
	"testing"
)

func newProtoStream() (*ProtoStream, *bytes.Buffer) {
	b := FakeBufferedStreamer{}
	s := NewStream(&b)
	return &ProtoStream{str: s}, &b.Buffer
}

func newProtoStreamContent(content []byte) *ProtoStream {
	buf := bytes.NewBuffer(content)
	b := FakeBufferedStreamer{*buf}
	s := NewStream(&b)
	return &ProtoStream{str: s}
}

func TestExpectExpected(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			t.Errorf("want no panic; got %v", err)
		}
	}()
	s := newProtoStreamContent([]byte{'x'})
	err := s.Expect('x')
	if err != nil {
		t.Errorf("want nil error; got %v", err)
	}
}

func TextExpectError(t *testing.T) {
	s := newProtoStreamContent([]byte{})
	err := s.Expect('x')
	if err == nil {
		t.Error("want error; got nil")
	}
}

func TestExpectUnexpected(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Error("want panic; got nil")
		}
	}()
	s := newProtoStreamContent([]byte{'x'})
	s.Expect('y')
}

func TextNext(t *testing.T) {
	s := newProtoStreamContent([]byte{'x'})
	val, err := s.Next()
	if err != nil {
		t.Errorf("want no err; got %v", err)
	}
	if val != 'x' {
		t.Errorf("want 'x'; got %v", val)
	}
}

func TextNextError(t *testing.T) {
	s := newProtoStreamContent([]byte{})
	_, err := s.Next()
	if err == nil {
		t.Error("want err; got nil")
	}
}

var startupMsgTests = []struct{
	opts map[string]string
	msgBytes []byte
}{
	{ map[string]string{}, []byte{0x0, 0x0, 0x0, 0x9, 0x0, 0x3, 0x0, 0x0, 0x0} },
	{ map[string]string{"user": "bob"}, []byte{0x0, 0x0, 0x0, 0x12, 0x0, 0x3, 0x0, 0x0, 0x75, 0x73, 0x65, 0x72, 0x0, 0x62, 0x6f, 0x62, 0x0, 0x0} },
}

func TestSendStartupMessage(t *testing.T) {
	for i, tt := range startupMsgTests {
		s, buf := newProtoStream()
		err := s.SendStartupMessage(tt.opts)
		if err != nil {
			t.Errorf("want nil err; got %v", err)
		}
		written := buf.Bytes()
		if !bytes.Equal(tt.msgBytes, written) {
			t.Errorf("%d: want %#v; got %#v", i, tt.msgBytes, written)
		}
	}
}

