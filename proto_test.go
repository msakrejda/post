package post

import (
	"bytes"
	"testing"
)

func newProtoStream(content []byte) *ProtoStream {
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
	s := newProtoStream([]byte{'x'})
	s.Expect('x')
}

func TestExpectUnexpected(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Error("want panic; got nil")
		}
	}()
	s := newProtoStream([]byte{'x'})
	s.Expect('y')
}

func TextNext(t *testing.T) {
	s := newProtoStream([]byte{'x'})
	val, err := s.Next()
	if err != nil {
		t.Errorf("want no err; got %v", err)
	}
	if val != 'x' {
		t.Errorf("want 'x'; got %v", val)
	}
}
