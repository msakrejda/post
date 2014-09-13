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

func TestSendSSLRequest(t *testing.T) {
	s, buf := newProtoStream()
	err := s.SendSSLRequest()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	expected := []byte{0x0, 0x0, 0x0, 0x8, 0x4, 0xd2, 0x16, 0x2f}
	written := buf.Bytes()
	if !bytes.Equal(expected, written) {
		t.Errorf("want %#v; got %#v", expected, written)
	}
}


func TestSendTerminate(t *testing.T) {
	s, buf := newProtoStream()
	err := s.SendTerminate()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	written := buf.Bytes()
	expected := []byte{'X', 0x0, 0x0, 0x0, 0x4}
	if !bytes.Equal(expected, written) {
		t.Errorf("want %#v; got %#v", expected, written)
	}
}

var authRecvTests = []struct{
	authType AuthResponseType
	payload []byte
	msgBytes []byte
}{
	{AuthenticationOk, []byte{}, []byte{0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0x0}},
	{AuthenticationKerberosV5, []byte{}, []byte{0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0x2}},
	{AuthenticationCleartextPassword, []byte{}, []byte{0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0x3}},
	{AuthenticationMD5Password, []byte{0x1, 0x2, 0x3, 0x4},
		[]byte{0x0, 0x0, 0x0, 0xC, 0x0, 0x0, 0x0, 0x5, 0x1, 0x2, 0x3, 0x4}},
	{AuthenticationSCMCredential, []byte{}, []byte{0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0x6}},
	{AuthenticationGSS, []byte{}, []byte{0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0x7}},
	{AuthenticationSSPI, []byte{}, []byte{0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0x9}},
	{AuthenticationGSSContinue, []byte{0x1, 0xFF, 0x0},
		[]byte{0x0, 0x0, 0x0, 0xB, 0x0, 0x0, 0x0, 0x8, 0x1, 0xFF, 0x0}},
}

func TestReceiveAuthResponse(t *testing.T) {
	for i, tt := range authRecvTests {
		s := newProtoStreamContent(tt.msgBytes)
		authResp, err := s.ReceiveAuthResponse()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if authResp.Subtype != tt.authType {
			t.Errorf("%d: want auth response subtype %v; got %v",
				i, tt.authType, authResp.Subtype)
		}
		if !bytes.Equal(authResp.Payload, tt.payload) {
			t.Errorf("%d: want %#v; got %#v", i, tt.payload, authResp.Payload)
		}
	}
}

func TestReceiveBackendKeyData(t *testing.T) {
	s := newProtoStreamContent([]byte{0x0, 0x0, 0x0, 0xC,
		0x0, 0x0, 0x1, 0x2,
		0x3, 0x4, 0x5, 0x6})
	keyData, err := s.ReceiveBackendKeyData()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	if keyData.Pid != 0x102 {
		t.Errorf("want pid 0x102; got %x", keyData.Pid)
	}
	if keyData.SecretKey != 0x03040506 {
		t.Errorf("want secret 0x03040506; got %x", keyData.SecretKey)
	}
}

func TestReceiveBind(t *testing.T) {
	s := newProtoStreamContent([]byte{0x0, 0x0, 0x0, 0x4})
	err := s.ReceiveBindComplete()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}
