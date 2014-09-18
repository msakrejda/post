package post

import (
	"bytes"
	"io"
	"testing"
)

func compareBytes(t *testing.T, expected, actual []byte) {
	if !bytes.Equal(expected, actual) {
		t.Errorf("want\n\t%#v;\ngot\n\t%#v", expected, actual)
	}
}

func compareBytesN(n int, t *testing.T, expected, actual []byte) {
	if !bytes.Equal(expected, actual) {
		t.Errorf("%d: want\n\t%#v;\ngot\n\t%#v", n, expected, actual)
	}
}


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

func TestExpectError(t *testing.T) {
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
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

func TestSendSSLRequest(t *testing.T) {
	s, buf := newProtoStream()
	err := s.SendSSLRequest()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	expected := []byte{0x0, 0x0, 0x0, 0x8, 0x4, 0xd2, 0x16, 0x2f}
	compareBytes(t, expected, buf.Bytes())
}


func TestSendTerminate(t *testing.T) {
	s, buf := newProtoStream()
	err := s.SendTerminate()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	expected := []byte{'X', 0x0, 0x0, 0x0, 0x4}
	compareBytes(t, expected, buf.Bytes())
}

var bindTests = []struct{
	portal string
	statement string
	formats []int16
	params [][]byte
	resultFormats []int16
	msgBytes []byte
}{
	{"", "", []int16{}, [][]byte{}, []int16{},
		[]byte{'B',
			0x0, 0x0, 0x0, 0xc,
			0x0, // portal
			0x0, // statement
			0x0, 0x0, // num formats
			0x0, 0x0, // num params
			0x0, 0x0, // num result formats
		}, // the simplest Bind
	},
	{"foo", "bar", []int16{}, [][]byte{}, []int16{},
		[]byte{'B',
			0x0, 0x0, 0x0, 0x12,
			'f', 'o', 'o', 0x0, // portal
			'b', 'a', 'r', 0x0, // statement
			0x0, 0x0, // num formats
			0x0, 0x0, // num params
			0x0, 0x0, // num result formats
		},
	},
	{"", "", []int16{0x1, 0x0}, [][]byte{[]byte{0x2,0x3},[]byte{0x4,0x5}}, []int16{0x0, 0x1},
		[]byte{'B',
			0x0, 0x0, 0x0, 0x20,
			0x0, // portal
			0x0, // statement
			0x0, 0x2, // num formats
			0x0, 0x1, // format 1
			0x0, 0x0, // format 2
			0x0, 0x2, // num params
			0x0, 0x0, 0x0, 0x2, // param 1 length
			0x2, 0x3, // param 1 value
			0x0, 0x0, 0x0, 0x2, // param 2 length
			0x4, 0x5, // param 1 value
			0x0, 0x2, // num result formats
			0x0, 0x0, // format 1
			0x0, 0x1, // format 2
		}, // the simplest Bind
	},
}

func TestSendBind(t *testing.T) {
	for i, tt := range bindTests {
		s, buf := newProtoStream()
		err := s.SendBind(tt.portal, tt.statement,
			tt.formats, tt.params, tt.resultFormats)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var cancelTests = []struct{
	pid int32
	secretKey int32
	msgBytes []byte
}{
	{0x1, 0x2, []byte{
		0x0, 0x0, 0x0, 0x10,   // length
		0x4, 0xd2, 0x16, 0x2e, // CancelRequest code
		0x0, 0x0, 0x0, 0x1,    // pid
		0x0, 0x0, 0x0, 0x2,    // secret key
	        },
	},
	{0xFFFF, 0x77777777, []byte{
		0x0, 0x0, 0x0, 0x10,     // length
		0x4, 0xd2, 0x16, 0x2e,   // CancelRequest code
		0x0, 0x0, 0xFF, 0xFF,    // pid
		0x77, 0x77, 0x77, 0x77,  // secret key
	        },
	},
}

func TestSendCancelRequest(t *testing.T) {
	for i, tt := range cancelTests {
		s, buf := newProtoStream()
		err := s.SendCancelRequest(tt.pid, tt.secretKey)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var closeTests = []struct{
	kind CloseType
	target string
	msgBytes []byte
}{
	{'C', "", []byte{0x0, 0x0, 0x0, 0x6, 'C', 0x0}},
	{'C', "hello", []byte{0x0, 0x0, 0x0, 0xb, 'C', 'h', 'e', 'l', 'l', 'o', 0x0}},
	{'P', "", []byte{0x0, 0x0, 0x0, 0x6, 'P', 0x0}},
	{'P', "yo", []byte{0x0, 0x0, 0x0, 0x8, 'P', 'y', 'o', 0x0}},
}

func TestSendClose(t *testing.T) {
	for i, tt := range closeTests {
		s, buf := newProtoStream()
		err := s.SendClose(tt.kind, tt.target)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var feCopyDataTests = []struct{
	data []byte
	msgBytes []byte
}{
	{[]byte{},[]byte{'d',0x0,0x0,0x0,0x4}},
	{[]byte{'x'},[]byte{'d',0x0,0x0,0x0,0x5,'x'}},
	{[]byte{'y','o'},[]byte{'d',0x0,0x0,0x0,0x6,'y','o'}},
}

func TestSendCopyData(t *testing.T) {
	for i, tt := range feCopyDataTests {
		s, buf := newProtoStream()
		err := s.SendCopyData(tt.data)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

func TestSendCopyDone(t *testing.T) {
	s, buf := newProtoStream()
	err := s.SendCopyDone()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	expected := []byte{'c', 0x0, 0x0, 0x0, 0x4}
	compareBytes(t, expected, buf.Bytes())
}

var copyFailTests = []struct{
	reason string
	msgBytes []byte
}{
	{"", []byte{'f',0x0,0x0,0x0,0x5,0x0}},
	{"bad copy", []byte{'f',0x0,0x0,0x0,0xd,'b','a','d',' ','c','o','p','y',0x0}},
}

func TestSendCopyFail(t *testing.T) {
	for i, tt := range copyFailTests {
		s, buf := newProtoStream()
		err := s.SendCopyFail(tt.reason)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
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
		compareBytesN(i, t, tt.payload, authResp.Payload)
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

func TestReceiveBindComplete(t *testing.T) {
	s := newProtoStreamContent([]byte{0x0, 0x0, 0x0, 0x4})
	err := s.ReceiveBindComplete()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

func TestReceiveCloseComplete(t *testing.T) {
	s := newProtoStreamContent([]byte{0x0, 0x0, 0x0, 0x4})
	err := s.ReceiveCloseComplete()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

var commandCompleteTests = []struct{
	tag string
	msgBytes []byte
}{
	{"INSERT 1 0", []byte{0x0,0x0,0x0,0xF,'I','N','S','E','R','T',' ','1',' ', '0', 0x0}},
	{"DELETE 42", []byte{0x0,0x0,0x0,0xE,'D','E','L','E','T','E',' ','4','2', 0x0}},
	{"UPDATE 3", []byte{0x0,0x0,0x0,0xD,'U','P','D','A','T','E',' ','3', 0x0}},
	{"SELECT 1", []byte{0x0,0x0,0x0,0xD,'S','E','L','E','C','T',' ','1', 0x0}},
	{"MOVE 2", []byte{0x0,0x0,0x0,0xB,'M','O','V','E',' ','2', 0x0}},
	{"FETCH 4", []byte{0x0,0x0,0x0,0xC,'F','E','T','C','H',' ','4', 0x0}},
	{"COPY 7", []byte{0x0,0x0,0x0,0xB,'C','O','P','Y',' ','7', 0x0}},
}

func TestReceiveCommandComplete(t *testing.T) {
	for i, tt := range commandCompleteTests {
		s := newProtoStreamContent(tt.msgBytes)
		cmd, err := s.ReceiveCommandComplete()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if tt.tag != cmd {
			t.Errorf("%d: want tag %v; got %v", i, tt.tag, cmd)
		}
	}
}

var beCopyDataTests = []struct{
	data []byte
	msgBytes []byte
}{
	{[]byte{},[]byte{0x0,0x0,0x0,0x4}},
	{[]byte{'x'},[]byte{0x0,0x0,0x0,0x5,'x'}},
	{[]byte{'y','o'},[]byte{0x0,0x0,0x0,0x6,'y','o'}},
}

func TestReceiveCopyData(t *testing.T) {
	for i, tt := range beCopyDataTests {
		s := newProtoStreamContent(tt.msgBytes)
		data, err := s.ReceiveCopyData()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		payload := make([]byte, len(tt.data))
		_, err = io.ReadFull(data, payload)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.data, payload)
		_, err = data.Read(payload)
		if err != io.EOF {
			t.Errorf("%d: want EOF; got %v", i, err)
		}
	}
}
