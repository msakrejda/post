package post

import (
	"bytes"
	"github.com/uhoh-itsmaciek/post/oid"
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

func compareFormats(t *testing.T, expected, actual []DataFormat) {
	var equal bool
	if len(expected) == len(actual) {
		equal = true
		for i := 0; i < len(expected) && equal; i++ {
			if expected[i] != actual[i] {
				equal = false
			}
		}
	} else {
		t.Errorf("want %v entries; got %v", len(expected), len(actual))
		equal = false
	}
	if !equal {
		t.Errorf("want\n\t%#v;\ngot\n\t%#v", expected, actual)
	}
}

func compareFormatsN(n int, t *testing.T, expected, actual []DataFormat) {
	var equal bool
	if len(expected) == len(actual) {
		equal = true
		for i := 0; i < len(expected) && equal; i++ {
			if expected[i] != actual[i] {
				equal = false
			}
		}
	} else {
		t.Errorf("%d: want %v entries; got %v", n,
			len(expected), len(actual))
		equal = false

	}
	if !equal {
		t.Errorf("%d: want\n\t%#v;\ngot\n\t%#v", n,
			expected, actual)
	}
}

func compareOidSliceN(n int, t *testing.T, expected, actual []oid.Oid) {
	var equal bool
	if len(expected) == len(actual) {
		equal = true
		for i := 0; i < len(expected) && equal; i++ {
			if expected[i] != actual[i] {
				equal = false
			}
		}
	} else {
		t.Errorf("%d: want %v entries; got %v", n,
			len(expected), len(actual))
	}
	if !equal {
		t.Errorf("%d: want\n\t%#v;\ngot\n\t%#v", n,
			expected, actual)
	}
}

func newProtoStream() (*ProtoStream, *bytes.Buffer) {
	var buf bytes.Buffer
	b := FakeConn{&buf}
	s := NewStream(&b)
	return &ProtoStream{str: s}, &buf
}

func newProtoStreamContent(content []byte) *ProtoStream {
	buf := bytes.NewBuffer(content)
	b := FakeConn{buf}
	s := NewStream(&b)
	return &ProtoStream{str: s}
}

func TestExpectExpected(t *testing.T) {
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
	s := newProtoStreamContent([]byte{'x'})
	err := s.Expect('y')
	if err == nil {
		t.Error("want error; got nil")
	}
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

var startupMsgTests = []struct {
	opts     map[string]string
	msgBytes []byte
}{
	{map[string]string{}, []byte{0x0, 0x0, 0x0, 0x9, 0x0, 0x3, 0x0, 0x0, 0x0}},
	{map[string]string{"user": "bob"}, []byte{0x0, 0x0, 0x0, 0x12, 0x0, 0x3, 0x0, 0x0, 0x75, 0x73, 0x65, 0x72, 0x0, 0x62, 0x6f, 0x62, 0x0, 0x0}},
}

func TestSendStartupMessage(t *testing.T) {
	for i, tt := range startupMsgTests {
		s, buf := newProtoStream()
		err := s.SendStartupMessage(tt.opts)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
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
	err = s.Flush()
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
	err = s.Flush()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	expected := []byte{'X', 0x0, 0x0, 0x0, 0x4}
	compareBytes(t, expected, buf.Bytes())
}

var bindTests = []struct {
	portal        string
	statement     string
	formats       []int16
	params        [][]byte
	resultFormats []int16
	msgBytes      []byte
}{
	{"", "", []int16{}, [][]byte{}, []int16{},
		[]byte{'B',
			0x0, 0x0, 0x0, 0xc,
			0x0,      // portal
			0x0,      // statement
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
	{"", "", []int16{0x1, 0x0}, [][]byte{[]byte{0x2, 0x3}, []byte{0x4, 0x5}}, []int16{0x0, 0x1},
		[]byte{'B',
			0x0, 0x0, 0x0, 0x20,
			0x0,      // portal
			0x0,      // statement
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
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var cancelTests = []struct {
	pid       int32
	secretKey int32
	msgBytes  []byte
}{
	{0x1, 0x2, []byte{
		0x0, 0x0, 0x0, 0x10, // length
		0x4, 0xd2, 0x16, 0x2e, // CancelRequest code
		0x0, 0x0, 0x0, 0x1, // pid
		0x0, 0x0, 0x0, 0x2, // secret key
	},
	},
	{0xFFFF, 0x77777777, []byte{
		0x0, 0x0, 0x0, 0x10, // length
		0x4, 0xd2, 0x16, 0x2e, // CancelRequest code
		0x0, 0x0, 0xFF, 0xFF, // pid
		0x77, 0x77, 0x77, 0x77, // secret key
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
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var closeTests = []struct {
	kind     TargetKind
	target   string
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
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var feCopyDataTests = []struct {
	data     []byte
	msgBytes []byte
}{
	{[]byte{}, []byte{'d', 0x0, 0x0, 0x0, 0x4}},
	{[]byte{'x'}, []byte{'d', 0x0, 0x0, 0x0, 0x5, 'x'}},
	{[]byte{'y', 'o'}, []byte{'d', 0x0, 0x0, 0x0, 0x6, 'y', 'o'}},
}

func TestSendCopyData(t *testing.T) {
	for i, tt := range feCopyDataTests {
		s, buf := newProtoStream()
		err := s.SendCopyData(tt.data)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		err = s.Flush()
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
	err = s.Flush()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	expected := []byte{'c', 0x0, 0x0, 0x0, 0x4}
	compareBytes(t, expected, buf.Bytes())
}

var copyFailTests = []struct {
	reason   string
	msgBytes []byte
}{
	{"", []byte{'f', 0x0, 0x0, 0x0, 0x5, 0x0}},
	{"bad copy", []byte{'f', 0x0, 0x0, 0x0, 0xd, 'b', 'a', 'd', ' ', 'c', 'o', 'p', 'y', 0x0}},
}

func TestSendCopyFail(t *testing.T) {
	for i, tt := range copyFailTests {
		s, buf := newProtoStream()
		err := s.SendCopyFail(tt.reason)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var describeTests = []struct {
	kind     TargetKind
	name     string
	msgBytes []byte
}{
	{'S', "", []byte{'D', 0x0, 0x0, 0x0, 0x6, 'S', 0x0}},
	{'P', "", []byte{'D', 0x0, 0x0, 0x0, 0x6, 'P', 0x0}},
	{'S', "joe", []byte{'D', 0x0, 0x0, 0x0, 0x9, 'S', 'j', 'o', 'e', 0x0}},
	{'P', "emily", []byte{'D', 0x0, 0x0, 0x0, 0xB, 'P', 'e', 'm', 'i', 'l', 'y', 0x0}},
}

func TestSendDescribe(t *testing.T) {
	for i, tt := range describeTests {
		s, buf := newProtoStream()
		err := s.SendDescribe(tt.kind, tt.name)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var executeTests = []struct {
	portal string
	maxRows int32
	msgBytes []byte
}{
	{"", 0, []byte{'E',
		0x0,0x0,0x0,0x9,
		0x0,
		0x0,0x0,0x0,0x0}},
	{"steve", 1, []byte{'E',
		0x0,0x0,0x0,0xe,
		's','t','e','v','e',0x0,
		0x0,0x0,0x0,0x1}},
}

func TestSendExecute(t *testing.T) {
	for i, tt := range executeTests {
		s, buf := newProtoStream()
		err := s.SendExecute(tt.portal, tt.maxRows)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

func TestSendFlush(t *testing.T) {
	s, buf := newProtoStream()
	err := s.SendFlush()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	err = s.Flush()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	compareBytes(t, []byte{'H',0x0,0x0,0x0,0x4}, buf.Bytes())
}

var parseTests = []struct {
	statement string
	query string
	paramTypes []oid.Oid
	msgBytes []byte

}{
	{"", "SELECT 1", []oid.Oid{}, []byte{'P',
		0x0,0x0,0x0,0x10,
		0x0,
		'S','E','L','E','C','T',' ','1', 0x0,
		0x0, 0x0}},
	{"steve", "SELECT $1 + $2", []oid.Oid{oid.Oid(20),oid.Oid(23)}, []byte{'P',
		0x0,0x0,0x0,0x23,
		's','t','e','v','e',0x0,
		'S','E','L','E','C','T',' ','$','1',' ','+',' ','$','2',0x0,
		0x0,0x2,
		0x0,0x0,0x0,0x14,
		0x0,0x0,0x0,0x17}},
}

func TestSendParse(t *testing.T) {
	for i, tt := range parseTests {
		s, buf := newProtoStream()
		err := s.SendParse(tt.statement, tt.query, tt.paramTypes)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var passwordTests = []struct {
	password string
	msgBytes []byte
}{
	{"", []byte{'p',0x0,0x0,0x0,0x5,0x0}},
	{"hunter2", []byte{'p',0x0,0x0,0x0,0xc,'h','u','n','t','e','r','2',0x0}},
}

func TestSendPasswordMessage(t *testing.T) {
	for i, tt := range passwordTests {
		s, buf := newProtoStream()
		err := s.SendPasswordMessage(tt.password)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

var queryTests = []struct {
	query string
	msgBytes []byte
}{
	{"", []byte{'Q',0x0,0x0,0x0,0x5,0x0}},
	{"SELECT 42", []byte{'Q',0x0,0x0,0x0,0xe,'S','E','L','E','C','T',' ','4','2',0x0}},
}

func TestSendQuery(t *testing.T) {
	for i, tt := range queryTests {
		s, buf := newProtoStream()
		err := s.SendQuery(tt.query)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		err = s.Flush()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareBytesN(i, t, tt.msgBytes, buf.Bytes())
	}
}

func TestSendSync(t *testing.T) {
	s, buf := newProtoStream()
	err := s.SendSync()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	err = s.Flush()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
	compareBytes(t, []byte{'S',0x0,0x0,0x0,0x4}, buf.Bytes())
}

var authRecvTests = []struct {
	authType AuthResponseType
	payload  []byte
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

var commandCompleteTests = []struct {
	tag      string
	msgBytes []byte
}{
	{"INSERT 1 0", []byte{0x0, 0x0, 0x0, 0xF, 'I', 'N', 'S', 'E', 'R', 'T', ' ', '1', ' ', '0', 0x0}},
	{"DELETE 42", []byte{0x0, 0x0, 0x0, 0xE, 'D', 'E', 'L', 'E', 'T', 'E', ' ', '4', '2', 0x0}},
	{"UPDATE 3", []byte{0x0, 0x0, 0x0, 0xD, 'U', 'P', 'D', 'A', 'T', 'E', ' ', '3', 0x0}},
	{"SELECT 1", []byte{0x0, 0x0, 0x0, 0xD, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0x0}},
	{"MOVE 2", []byte{0x0, 0x0, 0x0, 0xB, 'M', 'O', 'V', 'E', ' ', '2', 0x0}},
	{"FETCH 4", []byte{0x0, 0x0, 0x0, 0xC, 'F', 'E', 'T', 'C', 'H', ' ', '4', 0x0}},
	{"COPY 7", []byte{0x0, 0x0, 0x0, 0xB, 'C', 'O', 'P', 'Y', ' ', '7', 0x0}},
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

var beCopyDataTests = []struct {
	data     []byte
	msgBytes []byte
}{
	{[]byte{}, []byte{0x0, 0x0, 0x0, 0x4}},
	{[]byte{'x'}, []byte{0x0, 0x0, 0x0, 0x5, 'x'}},
	{[]byte{'y', 'o'}, []byte{0x0, 0x0, 0x0, 0x6, 'y', 'o'}},
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

var copyResponseTests = []struct {
	copyFormat CopyFormat
	colFormats []DataFormat
	msgBytes   []byte
}{
	{0x0, []DataFormat{0}, []byte{
		0x0, 0x0, 0x0, 0x9, // length
		0x0,      // overall copy format
		0x0, 0x1, // column count
		0x0, 0x0, // col 1 format
	}},
	{0x1, []DataFormat{0, 1, 0}, []byte{
		0x0, 0x0, 0x0, 0xD, // length
		0x1,      // overall copy format
		0x0, 0x3, // column count
		0x0, 0x0, // col 1 format
		0x0, 0x1, // col 2 format
		0x0, 0x0, // col 3 format
	}},
}

func TestReceiveCopyInResponse(t *testing.T) {
	for i, tt := range copyResponseTests {
		s := newProtoStreamContent(tt.msgBytes)
		response, err := s.ReceiveCopyInResponse()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if response.Format != tt.copyFormat {
			t.Errorf("%d: want copy format %v; got %v", i,
				tt.copyFormat, response.Format)
		}
		compareFormatsN(i, t, tt.colFormats, response.ColumnFormats)
	}
}

func TestReceiveCopyOutResponse(t *testing.T) {
	for i, tt := range copyResponseTests {
		s := newProtoStreamContent(tt.msgBytes)
		response, err := s.ReceiveCopyOutResponse()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if response.Format != tt.copyFormat {
			t.Errorf("%d: want copy format %v; got %v", i,
				tt.copyFormat, response.Format)
		}
		compareFormatsN(i, t, tt.colFormats, response.ColumnFormats)
	}
}

func TestReceiveCopyBothResponse(t *testing.T) {
	for i, tt := range copyResponseTests {
		s := newProtoStreamContent(tt.msgBytes)
		response, err := s.ReceiveCopyBothResponse()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if response.Format != tt.copyFormat {
			t.Errorf("%d: want copy format %v; got %v", i,
				tt.copyFormat, response.Format)
		}
		compareFormatsN(i, t, tt.colFormats, response.ColumnFormats)
	}
}

var dataRowTests = []struct {
	data     [][]byte
	msgBytes []byte
}{
	{[][]byte{}, []byte{
		0x0, 0x0, 0x0, 0x6, // length
		0x0, 0x0, // field count
	}},
	{[][]byte{nil}, []byte{
		0x0, 0x0, 0x0, 0xA, // length
		0x0, 0x1, // field count
		0xFF, 0xFF, 0xFF, 0xFF, // field 1 length
	}},
	{[][]byte{[]byte{0xF}}, []byte{
		0x0, 0x0, 0x0, 0xB, // length
		0x0, 0x1, // field count
		0x0, 0x0, 0x0, 0x1, // field 1 length
		0xF, // field 1 bytes
	}},
	{[][]byte{[]byte{0xF, 0x3}, nil, []byte{0x0, 0xA, 0x2}}, []byte{
		0x0, 0x0, 0x0, 0x17, // length
		0x0, 0x3, // field count
		0x0, 0x0, 0x0, 0x2, // field 1 length
		0xF, 0x3, // field 1 bytes
		0xFF, 0xFF, 0xFF, 0xFF, // field 2 length
		0x0, 0x0, 0x0, 0x3, // field 3 length
		0x0, 0xA, 0x2, // field 3 bytes
	}},
}

type fakeDecoder struct {
	t *testing.T
	lastField int16
	fields [][]byte
}

func (fd *fakeDecoder) Decode(colNum int16, data *Stream, length int32) error {
	// N.B.: Here we test the happy path and only use t.Fatal for
	// error reporting. We should additionally test error
	// reporting when Decode return an error.
	fd.lastField += 1
	if fd.lastField != colNum {
		fd.t.Fatalf("want callback for column %v; got %v", colNum)
	}
	if length >= 0 {
		result := make([]byte, length)
		fd.fields = append(fd.fields, result)
		n, err := io.ReadFull(data, result)
		if err != nil {
			fd.t.Fatalf("want nil err reading data for column %v; got %v", colNum, err)
		}
		if int32(n) != length {
			fd.t.Fatalf("want %v bytes read from result; got %v", length, n)
		}
	} else {
		fd.fields = append(fd.fields, nil)
	}
	return nil
}

func TestReceiveDataRow(t *testing.T) {
	for i, tt := range dataRowTests {
		decoder := &fakeDecoder{t: t}
		s := newProtoStreamContent(tt.msgBytes)
		err := s.ReceiveDataRow(decoder.Decode)
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		for j, colData := range decoder.fields {
			compareBytesN(i, t, tt.data[j], colData)
		}
	}
}

func TestReceiveEmptyQueryResponse(t *testing.T) {
	s := newProtoStreamContent([]byte{0x0, 0x0, 0x0, 0x4})
	err := s.ReceiveEmptyQueryResponse()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

var errorResponseTests = []struct {
	fields map[ErrorField]string
	msgBytes []byte
}{
	{map[ErrorField]string{Message: "hello"}, []byte{
		0x0,0x0,0x0,0xC, // length
		byte(Message), 'h','e','l','l','o',0x0, // field 1
		0x0}},
	{map[ErrorField]string{Message: "x", Detail: "y"}, []byte{
		0x0,0x0,0x0,0xB, // length
		byte(Message), 'x',0x0, // field 1
		byte(Detail), 'y',0x0, // field 2
		0x0}},
}

func TestReceiveErrorResponse(t *testing.T) {
	for i, tt := range errorResponseTests {
		s := newProtoStreamContent(tt.msgBytes)
		response, err := s.ReceiveErrorResponse()
		validateErrOrNoticeRespone(i, t, err, response, tt.fields)
	}
}

func TestReceiveNoticeResponse(t *testing.T) {
	// for now, we use the same test data
	for i, tt := range errorResponseTests {
		s := newProtoStreamContent(tt.msgBytes)
		response, err := s.ReceiveNoticeResponse()
		validateErrOrNoticeRespone(i, t, err, response, tt.fields)
	}
}

func validateErrOrNoticeRespone(i int, t *testing.T, err error,
	response, expected map[ErrorField]string) {
	if err != nil {
		t.Errorf("%d: want nil err; got %v", i, err)
	}
	if expected, actual := len(expected), len(response); expected != actual {
		t.Errorf("%d: want %v fields; got %v", i, expected, actual)
	}
	for k, expectedVal := range expected {
		actualVal, ok := response[k]
		if !ok {
			t.Errorf("%d: want field %c present; is absent", i, k)
		}
		if expectedVal != actualVal {
			t.Errorf("%d: want field %v to be %v; got %v", i, k,
				expectedVal, actualVal)
		}
	}
}

func TestReceiveNoData(t *testing.T) {
	s := newProtoStreamContent([]byte{0x0, 0x0, 0x0, 0x4})
	err := s.ReceiveNoData()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}


var notificationResponseTests = []struct {
	pid int32
	channel string
	payload string
	msgBytes []byte
}{
	{4, "x", "", []byte{0x0,0x0,0x0,0xB,
		0x0,0x0,0x0,0x4, // pid
		'x',0x0, // channel
		0x0, // payload
	}},
	{4, "foo", "bar", []byte{0x0,0x0,0x0,0x10,
		0x0,0x0,0x0,0x4, // pid
		'f','o','o',0x0, // channel
		'b','a','r',0x0, // payload
	}},
}

func TestReceiveNotificationResponse(t *testing.T) {
	for i, tt := range notificationResponseTests {
		s := newProtoStreamContent(tt.msgBytes)
		response, err := s.ReceiveNotificationResponse()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if tt.pid != response.Pid {
			t.Errorf("%d: want pid %v; got %v", i, tt.pid, response.Pid)
		}
		if tt.channel != response.Channel {
			t.Errorf("%d: want channel %v; got %v", i, tt.channel, response.Channel)
		}
		if tt.payload != response.Payload {
			t.Errorf("%d: want payload %v; got %v", i, tt.payload, response.Payload)
		}
	}
}

var parameterDescriptionTests = []struct {
	oids []oid.Oid
	msgBytes []byte
}{
	{[]oid.Oid{}, []byte{0x0,0x0,0x0,0x6,
		0x0,0x0,
	}},
	{[]oid.Oid{3}, []byte{0x0,0x0,0x0,0xA,
		0x0,0x1,
		0x0,0x0,0x0,0x3,
	}},
	{[]oid.Oid{3,2}, []byte{0x0,0x0,0x0,0xE,
		0x0,0x2,
		0x0,0x0,0x0,0x3,
		0x0,0x0,0x0,0x2,
	}},
}

func TestReceiveParameterDescription(t *testing.T) {
	for i, tt := range parameterDescriptionTests {
		s := newProtoStreamContent(tt.msgBytes)
		response, err := s.ReceiveParameterDescription()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		compareOidSliceN(i, t, tt.oids, response)
	}
}

var parameterStatusTests = []struct {
	param string
	value string
	msgBytes []byte
}{
	{"foo", "bar", []byte{0x0,0x0,0x0,0xC,
		'f','o','o',0x0,
		'b','a','r',0x0}},
	{"server_encoding", "UTF8", []byte{0x0,0x0,0x0,0x19,
		's','e','r','v','e','r','_','e','n','c','o','d','i','n','g',0x0,
		'U','T','F','8',0x0}},
}

func TestReceiveParameterStatus(t *testing.T) {
	for i, tt := range parameterStatusTests {
		s := newProtoStreamContent(tt.msgBytes)
		response, err := s.ReceiveParameterStatus()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if response.Parameter != tt.param {
			t.Errorf("%d: want parameter %v; got %v", i,
				tt.param, response.Parameter)
		}
		if response.Value != tt.value {
			t.Errorf("%d: want parameter %v; got %v", i,
				tt.value, response.Value)
		}
	}
}

func TestReceiveParseComplete(t *testing.T) {
	s := newProtoStreamContent([]byte{0x0, 0x0, 0x0, 0x4})
	err := s.ReceiveParseComplete()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

func TestReceivePortalSuspended(t *testing.T) {
	s := newProtoStreamContent([]byte{0x0, 0x0, 0x0, 0x4})
	err := s.ReceivePortalSuspended()
	if err != nil {
		t.Errorf("want nil err; got %v", err)
	}
}

var rfqTests = []struct {
	status TransactionStatus
	msgBytes []byte
}{
	{'I', []byte{0x0,0x0,0x0,0x5,'I'}},
	{'T', []byte{0x0,0x0,0x0,0x5,'T'}},
	{'E', []byte{0x0,0x0,0x0,0x5,'E'}},
}

func TestReceiveReadyForQuery(t *testing.T) {
	for i, tt := range rfqTests {
		s := newProtoStreamContent(tt.msgBytes)
		status, err := s.ReceiveReadyForQuery()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if status != tt.status {
			t.Errorf("%d: want status %v; got %v", i, tt.status, status)
		}
	}
}

var rowDescriptionTests = []struct {
	fields []FieldDescription
	msgBytes []byte
}{
	{[]FieldDescription{}, []byte{0x0,0x0,0x0,0x6,
		0x0,0x0}},
	{[]FieldDescription{FieldDescription{"foo",0,0,25,-1,0,TextFormat}},
		[]byte{0x0,0x0,0x0,0x1c,
			0x0,0x1,         // field count
			'f','o','o',0x0, // field 1 name
			0x0,0x0,0x0,0x0, // field 1 table oid
			0x0,0x0,         // field 1 attnum
			0x0,0x0,0x0,0x19, // field 1 data type
			0xff,0xff, // field 1 typlen
			0x0,0x0,0x0,0x0, // field 1 typmod
			0x0,0x0, // field 1 format
		}},
}

func TestReceiveRowDescription(t *testing.T) {
	for i, tt := range rowDescriptionTests {
		s := newProtoStreamContent(tt.msgBytes)
		fields, err := s.ReceiveRowDescription()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if len(fields) != len(tt.fields) {
			t.Errorf("%d: want %v fields; got %v", i, len(tt.fields), len(fields))
		}
		for j := 0; j < len(fields); j++ {
			expected, actual := tt.fields[j], fields[j]
			if expected.Name != actual.Name {
				t.Errorf("%d: want field %d Name %v; got %v",
					i, j, expected.Name, actual.Name)
			}
			if expected.TableOid != actual.TableOid {
				t.Errorf("%d: want field %d TableOid %v; got %v",
					i, j, expected.TableOid, actual.TableOid)
			}
			if expected.TableAttNo != actual.TableAttNo {
				t.Errorf("%d: want field %d TableAttNo %v; got %v",
					i, j, expected.TableAttNo, actual.TableAttNo)
			}
			if expected.TypeOid != actual.TypeOid {
				t.Errorf("%d: want field %d TypeOid %v; got %v",
					i, j, expected.TypeOid, actual.TypeOid)
			}
			if expected.TypLen != actual.TypLen {
				t.Errorf("%d: want field %d TypLen %v; got %v",
					i, j, expected.TypLen, actual.TypLen)
			}
			if expected.AttTypMod != actual.AttTypMod {
				t.Errorf("%d: want field %d AttTypMod %v; got %v",
					i, j, expected.AttTypMod, actual.AttTypMod)
			}
			if expected.Format != actual.Format {
				t.Errorf("%d: want field %d Format %v; got %v",
					i, j, expected.Format, actual.Format)
			}
		}
	}
}

var receiveSSLTests = []struct {
	ssl ServerSSL
	msgBytes []byte
}{
	{'S', []byte{'S'}},
	{'N', []byte{'N'}},
}

func TestReceiveSSLResponse(t *testing.T) {
	for i, tt := range receiveSSLTests {
		s := newProtoStreamContent(tt.msgBytes)
		ssl, err := s.ReceiveSSLResponse()
		if err != nil {
			t.Errorf("%d: want nil err; got %v", i, err)
		}
		if ssl != tt.ssl {
			t.Errorf("%d: want ssl %v; got %v", i, tt.ssl, ssl)
		}
	}
}
