package post

import (
	"fmt"
	"io"
	"github.com/uhoh-itsmaciek/post/oid"
	"log"
)

const debug bool = false

type ProtoFailErr struct {
	want string
	got string
}

type PGErr struct {
	Details map[ErrorField]string
}

func (e *PGErr) Error() string {
	return fmt.Sprintf("ERROR: %s", e.Details[Message])
}

type PGNotice struct {
	Details map[ErrorField]string
}

func (p *ProtoFailErr) Error() string {
	return fmt.Sprintf("proto: protocol failure: want %s; got %s", p.want, p.got)
}

func NewProtoErr(want, got string) error {
	return &ProtoFailErr{want, got}
}

func NewProtoMessageErr(want string, got byte) error {
	return &ProtoFailErr{want, fmt.Sprintf("type '%c'", got)}
}

const (
	MsgAuthentication  = 'R'
	MsgBackendKeyData  = 'K'
	MsgCommandComplete = 'C'
	MsgCopyInResponse  = 'G'
	MsgCopyOutResponse = 'H'
	MsgDataRow = 'D'
	MsgEmptyQueryResponse = 'I'
	MsgErrorResponse   = 'E'
	MsgNoticeResponse  = 'N'
	MsgNotificationResponse  = 'A'
	MsgParameterStatus = 'S'
	MsgReadyForQuery   = 'Z'
	MsgRowDescription  = 'T'
)

type ServerSSL byte

const (
	SSLAccepted ServerSSL = 'S'
	SSLRejected ServerSSL = 'N'
)

type AuthResponseType int32

const (
	AuthenticationOk                AuthResponseType = 0
	AuthenticationKerberosV5        AuthResponseType = 2
	AuthenticationCleartextPassword AuthResponseType = 3
	AuthenticationMD5Password       AuthResponseType = 5
	AuthenticationSCMCredential     AuthResponseType = 6
	AuthenticationGSS               AuthResponseType = 7
	AuthenticationSSPI              AuthResponseType = 9
	AuthenticationGSSContinue       AuthResponseType = 8
)

type AuthResponse struct {
	Subtype AuthResponseType
	Payload []byte
}

type TargetKind byte

const (
	Statement TargetKind = 'S'
	Portal    TargetKind = 'P'
)

type DataFormat int16

const (
	TextFormat   DataFormat = 0
	BinaryFormat DataFormat = 1
)

type CopyFormat byte

const (
	CopyText   CopyFormat = 0
	CopyBinary CopyFormat = 1
)

type BackendKeyData struct {
	Pid       int32
	SecretKey int32
}

type CopyResponse struct {
	Format        CopyFormat
	ColumnFormats []DataFormat
}

type Notification struct {
	Pid int32
	Channel string
	Payload string
}

type ParameterStatus struct {
	Parameter string
	Value string
}

type ErrorField byte

const (
	Severity         ErrorField = 'S'
	Code             ErrorField = 'C'
	Message          ErrorField = 'M'
	Detail           ErrorField = 'D'
	Hint             ErrorField = 'H'
	Position         ErrorField = 'P'
	InternalPosition ErrorField = 'p'
	InternalQuery    ErrorField = 'q'
	Where            ErrorField = 'W'
	Schema           ErrorField = 's'
	Table            ErrorField = 't'
	Column           ErrorField = 'c'
	DataType         ErrorField = 'd'
	Constraint       ErrorField = 'n'
	File             ErrorField = 'F'
	Line             ErrorField = 'L'
	Routine          ErrorField = 'R'
)

var ErrorFields map[byte]string = map[byte]string{
	'S': "Severity",
	'C': "Code",
	'M': "Message",
	'D': "Detail",
	'H': "Hint",
	'P': "Position",
	'p': "InternalPosition",
	'q': "InternalQuery",
	'W': "Where",
	's': "Schema",
	't': "Table",
	'c': "Column",
	'd': "DataType",
	'n': "Constraint",
	'F': "File",
	'L': "Line",
	'R': "Routine",
}

func ErrorCode(code byte) string {
	return ErrorFields[code]
}

type TransactionStatus byte

const (
	Idle          TransactionStatus = 'I'
	InTransaction TransactionStatus = 'T'
	Error         TransactionStatus = 'E'
)

type FieldDescription struct {
	Name       string
	TableOid   oid.Oid
	TableAttNo int16
	TypeOid    oid.Oid
	TypLen     int16
	AttTypMod  int32
	Format     DataFormat
}

type ProtoStream struct {
	str  *Stream

	hasNext bool
	next byte
	lastErr error
}

// Look at the next message type from the stream without consuming it.
func (p *ProtoStream) Peek() (msgType byte, err error) {
	if p.hasNext {
		return p.next, p.lastErr
	}

	p.next, p.lastErr = p.str.ReadByte()
	p.hasNext = true
	return p.next, p.lastErr
}

// Read the next message type from the stream.
func (p *ProtoStream) Next() (msgType byte, err error) {
	if p.hasNext {
		p.hasNext = false
		return p.next, p.lastErr
	}
	p.next, p.lastErr = p.str.ReadByte()
	return p.next, p.lastErr
}

// Read the next message type from the stream. Return an error if it
// could not be read or is not the expected type.
func (p *ProtoStream) Expect(expected byte) (err error) {
	next, err := p.Next()
	if err != nil {
		return err
	}
	if next != expected {
		return fmt.Errorf("post: expected message type %c; got %c",
			expected, next)
	}
	return nil
}

func (p *ProtoStream) SendStartupMessage(params map[string]string) (err error) {
	if debug { log.Println("> StartupMessage") }
	var msgSize int32 = 4 /* size itself */ + 4 /* protocol header */
	for key, val := range params {
		msgSize += int32(len(key)) + 1 + int32(len(val)) + 1
	}
	msgSize += 1 // the trailing zero byte
	_, err = p.str.WriteInt32(msgSize)
	if err != nil {
		return err
	}
	// the protocol version number
	_, err = p.str.WriteInt32(196608)
	if err != nil {
		return err
	}
	for key, val := range params {
		_, err = p.str.WriteCString(key)
		if err != nil {
			return err
		}
		_, err = p.str.WriteCString(val)
		if err != nil {
			return err
		}
	}
	_, err = p.str.WriteByte(0)
	return err
}

func (p *ProtoStream) SendSSLRequest() (err error) {
	if debug { log.Println("> SSLRequest") }
	_, err = p.str.WriteInt32(8)
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(80877103)
	return err
}

func (p *ProtoStream) SendTerminate() (err error) {
	if debug { log.Println("> Terminate") }
	return p.sendEmpty('X')
}

func (p *ProtoStream) SendBind(portal string, statement string,
	formats []int16, params [][]byte, resultFormats []int16) (err error) {
	if debug { log.Println("> Bind") }
	_, err = p.str.WriteByte('B')
	if err != nil {
		return err
	}
	msgSize := 4 + (len(portal) + 1) + (len(statement) + 1) +
		(2 + len(formats)*2) +
		2 + // param count; we account for actual params below
		(2 + len(resultFormats)*2)
	for _, param := range params {
		msgSize += 4 + len(param)
	}
	_, err = p.str.WriteInt32(int32(msgSize))
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(portal)
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(statement)
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt16(int16(len(formats)))
	if err != nil {
		return err
	}
	for _, fmt := range formats {
		_, err = p.str.WriteInt16(fmt)
		if err != nil {
			return err
		}
	}
	_, err = p.str.WriteInt16(int16(len(params)))
	for _, param := range params {
		_, err = p.str.WriteInt32(int32(len(param)))
		if err != nil {
			return err
		}
		_, err := p.str.Write(param)
		if err != nil {
			return err
		}
	}
	_, err = p.str.WriteInt16(int16(len(resultFormats)))
	if err != nil {
		return err
	}
	for _, fmt := range resultFormats {
		_, err = p.str.WriteInt16(fmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *ProtoStream) SendCancelRequest(pid, secretKey int32) (err error) {
	if debug { log.Println("> CancelRequest") }
	_, err = p.str.WriteInt32(16)
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(80877102)
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(pid)
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(secretKey)
	return err
}

func (p *ProtoStream) SendClose(targetType TargetKind, target string) (err error) {
	if debug { log.Println("> Close") }
	msgSize := int32(4 + 1 + len(target) + 1)
	_, err = p.str.WriteInt32(msgSize)
	if err != nil {
		return err
	}
	_, err = p.str.WriteByte(byte(targetType))
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(target)
	return err
}

func (p *ProtoStream) SendCopyData(data []byte) (err error) {
	if debug { log.Println("> CopyData") }
	_, err = p.str.WriteByte('d')
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(4 + int32(len(data)))
	if err != nil {
		return err
	}
	_, err = p.str.Write(data)
	return err
}

func (p *ProtoStream) SendCopyDone() (err error) {
	if debug { log.Println("> CopyDone") }
	return p.sendEmpty('c')
}

func (p *ProtoStream) SendCopyFail(reason string) (err error) {
	if debug { log.Println("> CopyFail") }
	_, err = p.str.WriteByte('f')
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(4 + int32(len(reason)) + 1)
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(reason)
	return err
}

func (p *ProtoStream) SendDescribe(kind TargetKind, name string) (err error) {
	if debug { log.Println("> Describe") }
	_, err = p.str.WriteByte('D')
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(4 + 1 + int32(len(name)) + 1)
	if err != nil {
		return err
	}
	_, err = p.str.WriteByte(byte(kind))
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(name)
	return err
}

func (p *ProtoStream) SendExecute(portal string, maxRows int32) (err error) {
	if debug { log.Println("> Execute") }
	_, err = p.str.WriteByte('E')
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(4 + int32(len(portal)) + 1 + 4)
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(portal)
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(maxRows)
	return err
}

func (p *ProtoStream) SendFlush() (err error) {
	if debug { log.Println("> Flush") }
	return p.sendEmpty('H')
}

func (p *ProtoStream) SendParse(statement, query string, paramTypes []oid.Oid) (err error) {
	if debug { log.Println("> Parse") }
	_, err = p.str.WriteByte('P')
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(4 + int32(len(statement)) + 1 +
		int32(len(query)) + 1 + 2 + 4 * int32(len(paramTypes)))
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(statement)
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(query)
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt16(int16(len(paramTypes)))
	if err != nil {
		return err
	}
	for _, paramType := range paramTypes {
		_, err = p.str.WriteInt32(int32(paramType))
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *ProtoStream) SendPasswordMessage(password string) (err error) {
	if debug { log.Println("> PasswordMessage") }
	_, err = p.str.WriteByte('p')
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(4 + int32(len(password)) + 1)
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(password)
	return err
}

func (p *ProtoStream) SendQuery(query string) (err error) {
	if debug { log.Println("> Query") }
	_, err = p.str.WriteByte('Q')
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(4 + int32(len(query)) + 1)
	if err != nil {
		return err
	}
	_, err = p.str.WriteCString(query)
	return err
}

func (p *ProtoStream) SendSync() (err error) {
	if debug { log.Println("> Sync") }
	return p.sendEmpty('S')
}

func (p *ProtoStream) Flush() error {
	return p.str.Flush()
}

func (p *ProtoStream) ReceiveAuthResponse() (response *AuthResponse, err error) {
	if debug { log.Println("< AuthResponse") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	subtype, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	var rest []byte
	if size-8 > 0 {
		rest = make([]byte, size-8)
		_, err = p.str.Read(rest)
		if err != nil {
			return nil, err
		}
	}
	return &AuthResponse{AuthResponseType(subtype), rest}, nil
}

func (p *ProtoStream) ReceiveBackendKeyData() (keyData *BackendKeyData, err error) {
	if debug { log.Println("< BackendKeyData") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	if size != 12 {
		return nil, fmt.Errorf("post: expected 12 byte BackendKeyData; got %v", size)
	}
	pid, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	key, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	return &BackendKeyData{pid, key}, nil
}

func (p *ProtoStream) ReceiveBindComplete() (err error) {
	if debug { log.Println("< BindComplete") }
	p.hasNext = false
	return p.receiveEmpty("BindComplete")
}

func (p *ProtoStream) ReceiveCloseComplete() (err error) {
	if debug { log.Println("< CloseComplete") }
	p.hasNext = false
	return p.receiveEmpty("CloseComplete")
}

func (p *ProtoStream) ReceiveCommandComplete() (tag string, err error) {
	if debug { log.Println("< CommandComplete") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return "", err
	}
	tag, err = p.str.ReadCString()
	if int32(4+len(tag)+1) != size {
		return "", fmt.Errorf("post: expected %v byte CommandComplete; got %v",
			size, 4+len(tag)+1)
	}
	return tag, err
}

func (p *ProtoStream) ReceiveCopyData() (data io.Reader, err error) {
	if debug { log.Println("< ReceiveCopyData") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	return io.LimitReader(p.str, int64(size-4)), nil
}

func (p *ProtoStream) ReceiveCopyInResponse() (response *CopyResponse, err error) {
	if debug { log.Println("< ReceiveCopyInResponse") }
	p.hasNext = false
	return p.receiveCopyResponse()
}

func (p *ProtoStream) ReceiveCopyOutResponse() (response *CopyResponse, err error) {
	if debug { log.Println("< ReceiveCopyOutResponse") }
	return p.receiveCopyResponse()
}

func (p *ProtoStream) ReceiveCopyBothResponse() (response *CopyResponse, err error) {
	if debug { log.Println("< ReceiveCopyBothResponse") }
	return p.receiveCopyResponse()
}

func (p *ProtoStream) receiveCopyResponse() (response *CopyResponse, err error) {
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	format, err := p.str.ReadByte()
	if err != nil {
		return nil, err
	}
	colCount, err := p.str.ReadInt16()
	if err != nil {
		return nil, err
	}
	colFormats := make([]DataFormat, colCount)
	for i, _ := range colFormats {
		fmt, err := p.str.ReadInt16()
		if err != nil {
			return nil, err
		}
		colFormats[i] = DataFormat(fmt)
	}
	read := 4 + 1 + 2 + (2 * int32(colCount))
	if read != size {
		return nil, fmt.Errorf("post: expected %v byte CopyInResponse; got %v",
			size, read)
	}
	return &CopyResponse{CopyFormat(format), colFormats}, nil
}

// TODO: this only needs the Read aspects of Stream--extract an
// interface?
//
// N.B.: -1 will be passed for length when the field to decode is null
// data must not be read in this case.
type Decode func(colNum int16, data *Stream, length int32) error

// ReceiveDataRow invokes decodeFn for each field in turn with an
// io.Reader containing the data to be read and an int32 indicating
// the size of the value. If the field in question is null, the
// decodeFn will be called with the arguments (nil, -1).
func (p *ProtoStream) ReceiveDataRow(decodeFn Decode) error {
	if debug { log.Println("< DataRow") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return err
	}
	var totRead int32 = 4
	colCount, err := p.str.ReadInt16()
	if err != nil {
		return err
	}
	totRead += 2
	for colNum := int16(1); colNum <= colCount; colNum++ {
		fieldSize, err := p.str.ReadInt32()
		if err != nil {
			return err
		}
		totRead += 4
		if fieldSize > 0 {
			totRead += fieldSize
		}
		err = decodeFn(colNum, p.str, fieldSize)
		if err != nil {
			return err
		}
	}
	if totRead == size {
		return nil
	} else {
		return fmt.Errorf("post: expected %v byte DataRow; got %v", size, totRead)
	}
}

func (p *ProtoStream) ReceiveEmptyQueryResponse() (err error) {
	if debug { log.Println("< EmptyQueryResponse") }
	return p.receiveEmpty("EmptyQueryResponse")
}

func (p *ProtoStream) ReceiveErrorResponse() (response map[ErrorField]string, err error) {
	// FIXME: this will bogusly print both ErrorResponse and NoticeResponse
	if debug { log.Println("< ErrorResponse") }
	// literally the same thing
	return p.ReceiveNoticeResponse()
}

func (p *ProtoStream) ReceiveNoticeResponse() (response map[ErrorField]string, err error) {
	if debug { log.Println("< NoticeResponse") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	response = make(map[ErrorField]string)
	var totRead int32 = 4
	for code, err := p.str.ReadByte(); err == nil && code != 0x0; code, err = p.str.ReadByte() {
		str, err := p.str.ReadCString()
		if err != nil {
			return nil, err
		}

		response[ErrorField(code)] = str
		totRead += 1 /* for code */ + int32(len(str)) + 1
	}
	if err != nil {
		return nil, err
	}
	totRead += 1 // for last code
	if totRead == size {
		return response, nil
	} else {
		return nil, fmt.Errorf("post: expected %v byte ErrorResponse; got %v", size, totRead)
	}
}

func (p *ProtoStream) ReceiveNoData() (err error) {
	if debug { log.Println("< NoData") }
	return p.receiveEmpty("NoData")
}

func (p *ProtoStream) ReceiveNotificationResponse() (notif *Notification, err error) {
	if debug { log.Println("< NotificationResponse") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	var totRead int32 = 4
	pid, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	totRead += 4
	channel, err := p.str.ReadCString()
	if err != nil {
		return nil, err
	}
	totRead += int32(len(channel)) + 1
	payload, err := p.str.ReadCString()
	if err != nil {
		return nil, err
	}
	totRead += int32(len(payload)) + 1
	if size == totRead {
		return &Notification{pid, channel, payload}, nil
	} else {
		return nil, fmt.Errorf("post: expected %v byte Notification; got %v", size, totRead)
	}
}

func (p *ProtoStream) ReceiveParameterDescription() (desc []oid.Oid, err error) {
	if debug { log.Println("< ParameterDescription") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	var totRead int32 = 4
	count, err := p.str.ReadInt16()
	if err != nil {
		return nil, err
	}
	totRead += 2
	desc = make([]oid.Oid, count)
	for i := int16(0); i < count; i++ {
		param, err := p.str.ReadInt32()
		if err != nil {
			return nil, err
		}
		desc[i] = oid.Oid(param)
		totRead += 4
	}
	if size == totRead {
		return desc, nil
	} else {
		return nil, fmt.Errorf("post: expected %v byte ParameterDescription; got %v",
			size, totRead)
	}
}

func (p *ProtoStream) ReceiveParameterStatus() (status *ParameterStatus, err error) {
	if debug { log.Println("< ParameterStatus") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	var totRead int32 = 4
	param, err := p.str.ReadCString()
	if err != nil {
		return nil, err
	}
	totRead += int32(len(param)) + 1
	value, err := p.str.ReadCString()
	if err != nil {
		return nil, err
	}
	totRead += int32(len(value)) + 1
	if size == totRead {
		return &ParameterStatus{param, value}, nil
	} else {
		return nil, fmt.Errorf("post: expected %v byte ParameterStatus; got %v",
			size, totRead)
	}
}

func (p *ProtoStream) ReceiveParseComplete() (err error) {
	if debug { log.Println("< ParseComplete") }
	return p.receiveEmpty("ParseComplete")
}

func (p *ProtoStream) ReceivePortalSuspended() (err error) {
	if debug { log.Println("< PortalSuspended") }
	return p.receiveEmpty("PortalSuspended")
}

func (p *ProtoStream) ReceiveReadyForQuery() (status TransactionStatus, err error) {
	if debug { log.Println("< ReadyForQuery") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return 0x0, err
	} else if size != 5 {
		return 0x0, fmt.Errorf("post: expected 5 byte ReadyForQuery; got %v", size)
	}
	st, err := p.str.ReadByte()
	return TransactionStatus(st), err
}

func (p *ProtoStream) ReceiveRowDescription() (descs []*FieldDescription, err error) {
	if debug { log.Println("< RowDescription") }
	p.hasNext = false
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	var totRead int32 = 4
	count, err := p.str.ReadInt16()
	if err != nil {
		return nil, err
	}
	totRead += 2
	descs = make([]*FieldDescription, count)
	for i := int16(0); i < count; i++ {
		var desc FieldDescription
		descs[i] = &desc
		desc.Name, err = p.str.ReadCString()
		if err != nil {
			return nil, err
		}
		totRead += int32(len(desc.Name)) + 1
		tableOid, err := p.str.ReadInt32()
		if err != nil {
			return nil, err
		}
		totRead += 4
		desc.TableOid = oid.Oid(tableOid)
		desc.TableAttNo, err = p.str.ReadInt16()
		if err != nil {
			return nil, err
		}
		totRead += 2
		typ, err := p.str.ReadInt32()
		if err != nil {
			return nil, err
		}
		totRead += 4
		desc.TypeOid = oid.Oid(typ)
		desc.TypLen, err = p.str.ReadInt16()
		if err != nil {
			return nil, err
		}
		totRead += 2
		desc.AttTypMod, err = p.str.ReadInt32()
		if err != nil {
			return nil, err
		}
		totRead += 4
		format, err := p.str.ReadInt16()
		if err != nil {
			return nil, err
		}
		desc.Format = DataFormat(format)
		totRead += 2
	}
	if size == totRead {
		return descs, nil
	} else {
		return nil, fmt.Errorf("post: expected %v byte RowDescription; got %v",
			size, totRead)
	}
}

func (p *ProtoStream) ReceiveSSLResponse() (ServerSSL, error) {
	ssl, err := p.str.ReadByte()
	return ServerSSL(ssl), err
}

func (p *ProtoStream) receiveEmpty(name string) error {
	size, err := p.str.ReadInt32()
	p.hasNext = false
	if err != nil {
		return err
	} else if size != 4 {
		return fmt.Errorf("post: expected 4 byte %v; got %v", name, size)
	} else {
		return nil
	}
}

func (p *ProtoStream) sendEmpty(code byte) (err error) {
	_, err = p.str.WriteByte(code)
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(4) // message size
	return err
}
