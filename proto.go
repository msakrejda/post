package post

import (
	"fmt"
)

type AuthResponseType int32

const (
	AuthenticationOk AuthResponseType = 0
	AuthenticationKerberosV5 AuthResponseType = 2
	AuthenticationCleartextPassword AuthResponseType = 3
	AuthenticationMD5Password AuthResponseType = 5
	AuthenticationSCMCredential AuthResponseType = 6
	AuthenticationGSS AuthResponseType = 7
	AuthenticationSSPI AuthResponseType = 9
	AuthenticationGSSContinue AuthResponseType = 8
)

type AuthResponse struct {
	Subtype AuthResponseType
	Payload []byte
}

type BackendKeyData struct {
	Pid int32
	SecretKey int32
}

type ProtoStream struct {
	str *Stream
	next byte
}

// Read the next message type from the stream.
func (p *ProtoStream) Next() (msgType byte, err error) {
	p.next, err = p.str.ReadByte()
	if err != nil {
		return 0, err
	}
	return p.next, nil
}

// Read the next message type from the stream. Panic if it's not the
// expected message type.
func (p *ProtoStream) Expect(expected byte) (err error) {
	p.next, err = p.str.ReadByte()
	if err != nil {
		return err
	}
	if p.next != expected {
		panic(fmt.Sprintf("expected message type %v; got %v",
			expected, p.next))
	}
	return nil
}

func (p *ProtoStream) SendStartupMessage(params map[string]string) (err error) {
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
	_, err = p.str.WriteInt32(8)
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(80877103)
	return err
}

func (p *ProtoStream) SendTerminate() (err error) {
	_, err = p.str.WriteByte('X')
	if err != nil {
		return err
	}
	_, err = p.str.WriteInt32(4) // message size
	return err
}

func (p *ProtoStream) SendBind(portal string, statement string,
	formats []int16, params [][]byte, resultFormats []int16) (err error) {
	_, err = p.str.WriteByte('B')
	if err != nil {
		return err
	}
	msgSize := 4 + (len(portal) + 1) + (len(statement) + 1) +
		(2 + len(formats) * 2) +
		2 + // param count; we account for actual params below
		(2 + len(resultFormats) * 2)
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

func (p *ProtoStream) ReceiveAuthResponse() (response *AuthResponse, err error) {
	size, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	subtype, err := p.str.ReadInt32()
	if err != nil {
		return nil, err
	}
	var rest []byte
	if size - 8 > 0 {
		rest = make([]byte, size - 8)
		_, err = p.str.Read(rest)
		if err != nil {
			return nil, err
		}
	}
	return &AuthResponse{AuthResponseType(subtype), rest}, nil
}

func (p *ProtoStream) ReceiveBackendKeyData() (keyData *BackendKeyData, err error) {
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
	size, err := p.str.ReadInt32()
	if err != nil {
		return err
	}
	if size != 4 {
		return fmt.Errorf("post: expected 4 byte BindComplete; got %v", size)
	}
	return nil
}
