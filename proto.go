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
