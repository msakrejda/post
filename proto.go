package post

import (
	"fmt"
)

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

