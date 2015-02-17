package post

import (
	"errors"
	"fmt"
	"net"
	"crypto/tls"
)

type Conn struct {
	conn net.Conn
 	str *ProtoStream

 	params map[string]string
	keyData *BackendKeyData

	xactStatus TransactionStatus

	notifications []*Notification
	notices []*PGNotice

	currResult *Rows
}

func NewConn(conn net.Conn) *Conn {
	c := &Conn{params: make(map[string]string)}
	c.setStream(conn)
	return c
}

// Provide configuration for and allow verification of a TLS
// connection. This allows us to implement all the different variants
// of libpq's sslmode.
type TLSNegotiator struct {
	Config *tls.Config
	OnResponse func(*tls.Conn) error
}

// before connecting
func (c *Conn) NegotiateTLS(tlsConf TLSNegotiator) error {
	// TODO: bail if already a TLS Conn
	if tlsConf.Config != nil {
		err := c.str.SendSSLRequest()
		if err != nil {
			return err
		}
		err = c.str.Flush()
		if err != nil {
			return err
		}

		response, err := c.str.ReceiveSSLResponse()
		if err != nil {
			return err
		}
		var tlsConn *tls.Conn
		if response == SSLAccepted {
			tlsConn = tls.Client(c.conn, tlsConf.Config)
		}
		if tlsConf.OnResponse != nil {
			err = tlsConf.OnResponse(tlsConn)
		}
		if err == nil {
			c.setStream(tlsConn)
		} else {
			return err
		}
	}
	return nil
}

func (c *Conn) Connect(opts map[string]string, auther Authenticator) error {
	if _, ok := opts["user"]; !ok {
		return errors.New("proto: user is required in connection options")
	}
	err := c.str.SendStartupMessage(opts)
	if err != nil {
		return err
	}
	nextType, err := c.str.Next()
	if err != nil {
		return err
	}
	switch nextType {
	case MsgAuthentication:
		auth, err := c.str.ReceiveAuthResponse()
		if err != nil {
			return err
		}
		return auther.Authenticate(auth, c.str)
	case MsgErrorResponse:
		errMap, err := c.str.ReceiveErrorResponse()
		if err != nil {
			return err
		}
		return &PGErr{Details: errMap}
	default:
		return NewProtoMessageErr("Authentication request or ErrorResponse", nextType)
	}
	// 0 or more ParameterStatus, a BackendKeyData, and an RFQ
	for {
		nextType, err := c.str.Next()
		if err != nil {
			return err
		}
		switch nextType {
		case MsgParameterStatus:
			err = c.readParameterStatus()
			if err != nil {
				return err
			}
		case MsgBackendKeyData:
			err = c.readBackendKeyData()
			if err != nil {
				return err
			}
		case MsgReadyForQuery:
			return c.readReadyForQuery()
		case MsgErrorResponse:
			errMap, err := c.str.ReceiveErrorResponse()
			if err != nil {
				return err
			}
			return &PGErr{Details: errMap}
		default:
			return NewProtoMessageErr("BackendKeyData, ParameterStatus, ReadyForQuery, or ErrorResponse", nextType)

		}
	}
}


func (c *Conn) nextFiltered() (msgType byte, err error) {
	for {
		next, err := c.str.Next()
		if err != nil {
			return 0, err
		}
		switch next {
		case MsgNoticeResponse:
			err = c.readNotice()
		case MsgNotificationResponse:
			err = c.readNotification()
		case MsgParameterStatus:
			err = c.readParameterStatus()
		default:
			return next, err
		}
		if err != nil {
			return 0, err
		}
	}
}

func (c *Conn) SimpleQuery(query string) (*Rows, error) {
	// submit query, return a resultChain that can process the results
	if c.currResult != nil {
		return nil, errors.New("post: query in progress")
	}
	err := c.str.SendQuery(query)
	if err != nil {
		return nil, err
	}
	c.currResult = &Rows{conn: c, query: query}
	return c.currResult, nil
}

type Authenticator interface {
	Authenticate(initialResp *AuthResponse, str *ProtoStream) error
}

type DefaultAuthenticator struct {
	user string
	password string
}

func (a *DefaultAuthenticator) Authenticate(initialResp *AuthResponse, str *ProtoStream) error {
	switch initialResp.Subtype {
	case AuthenticationOk:
		return nil
	case AuthenticationCleartextPassword:
		err := str.SendPasswordMessage(a.password)
		if err != nil {
			return err
		}
	case AuthenticationMD5Password:
		salt := string(initialResp.Payload)
		err := str.SendPasswordMessage(MD5ManglePassword(a.user, a.password, salt))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unsupported authentication subtype %v", initialResp.Subtype)
	}
	nextType, err := str.Next()
	if err != nil {
		return err
	}
	switch nextType {
	case MsgAuthentication:
		auth, err := str.ReceiveAuthResponse()
		if err != nil {
			return err
		}
		if auth.Subtype == AuthenticationOk  {
			return nil
		} else {
			return fmt.Errorf("proto: expected AuthenticationOK; got %v", auth.Subtype)
		}
	case MsgErrorResponse:
		errMap, err := str.ReceiveErrorResponse()
		if err != nil {
			return err
		}
		return &PGErr{Details: errMap}
	default:
		return NewProtoMessageErr("Authentication or ErrorResponse", nextType)
	}
}

func (c *Conn) setStream(conn net.Conn) {
	c.conn = conn
	c.str = &ProtoStream{str: NewStream(conn)}
}

func (c *Conn) readParameterStatus() error {
	status, err := c.str.ReceiveParameterStatus()
	if err != nil {
		return err
	}
	c.params[status.Parameter] = status.Value
	return nil
}

func (c *Conn) readBackendKeyData() error {
	if c.keyData != nil {
		return errors.New("proto: already have backend key data")
	}
	keyData, err := c.str.ReceiveBackendKeyData()
	if err != nil {
		return err
	}
	c.keyData = keyData
	return nil
}

func (c *Conn) readNotification() error {
	notif, err := c.str.ReceiveNotificationResponse()
	if err != nil {
		return err
	}
	c.notifications = append(c.notifications, notif)
	return nil
}

func (c *Conn) readNotice() error {
	notice, err := c.str.ReceiveNoticeResponse()
	if err != nil {
		return err
	}
	c.notices = append(c.notices, &PGNotice{Details: notice})
	return nil
}

func (c *Conn) readReadyForQuery() error {
	status, err := c.str.ReceiveReadyForQuery()
	if err != nil {
		return err
	}
	c.xactStatus = status
	return nil
}

// True if any notifications are pending
func (c *Conn) HasNotification() bool {
	return len(c.notifications) > 0
}

// Consume and return the next notification if any are pending;
// otherwise return nil.
func (c *Conn) NextNotification() *Notification {
	if len(c.notifications) > 0 {
		notif := c.notifications[0]
		c.notifications = c.notifications[1:]
		return notif
	} else {
		return nil
	}
}

