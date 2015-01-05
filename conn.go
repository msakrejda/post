package post

import (
	"errors"
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
	case Authentication:
		auth, err := c.str.ReceiveAuthResponse()
		if err != nil {
			return err
		}
		return auther.Authenticate(auth, c.str)
	case ErrorResponse:
		errMap, err := c.str.ReceiveErrorResponse()
		if err != nil {
			return err
		}
		return NewErrErrorResponse(errMap)
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
		case ParameterStatus:
			err = c.readParameterStatus()
			if err != nil {
				return err
			}
		case BackendKeyData:
			err = c.readBackendKeyData()
			if err != nil {
				return err
			}
		case ReadyForQuery:
			return c.readReadyForQuery()
		case ErrorResponse:
			errMap, err := c.str.ReceiveErrorResponse()
			if err != nil {
				return err
			}
			return NewErrErrorResponse(errMap)
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
		case NoticeResponse:
			err = c.readNotice()
		case NotificationResponse:
			err = c.readNotification()
		case ParameterStatus:
			err = c.readParameterStatus()
		default:
			return next, err
		}
		if err != nil {
			return 0, err
		}
	}
}

type simpleQuery struct {
	text string
}

type ResultChain struct {
	query *simpleQuery
}

func (rc *ResultChain) Next() *Result {
	// return the next pending result

	// if only an error was returned by the backend, without even
	// a RowDescription, return a dummy result that will return an
	// error immediately so that we can avoid having to return an
	// error directly from the result chain
	//
	// e.g., usage would be:
	//
	// chain := conn.SimpleQuery("SELECT 1; SELECT 2")
	// for result := chain.Next(); result != nil; result = chain.Next() {
	//         for result.Next() {
	//                 result.Scan(...)
	//         }
	// }
	//
	// We may also want to provide a simpler interface--a
	// ReallySimpleQuery that just returns a ResultSet instead of
	// a ResultChain, since in most situations, that's all that
	// matters. This can return an error after the first result
	// set is processed if there is another one, or simply discard
	// additional result sets. Alternately, we can do that in the
	// wrapping user-friendly connection class rather than this
	// raw, low-level one.
}

type Result struct {
	// 
}

func (r *Result) Fields() []FieldDescription {
	// read RowDescription if it has not been read yet and return
	// the field descriptions
}

func (r *Result) Next() bool {
	// read RowDescription if it has not been read yet and
	// see if there is another result. Alternately have Next()
	// return an error instead of a bool; e.g.,
	//for err := result.Next(); err == nil; err = result.Next() {}
}


func (r *Result) Scan(args... []interface{}) error {
	// look up corresponding decoder for each arg?
}

func (r *Result) Get() ([]interface{}, error) {

}

// the mechanism should support two interfaces:
//
//  * a Scan-like one similar to datbase/sql
//     - ideal for when you know the types you're getting back
//     - also provide support for scanning into a struct / slice of structs?
//  * a Get-like to let the driver decide what types to decode to
//     - return a slice of interfaces?

func (c *Conn) SimpleQuery(query string) (*ResultChain, error) {
	// submit query, return a resultChain that can process the results
	if c.currQuery != nil {
		return nil, errors.New("post: query in progress")
	}
	err := c.str.SendQuery(query)
	if err != nil {
		return nil, err
	}
	query := &simpleQuery{text: query}
	c.currResultChain = &ResultChain{query: query}
	return c.currResultChain, nil
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
	case AuthenticationOK:
		return nil
	case AuthenticationCleartextPassword:
		_, err = str.SendPasswordMessage(password)
		if err != nil {
			return err
		}
	case AuthenticationMD5Password:
		salt = string(initialResp.Payload)
		_, err = str.SendPasswordMessage(MD5ManglePassword(user, password, salt))
		if err != nil {
			return err
		}
	}
	nextType, err := c.str.Next()
	if err != nil {
		return err
	}
	switch nextType {
	case Authentication:
		auth, err := c.str.ReceiveAuthResponse()
		if err != nil {
			return err
		}
		if auth.Subtype == AuthenticationOK {
			return nil
		} else {
			// TODO: return textual subtype in error
			return errors.New("proto: expected AuthenticationOK; got %v", auth.Subtype)
		}
	case ErrorResponse:
		errMap, err := c.str.ReceiveErrorResponse()
		if err != nil {
			return err
		}
		return NewErrErrorResponse(errMap)
	}
}

func (c *Conn) setStream(conn net.Conn) {
	c.conn = conn
	c.str = NewProto(NewStream(conn))
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
	notif, err = c.str.ReceiveNotificationResponse()
	if err != nil {
		return err
	}
	c.notifications = append(c.notifications, notif)
	return nil
}

func (c *Conn) readNotice() error {
	notice, err = c.str.ReceiveNoticeResponse()
	if err != nil {
		return err
	}
	c.notices = append(c.notices, notice)
	return nil
}

func (c *Conn) c.readReadyForQuery() error {
	status, err := c.ReceiveReadyForQuery()
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

