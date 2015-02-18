package post

// TODO: sessions and their relationship to pools
// a pool should basically let you check out a session
// you should not alter any session state outside of
// specific

// session factory: go from url and/or parameters to a session
// session state
// session pooling vs. tx pooling at the application level
import (
//	"io"
	"time"
)

type ResultSet interface {}

type PGConn interface {
	// retryable... but this is ugly...

	// instead, we can automatically retry if we know it's "safe": we
	// got an error before having actually written anything that would
	// kick off the query
	Query(query string, args... interface{}) (ResultSet, error)
	QueryOnce(query string, args... interface{}) (ResultSet, error)
	Run(query string, args... interface{}) (ResultSet, error)
	RunOnce(query string, args... interface{}) (ResultSet, error)

	Tx(func(tx *Tx) error) error
	// or

	// TODO: review tigertonic interface
	OnError(func(error) error) PGConn
	RetryIf(func(error, string, ...interface{}) bool) PGConn
	// returns a PGConn that handles errors

	// alternately
	Begin() Tx

	// and/or
	Session() PGConn
}

type PGPool interface {
	Query()
	Session() // ?
}

type PGSessionState interface {
	// this sets it on the connection, but also ensures that we re-set it
	// if we reconnect
	//
	// TODO: use more generic session state management? E.g., accept callbacks
	// that take a connection and return an error and can do their own setup?
	// Either in addition to or instead of this.
	Set(guc, value string) // this is a SET SESSION
	// OnConnect(conn *Conn)

	// prepared statements? or is that complicated enough
	// that we just want to return errors?

	Listen(channel string) chan *Notification
}

type PGMonitoredSession struct {
	// conforms to session interface and re-establishes the session
	// given the stored session state and the pre-configured reconnection
	// strategy
	state PGSessionState
	session *PGSession
	// Set updates session state and then executes the set on the session
	// ditto prepare? or just error?
}

func (s *PGMonitoredSession) OnConnect(func(conn *Conn) error) {
	// do misc setup, set session variables, make dblink connections, etc.
	// can be called multiple times
}


// maybe need onConnect / onDisconnect / reconnect handlers?

// func Reconnect(state PGSessionState) (PGSession, error)

type PGSession struct {
	//Prepare(query string) (PGStatement, error)
	//Set("GUC", "value") // this is a SET SESSION; track it in a SessionState
}

//type PGStatement struct {
//	Close()
//	Exec(args... interface{}) (PGAffected, error)
//	Query(args... interface{}) (PGResult, error)
//}

// N.B.: it's always safe to retry something if we know we have not completed
// the specified protocol actions before we get the error: e.g., if we get
// an error before we send Sync in the extended query protocol.

// note that we *can* re-establish/clear a session if we track session
// "state" changes properly, but not always: e.g., dblink and other
// functions (random seeding?) can change session state

// provide an explicit way to check out a "session" so that these
// functions can be used safely, but offer an interface that can
// automagically do the right thing

// type Tx struct {
// 	Cursor() //
// 	Set("GUC", "value") // this is a SET LOCAL
// 	Query() ResultSet, error
// 	Run() Affected, error
// 	Commit() error
// 	Rollback() error
// }

// RetryIf built-in handlers
var Always = func(error) bool { return true }
var Never = func(error) bool { return false }

func WithBackoff(sleeps... time.Duration) (func(error) bool) {
	var i = 0
	return func(error) bool {
		// sleep more for each successive error, then fail
		if i >= len(sleeps) {
			return false
		} else {
			time.Sleep(sleeps[i])
			i += 1
			return true
		}
	}
}

// general idea
var SerializiationFailure = func(error *PGErr) bool { return error.Details[Message] == "serialization failure" }

type Tx struct {
	// conforms to PGConn interface above
}

type PGQueryer interface {
	Query(query string, args... interface{}) (ResultSet, error)
	Run(query string, args... interface{}) (ResultSet, error)
}

// TODO: prepared statements
// N.B.: also only works on session; must all be re-prepared if there is an error

// probably not generally useful but may be a nice, sane building
// block for some internal parts of the driver
type PGConnPool interface {
	CheckOut(Conn) // returned with conn.Close
}

// manage pool size

// pool implementation that implements PGConn above but
//  - reconnects automatically if appropriate
//  -

type PGRows interface {
	Next() error
	Columns()
	Scan(args... interface{}) error
	Read(columns... string) []interface{} // for when you don't know the returned types
}


// type TypeCodec interface {
// 	Encoder(format DataFormat, oid Oid) (func(interface{}, io.Writer) error)
// 	Decoder(format DataFormat, oid Oid) (func(io.Reader) (interface{}, error))
// }

// type DefaultCodec struct {
//
// }

// func (c *DefaultCodec) RegisterEncoder(oid Oid, format DataFormat,
// 	encoder func(interface{}, io.Writer) error) {
// 	c.encoders[format][oid] = encoder
// }

// func (c *DefaultCodec) RegisterDecoder(oid Oid, format DataFormat,
// 	decoder func(io.Reader) (interface{}, error)) {
// 	c.encoders[format][oid] = encoder
// }


// native driver support for postgres features:
//  - COPY
//  - LISTEN / NOTIFY -- tied to session
//  - prepared transactions
//  - savepoints--via nested xacts? or functions in xact interface
//  - DO?
//  - cursors? -- tied to session

// copy
//func (c *PGConn) CopyInReader(table string, source io.Reader) (error)
//func (c *PGConn) CopyIn(table string) (io.Writer, error)

//func (c *PGConn) CopyOut(table string) (io.Reader, error)
//func (c *PGConn) CopyQueryOut(query string) (io.Reader, error)

