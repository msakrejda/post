package post

// we initialize a Rows object and return it to the user when a user
// submits a SimpleQuery. At that point the Rows object has control of
// the protocol stream until it processes a ReadyForQuery, at which
// point, control of the stream returns to the Conn object.
//
// Note that COPY commands are handled with a separate interface, and
// a COPY issued through the existing mechanism will fail.
//
// TODO: for a general-purpose tool, it would be handy to be able to
// submit COPY queries through the same inteface and have a special
// interface for managing COPY data (e.g., Rows.IsCopy() or something)

// TODO: support database/sql.Scanner for values passed to rows.Scan

// TODO: QueryRow / sql.Row interface

// interface:
//
//   rows.Next() bool
//   rows.Close() error -- called automagically if Next() returns false
//     but may be used to close the result set early
//   rows.Err() error
//   rows.Get() ([]interface, error)
//   rows.Scan([]interface) error
//   rows.Fields() []*FieldDescription
//   rows.NextResult() bool -- jump to the next result set (since the
//      simple query protocol can include multiple result sets)

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

var ErrCopy = errors.New("post: COPY statements are not supported")
var ErrEmptyQuery = errors.New("post: got empty query")
var ErrNoRow = errors.New("post: no row available for reading")

type Rows struct {
	// TODO: maybe we actually want the underlying *ProtoStream
	// instead? Although handing off undesireable messages like NotificationResponse
	// would be trickier
	conn *Conn
	cm *CodecManager
	initialized bool
	query string

	currFields []*FieldDescription
	currDecoders []Decoder
	currValues []interface{}
	hasRow bool
	currTag string

	lastErr error
	lastNulls []bool
}

func (r *Rows) Fields() []*FieldDescription {
	if !r.initialized {
		r.initialize()
		if !r.initialized {
			return []*FieldDescription{}
		}
	}

	return r.currFields
}

func (r *Rows) Next() bool {
	if !r.initialized {
		r.initialize()
		if !r.initialized {
			// something went wrong with initialize;
			// caller should inspect rows.Err()
			return false
		}
	}

	var next byte
	next, r.lastErr = r.conn.nextFiltered()
	if r.lastErr != nil {
		return false
	}
	switch next {
	case MsgDataRow:
		r.hasRow = true
		return true
	case MsgCommandComplete:
		r.currTag, r.lastErr = r.conn.str.ReceiveCommandComplete()
		return false
	case MsgErrorResponse:
		errDetails, err := r.conn.str.ReceiveErrorResponse()
		if err == nil {
			r.lastErr = &PGErr{Details: errDetails}
		} else {
			r.lastErr = err
		}
		return false
	default:
		r.lastErr = NewProtoMessageErr("DataRow, CommandComplete, or ErrorResponse", next)
		return false
	}
}

func (r *Rows) Err() error {
	return r.lastErr
}

func (r *Rows) clear() {
	r.initialized = false

	r.currFields = nil
	r.currDecoders = nil
	r.currValues = nil
	r.currTag = ""

	r.hasRow = false
	r.lastErr = nil
	r.lastNulls = nil
}

func (r *Rows) decodeColumn(colNum int16, data *Stream, length int32) (err error) {
	// track an array of result values and populate it here if
	// they are nil, or scan into them otherwise to distinguish
	// between Decode and DecodeInto
	colIdx := colNum - 1
	currDecoder := r.currDecoders[colIdx]
	currField := r.currFields[colIdx]
	if r.currValues[colIdx] == nil {
		if length > -1 {
			r.currValues[colIdx], err = currDecoder.Decode(currField, data, length)
		} else {
			r.currValues[colIdx], err = nil, nil
		}
	} else {
		if length > -1 {
			err = currDecoder.DecodeInto(currField, data, length, r.currValues[colIdx])
		} else {
			r.lastNulls[colIdx] = true
		}
	}
	return err
}

func (r *Rows) initialize() {
	r.clear()
	var next byte
	next, r.lastErr = r.conn.nextFiltered()
	if r.lastErr != nil {
		return
	}
	switch next {
	case MsgRowDescription:
		r.currFields, r.lastErr = r.conn.str.ReceiveRowDescription()
		if r.lastErr != nil {
			return
		}
		r.currDecoders = make([]Decoder, len(r.currFields))
		for i, field := range r.currFields {
			decoder := r.cm.DecoderFor(field.Format, field.TypeOid)
			if decoder == nil {
				r.lastErr = fmt.Errorf("post: could not find decoder for %v", field)
				return
			}
			r.currDecoders[i] = decoder
		}
		r.lastNulls = make([]bool, len(r.currFields))
	case MsgCopyInResponse, MsgCopyOutResponse:
		r.lastErr = ErrCopy
	case MsgErrorResponse:
		var details map[ErrorField]string
		details, r.lastErr = r.conn.str.ReceiveErrorResponse()
		if r.lastErr == nil {
			r.lastErr = &PGErr{details}
		}
	case MsgEmptyQueryResponse:
		r.lastErr = ErrEmptyQuery
	}
	r.initialized = true
}

func (r *Rows) discardUntilCommandComplete() {
	if !r.initialized {
		// technically, we could just fast-track initialization in
		// this case, but it's probably not worth it
		r.initialize()
		if !r.initialized {
			return
		}
	}

	for {
		var next byte
		next, r.lastErr = r.conn.nextFiltered()
		if r.lastErr != nil {
			return
		}
		switch next {
		case MsgDataRow:
			// TODO: f
			r.lastErr = r.conn.str.ReceiveDataRow(r.decodeColumn)
			if r.lastErr != nil {
				return
			}
		}
	}
}

func (r *Rows) discardColumn(colNum int16, data *Stream, length int32) (err error) {
	_, err = io.CopyN(ioutil.Discard, data, int64(length))
	return err
}

func (r *Rows) Get() ([]interface{}, error) {
	if !r.hasRow {
		return nil, ErrNoRow
	}
	for i := range r.currValues {
		r.currValues[i] = nil
	}
	// TODO: should this error set lastErr? check db/sql
	err := r.conn.str.ReceiveDataRow(r.decodeColumn)
	r.hasRow = false
	return r.currValues, err
}

func (r *Rows) Scan(values ...interface{}) (nulls []bool, _ error) {
	if !r.hasRow {
		return nil, ErrNoRow
	}
	if len(values) != len(r.currFields) {
		return nil, fmt.Errorf("post: want %d fields; got %v",
			len(values), len(r.currFields))
	}
	for i, value := range values {
		if value == nil {
			return nil, fmt.Errorf("post: want non-nil scan destination at index %d", i)
		}
	}
	r.currValues = values
	r.hasRow = false
	return r.lastNulls, r.conn.str.ReceiveDataRow(r.decodeColumn)
}
