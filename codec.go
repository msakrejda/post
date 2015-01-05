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

// alternately, we could chain multiple query results by having a
// NextResult()

// interface:
//
//   rows.Next() bool
//   rows.Close() error -- called automagically if Next() returns false
//     but may be used to close the result set early
//   rows.Err() error
//   rows.Get() ([]interface, error)
//   rows.Scan([]interface) error
//   rows.Fields() []*FieldDescription
//

const ErrCopy = errors.New("post: COPY statements are not supported")
const ErrEmptyQuery = errors.New("post: got empty query")
const ErrNoRow = errors.New("post: no row available for reading")

type Rows struct {
	// TODO: maybe we actually want the underlying *ProtoStream
	// instead? Although handing off undesireable messages like NotificationResponse
	// would be trickier
	conn *Conn
	initialized bool

	currFields []*FieldDescription
	currDecoders []Decoder
	currValues []interface{}
	hasRow bool
	currTag string

	lastErr error
}

func (r *Rows) Fields() []*FieldDescription {
	if !r.initialized {
		r.initialize()
	}
	return r.currFields
}

func (r *Rows) Next() bool {
	if !r.initialized {
		r.initialize()
	}
	if !r.initialized {
		// something went wrong with initialize; caller should
		// inspect rows.Err()
		return false
	}

	// here we need to handle DataRow, CommandComplete, and
	// ErrorResponse
	next, r.lastErr := conn.str.Next()
	if r.lastErr != nil {
		return false
	}
	switch next {
	case DataRow:
		r.hasRow = true
		return true
	case CommandComplete:
		r.currTag, r.lastErr = conn.str.ReceiveCommandComplete()
		return false
	case ErrorResponse:
		errDetails, err := conn.str.ReceiveErrorResponse()
		if err == nil {
			r.lastErr = &PGErr{Details: errDetails}
		}
		return false
	default:
		return NewProtoMessageErr("DataRow, CommandComplete, or ErrorResponse", next)
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
}

// decoders must be instantiated once per connection so that they keep
// local state without fear of clobbering their internal state details
// through other concurrent queries. Alternately, if we require the
// decoders to be pure functions, we don't have to worry about that, but
// it gets harder to manage both Decode and DecodeInto in parallel
func (r *Rows) decodeColumn(colNum int16, data *Stream, length int32) (err error) {
	// track an array of result values and populate it here if
	// they are nil, or scan into them otherwise to distinguish
	// between Decode and DecodeInto
	colIdx = colNum - 1
	currDecoder := r.currDecoders[colIdx]
	currField := r.currFields[colIdx]
	if r.currValues[colIdx] == nil {
		if length > -1 {
			r.currValues[colIdx], err = currDecoder.Decode(currField, data, length)
		} else {
			r.currValues[colIdx], err = nil, nil
		}
	} else {
		err = currDecoder.DecodeInto(currField, data, length, r.currValues[colIdx])
	}
	return err
}

func (r *Rows) initialize() {
	r.clear()
	// possible messages during init:
	//   - here we've just submitted a Query message to the backend, so
	//     we expect it to respond with ErrorResponse,
	//     CopyInResponse, CopyOutResponse, RowDescription, or EmptyQueryResponse
	//
	next, err := r.conn.str.Next()
	switch next {
	case RowDescription:
		r.currFields, r.lastErr = r.conn.str.ReceiveRowDescription()
		if r.lastErr != nil {
			return
		}
		r.currDecoders = make([]Decoder, len(r.currFields))
		for i, field := range r.currFields {
			decoder := r.conn.getDecoder(field)
			if decoder == nil {
				return fmt.Errorf("post: could not find decoder for %v", field)
			}
			r.currDecoders[i] = decoder
		}
	case CopyInResponse, CopyOutResponse:
		r.lastErr = ErrCopy
	case ErrorResponse:
		details, r.lastErr := r.c.str.ReceiveErrorResponse()
		if r.lastErr == nil {
			r.lastErr = &PGErr{details}
		}
	case EmptyQueryResponse:
		r.lastErr = ErrEmptyQuery
	}
	r.initialized = true
}

type Decoder interface {
	// TODO: also pass in Parameters map?
	DecodeInto(metadata *FieldDescription, data *Stream, length int32, value interface{}) error
	Decode(metadata *FieldDescription, data *Stream, length int32) (interface{}, error)
}

func (r *Rows) Get() ([]interface{}, error) {
	if !r.hasRow {
		return ErrNoRow
	}
	r.currValues = make([]interface{}, len(r.currFields))
	err := proto.ReceiveDataRow(r.decodeColumn)
	r.hasRow = false
	return r.currValues, err
}

func (r *Rows) Scan(values ...interface{}) error {
	if !r.hasRow {
		return ErrNoRow
	}
	if len(values) != len(r.currFields) {
		return errors.New("post: want %d fields; got %v", len(values), len(r.currFields))
	}
	for i, value := range values {
		if value == nil {
			return errors.New("post: cannot scan result into nil value at index %d", i)
		}
	}
	r.currValues = values
	r.hasRow = false
	return r.conn.str.ReceiveDataRow(r.decodeColumn)
}

// NULLS: ?


type TextDecoder struct {}

func (dec *TextDecoder) Decode(field *FieldDescription, data *Stream,
	length int32) (value interface{}, err error) {
	return data.ReadCString()
}

func (dec *TextDecoder) DecodeInto(field *FieldDescription, data *Stream,
	length int32, value interface{}) error {
	var result string, err error
	if length > -1 {
		result, err = data.ReadCString()
		if err != nil {
			return err
		}
	}

	switch t := value.(type) {
	case string:
		*t = nil
	default:
		return errors.New("post: could not scan into type %T", value)
	}
}

codecManager.Register(oid.Text, StringTextDecoder{})



// rows come with a FieldDescription--Parameters and DataFormat can also affect decoding
//
// we want either for the driver to decode fields for us, or
// "scan" it into known data types

type CodecManager interface {
	// client always in utf8, datestyle always fixed,
	// extra_float_digits always 3, bytea_output always hex
}

// or just set FieldDescription?
func (cm *CodecManager) Register(fmt DataFormat, typOid Oid, decoder Decoder) error) {
	// track decoder
}

func (cm *CodecManager) DecoderFor(field FieldDescription) (Decoder, error) {
	// return decoder for this field
}

type StringDecoder struct {
	//
	Result string
}

func (d *StringDecoder) Decode(data io.Reader, length int) error {
	d.Result = io.ReadString()
}
