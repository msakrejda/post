package post

import (
	"fmt"
	"github.com/uhoh-itsmaciek/post/oid"
)

type Decoder interface {
	DecodeInto(metadata *FieldDescription, data *Stream, length int32, value interface{}) error
	Decode(metadata *FieldDescription, data *Stream, length int32) (interface{}, error)
}

type TextDecoder struct {}

func (dec *TextDecoder) Decode(field *FieldDescription, data *Stream,
	length int32) (value interface{}, err error) {
	return data.ReadCString()
}

func (dec *TextDecoder) DecodeInto(field *FieldDescription, data *Stream,
	length int32, value interface{}) (err error) {
	switch result := value.(type) {
	case *string:
		*result, err = data.ReadCString()
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("post: could not scan into type %T", value)
	}
}

// codecManager.Register(oid.Text, &TextDecoder{})

// rows come with a FieldDescription--Parameters and DataFormat can also affect decoding
//
// we want either for the driver to decode fields for us, or
// "scan" it into known data types

type CodecManager struct {
	codecs map[DataFormat](map[oid.Oid]Decoder)
	// client always in utf8, datestyle always fixed,
	// extra_float_digits always 3, bytea_output always hex
}

func NewCodecManager() *CodecManager {
	formatMap := make(map[DataFormat](map[oid.Oid]Decoder))
	formatMap[TextFormat] = make(map[oid.Oid]Decoder)
	formatMap[BinaryFormat] = make(map[oid.Oid]Decoder)
	return &CodecManager{formatMap}
}

// or just register by FieldDescription? that gets tricky with typmods et al
func (cm *CodecManager) Register(fmt DataFormat, typOid oid.Oid, decoder Decoder) {
	cm.codecs[fmt][typOid] = decoder
}

func (cm *CodecManager) DecoderFor(fmt DataFormat, typOid oid.Oid) Decoder {
	return cm.codecs[fmt][typOid]
}
