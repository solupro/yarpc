package codec

import "io"

type Header struct {
	ServerMethod string
	Seq          uint64
	Error        string
}

type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GOB_TYPE  Type = "application/gob"
	JSON_TYPE Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GOB_TYPE] = NewGobCodec
}
