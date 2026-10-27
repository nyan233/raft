package raft

import (
	"log"

	"github.com/nyan233/littlerpc/core/middle/codec"
	"google.golang.org/protobuf/proto"
)

type protoCodec struct{}

func (p *protoCodec) Scheme() string {
	return "protobuf"
}

func (p *protoCodec) Marshal(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

func (p *protoCodec) Unmarshal(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

func init() {
	log.SetFlags(log.Flags() | log.Lmicroseconds)
	codec.Register(new(protoCodec))
}
