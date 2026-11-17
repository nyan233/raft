package raft

import (
	"log"

	"github.com/nyan233/littlerpc/core/middle/codec"
	"github.com/nyan233/raft/pb/message/raft"
	"google.golang.org/protobuf/proto"
)

type LogDiskI interface {
	Init() error
	First(onlyIdx bool) (*raft.Entry, error)
	Last(onlyIdx bool) (*raft.Entry, error)
	TruncateWithLogIndex(end uint64) error
	CleanWithLogIndex(e uint64) error
	WriteBatch(entries []*raft.Entry) error
	Scope(s, e uint64) LogScope
}

type LogScope interface {
	Find() error
	ReadAll(onlyIdx bool) ([]*raft.Entry, error)
	RangeFor(fn func(f *diskList, startOff, count uint64) error) error
}

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
