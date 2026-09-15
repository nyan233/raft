package raft

import (
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

type StateMachine interface {
	Init(ctx *context.Context) error
	LastCommit(ctx *context.Context) (uint64, error)
	Apply(ctx *context.Context, entries []*raft.Entry) error
	Snapshot(ctx *context.Context) ([]*raft.Entry, error)
	ReCall(ctx *context.Context, entries []*raft.Entry) error
}
