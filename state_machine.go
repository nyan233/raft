package raft

import (
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

type StateMachine interface {
	Apply(ctx *context.Context, entries []raft.Entry) error
}
