package raft

import (
	error2 "github.com/nyan233/littlerpc/core/protocol/error"
	"github.com/nyan233/raft/pb/message/raft"
)

func emitErr(errcode raft.ErrCode, mores ...interface{}) error2.LErrorDesc {
	return error2.LNewStdError(int(errcode), raft.ErrCode_name[int32(errcode)], mores...)
}

func rpcErrorIs(err error, errcode raft.ErrCode) bool {
	ed, ok := err.(error2.LErrorDesc)
	if !ok {
		return false
	}
	return ed.Code() == int(errcode)
}
