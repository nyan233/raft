package raft

import (
	"cmp"
	"slices"

	"github.com/google/uuid"
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

func maxN(l []int, n int) []int {
	maxn := make([]int, n)
	maxnShift := func(v int) {
		copy(maxn[2:], maxn[1:])
		maxn[1] = v
	}
	for _, v := range l {
		if v > maxn[0] {
			vv := maxn[0]
			maxn[0] = v
			maxnShift(vv)
		}
	}
	return maxn
}

func maxNForSort(l []int, n int) []int {
	slices.SortFunc(l, func(a, b int) int {
		return cmp.Compare(b, a)
	})
	return l[:n]
}

func genID() string {
	return uuid.NewString()
}
