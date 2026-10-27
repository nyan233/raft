package raft

import (
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

type coreSmRpc struct {
	csm *CoreSm
}

func (c *coreSmRpc) RequestVote(ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	cq := make(chan smCommandRes, 1)
	c.csm.q <- smCommand{
		typ: smCommandRequestVote,
		ctx: ctx,
		req: req,
		cq:  cq,
	}
	select {
	case res := <-cq:
		if res.Err != nil {
			return nil, res.Err
		}
		rsp = res.Rsp.(*raft.RequestVoteRsp)
	}
	return rsp, nil
}

func (c *coreSmRpc) AppendEntries(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	cq := make(chan smCommandRes, 1)
	if len(req.Entries) == 0 {
		c.csm.q <- smCommand{
			typ: smCommandLeaderHeartBeat,
			ctx: ctx,
			req: req,
			cq:  cq,
		}
	} else {
		c.csm.q <- smCommand{
			typ: smCommandAppendEntries,
			ctx: ctx,
			req: req,
			cq:  cq,
		}
	}
	select {
	case res := <-cq:
		if res.Err != nil {
			return nil, res.Err
		}
		rsp = res.Rsp.(*raft.AppendEntriesRsp)
	}
	return rsp, nil
}

func (c *coreSmRpc) InstallSnapshot(ctx *context.Context, req *raft.InstallSnapshotReq) (rsp *raft.InstallSnapshotRsp, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *coreSmRpc) GetLeader(ctx *context.Context, req *raft.GetLeaderReq) (rsp *raft.GetLeaderRsp, err error) {
	cq := make(chan smCommandRes, 1)
	c.csm.q <- smCommand{
		typ: smCommandGetLeader,
		ctx: ctx,
		req: req,
		cq:  cq,
	}
	select {
	case res := <-cq:
		if res.Err != nil {
			return nil, res.Err
		}
		rsp = res.Rsp.(*raft.GetLeaderRsp)
	}
	return rsp, nil
}

func (c *coreSmRpc) AppendCommands(ctx *context.Context, req *raft.AppendCommandsReq) (rsp *raft.AppendCommandsRsp, err error) {
	cq := make(chan smCommandRes, 1)
	c.csm.q <- smCommand{
		typ: smCommandAppendCommands,
		ctx: ctx,
		req: req,
		cq:  cq,
	}
	select {
	case res := <-cq:
		if res.Err != nil {
			return nil, res.Err
		}
		rsp = res.Rsp.(*raft.AppendCommandsRsp)
	}
	return rsp, nil
}

func (c *coreSmRpc) LocalRead(ctx *context.Context, req *raft.LocalReadReq) (rsp *raft.LocalReadRsp, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *coreSmRpc) LinnerRead(ctx *context.Context, req *raft.LinnerReadReq) (rsp *raft.LinnerReadRsp, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *coreSmRpc) AddServer(ctx *context.Context, req *raft.AddServerReq) (rsp *raft.AddServerRsp, err error) {
	cq := make(chan smCommandRes, 1)
	c.csm.q <- smCommand{
		typ: smCommandAddServer,
		ctx: ctx,
		req: req,
		cq:  cq,
	}
	select {
	case res := <-cq:
		if res.Err != nil {
			return nil, res.Err
		}
		rsp = res.Rsp.(*raft.AddServerRsp)
	}
	return rsp, nil
}

func (c *coreSmRpc) RemoveServer(ctx *context.Context, req *raft.RemoveServerReq) (rsp *raft.RemoveServerRsp, err error) {
	cq := make(chan smCommandRes, 1)
	c.csm.q <- smCommand{
		typ: smCommandRemoveServer,
		ctx: ctx,
		req: req,
		cq:  cq,
	}
	select {
	case res := <-cq:
		if res.Err != nil {
			return nil, res.Err
		}
		rsp = res.Rsp.(*raft.RemoveServerRsp)
	}
	return rsp, nil
}
