package raft

import (
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
	"google.golang.org/protobuf/proto"
)

type coreSmRpc struct {
	csm *CoreSm
	raft.UnImplRaftServer
}

func (c *coreSmRpc) RequestVote(ctx *context.Context, req *raft.RequestVoteReq) (*raft.RequestVoteRsp, error) {
	rsp, err := c.smRequest(ctx, smCommandRequestVote, req)
	if err != nil {
		return nil, err
	}
	return rsp.(*raft.RequestVoteRsp), nil
}

func (c *coreSmRpc) AppendEntries(ctx *context.Context, req *raft.AppendEntriesReq) (*raft.AppendEntriesRsp, error) {
	var typ uint8
	if len(req.Entries) == 0 {
		typ = smCommandLeaderHeartBeat
	} else {
		typ = smCommandAppendEntries
	}
	rsp, err := c.smRequest(ctx, typ, req)
	if err != nil {
		return nil, err
	}
	return rsp.(*raft.AppendEntriesRsp), nil
}

func (c *coreSmRpc) GetLeader(ctx *context.Context, req *raft.GetLeaderReq) (*raft.GetLeaderRsp, error) {
	rsp, err := c.smRequest(ctx, smCommandGetLeader, req)
	if err != nil {
		return nil, err
	}
	return rsp.(*raft.GetLeaderRsp), nil
}

func (c *coreSmRpc) AppendCommands(ctx *context.Context, req *raft.AppendCommandsReq) (*raft.AppendCommandsRsp, error) {
	rsp, err := c.smRequest(ctx, smCommandAppendCommands, req)
	if err != nil {
		return nil, err
	}
	return rsp.(*raft.AppendCommandsRsp), nil
}

func (c *coreSmRpc) AddServer(ctx *context.Context, req *raft.AddServerReq) (*raft.AddServerRsp, error) {
	rsp, err := c.smRequest(ctx, smCommandAddServer, req)
	if err != nil {
		return nil, err
	}
	return rsp.(*raft.AddServerRsp), nil
}

func (c *coreSmRpc) RemoveServer(ctx *context.Context, req *raft.RemoveServerReq) (*raft.RemoveServerRsp, error) {
	rsp, err := c.smRequest(ctx, smCommandRemoveServer, req)
	if err != nil {
		return nil, err
	}
	return rsp.(*raft.RemoveServerRsp), nil
}

func (c *coreSmRpc) smRequest(ctx *context.Context, typ uint8, req proto.Message) (rsp proto.Message, err error) {
	cmd := smCommand{
		typ: typ,
		ctx: ctx,
		req: req,
		cq:  make(chan smCommandRes, 1),
	}
	select {
	case c.csm.q <- cmd:
		break
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case res := <-cmd.cq:
		if res.Err != nil {
			return nil, res.Err
		}
		rsp = res.Rsp
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return rsp, nil
}
