package raft

import (
	"github.com/nyan233/littlerpc/core/client"
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/littlerpc/core/server"
	"github.com/nyan233/raft/pb/message/raft"
)

type raftRpcHelper struct {
	cp         raft.RaftProxy
	rs         *raftServer
	s          *server.Server
	my         string
	leader     string
	membership []string
}

func newRaftRpcServer(my string, membership []string) (*raftRpcHelper, error) {
	c, err := client.New(
		client.WithCodec("protobuf"),
		client.WithMuxWriter(),
	)
	if err != nil {
		return nil, err
	}
	r := &raftRpcHelper{
		cp: raft.NewRaft(c),
	}
	r.s = server.New(
		server.WithAddressServer(my),
		server.WithDefaultServer(),
	)
	err = raft.RegisterRaftServer(r.s, r, nil)
	if err != nil {
		return nil, err
	}
	go r.s.Service()
	return r, nil
}

func (r *raftRpcHelper) RequestVote(ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	return r.rs.handleRequestVote(ctx, req)
}

func (r *raftRpcHelper) AppendEntries(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	return r.rs.handleAppendEntries(ctx, req)
}

func (r *raftRpcHelper) callRequestVote(addr string, ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	return r.cp.RequestVote(ctx, req, client.WithAddr(addr))
}

func (r *raftRpcHelper) callAppendEntries(addr string, ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	return r.cp.AppendEntries(ctx, req, client.WithAddr(addr))
}

func (r *raftRpcHelper) setLeader(leader string) {
	r.leader = leader
}

func (r *raftRpcHelper) broadcastHeartbeatAllMemberShip(ctx *context.Context, term uint64) {
	for _, member := range r.membership {
		r.cp.AppendEntries(ctx, &raft.AppendEntriesReq{
			Term:         term,
			LeaderId:     r.leader,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: 0,
		}, client.WithAddr(member))
	}
}
