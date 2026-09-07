package raft

import (
	"encoding/json"
	"log/slog"

	"github.com/nyan233/littlerpc/core/client"
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/littlerpc/core/middle/ns"
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

func newRaftRpcServer(rs *raftServer, my string, membership []string) (*raftRpcHelper, error) {
	c, err := client.New(
		client.WithCodec("json"),
		client.WithMuxWriter(),
		client.WithNsStorage(ns.NewFixedStorage(membership)),
	)
	if err != nil {
		return nil, err
	}
	r := &raftRpcHelper{
		cp:         raft.NewRaft(c),
		rs:         rs,
		my:         my,
		membership: membership,
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
	reqJson, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	slog.Info("handleRequestVote", slog.String("candidateId", req.CandiDateId), slog.String("my", r.getMy()), slog.String("req", string(reqJson)))
	return r.rs.handleRequestVote(ctx, req)
}

func (r *raftRpcHelper) AppendEntries(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	reqJson, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	slog.Info("handleAppendEntries", slog.String("leaderId", req.LeaderId), slog.String("my", r.getMy()), slog.String("req", string(reqJson)))
	return r.rs.handleAppendEntries(ctx, req)
}

func (r *raftRpcHelper) callRequestVote(addr string, ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	reqJson, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	rsp, err = r.cp.RequestVote(ctx, req, client.WithAddr(addr))
	rspJson, err := json.Marshal(rsp)
	if err != nil {
		return nil, err
	}
	slog.Info("callRequestVote",
		slog.String("src", r.getMy()),
		slog.String("target", addr),
		slog.String("req", string(reqJson)),
		slog.String("rsp", string(rspJson)))
	return rsp, err
}

func (r *raftRpcHelper) callAppendEntries(addr string, ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	reqJson, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	slog.Info("callAppendEntries", slog.String("src", r.getMy()), slog.String("target", addr), slog.String("req", string(reqJson)))
	return r.cp.AppendEntries(ctx, req, client.WithAddr(addr))
}

func (r *raftRpcHelper) setLeader(leader string) {
	r.leader = leader
}

func (r *raftRpcHelper) getLeader() string {
	return r.leader
}

func (r *raftRpcHelper) getMemberShip() []string {
	return r.membership
}

func (r *raftRpcHelper) getMy() string {
	return r.my
}

func (r *raftRpcHelper) broadcastHeartbeatAllMemberShip(ctx *context.Context, term, logIndex, logTerm, leaderCommit uint64) (string, error) {
	for _, member := range r.membership {
		_, err := r.cp.AppendEntries(ctx, &raft.AppendEntriesReq{
			Term:         term,
			LeaderId:     r.leader,
			PrevLogIndex: logIndex,
			PrevLogTerm:  logTerm,
			Entries:      nil,
			LeaderCommit: leaderCommit,
		}, client.WithAddr(member))
		if err != nil {
			return member, err
		}
	}
	return "", nil
}
