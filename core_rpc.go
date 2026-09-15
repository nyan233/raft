package raft

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/nyan233/littlerpc/core/client"
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/littlerpc/core/middle/ns"
	"github.com/nyan233/littlerpc/core/server"
	"github.com/nyan233/raft/pb/message/raft"
)

type coreRpc struct {
	My         string
	Membership []string
	Leader     string
	proxy      raft.RaftProxy
	rpcServer  *server.Server
	sm         *CoreSm
}

func newCoreRpc(sm *CoreSm, my string, membership []string) (*coreRpc, error) {
	c, err := client.New(
		client.WithCodec("json"),
		client.WithMuxWriter(),
		client.WithNsStorage(ns.NewFixedStorage(membership)),
	)
	if err != nil {
		return nil, err
	}
	s := server.New(
		server.WithAddressServer(my),
		server.WithDefaultServer(),
	)
	cr := &coreRpc{
		proxy:      raft.NewRaft(c),
		My:         my,
		Membership: membership,
		rpcServer:  s,
		sm:         sm,
	}
	err = raft.RegisterRaftServer(s, cr, nil)
	if err != nil {
		return nil, err
	}
	go s.Service()
	return cr, nil
}

func (c *coreRpc) RequestVote(ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	return c.sm.execRequestVote(ctx, req)
}

func (c *coreRpc) AppendEntries(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	return c.sm.execAppendEntries(ctx, req)
}

func (c *coreRpc) InstallSnapshot(ctx *context.Context, req *raft.InstallSnapshotReq) (rsp *raft.InstallSnapshotRsp, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *coreRpc) parallelRequestVote(ctx *context.Context, req *raft.RequestVoteReq) (rsp map[string]*rpcResult[raft.RequestVoteRsp], err error) {
	membershipRes := make([]rpcResult[raft.RequestVoteRsp], len(c.Membership))
	wg := sync.WaitGroup{}
	wg.Add(len(c.Membership))
	for idx, member := range c.Membership {
		go func(idx2 int, member2 string) {
			defer wg.Done()
			defer func() {
				if err := recover(); err != nil {
					errI, ok := err.(error)
					if ok {
						membershipRes[idx] = rpcResult[raft.RequestVoteRsp]{result: nil, err: errI}
					} else {
						membershipRes[idx] = rpcResult[raft.RequestVoteRsp]{result: nil, err: fmt.Errorf("%v", err)}
					}
				}
			}()
			reqJson, err := json.Marshal(req)
			if err != nil {
				panic(err)
			}
			res, err := c.proxy.RequestVote(ctx, req, client.WithAddr(member2))
			if err != nil {
				membershipRes[idx2] = rpcResult[raft.RequestVoteRsp]{result: nil, err: err}
			} else {
				membershipRes[idx2] = rpcResult[raft.RequestVoteRsp]{result: res, err: nil}
			}
			rspJson, err := json.Marshal(res)
			if err != nil {
				panic(err)
			}
			slog.Info("callRequestVote",
				slog.String("src", c.My),
				slog.String("target", member2),
				slog.String("req", string(reqJson)),
				slog.String("rsp", string(rspJson)))
		}(idx, member)
	}
	wg.Wait()
	rsp = make(map[string]*rpcResult[raft.RequestVoteRsp])
	for idx := range membershipRes {
		rsp[c.Membership[idx]] = &membershipRes[idx]
	}
	return rsp, nil
}

func (c *coreRpc) broadcastHeartbeatAllMemberShip(ctx *context.Context, req *raft.AppendEntriesReq) (string, error) {
	membershipErrs := make([]error, len(c.Membership))
	wg := sync.WaitGroup{}
	wg.Add(len(c.Membership))
	for idx, member := range c.Membership {
		go func(idx2 int, member2 string) {
			defer wg.Done()
			defer func() {
				if err := recover(); err != nil {
					errI, ok := err.(error)
					if ok {
						membershipErrs[idx] = errI
					} else {
						membershipErrs[idx] = fmt.Errorf("%v", err)
					}
				}
			}()
			_, err := c.proxy.AppendEntries(ctx, req, client.WithAddr(member2))
			if err != nil {
				membershipErrs[idx2] = err
			}
		}(idx, member)
	}
	wg.Wait()
	for idx, err := range membershipErrs {
		if err != nil {
			return c.Membership[idx], err
		}
	}
	return "", nil
}
