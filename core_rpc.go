package raft

import (
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nyan233/littlerpc/core/client"
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/littlerpc/core/middle/ns"
	"github.com/nyan233/littlerpc/core/server"
	"github.com/nyan233/raft/pb/message/raft"
	"google.golang.org/protobuf/proto"
)

const (
	coreRpcTaskAppendEntries = iota + 13
)

type asyncVoteRes struct {
	ReqCount  int
	VoteCount uint64
	IsTimeout bool
}

type asyncAppendEntriesRes struct {
	ReqCount     int
	SuccessCount int
	MemberErr    map[string]error
	Req          *raft.AppendEntriesReq
}

type coreRpcTask struct {
	Type     int
	Ctx      *context.Context
	Req      proto.Message
	Callback interface{}
}

type coreRpc struct {
	My         string
	Membership []string
	Leader     string
	proxy      raft.RaftProxy
	rpcServer  *server.Server
	sm         *CoreSm
	asyncQ     chan coreRpcTask
}

func newCoreRpc(sm *CoreSm, my string, membership []string) (*coreRpc, error) {
	c, err := client.New(
		client.WithCodec("json"),
		//client.WithMuxWriter(),
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
		asyncQ:     make(chan coreRpcTask, 1024),
	}
	err = raft.RegisterRaftServer(s, cr, nil)
	if err != nil {
		return nil, err
	}
	go s.Service()
	cr.startAsyncQueueHandler()
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

func (c *coreRpc) GetLeader(ctx *context.Context, req *raft.GetLeaderReq) (rsp *raft.GetLeaderRsp, err error) {
	return c.sm.execGetLeader(ctx, req)
}

func (c *coreRpc) AppendCommands(ctx *context.Context, req *raft.AppendCommandsReq) (rsp *raft.AppendCommandsRsp, err error) {
	return c.sm.execAppendCommands(ctx, req)
}

func (c *coreRpc) startAsyncQueueHandler() {
	go func() {
		for task := range c.asyncQ {
			switch task.Type {
			case coreRpcTaskAppendEntries:
				c.handleAppendEntriesTask(
					task.Ctx,
					task.Req.(*raft.AppendEntriesReq),
					task.Callback.(func(ctx *context.Context, res *asyncAppendEntriesRes)),
					len(c.Membership)/2+1,
				)
			}
		}
	}()
}

func (c *coreRpc) handleAppendEntriesTask(ctx *context.Context, req *raft.AppendEntriesReq, cb func(ctx *context.Context, res *asyncAppendEntriesRes), minReq int) {
	type rpcResult struct {
		Rsp *raft.AppendEntriesRsp
		Err error
	}
	var (
		errCount       atomic.Uint64
		succCount      atomic.Uint64
		memberRsp      = make(map[string]*rpcResult)
		getMemberErrFn = func() map[string]error {
			memberErr := make(map[string]error)
			for k, v := range memberRsp {
				memberErr[k] = v.Err
			}
			return memberErr
		}
	)
	for _, member := range c.Membership {
		memberRsp[member] = &rpcResult{
			Rsp: nil,
			Err: nil,
		}
	}
	for _, member := range c.Membership {
		go func(ctx *context.Context, member string) {
			defer func() {
				if err := recover(); err != nil {
					errCount.Add(1)
					return
				}
			}()
			var errStr = "nil"
			rsp, err := c.proxy.AppendEntries(ctx, req, client.WithAddr(member))
			if err != nil {
				memberRsp[member].Err = err
				errStr = err.Error()
			} else {
				memberRsp[member].Rsp = rsp
				succCount.Add(1)
			}
			if err != nil {
				slog.Info("append entries to membership",
					slog.String("src", c.My),
					slog.String("target", member),
					slog.Int("len", len(req.Entries)),
					slog.String("err", errStr),
				)
			}
		}(ctx.Clone(), member)
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(time.Second * 30)
	defer timeout.Stop()
	for {
		select {
		case <-ticker.C:
			if succCount.Load() >= uint64(minReq) || errCount.Load()+succCount.Load() == uint64(len(c.Membership)) {
				cb(ctx, &asyncAppendEntriesRes{
					ReqCount:     int(succCount.Load() + errCount.Load()),
					SuccessCount: int(succCount.Load()),
					MemberErr:    getMemberErrFn(),
					Req:          req,
				})
				return
			}
		case <-timeout.C:
			cb(ctx, &asyncAppendEntriesRes{
				ReqCount:     int(succCount.Load() + errCount.Load()),
				SuccessCount: int(succCount.Load()),
				MemberErr:    getMemberErrFn(),
				Req:          req,
			})
			return
		}
	}
}

// 请求兄弟节点投票, 遵循多数派规则, 多数节点投票了则认为选举成功, 或者所有节点返回了数据则认为此次任务执行完成
func (c *coreRpc) asyncRequestVote(ctx *context.Context, req *raft.RequestVoteReq, cb func(ctx *context.Context, res *asyncVoteRes)) {
	ctx = ctx.Clone()
	go func(ctx *context.Context) {
		var (
			minVote   = len(c.Membership)/2 + 1
			voteCount atomic.Uint64
			errCount  atomic.Uint64
			succCount atomic.Uint64
		)
		voteCount.Add(1)
		for _, member := range c.Membership {
			ctx2 := ctx.Clone()
			go func(ctx *context.Context, member string) {
				defer func() {
					if r := recover(); r != nil {
						errCount.Add(1)
					}
				}()
				rsp, err := c.proxy.RequestVote(ctx, req, client.WithAddr(member))
				if err != nil {
					errCount.Add(1)
				} else {
					succCount.Add(1)
					if rsp.VoteGranted {
						voteCount.Add(1)
					}
				}
				var errStr = "nil"
				if err != nil {
					errStr = err.Error()
				}
				reqBytes, _ := json.Marshal(req)
				rspBytes, _ := json.Marshal(rsp)
				slog.Info("rpc request vote",
					slog.String("my", c.My),
					slog.String("member", member),
					slog.String("req", string(reqBytes)),
					slog.String("rsp", string(rspBytes)),
					slog.String("err", errStr))
			}(ctx2, member)
		}
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		timeout := time.NewTimer(time.Second * 30)
		defer timeout.Stop()
		for {
			select {
			case <-ticker.C:
				if voteCount.Load() >= uint64(minVote) || errCount.Load()+succCount.Load() == uint64(len(c.Membership)) {
					cb(ctx, &asyncVoteRes{
						ReqCount:  int(errCount.Load() + succCount.Load()),
						VoteCount: voteCount.Load(),
						IsTimeout: false,
					})
					return
				}
			case <-timeout.C:
				cb(ctx, &asyncVoteRes{
					ReqCount:  int(succCount.Load() + errCount.Load()),
					VoteCount: voteCount.Load(),
					IsTimeout: true,
				})
				return
			}
		}
	}(ctx)
}

func (c *coreRpc) asyncAppendEntries(ctx *context.Context, req *raft.AppendEntriesReq, cb func(ctx *context.Context, res *asyncAppendEntriesRes)) {
	if len(req.Entries) > 0 {
		c.asyncQ <- coreRpcTask{
			Type:     coreRpcTaskAppendEntries,
			Ctx:      ctx.Clone(),
			Req:      req,
			Callback: cb,
		}
	} else {
		go c.handleAppendEntriesTask(ctx.Clone(), req, cb, 0)
	}
}
