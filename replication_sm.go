package raft

import (
	"slices"

	"github.com/nyan233/littlerpc/core/client"
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

const (
	replicationSmStateProbe = iota + 1
	replicationSmStateReplicate
)

type followerAppendEntriesCb2 struct {
	Name string
	Ctx  *context.Context
	Req  *raft.AppendEntriesReq
	Rsp  *raft.AppendEntriesRsp
	Err  error
}

type followerAppendEntriesReq struct {
	Ctx *context.Context
	Req *raft.AppendEntriesReq
}

type followerAppendEntriesCmdRes struct {
	Retry bool
}

type followerReplicationSm struct {
	csm         *CoreSm
	state       int
	matchIndex  uint64
	nextIndex   uint64
	maxLogIndex uint64
	addr        string
	rq          chan *followerAppendEntriesReq
}

func (s *followerReplicationSm) startReqProcess() {
	go func() {
		for {
			select {
			case req, ok := <-s.rq:
				if !ok {
					return
				}
			doRequest:
				cq := make(chan smCommandRes, 1)
				rsp, err := s.csm.rpcCli.AppendEntries(req.Ctx, req.Req, client.WithAddr(s.addr))
				s.csm.pushCommand(smCommand{
					typ: smCommandReplicateAppendEntriesCb,
					ctx: req.Ctx,
					anyReq: followerAppendEntriesCb2{
						Name: s.addr,
						Ctx:  req.Ctx,
						Req:  req.Req,
						Rsp:  rsp,
						Err:  err,
					},
					cq: cq,
				})
				select {
				case res := <-cq:
					// 核心状态机出错则exit
					if res.Err != nil {
						return
					}
					anyRsp, ok := res.AnyRsp.(followerAppendEntriesCmdRes)
					if !ok {
						panic("followerAppendEntriesCmd response error")
					}
					if anyRsp.Retry {
						goto doRequest
					}
				}
			}
		}
	}()
}

func (s *followerReplicationSm) pushReq(ctx *context.Context, req *raft.AppendEntriesReq) {
	s.rq <- &followerAppendEntriesReq{
		Ctx: ctx,
		Req: req,
	}
}

func (s *followerReplicationSm) next() (uint64, uint64) {
	// 初始探测时
	if s.state == replicationSmStateProbe && s.matchIndex == 0 {
		return s.nextIndex, s.nextIndex
	}
	if s.nextIndex > s.maxLogIndex {
		return s.maxLogIndex, s.maxLogIndex
	}
	endIndex := s.maxLogIndex
	if endIndex-s.nextIndex > 50 {
		endIndex = s.nextIndex + 50
	}
	return s.nextIndex, endIndex
}

type replicationSm struct {
	csm         *CoreSm
	state       int
	matchIndex  uint64
	maxLogIndex uint64
	followers   map[string]*followerReplicationSm
}

func newReplicationSm(csm *CoreSm) *replicationSm {
	return &replicationSm{
		csm:         csm,
		state:       replicationSmStateProbe,
		matchIndex:  0,
		maxLogIndex: 0,
		followers:   make(map[string]*followerReplicationSm),
	}
}

func (s *replicationSm) getState() int {
	return s.state
}

func (s *replicationSm) newLog(index uint64, newReqFunc func(f *followerReplicationSm, start, end uint64) error) error {
	s.maxLogIndex = index
	s.state = replicationSmStateReplicate
	for _, follower := range s.followers {
		follower.maxLogIndex = index
		if follower.state == replicationSmStateProbe {
			fStart, fEnd := follower.next()
			err := newReqFunc(follower, fStart, fEnd)
			if err != nil {
				return err
			}
			follower.state = replicationSmStateReplicate
		}
	}
	return nil
}

func (s *replicationSm) addFollower(name string, lastLogIndex uint64) {
	f := &followerReplicationSm{
		csm:         s.csm,
		state:       replicationSmStateProbe,
		matchIndex:  0,
		nextIndex:   lastLogIndex + 1,
		maxLogIndex: s.maxLogIndex,
		addr:        name,
		rq:          make(chan *followerAppendEntriesReq, 256),
	}
	s.followers[name] = f
	f.startReqProcess()
}

func (s *replicationSm) followerNextIndex(name string) (uint64, uint64) {
	f, ok := s.followers[name]
	if !ok {
		panic("follower not found: " + name)
	}
	return f.next()
}

func (s *replicationSm) quorum() int {
	return s.csm.mm.quorum()
}

func (s *replicationSm) calMatch() (uint64, uint64) {
	fmatch := make([]uint64, 0, 8)
	for _, follower := range s.followers {
		fmatch = append(fmatch, follower.matchIndex)
	}
	// leader自己的logIndex == matchIndex
	fmatch = append(fmatch, s.maxLogIndex)
	slices.Sort(fmatch)
	slices.Reverse(fmatch)
	i := s.quorum() - 1
	if i >= len(fmatch) {
		// what, 节点数量不一致?
		panic("quorum out of range")
	}
	oldMatchIndex := s.matchIndex
	s.matchIndex = fmatch[i]
	if s.matchIndex == s.maxLogIndex {
		s.state = replicationSmStateProbe
	}
	return oldMatchIndex, s.matchIndex
}

func (s *replicationSm) pushReq2Follower(name string, ctx *context.Context, req *raft.AppendEntriesReq) (err error) {
	f, ok := s.followers[name]
	if !ok {
		err = emitErr(raft.ErrCode_ErrFollowerNotFound, name)
		return
	}
	f.pushReq(ctx.Clone(), req)
	return
}

func (s *replicationSm) handleFollowerAppendEntriesCb(name string, ctx *context.Context, req *raft.AppendEntriesReq, rsp *raft.AppendEntriesRsp) (next bool, err error) {
	f, ok := s.followers[name]
	if !ok {
		err = emitErr(raft.ErrCode_ErrFollowerNotFound, name)
		return
	}
	lastLogIndex := req.PrevLogIndex
	if len(req.Entries) > 0 {
		lastLogIndex = req.Entries[len(req.Entries)-1].LogIndex
	}
	if rsp.Success {
		if lastLogIndex > f.matchIndex {
			f.matchIndex = lastLogIndex
			f.nextIndex = f.matchIndex + 1
		}
	} else {
		// 根据情况调整冲突索引
		var (
			firstEntry, lastEntry *raft.Entry
		)
		firstEntry, err = s.csm.getLogMgr().First(ctx, true)
		if err != nil {
			return
		}
		lastEntry, err = s.csm.getLogMgr().Last(ctx, true)
		if err != nil {
			return
		}
		if firstEntry == nil || lastEntry == nil {
			f.nextIndex = 1
		} else if firstEntry.LogIndex < rsp.ConflictIndex && rsp.ConflictIndex <= lastEntry.LogIndex {
			f.nextIndex = rsp.ConflictIndex - 1
		} else {
			f.nextIndex = 1
		}
	}

	if f.matchIndex == f.maxLogIndex {
		f.state = replicationSmStateProbe
	}
	next = f.state != replicationSmStateProbe
	oldMatch, newMatch := s.calMatch()
	if newMatch > oldMatch {
		var newMatchLog *raft.Entry
		newMatchLog, err = s.csm.logMgr.ReadLog(ctx, newMatch, true)
		if err != nil {
			return
		}
		if newMatchLog.Term == s.csm.getTerm() {
			s.csm.getLogMgr().SetCommitIndex(newMatch)
			s.csm.aw.asyncApply(applyUserSmReq{
				Ctx:      context.Background(),
				EndIndex: newMatch,
			})
		}
	}
	return
}

func (s *replicationSm) cleanFollowers() {
	for k, v := range s.followers {
		close(v.rq)
		delete(s.followers, k)
	}
}
