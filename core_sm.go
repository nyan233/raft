package raft

import (
	"cmp"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/nyan233/littlerpc/core/client"
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/littlerpc/core/server"
	"github.com/nyan233/raft/pb/message/raft"
	"google.golang.org/protobuf/proto"
)

const (
	smCommandCandidateTimeout = iota + 1
	smCommandRequestVote
	smCommandAppendEntries
	smCommandLeaderHeartBeat
	smCommandExecHeartBeat
	smCommandAppendCommands
	smCommandGetLeader
	smCommandAsyncRequestVoteComp
	smCommandInsertNoOp
	smCommandAddServer
	smCommandRemoveServer
	smCommandApplyUserSmCb
	smCommandTermLtEv
	smCommandReplicateAppendEntriesCb
)

const (
	candidateTicker = "candidateTicker"
	heartbeatTicker = "heartbeatTicker"
)

type holdAppendCommandsReq struct {
	ctx           *context.Context
	cq            chan smCommandRes
	startLogIndex uint64
	endLogIndex   uint64
	logTerm       uint64
}

type smCommandRes struct {
	Rsp    proto.Message
	AnyRsp interface{}
	Err    error
}

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

type smCommand struct {
	typ    uint8
	ctx    *context.Context
	req    proto.Message
	anyReq any
	cq     chan smCommandRes
}

type CoreSmConfig struct {
	My            string
	Membership    []string
	LogDir        string
	LogName       string
	UserSmFactory func() StateMachine
}

type CoreSm struct {
	cfg                 CoreSmConfig
	state               raft.State
	q                   chan smCommand
	heartbeatTicker     *time.Ticker
	lastLeaderHeartBeat time.Time
	logMgr              *raftLogManager
	md                  *meta
	tt                  *timeoutTicker
	aw                  *applyWorker
	holdList            []*holdAppendCommandsReq
	mm                  *memberManager
	rpcCli              raft.RaftProxy
	rsm                 *replicationSm
}

func NewCoreSm(cfg CoreSmConfig) *CoreSm {
	return &CoreSm{
		cfg:   cfg,
		state: raft.State_StateFollower,
		q:     make(chan smCommand, 16384),
	}
}

func (s *CoreSm) Init() error {
	var err error
	s.md = newMeta(filepath.Join(s.cfg.LogDir, s.cfg.LogName+".meta"))
	err = s.md.init()
	if err != nil {
		return err
	}
	err = s.initMember()
	if err != nil {
		return err
	}
	err = s.initRpc()
	if err != nil {
		return err
	}
	s.logMgr = newRaftLogManager(s.cfg.LogDir, s.cfg.LogName, s.cfg.UserSmFactory())
	err = s.logMgr.Init()
	if err != nil {
		return err
	}
	s.aw = newApplyWorker(s)
	s.aw.run()
	go s.startLoop()
	s.startTimeoutTicker()
	return nil
}

func (s *CoreSm) initMember() error {
	s.mm = new(memberManager)
	s.mm.intiMember(s.cfg.My, s.cfg.Membership)
	return nil
}

func (s *CoreSm) initRpc() error {
	rpcClient, err := client.New(
		client.WithCodec("protobuf"),
		//client.WithMuxWriter(),
		//client.WithNsStorage(ns.NewFixedStorage([]string{})),
	)
	if err != nil {
		return err
	}
	s.rpcCli = raft.NewRaft(rpcClient)
	rpcServer := server.New(
		server.WithAddressServer(s.mm.getMy()),
		server.WithDefaultServer(),
	)
	err = raft.RegisterRaftServer(rpcServer, &coreSmRpc{csm: s}, nil)
	if err != nil {
		return err
	}
	go rpcServer.Service()
	return nil
}

func (s *CoreSm) getTerm() uint64 {
	return s.md.get().Term
}

func (s *CoreSm) getLogMgr() *raftLogManager {
	return s.logMgr
}

func (s *CoreSm) myIsLeader() bool {
	return s.state == raft.State_StateLeader
}

func (s *CoreSm) startTimeoutTicker() {
	s.tt = newTimeoutTicker()
	s.tt.RegisterTicker(tickerTask{
		Name: heartbeatTicker,
		Next: func(t time.Duration) time.Duration {
			return time.Millisecond * 10
		},
		Callback: func(t time.Time) {
			cmd := smCommand{
				typ: smCommandExecHeartBeat,
				ctx: context.Background(),
				req: nil,
				cq:  make(chan smCommandRes, 1),
			}
			s.q <- cmd
			select {
			case <-cmd.cq:
				break
			}
		},
	})
	s.tt.RegisterTicker(tickerTask{
		Name: candidateTicker,
		Next: func(t time.Duration) time.Duration {
			return time.Duration(150+rand.Int63n(151)) * time.Millisecond
		},
		Callback: func(t time.Time) {
			cmd := smCommand{
				typ: smCommandCandidateTimeout,
				ctx: context.Background(),
				req: nil,
				cq:  make(chan smCommandRes, 1),
			}
			s.q <- cmd
			select {
			case <-cmd.cq:
				break
			}
		},
	})
	s.tt.init()
}

func (s *CoreSm) pushCommand(cmd smCommand) {
	s.q <- cmd
}

func (s *CoreSm) startLoop() {
	for {
		select {
		case cmd, ok := <-s.q:
			if !ok {
				return
			}
			switch cmd.typ {
			case smCommandRequestVote:
				rsp, err := s.execRequestVoteFromLoop(cmd.ctx, cmd.req.(*raft.RequestVoteReq))
				cmd.cq <- smCommandRes{
					Rsp: rsp,
					Err: err,
				}
			case smCommandAppendEntries:
				rsp, err := s.execAppendEntriesFromLoop(cmd.ctx, cmd.req.(*raft.AppendEntriesReq))
				cmd.cq <- smCommandRes{
					Rsp: rsp,
					Err: err,
				}
			case smCommandCandidateTimeout:
				s.handleCandidateTimeout(cmd)
			case smCommandLeaderHeartBeat:
				rsp, err := s.execLeaderHeartBeatFromLoop(cmd.ctx, cmd.req.(*raft.AppendEntriesReq))
				cmd.cq <- smCommandRes{
					Rsp: rsp,
					Err: err,
				}
			case smCommandExecHeartBeat:
				if s.state != raft.State_StateLeader {
					cmd.cq <- smCommandRes{
						Rsp: nil,
						Err: nil,
					}
					break
				}
				indexVal := s.logMgr.GetLogIndexVal(cmd.ctx)
				s.asyncHeartBeat(cmd.ctx, &raft.AppendEntriesReq{
					Term:         s.md.get().Term,
					LeaderId:     s.mm.getMy(),
					PrevLogIndex: indexVal.lastLogIndex,
					PrevLogTerm:  indexVal.lastLogTerm,
					Entries:      nil,
					LeaderCommit: indexVal.lastCommitIndex,
				})
				cmd.cq <- smCommandRes{
					Rsp: nil,
					Err: nil,
				}
			case smCommandAppendCommands:
				err := s.execAppendCommandsFromLoop(cmd.ctx, cmd.req.(*raft.AppendCommandsReq), cmd.cq)
				if err != nil {
					cmd.cq <- smCommandRes{
						Rsp: nil,
						Err: err,
					}
				}
			case smCommandGetLeader:
				rsp, err := s.execGetLeaderFromLoop(cmd.ctx, cmd.req.(*raft.GetLeaderReq))
				cmd.cq <- smCommandRes{
					Rsp: rsp,
					Err: err,
				}
			case smCommandAsyncRequestVoteComp:
				err := s.execCommandAsyncRequestVoteCompFromLoop(cmd.ctx, cmd.anyReq.(*asyncVoteRes))
				if cmd.cq != nil {
					cmd.cq <- smCommandRes{
						Rsp: nil,
						Err: err,
					}
				}
			case smCommandInsertNoOp:
				err := s.insertNoOp(cmd.ctx)
				if err != nil {
					slog.Error("insert noop", slog.String("err", err.Error()), slog.String("my", s.mm.getMy()))
				}
			case smCommandAddServer:
				rsp, err := s.execAddServerFromLoop(cmd.ctx, cmd.req.(*raft.AddServerReq))
				cmd.cq <- smCommandRes{
					Rsp: rsp,
					Err: err,
				}
			case smCommandRemoveServer:
				rsp, err := s.execRemoveServerFromLoop(cmd.ctx, cmd.req.(*raft.RemoveServerReq))
				cmd.cq <- smCommandRes{
					Rsp: rsp,
					Err: err,
				}
			case smCommandApplyUserSmCb:
				s.handleApply2UserSmCb(cmd.anyReq.(applyUserSmCb))
			case smCommandTermLtEv:
				s.handleTermLtEv(cmd.anyReq.(termLtEv))
			case smCommandReplicateAppendEntriesCb:
				rsp, err := s.handleReplicateAppendEntries(cmd.ctx, cmd.anyReq.(followerAppendEntriesCb2))
				if cmd.cq != nil {
					cmd.cq <- smCommandRes{
						Err:    err,
						AnyRsp: rsp,
					}
				}
			}
		}
	}
}

func (s *CoreSm) handleCandidateTimeout(cmd smCommand) {
	var (
		enterCandidate bool
		err            error
	)
	if s.state == raft.State_StateFollower {
		now := time.Now()
		if now.Sub(s.lastLeaderHeartBeat) > time.Second {
			enterCandidate = true
		}
	} else if s.state == raft.State_StateCandidate {
		enterCandidate = true
	}
	if enterCandidate {
		s.state = raft.State_StateCandidate
		err = s.enterCandidate()
		if err != nil {
			slog.Error("request vote rpc",
				slog.String("err", err.Error()),
				slog.String("my", s.mm.getMy()),
				slog.String("member", fmt.Sprintf("%v", s.mm.getList())),
			)
		}
	}
	cmd.cq <- smCommandRes{
		Rsp: nil,
		Err: err,
	}
}

func (s *CoreSm) execAddServerFromLoop(ctx *context.Context, req *raft.AddServerReq) (rsp *raft.AddServerRsp, err error) {
	rsp = new(raft.AddServerRsp)
	if s.state != raft.State_StateLeader {
		err = emitErr(raft.ErrCode_ErrNoLeader,
			slog.Int("state", int(s.state)),
			slog.String("my", s.mm.getMy()),
		)
		return
	}
	memberList := s.mm.getList()
	my := s.mm.getMy()
	if req.NewServer == my || slices.Contains(memberList, req.NewServer) {
		err = emitErr(raft.ErrCode_ErrChangeServerExists)
		return
	}
	// TODO 完整的AddServer实现
	s.mm.append(req.NewServer)
	newMemberList := s.mm.getList()
	err = s.insertMemberChange(ctx, memberList, newMemberList)
	if err != nil {
		return
	}
	return
}

func (s *CoreSm) execRemoveServerFromLoop(ctx *context.Context, req *raft.RemoveServerReq) (rsp *raft.RemoveServerRsp, err error) {
	rsp = new(raft.RemoveServerRsp)
	if s.state != raft.State_StateLeader {
		err = emitErr(raft.ErrCode_ErrNoLeader,
			slog.Int("state", int(s.state)),
			slog.String("my", s.mm.getMy()),
		)
		return
	}
	memberList := s.mm.getList()
	my := s.mm.getMy()
	if req.OldServer != my && !slices.Contains(memberList, req.OldServer) {
		err = emitErr(raft.ErrCode_ErrChangeServerNotFound)
		return
	}
	// TODO 移除自身
	if req.OldServer == my {
		err = fmt.Errorf("not support remove leader")
		return
	}
	s.mm.remove(req.OldServer)
	newMemberList := s.mm.getList()
	err = s.insertMemberChange(ctx, memberList, newMemberList)
	if err != nil {
		return
	}
	return
}

func (s *CoreSm) insertNoOp(ctx *context.Context) error {
	return s.insertLog(ctx, raft.CommandType_NoOpCommand, []byte("raft-noop"))
}

func (s *CoreSm) insertMemberChange(ctx *context.Context, old []string, new []string) error {
	pbVal := &raft.LogMemberChange{
		Old: old,
		New: new,
	}
	marshalBytes, err := proto.Marshal(pbVal)
	if err != nil {
		return err
	}
	return s.insertLog(ctx, raft.CommandType_MemberChangeCommand, marshalBytes)
}

func (s *CoreSm) insertLog(ctx *context.Context, typ raft.CommandType, dat []byte) error {
	entries := []*raft.Entry{
		{
			CommandType: uint32(typ),
			Command:     dat,
		},
	}
	_, endLogIndex, err := s.logMgr.LeaderAppendLog(ctx, s.getTerm(), entries)
	if err != nil {
		return err
	}
	return s.newLog(ctx, endLogIndex)
}

func (s *CoreSm) execCommandAsyncRequestVoteCompFromLoop(ctx *context.Context, res *asyncVoteRes) error {
	if s.state != raft.State_StateCandidate {
		return nil
	}
	// NOTE: 插入no-op后会自动管理复制进度, 不需要在此处手动发心跳
	if res.VoteCount >= uint64(s.mm.quorum()) {
		s.state = raft.State_StateLeader
		err := s.createReplicateSm(ctx)
		if err != nil {
			panic("create replicate sm failed")
		}
		// 插入no-op日志
		s.q <- smCommand{
			typ:    smCommandInsertNoOp,
			ctx:    ctx,
			anyReq: res,
		}
	}
	return nil
}

func (s *CoreSm) newLog(ctx *context.Context, index uint64) error {
	return s.rsm.newLog(index, func(f *followerReplicationSm, start, end uint64) error {
		return s.asyncSendAppendEntries(ctx, f.addr, start, end, false)
	})
}

func (s *CoreSm) asyncSendAppendEntries(ctx *context.Context, member string, start, end uint64, isHeartbeat bool) error {
	indexVal := s.getLogMgr().indexVal
	term := s.getTerm()
	leader := s.mm.getMy()
	leaderCommit := indexVal.lastCommitIndex
	if leaderCommit == 0 {
		leaderCommit = indexVal.lastApplied
	}
	prevLogIndex := uint64(0)
	prevLogTerm := uint64(0)
	if start > 1 {
		prevLogIndex = start - 1
		prevLog, err := s.logMgr.ReadLog(ctx, prevLogIndex, true)
		if err != nil {
			return err
		}
		prevLogTerm = prevLog.Term
	}
	if isHeartbeat {
		req := &raft.AppendEntriesReq{
			Term:         term,
			LeaderId:     leader,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      nil,
			LeaderCommit: leaderCommit,
		}
		err := s.rsm.pushReq2Follower(member, ctx, req)
		if err != nil {
			return err
		}
	} else {
		go func(ctx *context.Context) {
			entries, err := s.logMgr.ReadLogWithIndex(ctx, start, end, false)
			if err != nil {
				s.pushCommand(smCommand{
					typ: smCommandReplicateAppendEntriesCb,
					ctx: ctx,
					anyReq: followerAppendEntriesCb2{
						Name: member,
						Ctx:  ctx,
						Err:  err,
					},
				})
				return
			}
			req := &raft.AppendEntriesReq{
				Term:         term,
				LeaderId:     leader,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			err = s.rsm.pushReq2Follower(member, ctx, req)
			if err != nil {
				slog.Error("pushReq2Follower", slog.String("err", err.Error()))
			}
		}(ctx.Clone())
	}
	return nil
}

func (s *CoreSm) createReplicateSm(ctx *context.Context) error {
	s.rsm = newReplicationSm(s)
	indexVal := s.logMgr.GetLogIndexVal(context.Background())
	//s.rsm.newLog(indexVal.lastLogIndex)
	//for _, member := range s.mm.getList() {
	//	s.rsm.addFollower(member, &followerReplicationSm{
	//		state:      replicationSmStateProbe,
	//		matchIndex: 0,
	//		nextIndex:  indexVal.lastApplied,
	//	})
	//	err := s.asyncSendAppendEntries(ctx, member, indexVal.lastApplied, indexVal.lastApplied, true)
	//	if err != nil {
	//		return err
	//	}
	//}
	for _, member := range s.mm.getList() {
		if member != s.mm.getMy() {
			s.rsm.addFollower(member, indexVal.lastLogIndex)
		}
	}
	return s.rsm.newLog(indexVal.lastLogIndex, func(f *followerReplicationSm, start, end uint64) error {
		return s.asyncSendAppendEntries(ctx, f.addr, start, end, true)
	})
}

func (s *CoreSm) enterCandidate() error {
	s.state = raft.State_StateCandidate
	_, err := s.md.termIncr()
	if err != nil {
		return err
	}
	ctx := context.Background()
	err = s.md.save(metaData{
		VoteFor: s.mm.getMy(),
		Term:    s.md.get().Term,
	})
	if err != nil {
		return err
	}
	indexVal := s.logMgr.GetLogIndexVal(ctx)
	req := &raft.RequestVoteReq{
		Term:         s.md.get().Term,
		CandiDateId:  s.mm.getMy(),
		LastLogIndex: indexVal.lastLogIndex,
		LastLogTerm:  indexVal.lastLogTerm,
	}
	s.asyncRequestVote(ctx, req, func(ctx *context.Context, res *asyncVoteRes) {
		s.q <- smCommand{
			typ:    smCommandAsyncRequestVoteComp,
			ctx:    ctx,
			anyReq: res,
		}
	})
	return nil
}

func (s *CoreSm) insertCommands(ctx *context.Context, cq chan smCommandRes, commands [][]byte) error {
	const OneMaxCount = 100
	if !s.myIsLeader() {
		return emitErr(raft.ErrCode_ErrNoLeader,
			slog.Int("state", int(s.state)),
			slog.String("my", s.mm.getMy()),
		)
	}
	holdReq := &holdAppendCommandsReq{
		ctx:     ctx,
		cq:      cq,
		logTerm: s.md.get().Term,
	}
	for len(commands) > 0 {
		count := OneMaxCount
		if len(commands) < OneMaxCount {
			count = len(commands)
		}
		entries := make([]*raft.Entry, 0, count)
		for _, cmd := range commands[:count] {
			entries = append(entries, &raft.Entry{
				CommandType: uint32(raft.CommandType_UserCommand),
				Command:     cmd,
			})
		}
		commands = commands[count:]
		startLogIndex, endLogIndex, err := s.logMgr.LeaderAppendLog(ctx, s.md.get().Term, entries)
		if err != nil {
			return err
		}
		if holdReq.startLogIndex == 0 {
			holdReq.startLogIndex = startLogIndex
		}
		if endLogIndex > holdReq.endLogIndex {
			holdReq.endLogIndex = endLogIndex
		}
		err = s.newLog(ctx, endLogIndex)
		if err != nil {
			return err
		}
	}
	if cq != nil {
		s.holdList = append(s.holdList, holdReq)
		slices.SortFunc(s.holdList, func(a, b *holdAppendCommandsReq) int {
			return cmp.Compare(a.endLogIndex, b.endLogIndex)
		})
	}
	return nil
}

func (s *CoreSm) execAppendCommandsFromLoop(ctx *context.Context, req *raft.AppendCommandsReq, cq chan smCommandRes) error {
	return s.insertCommands(ctx, cq, req.Commands)
}

func (s *CoreSm) execGetLeaderFromLoop(ctx *context.Context, req *raft.GetLeaderReq) (rsp *raft.GetLeaderRsp, err error) {
	if s.state != raft.State_StateLeader {
		err = emitErr(raft.ErrCode_ErrNoLeader, s.mm.getMy())
		return
	}
	rsp = &raft.GetLeaderRsp{
		LeaderId: s.mm.getMy(),
		LeaderIp: s.mm.getMy(),
	}
	return rsp, nil
}

func (s *CoreSm) execRequestVoteFromLoop(ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	if s.mm.getMy() == req.CandiDateId {
		err = emitErr(raft.ErrCode_ErrInvalidRequest, "vote request")
		return
	}
	rsp = &raft.RequestVoteRsp{}
	md := *s.md.get()
	// 小于当前节点任期的请求直接拒绝
	if req.Term < md.Term {
		rsp.VoteGranted = false
		rsp.Term = md.Term
		return
	}
	if req.Term == md.Term {
		if md.VoteFor != "" {
			// 回复投票结果, 已经在该任期投过票给x节点
			if md.VoteFor == req.CandiDateId {
				rsp.VoteGranted = true
				rsp.Term = md.Term
				return rsp, nil
			}
			rsp.VoteGranted = false
			rsp.Term = md.Term
			return rsp, nil
		}
	}
	indexVal := s.logMgr.GetLogIndexVal(ctx)
	md.Term = req.Term
	rsp.Term = md.Term
	// 必须执行日志新旧检查
	if req.LastLogTerm > indexVal.lastLogTerm {
		rsp.VoteGranted = true
	} else if req.LastLogTerm == indexVal.lastLogTerm && req.LastLogIndex >= indexVal.lastLogIndex {
		rsp.VoteGranted = true
	}
	if rsp.VoteGranted {
		s.state = raft.State_StateFollower
		s.tt.ResetTicker(candidateTicker)
		md.VoteFor = req.CandiDateId
		err = s.md.save(md)
		if err != nil {
			return rsp, fmt.Errorf("save md failed: %v", err)
		}
	}
	return
}

func (s *CoreSm) appendEntriesCheck(ctx *context.Context, req *raft.AppendEntriesReq) (next bool, rsp *raft.AppendEntriesRsp, err error) {
	rsp = new(raft.AppendEntriesRsp)
	myTerm := s.getTerm()
	if req.Term > myTerm {
		s.state = raft.State_StateFollower
		err = s.md.save(metaData{
			VoteFor: req.LeaderId,
			Term:    req.Term,
		})
		if err != nil {
			return
		}
	} else if myTerm > req.Term {
		rsp.Success = false
		rsp.Term = myTerm
		return
	} else if myTerm == req.Term {
		s.state = raft.State_StateFollower
	}
	myTerm = s.getTerm()
	s.lastLeaderHeartBeat = time.Now()
	s.tt.ResetTicker(candidateTicker)
	s.mm.setLeader(req.LeaderId)
	rsp.Term = myTerm
	next = true
	return
}

func (s *CoreSm) execLeaderHeartBeatFromLoop(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	//slog.Debug("leader heartbeat", slog.String("leader", req.LeaderId), slog.String("my", s.rpc.My))
	var next bool
	next, rsp, err = s.appendEntriesCheck(ctx, req)
	if err != nil || !next {
		return
	}
	var (
		firstEntry, lastEntry, prevLogEntry *raft.Entry
	)
	firstEntry, err = s.logMgr.First(ctx, true)
	if err != nil {
		return
	}
	lastEntry, err = s.logMgr.Last(ctx, true)
	if err != nil {
		return
	}
	if firstEntry == nil || lastEntry == nil {
		rsp.Success = false
		rsp.ConflictIndex = req.PrevLogIndex
		rsp.ConflictTerm = req.PrevLogTerm
		return
	} else if firstEntry.LogIndex <= req.PrevLogIndex && req.PrevLogIndex <= lastEntry.LogIndex {
		prevLogEntry, err = s.logMgr.ReadLog(ctx, req.PrevLogIndex, true)
		if err != nil {
			return
		}
		if prevLogEntry.Term != req.PrevLogTerm {
			err = s.logMgr.Truncate(ctx, prevLogEntry.LogIndex-1)
			if err != nil {
				return
			}
			rsp.Success = false
			rsp.ConflictIndex = prevLogEntry.LogIndex
			rsp.ConflictTerm = prevLogEntry.Term
			return
		} else {
			rsp.Success = true
		}
	} else {
		rsp.Success = false
		rsp.ConflictIndex = lastEntry.LogIndex
		rsp.ConflictTerm = lastEntry.Term
		return
	}

	if s.state == raft.State_StateFollower {
		s.aw.asyncApply(applyUserSmReq{
			Ctx:        ctx.Clone(),
			StartIndex: 0,
			EndIndex:   min(lastEntry.LogIndex, req.LeaderCommit),
		})
	}

	return rsp, nil
}

func (s *CoreSm) execAppendEntriesFromLoop(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	slog.Info("leader append entries", slog.String("leader", req.LeaderId), slog.String("my", s.mm.getMy()))
	var next bool
	next, rsp, err = s.appendEntriesCheck(ctx, req)
	if err != nil || !next {
		return
	}
	var (
		firstEntry, lastEntry *raft.Entry
	)
	firstEntry, err = s.logMgr.First(ctx, true)
	if err != nil {
		return
	}
	lastEntry, err = s.logMgr.Last(ctx, true)
	if err != nil {
		return
	}
	if firstEntry == nil || lastEntry == nil {
		if req.PrevLogIndex > 0 {
			rsp.Success = false
			rsp.ConflictIndex = 1
			return
		}
		_, _, err = s.logMgr.FollowerAppendLog(ctx, req.Entries)
		if err != nil {
			return
		}
		rsp.Success = true
		s.logMgr.SetCommitIndex(min(s.logMgr.GetLogIndexVal(ctx).lastLogIndex, req.LeaderCommit))
		return
	}
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex < firstEntry.LogIndex {
			rsp.Success = false
			rsp.ConflictIndex = firstEntry.LogIndex
			rsp.ConflictTerm = firstEntry.Term
			return
		} else if req.PrevLogIndex > lastEntry.LogIndex {
			rsp.Success = false
			rsp.ConflictIndex = lastEntry.LogIndex
			rsp.ConflictTerm = lastEntry.Term
			return
		}
		var (
			prevLogEntry *raft.Entry
		)
		prevLogEntry, err = s.logMgr.ReadLog(ctx, req.PrevLogIndex, true)
		if err != nil {
			return
		}
		if prevLogEntry.Term != req.PrevLogTerm {
			rsp.Success = false
			rsp.ConflictIndex = prevLogEntry.LogIndex
			rsp.ConflictTerm = prevLogEntry.Term
			return
		}
	}
	leaderFirstLog := req.Entries[0].LogIndex
	leaderLastLog := req.Entries[len(req.Entries)-1].LogIndex
	newCommit := max(s.logMgr.GetLogIndexVal(ctx).lastCommitIndex, min(req.LeaderCommit, leaderLastLog))
	if firstEntry.LogIndex <= leaderLastLog && leaderFirstLog <= lastEntry.LogIndex {
		if leaderLastLog > lastEntry.LogIndex {
			leaderLastLog = lastEntry.LogIndex
		}
		var leaderCmpLog []*raft.Entry
		leaderCmpLog, err = s.logMgr.ReadLogWithIndex(ctx, leaderFirstLog, leaderLastLog, true)
		if err != nil {
			return
		}
		for i, v := range leaderCmpLog {
			if req.Entries[i].Term != v.Term {
				err = s.logMgr.Truncate(ctx, v.LogIndex-1)
				if err != nil {
					return
				}
				leaderCmpLog = leaderCmpLog[:i]
				break
			}
		}
		req.Entries = req.Entries[len(leaderCmpLog):]
	}
	if len(req.Entries) > 0 {
		_, _, err = s.logMgr.FollowerAppendLog(ctx, req.Entries)
		if err != nil {
			return
		}
	}
	s.logMgr.SetCommitIndex(newCommit)
	s.aw.asyncApply(applyUserSmReq{
		Ctx:      ctx.Clone(),
		EndIndex: newCommit,
	})
	rsp.Success = true
	return
}

func (s *CoreSm) pushApply2UserSmCb(rsp applyUserSmCb) {
	s.q <- smCommand{
		typ:    smCommandApplyUserSmCb,
		ctx:    context.Background(),
		anyReq: rsp,
	}
}

func (s *CoreSm) matchIndexChangeEv(ev leaderMatchIndexChangeEv) {
	if !s.myIsLeader() {
		slog.Warn("my is not leader, ignore apply usersm",
			slog.Uint64("old", ev.Old),
			slog.Uint64("new", ev.New),
		)
		return
	}
	s.logMgr.SetCommitIndex(ev.New)
	s.aw.asyncApply(applyUserSmReq{
		Ctx:      context.Background(),
		EndIndex: ev.New,
	})
}

func (s *CoreSm) pushTermLtEv(ev termLtEv) {
	s.q <- smCommand{
		typ:    smCommandTermLtEv,
		ctx:    context.Background(),
		anyReq: ev,
	}
}

func (s *CoreSm) handleApply2UserSmCb(cb applyUserSmCb) {
	if !s.myIsLeader() {
		slog.Warn("my is not leader, ignore handleApply2UserSmCb")
		return
	}
	succHold := make([]*holdAppendCommandsReq, 0, 16)
	for _, v := range s.holdList {
		if cb.Req.EndIndex >= v.endLogIndex {
			succHold = append(succHold, v)
		} else {
			break
		}
	}
	for _, h := range succHold {
		select {
		case h.cq <- smCommandRes{
			Rsp: &raft.AppendCommandsRsp{
				LastLogIndex: h.endLogIndex,
				LastLogTerm:  h.logTerm,
			},
			Err: cb.Error,
		}:
			break
		default:
			slog.Warn("cq hang",
				slog.Uint64("startLogIndex", h.startLogIndex),
				slog.Uint64("endLogIndex", h.endLogIndex),
			)
		}

	}
	s.holdList = s.holdList[len(succHold):]
}

func (s *CoreSm) leaderTermTermination() {
	s.state = raft.State_StateFollower
}

func (s *CoreSm) handleTermLtEv(ev termLtEv) {
	if !s.myIsLeader() {
		slog.Warn("my is not leader, ignore handleTermLtEv")
		return
	}
	myTerm := s.md.get().Term
	if ev.Term > myTerm {
		s.leaderTermTermination()
	}
}

func (s *CoreSm) asyncHeartBeat(ctx *context.Context, req *raft.AppendEntriesReq) {
	if !s.myIsLeader() {
		return
	}
	for _, v := range s.mm.getList() {
		if v != s.mm.getMy() {
			go func(member string) {
				rsp, err := s.rpcCli.AppendEntries(ctx, req, client.WithAddr(member))
				if err != nil {
					slog.Error("rpcCli.AppendEntries failed",
						slog.String("member", member),
						slog.String("action", "HeartBeat"),
						slog.String("err", err.Error()),
					)
				}
				if rsp.Term > req.Term {
					s.pushTermLtEv(termLtEv{
						Term: rsp.Term,
						Addr: member,
					})
				}
			}(v)
		}
	}
}

func (s *CoreSm) asyncRequestVote(ctx *context.Context, req *raft.RequestVoteReq, cb func(ctx *context.Context, res *asyncVoteRes)) {
	ctx = ctx.Clone()
	quorum := s.mm.quorum()
	memberList := s.mm.getList()
	go func(ctx *context.Context) {
		var (
			voteCount atomic.Uint64
			errCount  atomic.Uint64
			succCount atomic.Uint64
		)
		voteCount.Add(1)
		for _, member := range memberList {
			ctx2 := ctx.Clone()
			go func(ctx *context.Context, member string) {
				defer func() {
					if r := recover(); r != nil {
						errCount.Add(1)
					}
				}()
				rsp, err := s.rpcCli.RequestVote(ctx, req, client.WithAddr(member))
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
					slog.String("my", s.mm.getMy()),
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
				if voteCount.Load() >= uint64(quorum) || errCount.Load()+succCount.Load() == uint64(len(memberList)) {
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
	}(ctx.Clone())
}

func (s *CoreSm) handleReplicateAppendEntries(ctx *context.Context, anyReq followerAppendEntriesCb2) (rsp followerAppendEntriesCmdRes, err error) {
	// 出错了需要回退并重试
	if anyReq.Err != nil {
		rsp.Retry = true
		return
	}
	var next bool
	next, err = s.rsm.handleFollowerAppendEntriesCb(anyReq.Name, anyReq.Ctx, anyReq.Req, anyReq.Rsp)
	if err != nil {
		slog.Error("handleFollowerAppendEntriesCb", slog.String("err", err.Error()))
		return
	}
	// 完成hold的请求
	if next {
		// 并未结束, 接着同步进度
		nextStart, nextEnd := s.rsm.followerNextIndex(anyReq.Name)
		err = s.asyncSendAppendEntries(context.Background(), anyReq.Name, nextStart, nextEnd, false)
		if err != nil {
			slog.Error(err.Error())
		}
	}
	return
}
