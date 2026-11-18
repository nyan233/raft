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

type termLtEv struct {
	Term uint64
	Addr string
}

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
	ReqTerm   uint64
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
	Me            string
	Membership    []string
	LogDir        string
	LogName       string
	UserSmFactory func() StateMachine
}

type heartbeatTask struct {
	csm *CoreSm
	// TODO 如果系统时区因为配置的原因怎么解决？
	start    time.Time
	ctx      *context.Context
	cancelFn func()
	id       string
	req      *raft.AppendEntriesReq
	// 这里的member不包含自己
	memberList []string
	quorum     int
	succ       atomic.Uint64
	fail       atomic.Uint64
}

func newHeartbeatTask(csm *CoreSm, ctx *context.Context, req *raft.AppendEntriesReq) *heartbeatTask {
	newCtx, cancelFn := context.WithCancel(ctx)
	return &heartbeatTask{
		csm:        csm,
		ctx:        newCtx,
		cancelFn:   cancelFn,
		id:         genID(),
		req:        req,
		memberList: csm.mm.getListNotMe(),
		quorum:     csm.mm.quorum() - 1,
	}
}

func (t *heartbeatTask) cancelReq() {
	t.cancelFn()
}

func (t *heartbeatTask) run() {
	t.start = time.Now()
	for _, v := range t.memberList {
		go func(ctx *context.Context, member string) {
			rsp, err := t.csm.rpcCli.AppendEntries(ctx, t.req, client.WithAddr(member))
			if err != nil {
				slog.Error("rpcCli.AppendEntries failed",
					slog.String("member", member),
					slog.String("action", "HeartBeat"),
					slog.String("err", err.Error()),
				)
				t.fail.Add(1)
				return
			}
			t.succ.Add(1)
			if rsp.Term > t.req.Term {
				t.csm.pushTermLtEv(ctx, termLtEv{
					Term: rsp.Term,
					Addr: member,
				})
			}
		}(t.ctx.Clone(), v)
	}
}

func (t *heartbeatTask) check() int {
	// 心跳成功的follower数量已达成多数派, 不管总体完成与否
	if t.succ.Load() >= uint64(t.quorum) {
		return 1
	}
	// 是否已完成所有发送任务
	if t.succ.Load()+t.fail.Load() >= uint64(len(t.memberList)) {
		if t.succ.Load() >= uint64(t.quorum) {
			return 1
		}
		return 0
	}
	// 卡住1s视为超时, 这期间即为达成多数派也没有完成发送任务
	now := time.Now()
	if now.Sub(t.start).Seconds() > 1 {
		return 0
	}
	return -1
}

type requestVoteTask struct {
	csm        *CoreSm
	ctx        *context.Context
	id         string
	start      time.Time
	req        *raft.RequestVoteReq
	reqTerm    uint64
	memberList []string
	quorum     int
	succ       atomic.Uint64
	voteCount  atomic.Uint64
	fail       atomic.Uint64
	callback   func(ctx *context.Context, t *requestVoteTask)
	res        *asyncVoteRes
}

func newRequestVoteTask(csm *CoreSm, ctx *context.Context, req *raft.RequestVoteReq, cb func(ctx *context.Context, t *requestVoteTask)) *requestVoteTask {
	return &requestVoteTask{
		csm:        csm,
		ctx:        ctx,
		id:         genID(),
		req:        req,
		reqTerm:    req.Term,
		memberList: csm.mm.getListNotMe(),
		quorum:     csm.mm.quorum() - 1,
		callback:   cb,
	}
}

func (t *requestVoteTask) run() {
	t.start = time.Now()
	go func() {
		for _, v := range t.memberList {
			go func(ctx *context.Context, member string) {
				defer func() {
					if r := recover(); r != nil {
						t.fail.Add(1)
					}
				}()
				rsp, err := t.csm.rpcCli.RequestVote(ctx, t.req, client.WithAddr(member))
				if err != nil {
					t.fail.Add(1)
				} else {
					if rsp.VoteGranted {
						t.voteCount.Add(1)
					}
					t.succ.Add(1)
					if rsp.Term > t.reqTerm {
						t.csm.pushTermLtEv(ctx, termLtEv{
							Term: rsp.Term,
							Addr: member,
						})
					}
				}
				var errStr = "nil"
				if err != nil {
					errStr = err.Error()
				}
				reqBytes, _ := json.Marshal(t.req)
				rspBytes, _ := json.Marshal(rsp)
				slog.Info("rpc request vote",
					slog.String("member", member),
					slog.String("req", string(reqBytes)),
					slog.String("rsp", string(rspBytes)),
					slog.String("err", errStr))
			}(t.ctx.Clone(), v)
		}
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		timeout := time.NewTimer(time.Second * 30)
		defer timeout.Stop()
		for {
			select {
			case <-ticker.C:
				if t.voteCount.Load() >= uint64(t.quorum) || t.fail.Load()+t.succ.Load() == uint64(len(t.memberList)) {
					t.res = &asyncVoteRes{
						ReqTerm:   t.reqTerm,
						ReqCount:  int(t.fail.Load() + t.succ.Load()),
						VoteCount: t.voteCount.Load(),
						IsTimeout: false,
					}
					t.callback(t.ctx, t)
					return
				}
			case <-timeout.C:
				t.res = &asyncVoteRes{
					ReqTerm:   t.reqTerm,
					ReqCount:  int(t.fail.Load() + t.succ.Load()),
					VoteCount: t.voteCount.Load(),
					IsTimeout: true,
				}
				t.callback(t.ctx, t)
				return
			}
		}
	}()
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
	diskMM              *membership
	rpcCli              raft.RaftProxy
	rsm                 *replicationSm
	ht                  *heartbeatTask
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
	s.diskMM = newMembership(filepath.Join(s.cfg.LogDir, s.cfg.LogName+".mm"))
	err = s.diskMM.init()
	if err != nil {
		return err
	}
	if len(s.diskMM.get().List) == 0 {
		err = s.diskMM.save(membershipData{
			Me:       s.cfg.Me,
			LogIndex: 0,
			Term:     0,
			List:     s.cfg.Membership,
		})
		if err != nil {
			return err
		}
	} else {
		s.cfg.Me = s.diskMM.get().Me
		s.cfg.Membership = append([]string{}, s.diskMM.get().List...)
	}
	if !slices.Contains(s.cfg.Membership, s.cfg.Me) {
		return fmt.Errorf("Me(%s) not in membership", s.cfg.Me)
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
	s.mm.intiMember(s.cfg.Me, s.cfg.Membership)
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
		server.WithAddressServer(s.mm.getMe()),
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
				s.handleExecHeartBeat(cmd)
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
				err := s.execCommandAsyncRequestVoteCompFromLoop(cmd.ctx, cmd.anyReq.(*requestVoteTask))
				if cmd.cq != nil {
					cmd.cq <- smCommandRes{
						Rsp: nil,
						Err: err,
					}
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

func (s *CoreSm) handleExecHeartBeat(cmd smCommand) {
	if s.state != raft.State_StateLeader {
		cmd.cq <- smCommandRes{
			Rsp: nil,
			Err: nil,
		}
		return
	}
	indexVal := s.logMgr.GetLogIndexVal(cmd.ctx)
	if s.ht != nil {
		res := s.ht.check()
		// 心跳没获得多数派响应, 主动退位
		switch res {
		case 0:
			err := s.leaderStepDown("term_current", s.getTerm())
			if err != nil {
				slog.Error("leaderStepDown", slog.String("err", err.Error()))
			}
		case 1:
			s.ht.cancelReq()
			s.ht = nil
		}
	} else {
		s.ht = newHeartbeatTask(s, cmd.ctx, &raft.AppendEntriesReq{
			Term:         s.md.get().Term,
			LeaderId:     s.mm.getMe(),
			PrevLogIndex: indexVal.lastLogIndex,
			PrevLogTerm:  indexVal.lastLogTerm,
			Entries:      nil,
			LeaderCommit: indexVal.lastCommitIndex,
		})
		s.ht.run()
	}
	cmd.cq <- smCommandRes{
		Rsp: nil,
		Err: nil,
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
				slog.String("Me", s.mm.getMe()),
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
			slog.String("Me", s.mm.getMe()),
		)
		return
	}
	memberList := s.mm.getList()
	my := s.mm.getMe()
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
			slog.String("Me", s.mm.getMe()),
		)
		return
	}
	memberList := s.mm.getList()
	my := s.mm.getMe()
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

func (s *CoreSm) execCommandAsyncRequestVoteCompFromLoop(ctx *context.Context, t *requestVoteTask) error {
	if s.state != raft.State_StateCandidate {
		return nil
	}
	// 不是当前任期的投票结果则忽略掉
	myTerm := s.getTerm()
	res := t.res
	if res.ReqTerm != myTerm {
		slog.Warn("ignore request vote res, reason not is current term",
			slog.Uint64("req_term", res.ReqTerm),
			slog.Uint64("current_term", myTerm),
		)
		return nil
	}
	// NOTE: 插入no-op后会自动管理复制进度, 不需要在此处手动发心跳
	if res.VoteCount >= uint64(t.quorum) {
		s.state = raft.State_StateLeader
		err := s.createReplicateSm(ctx)
		if err != nil {
			panic("create replicate sm failed")
		}
		// 插入no-op日志
		err = s.insertNoOp(ctx)
		if err != nil {
			// slog.Error("insert noop", slog.String("err", err.Error()), slog.String("Me", s.mm.getMe()))
			panic("insert no-op failed, err=" + err.Error())
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
	leader := s.mm.getMe()
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
		// TODO 如何实现异步file read?file io卡住主循环效率不高
		entries, err := s.logMgr.ReadLogWithIndex(ctx, start, end, false)
		if err != nil {
			panic(fmt.Sprintf("node(%s) log(%d-%d) read error: %v", s.mm.getMe(), start, end, err))
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
	}
	return nil
}

func (s *CoreSm) createReplicateSm(ctx *context.Context) error {
	s.rsm = newReplicationSm(s)
	indexVal := s.logMgr.GetLogIndexVal(ctx)
	for _, member := range s.mm.getList() {
		if member != s.mm.getMe() {
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
		VoteFor: s.mm.getMe(),
		Term:    s.md.get().Term,
	})
	if err != nil {
		return err
	}
	indexVal := s.logMgr.GetLogIndexVal(ctx)
	req := &raft.RequestVoteReq{
		Term:         s.md.get().Term,
		CandiDateId:  s.mm.getMe(),
		LastLogIndex: indexVal.lastLogIndex,
		LastLogTerm:  indexVal.lastLogTerm,
	}
	s.asyncRequestVote(ctx, req, func(ctx *context.Context, t *requestVoteTask) {
		s.q <- smCommand{
			typ:    smCommandAsyncRequestVoteComp,
			ctx:    ctx,
			anyReq: t,
		}
	})
	return nil
}

func (s *CoreSm) insertCommands(ctx *context.Context, cq chan smCommandRes, commands [][]byte) error {
	const OneMaxCount = 100
	if !s.myIsLeader() {
		return emitErr(raft.ErrCode_ErrNoLeader,
			slog.Int("state", int(s.state)),
			slog.String("Me", s.mm.getMe()),
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
		err = emitErr(raft.ErrCode_ErrNoLeader, s.mm.getMe())
		return
	}
	rsp = &raft.GetLeaderRsp{
		LeaderId: s.mm.getMe(),
		LeaderIp: s.mm.getMe(),
	}
	return rsp, nil
}

func (s *CoreSm) execRequestVoteFromLoop(ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	if s.mm.getMe() == req.CandiDateId {
		err = emitErr(raft.ErrCode_ErrInvalidRequest, "vote request")
		return
	}
	// 当前已是有效的leader, 不再接收投票请求
	if s.state == raft.State_StateLeader {
		err = emitErr(raft.ErrCode_ErrInvalidRequest, "already is leader")
		return
	}
	rsp = &raft.RequestVoteRsp{}
	md := *s.md.get()
	// 小于当前节点任期的请求直接拒绝
	if req.Term < md.Term {
		rsp.VoteGranted = false
		rsp.Term = md.Term
		return
	} else if req.Term > md.Term {
		// 请求任期大于节点任期时需要保存新任期
		s.state = raft.State_StateFollower
		md.Term = req.Term
		md.VoteFor = ""
	} else if req.Term == md.Term {
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
	}
	err = s.md.save(md)
	if err != nil {
		return rsp, fmt.Errorf("save md failed: %v", err)
	}
	return
}

func (s *CoreSm) appendEntriesCheck(ctx *context.Context, req *raft.AppendEntriesReq) (next bool, rsp *raft.AppendEntriesRsp, err error) {
	rsp = new(raft.AppendEntriesRsp)
	myTerm := s.getTerm()
	if req.Term > myTerm {
		err = s.termChange(req.Term)
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
	//slog.Debug("leader heartbeat", slog.String("leader", req.LeaderId), slog.String("Me", s.rpc.Me))
	var next bool
	next, rsp, err = s.appendEntriesCheck(ctx, req)
	if err != nil || !next {
		return
	}
	if req.PrevLogIndex == 0 && req.PrevLogTerm == 0 {
		rsp.Success = true
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
			rsp.Success = false
			rsp.ConflictIndex = prevLogEntry.LogIndex
			rsp.ConflictTerm = prevLogEntry.Term
		} else {
			rsp.Success = true
		}
	} else {
		rsp.Success = false
		rsp.ConflictIndex = lastEntry.LogIndex
		rsp.ConflictTerm = lastEntry.Term
	}
	if rsp.Success && s.state == raft.State_StateFollower {
		oldCommitIndex := s.logMgr.GetCommitIndex()
		newCommitIndex := max(oldCommitIndex, min(req.PrevLogIndex, req.LeaderCommit))
		if newCommitIndex > oldCommitIndex {
			s.logMgr.SetCommitIndex(newCommitIndex)
			s.aw.asyncApply(applyUserSmReq{
				Ctx:        ctx.Clone(),
				StartIndex: 0,
				EndIndex:   newCommitIndex,
			})
		}
	}

	return rsp, nil
}

func (s *CoreSm) execAppendEntriesFromLoop(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	slog.Info("leader append entries", slog.String("leader", req.LeaderId), slog.String("Me", s.mm.getMe()))
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
	var (
		oldCommit = s.logMgr.GetCommitIndex()
		newCommit uint64
	)
	if firstEntry == nil || lastEntry == nil {
		if req.PrevLogIndex > 0 {
			rsp.Success = false
			rsp.ConflictIndex = 1
			return
		}
		var end uint64
		_, end, err = s.logMgr.FollowerAppendLog(ctx, req.Entries)
		if err != nil {
			return
		}
		rsp.Success = true
		newCommit = max(oldCommit, min(end, req.LeaderCommit))
	} else {
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
		newCommit = max(oldCommit, min(req.LeaderCommit, leaderLastLog))
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
		rsp.Success = true
		if len(req.Entries) > 0 {
			_, _, err = s.logMgr.FollowerAppendLog(ctx, req.Entries)
			if err != nil {
				return
			}
		}
	}
	if newCommit > oldCommit {
		s.logMgr.SetCommitIndex(newCommit)
		s.aw.asyncApply(applyUserSmReq{
			Ctx:      ctx.Clone(),
			EndIndex: newCommit,
		})
	}
	return
}

func (s *CoreSm) pushApply2UserSmCb(rsp applyUserSmCb) {
	s.q <- smCommand{
		typ:    smCommandApplyUserSmCb,
		ctx:    context.Background(),
		anyReq: rsp,
	}
}

func (s *CoreSm) pushTermLtEv(ctx *context.Context, ev termLtEv) {
	s.q <- smCommand{
		typ:    smCommandTermLtEv,
		ctx:    ctx,
		anyReq: ev,
	}
}

func (s *CoreSm) handleApply2UserSmCb(cb applyUserSmCb) {
	if !s.myIsLeader() {
		slog.Warn("Me is not leader, ignore handleApply2UserSmCb")
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

// leader退位
func (s *CoreSm) leaderStepDown(reason string, term uint64) error {
	if s.state == raft.State_StateFollower {
		return nil
	} else if s.state == raft.State_StateCandidate {
		s.state = raft.State_StateFollower
		s.tt.ResetTicker(candidateTicker)
		return nil
	}
	s.state = raft.State_StateFollower
	switch reason {
	case "term_gt":
		err := s.md.save(metaData{
			VoteFor: "",
			Term:    term,
		})
		if err != nil {
			return err
		}
	case "term_current":
		break
	}
	// 清理心跳任务
	if s.ht != nil {
		s.ht.cancelReq()
		s.ht = nil
	}
	// 退位时清理等待的请求
	for len(s.holdList) > 0 {
		holdReq := s.holdList[0]
		holdReq.cq <- smCommandRes{
			Err: emitErr(raft.ErrCode_ErrLeaderStepDown, reason, term),
		}
		s.holdList = s.holdList[1:]
	}
	// 停止复制状态机
	s.rsm.cleanFollowers()
	s.rsm = nil
	s.tt.ResetTicker(candidateTicker)
	return nil
}

func (s *CoreSm) handleTermLtEv(ev termLtEv) {
	myTerm := s.md.get().Term
	if ev.Term <= myTerm {
		slog.Warn("onclick term_lt ev, but current Term >= ev.Term",
			slog.Uint64("term_lt", ev.Term),
			slog.Uint64("my_term", myTerm),
		)
		return
	}
	err := s.termChange(ev.Term)
	if err != nil {
		slog.Error("termChange", slog.String("err", err.Error()))
	}
}

func (s *CoreSm) asyncRequestVote(ctx *context.Context, req *raft.RequestVoteReq, cb func(ctx *context.Context, t *requestVoteTask)) {
	t := newRequestVoteTask(s, ctx, req, cb)
	t.run()
}

func (s *CoreSm) termChange(term uint64) error {
	switch s.state {
	case raft.State_StateFollower:
		err := s.md.save(metaData{
			VoteFor: "",
			Term:    term,
		})
		if err != nil {
			slog.Error("save meta data", slog.String("err", err.Error()))
			return err
		}
	case raft.State_StateCandidate:
		s.state = raft.State_StateFollower
		err := s.md.save(metaData{
			VoteFor: "",
			Term:    term,
		})
		if err != nil {
			slog.Error("save meta data", slog.String("err", err.Error()))
			return err
		}
	case raft.State_StateLeader:
		err := s.leaderStepDown("term_gt", term)
		if err != nil {
			slog.Error("leaderStepDown", slog.String("err", err.Error()))
			return err
		}
	}
	return nil
}

func (s *CoreSm) handleReplicateAppendEntries(ctx *context.Context, anyReq followerAppendEntriesCb2) (rsp followerAppendEntriesCmdRes, err error) {
	// 出错了需要回退并重试
	if anyReq.Rsp != nil && (anyReq.Rsp.Term > s.getTerm()) {
		// 请求结果返回了更高的term则需要更新本地的term并清空votefor
		err = s.termChange(anyReq.Rsp.Term)
		return
	}

	if s.myIsLeader() {
		// 非本任期请求
		// TODO 不调整term的情况下退位的leader怎么处理？当前已是leader, 其请求的任期一致可以接受旧任期请求？
		if anyReq.Req.Term < s.getTerm() {
			rsp.Retry = false
			err = emitErr(raft.ErrCode_ErrLeaderStepDown)
			return
		}
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
	} else {
		// leader已经退位
		rsp.Retry = false
		err = emitErr(raft.ErrCode_ErrLeaderStepDown)
		return
	}

	return
}
