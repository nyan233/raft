package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"path/filepath"
	"slices"
	"time"

	"github.com/nyan233/littlerpc/core/common/context"
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
	smCommandAsyncAppendEntriesComp
	smCommandInsertNoOp
	smCommandAddServer
	smCommandRemoveServer
)

const (
	candidateTicker = "candidateTicker"
	heartbeatTicker = "heartbeatTicker"
)

type smCommandRes struct {
	Rsp proto.Message
	Err error
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
	rpc                 *coreRpc
	md                  *meta
	tt                  *timeoutTicker
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
	s.rpc, err = newCoreRpc(s, s.cfg.My, s.cfg.Membership)
	if err != nil {
		return err
	}
	s.logMgr = newRaftLogManager(s.cfg.LogDir, s.cfg.LogName, s.cfg.UserSmFactory())
	err = s.logMgr.init()
	if err != nil {
		return err
	}
	go s.startLoop()
	s.startTimeoutTicker()
	return nil
}

func (s *CoreSm) myIsLeader() bool {
	return s.state == raft.State_StateLeader && s.rpc.My == s.rpc.Leader
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
							slog.String("my", s.rpc.My),
							slog.String("member", fmt.Sprintf("%v", s.rpc.Membership)),
						)
					}
					cmd.cq <- smCommandRes{
						Rsp: nil,
						Err: err,
					}
				}
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
				indexVal := s.logMgr.getLogIndexVal(cmd.ctx)
				s.rpc.asyncAppendEntries(cmd.ctx, &raft.AppendEntriesReq{
					Term:         s.md.get().Term,
					LeaderId:     s.rpc.My,
					PrevLogIndex: indexVal.lastLogIndex,
					PrevLogTerm:  indexVal.lastLogTerm,
					Entries:      nil,
					LeaderCommit: indexVal.lastCommitIndex,
				}, func(ctx *context.Context, res *asyncAppendEntriesRes) {
					return
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
			case smCommandAsyncAppendEntriesComp:
				rsp, err := s.execCommandAsyncEntriesCompFromLoop(cmd.ctx, cmd.anyReq.(*asyncAppendEntriesRes))
				if cmd.cq != nil {
					cmd.cq <- smCommandRes{
						Rsp: rsp,
						Err: err,
					}
				}
			case smCommandInsertNoOp:
				err := s.insertNoOp(cmd.ctx)
				if err != nil {
					slog.Error("insert noop", slog.String("err", err.Error()), slog.String("my", s.rpc.My))
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
			}
		}
	}
}

func (s *CoreSm) execAddServerFromLoop(ctx *context.Context, req *raft.AddServerReq) (rsp *raft.AddServerRsp, err error) {
	rsp = new(raft.AddServerRsp)
	if s.state != raft.State_StateLeader {
		err = emitErr(raft.ErrCode_ErrNoLeader,
			slog.Int("state", int(s.state)),
			slog.String("my", s.rpc.My),
		)
		return
	}
	if req.NewServer == s.rpc.My || slices.Contains(s.rpc.Membership, req.NewServer) {
		err = emitErr(raft.ErrCode_ErrChangeServerExists)
		return
	}
	oldMemberList := append([]string{s.rpc.My}, s.rpc.Membership...)
	newMemberList := append(oldMemberList, req.NewServer)
	err = s.insertMemberChange(ctx, oldMemberList, newMemberList)
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
			slog.String("my", s.rpc.My),
		)
		return
	}
	if req.OldServer != s.rpc.My && !slices.Contains(s.rpc.Membership, req.OldServer) {
		err = emitErr(raft.ErrCode_ErrChangeServerNotFound)
		return
	}
	// TODO 移除自身
	if req.OldServer == s.rpc.My {
		err = fmt.Errorf("not support remove leader")
		return
	}
	oldMemberList := append([]string{s.rpc.My}, s.rpc.Membership...)
	newMemberList := append([]string(nil), oldMemberList...)
	newMemberList = slices.DeleteFunc(newMemberList, func(s string) bool {
		if s == req.OldServer {
			return true
		}
		return false
	})
	err = s.insertMemberChange(ctx, oldMemberList, newMemberList)
	if err != nil {
		return
	}
	return
}

func (s *CoreSm) execCommandAsyncEntriesCompFromLoop(ctx *context.Context, anyReq *asyncAppendEntriesRes) (*raft.AppendCommandsRsp, error) {
	rsp := &raft.AppendCommandsRsp{
		LastLogIndex: anyReq.Req.PrevLogIndex,
		LastLogTerm:  anyReq.Req.PrevLogTerm,
	}
	var err error
	if len(anyReq.Req.Entries) > 0 {
		s.logMgr.notifyNewCommit(anyReq.Req.PrevLogIndex + uint64(len(anyReq.Req.Entries)))
	}
	return rsp, err
}

func (s *CoreSm) insertNoOp(ctx *context.Context) error {
	return s.insertOneLog(ctx, raft.CommandType_NoOpCommand, []byte("raft-noop"))
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
	return s.insertOneLog(ctx, raft.CommandType_MemberChangeCommand, marshalBytes)
}

func (s *CoreSm) insertOneLog(ctx *context.Context, typ raft.CommandType, dat []byte) error {
	indexVal := s.logMgr.getLogIndexVal(ctx)
	entries := []*raft.Entry{
		{
			Term:        indexVal.lastLogTerm,
			LogIndex:    indexVal.lastLogIndex + 1,
			CommandType: uint32(typ),
			Command:     dat,
		},
	}
	err := s.logMgr.appendLog(ctx, entries)
	if err != nil {
		return err
	}
	s.rpc.asyncAppendEntries(ctx, &raft.AppendEntriesReq{
		Term:         s.md.get().Term,
		LeaderId:     s.rpc.Leader,
		PrevLogIndex: indexVal.lastLogIndex,
		PrevLogTerm:  indexVal.lastLogTerm,
		Entries:      entries,
	}, func(ctx *context.Context, res *asyncAppendEntriesRes) {
		s.q <- smCommand{
			typ:    smCommandAsyncAppendEntriesComp,
			ctx:    ctx,
			anyReq: res,
		}
	})
	return nil
}

func (s *CoreSm) execCommandAsyncRequestVoteCompFromLoop(ctx *context.Context, res *asyncVoteRes) error {
	if s.state != raft.State_StateCandidate {
		return nil
	}
	minVote := len(s.rpc.Membership)/2 + 1
	if res.VoteCount >= uint64(minVote) {
		s.state = raft.State_StateLeader
		s.rpc.Leader = s.rpc.My
		slog.Info("my is leader, call broadcastAppendEntries2AllMemberShip", slog.String("my", s.rpc.My))
		indexVal := s.logMgr.getLogIndexVal(ctx)
		s.rpc.asyncAppendEntries(ctx, &raft.AppendEntriesReq{
			Term:         s.md.get().Term,
			LeaderId:     s.rpc.My,
			PrevLogIndex: indexVal.lastLogIndex,
			PrevLogTerm:  indexVal.lastLogTerm,
			Entries:      nil,
			LeaderCommit: indexVal.lastCommitIndex,
		}, func(ctx *context.Context, res *asyncAppendEntriesRes) {
			// 插入no-op日志
			s.q <- smCommand{
				typ:    smCommandInsertNoOp,
				ctx:    ctx,
				anyReq: res,
			}
			return
		})

	}
	return nil
}

func (s *CoreSm) enterCandidate() error {
	s.state = raft.State_StateCandidate
	_, err := s.md.termIncr()
	if err != nil {
		return err
	}
	ctx := context.Background()
	err = s.md.save(metaData{
		VoteFor: s.rpc.My,
		Term:    s.md.get().Term,
	})
	if err != nil {
		return err
	}
	indexVal := s.logMgr.getLogIndexVal(ctx)
	req := &raft.RequestVoteReq{
		Term:         s.md.get().Term,
		CandiDateId:  s.rpc.My,
		LastLogIndex: indexVal.lastLogIndex,
		LastLogTerm:  indexVal.lastLogTerm,
	}
	s.rpc.asyncRequestVote(ctx, req, func(ctx *context.Context, res *asyncVoteRes) {
		s.q <- smCommand{
			typ:    smCommandAsyncRequestVoteComp,
			ctx:    context.Background(),
			anyReq: res,
		}
	})
	return nil
}

func (s *CoreSm) execAppendCommandsFromLoop(ctx *context.Context, req *raft.AppendCommandsReq, cq chan smCommandRes) error {
	const OneMaxCount = 100
	if !s.myIsLeader() {
		return emitErr(raft.ErrCode_ErrNoLeader,
			slog.Int("state", int(s.state)),
			slog.String("my", s.rpc.My),
		)
	}
	commands := req.Commands
	for len(commands) > 0 {
		count := OneMaxCount
		if len(commands) < OneMaxCount {
			count = len(commands)
		}
		entries := make([]*raft.Entry, 0, count)
		indexVal := s.logMgr.getLogIndexVal(ctx)
		for idx, cmd := range commands[:count] {
			entries = append(entries, &raft.Entry{
				Term:     s.md.get().Term,
				LogIndex: indexVal.lastLogIndex + uint64(idx),
				Command:  cmd,
			})
		}
		commands = commands[count:]
		err := s.logMgr.appendLog(ctx, entries)
		if err != nil {
			return err
		}
		s.rpc.asyncAppendEntries(ctx, &raft.AppendEntriesReq{
			Term:         s.md.get().Term,
			LeaderId:     s.rpc.Leader,
			PrevLogIndex: indexVal.lastLogIndex,
			PrevLogTerm:  indexVal.lastLogTerm,
			Entries:      entries,
		}, func(ctx *context.Context, res *asyncAppendEntriesRes) {
			s.q <- smCommand{
				typ:    smCommandAsyncAppendEntriesComp,
				ctx:    ctx,
				anyReq: res,
				cq:     cq,
			}
		})
	}
	return nil
}

func (s *CoreSm) execGetLeaderFromLoop(ctx *context.Context, req *raft.GetLeaderReq) (rsp *raft.GetLeaderRsp, err error) {
	rsp = &raft.GetLeaderRsp{
		LeaderId: s.rpc.Leader,
		LeaderIp: s.rpc.Leader,
	}
	return rsp, nil
}

func (s *CoreSm) execRequestVoteFromLoop(ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	if s.rpc.My == req.CandiDateId {
		err = errors.New("invalid vote request")
		return
	}
	md := *s.md.get()
	indexVal := s.logMgr.getLogIndexVal(ctx)
	rsp = &raft.RequestVoteRsp{}
	if req.Term > md.Term {
		md.Term = req.Term
		rsp.Term = md.Term
		rsp.VoteGranted = true
	} else if req.Term < md.Term {
		rsp.VoteGranted = false
		rsp.Term = md.Term
	} else if req.LastLogTerm > indexVal.lastLogTerm || req.LastLogIndex > indexVal.lastLogIndex {
		rsp.VoteGranted = true
		rsp.Term = md.Term
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

func (s *CoreSm) execLeaderHeartBeatFromLoop(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	slog.Debug("leader heartbeat", slog.String("leader", req.LeaderId), slog.String("my", s.rpc.My))
	rsp = new(raft.AppendEntriesRsp)
	if s.md.get().Term > req.Term {
		err = errors.New("term is greater than current term")
		return
	}
	indexVal := s.logMgr.getLogIndexVal(ctx)
	if indexVal.lastLogIndex > req.PrevLogIndex {
		err = errors.New("prev log is greater than current log index")
		return
	}
	if s.state == raft.State_StateFollower {
		s.logMgr.notifyNewCommit(req.LeaderCommit)
	}
	if req.Term > s.md.get().Term {
		s.state = raft.State_StateFollower
		err = s.md.save(metaData{
			VoteFor: "",
			Term:    req.Term,
		})
		if err != nil {
			return
		}
	} else if req.Term == s.md.get().Term {
		s.state = raft.State_StateFollower
	}
	s.lastLeaderHeartBeat = time.Now()
	s.rpc.Leader = req.LeaderId
	rsp.Term = s.md.get().Term
	return rsp, nil
}

func (s *CoreSm) execAppendEntriesFromLoop(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	slog.Info("leader append entries", slog.String("leader", req.LeaderId), slog.String("my", s.rpc.My))
	rsp = new(raft.AppendEntriesRsp)
	if s.md.get().Term > req.Term {
		err = errors.New("term is greater than current term")
		return
	}
	indexVal := s.logMgr.getLogIndexVal(ctx)
	if indexVal.lastLogIndex > req.PrevLogIndex {
		err = errors.New("prev log is greater than current log index")
		return
	}
	err = s.logMgr.appendLog(ctx, req.Entries)
	if err != nil {
		return
	}
	rsp.Term = s.md.get().Term
	return rsp, nil
}

func (s *CoreSm) execGetLeader(ctx *context.Context, req *raft.GetLeaderReq) (rsp *raft.GetLeaderRsp, err error) {
	cq := make(chan smCommandRes, 1)
	s.q <- smCommand{
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

func (s *CoreSm) execAppendCommands(ctx *context.Context, req *raft.AppendCommandsReq) (rsp *raft.AppendCommandsRsp, err error) {
	cq := make(chan smCommandRes, 1)
	s.q <- smCommand{
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

func (s *CoreSm) execAppendEntries(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	cq := make(chan smCommandRes, 1)
	if len(req.Entries) == 0 {
		s.q <- smCommand{
			typ: smCommandLeaderHeartBeat,
			ctx: ctx,
			req: req,
			cq:  cq,
		}
	} else {
		s.q <- smCommand{
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

func (s *CoreSm) execRequestVote(ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	cq := make(chan smCommandRes, 1)
	s.q <- smCommand{
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

func (s *CoreSm) execAddServer(ctx *context.Context, req *raft.AddServerReq) (rsp *raft.AddServerRsp, err error) {
	cq := make(chan smCommandRes, 1)
	s.q <- smCommand{
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

func (s *CoreSm) execRemoveServer(ctx *context.Context, req *raft.RemoveServerReq) (rsp *raft.RemoveServerRsp, err error) {
	cq := make(chan smCommandRes, 1)
	s.q <- smCommand{
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
