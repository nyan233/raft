package raft

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
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
)

type smCommandRes struct {
	Rsp proto.Message
	Err error
}

type smCommand struct {
	typ uint8
	ctx *context.Context
	req proto.Message
	cq  chan smCommandRes
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
	term                uint64
	q                   chan smCommand
	heartbeatTicker     *time.Ticker
	lastLeaderHeartBeat time.Time
	logMgr              *raftLogManager
	rpc                 *coreRpc
}

func NewCoreSm(cfg CoreSmConfig) *CoreSm {
	return &CoreSm{
		cfg:   cfg,
		state: raft.State_StateFollower,
		q:     make(chan smCommand, 1024),
	}
}

func (s *CoreSm) Init() error {
	var err error
	s.rpc, err = newCoreRpc(s, s.cfg.My, s.cfg.Membership)
	if err != nil {
		return err
	}
	s.logMgr = newRaftLogManager(s.cfg.LogDir, s.cfg.LogName, s.cfg.UserSmFactory())
	err = s.logMgr.init()
	if err != nil {
		return err
	}
	if s.term == 0 {
		s.term = s.logMgr.lastLogTerm
	}
	go s.startLoop()
	s.startTimeoutTicker()
	return nil
}

func (s *CoreSm) myIsLeader() bool {
	return s.state == raft.State_StateLeader && s.rpc.My == s.rpc.Leader
}

func (s *CoreSm) getLastLogIndex() uint64 {
	return s.logMgr.lastLogIndex
}

func (s *CoreSm) getLastLogTerm() uint64 {
	return s.logMgr.lastLogTerm
}

func (s *CoreSm) getLastCommitIndex() uint64 {
	return s.logMgr.lastCommitIndex
}

func (s *CoreSm) startTimeoutTicker() {
	go func() {
		ticker := time.NewTicker(time.Millisecond * 10)
		for {
			select {
			case <-ticker.C:
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
			}
		}
	}()
	go func() {
		for {
			select {
			case <-time.After(time.Duration(150+rand.Int63n(150)) * time.Millisecond):
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
			}
		}
	}()
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
				}
				cmd.cq <- smCommandRes{
					Rsp: nil,
					Err: err,
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
				// TODO 有超时的情况会堵很久, 优化一下
				addr, err := s.rpc.broadcastAppendEntries2AllMemberShip(cmd.ctx, &raft.AppendEntriesReq{
					Term:         s.term,
					LeaderId:     s.rpc.My,
					PrevLogIndex: s.getLastLogIndex(),
					PrevLogTerm:  s.getLastLogTerm(),
					Entries:      nil,
					LeaderCommit: s.getLastCommitIndex(),
				})
				if err != nil {
					slog.Error(err.Error(), slog.String("addr", addr))
				}
				cmd.cq <- smCommandRes{
					Rsp: nil,
					Err: err,
				}
			case smCommandAppendCommands:
				rsp, err := s.execAppendCommandsFromLoop(cmd.ctx, cmd.req.(*raft.AppendCommandsReq))
				cmd.cq <- smCommandRes{
					Rsp: rsp,
					Err: err,
				}
			case smCommandGetLeader:
				rsp, err := s.execGetLeaderFromLoop(cmd.ctx, cmd.req.(*raft.GetLeaderReq))
				cmd.cq <- smCommandRes{
					Rsp: rsp,
					Err: err,
				}
			}
		}
	}
}

func (s *CoreSm) enterCandidate() error {
	s.state = raft.State_StateCandidate
	s.term++
	voteCount := 1
	ctx := context.Background()
	pRsp, err := s.rpc.parallelRequestVote(ctx, &raft.RequestVoteReq{
		Term:         s.term,
		CandiDateId:  s.rpc.My,
		LastLogIndex: s.getLastLogIndex(),
		LastLogTerm:  s.getLastLogTerm(),
	})
	if err != nil {
		return err
	}
	for _, iRsp := range pRsp {
		if iRsp.err != nil {
			return iRsp.err
		} else if iRsp.result.VoteGranted {
			voteCount++
		}
	}
	if voteCount > 2 {
		s.state = raft.State_StateLeader
		s.rpc.Leader = s.rpc.My
		slog.Info("my is leader, call broadcastAppendEntries2AllMemberShip", slog.String("my", s.rpc.My))
		addr, err := s.rpc.broadcastAppendEntries2AllMemberShip(ctx, &raft.AppendEntriesReq{
			Term:         s.term,
			LeaderId:     s.rpc.My,
			PrevLogIndex: s.getLastLogIndex(),
			PrevLogTerm:  s.getLastLogTerm(),
			Entries:      nil,
			LeaderCommit: s.getLastCommitIndex(),
		})
		if err != nil {
			slog.Error(err.Error(), slog.String("addr", addr))
			return err
		}
	}
	return nil
}

func (s *CoreSm) execAppendCommandsFromLoop(ctx *context.Context, req *raft.AppendCommandsReq) (*raft.AppendCommandsRsp, error) {
	const OneMaxCount = 100
	if !s.myIsLeader() {
		return nil, fmt.Errorf("my is not leader, addr=%s, state=%d", s.rpc.My, s.state)
	}
	entries := make([]*raft.Entry, 0, OneMaxCount)
	for len(req.Commands) > 0 {
		count := OneMaxCount
		if len(req.Commands) < OneMaxCount {
			count = len(req.Commands)
		}
		entries = entries[:0]
		for idx, cmd := range req.Commands[:count] {
			entries = append(entries, &raft.Entry{
				Term:     s.term,
				LogIndex: s.getLastLogIndex() + uint64(idx),
				Command:  cmd,
			})
		}
		err := s.logMgr.applyLog(ctx, entries)
		if err != nil {
			return nil, err
		}
		// 多数提交, 70%, 最少2个节点提交即可返回
		addr, err := s.rpc.broadcastAppendEntries2AllMemberShip(ctx, &raft.AppendEntriesReq{
			Term:         s.term,
			LeaderId:     s.rpc.Leader,
			PrevLogIndex: s.getLastLogIndex(),
			PrevLogTerm:  s.getLastLogTerm(),
			Entries:      entries,
		})
		if err != nil {
			slog.Error(err.Error(), slog.String("addr", addr))
		}
	}
	return &raft.AppendCommandsRsp{
		LastLogIndex: s.getLastLogIndex(),
		LastLogTerm:  s.getLastLogTerm(),
	}, nil
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
	rsp = &raft.RequestVoteRsp{}
	if req.Term > s.term {
		rsp.Term = s.term
		s.term = req.Term
		rsp.VoteGranted = true
	} else if req.Term < s.term {
		rsp.VoteGranted = false
		rsp.Term = s.term
	} else if req.LastLogTerm > s.getLastLogTerm() || req.LastLogIndex > s.getLastLogIndex() {
		rsp.VoteGranted = true
		rsp.Term = s.term
	}
	if rsp.VoteGranted {
		s.state = raft.State_StateFollower
	}
	return
}

func (s *CoreSm) execLeaderHeartBeatFromLoop(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	slog.Debug("leader heartbeat", slog.String("leader", req.LeaderId), slog.String("my", s.rpc.My))
	rsp = new(raft.AppendEntriesRsp)
	if s.term > req.Term {
		err = errors.New("term is greater than current term")
		return
	}
	if s.getLastLogIndex() > req.PrevLogIndex {
		err = errors.New("prev log is greater than current log index")
		return
	}
	s.lastLeaderHeartBeat = time.Now()
	s.rpc.Leader = req.LeaderId
	rsp.Term = s.term
	return rsp, nil
}

func (s *CoreSm) execAppendEntriesFromLoop(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	slog.Info("leader append entries", slog.String("leader", req.LeaderId), slog.String("my", s.rpc.My))
	rsp = new(raft.AppendEntriesRsp)
	if s.term > req.Term {
		err = errors.New("term is greater than current term")
		return
	}
	if s.getLastLogIndex() > req.PrevLogIndex {
		err = errors.New("prev log is greater than current log index")
		return
	}
	maxTermEntry := slices.MaxFunc(req.Entries, func(a, b *raft.Entry) int {
		return cmp.Compare(a.Term, b.Term)
	})
	err = s.logMgr.applyLog(ctx, req.Entries)
	if err != nil {
		return
	}
	rsp.Term = s.term
	s.term = maxTermEntry.Term
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
