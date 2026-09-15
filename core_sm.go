package raft

import (
	"cmp"
	"errors"
	"log/slog"
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
	My         string
	Membership []string
	LogDir     string
	LogName    string
}

type CoreSm struct {
	cfg                 CoreSmConfig
	state               raft.State
	lastLogIndex        uint64
	lastLogTerm         uint64
	lastCommitIndex     uint64
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
	return nil
}

func (s *CoreSm) startTimeoutTicker() {
	go func() {
		ticker := time.NewTicker(time.Millisecond * 10)
		for {
			select {
			case <-ticker.C:
				s.q <- smCommand{
					typ: smCommandExecHeartBeat,
					ctx: context.Background(),
					req: nil,
					cq:  nil,
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
				var enterCandidate bool
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
					err := s.enterCandidate()
					if err != nil {
						slog.Error("request vote rpc err", slog.String("err", err.Error()))
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
					break
				}
				// TODO 有超时的情况会堵很久, 优化一下
				addr, err := s.rpc.broadcastHeartbeatAllMemberShip(cmd.ctx, &raft.AppendEntriesReq{
					Term:         s.term,
					LeaderId:     s.rpc.My,
					PrevLogIndex: s.lastLogIndex,
					PrevLogTerm:  s.lastLogTerm,
					Entries:      nil,
					LeaderCommit: s.lastCommitIndex,
				})
				if err != nil {
					slog.Error(err.Error(), slog.String("addr", addr))
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
		LastLogIndex: s.lastLogIndex,
		LastLogTerm:  s.lastLogTerm,
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
		slog.Info("my is leader, call broadcastHeartbeatAllMemberShip", slog.String("my", s.rpcHelper.getMy()))
		addr, err := s.rpc.broadcastHeartbeatAllMemberShip(ctx, &raft.AppendEntriesReq{
			Term:         s.term,
			LeaderId:     s.rpc.My,
			PrevLogIndex: s.lastLogIndex,
			PrevLogTerm:  s.lastLogTerm,
			Entries:      nil,
			LeaderCommit: s.lastCommitIndex,
		})
		if err != nil {
			slog.Error(err.Error(), slog.String("addr", addr))
			return err
		}
	}
	return nil
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
	} else if req.LastLogTerm > s.lastLogTerm || req.LastLogIndex > s.lastLogIndex {
		rsp.VoteGranted = true
		rsp.Term = s.term
	}
	if rsp.VoteGranted {
		s.state = raft.State_StateFollower
	}
	return
}

func (s *CoreSm) execLeaderHeartBeatFromLoop(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	slog.Info("leader heartbeat", slog.String("leader", req.LeaderId), slog.String("my", s.rpc.My))
	rsp = new(raft.AppendEntriesRsp)
	if s.term > req.Term {
		err = errors.New("term is greater than current term")
		return
	}
	if s.lastLogIndex > req.PrevLogIndex {
		err = errors.New("prev log is greater than current log index")
		return
	}
	s.lastLeaderHeartBeat = time.Now()
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
	if s.lastLogIndex > req.PrevLogIndex {
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
