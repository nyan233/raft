package raft

import (
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

type raftServer struct {
	mu             sync.Mutex
	rpcHelper      *raftRpcHelper
	state          raft.State
	term           uint64
	logs           []raft.Entry
	lastLogIndex   uint64
	lastLogTerm    uint64
	lastHeartbeat  time.Time
	done           chan struct{}
	leaderHeCancel func()
}

func newRaftServer(my string, membership []string) (*raftServer, error) {
	s := &raftServer{
		state:        raft.State_StateNil,
		logs:         make([]raft.Entry, 0, 128),
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	h, err := newRaftRpcServer(s, my, membership)
	if err != nil {
		return nil, err
	}
	s.rpcHelper = h
	return s, nil
}

func (s *raftServer) handleRequestVote(ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	rsp = &raft.RequestVoteRsp{}
	if s.rpcHelper.getMy() == req.CandiDateId {
		err = errors.New("invalid vote request")
		return
	}
	_ = s.lockFunc(func() error {
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
		return nil
	})
	return
}

func (s *raftServer) handleAppendEntries(ctx *context.Context, req *raft.AppendEntriesReq) (rsp *raft.AppendEntriesRsp, err error) {
	slog.Info("leader append entries", slog.String("leader", req.LeaderId), slog.String("my", s.rpcHelper.getMy()))
	if len(req.Entries) == 0 {
		_ = s.lockFunc(func() error {
			s.state = raft.State_StateFollower
			s.lastHeartbeat = time.Now()
			return nil
		})
	}
	return &raft.AppendEntriesRsp{}, nil
}

func (s *raftServer) Init() error {
	go func() {
		for {
			tick := time.Duration(150+rand.Int63n(150)) * time.Millisecond
			ticker := time.NewTimer(tick)
			select {
			case <-ticker.C:
				s.candidateTimeout()
				ticker.Stop()
			case <-s.done:
				ticker.Stop()
				return
			}
		}
	}()
	return nil
}

func (s *raftServer) runLeaderHeartbeat(ctx *context.Context) {
	go func() {
		ticker := time.NewTicker(time.Millisecond * 10)
		for {
			select {
			case <-ticker.C:
				var (
					term     uint64
					logIndex uint64
					logTerm  uint64
				)
				_ = s.lockFunc(func() error {
					term = s.term
					logIndex = s.lastLogIndex
					logTerm = s.lastLogTerm
					return nil
				})
				addr, err := s.rpcHelper.broadcastHeartbeatAllMemberShip(ctx, term, logIndex, logTerm, logIndex)
				if err != nil {
					slog.Error(err.Error(), slog.String("addr", addr))
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (s *raftServer) cancelLeaderHeartbeat() {
	if s.leaderHeCancel != nil {
		s.leaderHeCancel()
	}
}

func (s *raftServer) lockFunc(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn()
}

func (s *raftServer) enterCandidate() error {
	return s.lockFunc(func() error {
		s.state = raft.State_StateCandidate
		s.term++
		voteCount := 1
		ctx := context.Background()
		pRsp, err := s.rpcHelper.parallelRequestVote(ctx, &raft.RequestVoteReq{
			Term:         s.term,
			CandiDateId:  s.rpcHelper.getMy(),
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
			s.rpcHelper.setLeader(s.rpcHelper.getMy())
			slog.Info("my is leader, call broadcastHeartbeatAllMemberShip", slog.String("my", s.rpcHelper.getMy()))
			addr, err := s.rpcHelper.broadcastHeartbeatAllMemberShip(ctx, s.term, s.lastLogIndex, s.lastLogTerm, s.lastLogIndex)
			if err != nil {
				slog.Error(err.Error(), slog.String("addr", addr))
				return err
			}
			var ctx2 *context.Context
			ctx2, s.leaderHeCancel = context.WithCancel(context.Background())
			s.runLeaderHeartbeat(ctx2)
		}
		return nil
	})
}

func (s *raftServer) candidateTimeout() {
	switch s.state {
	case raft.State_StateFollower:
		var enterCandidate bool
		_ = s.lockFunc(func() error {
			now := time.Now()
			if now.Sub(s.lastHeartbeat) > time.Second {
				enterCandidate = true
			}
			return nil
		})
		if enterCandidate {
			s.state = raft.State_StateCandidate
			err := s.enterCandidate()
			if err != nil {
				slog.Error("request vote rpc err", slog.String("err", err.Error()))
			}
		}
	case raft.State_StateCandidate, raft.State_StateNil:
		err := s.enterCandidate()
		if err != nil {
			slog.Error("request vote rpc err", slog.String("err", err.Error()))
		}
	}
}
