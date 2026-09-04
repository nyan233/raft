package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

type raftServer struct {
	mu            sync.Mutex
	rpcHelper     *raftRpcHelper
	state         raft.State
	term          uint64
	logs          []raft.Entry
	lastLogIndex  uint64
	lastLogTerm   uint64
	lastHeartbeat time.Time
	done          chan struct{}
}

func newRaftServer(my string, membership []string) (*raftServer, error) {
	h, err := newRaftRpcServer(my, membership)
	if err != nil {
		return nil, err
	}
	s := &raftServer{
		rpcHelper:    h,
		my:           my,
		state:        raft.State_StateNil,
		logs:         make([]raft.Entry, 0, 128),
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	return s, nil
}

func (s *raftServer) handleRequestVote(ctx *context.Context, req *raft.RequestVoteReq) (rsp *raft.RequestVoteRsp, err error) {
	rsp = &raft.RequestVoteRsp{}
	if s.my == req.CandiDateId {
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
	if len(req.Entries) == 0 {
		s.lastHeartbeat = time.Now()
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

func (s *raftServer) lockFunc(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn()
}

func (s *raftServer) candidateTimeout() {
	if s.state == raft.State_StateNil {
		s.lockFunc(func() error {
			s.state = raft.State_StateCandidate
			s.term++
			voteCount := 1
			for _, member := range s.membership {
				vote, err := s.rpcHelper.callRequestVote(member, context.Background(), &raft.RequestVoteReq{
					Term:         s.term,
					CandiDateId:  s.my,
					LastLogIndex: s.lastLogIndex,
					LastLogTerm:  s.lastLogTerm,
				})
				if err != nil {
					return err
				}
				if vote.VoteGranted {
					voteCount++
				}
			}
			if voteCount > 2 {
				s.state = raft.State_StateLeader
			}
			return nil
		})
	}
}
