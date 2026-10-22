package raft

import (
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/nyan233/littlerpc/core/client"
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

type followerAppendEntriesCb struct {
	Name  string
	Start uint64
	End   uint64
}

type leaderMatchIndexChangeEv struct {
	Old uint64
	New uint64
}

type followerProgressWorker struct {
	mu          sync.Mutex
	csm         *CoreSm
	rpc         *coreRpc
	name        string
	ipAddr      string
	leader      string
	matchIndex  uint64
	nextIndex   uint64
	close       chan struct{}
	updateIndex chan struct{}
	cbch        chan followerAppendEntriesCb
	wg          sync.WaitGroup
}

func newFollowerProgressWorker(csm *CoreSm, rpc *coreRpc, cbch chan followerAppendEntriesCb, name, ipAddr, leader string) *followerProgressWorker {
	return &followerProgressWorker{
		csm:         csm,
		rpc:         rpc,
		name:        name,
		ipAddr:      ipAddr,
		leader:      leader,
		close:       make(chan struct{}, 1),
		updateIndex: make(chan struct{}, 128),
		matchIndex:  0,
		nextIndex:   0,
		cbch:        cbch,
	}
}

func (f *followerProgressWorker) run() {
	f.wg.Add(1)
	logMgr := f.csm.getLogMgr()
	indexVal := logMgr.getLogIndexVal(context.Background())
	f.nextIndex = indexVal.lastLogIndex
	go func() {
		for {
			select {
			case <-f.close:
				f.wg.Done()
				return
			case <-f.updateIndex:
				f.doSync2Node()
			default:
				if f.doSync2Node() {
					time.Sleep(time.Millisecond)
				}
			}
		}
	}()
}

func (f *followerProgressWorker) doSync2Node() bool {
	f.mu.Lock()
	if f.nextIndex == f.matchIndex {
		f.mu.Unlock()
		return true
	}
	startIndex := f.matchIndex
	endIndex := f.nextIndex
	if endIndex-startIndex > 50 {
		endIndex = startIndex + 50
	}
	f.mu.Unlock()
	logMgr := f.csm.getLogMgr()
	ctx := context.Background()
	ctx.SetValue("Src", "follower-progress")
	entries, err := logMgr.readLogWithIndex(ctx, startIndex, endIndex, false)
	if err != nil {
		slog.Error(err.Error())
		return false
	}
	indexVal := logMgr.getLogIndexVal(ctx)
	// TODO rsp.Term怎么处理？
	rsp, err := f.rpc.proxy.AppendEntries(ctx, &raft.AppendEntriesReq{
		Term:         f.csm.md.get().Term,
		LeaderId:     f.leader,
		PrevLogIndex: entries[0].LogIndex,
		PrevLogTerm:  entries[0].Term,
		Entries:      entries,
		LeaderCommit: indexVal.lastCommitIndex,
	}, client.WithAddr(f.ipAddr))
	if err != nil {
		slog.Error(err.Error())
		return false
	}
	_ = rsp
	f.mu.Lock()
	f.matchIndex = endIndex
	f.mu.Unlock()
	f.cbch <- followerAppendEntriesCb{
		Name:  f.name,
		Start: startIndex,
		End:   endIndex,
	}
	return false
}

func (f *followerProgressWorker) stop() {
	if f.close != nil {
		close(f.close)
		f.wg.Wait()
	}
}

func (f *followerProgressWorker) next(logIndex uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextIndex = logIndex
	select {
	case f.updateIndex <- struct{}{}:
		break
	default:
		break
	}
}

func (f *followerProgressWorker) progress() (uint64, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.matchIndex, f.nextIndex
}

type leaderProgressWorker struct {
	mu sync.Mutex
	// followers
	fmap map[string]*followerProgressWorker
	csm  *CoreSm
	cbch chan followerAppendEntriesCb
	// 多数完成的索引数
	// 所有follower最小的matchIndex, 表示所有follower在此index已达成多数派
	matchIndex uint64
	nextIndex  uint64
}

func newLeaderProgressWorker(csm *CoreSm) *leaderProgressWorker {
	return &leaderProgressWorker{
		csm:        csm,
		fmap:       make(map[string]*followerProgressWorker, 8),
		cbch:       make(chan followerAppendEntriesCb, 1024),
		matchIndex: 0,
		nextIndex:  0,
	}
}

func (f *leaderProgressWorker) quorum() int {
	return (len(f.fmap)+1)/2 + 1
}

// [100, 91, 92, 84, 87] -> 91
func (f *leaderProgressWorker) run() {
	indexVal := f.csm.getLogMgr().getLogIndexVal(context.Background())
	f.nextIndex = indexVal.lastLogIndex
	go func() {
		for {
			select {
			case _ = <-f.cbch:
				l := make([]uint64, 0, 8)
				indexVal := f.csm.getLogMgr().getLogIndexVal(context.Background())
				l = append(l, indexVal.lastCommitIndex)
				f.mu.Lock()
				for _, v := range f.fmap {
					matchIndex, _ := v.progress()
					l = append(l, matchIndex)
				}
				f.mu.Unlock()
				slices.Sort(l)
				slices.Reverse(l)
				quorum := f.quorum()
				oldMatchIndex := f.matchIndex
				newMatchIndex := l[quorum-1]
				if oldMatchIndex == newMatchIndex {
					break
				} else if newMatchIndex > oldMatchIndex {
					f.matchIndex = newMatchIndex
				}
			}
		}
	}()
}

func (f *leaderProgressWorker) next(logIndex uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextIndex = logIndex
	for _, v := range f.fmap {
		v.next(logIndex)
	}
}

func (f *leaderProgressWorker) addFollower(name, ipAddr, leader string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	follower := newFollowerProgressWorker(f.csm, f.csm.rpc, f.cbch, name, ipAddr, leader)
	f.fmap[name] = follower
	follower.run()
}

func (f *leaderProgressWorker) closeAndRemoveFollower(name string) {
	f.mu.Lock()
	follower, ok := f.fmap[name]
	if ok {
		delete(f.fmap, name)
		f.mu.Unlock()
		follower.stop()
	} else {
		f.mu.Unlock()
	}
}
