package raft

import (
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

const (
	cleanLogWindow = 5000
)

type raftLogIndexVal struct {
	lastLogIndex    uint64
	lastLogTerm     uint64
	lastCommitIndex uint64
	lastApplied     uint64
}

type raftLogManager struct {
	mu            sync.RWMutex
	indexVal      *raftLogIndexVal
	lrw           LogDiskI
	sm            StateMachine
	closeLogClean atomic.Bool
}

func newRaftLogManager(dirPath string, logName string, sm StateMachine) *raftLogManager {
	return &raftLogManager{
		lrw:      newDiskListReaderWriter(dirPath, logName, 0),
		sm:       sm,
		indexVal: new(raftLogIndexVal),
	}
}

func (mgr *raftLogManager) initFile() error {
	var err error
	err = mgr.lrw.Init()
	if err != nil {
		return err
	}
	entry, err := mgr.lrw.Last(true)
	if err != nil {
		return err
	}
	if entry != nil {
		mgr.indexVal.lastLogIndex = entry.LogIndex
		mgr.indexVal.lastLogTerm = entry.Term
	}
	return nil
}

func (mgr *raftLogManager) Init() error {
	err := mgr.initFile()
	if err != nil {
		return err
	}
	ctx := context.Background()
	err = mgr.sm.Init(ctx)
	if err != nil {
		return err
	}
	mgr.indexVal.lastApplied, err = mgr.sm.LastApplied(ctx)
	if err != nil {
		return err
	}
	// TODO 先不弄日志清理了, 后面再上安装快照这些优化吧, 日志清理涉及到很多东西
	// mgr.startBgLogSegClean()
	return nil
}

func (mgr *raftLogManager) startBgLogSegClean() {
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				mgr.cleanLogSeg()
			}
		}
	}()
}

// 清理lastCommit >= .seg.logIndex的文件, 未提交完的不清理
func (mgr *raftLogManager) cleanLogSeg() {
	defer func() {
		err := recover()
		if err != nil {
			return
		}
	}()
	if mgr.closeLogClean.Load() {
		return
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	lastApplied, err := mgr.sm.LastApplied(context.Background())
	if err != nil {
		return
	}
	if lastApplied <= 0 {
		return
	}
	if lastApplied < cleanLogWindow {
		return
	}
	cleanEnd := lastApplied - cleanLogWindow
	err = mgr.lrw.CleanWithLogIndex(cleanEnd)
	if err != nil {
		slog.Error("CleanWithLogIndex", slog.String("err", err.Error()))
	}
}

func (mgr *raftLogManager) SetCommitIndex(idx uint64) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.indexVal.lastCommitIndex = idx
}

func (mgr *raftLogManager) GetCommitIndex() uint64 {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.indexVal.lastCommitIndex
}

func (mgr *raftLogManager) GetLogIndexVal(ctx *context.Context) raftLogIndexVal {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return *mgr.indexVal
}

func (mgr *raftLogManager) FollowerAppendLog(ctx *context.Context, entries []*raft.Entry) (uint64, uint64, error) {
	if len(entries) == 0 {
		return mgr.indexVal.lastLogIndex, mgr.indexVal.lastLogIndex, nil
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	err := mgr.lrw.WriteBatch(entries)
	if err != nil {
		return 0, 0, err
	}
	mgr.indexVal.lastLogIndex = entries[len(entries)-1].LogIndex
	mgr.indexVal.lastLogTerm = entries[len(entries)-1].Term
	return entries[0].LogIndex, mgr.indexVal.lastLogIndex, nil
}

func (mgr *raftLogManager) LeaderAppendLog(ctx *context.Context, term uint64, entries []*raft.Entry) (uint64, uint64, error) {
	if len(entries) == 0 {
		return mgr.indexVal.lastLogIndex, mgr.indexVal.lastLogIndex, nil
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for i := uint64(0); i < uint64(len(entries)); i++ {
		entries[i].Term = term
		entries[i].LogIndex = mgr.indexVal.lastLogIndex + i + 1
	}
	err := mgr.lrw.WriteBatch(entries)
	if err != nil {
		return 0, 0, err
	}
	mgr.indexVal.lastLogIndex = entries[len(entries)-1].LogIndex
	mgr.indexVal.lastLogTerm = term
	return entries[0].LogIndex, mgr.indexVal.lastLogIndex, nil
}

func (mgr *raftLogManager) doApplyLog2UserSm(ctx *context.Context, entries []*raft.Entry) error {
	// 分批次应用, 单次最多500条
	const OneMaxCount = 500
	var (
		count = 0
	)
	for len(entries) > 0 {
		count = OneMaxCount
		if len(entries) < OneMaxCount {
			count = len(entries)
		}
		err := mgr.sm.Apply(ctx, entries[:count])
		if err != nil {
			return err
		}
		mgr.indexVal.lastApplied = entries[count-1].LogIndex
		entries = entries[count:]
	}
	return nil
}

func (mgr *raftLogManager) ApplyLogWithOffV2(ctx *context.Context, start, end uint64) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	scope := mgr.lrw.Scope(start, end)
	entries, err := scope.ReadAll(false)
	if err != nil {
		return err
	}
	err = mgr.doApplyLog2UserSm(ctx, entries)
	if err != nil {
		return err
	}
	return nil
}

func (mgr *raftLogManager) LogEntryExist(ctx *context.Context, logIndex uint64) (bool, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	scope := mgr.lrw.Scope(logIndex, logIndex)
	err := scope.Find()
	if err != nil {
		if strings.HasPrefix(err.Error(), "unknown file offset") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (mgr *raftLogManager) ReadLogWithIndex(ctx *context.Context, start, end uint64, onlyIdx bool) ([]*raft.Entry, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	scope := mgr.lrw.Scope(start, end)
	entries, err := scope.ReadAll(onlyIdx)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (mgr *raftLogManager) ReadLog(ctx *context.Context, index uint64, onlyIdx bool) (*raft.Entry, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.doReadLog(ctx, index, onlyIdx)
}

func (mgr *raftLogManager) doReadLog(ctx *context.Context, index uint64, onlyIdx bool) (*raft.Entry, error) {
	scope := mgr.lrw.Scope(index, index)
	entries, err := scope.ReadAll(onlyIdx)
	if err != nil {
		return nil, err
	}
	if len(entries) > 0 {
		return entries[0], nil
	}
	return nil, nil
}

func (mgr *raftLogManager) Truncate(ctx *context.Context, end uint64) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	err := mgr.lrw.TruncateWithLogIndex(end)
	if err != nil {
		return err
	}
	if end == 0 {
		mgr.indexVal.lastLogTerm = 0
		mgr.indexVal.lastLogIndex = 0
		return nil
	}
	// update index val
	endEntry, err := mgr.doReadLog(ctx, end, true)
	if err != nil {
		return err
	}
	if endEntry == nil {
		mgr.indexVal.lastLogTerm = 0
		mgr.indexVal.lastLogIndex = 0
	} else {
		mgr.indexVal.lastLogIndex = endEntry.LogIndex
		mgr.indexVal.lastLogTerm = endEntry.Term
	}
	return nil
}

func (mgr *raftLogManager) StopLogClean() {
	mgr.closeLogClean.Store(true)
}

func (mgr *raftLogManager) StartLogClean() {
	mgr.closeLogClean.Store(false)
}

func (mgr *raftLogManager) First(ctx *context.Context, onlyIdx bool) (*raft.Entry, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.lrw.First(onlyIdx)
}

func (mgr *raftLogManager) Last(ctx *context.Context, onlyIdx bool) (*raft.Entry, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.lrw.Last(onlyIdx)
}
