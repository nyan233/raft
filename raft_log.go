package raft

import (
	"log/slog"
	"sync"
	"time"

	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

const (
	logDataMaxSize = 1024 * 1024 // 1GB
)

type raftLogIndexVal struct {
	lastLogIndex    uint64
	lastLogTerm     uint64
	lastCommitIndex uint64
	lastApplied     uint64
}

type raftLogManager struct {
	mu       sync.RWMutex
	indexVal *raftLogIndexVal
	lrw      *diskListReaderWriter
	sm       StateMachine
}

func newRaftLogManager(dirPath string, logName string, sm StateMachine) *raftLogManager {
	return &raftLogManager{
		lrw:      newDiskListReaderWriter(dirPath, logName, -1),
		sm:       sm,
		indexVal: new(raftLogIndexVal),
	}
}

func (mgr *raftLogManager) initFile() error {
	var err error
	err = mgr.lrw.init()
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
	mgr.startBgLogSegClean()
	return nil
}

func (mgr *raftLogManager) startBgLogSegClean() {
	go func() {
		ticker := time.NewTicker(time.Second * 5)
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
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	lastApplied, err := mgr.sm.LastApplied(context.Background())
	if err != nil {
		return
	}
	if lastApplied <= 0 {
		return
	}
	// NOTE 2026/10/16 保留最后一条日志, 用于AppendEntries日志检查
	cleanEnd := lastApplied - 1
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
		entries = entries[count:]
		mgr.indexVal.lastApplied += uint64(count)
	}
	return nil
}

func (mgr *raftLogManager) noLockDoApplyLogWithOffV2(ctx *context.Context, start, end uint64) error {
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

func (mgr *raftLogManager) ApplyLogWithOffV2(ctx *context.Context, start, end uint64) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.noLockDoApplyLogWithOffV2(ctx, start, end)
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
	// update index val
	endEntry, err := mgr.doReadLog(ctx, end, true)
	if err != nil {
		return err
	}
	mgr.indexVal.lastLogIndex = endEntry.LogIndex
	mgr.indexVal.lastLogTerm = endEntry.Term
	return nil
}
