package raft

import (
	"cmp"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
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
	mu        sync.RWMutex
	indexVal  *raftLogIndexVal
	dirPath   string
	logName   string
	r         *diskList
	w         *diskList
	sm        StateMachine
	maxLogSeg int
}

func newRaftLogManager(dirPath string, logName string, sm StateMachine) *raftLogManager {
	return &raftLogManager{
		dirPath:  dirPath,
		logName:  logName,
		sm:       sm,
		indexVal: new(raftLogIndexVal),
	}
}

func (mgr *raftLogManager) initFile() error {
	var err error
	mgr.r, err = openDiskList(mgr.dirPath, mgr.logName, false)
	if err != nil {
		return err
	}
	mgr.w, err = openDiskList(mgr.dirPath, mgr.logName, true)
	if err != nil {
		return err
	}
	entry, err := mgr.r.last(true)
	if err != nil {
		return err
	}
	if entry != nil {
		mgr.indexVal.lastLogIndex = entry.LogIndex
		mgr.indexVal.lastLogTerm = entry.Term
	}
	err = mgr.initSeg()
	if err != nil {
		return err
	}
	return nil
}

func (mgr *raftLogManager) init() error {
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

func (mgr *raftLogManager) getSegFileName(segCount int) (string, string) {
	idx := fmt.Sprintf("%s.idx.seg.%d", mgr.logName, segCount)
	dat := fmt.Sprintf("%s.dat.seg.%d", mgr.logName, segCount)
	return idx, dat
}

func (mgr *raftLogManager) getSegList(descSort bool) ([]int, error) {
	dirEntry, err := os.ReadDir(mgr.dirPath)
	if err != nil {
		return nil, err
	}
	subStr := mgr.logName + ".dat.seg."
	segList := make([]int, 0)
	for _, entry := range dirEntry {
		name := entry.Name()
		if strings.Contains(name, subStr) {
			segCount, err := strconv.Atoi(name[len(subStr):])
			if err != nil {
				return nil, err
			}
			segList = append(segList, segCount)
		}
	}
	if descSort {
		slices.SortFunc(segList, func(a, b int) int {
			return cmp.Compare(b, a)
		})
	} else {
		slices.Sort(segList)
	}
	return segList, nil
}

func (mgr *raftLogManager) initSeg() error {
	segList, err := mgr.getSegList(false)
	if err != nil {
		return err
	}
	if len(segList) == 0 {
		return nil
	}
	mgr.maxLogSeg = segList[len(segList)-1]
	if mgr.indexVal.lastLogIndex != 0 {
		return nil
	}
	segFs, err := openDiskListSegFile(mgr.dirPath, mgr.logName, segList[len(segList)-1])
	if err != nil {
		return err
	}
	entry, err := segFs.last(true)
	if err != nil {
		return err
	}
	mgr.indexVal.lastLogIndex = entry.LogIndex
	mgr.indexVal.lastLogTerm = entry.Term
	return nil
}

func (mgr *raftLogManager) mergeLogFile() error {
	err := mgr.r.close()
	if err != nil {
		return err
	}
	err = mgr.w.closeAndRenameSeg(mgr.maxLogSeg)
	if err != nil {
		return err
	}
	mgr.maxLogSeg++
	mgr.w, err = openDiskList(mgr.dirPath, mgr.logName, true)
	if err != nil {
		return err
	}
	mgr.r, err = openDiskList(mgr.dirPath, mgr.logName, false)
	if err != nil {
		return err
	}
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
	segList, err := mgr.getSegList(false)
	if err != nil {
		return
	}
	lastApplied, err := mgr.sm.LastApplied(context.Background())
	if err != nil {
		return
	}
	// NOTE 2026/10/16 保留最后一条日志, 用于AppendEntries日志检查
	cleanEnd := lastApplied - 1
	for _, seg := range segList {
		segFile, err := openDiskListSegFile(mgr.dirPath, mgr.logName, seg)
		if err != nil {
			return
		}
		lastEntry, err := segFile.last(true)
		if err != nil {
			return
		}
		if lastEntry.LogIndex <= cleanEnd {
			err = segFile.Remove()
			if err != nil {
				return
			}
		}
	}
}

func (mgr *raftLogManager) setCommitIndex(idx uint64) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.indexVal.lastCommitIndex = idx
}

func (mgr *raftLogManager) getLogIndexVal(ctx *context.Context) raftLogIndexVal {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return *mgr.indexVal
}

func (mgr *raftLogManager) appendLog(ctx *context.Context, term uint64, entries []*raft.Entry) (uint64, uint64, error) {
	if len(entries) == 0 {
		return mgr.indexVal.lastLogIndex, mgr.indexVal.lastLogIndex, nil
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for i := uint64(0); i < uint64(len(entries)); i++ {
		entries[i].Term = term
		entries[i].LogIndex = mgr.indexVal.lastLogIndex + i + 1
	}
	// 分批次应用, 单次最多500条
	const OneMaxCount = 500
	var (
		count     int
		wbEntries = entries
	)
	for len(wbEntries) > 0 {
		count = OneMaxCount
		if len(wbEntries) < OneMaxCount {
			count = len(wbEntries)
		}
		err := mgr.w.write(wbEntries[:count])
		if err != nil {
			return 0, 0, err
		}
		wbEntries = wbEntries[count:]
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
	logSize, err := mgr.w.dataSize()
	if err != nil {
		return err
	}
	// 1GB
	if logSize > logDataMaxSize {
		err = mgr.mergeLogFile()
		if err != nil {
			return err
		}
	}
	return nil
}

func (mgr *raftLogManager) noLockDoApplyLogWithOffV2(ctx *context.Context, start, end uint64) error {
	scope := newListScope3(mgr, start, end)
	err := scope.find()
	if err != nil {
		return err
	}
	entries := make([]*raft.Entry, 0, 128)
	err = scope.rangeFor(func(f *diskList, startOff, count uint64) error {
		readEntries, err := f.batchRead(int(startOff), int(count), false)
		if err != nil {
			return err
		}
		entries = append(entries, readEntries...)
		return nil
	})
	if err != nil {
		return err
	}
	err = scope.close()
	if err != nil {
		return err
	}
	err = mgr.doApplyLog2UserSm(ctx, entries)
	if err != nil {
		return err
	}
	return nil
}

func (mgr *raftLogManager) applyLogWithOffV2(ctx *context.Context, start, end uint64) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.noLockDoApplyLogWithOffV2(ctx, start, end)
}

func (mgr *raftLogManager) applyLogWithCount(ctx *context.Context, count int) error {
	if count <= 0 {
		return nil
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	oldApplied := mgr.indexVal.lastApplied
	newApplied := oldApplied + uint64(count)
	return mgr.noLockDoApplyLogWithOffV2(ctx, oldApplied, newApplied)
}

func (mgr *raftLogManager) newScope(ctx *context.Context, start, end uint64) (*listScope3, error) {
	scope := newListScope3(mgr, start, end)
	err := scope.find()
	if err != nil {
		return nil, err
	}
	return scope, nil
}

func (mgr *raftLogManager) readLogWithIndex(ctx *context.Context, start, end uint64) ([]*raft.Entry, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	scope, err := mgr.newScope(ctx, start, end)
	if err != nil {
		return nil, err
	}
	defer scope.close()
	entries := make([]*raft.Entry, 0, end-start)
	err = scope.rangeFor(func(f *diskList, startOff, count uint64) error {
		readEntries, err := f.batchRead(int(startOff), int(count), false)
		if err != nil {
			return err
		}
		entries = append(entries, readEntries...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (mgr *raftLogManager) readLog(ctx *context.Context, index uint64, onlyIdx bool) (*raft.Entry, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	scope, err := mgr.newScope(ctx, index, index)
	if err != nil {
		return nil, err
	}
	defer scope.close()
	var entry *raft.Entry
	err = scope.rangeFor(func(f *diskList, startOff, count uint64) error {
		entry, err = f.readOff(int(startOff), onlyIdx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (mgr *raftLogManager) lastLog(ctx *context.Context, onlyIdx bool) (*raft.Entry, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.r.last(onlyIdx)
}

func (mgr *raftLogManager) firstLog(ctx *context.Context, onlyIdx bool) (*raft.Entry, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	if mgr.maxLogSeg > 0 {
		segList, err := mgr.getSegList(false)
		if err != nil {
			return nil, err
		}
		w, err := openDiskListSegFile(mgr.dirPath, mgr.logName, segList[0])
		if err != nil {
			return nil, err
		}
		return w.first(onlyIdx)
	}

	return mgr.r.first(onlyIdx)
}

func (mgr *raftLogManager) truncate(ctx *context.Context, end uint64) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	segList, err := mgr.getSegList(true)
	if err != nil {
		return err
	}
	f := mgr.w
	requireResetMainFile := false
	i := 0
	for {
		requireRemove := false
		firstEntry, err := f.first(true)
		if err != nil {
			return err
		}
		if firstEntry == nil {
			break
		}
		if firstEntry.LogIndex > end {
			err = f.truncate(0)
			if err != nil {
				return err
			}
			requireRemove = true
		}
		if firstEntry.LogIndex < end {
			err = f.truncate(int(end - firstEntry.LogIndex))
			if err != nil {
				return err
			}
			if f != mgr.w {
				err = f.close()
				if err != nil {
					return err
				}
			}
			break
		}
		if requireRemove {
			if mgr.w == f {
				requireResetMainFile = true
				err = mgr.r.close()
				if err != nil {
					return err
				}
				mgr.r = nil
			}
			err = f.Remove()
			if err != nil {
				return err
			}
		}
		if i >= len(segList) {
			break
		}
		f, err = openDiskListSegFile(mgr.dirPath, mgr.logName, segList[i])
		if err != nil {
			return err
		}
		i++
	}
	if requireResetMainFile {
		err = segFileUp2MainFile(mgr.dirPath, mgr.logName, segList[i-1])
		if err != nil {
			return err
		}
		err = mgr.initFile()
		if err != nil {
			return err
		}
	}
	return nil
}
