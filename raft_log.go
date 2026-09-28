package raft

import (
	"cmp"
	"fmt"
	"log/slog"
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
	mu         sync.RWMutex
	indexVal   *raftLogIndexVal
	dirPath    string
	logName    string
	r          *diskList
	w          *diskList
	sm         StateMachine
	maxLogSeg  int
	applyEvent chan struct{}
}

func newRaftLogManager(dirPath string, logName string, sm StateMachine) *raftLogManager {
	return &raftLogManager{
		dirPath:    dirPath,
		logName:    logName,
		sm:         sm,
		indexVal:   new(raftLogIndexVal),
		applyEvent: make(chan struct{}, 128),
	}
}

func (mgr *raftLogManager) init() error {
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
	ctx := context.Background()
	err = mgr.sm.Init(ctx)
	if err != nil {
		return err
	}
	mgr.startBgLogSegClean()
	mgr.startBgLogApply()
	return nil
}

func (mgr *raftLogManager) getSegFileName(segCount int) (string, string) {
	idx := fmt.Sprintf("%s.idx.seg.%d", mgr.logName, segCount)
	dat := fmt.Sprintf("%s.dat.seg.%d", mgr.logName, segCount)
	return idx, dat
}

func (mgr *raftLogManager) getSegList(isDesc bool) ([]int, error) {
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
	if isDesc {
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

func (mgr *raftLogManager) startBgLogApply() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-mgr.applyEvent:
				err := mgr.doLogApply()
				if err != nil {
					slog.Error("bg log apply",
						slog.String("err", err.Error()),
					)
				}
				ticker.Reset(time.Second)
			case <-ticker.C:
				err := mgr.doLogApply()
				if err != nil {
					slog.Error("bg log apply",
						slog.String("err", err.Error()),
					)
				}
			}
		}
	}()
}

func (mgr *raftLogManager) doLogApply() error {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	ctx := context.Background()
	lastApplied, err := mgr.sm.LastApplied(ctx)
	if err != nil {
		return err
	}
	if lastApplied > mgr.indexVal.lastCommitIndex || lastApplied == mgr.indexVal.lastCommitIndex {
		return nil
	}
	applyEnd := mgr.indexVal.lastCommitIndex
	if applyEnd > lastApplied+100 {
		applyEnd = lastApplied + 100
	}
	err = mgr.applyLogWithOffV2(ctx, lastApplied, applyEnd)
	if err != nil {
		return err
	}
	mgr.indexVal.lastApplied = applyEnd
	return nil
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
	//entry, err := mgr.r.first(true)
	//if err != nil {
	//	return
	//}
	//if entry == nil {
	//	if len(segList) > 1 {
	//		for _, segC := range segList[:len(segList)-1] {
	//			idxFileName, datFileName := mgr.getSegFileName(segC)
	//			os.Remove(filepath.Join(mgr.dirPath, idxFileName))
	//			os.Remove(filepath.Join(mgr.dirPath, datFileName))
	//		}
	//	}
	//} else {
	//	for _, segC := range segList {
	//		idxFileName, datFileName := mgr.getSegFileName(segC)
	//		os.Remove(filepath.Join(mgr.dirPath, idxFileName))
	//		os.Remove(filepath.Join(mgr.dirPath, datFileName))
	//	}
	//}
	for _, seg := range segList {
		segFile, err := openDiskListSegFile(mgr.dirPath, mgr.logName, seg)
		if err != nil {
			return
		}
		lastEntry, err := segFile.last(true)
		if err != nil {
			return
		}
		if lastEntry.LogIndex <= lastApplied {
			err = segFile.Remove()
			if err != nil {
				return
			}
		}
	}
}

func (mgr *raftLogManager) notifyNewCommit(idx uint64) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.indexVal.lastCommitIndex = idx
	select {
	case mgr.applyEvent <- struct{}{}:
		break
	default:
		break
	}
}

func (mgr *raftLogManager) getLogIndexVal(ctx *context.Context) raftLogIndexVal {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return *mgr.indexVal
}

func (mgr *raftLogManager) appendLog(ctx *context.Context, entries []*raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	// 分批次应用, 单次最多500条
	const OneMaxCount = 500
	var (
		count      int
		wbEntries  = entries
		numEntries = len(entries)
	)
	for len(wbEntries) > 0 {
		count = OneMaxCount
		if len(wbEntries) < OneMaxCount {
			count = len(wbEntries)
		}
		err := mgr.w.write(wbEntries[:count])
		if err != nil {
			return err
		}
		wbEntries = wbEntries[count:]
	}
	mgr.indexVal.lastLogIndex += uint64(numEntries)
	mgr.indexVal.lastLogTerm = entries[len(entries)-1].Term
	return nil
}

func (mgr *raftLogManager) applyLog2UserSm(ctx *context.Context, entries []*raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.doApplyLog2UserSm(ctx, entries)
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

func (mgr *raftLogManager) applyLogWithOffV2(ctx *context.Context, start, end uint64) error {
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
