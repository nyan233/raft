package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

const (
	logDiskSize    = 4 + 8*4 + 4
	logDataMaxSize = 1024 * 1024 * 1024 // 1GB
)

type logDisk struct {
	idxCheckSum  uint32
	logIndex     uint64
	logTerm      uint64
	dataSize     uint64
	datOffset    uint64
	dataCheckSum uint32
	readData     []byte
}

func (d *logDisk) checkSum(buf *[]byte) {
	checkSumBuf := *buf
	binary.BigEndian.PutUint64(checkSumBuf[:8], d.logIndex)
	binary.BigEndian.PutUint64(checkSumBuf[8:], d.logTerm)
	binary.BigEndian.PutUint64(checkSumBuf[16:], d.dataSize)
	binary.BigEndian.PutUint64(checkSumBuf[24:], d.datOffset)
	binary.BigEndian.PutUint32(checkSumBuf[32:], d.dataCheckSum)
	checkSum := crc32.ChecksumIEEE(checkSumBuf)
	d.idxCheckSum = checkSum
}

func (d *logDisk) writeToBuf(buf *[]byte) {
	*buf = binary.BigEndian.AppendUint32(*buf, d.idxCheckSum)
	*buf = binary.BigEndian.AppendUint64(*buf, d.logIndex)
	*buf = binary.BigEndian.AppendUint64(*buf, d.logTerm)
	*buf = binary.BigEndian.AppendUint64(*buf, d.dataSize)
	*buf = binary.BigEndian.AppendUint64(*buf, d.datOffset)
	*buf = binary.BigEndian.AppendUint32(*buf, d.dataCheckSum)
}

func (d *logDisk) parse(buf []byte) error {
	if len(buf) < logDiskSize {
		return fmt.Errorf("logDisk too short")
	}
	d.idxCheckSum = binary.BigEndian.Uint32(buf[:4])
	d.logIndex = binary.BigEndian.Uint64(buf[4:])
	d.logTerm = binary.BigEndian.Uint64(buf[12:])
	d.dataSize = binary.BigEndian.Uint64(buf[20:])
	d.datOffset = binary.BigEndian.Uint64(buf[28:])
	d.dataCheckSum = binary.BigEndian.Uint32(buf[36:])
	return nil
}

type logSet struct {
	idx        *os.File
	dat        *os.File
	onlyAppend bool
}

func openLogSet(dir string, name string, write bool) (*logSet, error) {
	var (
		s    = &logSet{onlyAppend: write}
		err  error
		flag int
	)
	if write {
		flag = os.O_CREATE | os.O_APPEND | os.O_WRONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR
	}
	s.idx, err = os.OpenFile(filepath.Join(dir, name+".idx"), flag, 0644)
	if err != nil {
		return nil, err
	}
	s.dat, err = os.OpenFile(filepath.Join(dir, name+".dat"), flag, 0644)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func openLogSegFile(dir, name string, segCount int) (*logSet, error) {
	var (
		s    = &logSet{onlyAppend: false}
		err  error
		flag = os.O_RDONLY
	)
	s.idx, err = os.OpenFile(filepath.Join(dir, name+".idx.seg."+strconv.Itoa(segCount)), flag, 0644)
	if err != nil {
		return nil, err
	}
	s.dat, err = os.OpenFile(filepath.Join(dir, name+".dat.seg."+strconv.Itoa(segCount)), flag, 0644)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *logSet) dataSize() (int64, error) {
	if s.onlyAppend {
		endOff, err := s.dat.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		return endOff, nil
	}
	fi, err := s.dat.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (s *logSet) close() error {
	err := s.idx.Close()
	if err != nil {
		return err
	}
	err = s.dat.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *logSet) closeAndRenameSeg(n int) error {
	idxName := s.idx.Name()
	datName := s.dat.Name()
	err := s.idx.Close()
	if err != nil {
		return err
	}
	err = s.dat.Close()
	if err != nil {
		return err
	}
	err = os.Rename(idxName, idxName+".seg."+strconv.Itoa(n))
	if err != nil {
		return err
	}
	err = os.Rename(datName, datName+".seg."+strconv.Itoa(n))
	if err != nil {
		return err
	}
	return nil
}

func (s *logSet) write(entries []*raft.Entry) error {
	idxBuf := make([]byte, 0, logDiskSize*len(entries))
	idxEntries := make([]*logDisk, 0, len(entries))
	endSeek, err := s.dat.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		writeCount, err2 := s.dat.Write(entry.Command)
		if err2 != nil {
			err = err2
			return err
		}
		if writeCount != len(entry.Command) {
			err = fmt.Errorf("write count not equal %d", writeCount)
			return err
		}
		idxEntries = append(idxEntries, &logDisk{
			logIndex:     entry.LogIndex,
			logTerm:      entry.Term,
			dataCheckSum: crc32.ChecksumIEEE(entry.Command),
			datOffset:    uint64(endSeek),
			dataSize:     uint64(len(entry.Command)),
		})
		endSeek += int64(len(entry.Command))
	}
	err = s.dat.Sync()
	if err != nil {
		return err
	}
	checkSumBuf := make([]byte, logDiskSize-4)
	for _, idxEntry := range idxEntries {
		idxEntry.checkSum(&checkSumBuf)
		idxEntry.writeToBuf(&idxBuf)
	}
	writeCount, err := s.idx.Write(idxBuf)
	if err != nil {
		return err
	}
	if writeCount != len(idxBuf) {
		err = fmt.Errorf("write count not equal %d", writeCount)
		return err
	}
	err = s.idx.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (s *logSet) len() (int64, error) {
	fi, err := s.dat.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size() / logDiskSize, nil
}

func (s *logSet) first(onlyIdx bool) (*raft.Entry, error) {
	return s.readOff(0, onlyIdx)
}

func (s *logSet) last(onlyIdx bool) (*raft.Entry, error) {
	info, err := s.idx.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, nil
	}
	if info.Size() < logDiskSize {
		return nil, fmt.Errorf("data corrupted")
	}
	off := (info.Size() / logDiskSize) - 1
	return s.readOff(int(off), onlyIdx)
}

func (s *logSet) batchRead(startOff, count int, onlyIdx bool) ([]*raft.Entry, error) {
	var (
		offset      = startOff * logDiskSize
		buf         = make([]byte, logDiskSize*count)
		entries     = make([]*raft.Entry, 0, count)
		logDiskList = make([]*logDisk, 0, count)
	)
	readCount, err := s.idx.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	if err == io.EOF {
		if readCount > 0 {
			buf = buf[:readCount]
		} else {
			return nil, nil
		}
	}
	for len(buf) > 0 {
		var d logDisk
		err = d.parse(buf[:logDiskSize])
		if err != nil {
			return nil, err
		}
		idxCk := crc32.ChecksumIEEE(buf[4:])
		if idxCk != d.idxCheckSum {
			err = fmt.Errorf("read idx checksum not equal %d", d.idxCheckSum)
			return nil, err
		}
		logDiskList = append(logDiskList, &d)
		entries = append(entries, &raft.Entry{
			Term:     d.logTerm,
			LogIndex: d.logIndex,
		})
	}
	if onlyIdx {
		return entries, nil
	}
	return entries, nil
}

func (s *logSet) readOff(idx int, onlyIdx bool) (*raft.Entry, error) {
	var (
		off = idx * logDiskSize
		buf = make([]byte, logDiskSize)
		d   logDisk
	)
	readCount, err := s.idx.ReadAt(buf, int64(off))
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if readCount == 0 {
		return nil, nil
	}
	if readCount != len(buf) {
		err = fmt.Errorf("read count not equal %d", readCount)
		return nil, err
	}
	err = d.parse(buf)
	if err != nil {
		return nil, err
	}
	idxCk := crc32.ChecksumIEEE(buf[4:])
	if idxCk != d.idxCheckSum {
		err = fmt.Errorf("read idx checksum not equal %d", d.idxCheckSum)
		return nil, err
	}
	if onlyIdx {
		return &raft.Entry{
			Term:     d.logTerm,
			LogIndex: d.logIndex,
			Command:  nil,
		}, nil
	}
	dataBuf := make([]byte, d.dataSize)
	readCount, err = s.dat.ReadAt(dataBuf, int64(d.datOffset))
	if err != nil {
		return nil, err
	}
	if readCount != int(d.dataSize) {
		err = fmt.Errorf("read count not equal %d", readCount)
		return nil, err
	}
	dataCk := crc32.ChecksumIEEE(dataBuf)
	if dataCk != d.dataCheckSum {
		err = fmt.Errorf("read dat checksum not equal %d", d.dataCheckSum)
		return nil, err
	}
	e := &raft.Entry{
		LogIndex: d.logIndex,
		Term:     d.logTerm,
		Command:  dataBuf,
	}
	return e, nil
}

type raftLogManager struct {
	mu           sync.RWMutex
	lastLogIndex uint64
	lastLogTerm  uint64
	dirPath      string
	logName      string
	r            *logSet
	w            *logSet
	sm           StateMachine
	maxLogSeg    int
}

func newRaftLogManager(dirPath string, logName string, sm StateMachine) *raftLogManager {
	return &raftLogManager{
		dirPath: dirPath,
		logName: logName,
		sm:      sm,
	}
}

func (mgr *raftLogManager) init() error {
	var err error
	mgr.r, err = openLogSet(mgr.dirPath, mgr.logName, false)
	if err != nil {
		return err
	}
	mgr.w, err = openLogSet(mgr.dirPath, mgr.logName, true)
	if err != nil {
		return err
	}
	entry, err := mgr.r.last(true)
	if err != nil {
		return err
	}
	if entry != nil {
		mgr.lastLogIndex = entry.LogIndex
		mgr.lastLogTerm = entry.Term
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
	// TODO 未提交完的数据? logIndex > lastCommitIndex
	mgr.startBgLogSegClean()
	return nil
}

func (mgr *raftLogManager) getSegFileName(segCount int) (string, string) {
	idx := fmt.Sprintf("%s.idx.seg.%d", mgr.logName, segCount)
	dat := fmt.Sprintf("%s.dat.seg.%d", mgr.logName, segCount)
	return idx, dat
}

func (mgr *raftLogManager) getSegList() ([]int, error) {
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
	slices.Sort(segList)
	return segList, nil
}

func (mgr *raftLogManager) initSeg() error {
	segList, err := mgr.getSegList()
	if err != nil {
		return err
	}
	if len(segList) == 0 {
		return nil
	}
	mgr.maxLogSeg = segList[len(segList)-1]
	if mgr.lastLogIndex != 0 {
		return nil
	}
	segFs, err := openLogSegFile(mgr.dirPath, mgr.logName, segList[len(segList)-1])
	if err != nil {
		return err
	}
	entry, err := segFs.last(true)
	if err != nil {
		return err
	}
	mgr.lastLogIndex = entry.LogIndex
	mgr.lastLogTerm = entry.Term
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
	mgr.w, err = openLogSet(mgr.dirPath, mgr.logName, true)
	if err != nil {
		return err
	}
	mgr.r, err = openLogSet(mgr.dirPath, mgr.logName, false)
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

func (mgr *raftLogManager) cleanLogSeg() {
	defer func() {
		err := recover()
		if err != nil {
			return
		}
	}()
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	segList, err := mgr.getSegList()
	if err != nil {
		return
	}
	entry, err := mgr.r.first(true)
	if err != nil {
		return
	}
	if entry == nil {
		if len(segList) > 1 {
			for _, segC := range segList[:len(segList)-1] {
				idxFileName, datFileName := mgr.getSegFileName(segC)
				os.Remove(filepath.Join(mgr.dirPath, idxFileName))
				os.Remove(filepath.Join(mgr.dirPath, datFileName))
			}
		}
	} else {
		for _, segC := range segList {
			idxFileName, datFileName := mgr.getSegFileName(segC)
			os.Remove(filepath.Join(mgr.dirPath, idxFileName))
			os.Remove(filepath.Join(mgr.dirPath, datFileName))
		}
	}
}

func (mgr *raftLogManager) getLastCommitIndex(ctx *context.Context) (uint64, error) {
	return mgr.sm.LastCommit(ctx)
}

func (mgr *raftLogManager) getLastLogIndex(ctx *context.Context) uint64 {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.lastLogIndex
}

func (mgr *raftLogManager) getLastLogTerm(ctx *context.Context) uint64 {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.lastLogTerm
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
	mgr.lastLogIndex += uint64(numEntries)
	mgr.lastLogTerm = entries[len(entries)-1].Term
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

func (mgr *raftLogManager) commitLogWithOff(ctx *context.Context, start, end uint64) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if start == end {
		return nil
	}
	entry, err := mgr.r.first(true)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil
	}
	startOff := start - entry.LogIndex
	endOff := end - entry.LogIndex
	// TODO 支持批量, 优化性能
	for i := startOff; i < endOff; i++ {
		entry, err = mgr.r.readOff(int(i), false)
		if err != nil {
			return err
		}
		if entry == nil {
			return nil
		}
		err = mgr.doApplyLog2UserSm(ctx, []*raft.Entry{entry})
		if err != nil {
			return err
		}
	}
	return nil
}

func (mgr *raftLogManager) flushUnCommitLog2UserSm(ctx *context.Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	userSmLastCommit, err := mgr.sm.LastCommit(ctx)
	if err != nil {
		return err
	}
	if mgr.lastLogIndex == userSmLastCommit {
		return nil
	}
	firstEntry, err := mgr.r.first(true)
	if err != nil {
		return err
	}
	lastEntry, err := mgr.r.last(true)
	if err != nil {
		return err
	}
	count := int64(lastEntry.LogIndex) - int64(userSmLastCommit)
	if count < 0 {
		return nil
	}
	startOff := int64(userSmLastCommit - firstEntry.LogIndex)
	for i := int64(0); i < count; i++ {
		entry, err := mgr.r.readOff(int(startOff+i), false)
		if err != nil {
			return err
		}
		err = mgr.sm.Apply(ctx, []*raft.Entry{entry})
		if err != nil {
			return err
		}
	}
	return nil
}
