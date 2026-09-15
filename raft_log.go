package raft

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"

	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

const (
	logDiskSize = 4 + 8*4 + 4
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
	idx *os.File
	dat *os.File
}

func openLogSet(dir string, name string, write bool) (*logSet, error) {
	var (
		s    = new(logSet)
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
	currentSeek, err := s.dat.Seek(0, io.SeekCurrent)
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
			datOffset:    uint64(currentSeek),
			dataSize:     uint64(len(entry.Command)),
		})
		currentSeek += int64(len(entry.Command))
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

func (s *logSet) first() (*raft.Entry, error) {
	return s.readOff(0)
}

func (s *logSet) last() (*raft.Entry, error) {
	info, err := s.idx.Stat()
	if err != nil {
		return nil, err
	}
	off := info.Size() / logDiskSize
	return s.readOff(int(off))
}

func (s *logSet) readOff(idx int) (*raft.Entry, error) {
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
	mu              sync.RWMutex
	lastLogIndex    uint64
	lastLogTerm     uint64
	lastCommitIndex uint64
	dirPath         string
	logName         string
	r               *logSet
	w               *logSet
	sm              StateMachine
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
	entry, err := mgr.r.last()
	if err != nil {
		return err
	}
	if entry != nil {
		mgr.lastLogIndex = entry.LogIndex
		mgr.lastLogTerm = entry.Term
	}
	ctx := context.Background()
	err = mgr.sm.Init(ctx)
	if err != nil {
		return err
	}
	mgr.lastCommitIndex, err = mgr.sm.LastCommit(ctx)
	if err != nil {
		return err
	}
	// TODO 未提交完的数据? logIndex > lastCommitIndex
	return nil
}

func (mgr *raftLogManager) applyLog(ctx *context.Context, entries []*raft.Entry) error {
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
		maxTerm    = slices.MaxFunc(entries, func(a, b *raft.Entry) int {
			return cmp.Compare(a.Term, b.Term)
		}).Term
	)
	for len(wbEntries) > 0 {
		count = OneMaxCount
		if len(wbEntries) < OneMaxCount {
			count = len(entries)
		}
		err := mgr.w.write(wbEntries[:count])
		if err != nil {
			return err
		}
		wbEntries = wbEntries[count:]
	}
	count = 0
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
	mgr.lastLogIndex += uint64(numEntries)
	mgr.lastLogTerm = maxTerm
	lastCommitIndex, err := mgr.sm.LastCommit(ctx)
	if err != nil {
		return err
	}
	mgr.lastCommitIndex = lastCommitIndex
	return nil
}
