package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/nyan233/raft/pb/message/raft"
)

const (
	logDiskSize = 4 + 8*4 + 4
)

type diskListItem struct {
	idxCheckSum  uint32
	logIndex     uint64
	logTerm      uint64
	dataSize     uint64
	dataOffset   uint64
	dataCheckSum uint32
	readData     []byte
}

func (d *diskListItem) checkSum(buf *[]byte) {
	checkSumBuf := *buf
	binary.BigEndian.PutUint64(checkSumBuf[:8], d.logIndex)
	binary.BigEndian.PutUint64(checkSumBuf[8:], d.logTerm)
	binary.BigEndian.PutUint64(checkSumBuf[16:], d.dataSize)
	binary.BigEndian.PutUint64(checkSumBuf[24:], d.dataOffset)
	binary.BigEndian.PutUint32(checkSumBuf[32:], d.dataCheckSum)
	checkSum := crc32.ChecksumIEEE(checkSumBuf)
	d.idxCheckSum = checkSum
}

func (d *diskListItem) writeToBuf(buf *[]byte) {
	*buf = binary.BigEndian.AppendUint32(*buf, d.idxCheckSum)
	*buf = binary.BigEndian.AppendUint64(*buf, d.logIndex)
	*buf = binary.BigEndian.AppendUint64(*buf, d.logTerm)
	*buf = binary.BigEndian.AppendUint64(*buf, d.dataSize)
	*buf = binary.BigEndian.AppendUint64(*buf, d.dataOffset)
	*buf = binary.BigEndian.AppendUint32(*buf, d.dataCheckSum)
}

func (d *diskListItem) parse(buf []byte) error {
	if len(buf) < logDiskSize {
		return fmt.Errorf("diskListItem too short")
	}
	d.idxCheckSum = binary.BigEndian.Uint32(buf[:4])
	d.logIndex = binary.BigEndian.Uint64(buf[4:])
	d.logTerm = binary.BigEndian.Uint64(buf[12:])
	d.dataSize = binary.BigEndian.Uint64(buf[20:])
	d.dataOffset = binary.BigEndian.Uint64(buf[28:])
	d.dataCheckSum = binary.BigEndian.Uint32(buf[36:])
	return nil
}

type diskList struct {
	idx        *os.File
	dat        *os.File
	onlyAppend bool
}

func openDiskList(dir string, name string, write bool) (*diskList, error) {
	var (
		s    = &diskList{onlyAppend: write}
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

func openDiskListSegFile(dir, name string, segCount int) (*diskList, error) {
	var (
		s    = &diskList{onlyAppend: false}
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

func (s *diskList) dataSize() (int64, error) {
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

func (s *diskList) close() error {
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

func (s *diskList) closeAndRenameSeg(n int) error {
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

func (s *diskList) Remove() error {
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
	err = os.Remove(idxName)
	if err != nil {
		return err
	}
	err = os.Remove(datName)
	if err != nil {
		return err
	}
	return nil
}

func (s *diskList) write(entries []*raft.Entry) error {
	idxBuf := make([]byte, 0, logDiskSize*len(entries))
	idxEntries := make([]*diskListItem, 0, len(entries))
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
		idxEntries = append(idxEntries, &diskListItem{
			logIndex:     entry.LogIndex,
			logTerm:      entry.Term,
			dataCheckSum: crc32.ChecksumIEEE(entry.Command),
			dataOffset:   uint64(endSeek),
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

func (s *diskList) len() (int64, error) {
	fi, err := s.idx.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size() / logDiskSize, nil
}

func (s *diskList) first(onlyIdx bool) (*raft.Entry, error) {
	return s.readOff(0, onlyIdx)
}

func (s *diskList) last(onlyIdx bool) (*raft.Entry, error) {
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

func (s *diskList) batchRead(startOff, count int, onlyIdx bool) ([]*raft.Entry, error) {
	var (
		offset      = startOff * logDiskSize
		buf         = make([]byte, logDiskSize*count)
		entries     = make([]*raft.Entry, 0, count)
		logDiskList = make([]*diskListItem, 0, count)
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
		var d diskListItem
		err = d.parse(buf[:logDiskSize])
		if err != nil {
			return nil, err
		}
		idxCk := crc32.ChecksumIEEE(buf[4:logDiskSize])
		if idxCk != d.idxCheckSum {
			err = fmt.Errorf("read idx checksum not equal %d", d.idxCheckSum)
			return nil, err
		}
		logDiskList = append(logDiskList, &d)
		entries = append(entries, &raft.Entry{
			Term:     d.logTerm,
			LogIndex: d.logIndex,
		})
		buf = buf[logDiskSize:]
	}
	if onlyIdx {
		return entries, nil
	}
	firstLogDisk := logDiskList[0]
	if len(entries) == 1 {
		entry := entries[0]
		buf = make([]byte, firstLogDisk.dataSize)
		_, err = s.dat.ReadAt(buf, int64(firstLogDisk.dataOffset))
		if err != nil {
			return nil, err
		}
		if crc32.ChecksumIEEE(buf) != firstLogDisk.dataCheckSum {
			err = fmt.Errorf("read dat checksum not equal %d", firstLogDisk.dataCheckSum)
			return nil, err
		}
		entry.Command = buf
	}
	batchStart := firstLogDisk.dataOffset
	batchEnd := firstLogDisk.dataOffset + firstLogDisk.dataSize
	startParse := 0
	dataBuf := make([]byte, 0, 1024)
	batchReadFn := func(currentIndex int) error {
		dataBufSize := batchEnd - batchStart
		if uint64(cap(dataBuf)) < dataBufSize {
			dataBuf = make([]byte, 0, dataBufSize)
		}
		dataBuf = dataBuf[:dataBufSize]
		readCount, err = s.dat.ReadAt(dataBuf, int64(batchStart))
		if err != nil {
			return err
		}
		for k, v := range logDiskList[startParse : currentIndex+1] {
			commandData := dataBuf[:v.dataSize]
			if crc32.ChecksumIEEE(commandData) != v.dataCheckSum {
				err = fmt.Errorf("read dat checksum not equal %d", v.dataCheckSum)
				return err
			}
			entry := entries[k]
			entry.Command = append(entry.Command, commandData...)
			dataBuf = dataBuf[v.dataSize:]
		}
		return nil
	}
	for i := 1; i < len(logDiskList); i++ {
		d := logDiskList[i]
		if d.dataOffset == batchEnd {
			batchEnd += d.dataSize
		} else {
			if err = batchReadFn(i); err != nil {
				return nil, err
			}
			batchStart = d.dataOffset
			batchEnd = d.dataOffset + d.dataSize
			startParse = i
		}
		// 兜一下底
		if startParse == len(logDiskList)-1 || i == len(logDiskList)-1 {
			if err = batchReadFn(i); err != nil {
				return nil, err
			}
		}
	}
	return entries, nil
}

func (s *diskList) truncate(count int) error {
	truncateSize := logDiskSize * count
	fi, err := s.idx.Stat()
	if err != nil {
		return err
	}
	idxSize := fi.Size()
	return s.idx.Truncate(idxSize - int64(truncateSize))
}

func (s *diskList) readOff(idx int, onlyIdx bool) (*raft.Entry, error) {
	var (
		off = idx * logDiskSize
		buf = make([]byte, logDiskSize)
		d   diskListItem
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
	readCount, err = s.dat.ReadAt(dataBuf, int64(d.dataOffset))
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
