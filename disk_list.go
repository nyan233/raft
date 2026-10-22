package raft

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/nyan233/raft/pb/message/raft"
)

const (
	logDiskSize     = 4 + 8*4 + 4
	fileDataSizeMax = 1024 * 1024 * 1024
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
		flag = os.O_RDWR
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

func segFileUp2MainFile(dir, name string, segCount int) error {
	segIdxName := filepath.Join(dir, name+".idx.seg."+strconv.Itoa(segCount))
	segDatName := filepath.Join(dir, name+".dat.seg."+strconv.Itoa(segCount))
	mainIdxName := filepath.Join(dir, name+".idx")
	mainDatName := filepath.Join(dir, name+".dat")
	err := os.Rename(segIdxName, mainIdxName)
	if err != nil {
		return err
	}
	err = os.Rename(segDatName, mainDatName)
	if err != nil {
		return err
	}
	return nil
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
	return s.idx.Truncate(int64(truncateSize))
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

type diskListSegFile struct {
	seg int
	// seg文件原则上不允许写, 只允许删除/截断/读这些操作
	r *diskList
}

type diskListReaderWriter struct {
	r           *diskList
	w           *diskList
	dir         string
	name        string
	segNextPtr  int
	segFileList []*diskListSegFile
	dataSizeMax int64
}

func newDiskListReaderWriter(dir string, name string, dataSizeMax int64) *diskListReaderWriter {
	d := &diskListReaderWriter{
		dir:         dir,
		name:        name,
		dataSizeMax: dataSizeMax,
	}
	if d.dataSizeMax == 0 {
		d.dataSizeMax = fileDataSizeMax
	}
	return d
}

func (d *diskListReaderWriter) init() error {
	err := d.initFile()
	if err != nil {
		return err
	}
	return d.initSegFile()
}

func (d *diskListReaderWriter) initFile() error {
	var (
		openFn = func(dir string, name string, write bool) (*diskList, error) {
			return openDiskList(dir, name, write)
		}
		err error
	)
	d.r, err = openFn(d.dir, d.name, false)
	if err != nil {
		return err
	}
	d.w, err = openFn(d.dir, d.name, true)
	if err != nil {
		return err
	}
	return nil
}

func (d *diskListReaderWriter) initSegFile() error {
	segList, err := d.getSegList(true)
	if err != nil {
		return err
	}
	if len(segList) == 0 {
		return nil
	}
	d.segNextPtr = segList[0] + 1
	for _, seg := range segList {
		segFile, err := openDiskListSegFile(d.dir, d.name, seg)
		if err != nil {
			return err
		}
		d.segFileList = append(d.segFileList, &diskListSegFile{
			seg: seg,
			r:   segFile,
		})
	}
	return nil
}

func (d *diskListReaderWriter) close() error {
	err := d.r.close()
	if err != nil {
		return err
	}
	err = d.w.close()
	if err != nil {
		return err
	}
	for _, segFile := range d.segFileList {
		err = segFile.r.close()
		if err != nil {
			return err
		}
	}
	d.segFileList = nil
	return nil
}

func (d *diskListReaderWriter) getSegList(descSort bool) ([]int, error) {
	dirEntry, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, err
	}
	subStr := d.name + ".dat.seg."
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

func (d *diskListReaderWriter) newScope(s, e uint64) *listScope3 {
	return newListScope3(d, s, e)
}

func (d *diskListReaderWriter) appendSegFile(seg int) error {
	f, err := openDiskListSegFile(d.dir, d.name, seg)
	if err != nil {
		return err
	}
	d.segFileList = append(d.segFileList, &diskListSegFile{
		seg: seg,
		r:   f,
	})
	slices.SortFunc(d.segFileList, func(a, b *diskListSegFile) int {
		return cmp.Compare(b.seg, a.seg)
	})
	return nil
}

func (d *diskListReaderWriter) removeSegFile(seg int) error {
	var f *diskListSegFile
	d.segFileList = slices.DeleteFunc(d.segFileList, func(file *diskListSegFile) bool {
		if file.seg == seg {
			f = file
		}
		return file.seg == seg
	})
	if f != nil {
		return f.r.Remove()
	}
	return nil
}

func (d *diskListReaderWriter) cleanWithLogIndex(e uint64) error {
	removeSeg := make([]int, 0, 8)
	for _, segFile := range d.segFileList {
		lastEntry, err := segFile.r.last(true)
		if err != nil {
			return err
		}
		if lastEntry.LogIndex <= e {
			removeSeg = append(removeSeg, segFile.seg)
		}
	}
	for _, seg := range removeSeg {
		err := d.removeSegFile(seg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *diskListReaderWriter) truncateWithLogIndex(end uint64) error {
	type rwFile struct {
		r   *diskList
		w   *diskList
		seg int
	}
	lastEntry, err := d.last(true)
	if err != nil {
		return err
	}
	if lastEntry.LogIndex < end {
		return errors.New("endIndex out of range")
	}
	allFile := make([]*rwFile, 0, 8)
	allFile = append(allFile, &rwFile{
		r: d.r,
		w: d.w,
	})
	for _, segF := range d.segFileList {
		allFile = append(allFile, &rwFile{
			r:   segF.r,
			w:   segF.r,
			seg: segF.seg,
		})
	}
	removeIdx := -1
	for idx, file := range allFile {
		firstEntry, err := file.r.first(true)
		if err != nil {
			return err
		}
		if firstEntry == nil {
			if file.r == d.r {
				continue
			}
			return errors.New("corrupt file")
		}
		if firstEntry.LogIndex > end {
			err = file.w.Remove()
			if err != nil {
				return err
			}
			removeIdx = idx
		} else {
			err = file.w.truncate(int(end-firstEntry.LogIndex) + 1)
			if err != nil {
				return err
			}
			break
		}
	}
	allFile = allFile[removeIdx+1:]
	if removeIdx >= 0 {
		if len(allFile) > 0 {
			err := segFileUp2MainFile(d.dir, d.name, allFile[0].seg)
			if err != nil {
				return err
			}
		}
		err := d.initFile()
		if err != nil {
			return err
		}
		if removeIdx > 0 {
			d.segFileList = d.segFileList[removeIdx:]
		}
	}
	return nil
}

func (d *diskListReaderWriter) writeBatch(entries []*raft.Entry) error {
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
		err := d.w.write(wbEntries[:count])
		if err != nil {
			return err
		}
		wbEntries = wbEntries[count:]
	}
	dataSize, err := d.w.dataSize()
	if err != nil {
		return err
	}
	// 1GB
	if dataSize > d.dataSizeMax {
		err = d.r.close()
		if err != nil {
			return err
		}
		err = d.w.closeAndRenameSeg(d.segNextPtr)
		if err != nil {
			return err
		}
		err = d.appendSegFile(d.segNextPtr)
		if err != nil {
			return err
		}
		d.segNextPtr++
		err = d.initFile()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *diskListReaderWriter) first(onlyIdx bool) (*raft.Entry, error) {
	if len(d.segFileList) == 0 {
		return d.r.first(onlyIdx)
	}
	return d.segFileList[len(d.segFileList)-1].r.first(onlyIdx)
}

func (d *diskListReaderWriter) last(onlyIdx bool) (*raft.Entry, error) {
	entry, err := d.r.last(onlyIdx)
	if err != nil {
		return nil, err
	}
	if entry != nil {
		return entry, nil
	}
	return d.segFileList[0].r.last(onlyIdx)
}
