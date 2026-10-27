package raft

import (
	"fmt"

	"github.com/nyan233/raft/pb/message/raft"
)

type listScope3 struct {
	rw         *diskListReaderWriter
	startIndex uint64
	endIndex   uint64
	fileOff    []listScopeFileOff
}

func newListScope3(rw *diskListReaderWriter, startIndex, endIndex uint64) *listScope3 {
	return &listScope3{
		rw:         rw,
		startIndex: startIndex,
		endIndex:   endIndex,
		fileOff:    make([]listScopeFileOff, 0, 8),
	}
}

func (s *listScope3) findStart() (foundStart bool, err error) {
	firstEntry, err := s.rw.r.first(true)
	if err != nil {
		return false, err
	}
	lastEntry, err := s.rw.r.last(false)
	if err != nil {
		return false, err
	}
	appendFileOff := func(ls *diskList, first, last *raft.Entry) (foundStart bool) {
		fileOff := listScopeFileOff{
			ls:       ls,
			startOff: 0,
			endOff:   lastEntry.LogIndex - firstEntry.LogIndex,
			first:    first,
			last:     last,
		}
		if fileOff.inRegion(s.startIndex) {
			fileOff.startOff = s.startIndex - firstEntry.LogIndex
			s.fileOff = append(s.fileOff, fileOff)
			return true
		}
		s.fileOff = append(s.fileOff, fileOff)
		return false
	}
	if firstEntry != nil {
		if foundStart = appendFileOff(s.rw.r, firstEntry, lastEntry); foundStart {
			return
		}
	}
	err = s.rw.RangeSegFile(func(f *diskList) (bool, error) {
		firstEntry, err = f.first(false)
		if err != nil {
			return false, err
		}
		lastEntry, err = f.last(false)
		if err != nil {
			return false, err
		}
		if appendFileOff(f, firstEntry, lastEntry) {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return false, err
	}
	return false, nil
}

func (s *listScope3) findEnd() error {
	trimCount := 0
	for i := 0; i < len(s.fileOff); i++ {
		fileOff := s.fileOff[i]
		if fileOff.inRegion(s.endIndex) {
			break
		} else {
			// 超过当前文件的有的索引范围
			if i == 0 && s.endIndex > fileOff.last.LogIndex {
				return fmt.Errorf("end index is out of range, end index is %d, Last index is %d", s.endIndex, fileOff.last.LogIndex)
			}
			trimCount++
		}
	}
	s.fileOff = s.fileOff[trimCount:]
	s.fileOff[0].endOff = s.endIndex - s.fileOff[0].first.LogIndex
	return nil
}

func (s *listScope3) Find() error {
	if s.startIndex > s.endIndex {
		return fmt.Errorf("start index is out of range, end index is %d", s.endIndex)
	}
	foundStart, err := s.findStart()
	if err != nil {
		return err
	}
	if !foundStart {
		return fmt.Errorf("unknown file offset, startIndex=%d, endIndex=%d", s.startIndex, s.endIndex)
	}
	err = s.findEnd()
	if err != nil {
		return err
	}
	return nil
}

func (s *listScope3) ReadAll(onlyIdx bool) ([]*raft.Entry, error) {
	var (
		err        error
		entriesBuf = make([]*raft.Entry, 0, 64)
	)
	if len(s.fileOff) == 0 {
		err = s.Find()
		if err != nil {
			return nil, err
		}
	}
	err = s.RangeFor(func(f *diskList, startOff, count uint64) error {
		entries, err := f.batchRead(int(startOff), int(count), onlyIdx)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			entriesBuf = append(entriesBuf, entries...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entriesBuf, nil
}

func (s *listScope3) RangeFor(fn func(f *diskList, startOff, count uint64) error) error {
	for i := len(s.fileOff) - 1; i >= 0; i-- {
		ff := s.fileOff[i]
		err := fn(ff.ls, ff.startOff, (ff.endOff-ff.startOff)+1)
		if err != nil {
			return err
		}
	}
	return nil
}

type listScopeFileOff struct {
	ls       *diskList
	startOff uint64
	endOff   uint64
	first    *raft.Entry
	last     *raft.Entry
}

func (f *listScopeFileOff) inRegion(x uint64) bool {
	return f.first.LogIndex <= x && f.last.LogIndex >= x
}
