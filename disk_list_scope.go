package raft

import (
	"fmt"

	"github.com/nyan233/raft/pb/message/raft"
)

type listScope3 struct {
	m          *raftLogManager
	startIndex uint64
	endIndex   uint64
	fileOff    []listScopeFileOff
}

func newListScope3(m *raftLogManager, startIndex, endIndex uint64) *listScope3 {
	return &listScope3{
		m:          m,
		startIndex: startIndex,
		endIndex:   endIndex,
		fileOff:    make([]listScopeFileOff, 0, 8),
	}
}

func (s *listScope3) findStart() (foundStart bool, err error) {
	firstEntry, err := s.m.r.first(true)
	if err != nil {
		return false, err
	}
	lastEntry, err := s.m.r.last(false)
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
		if foundStart = appendFileOff(s.m.r, firstEntry, lastEntry); foundStart {
			return
		}
	}
	segList, err := s.m.getSegList(true)
	if err != nil {
		return false, err
	}
	for _, seg := range segList {
		ls, err := openDiskListSegFile(s.m.dirPath, s.m.logName, seg)
		if err != nil {
			return false, err
		}
		firstEntry, err = ls.first(false)
		if err != nil {
			return false, err
		}
		lastEntry, err = ls.last(false)
		if err != nil {
			return false, err
		}
		if appendFileOff(ls, firstEntry, lastEntry) {
			return true, nil
		}
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
				return fmt.Errorf("end index is out of range, end index is %d, last index is %d", s.endIndex, fileOff.last.LogIndex)
			}
			trimCount++
			if fileOff.ls != s.m.r {
				err := fileOff.ls.close()
				if err != nil {
					return err
				}
			}
		}
	}
	s.fileOff = s.fileOff[trimCount:]
	s.fileOff[0].endOff = s.endIndex - s.fileOff[0].first.LogIndex
	return nil
}

func (s *listScope3) find() error {
	if s.startIndex > s.endIndex {
		return fmt.Errorf("start index is out of range, end index is %d", s.endIndex)
	}
	foundStart, err := s.findStart()
	if err != nil {
		s.close()
		return err
	}
	if !foundStart {
		s.close()
		return fmt.Errorf("unknown file offset, startIndex=%d, endIndex=%d", s.startIndex, s.endIndex)
	}
	err = s.findEnd()
	if err != nil {
		s.close()
		return err
	}
	return nil
}

func (s *listScope3) rangeFor(fn func(f *diskList, startOff, count uint64) error) error {
	for i := len(s.fileOff) - 1; i >= 0; i-- {
		ff := s.fileOff[i]
		err := fn(ff.ls, ff.startOff, (ff.endOff-ff.startOff)+1)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *listScope3) close() error {
	for _, f := range s.fileOff {
		if f.ls != s.m.r {
			err := f.ls.close()
			if err != nil {
				return err
			}
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
