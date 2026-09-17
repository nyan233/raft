package raft

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type metaData struct {
	VoteFor string `json:"vote_for"`
	Term    uint64 `json:"term"`
}

type meta struct {
	d    *metaData
	path string
}

func newMeta(path string) *meta {
	return &meta{
		path: path,
	}
}

func (m *meta) init() error {
	//err := m.prevOpenFile()
	//if err != nil {
	//	if errors.Is(err, os.ErrNotExist) {
	//		newFile, err2 := os.OpenFile(m.path+".new", os.O_RDWR, 0644)
	//		if err2 == nil {
	//			// 完成未成功的rename
	//			err2 = newFile.Close()
	//			if err2 != nil {
	//				return err2
	//			}
	//			err2 = os.Rename(m.path+".new", m.path)
	//			if err2 != nil {
	//				return err2
	//			}
	//			err2 = m.prevOpenFile()
	//			if err2 != nil {
	//				return err2
	//			}
	//		} else if errors.Is(err2, os.ErrNotExist) {
	//			err2 = m.openFile()
	//			if err2 != nil {
	//				return err2
	//			}
	//		} else {
	//			return err2
	//		}
	//	} else {
	//		return err
	//	}
	//}
initLogic:
	file, err := m.openFile(false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			file2, err2 := os.OpenFile(m.path+".new", os.O_RDWR, 0644)
			if err2 == nil {
				err2 = os.Rename(m.path+".new", m.path)
				if err2 != nil {
					file2.Close()
					return err2
				}
				file2.Close()
				goto initLogic
			} else if errors.Is(err2, os.ErrNotExist) {
				file, err = m.openFile(true)
				if err != nil {
					return err
				}
			} else {
				return err2
			}
		} else {
			return err
		}
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		m.d = &metaData{}
		return nil
	}
	var md metaData
	err = json.Unmarshal(bytes, &md)
	if err != nil {
		return err
	}
	m.d = &md
	return nil
}

func (m *meta) openFile(create bool) (*os.File, error) {
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}
	file, err := os.OpenFile(m.path, flag, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (m *meta) save(md metaData) error {
	newFile, err := os.OpenFile(m.path+".new", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer newFile.Close()
	jsBytes, err := json.Marshal(md)
	if err != nil {
		return err
	}
	_, err = newFile.Write(jsBytes)
	if err != nil {
		return err
	}
	err = newFile.Sync()
	if err != nil {
		return err
	}
	err = newFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(m.path+".new", m.path)
	if err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(m.path))
	if err != nil {
		return err
	}
	defer dir.Close()

	if err := dir.Sync(); err != nil {
		return err
	}

	m.d = &md
	return nil
}

func (m *meta) close() error {
	m.d = nil
	return nil
}

func (m *meta) get() *metaData {
	return m.d
}

func (m *meta) termIncr() (uint64, error) {
	m.d.Term++
	return m.d.Term, m.save(*m.d)
}
