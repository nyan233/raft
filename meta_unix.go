//go:build unix

package raft

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

func (m *meta) init() error {
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
	defer file.Close()
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
