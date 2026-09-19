//go:build windows

package raft

import (
	"encoding/json"
	"errors"
	"io"
	"os"

	"golang.org/x/sys/windows"
)

func (m *meta) openFile(create bool) (*os.File, error) {
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}

	return os.OpenFile(m.path, flag, 0644)
}

func (m *meta) init() error {
initLogic:
	file, err := m.openFile(false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			file2, err2 := os.OpenFile(m.path+".new", os.O_RDWR, 0644)
			if err2 == nil {
				// Windows 下 rename / replace 前必须先把句柄关掉。
				if err2 = file2.Close(); err2 != nil {
					return err2
				}

				if err2 = moveFileReplace(m.path+".new", m.path); err2 != nil {
					return err2
				}

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
	if err = json.Unmarshal(bytes, &md); err != nil {
		return err
	}

	m.d = &md
	return nil
}

func (m *meta) save(md metaData) error {
	newFile, err := os.OpenFile(
		m.path+".new",
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return err
	}

	jsBytes, err := json.Marshal(md)
	if err != nil {
		_ = newFile.Close()
		return err
	}

	if _, err = newFile.Write(jsBytes); err != nil {
		_ = newFile.Close()
		return err
	}

	if err = newFile.Sync(); err != nil {
		_ = newFile.Close()
		return err
	}

	if err = newFile.Close(); err != nil {
		return err
	}

	if err = moveFileReplace(m.path+".new", m.path); err != nil {
		return err
	}

	m.d = &md
	return nil
}

func moveFileReplace(oldPath, newPath string) error {
	oldPtr, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}

	newPtr, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}

	return windows.MoveFileEx(
		oldPtr,
		newPtr,
		windows.MOVEFILE_REPLACE_EXISTING|
			windows.MOVEFILE_WRITE_THROUGH,
	)
}
