//go:build unix

package raft

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

type jsonFile[T any] struct {
	val  atomic.Pointer[T]
	path string
}

func newJSONFile[T any](path string) *jsonFile[T] {
	return &jsonFile[T]{
		path: path,
	}
}

func (j *jsonFile[T]) init() error {
initLogic:
	file, err := j.openFile(false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			file2, err2 := os.OpenFile(j.path+".new", os.O_RDWR, 0644)
			if err2 == nil {
				err2 = os.Rename(j.path+".new", j.path)
				if err2 != nil {
					file2.Close()
					return err2
				}
				file2.Close()
				goto initLogic
			} else if errors.Is(err2, os.ErrNotExist) {
				file, err = j.openFile(true)
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
		j.val.Store(new(T))
		return nil
	}
	var val T
	err = json.Unmarshal(bytes, &val)
	if err != nil {
		return err
	}
	j.val.Store(&val)
	return nil
}

func (j *jsonFile[T]) openFile(create bool) (*os.File, error) {
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}
	file, err := os.OpenFile(j.path, flag, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (j *jsonFile[T]) get() *T {
	return j.val.Load()
}

func (j *jsonFile[T]) save(val T) error {
	newFile, err := os.OpenFile(j.path+".new", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	jsBytes, err := json.Marshal(val)
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
	err = os.Rename(j.path+".new", j.path)
	if err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(j.path))
	if err != nil {
		return err
	}
	defer dir.Close()

	if err := dir.Sync(); err != nil {
		return err
	}

	j.val.Store(&val)
	return nil
}
