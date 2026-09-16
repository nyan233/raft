package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

// |8Byte-LastCommitIndex|8Byte-Value
type testUserSm struct {
	name string
	file *os.File
}

func newTestSm(name string) *testUserSm {
	return &testUserSm{
		name: name,
	}
}

func (t *testUserSm) Init(ctx *context.Context) error {
	var err error
	t.file, err = os.OpenFile(filepath.Join("test", t.name+".count"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (t *testUserSm) Read() (uint64, uint64, error) {
	buf := make([]byte, 16)
	_, err := t.file.Read(buf)
	if err != nil {
		return 0, 0, err
	}
	v1 := binary.BigEndian.Uint64(buf)
	v2 := binary.BigEndian.Uint64(buf[8:])
	return v1, v2, nil
}

func (t *testUserSm) LastCommit(ctx *context.Context) (uint64, error) {
	buf := make([]byte, 8)
	n, err := t.file.ReadAt(buf, 0)
	if err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, err
	}
	if n != 8 {
		return 0, fmt.Errorf("expected 8 bytes, got %d", n)
	}
	return binary.BigEndian.Uint64(buf), nil
}

func (t *testUserSm) Apply(ctx *context.Context, entries []*raft.Entry) error {
	defer t.file.Sync()
	for _, entry := range entries {
		lastCommitIdxBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(lastCommitIdxBuf, entry.LogIndex)
		_, err := t.file.WriteAt(lastCommitIdxBuf, 0)
		if err != nil {
			return err
		}
		_, err = t.file.WriteAt(entry.Command, 8)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *testUserSm) Snapshot(ctx *context.Context) ([]*raft.Entry, error) {
	//TODO implement me
	panic("implement me")
}

func (t *testUserSm) ReCall(ctx *context.Context, entries []*raft.Entry) error {
	//TODO implement me
	panic("implement me")
}

func TestCandidate(t *testing.T) {
	nodes := []string{
		"127.0.0.1:8000",
		"127.0.0.1:8001",
		"127.0.0.1:8002",
	}
	node1 := NewCoreSm(CoreSmConfig{
		My:         nodes[0],
		Membership: []string{nodes[1], nodes[2]},
		LogDir:     "test",
		LogName:    "node1",
		UserSmFactory: func() StateMachine {
			return newTestSm("node1")
		},
	})
	node2 := NewCoreSm(CoreSmConfig{
		My:         nodes[1],
		Membership: []string{nodes[0], nodes[2]},
		LogDir:     "test",
		LogName:    "node2",
		UserSmFactory: func() StateMachine {
			return newTestSm("node2")
		},
	})
	node3 := NewCoreSm(CoreSmConfig{
		My:         nodes[2],
		Membership: []string{nodes[0], nodes[1]},
		LogDir:     "test",
		LogName:    "node3",
		UserSmFactory: func() StateMachine {
			return newTestSm("node3")
		},
	})
	go node1.Init()
	go node2.Init()
	go node3.Init()
	time.Sleep(time.Second * 5)
	raftCli, err := NewClient(nodes[2])
	if err != nil {
		t.Fatal(err)
	}
	buf := make([][]byte, 0, 256)
	for i := 100; i < 200000; i++ {
		command := make([]byte, 8)
		binary.BigEndian.PutUint64(command, uint64(i))
		buf = append(buf, command)
	}
	const BatchSize = 50
	for len(buf) > 0 {
		count := BatchSize
		if len(buf) < count {
			count = len(buf)
		}
		err = raftCli.AppendCommands(context.Background(), buf[:count])
		if err != nil {
			t.Fatal(err)
		}
		buf = buf[count:]
	}
	sm := newTestSm("node3")
	err = sm.Init(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lastCommitIndex, count, err := sm.Read()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("lastCommitIndex=%d, count=%d", lastCommitIndex, count)
}

func TestSmRead(t *testing.T) {
	sm := newTestSm("node3")
	err := sm.Init(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lastCommitIndex, count, err := sm.Read()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("lastCommitIndex=%d, count=%d", lastCommitIndex, count)
}
