package raft

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/raft/pb/message/raft"
)

func TestRaftLog(t *testing.T) {
	os.Remove(filepath.Join("test", "testlog1.idx"))
	os.Remove(filepath.Join("test", "testlog1.dat"))
	w, err := openLogSet("test", "testlog1", true)
	if err != nil {
		t.Fatal(err)
	}
	r, err := openLogSet("test", "testlog1", false)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		iErr := w.write([]*raft.Entry{
			{
				Term:     1,
				LogIndex: 0,
				Command:  []byte("1"),
			},
			{
				Term:     1,
				LogIndex: 1,
				Command:  []byte("2"),
			},
			{
				Term:     1,
				LogIndex: 2,
				Command:  []byte("3"),
			},
			{
				Term:     1,
				LogIndex: 3,
				Command:  []byte("4"),
			},
		})
		if iErr != nil {
			panic(iErr)
		}
	}()
	go func() {
		defer wg.Done()
		var logIdx uint64
		for {
			entry, err := r.readOff(int(logIdx), false)
			if err != nil {
				panic(err)
			}
			if entry == nil {
				continue
			}
			if entry.LogIndex == logIdx {
				logIdx++
				slog.Info("read log",
					slog.Uint64("logIdx", entry.LogIndex),
					slog.Uint64("logTerm", entry.Term),
					slog.String("command", string(entry.Command)))
			}
			if logIdx == 3 {
				break
			}
		}
	}()
	wg.Wait()
}

type nilSm struct {
	lastCommitIndex uint64
}

func (n *nilSm) Init(ctx *context.Context) error {
	return nil
}

func (n *nilSm) LastCommit(ctx *context.Context) (uint64, error) {
	return n.lastCommitIndex, nil
}

func (n *nilSm) Apply(ctx *context.Context, entries []*raft.Entry) error {
	n.lastCommitIndex = entries[len(entries)-1].LogIndex
	return nil
}

func (n *nilSm) Snapshot(ctx *context.Context) ([]*raft.Entry, error) {
	return nil, nil
}

func TestLogScope(t *testing.T) {

}
