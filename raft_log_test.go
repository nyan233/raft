package raft

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"

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
			entry, err := r.readOff(int(logIdx))
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
