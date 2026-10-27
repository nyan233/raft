package raft

import (
	"testing"

	"github.com/nyan233/raft/pb/message/raft"
)

func TestDiskList(t *testing.T) {
	d := newDiskListReaderWriter("test", "rw.test.l", 1024)
	err := d.init()
	if err != nil {
		t.Fatal(err)
	}
	err = d.TruncateWithLogIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		entries := make([]*raft.Entry, 0, 100)
		for j := i * 50; j < (i+1)*50; j++ {
			entries = append(entries, &raft.Entry{
				Term:        uint64(i),
				LogIndex:    uint64(j),
				CommandType: uint32(raft.CommandType_UserCommand),
				Command:     []byte("hello world, my is a raft lib"),
			})
		}
		err = d.WriteBatch(entries)
		if err != nil {
			t.Fatal(err)
		}
	}
	lastEntry, err := d.Last(false)
	if err != nil {
		t.Fatal(err)
	}
	firstEntry, err := d.First(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(firstEntry)
	t.Log(lastEntry)
	err = d.CleanWithLogIndex(2048)
	if err != nil {
		t.Fatal(err)
	}
	firstEntry, err = d.First(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(firstEntry)
	err = d.TruncateWithLogIndex(4500)
	if err != nil {
		t.Fatal(err)
	}
	lastEntry, err = d.Last(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(lastEntry)
	scope := d.Scope(4000, 4500)
	err = scope.Find()
	if err != nil {
		t.Fatal(err)
	}
	err = scope.RangeFor(func(f *diskList, startOff, count uint64) error {
		entries, err := f.batchRead(int(startOff), int(count), false)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(len(entries))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
