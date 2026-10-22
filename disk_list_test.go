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
	err = d.truncateWithLogIndex(0)
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
		err = d.writeBatch(entries)
		if err != nil {
			t.Fatal(err)
		}
	}
	lastEntry, err := d.last(false)
	if err != nil {
		t.Fatal(err)
	}
	firstEntry, err := d.first(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(firstEntry)
	t.Log(lastEntry)
	err = d.cleanWithLogIndex(2048)
	if err != nil {
		t.Fatal(err)
	}
	firstEntry, err = d.first(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(firstEntry)
	err = d.truncateWithLogIndex(4500)
	if err != nil {
		t.Fatal(err)
	}
	lastEntry, err = d.last(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(lastEntry)
	scope := d.newScope(4000, 4500)
	err = scope.find()
	if err != nil {
		t.Fatal(err)
	}
	err = scope.rangeFor(func(f *diskList, startOff, count uint64) error {
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
	err = scope.close()
	if err != nil {
		t.Fatal(err)
	}
}
