package raft

import "testing"

func TestMetaFile(t *testing.T) {
	m := newMeta("test/node1.meta")
	err := m.init()
	if err != nil {
		t.Fatal(err)
	}
	err = m.save(metaData{
		VoteFor: "node1",
		Term:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.save(metaData{
		VoteFor: "node2",
		Term:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.save(metaData{
		VoteFor: "node3",
		Term:    3,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.close()
	if err != nil {
		t.Fatal(err)
	}
	m = newMeta("test/node1.meta")
	err = m.init()
	if err != nil {
		t.Fatal(err)
	}
	if m.get().VoteFor != "node3" {
		t.Fatal("vote for", m.get().VoteFor, "should be node3")
	}
	if m.get().Term != 3 {
		t.Fatal("term should be 3", m.get().Term)
	}
}
