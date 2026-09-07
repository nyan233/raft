package raft

import (
	"testing"
	"time"
)

func TestCandidate(t *testing.T) {
	nodes := []string{
		"127.0.0.1:8000",
		"127.0.0.1:8001",
		"127.0.0.1:8002",
	}
	node1, err := newRaftServer(nodes[0], []string{nodes[1], nodes[2]})
	if err != nil {
		t.Fatal(err)
	}
	node2, err := newRaftServer(nodes[1], []string{nodes[0], nodes[2]})
	if err != nil {
		t.Fatal(err)
	}
	node3, err := newRaftServer(nodes[2], []string{nodes[0], nodes[1]})
	if err != nil {
		t.Fatal(err)
	}
	go node1.Init()
	go node2.Init()
	go node3.Init()
	time.Sleep(time.Second * 20)
}
