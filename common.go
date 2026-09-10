package raft

import "log"

func init() {
	log.SetFlags(log.Flags() | log.Lmicroseconds)
}
