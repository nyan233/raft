package raft

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
)

type logDisk struct {
	checksum uint32
	logIndex uint64
	logTerm  uint64
	command  []byte
}

func (disk *logDisk) write2File(f *os.File) error {
	b1 := make([]byte, 0, len(disk.command)+32)
	b1 = b1[0:4]
	b1 = binary.BigEndian.AppendUint64(b1, disk.logIndex)
	b1 = binary.BigEndian.AppendUint64(b1, disk.logTerm)
	b1 = binary.BigEndian.AppendUint32(b1, uint32(len(disk.command)))
	b1 = append(b1, disk.command...)
	disk.checksum = crc32.ChecksumIEEE(b1)
	binary.BigEndian.PutUint32(b1[0:4], disk.checksum)
	_, err := f.Write(b1)
	return err
}

type raftLog struct {
	mu              sync.Mutex
	lastLogIndex    uint64
	lastLogTerm     uint64
	lastCommitIndex uint64
	dirPath         string
	file            *os.File
}
