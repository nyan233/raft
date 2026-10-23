package raft

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"testing"

	pb "github.com/nyan233/raft/pb/message/raft"
)

// BenchmarkDiskListSequential measures contiguous reads with a warm file cache.
// Each operation reads the next batch, wrapping only at the end of the file.
// Fixture writes, full-content checks, and cache warming are outside the timer.
// Run without the other package tests:
//
//	go test . -run '^$' -bench '^BenchmarkDiskListSequential$' -benchmem -benchtime=250ms -count=5 -cpu=1
func BenchmarkDiskListSequential(b *testing.B) {
	for _, payloadSize := range []int{128, 1024, 4096, 65536} {
		b.Run(fmt.Sprintf("payload=%d", payloadSize), func(b *testing.B) {
			entryCount := max(1000, min(16000, (32<<20)/payloadSize)) / 500 * 500
			list := newDiskListBenchmarkFixture(b, payloadSize, entryCount)
			for _, batchSize := range []int{1, 10, 50, 500} {
				b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
					for _, impl := range []struct {
						name string
						read func(int, int, bool) ([]*pb.Entry, error)
					}{
						{"batchRead", list.batchRead},
					} {
						b.Run(impl.name, func(b *testing.B) {
							// Validate the reader against the generated data before timing.
							want := make([]byte, payloadSize)
							for start := 0; start < entryCount; start += batchSize {
								entries, err := impl.read(start, batchSize, false)
								if err != nil {
									b.Fatal(err)
								}
								if len(entries) != batchSize {
									b.Fatalf("read %d entries, want %d", len(entries), batchSize)
								}
								for i, entry := range entries {
									index := start + i + 1
									fillDiskListBenchmarkPayload(want, index)
									if entry == nil || entry.LogIndex != uint64(index) || entry.Term != 7 ||
										entry.CommandType != 1 || !bytes.Equal(entry.Command, want) {
										b.Fatalf("incorrect entry at index %d", index)
									}
								}
							}
							runtime.GC()
							b.ReportAllocs()
							b.SetBytes(int64(batchSize * payloadSize))
							start := 0
							for b.Loop() {
								if _, err := impl.read(start, batchSize, false); err != nil {
									b.Fatal(err)
								}
								start += batchSize
								if start == entryCount {
									start = 0
								}
							}
							b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(batchSize), "ns/entry")
						})
					}
				})
			}
		})
	}
}

func newDiskListBenchmarkFixture(b *testing.B, payloadSize, entryCount int) *diskList {
	b.Helper()
	if err := os.MkdirAll("test", 0755); err != nil {
		b.Fatal(err)
	}
	dir, err := os.MkdirTemp("test", "disklist-benchmark-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Errorf("remove benchmark fixture: %v", err)
		}
	})
	writer, err := openDiskList(dir, "log", true)
	if err != nil {
		b.Fatal(err)
	}
	entries := make([]*pb.Entry, entryCount)
	for i := range entries {
		command := make([]byte, payloadSize)
		fillDiskListBenchmarkPayload(command, i+1)
		entries[i] = &pb.Entry{LogIndex: uint64(i + 1), Term: 7, CommandType: 1, Command: command}
	}
	writeErr := writer.write(entries)
	closeErr := writer.close()
	if writeErr != nil {
		b.Fatal(writeErr)
	}
	if closeErr != nil {
		b.Fatal(closeErr)
	}
	reader, err := openDiskList(dir, "log", false)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := reader.close(); err != nil {
			b.Errorf("close benchmark fixture: %v", err)
		}
	})
	return reader
}

func fillDiskListBenchmarkPayload(command []byte, index int) {
	for i := range command {
		command[i] = byte((index*31 + i*17) % 251)
	}
}
