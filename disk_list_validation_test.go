package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	pb "github.com/nyan233/raft/pb/message/raft"
)

// Each fixture owns a fresh subdirectory under test. Cleanup closes handles
// before removing that subdirectory; existing test data is never reused.
type diskListFixture struct {
	t     *testing.T
	dir   string
	limit int64
	d     *diskListReaderWriter
}

func newDiskListFixture(t *testing.T, limit int64) *diskListFixture {
	t.Helper()
	if err := os.MkdirAll("test", 0755); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("test", "disklist-validation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("fixture: %s", dir)
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove fixture directory: %v", err)
		}
	})
	f := &diskListFixture{t: t, dir: dir, limit: limit}
	f.open()
	return f
}

func (f *diskListFixture) open() {
	f.t.Helper()
	d := newDiskListReaderWriter(f.dir, "log", f.limit)
	f.d = d
	f.t.Cleanup(func() {
		// A failed operation may leave closed handles in the segment list.
		// Close every remaining descriptor even when one is already closed.
		lists := []*diskList{d.r, d.w}
		for _, seg := range d.segFileList {
			lists = append(lists, seg.r)
		}
		for _, list := range lists {
			if list == nil {
				continue
			}
			for _, file := range []*os.File{list.idx, list.dat} {
				if file == nil {
					continue
				}
				if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					f.t.Errorf("close fixture file: %v", err)
				}
			}
		}
	})
	if err := d.init(); err != nil {
		f.t.Fatalf("Init: %v", err)
	}
}

func (f *diskListFixture) reopen() {
	f.t.Helper()
	if err := f.d.close(); err != nil {
		f.t.Fatalf("close before reopen: %v", err)
	}
	f.open()
}

func (f *diskListFixture) write(entries []*pb.Entry) {
	f.t.Helper()
	// Keep the oracle independent if an implementation mutates its input.
	input := make([]*pb.Entry, len(entries))
	for i, entry := range entries {
		input[i] = &pb.Entry{
			LogIndex: entry.LogIndex, Term: entry.Term,
			CommandType: entry.CommandType, Command: bytes.Clone(entry.Command),
		}
	}
	if err := f.d.WriteBatch(input); err != nil {
		f.t.Fatalf("WriteBatch: %v", err)
	}
}

func (f *diskListFixture) read(start, end uint64, onlyIndex bool) (entries []*pb.Entry, err error) {
	scope := f.d.Scope(start, end)
	if err = scope.Find(); err != nil {
		return nil, err // Find closes its temporary handles on failure.
	}
	err = scope.RangeFor(func(list *diskList, offset, count uint64) error {
		batch, err := list.batchRead(int(offset), int(count), onlyIndex)
		entries = append(entries, batch...)
		return err
	})
	return entries, err
}

func assertDiskEntry(t *testing.T, got, want *pb.Entry, onlyIndex bool) {
	t.Helper()
	if got == nil || want == nil {
		if got != want {
			t.Fatalf("entry = %v, want %v", got, want)
		}
		return
	}
	if got.LogIndex != want.LogIndex || got.Term != want.Term {
		t.Fatalf("entry identity = (%d,%d), want (%d,%d)", got.LogIndex, got.Term, want.LogIndex, want.Term)
	}
	if onlyIndex {
		if len(got.Command) != 0 {
			t.Fatalf("index-only read returned %d command bytes", len(got.Command))
		}
		return
	}
	if got.CommandType != want.CommandType {
		t.Fatalf("index %d: command type = %d, want %d", want.LogIndex, got.CommandType, want.CommandType)
	}
	if !bytes.Equal(got.Command, want.Command) {
		t.Fatalf("index %d: command differs (got %d bytes, want %d)", want.LogIndex, len(got.Command), len(want.Command))
	}
}

func (f *diskListFixture) assertRange(want []*pb.Entry, onlyIndex bool) {
	f.t.Helper()
	if len(want) == 0 {
		f.t.Fatal("assertRange requires a nonempty expected range")
	}
	got, err := f.read(want[0].LogIndex, want[len(want)-1].LogIndex, onlyIndex)
	if err != nil {
		f.t.Fatalf("read [%d,%d]: %v", want[0].LogIndex, want[len(want)-1].LogIndex, err)
	}
	if len(got) != len(want) {
		f.t.Fatalf("range length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		assertDiskEntry(f.t, got[i], want[i], onlyIndex)
	}
}

func (f *diskListFixture) assertState(want []*pb.Entry) {
	f.t.Helper()
	var first, last *pb.Entry
	if len(want) > 0 {
		first, last = want[0], want[len(want)-1]
	}
	for _, onlyIndex := range []bool{false, true} {
		got, err := f.d.First(onlyIndex)
		if err != nil {
			f.t.Fatalf("First: %v", err)
		}
		assertDiskEntry(f.t, got, first, onlyIndex)
		got, err = f.d.Last(onlyIndex)
		if err != nil {
			f.t.Fatalf("Last: %v", err)
		}
		assertDiskEntry(f.t, got, last, onlyIndex)
		if len(want) > 0 {
			f.assertRange(want, onlyIndex)
		}
	}
}

// Ordinary fixtures use the default command type so a type-serialization bug
// does not mask independent range/truncation failures. CommandTypes tests all types.
func diskListEntries(start uint64, count int, term uint64) []*pb.Entry {
	entries := make([]*pb.Entry, count)
	for i := range count {
		index := start + uint64(i)
		entries[i] = &pb.Entry{
			LogIndex: index, Term: term,
			Command: []byte(fmt.Sprintf("%020d", index)),
		}
	}
	return entries
}

// Each three-entry batch has 60 data bytes and rolls at the 50-byte limit.
// The resulting layout is [1..3], [4..6], [7..9], and optionally main [10..11].
func diskListSegmentFixture(t *testing.T, withMain bool) (*diskListFixture, []*pb.Entry) {
	t.Helper()
	f := newDiskListFixture(t, 50)
	var want []*pb.Entry
	for i := range 3 {
		batch := diskListEntries(uint64(1+i*3), 3, uint64(i+1))
		f.write(batch)
		want = append(want, batch...)
	}
	if withMain {
		batch := diskListEntries(10, 2, 4)
		f.write(batch)
		want = append(want, batch...)
	}
	return f, want
}

func runDiskListCase(t *testing.T, name string, fn func(*testing.T)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("unexpected panic: %v\n%s", p, debug.Stack())
			}
		}()
		fn(t)
	})
}

func TestDiskListValidation(t *testing.T) {
	runDiskListCase(t, "Empty", func(t *testing.T) {
		f := newDiskListFixture(t, 1<<20)
		f.write(nil)
		f.assertState(nil)
		f.reopen()
		f.assertState(nil)
	})

	runDiskListCase(t, "RoundTrip", func(t *testing.T) {
		f := newDiskListFixture(t, 1<<20)
		want := []*pb.Entry{
			{LogIndex: 100, Term: 3},
			{LogIndex: 101, Term: 3, Command: []byte{}},
			{LogIndex: 102, Term: 4, Command: []byte{0, 255, 0, 128}},
			{LogIndex: 103, Term: 4, Command: []byte("中文日志\nsecond line")},
			{LogIndex: 104, Term: 9, Command: bytes.Repeat([]byte("payload"), 10_000)},
		}
		f.write(want[:2])
		f.write(want[2:])
		f.assertState(want)
		for _, entry := range want {
			f.assertRange([]*pb.Entry{entry}, false)
		}
		f.reopen()
		f.assertState(want)
	})

	runDiskListCase(t, "CommandTypes", func(t *testing.T) {
		for _, typ := range []pb.CommandType{pb.CommandType_NoOpCommand, pb.CommandType_UserCommand, pb.CommandType_MemberChangeCommand} {
			runDiskListCase(t, typ.String(), func(t *testing.T) {
				f := newDiskListFixture(t, 1<<20)
				want := diskListEntries(1, 2, 7)
				for _, entry := range want {
					entry.CommandType = uint32(typ)
				}
				f.write(want)
				f.reopen()
				f.assertState(want)
			})
		}
	})

	runDiskListCase(t, "BatchSizes", func(t *testing.T) {
		for _, count := range []int{1, 499, 500, 501, 1001} {
			runDiskListCase(t, fmt.Sprint(count), func(t *testing.T) {
				f := newDiskListFixture(t, 1<<20)
				want := diskListEntries(25, count, 6)
				f.write(want)
				f.assertState(want)
			})
		}
	})

	runDiskListCase(t, "Ranges", func(t *testing.T) {
		for _, withMain := range []bool{false, true} {
			runDiskListCase(t, fmt.Sprintf("main=%t", withMain), func(t *testing.T) {
				f, want := diskListSegmentFixture(t, withMain)
				cases := []struct {
					name       string
					start, end uint64
					valid      bool
				}{
					{"all", 1, uint64(len(want)), true},
					{"First", 1, 1, true},
					{"segment_end", 3, 3, true},
					{"segment_start", 4, 4, true},
					{"cross_segment", 2, 8, true},
					{"middle", 5, 6, true},
					{"Last", uint64(len(want)), uint64(len(want)), true},
					{"missing_start", 0, 2, false},
					{"past_tail", 1, uint64(len(want) + 1), false},
					{"missing_range", 20, 25, false},
					{"reversed", 8, 2, false},
				}
				for _, tc := range cases {
					runDiskListCase(t, tc.name, func(t *testing.T) {
						for _, onlyIndex := range []bool{false, true} {
							got, err := f.read(tc.start, tc.end, onlyIndex)
							if !tc.valid {
								if err == nil {
									t.Fatalf("read [%d,%d] should reject an unavailable range", tc.start, tc.end)
								}
								continue
							}
							if err != nil {
								t.Fatal(err)
							}
							if len(got) != int(tc.end-tc.start+1) {
								t.Fatalf("range length = %d, want %d", len(got), tc.end-tc.start+1)
							}
							for i, entry := range got {
								assertDiskEntry(t, entry, want[int(tc.start)-1+i], onlyIndex)
							}
						}
					})
				}
				f.assertState(want) // Closing scopes must not close the store.
			})
		}
	})

	runDiskListCase(t, "ReopenAndAppend", func(t *testing.T) {
		f, want := diskListSegmentFixture(t, false)
		f.reopen()
		f.assertState(want)
		for i := range 2 {
			batch := diskListEntries(uint64(len(want)+1), 3, uint64(10+i))
			f.write(batch)
			want = append(want, batch...)
			f.assertState(want)
		}
		f.reopen()
		f.assertState(want)
	})

	runDiskListCase(t, "TruncateMain", func(t *testing.T) {
		for _, keep := range []int{1, 4, 9} {
			runDiskListCase(t, fmt.Sprintf("keep=%d", keep), func(t *testing.T) {
				f := newDiskListFixture(t, 1<<20)
				want := diskListEntries(1, 9, 2)
				f.write(want)
				for range 2 { // Repeating the same truncation must be harmless.
					if err := f.d.TruncateWithLogIndex(uint64(keep)); err != nil {
						t.Fatal(err)
					}
					f.assertState(want[:keep])
				}
				f.reopen()
				f.assertState(want[:keep])
			})
		}
		runDiskListCase(t, "past_tail", func(t *testing.T) {
			f := newDiskListFixture(t, 1<<20)
			want := diskListEntries(1, 3, 2)
			f.write(want)
			if err := f.d.TruncateWithLogIndex(4); err == nil {
				t.Fatal("truncation beyond the tail should fail")
			}
			f.assertState(want)
		})
	})

	runDiskListCase(t, "TruncateAppend", func(t *testing.T) {
		f := newDiskListFixture(t, 1<<20)
		want := diskListEntries(1, 8, 1)
		f.write(want)
		if err := f.d.TruncateWithLogIndex(3); err != nil {
			t.Fatal(err)
		}
		replacement := diskListEntries(4, 4, 9)
		for _, entry := range replacement {
			entry.Command = append([]byte("replacement:"), entry.Command...)
		}
		f.write(replacement)
		want = append(want[:3:3], replacement...)
		f.assertState(want)
		f.reopen()
		f.assertState(want)
	})

	runDiskListCase(t, "ZeroBasedIndices", func(t *testing.T) {
		f := newDiskListFixture(t, 1<<20)
		want := diskListEntries(0, 4, 1)
		f.write(want)
		f.assertState(want)
		// Truncation is inclusive: index zero is a real entry in this fixture.
		if err := f.d.TruncateWithLogIndex(0); err != nil {
			t.Fatal(err)
		}
		f.assertState(want[:1])
	})

	runDiskListCase(t, "TruncateSegments", func(t *testing.T) {
		for _, withMain := range []bool{false, true} {
			for _, keep := range []int{0, 1, 3, 4, 6, 8, 9} {
				runDiskListCase(t, fmt.Sprintf("main=%t/keep=%d", withMain, keep), func(t *testing.T) {
					f, want := diskListSegmentFixture(t, withMain)
					if err := f.d.TruncateWithLogIndex(uint64(keep)); err != nil {
						t.Fatal(err)
					}
					f.assertState(want[:keep])
					batch := diskListEntries(uint64(keep+1), 3, 20)
					f.write(batch) // Force a new rotation after promoting a segment.
					want = append(want[:keep:keep], batch...)
					f.assertState(want)
					f.reopen()
					f.assertState(want)
				})
			}
		}
		runDiskListCase(t, "promote_then_clean", func(t *testing.T) {
			f, _ := diskListSegmentFixture(t, true)
			if err := f.d.TruncateWithLogIndex(6); err != nil {
				t.Fatal(err)
			}
			batch := diskListEntries(7, 3, 20)
			f.write(batch)
			if err := f.d.CleanWithLogIndex(9); err != nil {
				t.Fatal(err)
			}
			// All retained data was rotated and is now eligible for cleanup.
			f.assertState(nil)
		})
	})

	runDiskListCase(t, "Cleanup", func(t *testing.T) {
		f, want := diskListSegmentFixture(t, true)
		for _, tc := range []struct{ cutoff, first uint64 }{
			{0, 1}, {2, 1}, {3, 4}, {4, 4}, {6, 7}, {9, 10}, {11, 10},
		} {
			t.Logf("cleanup <= %d; First retained index should be %d", tc.cutoff, tc.first)
			for range 2 {
				if err := f.d.CleanWithLogIndex(tc.cutoff); err != nil {
					t.Fatal(err)
				}
				f.assertState(want[tc.first-1:])
			}
		}
		if _, err := f.read(1, 3, false); err == nil {
			t.Fatal("a cleaned range should no longer be readable")
		}
		f.reopen()
		f.assertState(want[9:])
		batch := diskListEntries(12, 3, 8)
		f.write(batch)
		f.assertState(append(want[9:], batch...))
	})

	runDiskListCase(t, "Corruption", func(t *testing.T) {
		for _, target := range []string{"index_checksum", "data_checksum", "short_index", "short_data"} {
			runDiskListCase(t, target, func(t *testing.T) {
				f := newDiskListFixture(t, 1<<20)
				want := diskListEntries(1, 1, 7)
				f.write(want)
				ext := ".dat"
				if target == "index_checksum" || target == "short_index" {
					ext = ".idx"
				}
				file, err := os.OpenFile(filepath.Join(f.dir, "log"+ext), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				switch target {
				case "index_checksum", "data_checksum":
					var original [1]byte
					if _, err := file.ReadAt(original[:], 0); err != nil {
						t.Fatal(err)
					}
					original[0] ^= 0xff
					_, err = file.WriteAt(original[:], 0)
				case "short_index":
					err = file.Truncate(logDiskSize - 1)
				case "short_data":
					err = file.Truncate(int64(len(want[0].Command) - 1))
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.d.First(false); err == nil {
					t.Fatal("reading a corrupted entry should return an error, not success or an empty result")
				}
			})
		}
	})

	for _, segmented := range []bool{false, true} {
		name, limit := "ModelMain", int64(1<<20)
		if segmented {
			name, limit = "ModelSegments", 50
		}
		runDiskListCase(t, name, func(t *testing.T) {
			for _, seed := range []uint64{1, 42, 2026} {
				runDiskListCase(t, fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
					f := newDiskListFixture(t, limit)
					rng := rand.New(rand.NewPCG(seed, 99))
					want := diskListEntries(1, 2, 1)
					f.write(want)
					for step := range 60 {
						switch op := rng.IntN(4); op {
						case 0, 1:
							batch := diskListEntries(uint64(len(want)+1), 1+rng.IntN(6), uint64(step+2))
							t.Logf("step %d: append %d entries", step, len(batch))
							f.write(batch)
							want = append(want, batch...)
						case 2:
							keep := 1 + rng.IntN(len(want))
							t.Logf("step %d: keep through %d", step, keep)
							if err := f.d.TruncateWithLogIndex(uint64(keep)); err != nil {
								t.Fatal(err)
							}
							want = want[:keep:keep]
						case 3:
							t.Logf("step %d: close and reopen", step)
							f.reopen()
						}
						f.assertState(want)
						start := rng.IntN(len(want))
						end := start + 1 + rng.IntN(len(want)-start)
						f.assertRange(want[start:end], false)
					}
				})
			}
		})
	}
}
