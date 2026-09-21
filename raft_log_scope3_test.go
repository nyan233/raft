package raft

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/nyan233/raft/pb/message/raft"
)

// Bounds are inclusive. nil means an empty main file.
func scope3Fixture(t *testing.T, segments [][2]uint64, mainBounds *[2]uint64) *raftLogManager {
	t.Helper()
	// Leave fixture files in the system temp directory: this workspace requires
	// explicit approval for deletion. Cleanup still closes all fixture handles.
	dir, err := os.MkdirTemp("", "raft-scope3-test-")
	if err != nil {
		t.Fatal(err)
	}
	writeSet := func(name string, bounds *[2]uint64) {
		t.Helper()
		w, err := openLogSet(dir, name, true)
		if err != nil {
			t.Fatal(err)
		}
		defer w.close()
		var entries []*pb.Entry
		if bounds != nil {
			for index := bounds[0]; index <= bounds[1]; index++ {
				entries = append(entries, &pb.Entry{LogIndex: index, Term: 7, Command: scope3Command(index)})
			}
		}
		if err := w.write(entries); err != nil {
			t.Fatal(err)
		}
	}
	for i, bounds := range segments {
		name := fmt.Sprintf("segment%d", i)
		writeSet(name, &bounds)
		for _, ext := range []string{"idx", "dat"} {
			if err := os.Rename(filepath.Join(dir, name+"."+ext), filepath.Join(dir, fmt.Sprintf("log.%s.seg.%d", ext, i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeSet("log", mainBounds)
	r, err := openLogSet(dir, "log", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.close() })
	return &raftLogManager{dirPath: dir, logName: "log", r: r}
}

func scope3Command(index uint64) []byte {
	return bytes.Repeat([]byte(fmt.Sprintf("entry-%d/", index)), int(index%5)+1)
}

func TestLogScope3Ranges(t *testing.T) {
	cases := []struct {
		name       string
		segments   [][2]uint64
		main       *[2]uint64
		start, end uint64
		wantErr    bool
	}{
		{"main_middle", nil, new([2]uint64{1, 10}), 3, 5, false},
		{"main_single", nil, new([2]uint64{1, 10}), 5, 5, false},
		{"main_full", nil, new([2]uint64{1, 10}), 1, 10, false},
		{"segment_only", [][2]uint64{{1, 10}}, nil, 3, 5, false},
		{"segment_first_single", [][2]uint64{{1, 10}}, nil, 1, 1, false},
		{"cross_segments", [][2]uint64{{1, 10}, {11, 20}}, new([2]uint64{21, 30}), 8, 15, false},
		{"cross_to_main", [][2]uint64{{1, 10}}, new([2]uint64{11, 20}), 8, 13, false},
		{"end_at_main_first", [][2]uint64{{1, 10}}, new([2]uint64{11, 20}), 8, 11, false},
		{"trim_newer_files", [][2]uint64{{1, 10}, {11, 20}}, new([2]uint64{21, 30}), 3, 5, false},
		{"missing_start", nil, new([2]uint64{11, 20}), 8, 15, true},
		{"start_after_tail", nil, new([2]uint64{1, 10}), 11, 12, true},
		{"empty", nil, nil, 1, 2, true},
		// The latest implementation explicitly rejects an end beyond the tail.
		{"end_after_tail", nil, new([2]uint64{1, 10}), 8, 13, true},
		{"reversed_same_file", nil, new([2]uint64{1, 10}), 8, 5, true},
		{"reversed_before_file", nil, new([2]uint64{1, 10}), 8, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := scope3Fixture(t, tc.segments, tc.main)
			s := newLogScope3(m, tc.start, tc.end)
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("unexpected panic: %v", p)
				}
				s.close()
				for _, f := range []*os.File{m.r.idx, m.r.dat} {
					if _, err := f.Stat(); err != nil {
						t.Errorf("main handle closed: %v", err)
					}
				}
			}()
			err := s.find()
			if tc.wantErr {
				if err == nil {
					t.Fatal("find succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var got []*pb.Entry
			err = s.rangeFor(func(f *logSet, start, count uint64) error {
				// Bound allocations even if unsigned offset arithmetic regresses.
				if count == 0 || count > tc.end-tc.start+1 {
					return fmt.Errorf("invalid read count %d", count)
				}
				entries, err := f.batchRead(int(start), int(count), false)
				got = append(got, entries...)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if uint64(len(got)) != tc.end-tc.start+1 {
				t.Fatalf("got %d entries, want %d", len(got), tc.end-tc.start+1)
			}
			for i, e := range got {
				index := tc.start + uint64(i)
				if e.LogIndex != index || e.Term != 7 || !bytes.Equal(e.Command, scope3Command(index)) {
					t.Fatalf("entry %d: got index=%d term=%d command=%q; want index=%d and original payload", i, e.LogIndex, e.Term, e.Command, index)
				}
			}
			sentinel := errors.New("stop reading")
			calls := 0
			if err := s.rangeFor(func(*logSet, uint64, uint64) error { calls++; return sentinel }); !errors.Is(err, sentinel) || calls != 1 {
				t.Errorf("callback error not propagated: err=%v calls=%d", err, calls)
			}
		})
	}
}
