package raft

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestTimeoutTicker(t *testing.T) {
	const CandidateTimeout = "candidate_timeout_ticker"
	const CandidateTimeout2 = "candidate_timeout_ticker2"
	tt := newTimeoutTicker()
	var (
		count1 atomic.Uint64
		count2 atomic.Uint64
	)
	tt.RegisterTicker(tickerTask{
		Name: CandidateTimeout,
		Next: func(t time.Duration) time.Duration {
			return time.Millisecond * 20
		},
		Callback: func(t time.Time) {
			count1.Add(1)
		},
	})
	tt.RegisterTicker(tickerTask{
		Name: CandidateTimeout2,
		Next: func(t time.Duration) time.Duration {
			return time.Millisecond*150 + (time.Duration(rand.Intn(151)) * time.Millisecond)
		},
		Callback: func(t time.Time) {
			count2.Add(1)
		},
	})
	tt.init()
	time.Sleep(time.Second * 2)
	tt.Close()
	t.Logf("count1 = %d, count2 = %d", count1.Load(), count2.Load())
}

func TestTimeoutTickerReset(t *testing.T) {
	const CandidateTimeout = "candidate_timeout_ticker"
	tt := newTimeoutTicker()
	var (
		count1 atomic.Uint64
	)
	tt.RegisterTicker(tickerTask{
		Name: CandidateTimeout,
		Next: func(t time.Duration) time.Duration {
			return time.Millisecond * 20
		},
		Callback: func(t time.Time) {
			count1.Add(1)
		},
	})
	go func() {
		ticker := time.NewTicker(time.Millisecond * 2)
		defer ticker.Stop()
		for range ticker.C {
			tt.ResetTicker(CandidateTimeout)
		}
	}()
	tt.init()
	time.Sleep(time.Second * 2)
	tt.Close()
	if count1.Load() > 1 {
		t.Fatal("timeout ticker reset data error")
	}
}
