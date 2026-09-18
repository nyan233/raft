package raft

import (
	"sync"
	"time"
)

type tickerTask struct {
	Name     string
	Next     func(t time.Duration) time.Duration
	Callback func(t time.Time)
}

type timeoutTicker struct {
	mu       sync.Mutex
	ticker   map[string]*tickerTask
	tickNext map[string]time.Time
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

func newTimeoutTicker() *timeoutTicker {
	return &timeoutTicker{
		ticker:   make(map[string]*tickerTask),
		tickNext: make(map[string]time.Time),
	}
}

func (t *timeoutTicker) init() {
	t.closeCh = make(chan struct{})
	t.wg.Add(len(t.ticker))
	now := time.Now()
	for k, v := range t.ticker {
		t.tickNext[k] = now.Add(v.Next(0))
	}
	t.tick()
}
func (t *timeoutTicker) RegisterTicker(task tickerTask) {
	t.ticker[task.Name] = &task
}

func (t *timeoutTicker) ResetTicker(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.doResetTicker(name)
}

func (t *timeoutTicker) doResetTicker(name string) {
	now := time.Now()
	task := t.ticker[name]
	t.tickNext[name] = now.Add(task.Next(time.Duration(now.UnixNano())))
}

func (t *timeoutTicker) timeout(name string) (time.Time, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	next := t.tickNext[name]
	if next.Before(now) {
		t.doResetTicker(name)
		return next, true
	}
	return now, false
}

func (t *timeoutTicker) tick() {
	for name := range t.ticker {
		go func(name string) {
			defer t.wg.Done()
			ticker := time.NewTicker(time.Millisecond)
			for {
				select {
				case <-ticker.C:
					now, timeout := t.timeout(name)
					if timeout {
						task := t.ticker[name]
						task.Callback(now)
					}
				case <-t.closeCh:
					return
				}
			}
		}(name)
	}
}

func (t *timeoutTicker) Close() {
	close(t.closeCh)
	t.wg.Wait()
}
