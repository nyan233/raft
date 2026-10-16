package raft

import (
	"time"

	"github.com/nyan233/littlerpc/core/common/context"
)

type applyUserSmReq struct {
	Ctx        *context.Context
	StartIndex uint64
	EndIndex   uint64
}

type applyUserSmCb struct {
	Req   applyUserSmReq
	Error error
}

type applyWorker struct {
	csm    *CoreSm
	rq     chan applyUserSmReq
	ticker *time.Ticker
}

func newApplyWorker(csm *CoreSm) *applyWorker {
	return &applyWorker{
		csm:    csm,
		rq:     make(chan applyUserSmReq, 1024),
		ticker: time.NewTicker(1 * time.Second),
	}
}

func (w *applyWorker) doApply(req applyUserSmReq) error {
	defer w.ticker.Reset(1 * time.Second)
	logMgr := w.csm.getLogMgr()
	if req.StartIndex == 0 {
		indexVal := logMgr.getLogIndexVal(req.Ctx)
		req.StartIndex = indexVal.lastApplied
	}
	if req.StartIndex >= req.EndIndex {
		return nil
	}
	err := logMgr.applyLogWithOffV2(req.Ctx, req.StartIndex, req.EndIndex)
	if err != nil {
		return err
	}
	return nil
}

func (w *applyWorker) asyncApply(req applyUserSmReq) {
	select {
	case w.rq <- req:
		break
	}
}

func (w *applyWorker) run() {
	go func() {
		for {
			select {
			case req := <-w.rq:
				w.csm.pushApply2UserSmCb(applyUserSmCb{
					Req:   req,
					Error: w.doApply(req),
				})
			case <-w.ticker.C:
				// rq有数据不触发兜底, 这种情况下一般是go的select选择就绪channel的机制导致走到这个逻辑了
				if len(w.rq) > 0 {
					break
				}
				ctx := context.Background()
				indexVal := w.csm.getLogMgr().getLogIndexVal(ctx)
				noApplied := indexVal.lastCommitIndex - indexVal.lastApplied
				if noApplied > 0 {
					if noApplied > 100 {
						noApplied = 100
					}
					req := applyUserSmReq{StartIndex: indexVal.lastApplied, EndIndex: indexVal.lastApplied + noApplied}
					w.csm.pushApply2UserSmCb(applyUserSmCb{
						Req:   req,
						Error: w.doApply(req),
					})
				}
			}
		}
	}()
}
