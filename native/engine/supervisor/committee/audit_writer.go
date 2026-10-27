package committee

import (
	"fmt"
	"sync/atomic"
	"time"
)

// One bounded writer owns both buffers. Slow I/O never holds lifecycleMu.
// Exhaustion invalidates the run; no dropping, unbounded queue, or hidden wait.
const auditQueueBytes = 64 << 20

type auditRecord struct {
	data            []byte
	projection      bool
	barrier         chan struct{}
	projectionError error
}
type auditWriter struct {
	records                                 chan auditRecord
	queued, peak, written, writeNS, flushNS int64
}

func (a *batchAudit) startWriter() {
	a.async = &auditWriter{records: make(chan auditRecord, 131072)}
	go a.runWriter()
}
func (a *batchAudit) enqueueAudit(r auditRecord) {
	n := atomic.AddInt64(&a.async.queued, int64(len(r.data)))
	if n > auditQueueBytes {
		panic("audit queue exceeded 64 MiB; experiment invalid")
	}
	for old := atomic.LoadInt64(&a.async.peak); n > old; old = atomic.LoadInt64(&a.async.peak) {
		if atomic.CompareAndSwapInt64(&a.async.peak, old, n) {
			break
		}
	}
	select {
	case a.async.records <- r:
	default:
		panic("audit record queue exhausted; experiment invalid")
	}
}
func (a *batchAudit) flushBuffers() {
	begin := time.Now()
	if err := a.w.Flush(); err != nil {
		panic(fmt.Errorf("audit flush failed: %w", err))
	}
	if a.observer != nil {
		a.observer.flush()
	}
	atomic.AddInt64(&a.async.flushNS, time.Since(begin).Nanoseconds())
}
func (a *batchAudit) runWriter() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case r := <-a.async.records:
			if r.barrier != nil {
				a.flushBuffers()
				close(r.barrier)
				continue
			}
			if r.projectionError != nil {
				a.observer.fail(r.projectionError)
				continue
			}
			begin := time.Now()
			if r.projection {
				if !a.observer.failed {
					if a.observer.w.Available() < len(r.data) {
						a.flushBuffers()
					}
					if !a.observer.failed {
						if _, err := a.observer.w.Write(r.data); err != nil {
							a.observer.fail(err)
						}
					}
				}
			} else if _, err := a.w.Write(r.data); err != nil {
				panic(err)
			}
			atomic.AddInt64(&a.async.queued, -int64(len(r.data)))
			atomic.AddInt64(&a.async.written, int64(len(r.data)))
			atomic.AddInt64(&a.async.writeNS, time.Since(begin).Nanoseconds())
		case <-ticker.C:
			a.flushBuffers()
		}
	}
}
func (a *batchAudit) writerStats() map[string]interface{} {
	w := a.async
	if w == nil {
		return map[string]interface{}{"event": "audit_writer_stats", "async": false}
	}
	return map[string]interface{}{"event": "audit_writer_stats", "async": true, "capacity_bytes": auditQueueBytes, "queued_bytes": atomic.LoadInt64(&w.queued), "peak_queued_bytes": atomic.LoadInt64(&w.peak), "written_bytes": atomic.LoadInt64(&w.written), "write_ns": atomic.LoadInt64(&w.writeNS), "flush_ns": atomic.LoadInt64(&w.flushNS), "dropped_records": 0}
}
