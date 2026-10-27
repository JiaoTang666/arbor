package committee

import (
	"blockEmulator/message"
	"sync"
	"time"
)

// Local evidence only. Assigned-but-unconfirmed work includes anticipated
// relay legs and can overlap TxPoolSize; consumers must not add those fields.
type placementSignal struct {
	Assigned, Confirmed int64
	Queue               *int
	Height              int
	BlockHash           string
	CapturedAt          time.Time
	ReceivedAt          time.Time
	ClockID             string
	ReceivedMonoNS      *int64
	BodyLength          int
	CommitAt            time.Time
	Intervals           []float64
}

type placementSignals struct {
	lightRows []lightTelemetryObservationV2
	mu          sync.Mutex
	rows        []placementSignal
	clock       *placementClock
	history     []sharePendingSample
	historyHead int
	historyOn   bool
}

func newPlacementSignals(shards int) *placementSignals {
	return &placementSignals{rows: make([]placementSignal, shards), clock: processPlacementClock}
}

func (p *placementSignals) assign(source, target uint64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows[source].Assigned++
	if target != source {
		p.rows[target].Assigned++
	}
	// Work evidence always has a clock, independent of optional policy history.
	at := p.clock.now().MonoNS
	p.rememberSharePending(at)
	return at
}

// The lifecycle calls this only after validating and deduplicating a real leg.
func (p *placementSignals) commit(shard uint64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	x := &p.rows[shard]
	if x.Confirmed >= x.Assigned {
		panic("placement evidence confirms unassigned work")
	}
	x.Confirmed++
	at := p.clock.now().MonoNS
	p.rememberSharePending(at)
	return at
}

func (p *placementSignals) observe(b *message.BlockInfoMsg, received placementClockReading) bool {
	if b == nil || b.SenderShardID >= uint64(len(p.rows)) || b.Epoch < 1 ||
		b.CommitTime.IsZero() || received.Wall.IsZero() || b.CommittedBlockHash == "" ||
		received.ClockID == "" || received.ClockID != p.clock.id || received.MonoNS < 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	x := &p.rows[b.SenderShardID]
	// Older or repeated reports cannot refresh a stale queue sample.
	if b.Epoch <= x.Height {
		return false
	}
	if !x.CommitAt.IsZero() && b.CommitTime.After(x.CommitAt) {
		// Reports can arrive out of order or be missing. Use elapsed time per
		// committed height, not a multi-block gap as one block's service time.
		elapsed := b.CommitTime.Sub(x.CommitAt).Seconds() / float64(b.Epoch-x.Height)
		x.Intervals = append(x.Intervals, elapsed)
		if len(x.Intervals) > 5 {
			x.Intervals = x.Intervals[len(x.Intervals)-5:]
		}
	}
	x.Height, x.BlockHash = b.Epoch, b.CommittedBlockHash
	x.BodyLength, x.CommitAt = b.BlockBodyLength, b.CommitTime
	x.Queue, x.CapturedAt, x.ReceivedAt = nil, time.Time{}, received.Wall
	x.ClockID, x.ReceivedMonoNS = "", nil
	if b.TxPoolSize != nil && *b.TxPoolSize >= 0 && !b.TxPoolObservedAt.IsZero() {
		queue := *b.TxPoolSize
		monoNS := received.MonoNS
		x.Queue, x.CapturedAt = &queue, b.TxPoolObservedAt
		x.ClockID, x.ReceivedMonoNS = received.ClockID, &monoNS
	}
	return true
}

func (p *placementSignals) snapshot() []placementSignal {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows := append([]placementSignal(nil), p.rows...)
	for i := range rows {
		rows[i].Intervals = append([]float64(nil), rows[i].Intervals...)
		if rows[i].Queue != nil {
			queue := *rows[i].Queue
			rows[i].Queue = &queue
		}
		if rows[i].ReceivedMonoNS != nil {
			monoNS := *rows[i].ReceivedMonoNS
			rows[i].ReceivedMonoNS = &monoNS
		}
	}
	return rows
}
