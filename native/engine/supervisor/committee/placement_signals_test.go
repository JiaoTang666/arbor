package committee

import (
	"blockEmulator/message"
	"testing"
	"time"
)

func TestPlacementSignalsStagesAndQueueRemainSeparate(t *testing.T) {
	p := newPlacementSignals(2)
	p.assign(0, 0)
	p.assign(0, 1)
	p.commit(0)
	p.commit(1)
	queue, now := 10, time.Now()
	received := func(delta time.Duration) placementClockReading {
		return placementClockReading{Wall: now.Add(delta), ClockID: p.clock.id, MonoNS: int64(delta)}
	}
	b := &message.BlockInfoMsg{SenderShardID: 0, Epoch: 1, CommittedBlockHash: "a", CommitTime: now,
		TxPoolSize: &queue, TxPoolObservedAt: now}
	p.observe(b, received(0))
	rows := p.snapshot()
	if rows[0].Assigned-rows[0].Confirmed != 1 || *rows[0].Queue != 10 || rows[1].Assigned != 1 {
		t.Fatal(rows)
	}
	queue = 99
	*rows[0].Queue = 42
	*rows[0].ReceivedMonoNS = 42
	if *p.snapshot()[0].Queue != 10 {
		t.Fatal("queue snapshot aliases mutable report or consumer memory")
	}
	if *p.snapshot()[0].ReceivedMonoNS != 0 {
		t.Fatal("monotonic snapshot aliases consumer memory")
	}
	b.TxPoolObservedAt = now.Add(time.Hour)
	p.observe(b, received(time.Hour))
	if !p.snapshot()[0].ReceivedAt.Equal(now) {
		t.Fatal("duplicate refreshed observation age")
	}
	b.Epoch, b.CommittedBlockHash, b.TxPoolSize = 2, "b", nil
	p.observe(b, received(time.Hour))
	if p.snapshot()[0].Queue != nil || p.snapshot()[0].ReceivedMonoNS != nil || p.snapshot()[0].ClockID != "" {
		t.Fatal("missing measurement silently reused as fresh")
	}
	// A missing intermediate report must not turn two block intervals into one.
	b.Epoch, b.CommittedBlockHash, b.CommitTime = 4, "d", now.Add(10*time.Second)
	p.observe(b, received(11*time.Second))
	last := p.snapshot()[0]
	if len(last.Intervals) != 1 || last.Intervals[0] != 5 || !last.ReceivedAt.Equal(now.Add(11*time.Second)) {
		t.Fatal("report gap or receipt clock misrepresented", last)
	}
}

func TestPlacementSignalsCannotConfirmFutureWork(t *testing.T) {
	p := newPlacementSignals(2)
	defer func() {
		if recover() == nil {
			t.Fatal("unassigned confirmation accepted")
		}
	}()
	p.commit(0)
}
