package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/initialledger"
	"testing"
	"time"
)

func TestFiniteAcceptanceMultiBatchClock(t *testing.T) {
	t.Setenv("SAA_FINITE_ACCEPTANCE_V1", "")
	origin := time.Unix(1000, 0)
	now := origin
	s := &arrivalSchedule{now: func() time.Time { return now }, sleep: func(d time.Duration) { now = now.Add(d) }}
	m := &RelayCommitteeModule{arrivalClock: s, offeredLoadTPS: 1000, dataTotalNum: 12000}
	seen := 0
	for k := 0; k < 12; k++ {
		xs := make([]*core.Transaction, 1000)
		for i := range xs {
			xs[i] = &core.Transaction{IngressSequence: uint64(k*1000 + i)}
		}
		if k == 2 {
			now = now.Add(2 * time.Second)
		}
		m.submitScheduled(xs, func(v []*core.Transaction) {
			if len(v) != 1 || int(v[0].IngressSequence) != seen {
				t.Fatal("lost or duplicate occurrence")
			}
			seen++
		})
		if s.start != origin {
			t.Fatal("batch reset clock")
		}
	}
	if seen != 12000 || s.count != 12000 || now.Sub(origin) != 13998*time.Millisecond {
		t.Fatal(seen, now.Sub(origin))
	}
	_, p, a := s.next(1000)
	if p.Sub(origin) != 12*time.Second || a.Sub(p) != 1999*time.Millisecond {
		t.Fatal(p, a)
	}
}

func TestFiniteAcceptanceRealCapacityBlocksThenResumes(t *testing.T) {
	f := firstplacement.NewFence("finite-capacity")
	var first string
	for i := 0; i < streamOutstandingLimit; i++ {
		x, e := f.Admit(queueAddress(1), queueAddress(2), time.Now())
		if e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			first = x.ID
			if e = f.Route(x.ID, 0, 1, time.Now()); e != nil {
				t.Fatal(e)
			}
		}
	}
	m := &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: map[string]uint64{}}, admission: &RuntimeUnknownAdmissionManager{shardNum: 2}}
	m.firstAdmission = &firstAdmissionRuntime{module: m, ledger: f, started: true}
	h := &finiteAcceptanceHook{}
	h.active.Store(true)
	m.finiteHook = h
	done := make(chan struct{})
	tx := queueTx(1, 3)
	tx.IngressSequence = 256
	go func() { m.submitFirstPlacementStream([]*core.Transaction{tx}); close(done) }()
	select {
	case <-done:
		t.Fatal("production capacity check did not block")
	case <-time.After(30 * time.Millisecond):
	}
	// Actual fence completion releases one capacity slot, not a fake queue length.
	if e := f.Report(first, queueAddress(1), queueAddress(2), 0, 1, 0, "relay1", time.Now()); e != nil {
		t.Fatal(e)
	}
	select {
	case <-done:
		t.Fatal("relay1 incorrectly freed capacity")
	case <-time.After(10 * time.Millisecond):
	}
	if e := f.Report(first, queueAddress(1), queueAddress(2), 0, 1, 1, "relay2", time.Now()); e != nil {
		t.Fatal(e)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("capacity did not resume")
	}
	if len(m.streamPending) != 1 || m.streamPending[0] != tx || f.Outstanding() != 255 {
		t.Fatal("lost/repeated input")
	}
	h.active.Store(false)
	// Shared-account new creation cannot bypass still-held predecessors.
	m.pumpFirstPlacementStream()
	if len(m.streamPending) != 1 {
		t.Fatal("creation bypassed account barrier")
	}
}
