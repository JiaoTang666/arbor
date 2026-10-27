package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/initialledger"
	"blockEmulator/message"
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFinitePlanRecoveryUsesActualAdmissionAndWrites(t *testing.T) {
	t.Setenv("SAA_FINITE_ACCEPTANCE_V1", "")
	m, _, _ := dispatchFixture(t)
	f := m.firstAdmission
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	m.initialLedger = &initialledger.Ledger{Routes: map[string]uint64{a: 0, b: 0}}
	origin := time.Unix(1000, 0)
	now := origin
	var clockMu sync.Mutex
	clock := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	advance := func(d time.Duration) { clockMu.Lock(); defer clockMu.Unlock(); now = now.Add(d) }
	m.finitePlan = &finitePlan{now: clock, lastSecond: -1}
	m.arrivalClock = &arrivalSchedule{now: clock, sleep: advance}
	m.dataTotalNum = 12000
	m.offeredLoadTPS = 1000
	written := make(chan struct{}, 1)
	f.writeOrdered = func(context.Context, string, []byte) error { written <- struct{}{}; return nil }
	seen := 0
	for batch := 0; batch < 12; batch++ {
		txs := make([]*core.Transaction, 1000)
		for i := range txs {
			txs[i] = core.NewTransaction(a, b, big.NewInt(1), uint64(batch*1000+i), origin)
			txs[i].IngressSequence = uint64(batch*1000 + i)
		}
		m.submitScheduled(txs, func(xs []*core.Transaction) {
			tx := xs[0]
			m.submitFirstPlacementStream(xs)
			select {
			case <-written:
			case <-time.After(time.Second):
				t.Fatal("actual writer not reached", seen)
			}
			if tx.AdmissionID == "" || !tx.RoutingFrozen || tx.IngressSequence != uint64(seen) {
				t.Fatal("admission/route/order missing")
			}
			f.report(&message.BlockInfoMsg{InnerShardTxs: xs, SenderShardID: 0, Epoch: seen + 1, CommitTime: clock()})
			if f.ledger.Outstanding() != 0 || len(m.streamPending) != 0 {
				t.Fatal("not bounded/completed")
			}
			if seen == 0 {
				advance(15 * time.Second)
			}
			seen++
		})
	}
	if seen != 12000 || !m.finitePlan.exceeded {
		t.Fatal("recovery not covered")
	}
}

func TestFinitePlanLateReceiptCannotReleaseSuccessor(t *testing.T) {
	m, _, _ := dispatchFixture(t)
	f := m.firstAdmission
	first, next := queueTx(1, 2), queueTx(1, 2)
	if !f.admit(first) {
		t.Fatal("first held")
	}
	f.bindFixedRoute(first, 1)
	f.routed(first)
	if f.admit(next) {
		t.Fatal("successor bypass")
	}
	origin := time.Now()
	m.finitePlan = &finitePlan{now: time.Now, origin: origin, cutoff: origin.Add(-time.Nanosecond)}
	f.writeOrdered = func(context.Context, string, []byte) error { t.Error("post cutoff write"); return nil }
	report := &message.BlockInfoMsg{SenderShardID: first.RoutedSenderShard, Epoch: 1, CommitTime: time.Now()}
	if first.RoutedSenderShard == first.RoutedRecipientShard {
		report.InnerShardTxs = []*core.Transaction{first}
		f.report(report)
	} else {
		report.Relay1Txs = []*core.Transaction{first}
		f.report(report)
		f.report(&message.BlockInfoMsg{Relay2Txs: []*core.Transaction{first}, SenderShardID: first.RoutedRecipientShard, Epoch: 1, CommitTime: time.Now()})
	}
	if f.ledger.Outstanding() != 1 || f.ledger.HeldCount() != 1 {
		t.Fatal("receipt ignored or successor released")
	}
}

func TestFinitePlanDefaultDebtStillPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("default debt safeguard removed")
		}
	}()
	var p *finitePlan
	p.observe(12000, 1)
}
func TestFinitePlanRecoveryTwelveBatches(t *testing.T) {
	t.Setenv("SAA_FINITE_ACCEPTANCE_V1", "")
	origin := time.Unix(1000, 0)
	now := origin
	p := &finitePlan{now: func() time.Time { return now }, lastSecond: -1}
	m := &RelayCommitteeModule{finitePlan: p, dataTotalNum: 12000, offeredLoadTPS: 1000, arrivalClock: &arrivalSchedule{now: func() time.Time { return now }, sleep: func(d time.Duration) { now = now.Add(d) }}}
	seen := 0
	for batch := 0; batch < 12; batch++ {
		xs := make([]*core.Transaction, 1000)
		for i := range xs {
			xs[i] = &core.Transaction{IngressSequence: uint64(batch*1000 + i)}
		}
		m.submitScheduled(xs, func(v []*core.Transaction) {
			if len(v) != 1 || int(v[0].IngressSequence) != seen {
				t.Fatal("identity changed")
			}
			if seen == 0 {
				now = now.Add(15 * time.Second)
			}
			seen++
		})
	}
	if seen != 12000 || !p.exceeded || p.maxDebt != 11999 || m.arrivalClock.start != origin || p.cutoff != origin.Add(311999*time.Millisecond) {
		t.Fatal(seen, p)
	}
	before := p.cutoff
	p.observe(12000, 11999)
	p.start(now)
	if p.cutoff != before {
		t.Fatal("deadline rebased")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("12001st source allowed")
		}
	}()
	m.submitScheduled([]*core.Transaction{{IngressSequence: 12000}}, func([]*core.Transaction) { t.Fatal("excess source delivered") })
}
func TestFinitePlanCutoffCapacityWait(t *testing.T) {
	f := firstplacement.NewFence("finite-cutoff")
	for i := 0; i < 256; i++ {
		if _, e := f.Admit(queueAddress(1), queueAddress(2), time.Now()); e != nil {
			t.Fatal(e)
		}
	}
	origin := time.Now()
	p := &finitePlan{now: time.Now, origin: origin, cutoff: origin.Add(30 * time.Millisecond)}
	m := &RelayCommitteeModule{finitePlan: p, initialLedger: &initialledger.Ledger{Routes: map[string]uint64{}}, admission: &RuntimeUnknownAdmissionManager{shardNum: 2}, finiteHook: &finiteAcceptanceHook{}}
	m.finiteHook.active.Store(true)
	m.firstAdmission = &firstAdmissionRuntime{module: m, ledger: f, started: true}
	done := make(chan struct{})
	go func() { m.submitFirstPlacementStream([]*core.Transaction{queueTx(1, 3)}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("capacity wait not interrupted")
	}
	if !p.closed() || f.Outstanding() != 256 || len(m.streamPending) != 0 {
		t.Fatal("cutoff changed original flights")
	}
	if m.firstAdmission.admit(queueTx(2, 3)) {
		t.Fatal("post cutoff admitted")
	}
	m.firstAdmission.scheduleReadyLocked()
	if f.Outstanding() != 256 {
		t.Fatal("post cutoff admission altered census")
	}
}
func TestFinitePlanConcurrentBoundary(t *testing.T) {
	origin := time.Now()
	p := &finitePlan{now: time.Now, origin: origin, cutoff: origin.Add(-time.Nanosecond)}
	var w sync.WaitGroup
	for i := 0; i < 32; i++ {
		w.Add(1)
		go func() {
			defer w.Done()
			for j := 0; j < 100; j++ {
				if p.begin("write", uint64(j)) {
					t.Error("post cutoff start")
				}
				p.observe(12000, int64(j))
			}
		}()
	}
	w.Wait()
	if !p.stopped || !p.exceeded {
		t.Fatal("latch absent")
	}
}
func TestFinitePlanContextAndLockedDeadline(t *testing.T) {
	p := &finitePlan{now: time.Now}
	p.start(time.Now())
	cut := p.cutoff
	ctx, cancel := p.context()
	defer cancel()
	d, ok := ctx.Deadline()
	if !ok || d != cut {
		t.Fatal("context not bounded by original cutoff")
	}
	for _, ordinal := range []int64{0, 11999, 0, 11999} {
		p.observe(12000, ordinal)
	}
	p.start(time.Now().Add(time.Hour))
	if p.cutoff != cut {
		t.Fatal("deadline moved")
	}
}
func TestFinitePlanMissingContractRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	t.Setenv("SAA_FINITE_PLAN_CONFIG", path)
	for _, v := range []interface{}{map[string]interface{}{}, finitePlanConfig{Mode: "finite_plan_bounded_catchup", Count: 12000, Rate: 1000, AfterLastSeconds: 300, StopSeconds: 60, MaxRSSGiB: 240, MaxDatabaseGiB: 32, MaxTextGiB: 1, MaxProcesses: 65}} {
		b, _ := json.Marshal(v)
		if e := os.WriteFile(path, b, 0600); e != nil {
			t.Fatal(e)
		}
		m := &RelayCommitteeModule{}
		if m.configureFinitePlan() == nil || m.finitePlan != nil {
			t.Fatal("incomplete activation allowed")
		}
	}
	t.Setenv("SAA_FINITE_PLAN_CONFIG", "")
	m := &RelayCommitteeModule{}
	if m.configureFinitePlan() != nil || m.finitePlan != nil {
		t.Fatal("default changed")
	}
}
