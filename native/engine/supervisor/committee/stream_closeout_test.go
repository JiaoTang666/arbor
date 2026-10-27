package committee

import (
	"blockEmulator/firstplacement"
	"blockEmulator/initialledger"
	"blockEmulator/supervisor/signal"
	"fmt"
	"testing"
	"time"
)

func TestStreamCloseoutCompletionContract(t *testing.T) {
	m := &RelayCommitteeModule{streamFirstPlacement: true, initialLedger: &initialledger.Ledger{}, Ss: signal.NewStopSignal(48, 16), dataTotalNum: 1, nowDataNum: 1, admission: &RuntimeUnknownAdmissionManager{shardNum: 16}}
	f := newFirstAdmissionRuntime(m)
	m.firstAdmission = f
	f.started = true
	f.ledger = firstplacement.NewFence("proof")
	a, b := fmt.Sprintf("%040x", 1), fmt.Sprintf("%040x", 2)
	if m.confirmStreamUntouchedShards() == nil {
		t.Fatal("empty ledger must not prove completion")
	}
	x, e := f.ledger.Admit(a, b, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if e = f.ledger.Route(x.ID, 0, 0, time.Now()); e != nil {
		t.Fatal(e)
	}
	if m.confirmStreamUntouchedShards() == nil {
		t.Fatal("uncompleted work")
	}
	if e = f.ledger.Report(x.ID, a, b, 0, 0, 0, "inner", time.Now()); e != nil {
		t.Fatal(e)
	}
	m.Ss.ObserveBlockInfo(0, 1)
	if e = m.confirmStreamUntouchedShards(); e != nil {
		t.Fatal(e)
	}
	if m.Ss.GapEnough() {
		t.Fatal("quiet guard skipped")
	}
	m.nowDataNum = 0
	if m.confirmStreamUntouchedShards() == nil {
		t.Fatal("truncated input")
	}
	m.nowDataNum = 1
	m.streamFirstPlacement = false
	if m.confirmStreamUntouchedShards() == nil {
		t.Fatal("legacy opt-in missing")
	}
}
