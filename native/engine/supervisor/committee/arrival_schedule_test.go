package committee

import (
	"blockEmulator/core"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestArrivalScheduleBatchAndStall(t *testing.T) {
	start := time.Unix(100, 0)
	now := start
	s := &arrivalSchedule{now: func() time.Time { return now }, sleep: func(d time.Duration) { now = now.Add(d) }}
	m := &RelayCommitteeModule{offeredLoadTPS: 1000, arrivalClock: s}
	seen := 0
	deliver := func(xs []*core.Transaction) {
		if len(xs) != 1 {
			t.Fatal("not per occurrence")
		}
		seen++
	}
	m.submitScheduled([]*core.Transaction{{IngressSequence: 0}, {IngressSequence: 1}}, deliver)
	if now.Sub(start) != time.Millisecond {
		t.Fatal(now)
	}
	now = now.Add(20 * time.Millisecond) // control/HOLD work cannot reset origin
	m.submitScheduled([]*core.Transaction{{IngressSequence: 2}, {IngressSequence: 3}}, deliver)
	if seen != 4 || s.count != 4 || s.start != start || now.Sub(start) != 22*time.Millisecond {
		t.Fatal(seen, s, now)
	}
	_, planned, actual := s.next(1000)
	if planned.Sub(start) != 4*time.Millisecond || actual.Sub(planned) != 19*time.Millisecond {
		t.Fatal(planned, actual)
	}
}

func TestArrivalScheduleSameBeforeBothBranches(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		now := time.Unix(100, 0)
		m := &RelayCommitteeModule{offeredLoadTPS: 1000, offeredAuditPath: filepath.Join(t.TempDir(), "offered.csv"),
			arrivalClock: &arrivalSchedule{now: func() time.Time { return now }, sleep: func(d time.Duration) { now = now.Add(d) }}}
		if fenced {
			m.firstAdmission = &firstAdmissionRuntime{}
		}
		m.submitScheduled([]*core.Transaction{{IngressSequence: 0}}, func(xs []*core.Transaction) {
			raw, e := os.ReadFile(m.offeredAuditPath + ".arrivals.v1.jsonl")
			if e != nil {
				t.Fatal(e)
			}
			var row map[string]interface{}
			if e = json.Unmarshal(raw, &row); e != nil {
				t.Fatal(e)
			}
			if row["first_placement"] != fenced || row["ingress_sequence"] != float64(0) {
				t.Fatal(row)
			}
		})
	}
}

func TestArrivalRateRejectsNonFinite(t *testing.T) {
	for _, raw := range []string{"NaN", "Inf", "-Inf", "0", "-1"} {
		if parseOfferedLoadTPS(raw) != 0 {
			t.Fatal(raw)
		}
	}
	if parseOfferedLoadTPS("1000") != 1000 {
		t.Fatal("valid rate rejected")
	}
}

func TestArrivalSequenceRejectsRetryBeforeAdmission(t *testing.T) {
	m := &RelayCommitteeModule{offeredLoadTPS: 1000}
	n := 0
	deliver := func([]*core.Transaction) { n++ }
	m.submitScheduled([]*core.Transaction{{IngressSequence: 0}}, deliver)
	defer func() {
		if recover() == nil || n != 1 || m.arrivalClock.count != 1 {
			t.Fatal("retry consumed new arrival")
		}
	}()
	m.submitScheduled([]*core.Transaction{{IngressSequence: 0}}, deliver)
}

func TestArrivalDebtStopsWithoutAdmitting(t *testing.T) {
	now := time.Unix(100, 0)
	s := &arrivalSchedule{now: func() time.Time { return now }, sleep: func(d time.Duration) { now = now.Add(d) }}
	m := &RelayCommitteeModule{offeredLoadTPS: 1000, dataTotalNum: 20000, arrivalClock: s}
	n := 0
	deliver := func([]*core.Transaction) { n++ }
	m.submitScheduled([]*core.Transaction{{IngressSequence: 0}}, deliver)
	now = now.Add(11 * time.Second)
	defer func() {
		if recover() == nil || n != 1 {
			t.Fatal("debt overflow admitted")
		}
	}()
	m.submitScheduled([]*core.Transaction{{IngressSequence: 1}}, deliver)
}
