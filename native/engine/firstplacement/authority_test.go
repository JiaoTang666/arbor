package firstplacement

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func reservationFixture() ReservationRequest {
	now := time.Unix(100, 0)
	a := strings.Repeat("a", 40)
	proofs := []PresenceProof{{Account: a, Shard: 0, BoundaryEpoch: 7, StateRoot: strings.Repeat("1", 64), Status: "ABSENT", ObservedAt: now, ReceivedAt: now}, {Account: a, Shard: 1, BoundaryEpoch: 7, StateRoot: strings.Repeat("2", 64), Status: "ABSENT", ObservedAt: now, ReceivedAt: now}}
	return ReservationRequest{ID: "r1", BaseRouteVersion: 3, EffectiveEpoch: 8, DeadlineEpoch: 9, SubmittedAt: now, MaxEvidenceAge: time.Second, Claims: []Claim{{a, 1}}, Proofs: proofs}
}
func TestReservationLifecycleAndIdempotency(t *testing.T) {
	a := NewAuthority(2, nil, 3)
	q := reservationFixture()
	r := a.Reserve(q, 7)
	if r.Status != Reserved {
		t.Fatal(r)
	}
	if again := a.Reserve(q, 7); again.Status != Reserved || again.Fingerprint != r.Fingerprint {
		t.Fatal(again)
	}
	bad := q
	bad.Claims = []Claim{{q.Claims[0].Account, 0}}
	if a.Reserve(bad, 7).Reason != "duplicate_request_conflict" {
		t.Fatal("conflicting replay accepted")
	}
	active := a.ActivateDue(8, time.Unix(101, 0))
	if len(active) != 1 || active[0].Status != RouteActive || active[0].ActiveVersion != 4 {
		t.Fatal(active)
	}
	if e := a.Bind("r1", 4, q.Claims, 8, time.Unix(102, 0)); e != nil {
		t.Fatal(e)
	}
	if e := a.Commit("r1", q.Claims[0].Account, 1, time.Unix(103, 0)); e != nil {
		t.Fatal(e)
	}
	done, _ := a.Get("r1")
	if done.Status != CreationCommitted {
		t.Fatal(done)
	}
	if replay := a.Reserve(q, 7); replay.Status != CreationCommitted || replay.ActiveVersion != 4 {
		t.Fatal("committed request was not idempotent", replay)
	}
}
func TestReservationRequiresAllShardAbsenceAtOneBoundary(t *testing.T) {
	for name, edit := range map[string]func(*ReservationRequest){"missing": func(q *ReservationRequest) { q.Proofs = q.Proofs[:1] }, "present": func(q *ReservationRequest) { q.Proofs[1].Status = "EXISTS" }, "stale_boundary": func(q *ReservationRequest) { q.Proofs[1].BoundaryEpoch = 6 }, "late": func(q *ReservationRequest) { q.Proofs[1].ReceivedAt = q.SubmittedAt.Add(time.Second) }} {
		t.Run(name, func(t *testing.T) {
			q := reservationFixture()
			edit(&q)
			if NewAuthority(2, nil, 3).Reserve(q, 7).Status != Rejected {
				t.Fatal("accepted")
			}
		})
	}
}
func TestConcurrentAccountReservationConflictAndExpiry(t *testing.T) {
	a := NewAuthority(2, nil, 3)
	q := reservationFixture()
	if a.Reserve(q, 7).Status != Reserved {
		t.Fatal()
	}
	other := q
	other.ID = "r2"
	if a.Reserve(other, 7).Reason != "account_already_reserved" {
		t.Fatal("conflict accepted")
	}
	a.ActivateDue(10, time.Unix(110, 0))
	r, _ := a.Get("r1")
	if r.Status != Expired {
		t.Fatal(r)
	}
	if a.Reserve(other, 7).Status != Reserved {
		t.Fatal("expired owner not released")
	}
}
func TestBindRejectsWrongVersionRouteAndEarlyTime(t *testing.T) {
	a := NewAuthority(2, nil, 3)
	q := reservationFixture()
	a.Reserve(q, 7)
	a.ActivateDue(8, time.Unix(101, 0))
	if a.Bind("r1", 3, q.Claims, 8, time.Unix(102, 0)) == nil {
		t.Fatal("old version")
	}
	if a.Bind("r1", 4, []Claim{{q.Claims[0].Account, 0}}, 8, time.Unix(102, 0)) == nil {
		t.Fatal("wrong route")
	}
	if a.Bind("r1", 4, q.Claims, 8, time.Unix(100, 0)) == nil {
		t.Fatal("early")
	}
}

func TestConcurrentReservationHasOneOwner(t *testing.T) {
	a := NewAuthority(2, nil, 3)
	left, right := reservationFixture(), reservationFixture()
	right.ID = "r2"
	start, out := make(chan struct{}), make(chan Reservation, 2)
	go func() { <-start; out <- a.Reserve(left, 7) }()
	go func() { <-start; out <- a.Reserve(right, 7) }()
	close(start)
	x, y := <-out, <-out
	accepted := 0
	for _, r := range []Reservation{x, y} {
		if r.Status == Reserved {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("reserved=%d: %+v %+v", accepted, x, y)
	}
}

func TestSnapshotRestoresReservationAndActiveRoute(t *testing.T) {
	a := NewAuthority(2, nil, 3)
	q := reservationFixture()
	a.Reserve(q, 7)
	payload, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreAuthority(payload)
	if err != nil {
		t.Fatal(err)
	}
	if r := restored.Reserve(q, 7); r.Status != Reserved {
		t.Fatal(r)
	}
	restored.ActivateDue(8, time.Unix(101, 0))
	payload, _ = restored.Snapshot()
	restored, err = RestoreAuthority(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Bind("r1", 4, q.Claims, 8, time.Unix(102, 0)); err != nil {
		t.Fatal(err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	raw["owner"] = map[string]string{q.Claims[0].Account: "missing"}
	corrupt, _ := json.Marshal(raw)
	if _, err := RestoreAuthority(corrupt); err == nil {
		t.Fatal("orphan owner restored")
	}
}
