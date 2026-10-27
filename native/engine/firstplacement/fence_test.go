package firstplacement

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestFenceOldRelayBlocksPresence(t *testing.T) {
	a, u := strings.Repeat("a", 40), strings.Repeat("b", 40)
	f := NewFence("run")
	old, e := f.Admit(a, u, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	// Capture an accepted occurrence even before it reaches the send buffer.
	l, e := f.Begin("q", []string{u}, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Route(old.ID, 0, 1, time.Now()); e != nil {
		t.Fatal(e)
	}
	held, e := f.Admit(a, u, time.Now())
	if e != nil || !held.Held {
		t.Fatal(held, e)
	}
	if f.Route(held.ID, 0, 1, time.Now()) == nil {
		t.Fatal("held work routed")
	}
	if e = f.Report(old.ID, a, u, 0, 1, 0, "relay1", time.Now()); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, e = f.WaitDrained(ctx, "q"); e == nil {
		t.Fatal("presence began before relay2")
	}
	state, n, _ := f.Lease("q")
	if state.Querying || n != 1 {
		t.Fatal(state, n)
	}
	if e = f.Report(old.ID, a, u, 0, 1, 1, "relay2", time.Now()); e != nil {
		t.Fatal(e)
	}
	l, e = f.WaitDrained(context.Background(), "q")
	if e != nil || !l.Querying {
		t.Fatal(l, e)
	}
	if f.HeldCount() != 1 {
		t.Fatal("hold was released by drain")
	}
	if _, e = f.Begin("q2", []string{u}, time.Now()); e == nil {
		t.Fatal("duplicate query owner")
	}
}
func TestFenceDistinctOccurrencesAndInvalidTerminal(t *testing.T) {
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	f := NewFence("run")
	x, _ := f.Admit(a, b, time.Now())
	y, _ := f.Admit(a, b, time.Now())
	if x.ID == y.ID {
		t.Fatal("collapsed occurrences")
	}
	f.Route(x.ID, 0, 1, time.Now())
	if f.Report(x.ID, a, b, 0, 1, 0, "relay2", time.Now()) == nil {
		t.Fatal("wrong shard cleared drain")
	}
	if f.Report(x.ID, a, b, 0, 1, 0, "aborted", time.Now()) == nil {
		t.Fatal("unproven abort cleared drain")
	}
}
func proofFixture(shards int) (Lease, []PresenceResponse) {
	a := strings.Repeat("a", 40)
	now := time.Now()
	l := Lease{RequestID: "q", Session: "run", Generation: 7, Accounts: []string{a}, Querying: true, QueryStartedAt: now}
	p := []PresenceResponse{}
	for s := 0; s < shards; s++ {
		p = append(p, PresenceResponse{Query: PresenceQuery{Session: "run", RequestID: "q", Generation: 7, Account: a, Shard: s}, Status: "ABSENT", Height: uint64(10 + s), StateRoot: strings.Repeat("1", 64), ObservedAt: now, ReceivedAt: now})
	}
	return l, p
}
func TestSixteenShardProofsAndAsyncHeights(t *testing.T) {
	l, p := proofFixture(16)
	if e := ValidateProofSet(l, 16, p); e != nil {
		t.Fatal(e)
	}
	for name, edit := range map[string]func([]PresenceResponse) []PresenceResponse{
		"missing":     func(p []PresenceResponse) []PresenceResponse { return p[:15] },
		"stale":       func(p []PresenceResponse) []PresenceResponse { p[0].Query.Generation--; return p },
		"unavailable": func(p []PresenceResponse) []PresenceResponse { p[0].Status = "UNAVAILABLE"; return p },
		"exists":      func(p []PresenceResponse) []PresenceResponse { p[0].Status = "EXISTS"; return p },
		"multiple_exists": func(p []PresenceResponse) []PresenceResponse {
			p[0].Status = "EXISTS"
			p[1].Status = "EXISTS"
			return p
		},
		"duplicate": func(p []PresenceResponse) []PresenceResponse { p[1] = p[0]; return p },
	} {
		t.Run(name, func(t *testing.T) {
			c := append([]PresenceResponse(nil), p...)
			if e := ValidateProofSet(l, 16, edit(c)); e == nil {
				t.Fatal("invalid proof accepted")
			}
		})
	}
}
func TestGenerationReservationAcceptsDifferentHeights(t *testing.T) {
	q := reservationFixture()
	q.PresenceSession = "run"
	q.PresenceGeneration = 7
	for i := range q.Proofs {
		q.Proofs[i].Session = "run"
		q.Proofs[i].Generation = 7
		q.Proofs[i].RequestID = q.ID
		q.Proofs[i].BoundaryEpoch = i
		q.Proofs[i].CommittedHeight = uint64(i + 10)
	}
	a := NewAuthority(2, nil, 3)
	if r := a.Reserve(q, 7); r.Status != Reserved {
		t.Fatal(r)
	}
	a.ActivateDue(8, time.Unix(101, 0))
	a.Commit(q.ID, q.Claims[0].Account, 1, time.Unix(102, 0))
	r, _ := a.Get(q.ID)
	if len(r.Committed) != 1 {
		t.Fatal("clone lost committed evidence")
	}
}
