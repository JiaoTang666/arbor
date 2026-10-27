package firstplacement

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPresenceAppliedFloorAndCreatorOrder(t *testing.T) {
	l, p := proofFixture(2)
	p[0].Query.MinAppliedHeight = p[0].Height + 1
	if ValidateProofSet(l, 2, p) == nil {
		t.Fatal("stale state accepted")
	}
	p[0].Height++
	if e := ValidateProofSet(l, 2, p); e != nil {
		t.Fatal(e)
	}
	f := NewFence("fresh")
	a, u := strings.Repeat("a", 40), strings.Repeat("b", 40)
	f.Begin("q", []string{u}, time.Now())
	f.WaitDrained(context.Background(), "q")
	x, _ := f.Admit(a, u, time.Now())
	y, _ := f.Admit(a, u, time.Now())
	writes := 0
	e := f.RouteCreator("q", y.ID, 0, 1, time.Now(), func() error { writes++; return nil })
	if e == nil || writes != 0 || f.HeldCount() != 2 {
		t.Fatal("later creator changed state")
	}
	if e = f.RouteCreator("q", x.ID, 0, 1, time.Now(), func() error { return nil }); e != nil {
		t.Fatal(e)
	}
	f.Report(x.ID, a, u, 0, 1, 1, "relay2", time.Now())
	if f.CreatorTerminal("q") {
		t.Fatal("source execution position missing")
	}
	f.Report(x.ID, a, u, 0, 1, 0, "relay1", time.Now())
	if !f.CreatorTerminal("q") {
		t.Fatal("both reports did not close")
	}
}
