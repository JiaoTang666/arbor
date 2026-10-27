package firstplacement

import (
	"fmt"
	"testing"
	"time"
)

func TestCompletedRoutesRequireAllLegsAndExactCount(t *testing.T) {
	f := NewFence("closeout")
	a, b := fmt.Sprintf("%040x", 1), fmt.Sprintf("%040x", 2)
	x, e := f.Admit(a, b, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Route(x.ID, 1, 7, time.Now()); e != nil {
		t.Fatal(e)
	}
	f.MarkSend(x.ID, "uncertain")
	if _, e = f.CompletedRouteShards(1); e == nil {
		t.Fatal("uncertain is not completion")
	}
	if e = f.Report(x.ID, a, b, 1, 7, 1, "relay1", time.Now()); e != nil {
		t.Fatal(e)
	}
	if _, e = f.CompletedRouteShards(1); e == nil {
		t.Fatal("source leg is not completion")
	}
	if e = f.Report(x.ID, a, b, 1, 7, 7, "relay2", time.Now()); e != nil {
		t.Fatal(e)
	}
	if _, e = f.CompletedRouteShards(2); e == nil {
		t.Fatal("missing source occurrence")
	}
	shards, e := f.CompletedRouteShards(1)
	if e != nil || len(shards) != 2 || shards[0] != 1 || shards[1] != 7 {
		t.Fatal(shards, e)
	}
	if _, e = f.Begin("unconfirmed-creation", []string{a}, time.Now()); e != nil {
		t.Fatal(e)
	}
	if _, e = f.CompletedRouteShards(1); e == nil {
		t.Fatal("live creation owner")
	}
}
