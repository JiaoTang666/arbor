package firstplacement

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPipelineOrdinaryDoesNotWaitForConsensus(t *testing.T) {
	f := NewPipelinedFence("run")
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	x, e := f.Admit(a, b, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Route(x.ID, 0, 1, time.Now()); e != nil {
		t.Fatal(e)
	}
	y, e := f.Admit(a, b, time.Now())
	if e != nil || !y.Held {
		t.Fatal("must wait while sending", y, e)
	}
	f.MarkSend(x.ID, "")
	ready := f.TakeReady()
	if len(ready) != 1 || ready[0].ID != y.ID {
		t.Fatal(ready)
	}
	if f.Outstanding() != 2 || f.PendingDispatch() != 1 {
		t.Fatal("lost drain accounting")
	}
	if e = f.Route(y.ID, 0, 1, time.Now()); e != nil {
		t.Fatal(e)
	}
	f.MarkSend(y.ID, "")
	if f.PendingDispatch() != 0 || f.Outstanding() != 2 {
		t.Fatal("inflight still charged as entry queue")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if f.WaitIdle(ctx, nil) == nil {
		t.Fatal("write is not completion")
	}
}
func TestPipelineUncertainAndCreationStayProtected(t *testing.T) {
	f := NewPipelinedFence("run")
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	x, _ := f.Admit(a, b, time.Now())
	if e := f.Route(x.ID, 0, 1, time.Now()); e != nil {
		t.Fatal(e)
	}
	f.MarkSend(x.ID, "uncertain")
	y, _ := f.Admit(a, b, time.Now())
	if !y.Held || len(f.TakeReady()) != 0 {
		t.Fatal("uncertain predecessor bypass")
	}
	if _, e := f.Begin("q", []string{b}, time.Now()); e != nil {
		t.Fatal(e)
	}
	z, _ := f.Admit(c, b, time.Now())
	if !z.Held {
		t.Fatal("creation gate bypass")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, e := f.WaitDrained(ctx, "q"); e == nil {
		t.Fatal("query bypassed outstanding work")
	}
}
