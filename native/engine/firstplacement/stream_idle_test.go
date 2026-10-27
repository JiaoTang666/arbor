package firstplacement

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestWaitIdlePreservesDependencies(t *testing.T) {
	f := NewFence("s")
	a, b, c := strings.Repeat("0", 39)+"1", strings.Repeat("0", 39)+"2", strings.Repeat("0", 39)+"3"
	x, e := f.Admit(a, b, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if e = f.WaitIdle(ctx, []string{a}); e != context.DeadlineExceeded {
		t.Fatal(e)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if e = f.WaitIdle(ctx2, []string{c}); e != nil {
		t.Fatal(e)
	}
	if e = f.Route(x.ID, 0, 1, time.Now()); e != nil {
		t.Fatal(e)
	}
	if e = f.Report(x.ID, a, b, 0, 1, 0, "relay1", time.Now()); e != nil {
		t.Fatal(e)
	}
	short, done := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer done()
	if e = f.WaitIdle(short, nil); e != context.DeadlineExceeded {
		t.Fatal("first leg released wait", e)
	}
	if e = f.Report(x.ID, a, b, 0, 1, 1, "relay2", time.Now()); e != nil {
		t.Fatal(e)
	}
	if e = f.WaitIdle(ctx2, nil); e != nil {
		t.Fatal(e)
	}
}
