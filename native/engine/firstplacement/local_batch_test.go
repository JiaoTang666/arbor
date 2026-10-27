package firstplacement

import (
	"strings"
	"testing"
	"time"
)

func TestLocalBatchOrderedAndExternalPredecessor(t *testing.T) {
	f := NewFence("batch")
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	head, _ := f.Admit(a, b, time.Now())
	f.Route(head.ID, 0, 0, time.Now())
	xs := []Flight{}
	eligible := map[string]int{}
	for i := 0; i < 3; i++ {
		x, _ := f.Admit(a, b, time.Now())
		xs = append(xs, x)
		eligible[x.ID] = 0
	}
	if len(f.TakeReadyLocalBatches(eligible, 256)) != 0 {
		t.Fatal("outside in-flight predecessor bypassed")
	}
	f.MarkSend(head.ID, "uncertain")
	if len(f.TakeReadyLocalBatches(eligible, 256)) != 0 {
		t.Fatal("uncertain predecessor bypassed")
	}
	f.Report(head.ID, a, b, 0, 0, 0, "inner", time.Now())
	batch := f.TakeReadyLocalBatches(eligible, 2)[0]
	if len(batch) != 2 || batch[0].ID != xs[0].ID || batch[1].ID != xs[1].ID {
		t.Fatal(batch)
	}
	for _, x := range batch {
		if e := f.Route(x.ID, 0, 0, time.Now()); e != nil {
			t.Fatal(e)
		}
	}
	if len(f.TakeReadyLocalBatches(eligible, 256)) != 0 {
		t.Fatal("split batch overtook prior batch")
	}
	f.Report(batch[0].ID, a, b, 0, 0, 0, "inner", time.Now())
	if len(f.TakeReadyLocalBatches(eligible, 256)) != 0 {
		t.Fatal("partial batch released successor")
	}
	f.Report(batch[1].ID, a, b, 0, 0, 0, "inner", time.Now())
	if got := f.TakeReadyLocalBatches(eligible, 256)[0]; len(got) != 1 || got[0].ID != xs[2].ID {
		t.Fatal(got)
	}
}
func TestLocalBatchCannotSkipCrossOrCreationGate(t *testing.T) {
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	for _, mode := range []string{"cross", "gate", "excluded_middle"} {
		t.Run(mode, func(t *testing.T) {
			f := NewFence(mode)
			head, _ := f.Admit(a, b, time.Now())
			f.Route(head.ID, 0, 1, time.Now())
			x, _ := f.Admit(a, b, time.Now())
			y, _ := f.Admit(a, b, time.Now())
			eligible := map[string]int{x.ID: 0, y.ID: 0}
			f.Report(head.ID, a, b, 0, 1, 0, "relay1", time.Now())
			if mode != "cross" {
				f.Report(head.ID, a, b, 0, 1, 1, "relay2", time.Now())
			}
			if mode == "gate" {
				f.Begin("creation", []string{b}, time.Now())
			}
			if mode == "excluded_middle" {
				delete(eligible, x.ID)
			}
			if len(f.TakeReadyLocalBatches(eligible, 256)) != 0 {
				t.Fatal("protected predecessor/gate bypassed")
			}
		})
	}
}
