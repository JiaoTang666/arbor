package firstplacement

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOrderedAccountReleaseContract(t *testing.T) {
	f := NewFence("ordered")
	a, b, u, c, d, e := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("1", 40), strings.Repeat("c", 40), strings.Repeat("d", 40), strings.Repeat("e", 40)
	f.Begin("q", []string{u}, time.Now())
	f.WaitDrained(context.Background(), "q")
	first, _ := f.Admit(a, u, time.Now())
	t2, _ := f.Admit(b, u, time.Now())
	t3, _ := f.Admit(u, c, time.Now())
	if err := f.RouteCreator("q", first.ID, 0, 0, time.Now(), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	f.Report(first.ID, a, u, 0, 0, 0, "inner", time.Now())
	f.CompleteCreator("q")
	ready := f.TakeReady()
	if len(ready) != 1 || ready[0].ID != t2.ID {
		t.Fatal("not T2", ready)
	}
	f.Route(t2.ID, 0, 1, time.Now())
	t4, _ := f.Admit(d, e, time.Now())
	if t4.Held {
		t.Fatal("unrelated work blocked")
	}
	f.Route(t4.ID, 0, 0, time.Now())
	t5, _ := f.Admit(u, a, time.Now())
	if !t5.Held {
		t.Fatal("new arrival overtook HOLD")
	}
	f.Report(t2.ID, b, u, 0, 1, 0, "relay1", time.Now())
	if len(f.TakeReady()) != 0 {
		t.Fatal("relay1 unlocked successor")
	}
	f.MarkSend(t2.ID, "timeout")
	if len(f.TakeReady()) != 0 {
		t.Fatal("uncertain send unlocked successor")
	}
	f.Report(t4.ID, d, e, 0, 0, 0, "inner", time.Now())
	f.Report(t2.ID, b, u, 0, 1, 1, "relay2", time.Now())
	ready = f.TakeReady()
	if len(ready) != 1 || ready[0].ID != t3.ID {
		t.Fatal("T3 not next", ready)
	}
	f.Route(t3.ID, 1, 1, time.Now())
	f.Report(t3.ID, u, c, 1, 1, 1, "inner", time.Now())
	ready = f.TakeReady()
	if len(ready) != 1 || ready[0].ID != t5.ID {
		t.Fatal("T5 not last", ready)
	}
	if path := os.Getenv("SAA_ORDERED_EVIDENCE"); path != "" {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		err = json.NewEncoder(file).Encode(map[string]interface{}{"status": "PASS", "scope": "deterministic ledger contract, not native consensus", "same_account_order": []string{first.ID, t2.ID, t3.ID, t5.ID}, "independent_progress": t4.ID, "relay1_and_uncertain_send_release": false})
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
	}
}

func TestOrderedPreCutQueueStillDrains(t *testing.T) {
	f := NewFence("drain")
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	first, _ := f.Admit(a, b, time.Now())
	second, _ := f.Admit(a, b, time.Now())
	f.Route(first.ID, 0, 0, time.Now())
	f.Begin("q", []string{b}, time.Now())
	_, n, _ := f.Lease("q")
	if n != 2 {
		t.Fatal("queued pre-cut work omitted", n)
	}
	f.Report(first.ID, a, b, 0, 0, 0, "inner", time.Now())
	ready := f.TakeReady()
	if len(ready) != 1 || ready[0].ID != second.ID {
		t.Fatal("gate prevented old queue drain")
	}
	f.Route(second.ID, 0, 0, time.Now())
	f.Report(second.ID, a, b, 0, 0, 0, "inner", time.Now())
	_, n, _ = f.Lease("q")
	if n != 0 {
		t.Fatal(n)
	}
}
