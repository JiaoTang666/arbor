package committee

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

func forestTestV2(t *testing.T, shards int) *ForestFactsV2 {
	t.Helper()
	f, e := NewForestFactsV2(map[string]string{"h": "historical-root-1", "i": "historical-root-1", "j": "historical-root-2"}, "run", 100, "clock", shards)
	if e != nil {
		t.Fatal(e)
	}
	return f
}

func forestEventV2(t *testing.T, f *ForestFactsV2, seq uint64, a, b string, ka, kb bool, sa, sb uint64, at int64) {
	t.Helper()
	if e := f.Bind(seq, a, b, ka, kb, sa, sb, int64(seq), at); e != nil {
		t.Fatal(e)
	}
	if e := f.Assign(seq, fmt.Sprintf("hash-%d", seq), a, b, sa, sb, at); e != nil {
		t.Fatal(e)
	}
}

func TestForestFactsV2OriginsAndPosition(t *testing.T) {
	f := forestTestV2(t, 4)
	forestEventV2(t, f, 0, "a", "b", false, false, 2, 2, 0)
	a, _ := f.Source("a")
	b, _ := f.Source("b")
	if a.Parent != "" || b.Parent != "" || a.PermanentOrigin != b.PermanentOrigin || a.PermanentOrigin != "run:-102" || a.PlanningOrigin != "newnew:run:100" || !a.RuntimeNewNew || a.CommittedCreated {
		t.Fatalf("new-new roots: %#v %#v", a, b)
	}
	forestEventV2(t, f, 1, "c", "a", false, true, 1, 2, 1)
	c, _ := f.Source("c")
	if c.Parent != "a" || c.PlanningOrigin != a.PlanningOrigin || c.Shard != 1 {
		t.Fatalf("lineage is independent of placement: %#v", c)
	}
	forestEventV2(t, f, 2, "h", "d", true, false, 0, 0, 2)
	d, _ := f.Source("d")
	if d.Parent != "h" || d.PlanningOrigin != "historical:h" || d.PermanentOrigin != "historical-root-1" || d.RuntimeNewNew {
		t.Fatalf("historical child: %#v", d)
	}
	forestEventV2(t, f, 3, "c", "d", true, true, 1, 0, 3)
	c2, _ := f.Source("c")
	if c2.Parent != c.Parent || c2.PermanentOrigin != c.PermanentOrigin || c2.PlanningOrigin != c.PlanningOrigin {
		t.Fatal("cross-family transaction rewrote ancestry")
	}
	if e := f.Bind(4, "c", "h", true, true, 3, 0, 4, 4); e == nil {
		t.Fatal("accepted an existing-account move")
	}
	if _, e := f.Audit(); e != nil {
		t.Fatal(e)
	}
}

func TestForestFactsV2SelfAndIdentity(t *testing.T) {
	f := forestTestV2(t, 4)
	if e := f.Bind(0, "x", "x", false, false, 0, 1, 0, 0); e == nil {
		t.Fatal("split self")
	}
	forestEventV2(t, f, 0, "x", "x", false, false, 1, 1, 0)
	s, e := f.Snapshot(0, "clock")
	if e != nil {
		t.Fatal(e)
	}
	if s.NewOrigins20s[1] != 1 || s.ActiveOrigins20s[1] != 1 || s.AssignedNNHalfStages5s[1] != 2 || s.AssignedStages[1] != 1 {
		t.Fatalf("self double counting: %#v", s)
	}
	if _, ok := f.Source("missing"); ok {
		t.Fatal("unseen account known")
	}
	if e := f.Bind(1, "x", "y", false, false, 1, 1, 1, 1); e == nil {
		t.Fatal("claimed bound account is new")
	}
	if e := f.Assign(0, "different", "x", "x", 1, 1, 0); e == nil {
		t.Fatal("duplicate assignment")
	}
	if ok, e := f.Confirm(0, "wrong", "inner", 1, 1); e == nil || ok {
		t.Fatal("changed hash accepted")
	}
	if ok, e := f.Confirm(0, "hash-0", "inner", 1, 1); e != nil || !ok {
		t.Fatalf("confirm: %v %v", ok, e)
	}
	x, _ := f.Source("x")
	if !x.CommittedCreated {
		t.Fatal("creation evidence not recorded")
	}
	if len(f.pending) != 0 {
		t.Fatal("completed occurrence retained")
	}
	if _, e := f.Confirm(0, "hash-0", "inner", 1, 1); e == nil {
		t.Fatal("completed duplicate must go through native lifecycle gate")
	}
	// Equal payload hashes must not merge distinct accepted input occurrences.
	if e := f.Bind(1, "x", "x", true, true, 1, 1, 1, 1); e != nil {
		t.Fatal(e)
	}
	if e := f.Assign(1, "hash-0", "x", "x", 1, 1, 1); e != nil {
		t.Fatal(e)
	}
	s, _ = f.Snapshot(1, "clock")
	if s.AssignedStages[1] != 2 || s.NewOrigins20s[1] != 1 {
		t.Fatal("valid duplicate input was merged")
	}
}

func TestForestFactsV2HalfStageConservation(t *testing.T) {
	for _, shards := range []int{4, 8, 12, 16} {
		t.Run(fmt.Sprint(shards), func(t *testing.T) {
			f := forestTestV2(t, shards)
			forestEventV2(t, f, 0, "a", "b", false, false, 0, 0, 0)
			forestEventV2(t, f, 1, "a", "h", true, true, 0, uint64(shards-1), 1)
			forestEventV2(t, f, 2, "h", "i", true, true, uint64(shards-1), 1, 2)
			// Relay confirmations can arrive out of order and repeat while pending.
			if ok, e := f.Confirm(1, "hash-1", "relay2", uint64(shards-1), 3); e != nil || !ok {
				t.Fatal(ok, e)
			}
			if ok, e := f.Confirm(1, "hash-1", "relay2", uint64(shards-1), 4); e != nil || ok {
				t.Fatal("pending duplicate changed totals", ok, e)
			}
			if ok, e := f.Confirm(1, "hash-1", "relay1", 0, 4); e != nil || !ok {
				t.Fatal(ok, e)
			}
			s, _ := f.Snapshot(4, "clock")
			if s.AssignedStages[0] != 2 || s.AssignedCategoryHalfStages["runtime_newnew"][0] != 3 || s.AssignedCategoryHalfStages["historical_entrance"][0] != 1 {
				t.Fatal("split not one half per source")
			}
			// Same permanent root h/i still have distinct planning entrances.
			if f.familyWork[forestWorkKeyV2{"historical:h", 1}].assigned != 1 || f.familyWork[forestWorkKeyV2{"historical:i", 1}].assigned != 1 {
				t.Fatal("permanent and planning views collapsed")
			}
			a, e := f.Audit()
			if e != nil || a.Status != "PASS" || a.Assigned != 3 || a.Completed != 1 {
				t.Fatal(a, e)
			}
		})
	}
}

func TestForestFactsV2ClocksAndFailureAtomicity(t *testing.T) {
	f := forestTestV2(t, 4)
	forestEventV2(t, f, 0, "a", "b", false, false, 0, 0, 0)
	if _, e := f.Snapshot(0, "foreign"); e == nil {
		t.Fatal("accepted clock mismatch")
	}
	if _, e := f.Snapshot(-1, "clock"); e == nil {
		t.Fatal("negative clock")
	}
	if _, e := f.Snapshot(100, "clock"); e != nil {
		t.Fatal(e)
	}
	if e := f.Bind(1, "h", "c", true, false, 0, 0, 1, 99); e == nil {
		t.Fatal("regressed ingress clock")
	}
	if _, ok := f.Source("c"); ok {
		t.Fatal("failed operation left binding")
	}
	if _, e := f.ConfirmWithClock(0, "hash-0", "inner", 0, 50, "foreign"); e == nil {
		t.Fatal("foreign confirmation")
	}
	// Confirm may be delivered after a newer local decision timestamp.
	if ok, e := f.ConfirmWithClock(0, "hash-0", "inner", 0, 50, "clock"); e != nil || !ok {
		t.Fatal(ok, e)
	}
	if f.now != 100 {
		t.Fatal("late receipt changed activity watermark")
	}
	if e := f.Bind(1, "h", "c", true, false, 0, 0, 1, 100); e != nil {
		t.Fatal(e)
	}
	if e := f.Assign(1, "hash-1", "h", "c", 0, 0, 99); e == nil {
		t.Fatal("assignment precedes binding")
	}
	if e := f.Assign(1, "hash-1", "h", "c", 0, 0, 100); e != nil {
		t.Fatal(e)
	}
	if e := f.Bind(2, "h", "d", true, false, 0, 0, 0, 101); e == nil {
		t.Fatal("source time regressed")
	}
	if _, e := f.Audit(); e != nil {
		t.Fatal(e)
	}
}

func TestForestFactsV2WindowExpiryAndConcentration(t *testing.T) {
	f := forestTestV2(t, 4)
	sec := int64(time.Second)
	forestEventV2(t, f, 0, "a", "b", false, false, 0, 0, 0)
	forestEventV2(t, f, 1, "c", "d", false, false, 0, 0, sec)
	s, _ := f.Snapshot(sec, "clock")
	if s.NewOrigins20s[0] != 2 || s.ActiveOrigins20s[0] != 2 || math.Abs(s.Concentration20s[0]-0.5) > 1e-12 {
		t.Fatal("equal family concentration", s)
	}
	forestEventV2(t, f, 2, "a", "b", true, true, 0, 0, 2*sec)
	s, _ = f.Snapshot(2*sec, "clock")
	if math.Abs(s.Concentration20s[0]-5.0/9.0) > 1e-12 {
		t.Fatal("weighted concentration", s.Concentration20s[0])
	}
	s, _ = f.Snapshot(5*sec, "clock")
	if s.AssignedNNHalfStages5s[0] != 4 {
		t.Fatal("5s boundary should expire t=0")
	}
	s, _ = f.Snapshot(20*sec, "clock")
	if s.NewOrigins20s[0] != 1 || s.ActiveOrigins20s[0] != 2 {
		t.Fatal("birth and activity semantics conflated")
	}
	s, _ = f.Snapshot(22*sec, "clock")
	if s.ActiveOrigins20s[0] != 0 || !s.NoActivity20s[0] || s.RetainedRecentEvents != 0 {
		t.Fatal("dormant family remains active")
	}
	if a, ok := f.Source("a"); !ok || a.PlanningOrigin == "" {
		t.Fatal("expiration deleted permanent fact")
	}
	forestEventV2(t, f, 3, "a", "b", true, true, 0, 0, 23*sec)
	s, _ = f.Snapshot(23*sec, "clock")
	if s.ActiveOrigins20s[0] != 1 || s.NewOrigins20s[0] != 0 || s.Concentration20s[0] != 1 {
		t.Fatal("reactivation counted as birth")
	}
}

func TestForestFactsV2PrefixSnapshotAndCopies(t *testing.T) {
	f := forestTestV2(t, 4)
	before, _ := f.Snapshot(0, "clock")
	forestEventV2(t, f, 0, "a", "b", false, false, 1, 1, 0)
	after, _ := f.Snapshot(0, "clock")
	if before.BoundPrefix != 0 || before.AssignedPrefix != 0 || after.BoundPrefix != 1 || after.AssignedPrefix != 1 || before.NewOrigins20s[1] != 0 {
		t.Fatal("same-time prefix ambiguity")
	}
	after.AssignedStages[1] = 999
	after.NewOrigins20s[1] = 999
	after.AssignedCategoryHalfStages["runtime_newnew"][1] = 999
	again, _ := f.Snapshot(0, "clock")
	if again.AssignedStages[1] != 1 || again.NewOrigins20s[1] != 1 || again.AssignedCategoryHalfStages["runtime_newnew"][1] != 2 {
		t.Fatal("mutable snapshot alias")
	}
	// Reading a different future suffix cannot alter a saved prior decision view.
	forestEventV2(t, f, 1, "a", "h", true, true, 1, 3, 1)
	if before.AssignedStages[1] != 0 || again.AssignedStages[1] != 1 {
		t.Fatal("future write changed prefix evidence")
	}
}

func TestForestFactsV2ShardPermutation(t *testing.T) {
	f, g := forestTestV2(t, 4), forestTestV2(t, 4)
	perm := []uint64{2, 0, 3, 1}
	for _, x := range []struct {
		seq    uint64
		a, b   string
		ka, kb bool
		sa, sb uint64
	}{{0, "a", "b", false, false, 0, 0}, {1, "a", "h", true, true, 0, 1}, {2, "c", "d", false, false, 3, 3}} {
		forestEventV2(t, f, x.seq, x.a, x.b, x.ka, x.kb, x.sa, x.sb, int64(x.seq))
		forestEventV2(t, g, x.seq, x.a, x.b, x.ka, x.kb, perm[x.sa], perm[x.sb], int64(x.seq))
	}
	a, _ := f.Snapshot(3, "clock")
	b, _ := g.Snapshot(3, "clock")
	for s, p := range perm {
		if a.AssignedStages[s] != b.AssignedStages[p] || a.NewOrigins20s[s] != b.NewOrigins20s[p] || a.Concentration20s[s] != b.Concentration20s[p] {
			t.Fatal("shard identities changed facts")
		}
	}
}

func TestForestFactsV2ConcurrentReceiptSnapshot(t *testing.T) {
	f := forestTestV2(t, 4)
	for i := 0; i < 100; i++ {
		forestEventV2(t, f, uint64(i), "h", "i", true, true, 0, 1, int64(i))
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for leg, s := range map[string]uint64{"relay1": 0, "relay2": 1} {
				if _, e := f.Confirm(uint64(i), fmt.Sprintf("hash-%d", i), leg, s, 200); e != nil {
					t.Error(e)
				}
			}
		}(i)
	}
	for i := 0; i < 100; i++ {
		if _, e := f.Snapshot(200, "clock"); e != nil {
			t.Fatal(e)
		}
	}
	wg.Wait()
	a, e := f.Audit()
	if e != nil || a.Completed != 100 || a.OutstandingOccurrences != 0 || !reflect.DeepEqual(a.AssignedStages, a.ConfirmedStages) {
		t.Fatal(a, e)
	}
}

func TestForestFactsV2UnknownPermanentAndSeedBoundary(t *testing.T) {
	f, e := NewForestFactsV2(map[string]string{"h": ""}, "run", 3, "clock", 4)
	if e != nil {
		t.Fatal(e)
	}
	h, ok := f.Source("h")
	if !ok || !h.Historical || !h.CommittedCreated || h.BirthKnown || h.PositionKnown || h.PermanentOrigin != forestUnknownV2 || h.PlanningOrigin != "historical:h" {
		t.Fatal(h)
	}
	// An independently proven cutoff-account identity remains a known planning
	// entrance even when its earlier permanent ancestry is absent.
	forestEventV2(t, f, 0, "h", "n", true, false, 0, 0, 0)
	n, _ := f.Source("n")
	if n.PlanningOrigin != "historical:h" || n.PermanentOrigin != forestUnknownV2 {
		t.Fatal(n)
	}
}

func TestForestFactsV2ConfirmationQueryBoundary(t *testing.T) {
	f := forestTestV2(t, 4)
	forestEventV2(t, f, 0, "a", "b", false, false, 0, 0, 0)
	if _, e := f.Confirm(0, "hash-0", "inner", 0, 10); e != nil {
		t.Fatal(e)
	}
	if _, e := f.Snapshot(9, "clock"); e == nil {
		t.Fatal("future confirmed state leaked into past query")
	}
	s, e := f.Snapshot(10, "clock")
	if e != nil || s.ConfirmedReceiptMonoNS != 10 {
		t.Fatal(s, e)
	}
	clock := newPlacementClock()
	g, e := NewForestFactsV2(map[string]string{}, "live", 0, clock.id, 4)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := g.SnapshotNow(clock); e != nil {
		t.Fatal(e)
	}
	if _, e := g.SnapshotNow(newPlacementClock()); e == nil {
		t.Fatal("foreign clock now accepted")
	}
}
