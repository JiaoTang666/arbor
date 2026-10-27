package committee

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSerialExpiredOrExhaustedPlanDoesNotMutateRoutes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		accepted int64
		expiry   time.Time
		reason   string
	}{
		{"expired", 10, time.Now().Add(-time.Minute), "evidence_expired"},
		{"exhausted", 20, time.Now().Add(time.Minute), "input_fully_admitted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := params.TotalDataSize
			params.TotalDataSize = 20
			defer func() { params.TotalDataSize = old }()
			a, b := fmt.Sprintf("%040x", 2), fmt.Sprintf("%040x", 4)
			s := &serialGroupControl{directory: t.TempDir(), rounds: 3, next: 1, routes: map[string]uint64{}, history: map[string]nativeGroupSpec{}, used: map[string]bool{}, initial: map[string]uint64{a: 0, b: 0}, shards: 2}
			g := &nativeGroupControl{serial: s, stage: -1, inputCount: tc.accepted}
			p := serialGroupSelection{Schema: "saa_serial_selection_v2", Round: 1, ObservedOccurrences: 10, Decision: "MOVE", ExpiresAt: tc.expiry, Spec: nativeGroupSpec{Arm: "move", AfterOccurrences: 10}}
			for _, a := range []string{a, b} {
				p.Spec.Command.Members = append(p.Spec.Command.Members, core.SourcePrepareCommand{ProposalID: "expired-test", Account: a, SourceShard: 0, TargetShard: 1, BindAtProposal: true})
			}
			raw, _ := json.Marshal(p)
			if e := os.WriteFile(filepath.Join(s.directory, "round-001.json"), raw, 0600); e != nil {
				t.Fatal(e)
			}
			if active, e := g.pollSerialGroup(); e != nil || active {
				t.Fatal("skip failed", e)
			}
			if s.next != 2 || s.loaded || s.version != 0 || len(s.used) != 0 || len(s.routes) != 0 || len(s.history) != 0 || g.authority != nil {
				t.Fatal("skip mutated authority")
			}
			raw, e := os.ReadFile(filepath.Join(s.directory, "outcome-001.json"))
			if e != nil {
				t.Fatal(e)
			}
			var out map[string]interface{}
			if e = json.Unmarshal(raw, &out); e != nil {
				t.Fatal(e)
			}
			if out["decision"] != "KEEP" || out["requested"] != "MOVE" || out["reason"] != tc.reason {
				t.Fatal(string(raw))
			}
			// Final no-action rounds share the actual final prefix, no fake observations.
			g.inputCount = 20
			for r := 2; r <= 3; r++ {
				p = serialGroupSelection{Schema: "saa_serial_selection_v2", Round: r, ObservedOccurrences: 20, Decision: "KEEP", Reason: "observer_input_exhausted"}
				raw, _ = json.Marshal(p)
				if e = os.WriteFile(filepath.Join(s.directory, fmt.Sprintf("round-%03d.json", r)), raw, 0600); e != nil {
					t.Fatal(e)
				}
				if active, e := g.pollSerialGroup(); e != nil || active {
					t.Fatal("terminal keep failed", e)
				}
			}
			if _, e := g.pollSerialGroup(); e != nil || !s.done {
				t.Fatal("schedule failed to close", e)
			}
		})
	}
}
