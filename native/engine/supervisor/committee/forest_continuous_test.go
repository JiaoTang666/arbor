package committee

import (
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestForestContinuousReviewsPastThreeAndExplicitEOF(t *testing.T) {
	old := params.TotalDataSize
	params.TotalDataSize = 100
	defer func() { params.TotalDataSize = old }()
	s := &serialGroupControl{directory: t.TempDir(), next: 1, continuous: true}
	g := &nativeGroupControl{serial: s, inputCount: 100, stage: -1}
	for r := 1; r <= 20; r++ {
		decision := "KEEP"
		if r%2 == 0 {
			decision = "DEFER"
		}
		p := serialGroupSelection{Schema: "saa_serial_selection_v2", Round: r, ObservedOccurrences: int64(r), Decision: decision, Reason: "unit review, no authority action"}
		raw, _ := json.Marshal(p)
		if e := os.WriteFile(filepath.Join(s.directory, fmt.Sprintf("round-%03d.json", r)), raw, 0600); e != nil {
			t.Fatal(e)
		}
		if active, e := g.pollSerialGroup(); e != nil || active || s.done {
			t.Fatal("review incorrectly stopped", r, e)
		}
	}
	if s.next != 21 || g.authority != nil || s.version != 0 {
		t.Fatal("review mutated routes")
	}
	write := func(total int) {
		raw, _ := json.Marshal(map[string]interface{}{"schema": "forest_stream_closed_v1", "last_round": 20, "observed_occurrences": total, "mapping_version": 0})
		if e := os.WriteFile(filepath.Join(s.directory, "closed.json"), raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	write(99)
	if _, e := g.pollSerialGroup(); e == nil {
		t.Fatal("premature close accepted")
	}
	write(100)
	if _, e := g.pollSerialGroup(); e != nil || !s.done {
		t.Fatal("EOF did not close", e)
	}
}
