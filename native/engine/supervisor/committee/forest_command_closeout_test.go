package committee

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func forestCommandCloseFixture(t *testing.T) *nativeGroupControl {
	t.Helper()
	old := params.TotalDataSize
	params.TotalDataSize = 100
	t.Cleanup(func() { params.TotalDataSize = old })
	t.Setenv("SAA_FOREST_INPUT_SHA256", strings.Repeat("a", 64))
	return &nativeGroupControl{serial: &serialGroupControl{directory: t.TempDir(), next: 1, continuous: true}, inputCount: 100, stage: -1}
}

func writeForestCommandSeal(t *testing.T, g *nativeGroupControl, change func(map[string]interface{})) {
	t.Helper()
	s := g.serial
	c := map[string]interface{}{
		"schema": "forest_commands_closed_v1", "last_round": s.next - 1,
		"mapping_version": s.version, "observed_occurrences": s.lastCutoff,
		"accepted_occurrences": 100, "total_occurrences": 100,
		"input_sha256": strings.Repeat("a", 64), "new_commands_closed": true,
	}
	if change != nil {
		change(c)
	}
	if err := os.WriteFile(filepath.Join(s.directory, "commands-closed.json"), mustSerialJSON(t, c), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestForestCommandCloseDoesNotWaitForObserverButWaitsForTransactions(t *testing.T) {
	g := forestCommandCloseFixture(t)
	g.serial.next, g.serial.version, g.serial.lastCutoff = 4, 2, 60
	writeForestCommandSeal(t, g, nil)
	m := &RelayCommitteeModule{groupControl: g, lifecycle: map[string]*ordinaryLifecycle{"pending": {Done: false}}}
	if active, err := g.pollSerialGroup(); err != nil || active || !g.serial.done {
		t.Fatalf("command seal did not close with lagging observer: active=%v done=%v err=%v", active, g.serial.done, err)
	}
	if _, err := os.Stat(filepath.Join(g.serial.directory, "closed.json")); !os.IsNotExist(err) {
		t.Fatalf("final observer seal unexpectedly required or created: %v", err)
	}
	if m.GroupCompletionReady() || m.NativeCloseoutError() == nil {
		t.Fatal("command closure bypassed unfinished transaction")
	}
	m.lifecycle["pending"].Done = true
	if !m.GroupCompletionReady() || m.NativeCloseoutError() != nil {
		t.Fatal("completed native work still waits for observer")
	}
}

func TestForestCommandCloseRequiresSealAndFullNativeAdmission(t *testing.T) {
	g := forestCommandCloseFixture(t)
	if _, err := g.pollSerialGroup(); err != nil || g.serial.done {
		t.Fatal("input EOF without command seal closed the stream", err)
	}
	writeForestCommandSeal(t, g, nil)
	g.inputCount = 99
	if _, err := g.pollSerialGroup(); err != nil || g.serial.done {
		t.Fatal("partial native admission closed the stream", err)
	}
	g.inputCount = 101
	if _, err := g.pollSerialGroup(); err == nil || g.serial.done {
		t.Fatal("excess native admission was accepted")
	}
	g.inputCount = 100
	if _, err := g.pollSerialGroup(); err != nil || !g.serial.done {
		t.Fatal("exact native admission did not close", err)
	}
}

func TestForestCommandCloseRejectsInvalidSealWithoutLegacyFallback(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		value       interface{}
	}{
		{"schema", "schema", "forest_stream_closed_v1"},
		{"open_commands", "new_commands_closed", false},
		{"partial_accepted", "accepted_occurrences", 99},
		{"wrong_total", "total_occurrences", 101},
		{"stale_round", "last_round", 1},
		{"future_round", "last_round", 3},
		{"stale_version", "mapping_version", 1},
		{"stale_observation", "observed_occurrences", 39},
		{"excess_observation", "observed_occurrences", 101},
		{"different_input", "input_sha256", strings.Repeat("b", 64)},
		{"malformed_input_hash", "input_sha256", "not-a-digest"},
		{"missing_closed_flag", "new_commands_closed", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := forestCommandCloseFixture(t)
			g.serial.next, g.serial.version, g.serial.lastCutoff = 3, 2, 40
			legacy := map[string]interface{}{"schema": "forest_stream_closed_v1", "last_round": 2, "observed_occurrences": 100, "mapping_version": 2}
			if err := os.WriteFile(filepath.Join(g.serial.directory, "closed.json"), mustSerialJSON(t, legacy), 0600); err != nil {
				t.Fatal(err)
			}
			writeForestCommandSeal(t, g, func(c map[string]interface{}) {
				if tc.value == nil {
					delete(c, tc.field)
				} else {
					c[tc.field] = tc.value
				}
			})
			if _, err := g.pollSerialGroup(); err == nil || g.serial.done {
				t.Fatal("invalid command seal was accepted or used legacy fallback")
			}
		})
	}
}

func TestForestCommandCloseDrainsEOFRacingPublishedRound(t *testing.T) {
	g := forestCommandCloseFixture(t)
	s := g.serial
	s.initial = map[string]uint64{}
	p := serialGroupSelection{Schema: "saa_serial_selection_v2", Round: 1, ObservedOccurrences: 20, Decision: "MOVE", ExpiresAt: time.Now().Add(time.Minute), Spec: nativeGroupSpec{Arm: "move", AfterOccurrences: 20}}
	for _, n := range []int{2, 4} {
		account := fmt.Sprintf("%040x", n)
		s.initial[account] = 0
		p.Spec.Command.Members = append(p.Spec.Command.Members, core.SourcePrepareCommand{ProposalID: "eof-race", Account: account, SourceShard: 0, TargetShard: 1, BindAtProposal: true})
	}
	s.shards = 2
	if err := os.WriteFile(filepath.Join(s.directory, "round-001.json"), mustSerialJSON(t, p), 0600); err != nil {
		t.Fatal(err)
	}
	writeForestCommandSeal(t, g, func(c map[string]interface{}) {
		c["last_round"], c["observed_occurrences"] = 1, 20
	})
	if active, err := g.pollSerialGroup(); err != nil || active || s.done || s.next != 2 {
		t.Fatalf("EOF-racing round was skipped: next=%d done=%v err=%v", s.next, s.done, err)
	}
	raw, err := os.ReadFile(filepath.Join(s.directory, "outcome-001.json"))
	if err != nil {
		t.Fatal(err)
	}
	var outcome struct {
		Decision string `json:"decision"`
		Reason   string `json:"reason"`
	}
	if err = json.Unmarshal(raw, &outcome); err != nil || outcome.Decision != "DEFER" || outcome.Reason != "input_fully_admitted" || g.authority != nil {
		t.Fatalf("EOF-racing MOVE did not get its normal terminal outcome: %s err=%v", raw, err)
	}
	if _, err = g.pollSerialGroup(); err != nil || !s.done {
		t.Fatal("drained last round did not permit command closure", err)
	}
}

func TestForestCommandCloseWaitsForPendingRetirementAndOutcome(t *testing.T) {
	g := forestCommandCloseFixture(t)
	s := g.serial
	s.loaded, s.version, s.lastCutoff = true, 1, 20
	s.selection = serialGroupSelection{Schema: "saa_serial_selection_v2", Round: 1, ObservedOccurrences: 20, Decision: "MOVE"}
	s.selectionHash = strings.Repeat("b", 64)
	g.spec.Arm, g.active, g.stage = "move", true, 4
	writeForestCommandSeal(t, g, func(c map[string]interface{}) {
		c["last_round"] = 1
	})
	m := &RelayCommitteeModule{groupControl: g}
	if active, err := g.pollSerialGroup(); err != nil || !active || s.done || !s.loaded || m.GroupCompletionReady() {
		t.Fatal("command closure bypassed pending retirement", err)
	}
	if _, err := os.Stat(filepath.Join(s.directory, "outcome-001.json")); !os.IsNotExist(err) {
		t.Fatalf("outcome published before retirement: %v", err)
	}
	// The existing migration controller sets retired only after validating the
	// committed source receipt and empty historical drain snapshot.
	g.retired = true
	if active, err := g.pollSerialGroup(); err != nil || active || !s.done || s.loaded || s.next != 2 || !m.GroupCompletionReady() {
		t.Fatal("validated retirement did not drain the round before closure", err)
	}
	raw, err := os.ReadFile(filepath.Join(s.directory, "outcome-001.json"))
	if err != nil || !strings.Contains(string(raw), `"reason":"retired"`) {
		t.Fatalf("terminal retirement outcome missing: %s err=%v", raw, err)
	}
}
