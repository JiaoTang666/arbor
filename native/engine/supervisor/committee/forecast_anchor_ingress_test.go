package committee

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestForecastAnchorBoundaryIngress(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	t.Setenv("SAA_FORECAST_STREAM_ID", "run")
	t.Setenv("SAA_FORECAST_CHECKPOINT_SHA256", strings.Repeat("1", 64))
	path := filepath.Join(t.TempDir(), "anchors.jsonl")
	t.Setenv("SAA_FORECAST_ANCHOR_PATH", path)
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2}
	defer saaIntegratedRuntimeStates.Delete(m)
	s := m.integratedStateLocked()
	s.forecastObservedSequence = 8000
	event := forecastAnchorState{StreamID: "run", ObservationKind: "relay_routed_input_not_commit", CheckpointHash: strings.Repeat("1", 64), AssignmentHash: strings.Repeat("2", 64), MembershipSemantics: "conditional_on_candidates", PredictedUncoveredCounts: []float64{1, 2, 3, 4}, AnchorVersion: 1, EvidenceEnd: 8000, Accounts: map[string]forecastAnchorAccount{}}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	m.applySAAIntegratedDueLocked(1)
	if s.forecastAnchorVersion != 0 {
		t.Fatal("partial event consumed")
	}
	if err = os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	m.applySAAIntegratedDueLocked(1)
	if s.forecastAnchorVersion != 1 || s.modelAnchor.StreamID != "run" {
		t.Fatal("boundary did not install Anchor")
	}
	m.applySAAIntegratedDueLocked(1)
	if s.forecastAnchorVersion != 1 {
		t.Fatal("event installed twice")
	}
	event.AnchorVersion = 2
	event.EvidenceEnd = 10000
	if err = m.installForecastAnchorLocked(event, 10000); err == nil {
		t.Fatal("caller invented watermark")
	}
	event.StreamID = "other"
	if err = m.installForecastAnchorLocked(event, 8000); err == nil {
		t.Fatal("wrong stream accepted")
	}
}
