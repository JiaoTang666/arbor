package committee

import (
	"blockEmulator/core"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestForecastRelayStreamWatermark(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	path := filepath.Join(t.TempDir(), "input.jsonl")
	t.Setenv("SAA_FORECAST_STREAM_PATH", path)
	t.Setenv("SAA_FORECAST_STREAM_ID", "test-run")
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2}
	defer saaIntegratedRuntimeStates.Delete(m)
	tx := &core.Transaction{Sender: authorityAccount('a'), Recipient: authorityAccount('b'), RoutingFrozen: true, RoutingEpoch: 1}
	if err := m.recordForecastRoutedInput(tx); err != nil {
		t.Fatal(err)
	}
	if err := m.recordForecastRoutedInput(tx); err == nil {
		t.Fatal("duplicate accepted")
	}
	tx.IngressSequence = 2
	if err := m.recordForecastRoutedInput(tx); err == nil {
		t.Fatal("gap accepted")
	}
	tx.IngressSequence = 1
	if err := m.recordForecastRoutedInput(tx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 || m.integratedStateLocked().forecastObservedSequence != 2 {
		t.Fatal("watermark differs from output")
	}
	for i, line := range lines {
		var r map[string]interface{}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatal(err)
		}
		if r["accepted_sequence"] != float64(i) || r["stream_id"] != "test-run" {
			t.Fatal(r)
		}
	}
	// A new manager/run may not append to an existing stream silently.
	other := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2}
	defer saaIntegratedRuntimeStates.Delete(other)
	tx.IngressSequence = 0
	if err := other.recordForecastRoutedInput(tx); err == nil {
		t.Fatal("existing stream overwritten")
	}
}
