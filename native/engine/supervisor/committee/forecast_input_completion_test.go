package committee

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestForecastInputCompletionOwnWatermark(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	path := filepath.Join(t.TempDir(), "input.jsonl")
	t.Setenv("SAA_FORECAST_STREAM_PATH", path)
	t.Setenv("SAA_FORECAST_STREAM_ID", "run")
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2}
	defer saaIntegratedRuntimeStates.Delete(m)
	m.integratedStateLocked().forecastObservedSequence = 8
	if err := m.finishForecastInput(9); err == nil {
		t.Fatal("unexported inputs declared complete")
	}
	if err := m.finishForecastInput(8); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path + ".done.json")
	if err != nil {
		t.Fatal(err)
	}
	var marker map[string]interface{}
	if err = json.Unmarshal(data, &marker); err != nil {
		t.Fatal(err)
	}
	if marker["observed_sequence"] != float64(8) || marker["consumer_drained"] != false {
		t.Fatal(marker)
	}
	if err = m.finishForecastInput(8); err == nil {
		t.Fatal("duplicate completion replaced")
	}
}
