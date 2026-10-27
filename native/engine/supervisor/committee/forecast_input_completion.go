package committee

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Completion means no more relay inputs, not that consumers or native commits
// have finished. Publish atomically so readers never observe half a marker.
func (m *RuntimeUnknownAdmissionManager) finishForecastInput(expected uint64) error {
	if m == nil || m.softDecisionMode != saaIntegratedDecisionMode {
		return nil
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	s := m.integratedStateLocked()
	if s.controllerMode != forecastControllerMode {
		return nil
	}
	if s.forecastStreamPath == "" || s.forecastStreamID == "" || s.forecastObservedSequence != expected {
		return fmt.Errorf("relay completion watermark mismatch")
	}
	path := s.forecastStreamPath + ".done.json"
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("completion marker already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	data, err := json.Marshal(map[string]interface{}{"schema": "forecast_input_complete_v1", "stream_id": s.forecastStreamID, "observed_sequence": expected, "native_commits_complete": false, "consumer_drained": false})
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".forecast-complete-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.Write(append(data, '\n')); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
