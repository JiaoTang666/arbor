package committee

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func publishForecastMarker(path string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err = os.Stat(path); err == nil {
		return fmt.Errorf("marker already exists: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".forecast-marker-")
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

// Drain generated records without advancing logical time or applying future
// routes. Pending proposals remain explicitly pending at the injection cutoff.
func (m *RuntimeUnknownAdmissionManager) drainForecastConsumers(round int, timeout time.Duration) error {
	if m == nil || m.softDecisionMode != saaIntegratedDecisionMode {
		return nil
	}
	m.lock.Lock()
	s := m.integratedStateLocked()
	mode := s.controllerMode
	anchorPath, contextPath, decisionPath := s.forecastAnchorPath, s.forecastContextPath, s.forecastDecisionPath
	m.lock.Unlock()
	if mode != forecastControllerMode {
		return nil
	}
	if anchorPath == "" || contextPath == "" || decisionPath == "" {
		return fmt.Errorf("complete forecast consumer paths required")
	}
	deadline := time.Now().Add(timeout)
	contextClosed := false
	for {
		done, err := func() (bool, error) {
			m.lock.Lock()
			defer m.lock.Unlock()
			if !contextClosed {
				data, err := os.ReadFile(filepath.Join(filepath.Dir(anchorPath), "manifest.json"))
				if os.IsNotExist(err) {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				var marker struct {
					Status   string `json:"status"`
					StreamID string `json:"stream_id"`
					Observed uint64 `json:"observed_sequence"`
					Versions uint64 `json:"anchor_versions"`
				}
				if err = json.Unmarshal(data, &marker); err != nil {
					return false, err
				}
				expected := uint64(0)
				if s.forecastObservedSequence/2000 >= 4 {
					expected = s.forecastObservedSequence/2000 - 3
				}
				if marker.Status != "input_consumed" || marker.StreamID != s.forecastStreamID || marker.Observed != s.forecastObservedSequence || marker.Versions != expected {
					return false, fmt.Errorf("Anchor producer completion mismatch")
				}
				if err = m.consumeForecastAnchorsLocked(); err != nil {
					return false, err
				}
				if s.forecastAnchorVersion != expected {
					return false, fmt.Errorf("Anchor producer output not fully installed")
				}
				if err = m.exportForecastContextLocked(round); err != nil {
					return false, err
				}
				if s.forecastContextCount == 0 {
					file, err := os.OpenFile(contextPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
					if err != nil {
						return false, err
					}
					if err = file.Close(); err != nil {
						return false, err
					}
				}
				if err = publishForecastMarker(contextPath+".done.json", map[string]interface{}{"schema": "forecast_context_complete_v1", "stream_id": s.forecastStreamID, "contexts": s.forecastContextCount}); err != nil {
					return false, err
				}
				contextClosed = true
			}
			data, err := os.ReadFile(filepath.Join(filepath.Dir(decisionPath), "manifest.json"))
			if os.IsNotExist(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			var marker struct {
				Status    string `json:"status"`
				Decisions uint64 `json:"decisions"`
			}
			if err = json.Unmarshal(data, &marker); err != nil {
				return false, err
			}
			if marker.Status != "contexts_consumed" || marker.Decisions != s.forecastContextCount {
				return false, fmt.Errorf("decision producer completion mismatch")
			}
			if err = m.consumeForecastDecisionsLocked(round); err != nil {
				return false, err
			}
			info, err := os.Stat(decisionPath)
			if err != nil {
				return false, err
			}
			if info.Size() != s.forecastDecisionOffset {
				return false, fmt.Errorf("unconsumed decision tail")
			}
			err = publishForecastMarker(contextPath+".drained.json", map[string]interface{}{"stream_id": s.forecastStreamID, "consumer_drained": true, "logical_round": round, "anchor_version": s.forecastAnchorVersion, "contexts": s.forecastContextCount, "pending_proposals": len(s.authority.pending), "native_handoff_status": "not_connected_to_this_authority"})
			return err == nil, err
		}()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("forecast consumer drain timeout")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
