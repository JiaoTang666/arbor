package committee

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
)

type forecastDecisionMessage struct {
	StreamID  string                    `json:"stream_id"`
	InputJSON string                    `json:"input_json"`
	Envelope  forecastAuthorityEnvelope `json:"envelope"`
	Decision  struct {
		DecisionID     string `json:"decision_id"`
		InputHash      string `json:"input_sha256"`
		MappingVersion uint64 `json:"mapping_version"`
		AnchorVersion  uint64 `json:"anchor_version"`
		Selected       string `json:"selected"`
		Candidates     []struct {
			ID       string            `json:"id"`
			Feasible bool              `json:"feasible"`
			Changes  map[string]string `json:"changes"`
		} `json:"candidates"`
	} `json:"decision"`
}

func (message forecastDecisionMessage) validate(streamID string) error {
	e, d := message.Envelope, message.Decision
	hash := sha256.Sum256([]byte(message.InputJSON))
	if message.StreamID != streamID || streamID == "" || hex.EncodeToString(hash[:]) != e.InputHash || d.InputHash != e.InputHash || d.DecisionID != e.DecisionID || d.MappingVersion != e.MappingVersion || d.AnchorVersion != e.AnchorVersion {
		return fmt.Errorf("decision provenance mismatch")
	}
	var input struct {
		StreamID       string                       `json:"stream_id"`
		DecisionID     string                       `json:"decision_id"`
		MappingVersion uint64                       `json:"mapping_version"`
		AnchorVersion  uint64                       `json:"anchor_version"`
		Routes         map[string]uint64            `json:"routes"`
		QueueEpoch     int                          `json:"queue_epoch"`
		QueueFresh     bool                         `json:"queue_fresh"`
		Proposals      map[string]map[string]uint64 `json:"proposals"`
		RuntimeContext struct {
			LogicalRound int `json:"logical_round"`
		} `json:"runtime_context"`
	}
	if err := json.Unmarshal([]byte(message.InputJSON), &input); err != nil {
		return err
	}
	source, ok := input.Routes[e.Account]
	if e.EffectiveEpoch != input.RuntimeContext.LogicalRound+1 {
		return fmt.Errorf("effective epoch differs from cost context")
	}
	if !ok || source != e.SourceShard || input.StreamID != streamID || input.DecisionID != e.DecisionID || input.MappingVersion != e.MappingVersion || input.AnchorVersion != e.AnchorVersion || input.QueueEpoch != e.QueueEpoch || (!e.Keep && !input.QueueFresh) {
		return fmt.Errorf("envelope differs from cost input")
	}
	found := 0
	for _, candidate := range d.Candidates {
		if candidate.ID == d.Selected {
			found++
			if e.Keep {
				if d.Selected != "keep" || len(candidate.Changes) != 0 || e.TargetShard != e.SourceShard {
					return fmt.Errorf("keep selection mismatch")
				}
			} else {
				target, err := strconv.ParseUint(candidate.Changes[e.Account], 10, 64)
				if d.Selected == "keep" || !candidate.Feasible || len(candidate.Changes) != 1 || err != nil || target != e.TargetShard {
					return fmt.Errorf("migration selection mismatch")
				}
				proposal := input.Proposals[d.Selected]
				proposedTarget, exists := proposal[e.Account]
				if !exists || len(proposal) != 1 || proposedTarget != e.TargetShard {
					return fmt.Errorf("selected change absent from input proposals")
				}
			}
		}
	}
	if found != 1 {
		return fmt.Errorf("selected candidate missing or duplicated")
	}
	return nil
}

func (m *RuntimeUnknownAdmissionManager) consumeForecastDecisionsLocked(round int) error {
	s := m.integratedStateLocked()
	if s.controllerMode != forecastControllerMode || s.forecastDecisionPath == "" {
		return nil
	}
	file, err := os.Open(s.forecastDecisionPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < s.forecastDecisionOffset {
		return fmt.Errorf("decision stream truncated")
	}
	if _, err = file.Seek(s.forecastDecisionOffset, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		var message forecastDecisionMessage
		if err = json.Unmarshal(line, &message); err != nil {
			return err
		}
		if err = message.validate(s.forecastStreamID); err != nil {
			return err
		}
		receipt := m.submitForecastLocked(message.Envelope, round, 2)
		m.appendSAAIntegratedAuditLocked(map[string]interface{}{"record_type": "forecast_candidate_receipt", "stream_id": s.forecastStreamID, "decision_id": message.Envelope.DecisionID, "input_sha256": message.Envelope.InputHash, "selected": message.Decision.Selected, "authority_decision": receipt})
		s.forecastDecisionOffset += int64(len(line))
	}
}
