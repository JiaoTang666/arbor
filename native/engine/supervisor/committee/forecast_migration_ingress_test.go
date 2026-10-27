package committee

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func migrationWire(t *testing.T, account string) forecastDecisionMessage {
	t.Helper()
	input := map[string]interface{}{"stream_id": "run", "decision_id": "move", "mapping_version": 0, "anchor_version": 1, "routes": map[string]int{account: 0}, "queue_epoch": 1, "queue_fresh": true, "proposals": map[string]interface{}{"move": map[string]int{account: 1}}, "runtime_context": map[string]int{"logical_round": 1}}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	e := forecastAuthorityEnvelope{DecisionID: "move", InputHash: hex.EncodeToString(hash[:]), AnchorVersion: 1, Account: account, TargetShard: 1, EffectiveEpoch: 2, QueueAvailable: true, QueueEpoch: 1}
	raw := map[string]interface{}{"stream_id": "run", "input_json": string(data), "envelope": e, "decision": map[string]interface{}{"decision_id": "move", "input_sha256": e.InputHash, "mapping_version": 0, "anchor_version": 1, "selected": "move", "candidates": []interface{}{map[string]interface{}{"id": "move", "feasible": true, "changes": map[string]string{account: "1"}}}}}
	data, err = json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var wire forecastDecisionMessage
	if err = json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestForecastMigrationIngressActivationAndQueueVeto(t *testing.T) {
	for _, queues := range []bool{true, false} {
		t.Run(map[bool]string{true: "activate", false: "queue_missing"}[queues], func(t *testing.T) {
			t.Setenv(integratedControllerEnv, forecastControllerMode)
			t.Setenv("SAA_FORECAST_STREAM_ID", "run")
			path := filepath.Join(t.TempDir(), "requests.jsonl")
			t.Setenv("SAA_FORECAST_DECISION_PATH", path)
			account := authorityAccount('a')
			wire := migrationWire(t, account)
			data, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			// Duplicate lines exercise transport/authority idempotence, not just offsets.
			if err = os.WriteFile(path, []byte(string(data)+"\n"+string(data)+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2, stableShardByAccount: map[string]uint64{account: 0}, pressureHintEpoch: 1, saaSoftAuditPath: filepath.Join(t.TempDir(), "audit.jsonl")}
			if queues {
				m.liveQueueByShard = map[uint64]float64{0: 3, 1: 4}
			}
			defer saaIntegratedRuntimeStates.Delete(m)
			s := m.integratedStateLocked()
			s.forecastAnchorVersion = 1
			m.applySAAIntegratedDueLocked(1)
			if shard, _ := s.authority.resolve(account); shard != 0 {
				t.Fatal("early route activation")
			}
			if queues && len(s.authority.pending) != 1 {
				t.Fatal("duplicate submit changed pending count")
			}
			m.applySAAIntegratedDueLocked(2)
			shard, _ := s.authority.resolve(account)
			if queues {
				if shard != 1 || s.authority.routeVersion != 1 {
					t.Fatal("migration not applied exactly once")
				}
			} else {
				if shard != 0 || len(s.authority.pending) != 0 {
					t.Fatal("missing queue allowed migration")
				}
			}
			audit, err := os.ReadFile(m.saaSoftAuditPath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(audit), "forecast_candidate_receipt") || !strings.Contains(string(audit), wire.Envelope.InputHash) {
				t.Fatal("candidate receipt lineage missing")
			}
			if queues && (!strings.Contains(string(audit), "supervisor_route_map_only") || !strings.Contains(string(audit), "not_connected_to_this_authority")) {
				t.Fatal("route activation overstates native handoff")
			}
			if !queues && !strings.Contains(string(audit), "native_queue_evidence_missing_or_mismatched") {
				t.Fatal("queue veto not explained")
			}
		})
	}
}

func TestForecastMigrationWireRejectsChangedEpochAndTarget(t *testing.T) {
	wire := migrationWire(t, authorityAccount('a'))
	if err := wire.validate("run"); err != nil {
		t.Fatal(err)
	}
	wire.Envelope.EffectiveEpoch = 3
	if err := wire.validate("run"); err == nil {
		t.Fatal("changed epoch accepted")
	}
	wire.Envelope.EffectiveEpoch = 2
	wire.Envelope.TargetShard = 0
	if err := wire.validate("run"); err == nil {
		t.Fatal("changed target accepted")
	}
}
