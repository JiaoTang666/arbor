package committee

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Read-only old-run reconstruction + one decision. Does not start nodes,
// change old artifacts or advance native state. A non-KEEP result requires a
// separate native execution stage, not fabricated execution success here.
func TestNativeSAAReDecisionCurrentMapping(t *testing.T) {
	root, out, project := os.Getenv("SAA_REDECISION_SOURCE"), os.Getenv("SAA_REDECISION_OUTPUT"), os.Getenv("SAA_PROTOCOL_TEST_PROJECT")
	if root == "" || out == "" || project == "" {
		t.Skip("explicit stopped-run redecision only")
	}
	read := func(name string, v interface{}) {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(raw, v); err != nil {
			t.Fatal(err)
		}
	}
	write := func(name string, v interface{}) {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(filepath.Join(out, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(append(raw, '\n'))
		closeErr := f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	// Bind the replay to the previous verified candidate, not chat history.
	var manifest struct {
		Hashes map[string]string `json:"hashes"`
	}
	read("verified_manifest.json", &manifest)
	provenance := map[string]string{}
	for _, name := range []string{"contexts.jsonl", "decision/authority_requests.jsonl", "model/anchor_runtime.jsonl", "model/relation_predictions.jsonl", "weights.json", "result.json", "control/source_prepared.json", "control/install_receipts.jsonl", "control/target_activated.json", "control/source_retired.json"} {
		path := filepath.Join(root, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		hash := hex.EncodeToString(sum[:])
		if manifest.Hashes[path] != hash {
			t.Fatalf("old evidence changed: %s", name)
		}
		provenance[path] = hash
	}
	var old struct {
		Routes     map[string]uint64  `json:"routes"`
		Round      int                `json:"logical_round"`
		QueueEpoch int                `json:"queue_epoch"`
		Queues     map[uint64]float64 `json:"queues"`
		Sequence   uint64             `json:"observed_sequence"`
	}
	read("contexts.jsonl", &old)
	var anchor forecastAnchorState
	read("model/anchor_runtime.jsonl", &anchor)
	var request forecastDecisionMessage
	read("decision/authority_requests.jsonl", &request)
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	t.Setenv("SAA_FORECAST_STREAM_ID", anchor.StreamID)
	t.Setenv("SAA_FORECAST_CHECKPOINT_SHA256", anchor.CheckpointHash)
	t.Setenv("SAA_REQUIRE_NATIVE_INSTALLATION_RECEIPT", "true")
	t.Setenv("SAA_FORECAST_STATE_COST_SCOPE", "committed_payload_v1")
	// Explicitly disable all producer paths in this read-only replay.
	for _, env := range []string{"SAA_NATIVE_TARGET_INSTALL_PATH", "SAA_NATIVE_TARGET_ACTIVATION_PATH", "SAA_NATIVE_SOURCE_CANDIDATE_PATH", "SAA_FORECAST_CONTEXT_PATH", "SAA_FORECAST_ANCHOR_PATH", "SAA_FORECAST_DECISION_PATH"} {
		t.Setenv(env, "")
	}
	t.Setenv("SAA_NATIVE_SOURCE_RETIRED_PATH", filepath.Join(root, "control/source_retired.json"))
	t.Setenv("SAA_NATIVE_TARGET_ACTIVATED_PATH", filepath.Join(root, "control/target_activated.json"))
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2, stableShardByAccount: old.Routes, stableShardByCluster: map[int]uint64{0: 0, 1: 1}, liveQueueByShard: old.Queues, pressureHintEpoch: old.QueueEpoch}
	defer saaIntegratedRuntimeStates.Delete(m)
	s := m.integratedStateLocked()
	s.forecastObservedSequence = old.Sequence
	if err := m.installForecastAnchorLocked(anchor, old.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := request.validate(anchor.StreamID); err != nil {
		t.Fatal(err)
	}
	accepted := m.submitForecastLocked(request.Envelope, old.Round, 2)
	if accepted.Status != mappingAccepted {
		t.Fatal("previous decision no longer validates", accepted)
	}
	if err := m.consumeNativeSourcePreparedLocked(filepath.Join(root, "control/source_prepared.json")); err != nil {
		t.Fatal(err)
	}
	var offset int64
	n, err := m.consumeNativeInstallationReceiptsLocked(filepath.Join(root, "control/install_receipts.jsonl"), &offset)
	if err != nil || n == 0 {
		t.Fatal("installation replay failed", n, err)
	}
	s.authority.applyDue(request.Envelope.EffectiveEpoch)
	if s.authority.routeVersion != 1 || s.authority.active[request.Envelope.Account] != request.Envelope.TargetShard {
		t.Fatal("could not reconstruct applied route version 1")
	}
	if err = m.validateNativeRetirementLocked(request.Envelope.DecisionID, request.Envelope.Account); err != nil {
		t.Fatal(err)
	}
	write("restored_authority.json", map[string]interface{}{"route_version": s.authority.routeVersion, "previous_decision": s.authority.decisionByID[request.Envelope.DecisionID], "retirement_witness_validated": true, "provenance": provenance})
	// Current native heads determine the read boundary. Historical queue/rate
	// observations are intentionally not promoted to the current generation.
	h0, _, _, err := readSAAAccount(root, 0, "0000000000000000000000000000000000000010")
	if err != nil {
		t.Fatal(err)
	}
	h1, _, _, err := readSAAAccount(root, 1, "00000000000000000000000000000000000003eb")
	if err != nil {
		t.Fatal(err)
	}
	round := int(h0.Header.Number) + 1
	if int(h1.Header.Number)+1 > round {
		round = int(h1.Header.Number) + 1
	}
	m.liveQueueByShard = nil
	m.pressureHintEpoch = 0
	snapshot := m.forecastContextLocked(round)
	snapshot["prediction_reuse_scope"] = "historical_prediction_not_new_inference; background_mapping_version_not_rewritten"
	if err = supplementPausedPayloads(root, snapshot); err != nil {
		t.Fatal(err)
	}
	sizes := snapshot["state_bytes"].(map[string]uint64)
	if sizes["0x0000000000000000000000000000000000000010"] == 0 {
		t.Fatal("hot sender payload still missing")
	}
	write("contexts.jsonl", snapshot)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", filepath.Join(project, "saa_research_data_contract_v1/scripts/build_runtime_decisions.py"), "--predictions", filepath.Join(root, "model/relation_predictions.jsonl"), "--anchors", filepath.Join(root, "model/anchor_runtime.jsonl"), "--contexts", filepath.Join(out, "contexts.jsonl"), "--weights", filepath.Join(root, "weights.json"), "--output", filepath.Join(out, "decision"))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("decision failed: %v %s", err, output)
	}
	if err = os.WriteFile(filepath.Join(out, "controller.log"), output, 0600); err != nil {
		t.Fatal(err)
	}
	s.forecastDecisionPath = filepath.Join(out, "decision/authority_requests.jsonl")
	if err = m.consumeForecastDecisionsLocked(round); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(s.forecastDecisionPath)
	if err != nil {
		t.Fatal(err)
	}
	var next forecastDecisionMessage
	if err = json.Unmarshal(data, &next); err != nil {
		t.Fatal(err)
	}
	receipt := s.forecastBridge.results[next.Envelope.DecisionID]
	write("authority_receipt.json", receipt)
	status := "MIGRATION_SELECTED_NOT_EXECUTED"
	if next.Envelope.Keep {
		if receipt.Status != mappingNoop || s.authority.routeVersion != 1 || len(s.authority.pending) != 0 {
			t.Fatal("KEEP altered restored routes")
		}
		status = "KEEP_NO_NATIVE_EXECUTION"
	}
	write("result.json", map[string]interface{}{"status": status, "selected": next.Decision.Selected, "decision_id": next.Envelope.DecisionID, "mapping_version": s.authority.routeVersion, "candidate_accounts": len(snapshot["routes"].(map[string]uint64)), "payload_accounts": len(sizes), "hot_sender_bytes": sizes["0x0000000000000000000000000000000000000010"], "native_started": false, "new_inference": false, "old_evidence_unchanged": true})
	// Verify preservation again after replay, including any accidental producer.
	for path, hash := range provenance {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != hash {
			t.Fatal("replay modified original evidence", path)
		}
	}
	t.Logf("REDECISION selected=%s receipt=%s mapping_version=%d payload_accounts=%d hot_sender_bytes=%d native_started=false", next.Decision.Selected, receipt.Status, s.authority.routeVersion, len(sizes), sizes["0x0000000000000000000000000000000000000010"])
}
