package committee

import (
	"blockEmulator/core"
	"bytes"
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

// Opt-in protocol integration using synthetic relations and the actual trained
// checkpoint. It measures neither native execution nor prediction quality.
func TestForecastCrossLanguageProtocol(t *testing.T) {
	project := os.Getenv("SAA_PROTOCOL_TEST_PROJECT")
	python := os.Getenv("SAA_PROTOCOL_TEST_PYTHON")
	if project == "" || python == "" {
		t.Skip("set protocol test project and Python paths")
	}
	checkpoint := filepath.Join(project, "evidence/v35_clockfix_20260906/relation_model_v2_seed43/relation_model.pt")
	data, err := os.ReadFile(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	root := t.TempDir()
	stream := filepath.Join(root, "relay.jsonl")
	output := filepath.Join(root, "controller")
	contexts := filepath.Join(root, "contexts.jsonl")
	assignments := filepath.Join(root, "assignments.csv")
	left, right := authorityAccount('a'), authorityAccount('b')
	if err = os.WriteFile(assignments, []byte("account,cluster_id,is_noise\n"+left+",0,0\n"+right+",1,0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	t.Setenv("SAA_FORECAST_STREAM_ID", "synthetic-protocol-only")
	t.Setenv("SAA_FORECAST_STREAM_PATH", stream)
	t.Setenv("SAA_FORECAST_CHECKPOINT_SHA256", hex.EncodeToString(hash[:]))
	t.Setenv("SAA_FORECAST_ANCHOR_PATH", filepath.Join(output, "anchor_runtime.jsonl"))
	t.Setenv("SAA_FORECAST_CONTEXT_PATH", contexts)
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2,
		stableShardByAccount: map[string]uint64{left: 0, right: 1}, stableShardByCluster: map[int]uint64{0: 0, 1: 1},
		liveQueueByShard: map[uint64]float64{0: 3, 1: 4}, pressureHintEpoch: 1}
	defer saaIntegratedRuntimeStates.Delete(m)
	before := m.integratedStateLocked().authority.stateDigest()
	nativeTx := func(i int) *core.Transaction {
		tx, ok := data2tx([]string{"0", "0", "fixture", left, right, "", "0", "0", "1"}, uint64(i))
		if !ok {
			t.Fatal("native input fixture rejected")
		}
		tx.IngressSequence = uint64(i)
		tx.RoutingFrozen = true
		tx.RoutingEpoch = 1
		tx.RoutedRecipientShard = 1
		return tx
	}
	for i := 0; i < 1; i++ {
		tx := nativeTx(i)
		if err = m.recordForecastRoutedInput(tx); err != nil {
			t.Fatal(err)
		}
	}
	scripts := filepath.Join(project, "saa_research_data_contract_v1/scripts")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, python, filepath.Join(scripts, "run_live_anchor_controller.py"), "--input", stream, "--stream-id", "synthetic-protocol-only", "--checkpoint", checkpoint, "--assignments", assignments, "--output", output, "--follow")
	var modelOutput, candidateOutput bytes.Buffer
	command.Stdout = &modelOutput
	command.Stderr = &modelOutput
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	modelWaited := false
	defer func() {
		if !modelWaited {
			cancel()
			command.Wait()
		}
	}()
	state := m.integratedStateLocked()
	decisions := filepath.Join(root, "decisions")
	state.forecastDecisionPath = filepath.Join(decisions, "authority_requests.jsonl")
	candidate := exec.CommandContext(ctx, python, filepath.Join(scripts, "build_runtime_decisions.py"), "--predictions", filepath.Join(output, "relation_predictions.jsonl"), "--anchors", filepath.Join(output, "anchor_runtime.jsonl"), "--contexts", contexts, "--output", decisions, "--follow", "--stop-file", contexts+".done.json")
	candidate.Stdout = &candidateOutput
	candidate.Stderr = &candidateOutput
	if err = candidate.Start(); err != nil {
		t.Fatal(err)
	}
	candidateWaited := false
	defer func() {
		if !candidateWaited {
			cancel()
			candidate.Wait()
		}
	}()
	for i := 1; i < 8000; i++ {
		tx := nativeTx(i)
		if err = m.recordForecastRoutedInput(tx); err != nil {
			t.Fatal(err)
		}
	}
	if err = m.finishForecastInput(8000); err != nil {
		t.Fatal(err)
	}
	drainErr := m.drainForecastConsumers(1, 15*time.Second)
	if drainErr != nil {
		cancel()
	}
	modelErr := command.Wait()
	modelWaited = true
	candidateErr := candidate.Wait()
	candidateWaited = true
	if drainErr != nil || modelErr != nil || candidateErr != nil {
		t.Fatalf("drain=%v model=%v candidate=%v\nmodel: %s\ncandidate: %s", drainErr, modelErr, candidateErr, modelOutput.String(), candidateOutput.String())
	}
	if state.forecastAnchorVersion != 1 || state.modelAnchor == nil || len(state.modelAnchor.Accounts) != 2 {
		t.Fatal("Python Anchor not installed")
	}
	if before != state.authority.stateDigest() {
		t.Fatal("Anchor changed mapping")
	}
	result, err := os.ReadFile(filepath.Join(decisions, "decision_candidates.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var decision struct {
		Selected           string            `json:"selected"`
		Candidates         []json.RawMessage `json:"candidates"`
		AuthoritySubmitted bool              `json:"authority_submitted"`
	}
	if err = json.Unmarshal(result, &decision); err != nil {
		t.Fatal(err)
	}
	if decision.Selected != "keep" || len(decision.Candidates) != 3 || decision.AuthoritySubmitted {
		t.Fatalf("unexpected candidates: %s", result)
	}
	state.forecastDecisionPath = filepath.Join(decisions, "authority_requests.jsonl")
	m.applySAAIntegratedDueLocked(1)
	if len(state.forecastBridge.results) != 1 {
		t.Fatal("candidate request not consumed")
	}
	for _, receipt := range state.forecastBridge.results {
		if receipt.Status != mappingNoop || receipt.Reason != "controller_keep" {
			t.Fatalf("unexpected receipt: %+v", receipt)
		}
	}
	if before != state.authority.stateDigest() {
		t.Fatal("keep changed mapping")
	}
	m.applySAAIntegratedDueLocked(1)
	if len(state.forecastBridge.results) != 1 {
		t.Fatal("repeat boundary duplicated request")
	}
	if _, err = os.Stat(contexts + ".drained.json"); err != nil {
		t.Fatal("drain completion not recorded", err)
	}
	t.Log("concurrent model and candidate followers + Go drain -> 3 candidates -> authority keep receipt; actual checkpoint, synthetic protocol fixture only")
}
