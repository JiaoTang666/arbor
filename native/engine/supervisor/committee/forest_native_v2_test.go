package committee

import (
	"blockEmulator/initialledger"
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func forestTestEnvelope(t *testing.T, version int64, cutoff int64, source int64, mutate func(map[string]interface{})) []byte {
	t.Helper()
	n := 16
	zero := make([]float64, n)
	flags := make([]bool, n)
	for i := range flags {
		flags[i] = true
	}
	activity := map[string]interface{}{}
	for _, key := range []string{"mature_weight", "active_weight", "participation_weight", "mature_sample_count", "pending_children", "censored_children", "active_fraction", "active_child_mean", "arrival_rate", "bound_accounts"} {
		activity[key] = zero
	}
	activity["no_mature"], activity["no_active"] = flags, flags
	if mutate != nil {
		mutate(activity)
	}
	digest := hex.EncodeToString(make([]byte, 32))
	f := map[string]interface{}{"schema": "forest_slow_facts_v2", "semantics": "source_activity_facts_v2.0", "graph_version": version, "graph_sha256": digest, "accepted_cutoff_exclusive": cutoff, "source_time": source, "namespace": "test", "shards": n, "seed_cutoff": 2, "activity": activity}
	g := map[string]interface{}{"version": version, "accepted_cutoff_exclusive": cutoff, "source_time": source, "snapshot_sha256": digest}
	payload, _ := json.Marshal(map[string]interface{}{"graph": g, "forest": f})
	h := sha256.Sum256(payload)
	raw, _ := json.Marshal(map[string]interface{}{"schema": "graph_forest_joint_v2", "payload_json": string(payload), "payload_sha256": hex.EncodeToString(h[:])})
	return raw
}

func TestForestNativeJointCacheAndRejections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "joint.json")
	clock := newPlacementClock()
	c := &forestJointCacheV2{path: path}
	if e := os.WriteFile(path, forestTestEnvelope(t, 0, 2, 1000, nil), 0600); e != nil {
		t.Fatal(e)
	}
	if e := c.refresh(16, clock); e != nil {
		t.Fatal(e)
	}
	old := c.joint
	if e := os.WriteFile(path, forestTestEnvelope(t, 1, 3, 1001, nil), 0600); e != nil {
		t.Fatal(e)
	}
	stamp := time.Now().Add(time.Second)
	os.Chtimes(path, stamp, stamp)
	if e := c.refresh(16, clock); e != nil {
		t.Fatal(e)
	}
	if c.meta.Cutoff != 3 || string(old.Forest) == string(c.joint.Forest) {
		t.Fatal("cache generation did not replace atomically")
	}
	bad := forestTestEnvelope(t, 1, 3, 1001, func(a map[string]interface{}) { a["no_mature"] = make([]bool, 16) })
	if _, _, e := parseForestJointV2(bad, 16); e == nil {
		t.Fatal("inconsistent missing flags accepted")
	}
	var envelope map[string]interface{}
	json.Unmarshal(bad, &envelope)
	envelope["payload_sha256"] = "wrong"
	bad, _ = json.Marshal(envelope)
	if _, _, e := parseForestJointV2(bad, 16); e == nil {
		t.Fatal("content corruption accepted")
	}
	os.WriteFile(path, forestTestEnvelope(t, 0, 2, 1000, nil), 0600)
	stamp = stamp.Add(time.Second)
	os.Chtimes(path, stamp, stamp)
	if e := c.refresh(16, clock); e == nil {
		t.Fatal("regressed generation accepted")
	}
	if c.meta.Cutoff != 3 {
		t.Fatal("bad load replaced good cache")
	}
}

func TestForestNativeStartupRequiresAuditPolicy(t *testing.T) {
	t.Setenv("SAA_FOREST_FACTS_V2", "true")
	t.Setenv("SAA_NEWNEW_POLICY", "")
	m := &RelayCommitteeModule{}
	if e := m.configureNewNewPolicy(); e == nil {
		t.Fatal("silently disabled requested forest")
	}
	if e := m.initForestNativeV2(); e == nil {
		t.Fatal("enabled without audit/seed/policy")
	}
	t.Setenv("SAA_FOREST_FACTS_V2", "false")
	t.Setenv("SAA_FOREST_SEED_MANIFEST", "unexpected")
	if e := m.initForestNativeV2(); e == nil {
		t.Fatal("ignored disabled seed parameter")
	}
}

func TestForestNativePythonJointExport(t *testing.T) {
	path := os.Getenv("FOREST_V2_JOINT_FIXTURE")
	if path == "" {
		t.Skip("external full-history snapshot")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	_, f, e := parseForestJointV2(raw, 16)
	if e != nil {
		t.Fatal(e)
	}
	if f.Cutoff < 8840117 {
		t.Fatal("not full history fixture")
	}
}

func TestForestNativeRealSeedStartup(t *testing.T) {
	ledgerPath := os.Getenv("FOREST_V2_LEDGER")
	if ledgerPath == "" {
		t.Skip("explicit saved historical ledger required")
	}
	ledger, e := initialledger.Load(ledgerPath, os.Getenv("FOREST_V2_LEDGER_SHA256"), 16)
	if e != nil {
		t.Fatal(e)
	}
	metaRaw, e := os.ReadFile(os.Getenv("FOREST_V2_SEED"))
	if e != nil {
		t.Fatal(e)
	}
	var meta forestSeedManifestV2
	if e = json.Unmarshal(metaRaw, &meta); e != nil {
		t.Fatal(e)
	}
	jointRaw, e := os.ReadFile(os.Getenv("FOREST_V2_JOINT_SEED"))
	if e != nil {
		t.Fatal(e)
	}
	_, slow, e := parseForestJointV2(jointRaw, 16)
	if e != nil {
		t.Fatal(e)
	}
	m := &RelayCommitteeModule{initialLedger: ledger, offeredAuditPath: "test-memory", nativeAudit: &batchAudit{w: bufio.NewWriter(io.Discard)}, placementSignals: newPlacementSignals(16), newNewWork: &newNewWorkLedger{work: make([]int64, 16)}, newNewPolicy: &newNewPlacementPolicy{mode: "pressure_share", base: int64(meta.Cutoff), prior: &newNewPrior{Cutoff: int64(meta.Cutoff), SourceTime: meta.SourceTime, Digest: slow.GraphHash}}}
	t.Setenv("SAA_FOREST_FACTS_V2", "true")
	t.Setenv("SAA_FOREST_SEED_MANIFEST", os.Getenv("FOREST_V2_SEED"))
	t.Setenv("SAA_FOREST_JOINT_PATH", os.Getenv("FOREST_V2_JOINT_SEED"))
	if e = m.initForestNativeV2(); e != nil {
		t.Fatal(e)
	}
	if m.forestFacts == nil || len(m.forestFacts.seed) != 4412554 {
		t.Fatal("full historical seed not attached")
	}
	t.Logf("validated exact membership of %d real historical addresses; no nodes started", len(m.forestFacts.seed))
}
