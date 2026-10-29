package committee

// Optional facts-only integration. This file never supplies a placement score.
import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type forestSeedManifestV2 struct {
	Schema      string `json:"schema"`
	Count       int    `json:"count"`
	RecordBytes int    `json:"record_bytes"`
	Cutoff      uint64 `json:"cutoff"`
	SourceTime  int64  `json:"source_time"`
	Namespace   string `json:"namespace"`
	SHA256      string `json:"sha256"`
	File        string `json:"file"`
}

func loadForestSeedV2(path string) (map[string]string, forestSeedManifestV2, error) {
	var meta forestSeedManifestV2
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, meta, err
	}
	if err = json.Unmarshal(raw, &meta); err != nil {
		return nil, meta, err
	}
	if meta.Schema != "forest_native_seed_v2" || meta.RecordBytes != 48 || meta.Count < 1 || meta.Namespace == "" || len(meta.SHA256) != 64 || filepath.Base(meta.File) != meta.File || meta.File == "." || meta.File == "" {
		return nil, meta, fmt.Errorf("invalid forest seed manifest")
	}
	f, err := os.Open(filepath.Join(filepath.Dir(path), meta.File))
	if err != nil {
		return nil, meta, err
	}
	defer f.Close()
	h := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(f, h), 1<<20)
	seed := make(map[string]string, meta.Count)
	var record [48]byte
	for i := 0; i < meta.Count; i++ {
		if _, err = io.ReadFull(reader, record[:]); err != nil {
			return nil, meta, err
		}
		for _, c := range record[:40] {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				return nil, meta, fmt.Errorf("forest seed contains non-address identity")
			}
		}
		a := string(record[:40])
		root := int64(binary.LittleEndian.Uint64(record[40:]))
		if root >= -1 {
			return nil, meta, fmt.Errorf("invalid permanent root")
		}
		if _, ok := seed[a]; ok {
			return nil, meta, fmt.Errorf("duplicate forest seed account")
		}
		seed[a] = meta.Namespace + ":" + strconv.FormatInt(root, 10)
	}
	if _, err = reader.ReadByte(); err != io.EOF {
		return nil, meta, fmt.Errorf("forest seed trailing data")
	}
	if hex.EncodeToString(h.Sum(nil)) != meta.SHA256 {
		return nil, meta, fmt.Errorf("forest seed hash mismatch")
	}
	return seed, meta, nil
}

type forestSlowMetaV2 struct {
	Schema     string                     `json:"schema"`
	Semantics  string                     `json:"semantics"`
	Version    int64                      `json:"graph_version"`
	GraphHash  string                     `json:"graph_sha256"`
	Cutoff     int64                      `json:"accepted_cutoff_exclusive"`
	SourceTime int64                      `json:"source_time"`
	Namespace  string                     `json:"namespace"`
	Shards     int                        `json:"shards"`
	SeedCutoff int64                      `json:"seed_cutoff"`
	Activity   map[string]json.RawMessage `json:"activity"`
}

// Graph and slow forest travel as ONE immutable file, never two latest values.
type forestJointFileV2 struct {
	Schema string          `json:"schema"`
	Graph  json.RawMessage `json:"graph"`
	Forest json.RawMessage `json:"forest"`
}
type forestJointCacheV2 struct {
	lightDayRates []float64
	mu             sync.RWMutex
	path           string
	modified       time.Time
	size           int64
	digest         string
	meta           forestSlowMetaV2
	joint          forestJointFileV2
	receivedMonoNS int64
}

func parseForestJointV2(raw []byte, shards int) (forestJointFileV2, forestSlowMetaV2, error) {
	var j forestJointFileV2
	var f forestSlowMetaV2
	var envelope struct {
		Schema  string `json:"schema"`
		Payload string `json:"payload_json"`
		Digest  string `json:"payload_sha256"`
	}
	if e := json.Unmarshal(raw, &envelope); e != nil {
		return j, f, e
	}
	h := sha256.Sum256([]byte(envelope.Payload))
	if envelope.Schema != "graph_forest_joint_v2" || hex.EncodeToString(h[:]) != envelope.Digest {
		return j, f, fmt.Errorf("forest joint payload integrity mismatch")
	}
	if e := json.Unmarshal([]byte(envelope.Payload), &j); e != nil {
		return j, f, e
	}
	j.Schema = envelope.Schema
	if e := json.Unmarshal(j.Forest, &f); e != nil {
		return j, f, e
	}
	var g struct {
		Version int64  `json:"version"`
		Cutoff  int64  `json:"accepted_cutoff_exclusive"`
		Time    int64  `json:"source_time"`
		Hash    string `json:"snapshot_sha256"`
	}
	if e := json.Unmarshal(j.Graph, &g); e != nil {
		return j, f, e
	}
	if f.Schema != "forest_slow_facts_v2" || f.Semantics != "source_activity_facts_v2.0" || f.Shards != shards || f.Namespace == "" || f.Version < 0 || f.Cutoff < f.SeedCutoff || f.SourceTime < 0 || len(f.GraphHash) != 64 || f.GraphHash != g.Hash || f.Version != g.Version || f.Cutoff != g.Cutoff || f.SourceTime != g.Time {
		return j, f, fmt.Errorf("graph/forest schema or causal watermarks mismatch")
	}
	values := map[string][]float64{}
	for _, key := range []string{"mature_weight", "active_weight", "participation_weight", "mature_sample_count", "pending_children", "censored_children", "active_fraction", "active_child_mean", "arrival_rate", "bound_accounts"} {
		var v []float64
		if e := json.Unmarshal(f.Activity[key], &v); e != nil || len(v) != shards {
			return j, f, fmt.Errorf("invalid forest activity %s", key)
		}
		for _, x := range v {
			if !finite(x) || x < 0 {
				return j, f, fmt.Errorf("nonfinite/negative forest activity")
			}
		}
		values[key] = v
	}
	flags := map[string][]bool{}
	for _, key := range []string{"no_mature", "no_active"} {
		var v []bool
		if e := json.Unmarshal(f.Activity[key], &v); e != nil || len(v) != shards {
			return j, f, fmt.Errorf("missing forest activity flags")
		}
		flags[key] = v
	}
	for s := 0; s < shards; s++ {
		mass, active, work := values["mature_weight"][s], values["active_weight"][s], values["participation_weight"][s]
		q, mean := 0., 0.
		if mass > 0 {
			q = active / mass
		}
		if active > 0 {
			mean = work / active
		}
		if active > mass+1e-9 || flags["no_mature"][s] != (mass == 0) || flags["no_active"][s] != (active == 0) || math.Abs(q-values["active_fraction"][s]) > 1e-9 || math.Abs(mean-values["active_child_mean"][s]) > 1e-9*math.Max(1, mean) {
			return j, f, fmt.Errorf("forest mature ratios or missing flags inconsistent")
		}
	}
	return j, f, nil
}

func (c *forestJointCacheV2) refresh(shards int, clock *placementClock) error {
	info, e := os.Stat(c.path)
	if e != nil {
		return e
	}
	c.mu.RLock()
	unchanged := !c.modified.IsZero() && info.ModTime() == c.modified && info.Size() == c.size
	c.mu.RUnlock()
	if unchanged {
		return nil
	}
	raw, e := os.ReadFile(c.path)
	if e != nil {
		return e
	}
	j, f, e := parseForestJointV2(raw, shards)
	if e != nil {
		return e
	}
	var graph struct { Rates [][]float64 `json:"relation_stage_rate_by_scale"` }
	if e=json.Unmarshal(j.Graph,&graph);e!=nil { return e }
	var day []float64
	if len(graph.Rates)==shards {
		day=make([]float64,shards)
		for s,row:=range graph.Rates {
			if len(row)!=3 || !finite(row[1]) || row[1]<0 { return fmt.Errorf("light graph day rate invalid") }
			day[s]=row[1]
		}
	}
	h := sha256.Sum256(raw)
	digest := hex.EncodeToString(h[:])
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.digest != "" {
		if f.Namespace != c.meta.Namespace || f.SeedCutoff != c.meta.SeedCutoff || f.Version < c.meta.Version || f.Cutoff < c.meta.Cutoff || f.SourceTime < c.meta.SourceTime {
			return fmt.Errorf("forest joint generation regressed or changed identity")
		}
		if f.Version == c.meta.Version && digest != c.digest {
			return fmt.Errorf("forest joint generation mutated")
		}
	}
	if digest != c.digest {
		c.joint, c.meta, c.digest, c.receivedMonoNS = j, f, digest, clock.now().MonoNS
		c.lightDayRates=day
	}
	c.modified, c.size = info.ModTime(), info.Size()
	return nil
}

func (m *RelayCommitteeModule) initForestNativeV2() error {
	enabled := os.Getenv("SAA_FOREST_FACTS_V2")
	if enabled == "" || enabled == "false" {
		if os.Getenv("SAA_FOREST_SEED_MANIFEST") != "" || os.Getenv("SAA_FOREST_JOINT_PATH") != "" {
			return fmt.Errorf("forest parameters present with facts interface disabled")
		}
		return nil
	}
	if enabled != "true" || !m.nativeBatch || m.nativeAudit == nil || m.offeredAuditPath == "" || m.placementSignals == nil || m.initialLedger == nil {
		return fmt.Errorf("forest v2 requires audited native binding and clock, not a placement policy")
	}
	seed, meta, e := loadForestSeedV2(os.Getenv("SAA_FOREST_SEED_MANIFEST"))
	if e != nil {
		return e
	}
	if len(seed) != len(m.initialLedger.Accounts) {
		return fmt.Errorf("forest seed/history ledger boundary mismatch")
	}
	for a := range seed {
		if _, exists := m.initialLedger.HistoricalOwner(a); !exists {
			return fmt.Errorf("forest seed account absent from initial ledger")
		}
	}
	if m.newNewPolicy != nil {
		if int64(meta.Cutoff) != m.newNewPolicy.base || meta.SourceTime != m.newNewPolicy.prior.SourceTime {
			return fmt.Errorf("forest/policy seed mismatch")
		}
		m.forestSource = m.newNewPolicy.source
	} else {
		raw, err := checkedFile(os.Getenv("SAA_FOREST_SOURCE_TIME_PATH"), os.Getenv("SAA_FOREST_SOURCE_TIME_SHA256"))
		if err != nil {
			return err
		}
		if len(raw) != 8*m.dataTotalNum {
			return fmt.Errorf("forest source time count mismatch")
		}
		m.forestSource = make([]int64, m.dataTotalNum)
		for i := range m.forestSource {
			m.forestSource[i] = int64(binary.LittleEndian.Uint64(raw[8*i : 8*i+8]))
			if m.forestSource[i] < meta.SourceTime || i > 0 && m.forestSource[i] < m.forestSource[i-1] {
				return fmt.Errorf("forest source clock regressed")
			}
		}
	}
	f, e := NewForestFactsV2(seed, meta.Namespace, meta.Cutoff, m.placementSignals.clock.id, params.ShardNum)
	if e != nil {
		return e
	}
	c := &forestJointCacheV2{path: os.Getenv("SAA_FOREST_JOINT_PATH")}
	if e = c.refresh(params.ShardNum, m.placementSignals.clock); e != nil {
		return e
	}
	if c.meta.Namespace != meta.Namespace || c.meta.Cutoff != int64(meta.Cutoff) || c.meta.SeedCutoff != int64(meta.Cutoff) || c.meta.SourceTime != meta.SourceTime {
		return fmt.Errorf("forest initial joint snapshot is not the pinned graph seed")
	}
	m.forestFacts, m.forestJoint = f, c
	go func() {
		for range time.NewTicker(250 * time.Millisecond).C {
			if e := c.refresh(params.ShardNum, m.placementSignals.clock); e != nil {
				panic(e)
			}
		}
	}()
	usage:="facts_only_not_placement_score"
	if m.newNewPolicy!=nil && lightModeV2(m.newNewPolicy.mode) { usage="causal_online_feature_facts_v2" }
	m.nativeAudit.write(map[string]interface{}{"event": "forest_facts_v2_startup", "seed": meta, "joint_sha256": c.digest, "clock_id": m.placementSignals.clock.id, "usage": usage, "permanent_and_planning_origins_separate": true})
	return nil
}

// Called before the current NN decision and before its first binding. The
// independent facts clock is sampled while holding the facts lock, not borrowed
// from a previously computed policy decision that a concurrent receipt may pass.
func (m *RelayCommitteeModule) queryForestNativeV2() {
	if m.forestFacts == nil {
		return
	}
	began := time.Now()
	fast, e := m.forestFacts.SnapshotNow(m.placementSignals.clock)
	if e != nil {
		panic(e)
	}
	c := m.forestJoint
	c.mu.RLock()
	meta, digest, received := c.meta, c.digest, c.receivedMonoNS
	c.mu.RUnlock()
	seq := m.placementSequence
	source := m.forestSource
	if source == nil && m.newNewPolicy != nil {
		source = m.newNewPolicy.source
	} // immutable legacy fixtures
	base := int64(m.forestFacts.cutoff)
	if meta.Cutoff > base+int64(seq) || meta.SourceTime > source[seq] {
		panic("forest query contains future accepted evidence")
	}
	// RawMessage buffers are immutable after cache replacement, hence readers
	// cannot observe a partially loaded generation and dispatch does no file I/O.
	m.nativeAudit.write(map[string]interface{}{"event": "forest_facts_v2_query", "sequence": seq, "fast": fast, "query_read_mono_ns": m.placementSignals.clock.now().MonoNS, "slow_graph_version": meta.Version, "slow_prefix": meta.Cutoff, "slow_source_time": meta.SourceTime, "slow_joint_sha256": digest, "slow_received_mono_ns": received, "prefix_lag": base + int64(seq) - meta.Cutoff, "source_age_seconds": source[seq] - meta.SourceTime, "query_ns": time.Since(began).Nanoseconds(), "influences_placement": false})
}

// Observe the route AFTER selection, regardless of which selector produced it.
// No policy, population ledger, or share controller is needed by this hook.
func (m *RelayCommitteeModule) bindForestFacts(tx *core.Transaction) {
	if m.forestFacts == nil {
		return
	}
	a, b := nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient)
	_, ka := m.forestFacts.Source(a)
	_, kb := m.forestFacts.Source(b)
	if m.newNewPolicy == nil && !ka && !kb {
		m.queryForestNativeV2()
	}
	source := m.forestSource
	if source == nil && m.newNewPolicy != nil {
		source = m.newNewPolicy.source
	}
	at := m.placementSignals.clock.now()
	if err := m.forestFacts.Bind(tx.IngressSequence, a, b, ka, kb, tx.RoutedSenderShard, tx.RoutedRecipientShard, source[tx.IngressSequence], at.MonoNS); err != nil {
		panic(err)
	}
	m.nativeAudit.write(map[string]interface{}{"event": "forest_binding", "sequence": tx.IngressSequence, "sender": a, "recipient": b, "known_sender": ka, "known_recipient": kb, "source_shard": tx.RoutedSenderShard, "target_shard": tx.RoutedRecipientShard, "dispatch_mono_ns": at.MonoNS, "clock_id": at.ClockID})
}
