package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type nativeGroupSpec struct {
	Arm              string                   `json:"arm"`
	AfterOccurrences int64                    `json:"after_occurrences"`
	Command          core.GroupPrepareCommand `json:"command"`
}
type nativeGroupControl struct {
	serial             *serialGroupControl
	mu                 sync.Mutex
	inputCount         int64
	spec               nativeGroupSpec
	authority          *mappingAuthority
	stage              int
	active             bool
	prepared           *core.GroupPreparation
	activation         chain.GroupTargetActivation
	retired            bool
	audit              *batchAudit
	lastCompletionScan time.Time
	completedHandoffs  map[string]bool
	deferred           *deferredGroupControl
}
type groupControlEvidence struct {
	Record    json.RawMessage `json:"record"`
	Committed bool            `json:"committed"`
	Block     uint64          `json:"block"`
	BlockHash string          `json:"block_hash"`
	StateRoot string          `json:"state_root"`
	Shard     uint64          `json:"shard"`
}

func (m *RelayCommitteeModule) configureGroupControl() error {
	if core.SerialGroupEnabled() {
		return m.configureSerialGroupControl()
	}
	if os.Getenv("SAA_DEFERRED_GROUP_CONTROL") == "true" {
		return m.configureDeferredGroupControl()
	}
	path := os.Getenv("SAA_NATIVE_GROUP_CONTROL_SPEC")
	if path == "" {
		return nil
	}
	if !m.nativeBatch || m.initialLedger == nil {
		return fmt.Errorf("group control requires native batch and initial ledger")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	var spec nativeGroupSpec
	if e = json.Unmarshal(raw, &spec); e != nil {
		return e
	}
	if spec.Arm != "keep" && spec.Arm != "move" {
		return fmt.Errorf("invalid group arm")
	}
	if spec.AfterOccurrences < 1 || len(spec.Command.Members) < core.MinMigrationGroupMembers || len(spec.Command.Members) > core.MaxMigrationGroupMembers {
		return fmt.Errorf("invalid group size/start")
	}
	initial := map[string]uint64{}
	first := spec.Command.Members[0]
	for i, c := range spec.Command.Members {
		decoded, e := hex.DecodeString(c.Account)
		if e != nil || len(decoded) != 20 || c.Account != strings.ToLower(c.Account) || c.ProposalID == "" || c.ProposalID != first.ProposalID || c.SourceShard != first.SourceShard || c.TargetShard != first.TargetShard || c.MappingVersion != 0 || c.AnchorVersion != first.AnchorVersion || !c.BindAtProposal || c.ExpectedBlockHash != "" || c.ExpectedStateRoot != "" || c.ExpectedStateHash != "" || (i > 0 && spec.Command.Members[i-1].Account >= c.Account) {
			return fmt.Errorf("invalid group intent")
		}
		shard, ok := m.initialLedger.Routes[c.Account]
		if !ok || shard != c.SourceShard {
			return fmt.Errorf("group member not owned by source initial ledger")
		}
		initial["0x"+c.Account] = shard
	}
	for _, key := range []string{"SAA_GROUP_SOURCE_PREPARE_PATH", "SAA_GROUP_SOURCE_PREPARED_PATH", "SAA_GROUP_TARGET_INSTALL_PATH", "SAA_GROUP_TARGET_INSTALLED_PATH", "SAA_GROUP_TARGET_ACTIVATION_PATH", "SAA_GROUP_TARGET_ACTIVATED_PATH", "SAA_GROUP_SOURCE_RETIRED_PATH"} {
		if os.Getenv(key) == "" {
			return fmt.Errorf("group transport missing %s", key)
		}
	}
	if m.admission == nil {
		return fmt.Errorf("group control requires configured shard count")
	}
	if first.SourceShard >= m.admission.shardNum || first.TargetShard >= m.admission.shardNum || first.SourceShard == first.TargetShard {
		return fmt.Errorf("invalid group source/target")
	}
	a := newMappingAuthority(m.admission.shardNum, initial)
	a.enableInstallationReceiptGate()
	m.groupControl = &nativeGroupControl{spec: spec, authority: a, audit: m.nativeAudit}
	return nil
}
func (g *nativeGroupControl) event(name string, details interface{}) {
	if g.audit != nil {
		id := ""
		if len(g.spec.Command.Members) > 0 {
			id = g.spec.Command.Members[0].ProposalID
		}
		g.audit.write(map[string]interface{}{"event": "group_" + name, "at": time.Now().UTC(), "proposal_id": id, "details": details})
	}
}
func groupControlRead(key string, id ...string) (*groupControlEvidence, error) {
	raw, e := os.ReadFile(groupControlPath(key, id...))
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var r groupControlEvidence
	if e = json.Unmarshal(raw, &r); e != nil {
		return nil, e
	}
	if !r.Committed || r.Block == 0 || !validTransferDigest(r.BlockHash) || !validTransferDigest(r.StateRoot) {
		return nil, fmt.Errorf("invalid committed group evidence %s", key)
	}
	return &r, nil
}
func groupControlWrite(key string, v interface{}, id ...string) error {
	raw, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return core.PublishSourcePrepareFile(groupControlPath(key, id...), raw)
}
func (g *nativeGroupControl) poll() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.serial != nil {
		proceed, err := g.pollSerialGroup()
		if err != nil || !proceed {
			return err
		}
	}
	if g.deferred != nil {
		proceed, err := g.pollDeferredGroup(time.Now())
		if err != nil || !proceed {
			return err
		}
	}
	if atomic.LoadInt64(&g.inputCount) < g.spec.AfterOccurrences {
		return nil
	}
	first := g.spec.Command.Members[0]
	if g.stage == 0 {
		if g.spec.Arm == "keep" {
			g.event("keep", g.spec)
			g.stage = 99
			return nil
		}
		accounts := []string{}
		for _, c := range g.spec.Command.Members {
			accounts = append(accounts, "0x"+c.Account)
		}
		d := g.authority.submit(mappingAuthorityProposal{ProposalID: first.ProposalID, Source: "fixed_group_native_v1", Addresses: accounts, SourceShard: first.SourceShard, TargetShard: first.TargetShard, BaseRouteVersion: first.MappingVersion, EffectiveEpoch: 1}, 0)
		if d.Status != mappingAccepted {
			return fmt.Errorf("group proposal rejected: %+v", d)
		}
		if e := groupControlWrite("SAA_GROUP_SOURCE_PREPARE_PATH", g.spec.Command, first.ProposalID); e != nil {
			return e
		}
		g.stage = 1
		g.event("requested", g.spec)
	}
	if g.stage == 1 {
		r, e := groupControlRead("SAA_GROUP_SOURCE_PREPARED_PATH", first.ProposalID)
		if e != nil || r == nil {
			return e
		}
		var p core.GroupPreparation
		if e = json.Unmarshal(r.Record, &p); e != nil {
			return e
		}
		if r.Shard != first.SourceShard || r.Block != p.PrepareBlock || p.PrepareSequence == 0 || !validTransferDigest(p.CommitDigest) || len(p.Members) != len(g.spec.Command.Members) || len(p.Command.Members) != len(p.Members) {
			return fmt.Errorf("source group evidence mismatch")
		}
		for i, m := range p.Members {
			intent := m.Command
			intent.ExpectedBlockHash = ""
			intent.ExpectedStateRoot = ""
			intent.ExpectedStateHash = ""
			hash := sha256.Sum256(m.State)
			if intent != g.spec.Command.Members[i] || m.Command != p.Command.Members[i] || m.PrepareBlock != p.PrepareBlock || m.PrepareSequence != p.PrepareSequence || m.CommitDigest != p.CommitDigest || hex.EncodeToString(hash[:]) != m.StateHash || m.StateHash != m.Command.ExpectedStateHash || m.SourceStateRoot != m.Command.ExpectedStateRoot || !validTransferDigest(m.Command.ExpectedBlockHash) {
				return fmt.Errorf("source group member mismatch")
			}
			e = g.authority.registerPreparedTransfer(NativeStateTransferEnvelope{ProposalID: first.ProposalID, Account: "0x" + m.Command.Account, SourceShard: first.SourceShard, TargetShard: first.TargetShard, MappingVersion: first.MappingVersion, AnchorVersion: m.Command.AnchorVersion, SourceStateRoot: m.SourceStateRoot, StateHash: m.StateHash})
			if e != nil {
				return e
			}
		}
		if e = groupControlWrite("SAA_GROUP_TARGET_INSTALL_PATH", p, first.ProposalID); e != nil {
			return e
		}
		g.prepared = &p
		g.stage = 2
		g.event("prepared", map[string]interface{}{"block": r.Block, "root": r.StateRoot})
	}
	if g.stage == 2 {
		r, e := groupControlRead("SAA_GROUP_TARGET_INSTALLED_PATH", first.ProposalID)
		if e != nil || r == nil {
			return e
		}
		var p chain.GroupTargetInstallation
		if e = json.Unmarshal(r.Record, &p); e != nil {
			return e
		}
		want, _ := json.Marshal(g.prepared)
		got, _ := json.Marshal(p.Prepared)
		if !bytes.Equal(want, got) || r.Shard != first.TargetShard || r.Block != p.InstallBlock || p.InstallSequence == 0 || !validTransferDigest(p.CommitDigest) {
			return fmt.Errorf("target group installation mismatch")
		}
		for _, m := range p.Prepared.Members {
			g.authority.recordInstallationReceiptMeta(nativeInstallationReceiptMeta{ProposalID: first.ProposalID, Account: "0x" + m.Command.Account, SourceShard: first.SourceShard, TargetShard: first.TargetShard, MappingVersion: first.MappingVersion, AnchorVersion: m.Command.AnchorVersion, StateHash: m.StateHash, Committed: true, Installed: true, SourceStateRoot: m.SourceStateRoot, TargetStateRoot: r.StateRoot, InstallBlock: p.InstallBlock, InstallRound: p.InstallSequence, CommitDigest: p.CommitDigest})
		}
		g.authority.applyDue(1)
		if g.authority.decisionByID[first.ProposalID].Status != mappingApplied {
			return fmt.Errorf("whole group authority gate not satisfied")
		}
		for _, m := range p.Prepared.Members {
			g.activation.Members = append(g.activation.Members, chain.TargetActivation{TargetShard: first.TargetShard, ProposalID: first.ProposalID, Account: m.Command.Account, MappingVersion: g.authority.routeVersion, InstallDigest: p.CommitDigest, StateHash: m.StateHash})
		}
		if e = groupControlWrite("SAA_GROUP_TARGET_ACTIVATION_PATH", g.activation, first.ProposalID); e != nil {
			return e
		}
		g.stage = 3
		g.event("installed", map[string]interface{}{"block": r.Block, "root": r.StateRoot})
	}
	if g.stage == 3 {
		r, e := groupControlRead("SAA_GROUP_TARGET_ACTIVATED_PATH", first.ProposalID)
		if e != nil || r == nil {
			return e
		}
		var a chain.GroupTargetActivation
		if e = json.Unmarshal(r.Record, &a); e != nil {
			return e
		}
		want, _ := json.Marshal(g.activation)
		got, _ := json.Marshal(a)
		if !bytes.Equal(want, got) || r.Shard != first.TargetShard {
			return fmt.Errorf("group activation commit mismatch")
		}
		g.active = true
		if g.serial != nil {
			g.serial.activate(g.spec)
		}
		g.stage = 4
		g.event("route_active", map[string]interface{}{"version": g.authority.routeVersion, "block": r.Block, "root": r.StateRoot})
	}
	if g.stage == 4 {
		r, e := groupControlRead("SAA_GROUP_SOURCE_RETIRED_PATH", first.ProposalID)
		if e != nil || r == nil {
			return e
		}
		var retired chain.GroupSourceRetirement
		if e = json.Unmarshal(r.Record, &retired); e != nil {
			return e
		}
		if r.Shard != first.SourceShard || r.Block != retired.Block || retired.Sequence == 0 || !validTransferDigest(retired.Digest) {
			return fmt.Errorf("group retirement commit mismatch")
		}
		activationRaw, e := os.ReadFile(groupControlPath("SAA_GROUP_TARGET_ACTIVATED_PATH", first.ProposalID))
		if e != nil {
			return e
		}
		if !bytes.Equal(activationRaw, retired.Command.ActivationReceipt) {
			return fmt.Errorf("retirement references different activation")
		}
		var snapshot struct {
			Accounts     []string
			Account      string
			Sequence     uint64
			Transactions [][]byte
		}
		if e = json.Unmarshal(retired.Command.DrainSnapshot, &snapshot); e != nil {
			return e
		}
		if snapshot.Account != "" || snapshot.Sequence == 0 || len(snapshot.Transactions) != 0 || len(snapshot.Accounts) != len(g.spec.Command.Members) {
			return fmt.Errorf("retirement group drain mismatch")
		}
		for i, c := range g.spec.Command.Members {
			if snapshot.Accounts[i] != c.Account {
				return fmt.Errorf("retirement member mismatch")
			}
		}
		g.retired = true
		g.stage = 5
		g.event("retired", map[string]interface{}{"block": r.Block, "root": r.StateRoot, "historical_drain_only": true})
	}
	return nil
}
