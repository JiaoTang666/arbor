package committee

import (
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type deferredGroupControl struct {
	routes                     map[string]uint64
	shards                     uint64
	requestPath, selectionPath string
	deadline                   time.Duration
	request                    *core.QueueObservationRequest
	done                       bool
}
type deferredGroupSelection struct {
	Schema    string          `json:"schema"`
	RequestID string          `json:"request_id"`
	Decision  string          `json:"decision"`
	Reason    string          `json:"reason"`
	Spec      nativeGroupSpec `json:"spec"`
}

func (m *RelayCommitteeModule) configureDeferredGroupControl() error {
	if !m.nativeBatch || m.initialLedger == nil || m.admission == nil {
		return fmt.Errorf("deferred group requires native batch, ledger and shard configuration")
	}
	if os.Getenv("SAA_NATIVE_GROUP_CONTROL_SPEC") != "" {
		return fmt.Errorf("fixed and deferred group controls are mutually exclusive")
	}
	arm := os.Getenv("SAA_DEFERRED_GROUP_ARM")
	if arm != "keep" && arm != "move" {
		return fmt.Errorf("deferred group arm invalid")
	}
	after, err := strconv.ParseInt(os.Getenv("SAA_DEFERRED_GROUP_AFTER_OCCURRENCES"), 10, 64)
	if err != nil || after < 1 {
		return fmt.Errorf("deferred prefix invalid")
	}
	seconds, err := strconv.ParseInt(os.Getenv("SAA_DEFERRED_GROUP_DEADLINE_SECONDS"), 10, 64)
	if err != nil || seconds < 1 || seconds > 3600 {
		return fmt.Errorf("deferred selection deadline invalid")
	}
	for _, key := range []string{"SAA_DEFERRED_GROUP_REQUEST_PATH", "SAA_DEFERRED_GROUP_SELECTION_PATH", "SAA_DEFERRED_GROUP_SNAPSHOT_DIR", "SAA_GROUP_SOURCE_PREPARE_PATH", "SAA_GROUP_SOURCE_PREPARED_PATH", "SAA_GROUP_TARGET_INSTALL_PATH", "SAA_GROUP_TARGET_INSTALLED_PATH", "SAA_GROUP_TARGET_ACTIVATION_PATH", "SAA_GROUP_TARGET_ACTIVATED_PATH", "SAA_GROUP_SOURCE_RETIRED_PATH"} {
		if os.Getenv(key) == "" {
			return fmt.Errorf("deferred group missing %s", key)
		}
	}
	// Pointer identity is fixed before the producer/control goroutines start.
	m.groupControl = &nativeGroupControl{spec: nativeGroupSpec{Arm: arm, AfterOccurrences: after}, stage: -1, audit: m.nativeAudit,
		deferred: &deferredGroupControl{routes: m.initialLedger.Routes, shards: m.admission.shardNum, requestPath: os.Getenv("SAA_DEFERRED_GROUP_REQUEST_PATH"), selectionPath: os.Getenv("SAA_DEFERRED_GROUP_SELECTION_PATH"), deadline: time.Duration(seconds) * time.Second}}
	return nil
}

func validateDeferredGroupSpec(spec nativeGroupSpec, arm string, after int64, routes map[string]uint64, shards uint64) (map[string]uint64, error) {
	if spec.Arm != arm || spec.AfterOccurrences != after || len(spec.Command.Members) < core.MinMigrationGroupMembers || len(spec.Command.Members) > core.MaxMigrationGroupMembers {
		return nil, fmt.Errorf("deferred group arm/prefix/size mismatch")
	}
	first := spec.Command.Members[0]
	if first.SourceShard == first.TargetShard || first.SourceShard >= uint64(shards) || first.TargetShard >= uint64(shards) {
		return nil, fmt.Errorf("deferred group shards invalid")
	}
	initial := map[string]uint64{}
	for i, c := range spec.Command.Members {
		decoded, err := hex.DecodeString(c.Account)
		if err != nil || len(decoded) != 20 || strings.ToLower(c.Account) != c.Account || (i > 0 && spec.Command.Members[i-1].Account >= c.Account) || c.ProposalID == "" || c.ProposalID != first.ProposalID || c.SourceShard != first.SourceShard || c.TargetShard != first.TargetShard || c.MappingVersion != 0 || c.AnchorVersion != first.AnchorVersion || !c.BindAtProposal || c.ExpectedBlockHash != "" || c.ExpectedStateRoot != "" || c.ExpectedStateHash != "" {
			return nil, fmt.Errorf("deferred group intent invalid")
		}
		shard, ok := routes[c.Account]
		if !ok || shard != c.SourceShard {
			return nil, fmt.Errorf("deferred member not in source initial ledger")
		}
		initial["0x"+c.Account] = shard
	}
	return initial, nil
}

// Called only by the existing control goroutine, with g.mu held. No waiting
// for external files: missing/late/invalid results select KEEP for this round.
func (g *nativeGroupControl) pollDeferredGroup(now time.Time) (bool, error) {
	d := g.deferred
	if d.done {
		return false, nil
	}
	count := atomic.LoadInt64(&g.inputCount)
	if count < g.spec.AfterOccurrences {
		return false, nil
	}
	finish := func(reason string) (bool, error) {
		d.done = true
		g.stage = 99
		g.retired = true
		g.event("deferred_keep", map[string]interface{}{"reason": reason, "observed_occurrences": count})
		return false, nil
	}
	if d.request == nil {
		r := &core.QueueObservationRequest{Schema: "saa_queue_request_v1", RequestID: fmt.Sprintf("queue-%d-%d", g.spec.AfterOccurrences, now.UnixNano()), RequestedAt: now.UTC(), ExpiresAt: now.Add(d.deadline).UTC(), AfterOccurrences: g.spec.AfterOccurrences, ObservedOccurrences: count}
		raw, err := json.Marshal(r)
		if err != nil {
			return finish("request_marshal_failed")
		}
		if err = core.PublishSourcePrepareFile(d.requestPath, append(raw, '\n')); err != nil {
			return finish("request_publish_failed: " + err.Error())
		}
		d.request = r
		g.event("queue_requested", r)
	}
	if !now.Before(d.request.ExpiresAt) {
		return finish("selection_deadline_expired; input was not paused")
	}
	raw, err := os.ReadFile(d.selectionPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return finish("selection_read_failed: " + err.Error())
	}
	var selection deferredGroupSelection
	if err = json.Unmarshal(raw, &selection); err != nil {
		return finish("selection_invalid_json")
	}
	if selection.Schema != "saa_queue_selection_v1" || selection.RequestID != d.request.RequestID {
		return finish("selection_request_mismatch")
	}
	if selection.Decision == "KEEP" {
		g.event("selection", selection)
		return finish("selector_keep: " + selection.Reason)
	}
	if selection.Decision != "MOVE" {
		return finish("selection_action_invalid")
	}
	initial, err := validateDeferredGroupSpec(selection.Spec, g.spec.Arm, g.spec.AfterOccurrences, d.routes, d.shards)
	if err != nil {
		return finish("selection_contract_failed: " + err.Error())
	}
	g.spec = selection.Spec
	g.authority = newMappingAuthority(d.shards, initial)
	g.authority.enableInstallationReceiptGate()
	g.stage = 0
	g.event("selection", selection)
	g.deferred = nil
	return true, nil
}
