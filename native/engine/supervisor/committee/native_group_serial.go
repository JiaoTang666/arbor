package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/params"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

type serialGroupControl struct {
	directory         string
	rounds, next      int
	continuous        bool
	loaded, done      bool
	version           uint64
	routes            map[string]uint64 // Only actually activated destinations.
	history           map[string]nativeGroupSpec
	used              map[string]bool
	populationApplied map[string]bool
	initial           map[string]uint64
	dynamic           map[string]uint64 // Native first-placement bindings, protected by g.mu.
	shards            uint64
	lastCutoff        int64
	selection         serialGroupSelection
	selectionHash     string
}
type serialGroupSelection struct {
	Schema              string          `json:"schema"`
	Round               int             `json:"round"`
	BaseVersion         uint64          `json:"base_version"`
	ObservedOccurrences int64           `json:"observed_occurrences"`
	Decision            string          `json:"decision"`
	Reason              string          `json:"reason"`
	Spec                nativeGroupSpec `json:"spec"`
	ExpiresAt           time.Time       `json:"expires_at"`
}

// Immutable controller disposition: a skipped MOVE is not a completed migration.
func (s *serialGroupControl) outcome(p serialGroupSelection, hash, decision, reason string) error {
	value := map[string]interface{}{"schema": "saa_serial_outcome_v1", "round": p.Round, "base_version": p.BaseVersion, "mapping_version": s.version, "observed_occurrences": p.ObservedOccurrences, "selection_sha256": hash, "requested": p.Decision, "decision": decision, "reason": reason}
	raw, e := json.Marshal(value)
	if e != nil {
		return e
	}
	return core.PublishSourcePrepareFile(filepath.Join(s.directory, fmt.Sprintf("outcome-%03d.json", p.Round)), raw)
}
func groupControlPath(key string, id ...string) string {
	path := os.Getenv(key)
	if len(id) > 0 {
		return core.GroupTransportPath(path, id[0])
	}
	return path
}
func (m *RelayCommitteeModule) configureSerialGroupControl() error {
	if !m.nativeBatch || m.initialLedger == nil || m.admission == nil {
		return fmt.Errorf("serial group requires native batch, ledger and shard configuration")
	}
	if os.Getenv("SAA_DEFERRED_GROUP_CONTROL") == "true" || os.Getenv("SAA_NATIVE_GROUP_CONTROL_SPEC") != "" {
		return fmt.Errorf("serial and legacy group controllers are mutually exclusive")
	}
	rounds, e := strconv.Atoi(os.Getenv("SAA_SERIAL_GROUP_ROUNDS"))
	continuous := os.Getenv("SAA_FOREST_CONTINUOUS") == "true"
	if e != nil || (!continuous && (rounds < 1 || rounds > core.MaxSerialMigrationGroups)) || (continuous && rounds != 0) {
		return fmt.Errorf("invalid serial round count")
	}
	dir := os.Getenv("SAA_SERIAL_GROUP_PLAN_DIR")
	info, e := os.Stat(dir)
	if e != nil || !info.IsDir() {
		return fmt.Errorf("serial plan directory missing")
	}
	for _, key := range []string{"SAA_GROUP_SOURCE_PREPARE_PATH", "SAA_GROUP_SOURCE_PREPARED_PATH", "SAA_GROUP_TARGET_INSTALL_PATH", "SAA_GROUP_TARGET_INSTALLED_PATH", "SAA_GROUP_TARGET_ACTIVATION_PATH", "SAA_GROUP_TARGET_ACTIVATED_PATH", "SAA_GROUP_SOURCE_RETIRED_PATH"} {
		if os.Getenv(key) == "" {
			return fmt.Errorf("serial transport missing %s", key)
		}
	}
	m.groupControl = &nativeGroupControl{stage: -1, audit: m.nativeAudit, serial: &serialGroupControl{directory: dir, rounds: rounds, continuous: continuous, next: 1, routes: map[string]uint64{}, history: map[string]nativeGroupSpec{}, used: map[string]bool{}, initial: m.initialLedger.Routes, dynamic: m.nativeRoutes, shards: m.admission.shardNum}}
	return nil
}

// A native first-placement binding is an owner claim, not proof that account
// state exists. The source node must still bind the command to its actual
// committed trie before proposing a migration. Unknown accounts are rejected.
func (s *serialGroupControl) originalRoute(account string) (uint64, bool) {
	if shard, ok := s.initial[account]; ok {
		return shard, true
	}
	shard, ok := s.dynamic[account]
	return shard, ok
}

func (s *serialGroupControl) memberRoutes(spec nativeGroupSpec) map[string]uint64 {
	routes := make(map[string]uint64, len(spec.Command.Members))
	for _, c := range spec.Command.Members {
		if shard, ok := s.routes[c.Account]; ok {
			routes[c.Account] = shard
		} else if shard, ok := s.originalRoute(c.Account); ok {
			routes[c.Account] = shard
		}
	}
	return routes
}
func (s *serialGroupControl) activate(spec nativeGroupSpec) {
	for _, c := range spec.Command.Members {
		s.routes[c.Account] = c.TargetShard
	}
	s.version = spec.Command.Members[0].MappingVersion + 1
}

// g.mu protects serial history, current state and the committed route overlay.
// Missing plans never pause workload admission or fabricate an update.
func (g *nativeGroupControl) pollSerialGroup() (bool, error) {
	s := g.serial
	if s.done {
		return false, nil
	}
	if s.loaded {
		if !g.retired {
			return true, nil
		}
		if e := s.outcome(s.selection, s.selectionHash, "MOVE", "retired"); e != nil {
			return false, e
		}
		s.next++
		s.loaded = false
	}
	if !s.continuous && s.next > s.rounds {
		s.done = true
		g.event("serial_complete", map[string]interface{}{"rounds": s.rounds, "version": s.version})
		return false, nil
	}
	raw, e := os.ReadFile(filepath.Join(s.directory, fmt.Sprintf("round-%03d.json", s.next)))
	if os.IsNotExist(e) {
		if s.continuous {
			return false, g.closeForestStream()
		}
		return false, nil
	}
	if e != nil {
		return false, e
	}
	var p serialGroupSelection
	if e = json.Unmarshal(raw, &p); e != nil {
		return false, e
	}
	// A continuous layout revision may require several versioned batches from
	// the same observed prefix. Never fabricate increments between those batches;
	// round, base version, expiry and unique proposal still bind every command.
	terminalKeep := p.Decision == "KEEP" && p.ObservedOccurrences == int64(params.TotalDataSize) && params.TotalDataSize > 0
	if p.Schema != "saa_serial_selection_v2" || p.Round != s.next || p.BaseVersion != s.version || p.ObservedOccurrences <= 0 || p.ObservedOccurrences < s.lastCutoff || (p.ObservedOccurrences == s.lastCutoff && !terminalKeep && !s.continuous) {
		return false, fmt.Errorf("serial selection generation/cutoff mismatch")
	}
	accepted := atomic.LoadInt64(&g.inputCount)
	if params.TotalDataSize > 0 && p.ObservedOccurrences > int64(params.TotalDataSize) {
		return false, fmt.Errorf("serial prefix exceeds input")
	}
	if accepted < p.ObservedOccurrences {
		return false, nil
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	if p.Decision == "KEEP" || (s.continuous && p.Decision == "DEFER") {
		if p.Reason == "" {
			return false, fmt.Errorf("serial KEEP needs reason")
		}
		if e := s.outcome(p, hash, p.Decision, p.Reason); e != nil {
			return false, e
		}
		s.lastCutoff = p.ObservedOccurrences
		s.next++
		g.event("serial_keep", p)
		return false, nil
	}
	if p.Decision != "MOVE" {
		return false, fmt.Errorf("invalid serial decision")
	}
	spec := p.Spec
	if spec.AfterOccurrences != p.ObservedOccurrences {
		return false, fmt.Errorf("serial prefix mismatch")
	}
	// Reuse legacy canonical/member/owner validation without mutating input.
	normalized := spec
	normalized.Command.Members = append([]core.SourcePrepareCommand(nil), spec.Command.Members...)
	for i, c := range spec.Command.Members {
		if c.MappingVersion != s.version || s.used[c.Account] {
			return false, fmt.Errorf("serial stale version or remigration")
		}
		normalized.Command.Members[i].MappingVersion = 0
	}
	initial, e := validateDeferredGroupSpec(normalized, "move", p.ObservedOccurrences, s.memberRoutes(spec), s.shards)
	if e != nil {
		return false, e
	}
	if p.ExpiresAt.IsZero() {
		return false, fmt.Errorf("serial MOVE needs explicit evidence expiry")
	}
	reason := ""
	if !time.Now().Before(p.ExpiresAt) {
		reason = "evidence_expired"
	}
	if params.TotalDataSize > 0 && accepted >= int64(params.TotalDataSize) {
		reason = "input_fully_admitted"
	}
	if reason != "" {
		disposition := "KEEP"
		if s.continuous {
			disposition = "DEFER"
		}
		if e := s.outcome(p, hash, disposition, reason); e != nil {
			return false, e
		}
		s.lastCutoff = p.ObservedOccurrences
		s.next++
		g.event("serial_skip", map[string]interface{}{"selection": p, "reason": reason})
		return false, nil
	}
	id := spec.Command.Members[0].ProposalID
	if _, exists := s.history[id]; exists {
		return false, fmt.Errorf("serial duplicate proposal")
	}
	a := newMappingAuthority(s.shards, initial)
	a.routeVersion = s.version
	a.enableInstallationReceiptGate()
	g.spec = spec
	g.authority = a
	g.stage = 0
	g.active = false
	g.retired = false
	g.prepared = nil
	g.activation = chain.GroupTargetActivation{}
	s.loaded = true
	s.lastCutoff = p.ObservedOccurrences
	s.history[id] = spec
	s.selection = p
	s.selectionHash = hash
	for _, c := range spec.Command.Members {
		s.used[c.Account] = true
	}
	g.event("serial_selection", p)
	return true, nil
}

// Command closure follows every published round's terminal outcome. It is
// independent of the observer's final drain of the persistent audit stream.
// pollSerialGroup checks loaded migrations and the next round before reaching
// here, so an EOF-racing published round still receives its normal disposition.
func (g *nativeGroupControl) closeForestStream() error {
	s := g.serial
	raw, e := os.ReadFile(filepath.Join(s.directory, "commands-closed.json"))
	if e == nil {
		return g.closeForestCommands(raw)
	}
	if !os.IsNotExist(e) {
		return e
	}
	// Compatibility with workers that only seal after final observation.
	// An invalid command seal above must never fall back to this older marker.
	raw, e = os.ReadFile(filepath.Join(s.directory, "closed.json"))
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	var c struct {
		Schema    string `json:"schema"`
		LastRound int    `json:"last_round"`
		Observed  int64  `json:"observed_occurrences"`
		Version   uint64 `json:"mapping_version"`
	}
	if e = json.Unmarshal(raw, &c); e != nil {
		return e
	}
	if c.Schema != "forest_stream_closed_v1" || c.LastRound != s.next-1 || c.Observed != int64(params.TotalDataSize) || c.Observed < s.lastCutoff || c.Version != s.version {
		return fmt.Errorf("forest close watermark mismatch")
	}
	if atomic.LoadInt64(&g.inputCount) < c.Observed {
		return nil
	}
	s.done = true
	g.event("serial_complete", map[string]interface{}{"rounds": c.LastRound, "version": s.version, "continuous": true})
	return nil
}

func (g *nativeGroupControl) closeForestCommands(raw []byte) error {
	s := g.serial
	var c struct {
		Schema    string `json:"schema"`
		LastRound int    `json:"last_round"`
		Observed  int64  `json:"observed_occurrences"`
		Accepted  int64  `json:"accepted_occurrences"`
		Total     int64  `json:"total_occurrences"`
		Version   uint64 `json:"mapping_version"`
		InputHash string `json:"input_sha256"`
		Closed    bool   `json:"new_commands_closed"`
	}
	if e := json.Unmarshal(raw, &c); e != nil {
		return e
	}
	total := int64(params.TotalDataSize)
	if c.Schema != "forest_commands_closed_v1" || !c.Closed || total <= 0 || c.Accepted != total || c.Total != total || c.LastRound != s.next-1 || c.Version != s.version || c.Observed < s.lastCutoff || c.Observed > c.Accepted || !validTransferDigest(c.InputHash) || c.InputHash != os.Getenv("SAA_FOREST_INPUT_SHA256") {
		return fmt.Errorf("forest command close watermark mismatch")
	}
	accepted := atomic.LoadInt64(&g.inputCount)
	if accepted > total {
		return fmt.Errorf("forest command close exceeds finite input")
	}
	if accepted < total || s.loaded || (g.spec.Arm == "move" && !g.retired) {
		return nil
	}
	s.done = true
	g.event("serial_complete", map[string]interface{}{"rounds": c.LastRound, "version": s.version, "continuous": true, "commands_closed": true, "observed_occurrences": c.Observed, "accepted_occurrences": c.Accepted})
	return nil
}
