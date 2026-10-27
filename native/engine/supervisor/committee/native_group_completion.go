package committee

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// The lifecycle stream is the authoritative end-to-end measurement for group
// runs. Legacy per-block TPS summaries omit these special execution branches.
func (m *RelayCommitteeModule) pollGroupCompletions() error {
	g := m.groupControl
	if g == nil {
		return nil
	}
	g.mu.Lock()
	spec := g.spec
	ready := g.deferred == nil && len(spec.Command.Members) > 0 && spec.Arm == "move"
	specs := map[string]nativeGroupSpec{}
	if ready {
		specs[spec.Command.Members[0].ProposalID] = spec
	}
	if g.serial != nil {
		for id, s := range g.serial.history {
			specs[id] = s
		}
		ready = len(specs) > 0
	}
	g.mu.Unlock()
	if !ready {
		return nil
	}
	// Called only by the supervisor-owned control goroutine.
	if time.Since(g.lastCompletionScan) < time.Second {
		return nil
	}
	g.lastCompletionScan = time.Now()
	dir, ackDir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR"), os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR")
	if dir == "" || ackDir == "" {
		return fmt.Errorf("group completion requires outbox and ACK directories")
	}
	files, e := core.LoadMigrationArtifacts(dir, ".handoff.json", false)
	if e != nil {
		return e
	}
	if g.completedHandoffs == nil {
		g.completedHandoffs = map[string]bool{}
	}
	acks, e := core.LoadMigrationArtifacts(ackDir, ".ack.json", false)
	if e != nil {
		return e
	}
	relays, e := core.LoadMigrationArtifacts(ackDir, ".relay-ack.json", false)
	if e != nil {
		return e
	}
	for _, id := range files.IDs() {
		if g.completedHandoffs[id] {
			continue
		}
		raw := files[id]
		var h core.MigrationHandoff
		if e = json.Unmarshal(raw, &h); e != nil {
			return e
		}
		spec, known := specs[h.ProposalID]
		if !known {
			continue
		}
		if h.ID != id || len(h.GroupAccounts) != len(spec.Command.Members) {
			return fmt.Errorf("group completion identity mismatch")
		}
		for i, c := range spec.Command.Members {
			if h.GroupAccounts[i] != c.Account || h.SourceShard != c.SourceShard || h.TargetShard != c.TargetShard || h.MappingVersion != c.MappingVersion+1 {
				return fmt.Errorf("group completion member mismatch")
			}
		}
		if e = core.ValidateMigrationHandoff(&h); e != nil {
			return e
		}
		raw, e = acks.Read(h.ID)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		var ack core.HandoffAck
		if e = json.Unmarshal(raw, &ack); e != nil {
			return e
		}
		if e = core.ValidateHandoffAck(&h, ack); e != nil {
			return e
		}
		var final *core.HandoffRelayAck
		if ack.RelayPending {
			raw, e = relays.Read(h.ID)
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return e
			}
			var a core.HandoffRelayAck
			if e = json.Unmarshal(raw, &a); e != nil {
				return e
			}
			if e = core.ValidateHandoffRelayAck(&h, a); e != nil {
				return e
			}
			final = &a
		}
		done, e := m.registerGroupCompletion(&h, ack, final)
		if e != nil {
			return e
		}
		if done {
			g.completedHandoffs[h.ID] = true
		}
	}
	return nil
}

func (m *RelayCommitteeModule) registerGroupCompletion(h *core.MigrationHandoff, ack core.HandoffAck, final *core.HandoffRelayAck) (bool, error) {
	if e := core.ValidateHandoffAck(h, ack); e != nil {
		return false, e
	}
	if ack.RelayPending {
		if final == nil {
			return false, nil
		}
		if e := core.ValidateHandoffRelayAck(h, *final); e != nil {
			return false, e
		}
	}
	tx := core.DecodeTx(h.Original)
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	x := m.lifecycle[occurrenceKey(tx)]
	if x == nil || !x.Routed || x.Tx != snapshotOrdinaryRoute(tx) {
		return false, fmt.Errorf("group completion does not match original accepted route")
	}
	if x.Done {
		return true, nil
	}
	// A late old-route credit still needs the ordinary source commit report.
	// Wait for it before updating the legacy drain counter (which clamps at 0).
	oldCross := x.Tx.RoutedSenderShard != x.Tx.RoutedRecipientShard
	if tx.Relayed && oldCross && !x.Source {
		return false, nil
	}
	x.Source = true
	x.Target = true
	x.Done = true
	m.lifecycleEvent(tx, "group_execution_registered", time.Now(), map[string]interface{}{"handoff": h, "ack": ack, "relay_ack": final, "trusted_reporters": true})
	m.lifecycleEvent(tx, "completed", time.Now(), "all group handoff execution legs confirmed; end-to-end observation includes migration waiting")
	if tx.Relayed && oldCross && m.Ss != nil {
		m.Ss.ObserveSecondLegProgress(0, 1)
	}
	return true, nil
}

// This is a shutdown condition, never an input/admission throttle. Only used
// after the finite workload has been sent; observers keep running while waiting.
func (m *RelayCommitteeModule) GroupCompletionReady() bool {
	g := m.groupControl
	if g == nil {
		return true
	}
	g.mu.Lock()
	ready := g.spec.Arm == "keep" || g.retired
	if g.deferred != nil {
		ready = g.deferred.done
	}
	if g.serial != nil {
		ready = g.serial.done
	}
	g.mu.Unlock()
	if !ready {
		return false
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	for _, x := range m.lifecycle {
		if !x.Done {
			return false
		}
	}
	return true
}
