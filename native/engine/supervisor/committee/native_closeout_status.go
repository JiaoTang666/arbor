package committee

import (
	"fmt"
	"sort"
)

// Until a source-finalization receipt exists, applied routing is deliberately
// not accepted as completed migration. This guard must not synthesize success.
func (r *RelayCommitteeModule) NativeCloseoutError() error {
	if r.groupControl != nil {
		g := r.groupControl
		g.mu.Lock()
		move, retired := g.spec.Arm == "move", g.retired
		if g.serial != nil {
			move = true
			retired = g.serial.done
		}
		g.mu.Unlock()
		if move && !retired {
			return fmt.Errorf("group migration has no validated retirement")
		}
		if move {
			r.lifecycleMu.Lock()
			pending := 0
			for _, tx := range r.lifecycle {
				if !tx.Done {
					pending++
				}
			}
			r.lifecycleMu.Unlock()
			if pending > 0 {
				return fmt.Errorf("group run has %d uncompleted occurrences", pending)
			}
		}
	}
	if r.nativeAudit != nil {
		r.nativeAudit.flush()
		r.nativeAudit.write(r.nativeAudit.writerStats())
		if r.forestFacts != nil {
			a, e := r.forestFacts.CompleteAudit(uint64(r.dataTotalNum))
			if e != nil {
				return e
			}
			r.nativeAudit.write(map[string]interface{}{"event": "forest_facts_v2_final", "audit": a})
		}
		r.nativeAudit.flush()
	}
	if err := r.ownership.CloseRuntimeDiagnostics(); err != nil {
		return fmt.Errorf("ownership diagnostic close failed: %w", err)
	}
	m := r.admission
	if m == nil {
		return nil
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.softDecisionMode != saaIntegratedDecisionMode {
		return nil
	}
	a := m.integratedStateLocked().authority
	if !a.requireInstallationReceipt {
		return nil
	}
	var unresolved []string
	for id, decision := range a.decisionByID {
		if decision.Status == mappingAccepted {
			unresolved = append(unresolved, id+":accepted_pending")
		}
	}
	for id, accounts := range a.preparedTransfers {
		for account := range accounts {
			if err := m.validateNativeRetirementLocked(id, account); err != nil {
				unresolved = append(unresolved, id+":source_finalization_unproven:"+err.Error())
			} else {
				// A historical retirement receipt does not certify that later
				// arrivals and their second legs have drained at shutdown.
				unresolved = append(unresolved, id+":post_retirement_drain_unproven")
			}
		}
	}
	if len(unresolved) == 0 {
		return nil
	}
	sort.Strings(unresolved)
	return fmt.Errorf("native migration incomplete: %v", unresolved)
}
