package committee

import (
	"blockEmulator/ownership"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"os"
)

func (m *RelayCommitteeModule) configureOwnershipSecurity() error {
	var routes map[string]uint64
	var genesisHash string
	if m.initialLedger != nil {
		routes, genesisHash = m.initialLedger.Routes, m.initialLedger.FileHash
	}
	view, err := ownership.FromEnv(routes, genesisHash, uint64(params.ShardNum))
	if err != nil {
		return err
	}
	if os.Getenv("SAA_LIVE_PLAN_URL") != "" && (view == nil || os.Getenv("SAA_OWNERSHIP_RUNTIME_JOURNAL_PATH") == "") {
		return fmt.Errorf("live placement requires runtime quorum authorization, not an offline owner bundle")
	}
	if view == nil {
		return nil
	}
	dynamic := m.admission != nil && (m.admission.Enabled() || m.admission.dynamicAdmissionEnabled || m.admission.externalActionEnabled || m.admission.mappingBoundaryEnabled || m.admission.softDecisionMode == saaIntegratedDecisionMode)
	if !m.nativeBatch || m.groupControl != nil || m.firstAdmission != nil || m.streamFirstPlacement || dynamic {
		return fmt.Errorf("ownership security supports fixed-ledger native RawRelay only")
	}
	m.ownership = view
	return nil
}

// The constructor calls this only after its final configuration succeeds.
// A producer placement guard is not a chain execution guard.
func (m *RelayCommitteeModule) logOwnershipStartup() {
	if m.sl != nil {
		enabled, scope := m.ownership != nil, "disabled"
		if enabled {
			scope = "producer_placement_guard"
		}
		raw, err := json.Marshal(struct {
			Role               string                    `json:"role"`
			PID                int                       `json:"pid"`
			EnforcementEnabled bool                      `json:"enforcement_enabled"`
			GuardScope         string                    `json:"guard_scope"`
			Status             ownership.BootstrapStatus `json:"status"`
		}{"supervisor", os.Getpid(), enabled, scope, m.ownership.Status()})
		if err != nil {
			panic(err)
		}
		m.sl.Slog.Printf("OWNERSHIP_SECURITY_STATUS %s", raw)
	}
}

func (m *RelayCommitteeModule) checkAuthorizedPlacement(sender, recipient string, source, target uint64) {
	if m.ownership == nil {
		return
	}
	if err := m.ownership.Check(sender, source); err != nil {
		panic(fmt.Sprintf("unauthorized sender placement: %v", err))
	}
	if err := m.ownership.Check(recipient, target); err != nil {
		panic(fmt.Sprintf("unauthorized recipient placement: %v", err))
	}
}
