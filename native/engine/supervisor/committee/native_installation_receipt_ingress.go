package committee

import (
	"fmt"
	"strings"
)

// RecordNativeInstallationReceipt is the runtime ingress used by the native
// CLPA/PBFT path after destination state has been committed. It deliberately
// records only the proposal identity here; state/hash validation belongs to
// the native receiver before this boundary.
func (m *RuntimeUnknownAdmissionManager) RecordNativeInstallationReceipt(proposalID string) error {
	if m == nil || strings.TrimSpace(proposalID) == "" {
		return fmt.Errorf("proposal id is required")
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	state := m.integratedStateLocked()
	if state == nil || state.authority == nil {
		return fmt.Errorf("integrated authority is unavailable")
	}
	state.authority.recordInstallationReceipt(proposalID)
	return nil
}
