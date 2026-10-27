package committee

import "fmt"

// CanFinalizeMigration exposes the source-side safety check to the native
// finalization adapter. It never mutates routing or state.
func (m *RuntimeUnknownAdmissionManager) CanFinalizeMigration(proposalID, account, stateHash string) error {
	if m == nil {
		return fmt.Errorf("admission manager is nil")
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	s := m.integratedStateLocked()
	if s == nil || s.authority == nil || !s.authority.canFinalizeMigration(proposalID, account, stateHash) {
		return fmt.Errorf("committed installation receipt missing or mismatched")
	}
	return nil
}
