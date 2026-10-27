package committee

// Poll only at a real globally observed epoch. This does not invent locality
// observations or advance time while waiting for a migration receipt.
func (m *RuntimeUnknownAdmissionManager) pollNativeControlAtObservedEpoch(epoch int) {
	if m == nil || epoch < 0 {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.softDecisionMode != saaIntegratedDecisionMode {
		return
	}
	if !m.integratedStateLocked().authority.requireInstallationReceipt {
		return
	}
	m.applySAAIntegratedDueLocked(epoch)
}
