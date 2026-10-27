package core

// Same source-hold eligibility as packing, without removing transactions.
func (p *TxPool) HasOrdinaryPending() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lanes != nil {
		for _, e := range p.lanes.ordinary {
			if !p.heldForMigration(e.tx) {
				return true
			}
		}
		for _, e := range p.lanes.related {
			if !p.heldForMigration(e.tx) {
				return true
			}
		}
		return false
	}
	for _, tx := range p.TxQueue {
		if !p.heldForMigration(tx) {
			return true
		}
	}
	return false
}
