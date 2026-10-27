package signal

import "fmt"

// Caller must prove exclusive finite ingress is exhausted, every registered
// occurrence is terminal, all creation leases are released, and these shards
// are absent from BOTH frozen route endpoints. TCP readiness alone is insufficient.
// No fabricated block, seen flag or empty streak is inserted. All quiet-period
// thresholds and second-leg checks remain unchanged.
func (ss *StopSignal) ConfirmUntouchedAfterDrainedInput(shards []uint64) error {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	if ss.requiredShards <= 0 || ss.pendingSecondLegTx != 0 {
		return fmt.Errorf("undrained_stop_signal")
	}
	seen := map[uint64]bool{}
	for _, s := range shards {
		if s >= uint64(ss.requiredShards) || seen[s] {
			return fmt.Errorf("invalid_untouched_set")
		}
		seen[s] = true
	}
	if ss.untouchedAfterDrain == nil {
		ss.untouchedAfterDrain = map[uint64]bool{}
	}
	for _, s := range shards {
		if !ss.shardSeen[s] {
			ss.untouchedAfterDrain[s] = true
		}
	}
	return nil
}
func (ss *StopSignal) allShardsCoveredLocked() bool {
	for i := 0; i < ss.requiredShards; i++ {
		s := uint64(i)
		if !ss.shardSeen[s] && !ss.untouchedAfterDrain[s] {
			return false
		}
	}
	return true
}
