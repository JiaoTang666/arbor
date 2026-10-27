package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"fmt"
)

// A committed local credit is the only reason to discard a Relay second leg.
// The read is advisory admission filtering; ledger execution still checks the
// same effect because another commit can race with this queue admission.
func (n *PbftConsensusNode) filterCommittedRelayCredits(txs []*core.Transaction) ([]*core.Transaction, error) {
	enabled, err := chain.NativeEffectIndexConfiguration()
	if err != nil {
		return nil, err
	}
	if !enabled {
		// No ledger read is needed in this mode. Do not make queue admission
		// wait for unrelated block construction or commit behind pbftLock.
		// Transport authorization and occurrence checks still run separately.
		return txs, nil
	}
	n.pbftLock.Lock()
	statuses, err := n.CurChain.TransactionEffectStatuses(txs, core.TransactionCredit)
	n.pbftLock.Unlock()
	if err != nil {
		return nil, err
	}
	i := 0
	return filterRelayCredits(txs, func(tx *core.Transaction) (chain.TransactionEffectStatus, error) {
		status := statuses[i]
		i++
		return status, nil
	})
}

func filterRelayCredits(txs []*core.Transaction, lookup func(*core.Transaction) (chain.TransactionEffectStatus, error)) ([]*core.Transaction, error) {
	out := make([]*core.Transaction, 0, len(txs))
	for i, tx := range txs {
		status, err := lookup(tx)
		if err != nil {
			return nil, fmt.Errorf("relay credit %d lookup: %w", i, err)
		}
		if status != chain.EffectIndexDisabled && (tx == nil || !tx.Relayed) {
			return nil, fmt.Errorf("relay credit %d lacks second-leg execution identity", i)
		}
		switch status {
		case chain.EffectIndexDisabled, chain.EffectUnseen:
			out = append(out, tx)
		case chain.EffectSeen:
			// Its credit is already in this committed ledger, not merely sent.
		default:
			return nil, fmt.Errorf("relay credit %d has unknown effect coverage", i)
		}
	}
	return out, nil
}
