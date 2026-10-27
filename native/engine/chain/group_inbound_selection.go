package chain

import "blockEmulator/core"

// A group inbound debit is owned by the unchanged source sender. Its recipient
// alone supplies migration identity; a migrated sender must use group handoff.
func (bc *BlockChain) inboundPreparation(tx *core.Transaction) *core.SourcePreparation {
	if tx == nil {
		return nil
	}
	groups := bc.GroupSourcePreparations()
	for _, group := range groups {
		for _, m := range group.Members {
			if normalizePartitionMapKey(tx.Sender) == m.Command.Account {
				return nil
			}
		}
	}
	for _, group := range groups {
		for _, m := range group.Members {
			if normalizePartitionMapKey(tx.Recipient) == m.Command.Account {
				copy := m
				return &copy
			}
		}
	}
	if len(groups) > 0 {
		return nil
	}
	return bc.SourcePreparation()
}
