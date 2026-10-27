package core

import "strings"

func canonicalPrepareAccount(account string) string {
	return strings.TrimPrefix(strings.ToLower(strings.TrimSpace(account)), "0x")
}

func TransactionTouchesAccount(tx *Transaction, account string) bool {
	if tx == nil {
		return false
	}
	account = canonicalPrepareAccount(account)
	if account == "" {
		return false
	}
	// RawRelay's second leg credits only its recipient; it never writes sender.
	if tx.Relayed && !tx.HasBroker && tx.OriginalSender == "" && tx.FinalRecipient == "" {
		return canonicalPrepareAccount(tx.Recipient) == account
	}
	for _, endpoint := range []string{tx.Sender, tx.Recipient, tx.OriginalSender, tx.FinalRecipient} {
		if canonicalPrepareAccount(endpoint) == account {
			return true
		}
	}
	return false
}

// A new frozen route can debit an ordinary sender on this source and relay
// credit to the migrated target. Earlier source-local routes still need the
// migration handoff. Migrated senders and relay credits remain held.
func holdMigrationTransaction(tx *Transaction, account string) bool {
	if !TransactionTouchesAccount(tx, account) {
		return false
	}
	key := canonicalPrepareAccount(account)
	if tx != nil && tx.RoutingFrozen && !tx.Relayed && !tx.HasBroker &&
		canonicalPrepareAccount(tx.Recipient) == key &&
		canonicalPrepareAccount(tx.Sender) != key &&
		tx.RoutedRecipientShard != tx.RoutedSenderShard {
		return false
	}
	return true
}

func (p *TxPool) heldForMigration(tx *Transaction) bool {
	if tx == nil {
		return false
	}
	prepared := canonicalPrepareAccount(p.sourcePreparedAccount)
	held := func(account string) bool {
		if account == "" {
			return false
		}
		if account == prepared {
			return true
		}
		_, exists := p.heldMigrationAccounts[account]
		return exists
	}
	recipient := canonicalPrepareAccount(tx.Recipient)
	// RawRelay's second leg writes only its recipient, even when its sender
	// belongs to the migrating group. Keep the legacy Broker metadata rule.
	if tx.Relayed && !tx.HasBroker && tx.OriginalSender == "" && tx.FinalRecipient == "" {
		return held(recipient)
	}
	sender := canonicalPrepareAccount(tx.Sender)
	inboundDebit := tx.RoutingFrozen && !tx.Relayed && !tx.HasBroker &&
		sender != recipient && tx.RoutedRecipientShard != tx.RoutedSenderShard
	// Membership is canonical at publication. Check at most four actual
	// endpoints instead of scanning every held account for every queued tx.
	for _, account := range [...]string{sender, recipient,
		canonicalPrepareAccount(tx.OriginalSender), canonicalPrepareAccount(tx.FinalRecipient)} {
		if inboundDebit && account == recipient {
			continue
		}
		if held(account) {
			return true
		}
	}
	return false
}

// HoldSourceAccount is invoked only after source preparation commits. Entries
// remain pending, including late Relay second legs; unrelated entries retain
// FIFO order and are still eligible. This is not a durable transaction queue.
func (p *TxPool) HoldSourceAccount(account string) {
	p.HoldSourceAccounts([]string{account})
}

// HoldSourceAccounts publishes a committed group's fence atomically to the
// packer. It does not persist pending transactions or move account state.
func (p *TxPool) HoldSourceAccounts(accounts []string) {
	keys := make([]string, 0, len(accounts))
	for _, account := range accounts {
		if key := canonicalPrepareAccount(account); key != "" {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return
	}
	// Journal enable/extension already installs the fence. Reasserting that
	// same membership changes no durable obligation and must not wait for I/O.
	// New members still take journalGate below, preserving ACK/admission order.
	p.lock.Lock()
	alreadyHeld := true
	for _, key := range keys {
		if !p.heldMigrationAccounts[key] {
			alreadyHeld = false
			break
		}
	}
	if alreadyHeld {
		p.sourcePreparedAccount = keys[len(keys)-1]
		p.lock.Unlock()
		return
	}
	p.lock.Unlock()
	p.journalGate.Lock()
	defer p.journalGate.Unlock()
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.heldMigrationAccounts == nil {
		p.heldMigrationAccounts = make(map[string]bool)
	}
	for _, key := range keys {
		p.heldMigrationAccounts[key] = true
	}
	// Preserve the marker produced by successive valid single-account calls.
	p.sourcePreparedAccount = keys[len(keys)-1]
}

// Called only after durable target activation, never on a timeout.
func (p *TxPool) ReleaseMigrationAccount(account string) {
	p.journalGate.Lock()
	defer p.journalGate.Unlock()
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.heldMigrationAccounts, canonicalPrepareAccount(account))
	p.sourcePreparedAccount = ""
	for a := range p.heldMigrationAccounts {
		p.sourcePreparedAccount = a
		break
	}
}

func (p *TxPool) packWithSourceHold(limit uint64, byteLimit int, size int) []*Transaction {
	selected := make([]*Transaction, 0)
	retained := make([]*Transaction, 0, len(p.TxQueue))
	used := 0
	for _, tx := range p.TxQueue {
		eligible := !p.heldForMigration(tx) && uint64(len(selected)) < limit
		if byteLimit >= 0 && used+size > byteLimit {
			eligible = false
		}
		if eligible {
			selected = append(selected, tx)
			used += size
		} else {
			retained = append(retained, tx)
		}
	}
	p.TxQueue = retained
	return selected
}
