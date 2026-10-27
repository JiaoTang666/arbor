package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

var targetHoldIndexKey = []byte("__sef_target_holds_v1__")

func (bc *BlockChain) targetHeldAccounts() ([]string, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	raw, err := st.Get(targetHoldIndexKey)
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var accounts []string
	err = json.Unmarshal(raw, &accounts)
	return accounts, err
}
func (bc *BlockChain) restoreTargetHolds() error {
	accounts, err := bc.targetHeldAccounts()
	if err != nil {
		return err
	}
	for _, a := range accounts {
		p, err := bc.TargetInstallation(a)
		if err != nil {
			return err
		}
		if p == nil {
			return fmt.Errorf("target hold missing installation")
		}
		bc.Txpool.HoldSourceAccount(a)
	}
	return bc.restoreTargetActivationRoutes()
}
func (bc *BlockChain) checkTargetTransactions(txs []*core.Transaction) error {
	accounts, err := bc.targetHeldAccounts()
	if err != nil {
		return err
	}
	for _, a := range accounts {
		for _, tx := range txs {
			// Only frozen RawRelay routing can establish a debit-only write.
			// Unfrozen routing may still resolve to the group's old shard;
			// retain the conservative contact fence for it and Broker traffic.
			blocked := core.TransactionTouchesAccount(tx, a)
			if tx != nil && tx.RoutingFrozen && !tx.HasBroker && !tx.SenderIsBroker && tx.OriginalSender == "" && tx.FinalRecipient == "" {
				blocked = bc.transactionWritesHeldAccount(tx, a)
			}
			if blocked {
				return fmt.Errorf("target account awaits activation: %s", a)
			}
		}
	}
	return nil
}
func (bc *BlockChain) checkTargetHeldRoot(root []byte) error {
	accounts, err := bc.targetHeldAccounts()
	if err != nil || len(accounts) == 0 {
		return err
	}
	old, err := bc.stateTrie()
	if err != nil {
		return err
	}
	next, err := trie.New(trie.TrieID(common.BytesToHash(root)), bc.triedb)
	if err != nil {
		return err
	}
	keys := [][]byte{targetHoldIndexKey}
	for _, a := range accounts {
		keys = append(keys, []byte(a), targetInstallationKey(a))
	}
	for _, key := range keys {
		before, e := old.Get(key)
		if e != nil {
			return e
		}
		after, e := next.Get(key)
		if e != nil {
			return e
		}
		if !bytes.Equal(before, after) {
			return fmt.Errorf("block altered unactivated target state")
		}
	}
	return nil
}
