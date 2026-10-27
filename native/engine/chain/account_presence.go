package chain

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie"
)

// Some versions of go-ethereum surface a missing trie node as a panic instead
// of an error. Convert both forms into an ordinary unavailable observation.
func openStateTrieAtRoot(root common.Hash, db *trie.Database) (opened *trie.Trie, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			opened = nil
			err = fmt.Errorf("%v", recovered)
		}
	}()
	if root != gethtypes.EmptyRootHash {
		if _, err := db.Node(root); err != nil {
			return nil, err
		}
	}
	opened, err = trie.New(trie.TrieID(root), db)
	if err != nil {
		return nil, err
	}
	// Force lazy readers to resolve the root now, so an unavailable root cannot
	// escape the fail-closed open boundary and panic during a later key read.
	if _, err = readTrieValue(opened, nil); err != nil {
		return nil, err
	}
	return opened, nil
}

func readTrieValue(opened *trie.Trie, key []byte) (value []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			value = nil
			err = fmt.Errorf("%v", recovered)
		}
	}()
	return opened.Get(key)
}

type AccountPresenceStatus string

const (
	AccountPresent     AccountPresenceStatus = "EXISTS"
	AccountAbsent      AccountPresenceStatus = "ABSENT"
	AccountUnavailable AccountPresenceStatus = "UNAVAILABLE"
)

type AccountPresenceObservation struct {
	Account      string                `json:"account"`
	ShardID      uint64                `json:"shard_id"`
	BlockHeight  uint64                `json:"block_height"`
	StateRoot    string                `json:"state_root"`
	Status       AccountPresenceStatus `json:"status"`
	ObservedAt   time.Time             `json:"observed_at"`
	FailureCause string                `json:"failure_cause,omitempty"`
}

// ObserveAccountPresence reads only the current committed trie. Unlike
// FetchAccounts it never turns a missing key into a default account.
func (bc *BlockChain) ObserveAccountPresence(raw string, observedAt time.Time) AccountPresenceObservation {
	account := normalizePartitionMapKey(raw)
	result := AccountPresenceObservation{Account: account, Status: AccountUnavailable, ObservedAt: observedAt}
	if bc == nil || bc.CurrentBlock == nil || bc.CurrentBlock.Header == nil || bc.triedb == nil || bc.ChainConfig == nil {
		result.FailureCause = "committed_state_unavailable"
		return result
	}
	result.ShardID = bc.ChainConfig.ShardID
	result.BlockHeight = bc.CurrentBlock.Header.Number
	result.StateRoot = hex.EncodeToString(bc.CurrentBlock.Header.StateRoot)
	decoded, decodeErr := hex.DecodeString(account)
	if strings.ContainsAny(account, " \t\r\n") || decodeErr != nil || len(decoded) != 20 {
		result.FailureCause = "invalid_account"
		return result
	}
	st, err := openStateTrieAtRoot(common.BytesToHash(bc.CurrentBlock.Header.StateRoot), bc.triedb)
	if err != nil {
		result.FailureCause = "open_committed_trie"
		return result
	}
	rawState, err := readTrieValue(st, []byte(account))
	if err != nil {
		result.FailureCause = "read_committed_state"
		return result
	}
	// Existing migration fixtures may contain legacy prefixed trie keys. A
	// canonical miss must not hide a real state stored under that spelling.
	legacyState, err := readTrieValue(st, []byte("0x"+account))
	if err != nil {
		result.FailureCause = "read_legacy_state"
		return result
	}
	if len(rawState) > 0 && len(legacyState) > 0 {
		result.FailureCause = "duplicate_address_state_keys"
		return result
	}
	if len(legacyState) > 0 {
		rawState = legacyState
	}
	if len(rawState) == 0 {
		result.Status = AccountAbsent
	} else {
		result.Status = AccountPresent
	}
	return result
}
