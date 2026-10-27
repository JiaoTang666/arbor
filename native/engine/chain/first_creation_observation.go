package chain

import (
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CreatorExpectation identifies an occurrence, not merely a transaction hash.
// This is a read-side contract, NOT an authority permit or proof of consensus.
type CreatorExpectation struct {
	AdmissionID    string `json:"admission_id"`
	TxHash         string `json:"tx_hash"`
	Account        string `json:"account"`
	Shard          uint64 `json:"shard"`
	MappingVersion uint64 `json:"mapping_version"`
}

type FirstCreationObservation struct {
	CreatorExpectation
	BlockHash       string    `json:"block_hash"`
	BlockHeight     uint64    `json:"block_height"`
	ParentStateRoot string    `json:"parent_state_root"`
	StateRoot       string    `json:"state_root"`
	StateHash       string    `json:"state_hash"`
	ObservedAt      time.Time `json:"observed_at"`
}

// ObserveFirstCreation must run under the caller's chain/commit serialization
// lock. It reads the actual committed head and its stored parent, never a
// proposal root or FetchAccounts' fabricated default. Ambiguous attribution
// (two transactions touching the account in one block) fails closed.
// An observation alone must NOT open an admission gate: authority binding and
// the original occurrence's complete lifecycle remain separate requirements.
func (bc *BlockChain) ObserveFirstCreation(blockHash []byte, expected CreatorExpectation, at time.Time) (FirstCreationObservation, error) {
	fail := func(reason string) (FirstCreationObservation, error) {
		return FirstCreationObservation{}, fmt.Errorf("first_creation: %s", reason)
	}
	accountBytes, err := hex.DecodeString(expected.Account)
	txHash, hashErr := hex.DecodeString(expected.TxHash)
	if err != nil || len(accountBytes) != 20 || expected.Account != strings.ToLower(expected.Account) || hashErr != nil || len(txHash) != 32 || expected.AdmissionID == "" || at.IsZero() {
		return fail("invalid_expectation")
	}
	if bc == nil || bc.CurrentBlock == nil || bc.CurrentBlock.Header == nil || bc.ChainConfig == nil || bc.Storage == nil || bc.triedb == nil {
		return fail("committed_state_unavailable")
	}
	b := bc.CurrentBlock
	if len(blockHash) != 32 || !bytes.Equal(b.Hash, blockHash) || !bytes.Equal(b.Header.Hash(), blockHash) || b.Header.Number == 0 {
		return fail("not_current_committed_block")
	}
	// Read the stored body, not a caller-owned pointer which the relay hook
	// may already have mutated after AddBlock.
	b, err = bc.Storage.GetBlock(blockHash)
	if err != nil || b == nil || b.Header == nil || !bytes.Equal(b.Header.Hash(), blockHash) {
		return fail("stored_block_unavailable")
	}
	if expected.Shard != bc.ChainConfig.ShardID {
		return fail("wrong_shard")
	}
	var creator *core.Transaction
	touches, occurrences := 0, 0
	for _, tx := range b.Body {
		if tx == nil {
			return fail("nil_block_transaction")
		}
		if normalizePartitionMapKey(tx.Sender) == expected.Account || normalizePartitionMapKey(tx.Recipient) == expected.Account {
			touches++
		}
		if tx.AdmissionID == expected.AdmissionID {
			creator = tx
			occurrences++
		}
	}
	if creator == nil || occurrences != 1 || touches != 1 {
		return fail("missing_or_ambiguous_creator")
	}
	if !bytes.Equal(creator.TxHash, txHash) || !creator.RoutingFrozen || creator.MappingVersion != expected.MappingVersion || creator.HasBroker {
		return fail("creator_binding_mismatch")
	}
	// Do not use Relayed here: the existing source commit hook mutates that
	// field when constructing relay messages. Frozen endpoint shards are stable.
	localEffect := (normalizePartitionMapKey(creator.Sender) == expected.Account && creator.RoutedSenderShard == expected.Shard) || (normalizePartitionMapKey(creator.Recipient) == expected.Account && creator.RoutedRecipientShard == expected.Shard)
	if !localEffect {
		return fail("no_local_creator_effect")
	}
	parent, err := bc.Storage.GetBlock(b.Header.ParentBlockHash)
	if err != nil || parent == nil || parent.Header == nil || parent.Header.Number+1 != b.Header.Number || !bytes.Equal(parent.Header.Hash(), b.Header.ParentBlockHash) {
		return fail("parent_unavailable")
	}
	read := func(root []byte) ([]byte, error) {
		st, e := openStateTrieAtRoot(common.BytesToHash(root), bc.triedb)
		if e != nil {
			return nil, e
		}
		canonical, e := readTrieValue(st, []byte(expected.Account))
		if e != nil {
			return nil, e
		}
		legacy, e := readTrieValue(st, []byte("0x"+expected.Account))
		if e != nil {
			return nil, e
		}
		if len(canonical) > 0 && len(legacy) > 0 {
			return nil, fmt.Errorf("duplicate_address_state_keys")
		}
		if len(legacy) > 0 {
			return legacy, nil
		}
		return canonical, nil
	}
	before, err := read(parent.Header.StateRoot)
	if err != nil {
		return fail("parent_state_unavailable")
	}
	if len(before) > 0 {
		return fail("account_already_existed")
	}
	after, err := read(b.Header.StateRoot)
	if err != nil {
		return fail("committed_state_unavailable")
	}
	if len(after) == 0 {
		return fail("account_not_created")
	}
	var state core.AccountState
	if err := gob.NewDecoder(bytes.NewReader(after)).Decode(&state); err != nil || state.Balance == nil || state.Balance.Sign() < 0 {
		return fail("invalid_account_state")
	}
	digest := sha256.Sum256(after)
	return FirstCreationObservation{CreatorExpectation: expected, BlockHash: hex.EncodeToString(b.Hash), BlockHeight: b.Header.Number, ParentStateRoot: hex.EncodeToString(parent.Header.StateRoot), StateRoot: hex.EncodeToString(b.Header.StateRoot), StateHash: hex.EncodeToString(digest[:]), ObservedAt: at}, nil
}
