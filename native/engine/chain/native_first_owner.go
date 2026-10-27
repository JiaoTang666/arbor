package chain

import (
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
)

// This is an execution provenance record, not a new placement decision or a
// Byzantine ownership certificate. It binds the trusted supervisor's frozen
// route to the shard that actually creates the state in an ordinary block.
// Like the account state, a proposal's record is invisible until its root is
// the committed CurrentBlock root. No in-memory routing side effect is used.
type NativeFirstOwner struct {
	Schema          string `json:"schema"`
	Account         string `json:"account"`
	Shard           uint64 `json:"shard"`
	MappingVersion  uint64 `json:"mapping_version"`
	CreationHeight  uint64 `json:"creation_height"`
	IngressSequence uint64 `json:"ingress_sequence"`
	TxHash          string `json:"tx_hash"`
	Role            string `json:"role"`
}

func nativeFirstOwnerKey(account string) []byte {
	return []byte("__native_first_owner_v1__" + normalizePartitionMapKey(account))
}

// Called ONLY where an absent account is actually written, using the SAME
// trie transaction. A skipped debit, unrelated shard or relay sender cannot
// acquire ownership merely because its address appeared in a block.
func (bc *BlockChain) recordNativeFirstOwner(st *trie.Trie, account string, tx *core.Transaction, role string) error {
	if !tx.RoutingFrozen || tx.HasBroker {
		return nil // Legacy execution has its existing mapping contract.
	}
	account = normalizePartitionMapKey(account)
	owner := tx.RoutedRecipientShard
	if role == "sender" {
		owner = tx.RoutedSenderShard
		if tx.Relayed || normalizePartitionMapKey(tx.Sender) != account {
			return fmt.Errorf("invalid native creator sender")
		}
	} else if role != "recipient" || normalizePartitionMapKey(tx.Recipient) != account {
		return fmt.Errorf("invalid native creator recipient")
	}
	if owner != bc.ChainConfig.ShardID || owner >= bc.ChainConfig.ShardNums {
		return fmt.Errorf("native creator route is not this shard")
	}
	if bc.initialLedger != nil {
		if shard, exists := bc.initialLedger.HistoricalOwner(account); exists {
			if shard != owner {
				return fmt.Errorf("native creation conflicts with initial ledger owner")
			}
			return nil // Immutable genesis ownership already supplies provenance.
		}
	}
	bc.pmlock.RLock()
	explicit, exists := bc.PartitionMap[account]
	bc.pmlock.RUnlock()
	if exists && explicit != owner {
		return fmt.Errorf("native creation conflicts with explicit owner")
	}
	old, err := readTrieValue(st, nativeFirstOwnerKey(account))
	if err != nil {
		return err
	}
	if len(old) != 0 {
		return fmt.Errorf("native first owner already exists while account state is absent")
	}
	record := NativeFirstOwner{Schema: "native_first_owner_v1", Account: account, Shard: owner,
		MappingVersion: tx.MappingVersion, CreationHeight: bc.CurrentBlock.Header.Number + 1,
		IngressSequence: tx.IngressSequence, TxHash: hex.EncodeToString(tx.TxHash), Role: role}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return st.Update(nativeFirstOwnerKey(account), raw)
}

// Reads committed ownership, never the newest persisted but uncommitted trie.
func (bc *BlockChain) NativeFirstOwner(account string) (*NativeFirstOwner, error) {
	account = normalizePartitionMapKey(account)
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	raw, err := readTrieValue(st, nativeFirstOwnerKey(account))
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var r NativeFirstOwner
	if err = json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	hash, hashErr := hex.DecodeString(r.TxHash)
	if r.Schema != "native_first_owner_v1" || r.Account != account || r.Shard != bc.ChainConfig.ShardID || r.Shard >= bc.ChainConfig.ShardNums || r.CreationHeight == 0 || r.CreationHeight > bc.CurrentBlock.Header.Number || hashErr != nil || len(hash) != 32 || (r.Role != "sender" && r.Role != "recipient") {
		return nil, fmt.Errorf("invalid committed native first owner")
	}
	return &r, nil
}

func (bc *BlockChain) sourcePreparationOwner(account string, version uint64) (uint64, error) {
	account = normalizePartitionMapKey(account)
	r, err := bc.NativeFirstOwner(account)
	if err != nil {
		return 0, err
	}
	if bc.initialLedger != nil {
		if owner, exists := bc.initialLedger.HistoricalOwner(account); exists {
			if r != nil && r.Shard != owner {
				return 0, fmt.Errorf("committed first owner conflicts with initial ledger")
			}
			return owner, nil
		}
	}
	if r != nil {
		if r.MappingVersion > version {
			return 0, fmt.Errorf("migration predates native account creation")
		}
		bc.pmlock.RLock()
		explicit, exists := bc.PartitionMap[account]
		bc.pmlock.RUnlock()
		if exists && explicit != r.Shard {
			return 0, fmt.Errorf("committed first owner conflicts with explicit route")
		}
		return r.Shard, nil
	}
	// Old/legacy snapshots retain their existing explicit/hash contract. State
	// existence alone is NOT an owner, and is checked separately by preparation.
	return bc.Get_PartitionMap(account), nil
}
