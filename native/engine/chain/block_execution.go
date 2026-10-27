package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

// Consensus serializes these objects. Only one candidate and one committed
// report are retained; no unbounded transaction-history cache is introduced.
type blockExecution struct {
	key, body [32]byte
	root      common.Hash
	state     *trie.Trie
	applied   []bool
	persisted bool
}

type committedBlockExecution struct {
	identity [32]byte
	applied  []bool
}

// TxRoot is a set of transaction hashes in the legacy format. It is NOT an
// ordered execution-input commitment, so it cannot identify a cached result.
func transactionBodyDigest(txs []*core.Transaction) ([32]byte, error) {
	return canonicalExecutionDigest(txs)
}

func canonicalExecutionDigest(value interface{}) ([32]byte, error) {
	h := sha256.New()
	// FirstPlacement contains a map. JSON sorts its string keys; gob map
	// iteration is not canonical and would invalidate an otherwise equal body.
	if err := json.NewEncoder(h).Encode(value); err != nil {
		return [32]byte{}, fmt.Errorf("encode execution body: %w", err)
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func committedExecutionIdentity(block *core.Block, body [32]byte) ([32]byte, error) {
	// Legacy Header.Hash hashes gob bytes, whose type IDs depend on the
	// process's registration order. Do not change that consensus format here.
	// Compare complete canonical content against our verified/stored block.
	return canonicalExecutionDigest(struct {
		Header *core.BlockHeader
		Hash   []byte
		Body   [32]byte
	}{block.Header, block.Hash, body})
}

func (bc *BlockChain) executeBlockTransactions(txs []*core.Transaction, timing *blockBuildTiming) (*blockExecution, error) {
	// Authorization and preparation guards are never cached.
	if err := bc.checkOwnershipTransactions(txs); err != nil {
		return nil, err
	}
	if err := bc.CheckPreparedTransactions(txs); err != nil {
		return nil, err
	}
	timing.mark(buildPrecheck)
	indexed, err := NativeEffectIndexConfiguration()
	if err != nil {
		return nil, err
	}
	body, err := transactionBodyDigest(txs)
	if err != nil {
		return nil, err
	}
	routes := make([][2]uint64, len(txs))
	for i, tx := range txs {
		if tx == nil {
			return nil, fmt.Errorf("nil transaction at position %d", i)
		}
		routes[i] = [2]uint64{bc.ResolveTxSenderShard(tx), bc.ResolveTxRecipientShard(tx)}
	}
	strictBalance := os.Getenv("BLOCK_EMULATOR_CLPA_EPOCH_V1") == "1"
	h := sha256.New()
	input := struct {
		ParentHash, ParentRoot []byte
		Height, Shard, Shards  uint64
		Body                   [32]byte
		Routes                 [][2]uint64
		Indexed, Strict        bool
		InitialBalance         string
	}{bc.CurrentBlock.Hash, bc.CurrentBlock.Header.StateRoot, bc.CurrentBlock.Header.Number + 1,
		bc.ChainConfig.ShardID, bc.ChainConfig.ShardNums, body, routes, indexed, strictBalance, params.Init_Balance.String()}
	if err := gob.NewEncoder(h).Encode(input); err != nil {
		return nil, err
	}
	var key [32]byte
	copy(key[:], h.Sum(nil))
	if cached := bc.pendingExecution; cached != nil && cached.key == key {
		return cached, nil
	}
	bc.pendingExecution = nil
	st, err := openStateTrieAtRoot(common.BytesToHash(bc.CurrentBlock.Header.StateRoot), bc.triedb)
	if err != nil {
		return nil, fmt.Errorf("open_trie: %w", err)
	}
	if err := InitializeTransactionEffectIndex(st, bc.CurrentBlock.Header.Number); err != nil {
		return nil, err
	}
	timing.mark(buildOpenTrie)
	if stopTrace := beginExecutionWaitTrace(bc.ChainConfig.ShardID, bc.CurrentBlock.Header.Number+1); stopTrace != nil {
		defer stopTrace()
	}
	result := &blockExecution{key: key, body: body, state: st, applied: make([]bool, len(txs))}
	for i, tx := range txs {
		if tx.Value == nil || tx.Value.Sign() < 0 {
			return nil, fmt.Errorf("invalid transfer value at position %d", i)
		}
		if indexed {
			changes, err := bc.applyIndexedRawRelayObserved(st, tx, false, timing.executionTiming())
			if err != nil {
				return nil, err
			}
			// Indexed reporting still checks the stage-specific effect receipt
			// and its block height; this bit alone does not replace that proof.
			result.applied[i] = changes > 0
		} else {
			applied, err := bc.applyNativeTransfer(st, tx, strictBalance)
			if err != nil {
				return nil, fmt.Errorf("transaction %d: %w", i, err)
			}
			result.applied[i] = applied
		}
	}
	timing.mark(buildExecute)
	result.root = st.Hash()
	bc.pendingExecution = result
	return result, nil
}

// All writes are private until the caller validates the root and persists it.
// An insufficient debit is a rejected transaction, not a successful Relay1.
func (bc *BlockChain) applyNativeTransfer(st *trie.Trie, tx *core.Transaction, strictBalance bool) (bool, error) {
	applied := false
	write := func(account, role string, state *core.AccountState, existed bool) error {
		if err := st.Update([]byte(account), state.Encode()); err != nil {
			return err
		}
		if !existed {
			return bc.recordNativeFirstOwner(st, account, tx, role)
		}
		return nil
	}
	read := func(account, role string, mustExist bool) (*core.AccountState, bool, error) {
		state, existed, err := readExecutionAccount(st, account, false, nil)
		if err == nil && !existed && mustExist {
			err = fmt.Errorf("migrated %s state missing: account=%s mapping_version=%d", role, account, tx.MappingVersion)
		}
		return state, existed, err
	}
	if !tx.Relayed && (bc.ResolveTxSenderShard(tx) == bc.ChainConfig.ShardID || tx.HasBroker) {
		state, existed, err := read(tx.Sender, "sender", tx.SenderRequiresExistingState)
		if err != nil {
			return false, err
		}
		if !state.Deduct(tx.Value) {
			if strictBalance {
				return false, fmt.Errorf("insufficient_balance: sender %s", tx.Sender)
			}
			return false, nil // Do not reach the credit branch.
		}
		state.Nonce++
		if err := write(tx.Sender, "sender", state, existed); err != nil {
			return false, err
		}
		applied = true
	}
	if bc.ResolveTxRecipientShard(tx) == bc.ChainConfig.ShardID || tx.HasBroker {
		state, existed, err := read(tx.Recipient, "recipient", tx.RecipientRequiresExistingState)
		if err != nil {
			return false, err
		}
		state.Deposit(tx.Value)
		if err := write(tx.Recipient, "recipient", state, existed); err != nil {
			return false, err
		}
		applied = true
	}
	return applied, nil
}

func (bc *BlockChain) persistBlockExecution(result *blockExecution, timing *blockBuildTiming) error {
	if result.persisted {
		return nil
	}
	root, nodes := result.state.Commit(false)
	timing.mark(buildTrieCommit)
	var err error
	if root != result.root {
		err = fmt.Errorf("execution root changed before persistence")
	} else if nodes != nil {
		err = bc.triedb.Update(trie.NewWithNodeSet(nodes))
		timing.mark(buildTrieUpdate)
		if err == nil {
			err = bc.triedb.Commit(root, false)
			timing.mark(buildTriePersist)
		}
	}
	if err != nil {
		bc.pendingExecution = nil // A committed trie object must not be reused.
		return fmt.Errorf("persist_state: %w", err)
	}
	result.persisted, result.state = true, nil
	return nil
}

func (bc *BlockChain) validateBlockExecution(b *core.Block) (*blockExecution, error) {
	if b == nil || b.Header == nil || bc.CurrentBlock == nil || b.Header.Number != bc.CurrentBlock.Header.Number+1 {
		return nil, fmt.Errorf("block is not the next ledger height")
	}
	if !bytes.Equal(b.Header.ParentBlockHash, bc.CurrentBlock.Hash) || len(b.Hash) != sha256.Size {
		return nil, fmt.Errorf("block parent or hash identity mismatch")
	}
	for _, tx := range b.Body {
		if tx == nil {
			return nil, fmt.Errorf("nil block transaction")
		}
	}
	if !bytes.Equal(GetTxTreeRoot(b.Body), b.Header.TxRoot) {
		return nil, fmt.Errorf("transaction root mismatch")
	}
	result, err := bc.executeBlockTransactions(b.Body, nil)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(result.root.Bytes(), b.Header.StateRoot) {
		bc.pendingExecution = nil
		return nil, fmt.Errorf("replayed_root_mismatch: computed root differs from proposed block")
	}
	if b.ExecutionResults != nil && !reflect.DeepEqual(b.ExecutionResults, result.applied) {
		bc.pendingExecution = nil
		return nil, fmt.Errorf("block execution outcomes differ from local execution")
	}
	return result, nil
}
