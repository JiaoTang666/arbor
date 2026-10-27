package chain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"blockEmulator/core"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

type AccountStateMigration struct {
	EncodedState []byte // exact trie bytes; gob re-encoding is not a cross-process identity
	Address      string
	State        *core.AccountState
	SourceRoot   []byte
	StateHash    []byte
}

func cloneAccountState(state *core.AccountState) *core.AccountState {
	if state == nil {
		return nil
	}
	return &core.AccountState{
		AcAddress:   state.AcAddress,
		Nonce:       state.Nonce,
		Balance:     new(big.Int).Set(state.Balance),
		StorageRoot: append([]byte(nil), state.StorageRoot...),
		CodeHash:    append([]byte(nil), state.CodeHash...),
	}
}

func migrationMarkerKey(migration *AccountStateMigration) []byte {
	digest := sha256.Sum256(append([]byte(migration.Address+"|"), migration.StateHash...))
	return []byte("__state_migration_receipt__" + hex.EncodeToString(digest[:]))
}

func (bc *BlockChain) stateTrie() (*trie.Trie, error) {
	if bc.CurrentBlock == nil || bc.triedb == nil {
		return nil, errors.New("state trie is not initialized")
	}
	return trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
}

func (bc *BlockChain) commitStateTrie(st *trie.Trie) ([]byte, error) {
	root, nodes := st.Commit(false)
	if nodes != nil {
		if err := bc.triedb.Update(trie.NewWithNodeSet(nodes)); err != nil {
			return nil, err
		}
		if err := bc.triedb.Commit(root, false); err != nil {
			return nil, err
		}
	}
	bh := &core.BlockHeader{
		ParentBlockHash: append([]byte(nil), bc.CurrentBlock.Hash...),
		StateRoot:       append([]byte(nil), root.Bytes()...),
		TxRoot:          GetTxTreeRoot(nil),
		Bloom:           *GetBloomFilter(nil),
		Number:          bc.CurrentBlock.Header.Number + 1,
	}
	block := core.NewBlock(bh, nil)
	block.Hash = block.Header.Hash()
	bc.CurrentBlock = block
	bc.Storage.AddBlock(block)
	return append([]byte(nil), root.Bytes()...), nil
}

func (bc *BlockChain) ExportAccountState(address string) (*AccountStateMigration, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	encoded, err := st.Get([]byte(address))
	if err != nil {
		return nil, err
	}
	if encoded == nil {
		return nil, fmt.Errorf("account %q is not present", address)
	}
	state := core.DecodeAS(encoded)
	storedHash := sha256.Sum256(encoded)
	return &AccountStateMigration{
		Address:      address,
		State:        cloneAccountState(state),
		EncodedState: append([]byte(nil), encoded...),
		SourceRoot:   append([]byte(nil), bc.CurrentBlock.Header.StateRoot...),
		StateHash:    storedHash[:],
	}, nil
}

func (bc *BlockChain) ImportAccountState(migration *AccountStateMigration) ([]byte, error) {
	if migration != nil {
		if err := bc.rejectPreparedWrite(migration.Address); err != nil {
			return nil, err
		}
	}
	if migration == nil || migration.State == nil || migration.Address == "" {
		return nil, errors.New("invalid account state migration")
	}
	if !bytes.Equal(migration.StateHash, migration.State.Hash()) {
		return nil, errors.New("migration state hash mismatch")
	}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	existing, err := st.Get([]byte(migration.Address))
	if err != nil {
		return nil, err
	}
	if existing != nil {
		current := core.DecodeAS(existing)
		if bytes.Equal(migration.StateHash, current.Hash()) {
			return append([]byte(nil), bc.CurrentBlock.Header.StateRoot...), nil
		}
		return nil, fmt.Errorf("target account %q conflicts with migration", migration.Address)
	}
	st.Update([]byte(migration.Address), migration.State.Encode())
	return bc.commitStateTrie(st)
}

func (bc *BlockChain) FinalizeAccountStateMigration(target *BlockChain, migration *AccountStateMigration) ([]byte, error) {
	if migration != nil {
		if err := bc.rejectPreparedWrite(migration.Address); err != nil {
			return nil, err
		}
	}
	if target == nil {
		return nil, errors.New("target chain is required")
	}
	if migration == nil || migration.State == nil || migration.Address == "" {
		return nil, errors.New("invalid account state migration")
	}
	targetState, err := target.ExportAccountState(migration.Address)
	if err != nil {
		return nil, fmt.Errorf("target import not confirmed: %w", err)
	}
	if !bytes.Equal(targetState.StateHash, migration.StateHash) {
		return nil, errors.New("target state does not match migration")
	}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	encoded, err := st.Get([]byte(migration.Address))
	if err != nil {
		return nil, err
	}
	if encoded == nil {
		receipt, receiptErr := st.Get(migrationMarkerKey(migration))
		if receiptErr != nil {
			return nil, receiptErr
		}
		if bytes.Equal(receipt, migration.StateHash) {
			return append([]byte(nil), bc.CurrentBlock.Header.StateRoot...), nil
		}
		return nil, fmt.Errorf("source account %q is missing", migration.Address)
	}
	current := core.DecodeAS(encoded)
	if !bytes.Equal(migration.StateHash, current.Hash()) {
		return nil, errors.New("source state changed after prepare")
	}
	st.Delete([]byte(migration.Address))
	st.Update(migrationMarkerKey(migration), append([]byte(nil), migration.StateHash...))
	return bc.commitStateTrie(st)
}
