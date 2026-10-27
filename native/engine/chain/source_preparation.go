package chain

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

var sourcePreparationKey = []byte("__sef_source_preparation_v1__")

func (bc *BlockChain) SourcePreparation() *core.SourcePreparation {
	bc.sourcePreparationMu.RLock()
	defer bc.sourcePreparationMu.RUnlock()
	if bc.sourcePreparation == nil {
		return nil
	}
	copy := *bc.sourcePreparation
	copy.State = append([]byte(nil), copy.State...)
	copy.RequestJSON = append([]byte(nil), copy.RequestJSON...)
	return &copy
}

// Native consensus serializes this check and CommitSourcePreparation against
// normal block execution. It is not an independently synchronized RPC writer.
func (bc *BlockChain) ValidateSourcePreparation(c core.SourcePrepareCommand) error {
	indexed, err := NativeEffectIndexConfiguration()
	if err != nil {
		return err
	}
	if indexed {
		return fmt.Errorf("indexed migration requires the whole-group snapshot protocol")
	}
	if bc.GroupSourcePreparation() != nil {
		return fmt.Errorf("group preparation already active")
	}
	return bc.validateSourcePreparationState(c)
}

func (bc *BlockChain) validateSourcePreparationState(c core.SourcePrepareCommand) error {
	if old := bc.SourcePreparation(); old != nil {
		if old.Command == c {
			return nil
		}
		return fmt.Errorf("source already has a prepared proposal")
	}
	account, err := hex.DecodeString(c.Account)
	if err != nil || len(account) != 20 || normalizePartitionMapKey(c.Account) != c.Account || c.ProposalID == "" {
		return fmt.Errorf("invalid source preparation identity")
	}
	owner, err := bc.sourcePreparationOwner(c.Account, c.MappingVersion)
	if err != nil {
		return err
	}
	if c.SourceShard != bc.ChainConfig.ShardID || c.TargetShard == c.SourceShard || c.TargetShard >= bc.ChainConfig.ShardNums || owner != c.SourceShard {
		return fmt.Errorf("source route or target mismatch")
	}
	if c.ExpectedBlockHash != hex.EncodeToString(bc.CurrentBlock.Hash) || c.ExpectedStateRoot != hex.EncodeToString(bc.CurrentBlock.Header.StateRoot) {
		return fmt.Errorf("stale source snapshot")
	}
	state, err := bc.ExportAccountState(c.Account)
	if err != nil {
		return err
	}
	if c.ExpectedStateHash != hex.EncodeToString(state.StateHash) {
		return fmt.Errorf("source account hash mismatch")
	}
	return nil
}

// Called by the PBFT commit hook, never the pre-prepare hook. Earlier requests
// have committed; all source replicas execute this same preparation command.
func (bc *BlockChain) CommitSourcePreparation(c core.SourcePrepareCommand, sequence uint64, digest []byte, requestJSON ...[]byte) (*core.SourcePreparation, error) {
	if len(digest) != 32 || sequence == 0 {
		return nil, fmt.Errorf("preparation commit context missing")
	}
	if err := bc.ValidateSourcePreparation(c); err != nil {
		return nil, err
	}
	if old := bc.SourcePreparation(); old != nil {
		return old, nil
	}
	state, err := bc.ExportAccountState(c.Account)
	if err != nil {
		return nil, err
	}
	record := &core.SourcePreparation{Command: c, State: state.EncodedState, StateHash: hex.EncodeToString(state.StateHash),
		SourceStateRoot: hex.EncodeToString(state.SourceRoot), PrepareBlock: bc.CurrentBlock.Header.Number + 1,
		PrepareSequence: sequence, CommitDigest: hex.EncodeToString(digest)}
	if len(requestJSON) > 0 {
		record.RequestJSON = append([]byte(nil), requestJSON[0]...)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	if err = st.Update(sourcePreparationKey, encoded); err != nil {
		return nil, err
	}
	if _, err = bc.commitStateTrie(st); err != nil {
		return nil, err
	}
	bc.sourcePreparationMu.Lock()
	bc.sourcePreparation = record
	bc.sourcePreparationMu.Unlock()
	bc.Txpool.HoldSourceAccount(c.Account)
	return bc.SourcePreparation(), nil
}

func (bc *BlockChain) restoreSourcePreparation() error {
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	raw, err := st.Get(sourcePreparationKey)
	if err != nil || len(raw) == 0 {
		return err
	}
	var record core.SourcePreparation
	if err = json.Unmarshal(raw, &record); err != nil {
		return err
	}
	actual, err := bc.ExportAccountState(record.Command.Account)
	if err != nil {
		return err
	}
	if record.StateHash != hex.EncodeToString(actual.StateHash) || record.Command.ProposalID == "" {
		return fmt.Errorf("invalid persisted preparation")
	}
	bc.sourcePreparationMu.Lock()
	bc.sourcePreparation = &record
	bc.sourcePreparationMu.Unlock()
	bc.Update_PartitionMap(record.Command.Account, record.Command.SourceShard)
	bc.Txpool.HoldSourceAccount(record.Command.Account)
	return nil
}

func (bc *BlockChain) rejectPreparedWrite(account string) error {
	if bc.groupPreparedAccount(account) {
		return fmt.Errorf("account held by group preparation")
	}
	if p, err := bc.TargetInstallation(normalizePartitionMapKey(account)); err != nil {
		return err
	} else if p != nil {
		return fmt.Errorf("target account awaits activation")
	}
	p := bc.SourcePreparation()
	if p != nil && normalizePartitionMapKey(account) == p.Command.Account {
		return fmt.Errorf("account held by preparation %s", p.Command.ProposalID)
	}
	return nil
}

func (bc *BlockChain) CheckPreparedTransactions(txs []*core.Transaction) error {
	if err := bc.checkGroupPreparedTransactions(txs); err != nil {
		return err
	}
	if err := bc.checkTargetTransactions(txs); err != nil {
		return err
	}
	p := bc.SourcePreparation()
	if p == nil {
		return nil
	}
	for _, tx := range txs {
		if bc.transactionWritesHeldAccount(tx, p.Command.Account) {
			return fmt.Errorf("transaction touches prepared account: %s", p.Command.ProposalID)
		}
	}
	return nil
}

// Cached state roots must not bypass the fence by taking AddBlock's fast path.
func (bc *BlockChain) checkPreparedRoot(root []byte) error {
	if err := bc.checkGroupPreparedRoot(root); err != nil {
		return err
	}
	if err := bc.checkTargetHeldRoot(root); err != nil {
		return err
	}
	p := bc.SourcePreparation()
	if p == nil {
		return nil
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(root)), bc.triedb)
	if err != nil {
		return err
	}
	marker, err := st.Get(sourcePreparationKey)
	if err != nil {
		return err
	}
	expected, _ := json.Marshal(p)
	if string(marker) != string(expected) {
		return fmt.Errorf("prepared marker altered by block")
	}
	state, err := st.Get([]byte(p.Command.Account))
	if err != nil {
		return err
	}
	storedHash := sha256.Sum256(state)
	if len(state) == 0 || hex.EncodeToString(storedHash[:]) != p.StateHash {
		return fmt.Errorf("prepared account altered by block")
	}
	return nil
}
