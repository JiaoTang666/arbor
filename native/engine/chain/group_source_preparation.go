package chain

import (
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

var groupPreparationKey = []byte("__sef_group_source_preparation_v1__")

func cloneGroup(p *core.GroupPreparation) *core.GroupPreparation {
	if p == nil {
		return nil
	}
	copy := *p
	copy.Command.Members = append([]core.SourcePrepareCommand(nil), p.Command.Members...)
	copy.Members = append([]core.SourcePreparation(nil), p.Members...)
	copy.RequestJSON = append([]byte(nil), p.RequestJSON...)
	copy.EffectRecords = append([]core.TransactionEffectSnapshot(nil), p.EffectRecords...)
	for i := range copy.EffectRecords {
		copy.EffectRecords[i].Original = append([]byte(nil), p.EffectRecords[i].Original...)
		copy.EffectRecords[i].Receipt = append([]byte(nil), p.EffectRecords[i].Receipt...)
		copy.EffectRecords[i].Binding = append([]byte(nil), p.EffectRecords[i].Binding...)
	}
	for i := range copy.Members {
		copy.Members[i].State = append([]byte(nil), p.Members[i].State...)
		copy.Members[i].RequestJSON = append([]byte(nil), p.Members[i].RequestJSON...)
	}
	return &copy
}

func (bc *BlockChain) GroupSourcePreparation(id ...string) *core.GroupPreparation {
	bc.sourcePreparationMu.RLock()
	defer bc.sourcePreparationMu.RUnlock()
	if len(id) > 0 {
		for _, p := range bc.groupSourceHistory {
			if p.Command.Members[0].ProposalID == id[0] {
				return cloneGroup(p)
			}
		}
		if bc.groupSourcePreparation == nil || bc.groupSourcePreparation.Command.Members[0].ProposalID != id[0] {
			return nil
		}
	}
	return cloneGroup(bc.groupSourcePreparation)
}

func sameGroupCommand(a, b core.GroupPrepareCommand) bool {
	if len(a.Members) != len(b.Members) {
		return false
	}
	for i := range a.Members {
		if a.Members[i] != b.Members[i] {
			return false
		}
	}
	return true
}

func validGroupIdentity(c core.GroupPrepareCommand) error {
	if len(c.Members) < core.MinMigrationGroupMembers || len(c.Members) > core.MaxMigrationGroupMembers {
		return fmt.Errorf("group requires %d..%d members", core.MinMigrationGroupMembers, core.MaxMigrationGroupMembers)
	}
	first := c.Members[0]
	for i, m := range c.Members {
		address, e := hex.DecodeString(m.Account)
		if e != nil || len(address) != 20 || normalizePartitionMapKey(m.Account) != m.Account || m.ProposalID == "" {
			return fmt.Errorf("invalid group member identity")
		}
		if i > 0 && c.Members[i-1].Account >= m.Account {
			return fmt.Errorf("group members must be unique and sorted")
		}
		if m.ProposalID != first.ProposalID || m.SourceShard != first.SourceShard || m.TargetShard != first.TargetShard ||
			m.MappingVersion != first.MappingVersion || m.AnchorVersion != first.AnchorVersion ||
			m.ExpectedBlockHash != first.ExpectedBlockHash || m.ExpectedStateRoot != first.ExpectedStateRoot || m.BindAtProposal != first.BindAtProposal {
			return fmt.Errorf("group identity or snapshot mismatch")
		}
	}
	return nil
}

// All validation and exports precede the only trie commit. Called under the
// same PBFT sequence serialization as single-account preparation.
func (bc *BlockChain) ValidateGroupSourcePreparation(c core.GroupPrepareCommand) error {
	if err := validGroupIdentity(c); err != nil {
		return err
	}
	if err := bc.validateGroupEffectCoverage(0, false); err != nil {
		return err
	}
	if bc.SourcePreparation() != nil {
		return fmt.Errorf("legacy preparation already active")
	}
	if old := bc.GroupSourcePreparation(c.Members[0].ProposalID); old != nil {
		if sameGroupCommand(old.Command, c) {
			return nil
		}
		return fmt.Errorf("different group preparation already active")
	}
	if err := bc.validateNextSourceGroup(c); err != nil {
		return err
	}
	for _, m := range c.Members {
		if err := bc.validateSourcePreparationState(m); err != nil {
			return err
		}
	}
	return nil
}

func (bc *BlockChain) CommitGroupSourcePreparation(c core.GroupPrepareCommand, sequence uint64, digest []byte, requestJSON []byte) (*core.GroupPreparation, error) {
	return bc.CommitGroupSourcePreparationObserved(c, sequence, digest, requestJSON, nil)
}

// Stage entry diagnostics distinguish trie/storage work from queue-fence waits.
// The observer must not access chain state or change execution ordering.
func (bc *BlockChain) CommitGroupSourcePreparationObserved(c core.GroupPrepareCommand, sequence uint64, digest []byte, requestJSON []byte, observe func(string)) (*core.GroupPreparation, error) {
	stage := func(name string) {
		if observe != nil {
			observe(name)
		}
	}
	stage("validate")
	if sequence == 0 || len(digest) != 32 {
		return nil, fmt.Errorf("group commit context missing")
	}
	if err := bc.ValidateGroupSourcePreparation(c); err != nil {
		return nil, err
	}
	if old := bc.GroupSourcePreparation(c.Members[0].ProposalID); old != nil {
		return old, nil
	}
	p := &core.GroupPreparation{Command: c, PrepareBlock: bc.CurrentBlock.Header.Number + 1, PrepareSequence: sequence, CommitDigest: hex.EncodeToString(digest), RequestJSON: append([]byte(nil), requestJSON...)}
	indexed, err := NativeEffectIndexConfiguration()
	if err != nil {
		return nil, err
	}
	if indexed {
		p.EffectIndexVersion = 1
	}
	stage("export_accounts")
	for _, m := range c.Members {
		state, err := bc.ExportAccountState(m.Account)
		if err != nil {
			return nil, err
		}
		p.Members = append(p.Members, core.SourcePreparation{Command: m, State: state.EncodedState,
			StateHash: hex.EncodeToString(state.StateHash), SourceStateRoot: hex.EncodeToString(state.SourceRoot),
			PrepareBlock: p.PrepareBlock, PrepareSequence: sequence, CommitDigest: p.CommitDigest})
	}
	p = cloneGroup(p)
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	if indexed {
		stage("export_effects")
		p.EffectRecords, err = exportGroupEffectSnapshot(st, c, bc.CurrentBlock.Header.Number)
		if err != nil {
			return nil, err
		}
	}
	stage("record_history")
	encoded, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if err = st.Update(groupPreparationKey, encoded); err != nil {
		return nil, err
	}
	previous := bc.GroupSourcePreparations()
	if err = prepareSourceGeneration(st, previous, c.Members[0].ProposalID); err != nil {
		return nil, err
	}
	history := append(previous, p)
	if err = writeGroupHistory(st, history); err != nil {
		return nil, err
	}
	stage("commit_trie_block")
	if _, err = bc.commitStateTrie(st); err != nil {
		return nil, err
	}
	bc.sourcePreparationMu.Lock()
	bc.groupSourcePreparation = p
	bc.groupSourceHistory = history
	bc.sourcePreparationMu.Unlock()
	accounts := make([]string, len(c.Members))
	for i, m := range c.Members {
		accounts[i] = m.Account
	}
	stage("hold_fence")
	bc.Txpool.HoldSourceAccounts(accounts)
	stage("complete")
	return bc.GroupSourcePreparation(), nil
}

func (bc *BlockChain) restoreGroupSourcePreparation() error {
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	raw, err := st.Get(groupPreparationKey)
	if err != nil || len(raw) == 0 {
		return err
	}
	var p core.GroupPreparation
	if err = json.Unmarshal(raw, &p); err != nil {
		return err
	}
	if err = validGroupIdentity(p.Command); err != nil {
		return err
	}
	history, err := readGroupHistory(st, &p)
	if err != nil {
		return err
	}
	if _, err = sourceGenerationAt(st, history); err != nil {
		return err
	}
	for _, group := range history {
		if err = bc.validateRestoredGroup(group); err != nil {
			return err
		}
	}
	bc.sourcePreparationMu.Lock()
	bc.groupSourcePreparation = cloneGroup(&p)
	bc.groupSourceHistory = history
	bc.sourcePreparationMu.Unlock()
	for _, group := range history {
		accounts := make([]string, len(group.Members))
		for i, m := range group.Members {
			bc.Update_PartitionMap(m.Command.Account, m.Command.SourceShard)
			accounts[i] = m.Command.Account
		}
		bc.Txpool.HoldSourceAccounts(accounts)
	}
	return nil
}

func (bc *BlockChain) validateRestoredGroup(p *core.GroupPreparation) error {
	if err := bc.validateGroupEffectCoverage(p.EffectIndexVersion, true); err != nil {
		return err
	}
	if err := validGroupIdentity(p.Command); err != nil {
		return err
	}
	if bc.SourcePreparation() != nil || len(p.Members) != len(p.Command.Members) || p.PrepareBlock == 0 || p.PrepareSequence == 0 {
		return fmt.Errorf("invalid persisted group preparation")
	}
	digest, e := hex.DecodeString(p.CommitDigest)
	if e != nil || len(digest) != 32 {
		return fmt.Errorf("invalid group commit digest")
	}
	for i, m := range p.Members {
		if m.Command != p.Command.Members[i] || m.PrepareBlock != p.PrepareBlock || m.PrepareSequence != p.PrepareSequence || m.CommitDigest != p.CommitDigest ||
			m.Command.SourceShard != bc.ChainConfig.ShardID || m.Command.TargetShard >= bc.ChainConfig.ShardNums || m.Command.TargetShard == m.Command.SourceShard {
			return fmt.Errorf("persisted group member binding mismatch")
		}
		state, e := bc.ExportAccountState(m.Command.Account)
		if e != nil {
			return e
		}
		h := sha256.Sum256(m.State)
		if m.StateHash != hex.EncodeToString(h[:]) || !bytes.Equal(state.EncodedState, m.State) || m.StateHash != m.Command.ExpectedStateHash || m.SourceStateRoot != m.Command.ExpectedStateRoot {
			return fmt.Errorf("persisted group member state mismatch")
		}
	}
	return nil
}

func (bc *BlockChain) groupPreparedAccount(account string) bool {
	for _, p := range bc.GroupSourcePreparations() {
		for _, m := range p.Command.Members {
			if m.Account == normalizePartitionMapKey(account) {
				return true
			}
		}
	}
	return false
}

func (bc *BlockChain) checkGroupPreparedTransactions(txs []*core.Transaction) error {
	for _, p := range bc.GroupSourcePreparations() {
		for _, tx := range txs {
			for _, m := range p.Command.Members {
				if bc.transactionWritesHeldAccount(tx, m.Account) {
					return fmt.Errorf("transaction touches prepared group %s", m.ProposalID)
				}
			}
		}
	}
	return nil
}

func (bc *BlockChain) checkGroupPreparedRoot(root []byte) error {
	p := bc.GroupSourcePreparation()
	if p == nil {
		return nil
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(root)), bc.triedb)
	if err != nil {
		return err
	}
	marker, err := st.Get(groupPreparationKey)
	if err != nil {
		return err
	}
	expected, err := json.Marshal(p)
	if err != nil {
		return err
	}
	if !bytes.Equal(marker, expected) {
		return fmt.Errorf("prepared group marker altered")
	}
	history, err := readGroupHistory(st, p)
	if err != nil {
		return err
	}
	if _, err = sourceGenerationAt(st, history); err != nil {
		return err
	}
	want, _ := json.Marshal(bc.GroupSourcePreparations())
	got, _ := json.Marshal(history)
	if !bytes.Equal(want, got) {
		return fmt.Errorf("prepared group history altered")
	}
	for _, group := range history {
		for _, m := range group.Members {
			state, err := st.Get([]byte(m.Command.Account))
			if err != nil {
				return err
			}
			if !bytes.Equal(state, m.State) {
				return fmt.Errorf("prepared group account altered")
			}
		}
	}
	return nil
}
