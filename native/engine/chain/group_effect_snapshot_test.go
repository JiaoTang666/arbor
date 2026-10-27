package chain

import (
	"blockEmulator/core"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

func effectSnapshotTrie(t *testing.T, covered bool) *trie.Trie {
	t.Helper()
	st := trie.NewEmpty(trie.NewDatabase(rawdb.NewMemoryDatabase()))
	if covered {
		if err := InitializeTransactionEffectIndex(st, 0); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func effectSnapshotRecord(t *testing.T, tx *core.Transaction, effect string, block uint64) core.TransactionEffectSnapshot {
	t.Helper()
	st := effectMemoryTrie{}
	if err := InitializeTransactionEffectIndex(st, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordTransactionEffect(st, tx, effect, block); err != nil {
		t.Fatal(err)
	}
	e, err := core.BuildTransactionEffect(tx, effect)
	if err != nil {
		t.Fatal(err)
	}
	return core.TransactionEffectSnapshot{Original: tx.Encode(), Receipt: st[string(transactionEffectKey(e))], Binding: st[string(transactionAdmissionBindingKey(tx.AdmissionID))]}
}

func executedGroupSnapshot(t *testing.T) (*BlockChain, *core.GroupPreparation, *core.Transaction, string) {
	t.Helper()
	t.Setenv(NativeEffectIndexEnv, "true")
	source, c, path := groupFixture(t)
	t.Cleanup(source.CloseBlockChain)
	accounts := []string{c.Members[0].Account, c.Members[1].Account}
	tx := indexedAdmission(accounts[0], accounts[1], 7, 1)
	committedIndexedNative(t, source, tx)
	c = serialGroupCommand(t, source, c.Members[0].ProposalID, accounts, 0)
	p, err := source.CommitGroupSourcePreparation(c, 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.EffectRecords) != 2 {
		t.Fatalf("pre-freeze debit/credit not exported together: %d", len(p.EffectRecords))
	}
	return source, p, tx, path
}

func TestGroupEffectSnapshotPreservesPreFreezeEffectsAcrossOrdinaryAndHandoff(t *testing.T) {
	_, p, original, path := executedGroupSnapshot(t)
	target := newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	serialActivate(t, target, p, 1)
	for _, record := range p.EffectRecords {
		tx, old, _, err := decodeGroupEffectSnapshot(record)
		if err != nil {
			t.Fatal(err)
		}
		st, _ := target.stateTrie()
		receipt, err := ReadTransactionEffectReceipt(st, tx, old.Effect.Effect)
		if err != nil || receipt == nil || receipt.Block != 1 || receipt.ImportedSourceBlock != old.Block || receipt.ImportedSourceRoot != p.Command.Members[0].ExpectedStateRoot || receipt.ImportedSourceShard != 0 {
			t.Fatal("imported effect lacks target-local block and source hop", err)
		}
	}
	accounts := []string{original.Sender, original.Recipient}
	moved := core.DecodeTx(original.Encode())
	moved.RoutedSenderShard, moved.RoutedRecipientShard = 1, 1
	committedIndexedNative(t, target, moved)
	h, err := core.BuildGroupMigrationHandoff(original, p.Command.Members[0].ProposalID, accounts, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = target.CommitMigrationHandoff(*h); err != nil {
		t.Fatal(err)
	}
	a, _ := target.ExportAccountState(accounts[0])
	b, _ := target.ExportAccountState(accounts[1])
	if a.State.Balance.Int64() != 93 || a.State.Nonce != 1 || b.State.Balance.Int64() != 107 {
		t.Fatal("post-migration ordinary/handoff replay changed pre-freeze balances")
	}
	fresh := indexedAdmission(accounts[0], accounts[1], 3, 2)
	fresh.RoutedSenderShard, fresh.RoutedRecipientShard = 1, 1
	committedIndexedNative(t, target, fresh)
	a, _ = target.ExportAccountState(accounts[0])
	b, _ = target.ExportAccountState(accounts[1])
	if a.State.Balance.Int64() != 90 || a.State.Nonce != 2 || b.State.Balance.Int64() != 110 {
		t.Fatal("effect snapshot blocked a distinct admission")
	}
}

func TestGroupEffectImportPrevalidatesBeforeAnyWriteAndIsIdempotent(t *testing.T) {
	_, p, original, _ := executedGroupSnapshot(t)
	st := effectSnapshotTrie(t, true)
	if err := importGroupEffectSnapshot(st, *p, 9); err != nil {
		t.Fatal(err)
	}
	before := st.Hash()
	if err := importGroupEffectSnapshot(st, *p, 10); err != nil || st.Hash() != before {
		t.Fatal("same effect import overwrote its original local receipt", err)
	}
	clone := func() core.GroupPreparation {
		raw, _ := json.Marshal(p)
		var copy core.GroupPreparation
		if err := json.Unmarshal(raw, &copy); err != nil {
			t.Fatal(err)
		}
		return copy
	}
	tests := []struct {
		name   string
		mutate func(*core.GroupPreparation)
	}{
		{"duplicate", func(q *core.GroupPreparation) { q.EffectRecords = append(q.EffectRecords, q.EffectRecords[0]) }},
		{"unsorted", func(q *core.GroupPreparation) {
			q.EffectRecords[0], q.EffectRecords[1] = q.EffectRecords[1], q.EffectRecords[0]
		}},
		{"missing_original", func(q *core.GroupPreparation) { q.EffectRecords[1].Original = nil }},
		{"invalid_binding", func(q *core.GroupPreparation) { q.EffectRecords[1].Binding = []byte("{}") }},
		{"missing_source_coverage", func(q *core.GroupPreparation) { q.EffectIndexVersion = 0 }},
		{"conflicting_payload_in_batch", func(q *core.GroupPreparation) {
			changed := core.DecodeTx(original.Encode())
			changed.Value = big.NewInt(8)
			q.EffectRecords[1] = effectSnapshotRecord(t, changed, core.TransactionCredit, 1)
		}},
		{"foreign_account", func(q *core.GroupPreparation) {
			changed := core.DecodeTx(original.Encode())
			changed.Recipient = "ffffffffffffffffffffffffffffffffffffffff"
			q.EffectRecords = append(q.EffectRecords, effectSnapshotRecord(t, changed, core.TransactionCredit, 1))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := clone()
			test.mutate(&q)
			working := effectSnapshotTrie(t, true)
			root := working.Hash()
			if err := importGroupEffectSnapshot(working, q, 9); err == nil || working.Hash() != root {
				t.Fatal("invalid effect snapshot was accepted or published a prefix", err)
			}
		})
	}
	uncovered := effectSnapshotTrie(t, false)
	if err := importGroupEffectSnapshot(uncovered, *p, 9); err == nil {
		t.Fatal("target without complete coverage accepted effect import")
	}
	conflict := effectSnapshotTrie(t, true)
	changed := core.DecodeTx(original.Encode())
	changed.Value = big.NewInt(8)
	if _, err := RecordTransactionEffect(conflict, changed, core.TransactionCredit, 1); err != nil {
		t.Fatal(err)
	}
	root := conflict.Hash()
	if err := importGroupEffectSnapshot(conflict, *p, 9); err == nil || conflict.Hash() != root {
		t.Fatal("target admission conflict accepted or caused partial writes", err)
	}
}

func TestGroupEffectExportRequiresSameRootAndOriginalReceipt(t *testing.T) {
	_, p, _, _ := executedGroupSnapshot(t)
	st := effectSnapshotTrie(t, true)
	if _, err := exportGroupEffectSnapshot(st, p.Command, p.PrepareBlock-1); err == nil {
		t.Fatal("different source root accepted")
	}
	first := p.EffectRecords[0]
	tx, receipt, _, err := decodeGroupEffectSnapshot(first)
	if err != nil {
		t.Fatal(err)
	}
	receipt.Transaction = nil
	raw, _ := json.Marshal(receipt)
	if err = st.Update(transactionEffectKey(receipt.Effect), raw); err != nil {
		t.Fatal(err)
	}
	if err = st.Update(transactionAdmissionBindingKey(tx.AdmissionID), first.Binding); err != nil {
		t.Fatal(err)
	}
	command := p.Command
	command.Members = append([]core.SourcePrepareCommand(nil), command.Members...)
	for i := range command.Members {
		command.Members[i].ExpectedStateRoot = st.Hash().Hex()[2:]
	}
	if records, err := exportGroupEffectSnapshot(st, command, p.PrepareBlock-1); err == nil || records != nil {
		t.Fatal("legacy receipt without original was falsely exported", err)
	}
}
