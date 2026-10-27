package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"encoding/json"
	"math/big"
	"os"
	"testing"
	"time"
)

func activatedLazyFixture(t *testing.T) (*BlockChain, *BlockChain, core.SourcePrepareCommand) {
	t.Helper()
	source, command, path := preparationFixture(t)
	t.Cleanup(source.CloseBlockChain)
	p, err := source.CommitSourcePreparation(command, 2, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	t.Cleanup(target.CloseBlockChain)
	installed, err := target.CommitPreparedTarget(*p, 1, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	if err = target.CommitTargetActivation(TargetActivation{TargetShard: 1, ProposalID: command.ProposalID, Account: command.Account, MappingVersion: 1, InstallDigest: installed.CommitDigest, StateHash: p.StateHash}); err != nil {
		t.Fatal(err)
	}
	return source, target, command
}

func TestLazyInboundNewSenderAndRequiredExisting(t *testing.T) {
	source, target, c := activatedLazyFixture(t)
	a := "f93b779a96e54868d9cbe6f64351015c97b67b02"
	tx := core.NewTransaction(a, c.Account, big.NewInt(9), 10, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.SenderRequiresExistingState = true
	if source.ValidateInboundDebit(tx, 1) == nil {
		t.Fatal("required sender recreated")
	}
	tx.SenderRequiresExistingState = false
	head := append([]byte(nil), source.CurrentBlock.Hash...)
	if err := source.ValidateInboundDebit(tx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExportAccountState(a); err == nil || !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("validation wrote state")
	}
	r, err := source.CommitInboundDebit(tx, 1)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := source.ExportAccountState(a)
	want := new(big.Int).Sub(new(big.Int).Set(params.Init_Balance), tx.Value)
	if s.State.Balance.Cmp(want) != 0 || s.State.Nonce != 1 {
		t.Fatal("wrong lazy debit")
	}
	head = append([]byte(nil), source.CurrentBlock.Hash...)
	if _, err = source.CommitInboundDebit(tx, 1); err != nil || !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("duplicate debit")
	}
	if _, err = target.CommitMigrationHandoff(r.Credit); err != nil {
		t.Fatal(err)
	}
	s, _ = target.ExportAccountState(c.Account)
	if s.State.Balance.Int64() != 109 {
		t.Fatal("missing migrated credit")
	}
}

func TestLazyLocalHandoffBatchSharedNewRecipient(t *testing.T) {
	_, target, c := activatedLazyFixture(t)
	a := "f93b779a96e54868d9cbe6f64351015c97b67b02"
	var batch []core.MigrationHandoff
	for i := 0; i < 2; i++ {
		tx := core.NewTransaction(c.Account, a, big.NewInt(7), uint64(i+1), time.Unix(1, 0))
		tx.RoutingFrozen = true
		tx.RoutedRecipientShard = 1
		h, err := core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, 0, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		batch = append(batch, *h)
	}
	if err := target.ValidateMigrationHandoffBatch(batch); err != nil {
		t.Fatal(err)
	}
	if _, err := target.ExportAccountState(a); err == nil {
		t.Fatal("batch validation created state")
	}
	for _, h := range batch {
		if _, err := target.CommitMigrationHandoff(h); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := target.ExportAccountState(a)
	want := new(big.Int).Add(new(big.Int).Set(params.Init_Balance), big.NewInt(14))
	if s.State.Balance.Cmp(want) != 0 {
		t.Fatal("initial balance duplicated in batch")
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	for _, h := range batch {
		if _, err := target.CommitMigrationHandoff(h); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("batch replay changed state")
	}
}

// Replay the exact durable Relay payload responsible for 1,250 rejected
// proposals in round4_fix2, without opening or mutating its original database.
func TestLazyRelayRealFailureArtifact(t *testing.T) {
	raw, err := os.ReadFile("testdata/round4_missing_recipient.relay.json")
	if err != nil {
		t.Fatal(err)
	}
	var first HandoffExecution
	if err = json.Unmarshal(raw, &first); err != nil {
		t.Fatal(err)
	}
	tx := core.DecodeTx(first.Relay)
	if tx.Recipient != "f93b779a96e54868d9cbe6f64351015c97b67b02" || tx.RecipientRequiresExistingState {
		t.Fatal("unexpected original contract")
	}
	bc := newMigrationChain(t, t.TempDir(), 5)
	// This retained artifact came from a 16-shard run. The generic helper's
	// two-shard default cannot describe shard 5 or its actual frozen routes.
	bc.ChainConfig.ShardNums = 16
	t.Cleanup(bc.CloseBlockChain)
	head := append([]byte(nil), bc.CurrentBlock.Hash...)
	if err = bc.ValidateHandoffRelay(first); err != nil {
		t.Fatal(err)
	}
	if _, err = bc.ExportAccountState(tx.Recipient); err == nil || !bytes.Equal(head, bc.CurrentBlock.Hash) {
		t.Fatal("validation wrote state")
	}
	if _, err = bc.CommitHandoffRelay(first); err != nil {
		t.Fatal(err)
	}
	s, err := bc.ExportAccountState(tx.Recipient)
	if err != nil {
		t.Fatal(err)
	}
	want := new(big.Int).Add(new(big.Int).Set(params.Init_Balance), tx.Value)
	if s.State.Balance.Cmp(want) != 0 {
		t.Fatal("wrong lazy relay credit")
	}
	head = append([]byte(nil), bc.CurrentBlock.Hash...)
	if _, err = bc.CommitHandoffRelay(first); err != nil || !bytes.Equal(head, bc.CurrentBlock.Hash) {
		t.Fatal("duplicate relay credit")
	}
}

func TestLazyHandoffRequiredStateAndBalanceRemainStrict(t *testing.T) {
	_, target, c := activatedLazyFixture(t)
	a := "f93b779a96e54868d9cbe6f64351015c97b67b02"
	tx := core.NewTransaction(c.Account, a, big.NewInt(101), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.RoutedRecipientShard = 1
	h, err := core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if target.ValidateMigrationHandoffExecution(*h) == nil {
		t.Fatal("insufficient migrated balance accepted")
	}
	tx.Value = big.NewInt(7)
	tx.RecipientRequiresExistingState = true
	h, err = core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if target.ValidateMigrationHandoffExecution(*h) == nil {
		t.Fatal("missing required recipient accepted")
	}
	if _, err := target.ExportAccountState(a); err == nil {
		t.Fatal("failed validation created state")
	}
}
