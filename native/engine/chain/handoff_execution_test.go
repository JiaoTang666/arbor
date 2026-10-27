package chain

import (
	"blockEmulator/core"
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestHandoffExecutesOnceAcrossReopen(t *testing.T) {
	source, c, path := preparationFixture(t)
	defer source.CloseBlockChain()
	p, err := source.CommitSourcePreparation(c, 2, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	installed, err := target.CommitPreparedTarget(*p, 1, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	recipient := "0000000000000000000000000000000000000031"
	target.Update_PartitionMap(recipient, 1)
	target.AddAccounts([]string{recipient}, []*core.AccountState{{Balance: big.NewInt(10)}}, 0)
	if err = target.CommitTargetActivation(TargetActivation{TargetShard: 1, ProposalID: c.ProposalID, Account: c.Account, MappingVersion: 1, InstallDigest: installed.CommitDigest, StateHash: p.StateHash}); err != nil {
		t.Fatal(err)
	}
	tx := core.NewTransaction(c.Account, recipient, big.NewInt(7), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.RoutedSenderShard = 0
	tx.RoutedRecipientShard = 1
	h, err := core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	record, err := target.CommitMigrationHandoff(*h)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Relay) != 0 {
		t.Fatal("local credit produced relay")
	}
	sender, _ := target.ExportAccountState(c.Account)
	received, _ := target.ExportAccountState(recipient)
	if sender.State.Balance.Int64() != 93 || received.State.Balance.Int64() != 17 {
		t.Fatal("wrong debit/credit")
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	target.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	if _, err = target.CommitMigrationHandoff(*h); err != nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("replay performed another write")
	}
	sender, _ = target.ExportAccountState(c.Account)
	received, _ = target.ExportAccountState(recipient)
	if sender.State.Balance.Int64() != 93 || received.State.Balance.Int64() != 17 {
		t.Fatal("duplicate transaction executed")
	}
}
