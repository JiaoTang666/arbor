package chain

import (
	"blockEmulator/core"
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestMigratedOutgoingRelayConservesBalanceAndDeduplicates(t *testing.T) {
	source, c, path := preparationFixture(t)
	defer source.CloseBlockChain()
	p, err := source.CommitSourcePreparation(c, 2, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	installed, err := target.CommitPreparedTarget(*p, 1, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	if err = target.CommitTargetActivation(TargetActivation{TargetShard: 1, ProposalID: c.ProposalID, Account: c.Account, MappingVersion: 1, InstallDigest: installed.CommitDigest, StateHash: p.StateHash}); err != nil {
		t.Fatal(err)
	}
	recipient := "0000000000000000000000000000000000000020"
	tx := core.NewTransaction(c.Account, recipient, big.NewInt(7), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, err := core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	debit, err := target.CommitMigrationHandoff(*h)
	if err != nil {
		t.Fatal(err)
	}
	if len(debit.Relay) == 0 {
		t.Fatal("missing durable relay")
	}
	if _, err = source.CommitHandoffRelay(*debit); err != nil {
		t.Fatal(err)
	}
	sender, _ := target.ExportAccountState(c.Account)
	receiver, _ := source.ExportAccountState(recipient)
	frozen, _ := source.ExportAccountState(c.Account)
	if sender.State.Balance.Int64() != 93 || receiver.State.Balance.Int64() != 107 || frozen.State.Balance.Int64() != 100 {
		t.Fatal("incorrect two-leg balances")
	}
	head := append([]byte(nil), source.CurrentBlock.Hash...)
	if _, err = source.CommitHandoffRelay(*debit); err != nil || !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("duplicate credit executed")
	}
}
