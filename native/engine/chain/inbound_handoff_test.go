package chain

import (
	"blockEmulator/core"
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestInboundDebitThenMigratedCredit(t *testing.T) {
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
	sender := "0000000000000000000000000000000000000020"
	tx := core.NewTransaction(sender, c.Account, big.NewInt(9), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	record, err := source.CommitInboundDebit(tx, 1)
	if err != nil {
		t.Fatal(err)
	}
	debited, _ := source.ExportAccountState(sender)
	frozen, _ := source.ExportAccountState(c.Account)
	if debited.State.Balance.Int64() != 91 || frozen.State.Balance.Int64() != 100 {
		t.Fatal("source debit touched wrong account")
	}
	head := append([]byte(nil), source.CurrentBlock.Hash...)
	if _, err = source.CommitInboundDebit(tx, 1); err != nil || !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("debit repeated")
	}
	if _, err = target.CommitMigrationHandoff(record.Credit); err != nil {
		t.Fatal(err)
	}
	credited, _ := target.ExportAccountState(c.Account)
	if credited.State.Balance.Int64() != 109 {
		t.Fatal("target credit missing")
	}
}
