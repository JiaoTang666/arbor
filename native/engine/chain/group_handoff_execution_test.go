package chain

import (
	"blockEmulator/core"
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestGroupHandoffExecutionBalancesAndReplay(t *testing.T) {
	source, c, path := groupFixture(t)
	defer source.CloseBlockChain()
	p, e := source.CommitGroupSourcePreparation(c, 2, make([]byte, 32), nil)
	if e != nil {
		t.Fatal(e)
	}
	target := newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	installed, e := target.CommitGroupTargetInstallation(*p, 1, make([]byte, 32))
	if e != nil {
		t.Fatal(e)
	}
	activation := GroupTargetActivation{}
	accounts := []string{}
	for _, m := range p.Members {
		accounts = append(accounts, m.Command.Account)
		activation.Members = append(activation.Members, TargetActivation{TargetShard: 1, ProposalID: m.Command.ProposalID, Account: m.Command.Account, MappingVersion: 1, InstallDigest: installed.CommitDigest, StateHash: m.StateHash})
	}
	tx := core.NewTransaction(accounts[0], accounts[1], big.NewInt(10), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, e := core.BuildGroupMigrationHandoff(tx, c.Members[0].ProposalID, accounts, 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	if target.ValidateMigrationHandoffExecution(*h) == nil {
		t.Fatal("unactivated group executed")
	}
	if e = target.CommitGroupTargetActivation(activation); e != nil {
		t.Fatal(e)
	}
	legacy, e := core.BuildMigrationHandoff(tx, c.Members[0].ProposalID, accounts[0], 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	if target.ValidateMigrationHandoffExecution(*legacy) == nil {
		t.Fatal("singleton bypass")
	}
	record, e := target.CommitMigrationHandoff(*h)
	if e != nil {
		t.Fatal(e)
	}
	if len(record.Relay) != 0 {
		t.Fatal("internal group generated relay")
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	if _, e = target.CommitMigrationHandoff(*h); e != nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("duplicate execution", e)
	}
	a, _ := target.ExportAccountState(accounts[0])
	b, _ := target.ExportAccountState(accounts[1])
	if a.State.Balance.Int64() != 90 || b.State.Balance.Int64() != 110 {
		t.Fatal("wrong debit/credit")
	}
	late := core.NewTransaction("0000000000000000000000000000000000000030", accounts[1], big.NewInt(5), 2, time.Unix(1, 0))
	late.RoutingFrozen = true
	late.Relayed = true
	credit, e := core.BuildGroupMigrationHandoff(late, c.Members[0].ProposalID, accounts, 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = target.CommitMigrationHandoff(*credit); e != nil {
		t.Fatal(e)
	}
	a, _ = target.ExportAccountState(accounts[0])
	b, _ = target.ExportAccountState(accounts[1])
	if a.State.Balance.Int64() != 90 || b.State.Balance.Int64() != 115 {
		t.Fatal("late relay repeated debit or missed credit")
	}
}
