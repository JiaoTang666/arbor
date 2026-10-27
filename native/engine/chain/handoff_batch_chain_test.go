package chain

import (
	"blockEmulator/core"
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestHandoffBatchCumulativeAndPrefixRecovery(t *testing.T) {
	source, c, path := preparationFixture(t)
	defer source.CloseBlockChain()
	p, e := source.CommitSourcePreparation(c, 2, make([]byte, 32))
	if e != nil {
		t.Fatal(e)
	}
	target := newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	target.ChainConfig.BlockSize = 1000
	installed, e := target.CommitPreparedTarget(*p, 1, make([]byte, 32))
	if e != nil {
		t.Fatal(e)
	}
	recipient := "0000000000000000000000000000000000000031"
	target.Update_PartitionMap(recipient, 1)
	target.AddAccounts([]string{recipient}, []*core.AccountState{{Balance: big.NewInt(10)}}, 0)
	if e = target.CommitTargetActivation(TargetActivation{TargetShard: 1, ProposalID: c.ProposalID, Account: c.Account, MappingVersion: 1, InstallDigest: installed.CommitDigest, StateHash: p.StateHash}); e != nil {
		t.Fatal(e)
	}
	makeH := func(value int64, n uint64) core.MigrationHandoff {
		tx := core.NewTransaction(c.Account, recipient, big.NewInt(value), n, time.Unix(1, 0))
		tx.RoutingFrozen = true
		tx.RoutedSenderShard = 0
		tx.RoutedRecipientShard = 1
		tx.IngressSequence = n
		h, e := core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, 0, 1, 1)
		if e != nil {
			t.Fatal(e)
		}
		return *h
	}
	before := append([]byte(nil), target.CurrentBlock.Hash...)
	if e = target.ValidateMigrationHandoffBatch([]core.MigrationHandoff{makeH(60, 1), makeH(60, 2)}); e == nil {
		t.Fatal("cumulative overdraft accepted")
	}
	if !bytes.Equal(before, target.CurrentBlock.Hash) {
		t.Fatal("validation mutated state")
	}
	hs := []core.MigrationHandoff{makeH(7, 1), makeH(8, 2)}
	if e = target.ValidateMigrationHandoffBatch(hs); e != nil {
		t.Fatal(e)
	}
	if e = target.ValidateMigrationHandoffBatch([]core.MigrationHandoff{hs[0], hs[0]}); e == nil {
		t.Fatal("duplicate batch identity accepted")
	}
	if _, e = target.CommitMigrationHandoff(hs[0]); e != nil {
		t.Fatal(e)
	}
	// A committed prefix survives interruption: retry validates and skips its marker.
	if e = target.ValidateMigrationHandoffBatch(hs); e != nil {
		t.Fatal(e)
	}
	for _, h := range hs {
		if _, e = target.CommitMigrationHandoff(h); e != nil {
			t.Fatal(e)
		}
	}
	state, _ := target.ExportAccountState(c.Account)
	if state.State.Balance.Int64() != 85 {
		t.Fatal("prefix double executed")
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	for _, h := range hs {
		if _, e = target.CommitMigrationHandoff(h); e != nil {
			t.Fatal(e)
		}
	}
	if !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("replay changed head")
	}
}
