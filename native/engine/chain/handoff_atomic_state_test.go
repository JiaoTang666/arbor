package chain

import (
	"blockEmulator/core"
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestAtomicHandoffBatchOneRootAndReopen(t *testing.T) {
	source, c, path := preparationFixture(t)
	defer source.CloseBlockChain()
	p, e := source.CommitSourcePreparation(c, 2, make([]byte, 32))
	if e != nil {
		t.Fatal(e)
	}
	target := newMigrationChain(t, path, 1)
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
	makeH := func(value int64, n uint64, to string, sh uint64) core.MigrationHandoff {
		tx := core.NewTransaction(c.Account, to, big.NewInt(value), n, time.Unix(1, 0))
		tx.RoutingFrozen = true
		tx.RoutedSenderShard = 0
		tx.RoutedRecipientShard = sh
		tx.IngressSequence = n
		h, e := core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, 0, 1, 1)
		if e != nil {
			t.Fatal(e)
		}
		return *h
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	bad := []core.MigrationHandoff{makeH(60, 1, recipient, 1), makeH(60, 2, recipient, 1)}
	if e = target.CommitMigrationHandoffBatch(bad); e == nil {
		t.Fatal("overdraft committed")
	}
	if !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("failed batch committed prefix")
	}
	st, _ := target.stateTrie()
	if raw, _ := st.Get(handoffExecutionKey(bad[0].ID)); len(raw) != 0 {
		t.Fatal("failed batch left marker")
	}
	hs := []core.MigrationHandoff{makeH(7, 1, recipient, 1), makeH(8, 2, recipient, 1), makeH(1, 3, c.Account, 0), makeH(2, 4, "0000000000000000000000000000000000000040", 0)}
	height := target.CurrentBlock.Header.Number
	if e = target.CommitMigrationHandoffBatch(hs); e != nil {
		t.Fatal(e)
	}
	if target.CurrentBlock.Header.Number != height+1 {
		t.Fatal("batch created more than one block")
	}
	sender, _ := target.ExportAccountState(c.Account)
	credit, _ := target.ExportAccountState(recipient)
	if sender.State.Balance.Int64() != 83 || credit.State.Balance.Int64() != 25 {
		t.Fatal("wrong sequential balances")
	}
	for i, h := range hs {
		rec, e := target.CommitMigrationHandoff(h)
		if e != nil || rec.Block != height+1 {
			t.Fatal("per-item marker not in batch block", e)
		}
		if (len(rec.Relay) > 0) != (i == 3) {
			t.Fatal("relay marker mismatch")
		}
	}
	head = append([]byte(nil), target.CurrentBlock.Hash...)
	target.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	target.ChainConfig.BlockSize = 1000
	if e = target.CommitMigrationHandoffBatch(hs); e != nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("restart replay changed state", e)
	}
}
