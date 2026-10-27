package chain

import (
	"blockEmulator/core"
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestTargetActivationReleasesOnlyAfterMatchingCommand(t *testing.T) {
	source, c, path := preparationFixture(t)
	defer source.CloseBlockChain()
	p, err := source.CommitSourcePreparation(c, 2, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	installed, err := target.CommitPreparedTarget(*p, 1, make([]byte, 32))
	if err != nil {
		target.CloseBlockChain()
		t.Fatal(err)
	}
	tx := core.NewTransaction(c.Account, "0000000000000000000000000000000000000031", big.NewInt(1), 1, time.Unix(1, 0))
	target.SendTx2Pool([]*core.Transaction{tx})
	command := TargetActivation{ProposalID: c.ProposalID, Account: c.Account, MappingVersion: 1, InstallDigest: installed.CommitDigest, StateHash: p.StateHash}
	bad := command
	bad.StateHash = "bad"
	if target.CommitTargetActivation(bad) == nil {
		t.Fatal("bad activation accepted")
	}
	if len(target.Txpool.PackTxs(1)) != 0 {
		t.Fatal("bad command released queue")
	}
	if err = target.CommitTargetActivation(command); err != nil {
		t.Fatal(err)
	}
	if target.CheckPreparedTransactions([]*core.Transaction{tx}) != nil || target.Get_PartitionMap(c.Account) != 1 {
		t.Fatal("valid activation not effective")
	}
	if got := target.Txpool.PackTxs(1); len(got) != 1 || got[0] != tx {
		t.Fatal("queued transaction lost")
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	if err = target.CommitTargetActivation(command); err != nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("activation replay not idempotent")
	}
	target.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	if target.Get_PartitionMap(c.Account) != 1 || target.CheckPreparedTransactions([]*core.Transaction{tx}) != nil {
		t.Fatal("activation not restored")
	}
	if source.CheckPreparedTransactions([]*core.Transaction{tx}) == nil {
		t.Fatal("source regained execution rights")
	}
}
