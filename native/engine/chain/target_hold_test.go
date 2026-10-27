package chain

import (
	"blockEmulator/core"
	"math/big"
	"testing"
	"time"
)

func TestTargetHoldSurvivesReopenAndBlocksLateRelay(t *testing.T) {
	source, c, path := preparationFixture(t)
	defer source.CloseBlockChain()
	p, err := source.CommitSourcePreparation(c, 2, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	if _, err = target.CommitPreparedTarget(*p, 1, make([]byte, 32)); err != nil {
		target.CloseBlockChain()
		t.Fatal(err)
	}
	target.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	tx := core.NewTransaction("0000000000000000000000000000000000000031", c.Account, big.NewInt(1), 1, time.Unix(1, 0))
	tx.Relayed = true
	target.SendTx2Pool([]*core.Transaction{tx})
	if got := target.Txpool.PackTxs(10); len(got) != 0 || target.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("late relay escaped restored target hold")
	}
	if target.CheckPreparedTransactions([]*core.Transaction{tx}) == nil {
		t.Fatal("execution bypass not rejected")
	}
	if target.rejectPreparedWrite(c.Account) == nil {
		t.Fatal("direct state mutation not rejected")
	}
}
