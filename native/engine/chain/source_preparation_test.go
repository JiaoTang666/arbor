package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"encoding/hex"
	"math/big"
	"path/filepath"
	"testing"
	"time"
)

func preparationFixture(t *testing.T) (*BlockChain, core.SourcePrepareCommand, string) {
	t.Helper()
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	})
	t.Setenv("SAA_INTEGRATED_CONTROLLER", "forecast")
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "database") + "/"
	params.ShardNum = 2
	bc := newMigrationChain(t, params.DatabaseWrite_path, 0)
	a, b, c := "0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020", "0000000000000000000000000000000000000030"
	for _, account := range []string{a, b, c} {
		bc.Update_PartitionMap(account, 0)
	}
	bc.AddAccounts([]string{a, b, c}, []*core.AccountState{{Balance: big.NewInt(100)}, {Balance: big.NewInt(100)}, {Balance: big.NewInt(100)}}, 0)
	state, err := bc.ExportAccountState(a)
	if err != nil {
		t.Fatal(err)
	}
	return bc, core.SourcePrepareCommand{ProposalID: "p", Account: a, SourceShard: 0, TargetShard: 1,
		ExpectedBlockHash: hex.EncodeToString(bc.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(state.SourceRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)}, params.DatabaseWrite_path
}

func TestPreparedSourceStateStableWhileOtherAccountsProgress(t *testing.T) {
	bc, c, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	peer := "0000000000000000000000000000000000000020"
	other := "0000000000000000000000000000000000000030"
	earlier := core.NewTransaction(c.Account, peer, big.NewInt(7), 1, time.Unix(1, 0))
	bc.SendTx2Pool([]*core.Transaction{earlier})
	bc.AddBlock(bc.GenerateBlock(0))
	if err := bc.ValidateSourcePreparation(c); err == nil {
		t.Fatal("stale pre-commit snapshot accepted")
	}
	state, _ := bc.ExportAccountState(c.Account)
	c.ExpectedBlockHash = hex.EncodeToString(bc.CurrentBlock.Hash)
	c.ExpectedStateRoot = hex.EncodeToString(state.SourceRoot)
	c.ExpectedStateHash = hex.EncodeToString(state.StateHash)
	p, err := bc.CommitSourcePreparation(c, 3, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	if core.DecodeAS(p.State).Balance.Int64() != 93 {
		t.Fatal("earlier committed transaction omitted")
	}
	head := append([]byte(nil), bc.CurrentBlock.Hash...)
	if _, err = bc.CommitSourcePreparation(c, 4, make([]byte, 32)); err != nil || !bytes.Equal(head, bc.CurrentBlock.Hash) {
		t.Fatal("duplicate preparation changed chain")
	}
	held := core.NewTransaction(c.Account, peer, big.NewInt(5), 2, time.Unix(2, 0))
	lateRelay := core.NewTransaction(other, c.Account, big.NewInt(8), 3, time.Unix(3, 0))
	lateRelay.Relayed = true
	unrelated := core.NewTransaction(peer, other, big.NewInt(1), 4, time.Unix(4, 0))
	bc.SendTx2Pool([]*core.Transaction{held, lateRelay, unrelated})
	b := bc.GenerateBlock(0)
	if len(b.Body) != 1 || b.Body[0] != unrelated {
		t.Fatal("wrong transactions admitted after preparation")
	}
	bc.AddBlock(b)
	after, _ := bc.ExportAccountState(c.Account)
	if hex.EncodeToString(after.StateHash) != p.StateHash || bc.Txpool.GetTxQueueLen() != 2 || bc.Get_PartitionMap(c.Account) != 0 {
		t.Fatal("prepared source drift or queue loss")
	}
	if bc.CheckPreparedTransactions([]*core.Transaction{held}) == nil {
		t.Fatal("commit admission guard absent")
	}
	if _, err = bc.ImportAccountState(after); err == nil {
		t.Fatal("import bypassed fence")
	}
}

func TestPreparedSourceFenceRestoresAfterRestart(t *testing.T) {
	bc, c, path := preparationFixture(t)
	if _, err := bc.CommitSourcePreparation(c, 2, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	bc.CloseBlockChain()
	recovered := newMigrationChain(t, path, 0)
	defer recovered.CloseBlockChain()
	if p := recovered.SourcePreparation(); p == nil || p.Command != c {
		t.Fatal("preparation lost on restart")
	}
	tx := core.NewTransaction(c.Account, "0000000000000000000000000000000000000020", big.NewInt(1), 1, time.Unix(1, 0))
	recovered.SendTx2Pool([]*core.Transaction{tx})
	if len(recovered.GenerateBlock(0).Body) != 0 || recovered.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("recovered fence did not hold new arrivals")
	}
}

func TestPreparedSourceRejectsConflictAndWrongSnapshot(t *testing.T) {
	bc, c, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	for name, mutate := range map[string]func(*core.SourcePrepareCommand){
		"source": func(x *core.SourcePrepareCommand) { x.SourceShard = 1 },
		"target": func(x *core.SourcePrepareCommand) { x.TargetShard = 0 },
		"hash":   func(x *core.SourcePrepareCommand) { x.ExpectedStateHash = "bad" },
		"block":  func(x *core.SourcePrepareCommand) { x.ExpectedBlockHash = "bad" },
	} {
		bad := c
		mutate(&bad)
		if bc.ValidateSourcePreparation(bad) == nil {
			t.Fatalf("accepted %s", name)
		}
	}
	if _, err := bc.CommitSourcePreparation(c, 2, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	other := c
	other.ProposalID = "other"
	if bc.ValidateSourcePreparation(other) == nil {
		t.Fatal("conflicting proposal accepted")
	}
}

func TestPreparedSourceRejectsCachedRootBypass(t *testing.T) {
	bc, c, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	tx := core.NewTransaction(c.Account, "0000000000000000000000000000000000000020", big.NewInt(1), 1, time.Unix(1, 0))
	bc.SendTx2Pool([]*core.Transaction{tx})
	oldBlock := bc.GenerateBlock(0)
	if _, err := bc.CommitSourcePreparation(c, 2, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if bc.IsValidBlock(oldBlock) == nil {
		t.Fatal("preparation bypass block admitted")
	}
	// The error-returning commit path must reject without advancing the head.
	head := bc.CurrentBlock
	if err := bc.AddBlock(oldBlock); err == nil {
		t.Fatal("commit bypass did not fail closed")
	}
	if bc.CurrentBlock != head {
		t.Fatal("rejected commit advanced the head")
	}
}
