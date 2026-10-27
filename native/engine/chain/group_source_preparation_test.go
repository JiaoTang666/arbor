package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/hex"
	"math/big"
	"testing"
	"time"
)

func groupFixture(t *testing.T) (*BlockChain, core.GroupPrepareCommand, string) {
	t.Helper()
	bc, c, path := preparationFixture(t)
	d := c
	d.Account = "0000000000000000000000000000000000000020"
	state, err := bc.ExportAccountState(d.Account)
	if err != nil {
		t.Fatal(err)
	}
	d.ExpectedStateHash = hex.EncodeToString(state.StateHash)
	return bc, core.GroupPrepareCommand{Members: []core.SourcePrepareCommand{c, d}}, path
}

func TestGroupSourceAtomicFailureAndRetry(t *testing.T) {
	bc, c, _ := groupFixture(t)
	defer bc.CloseBlockChain()
	head := append([]byte(nil), bc.CurrentBlock.Hash...)
	bad := core.GroupPrepareCommand{Members: append([]core.SourcePrepareCommand(nil), c.Members...)}
	bad.Members[1].ExpectedStateHash = "bad"
	if _, err := bc.CommitGroupSourcePreparation(bad, 2, make([]byte, 32), nil); err == nil {
		t.Fatal("invalid last member accepted")
	}
	if !bytes.Equal(head, bc.CurrentBlock.Hash) || bc.GroupSourcePreparation() != nil {
		t.Fatal("partial commit after invalid last member")
	}
	tx := core.NewTransaction(c.Members[0].Account, c.Members[1].Account, big.NewInt(1), 1, time.Unix(1, 0))
	bc.SendTx2Pool([]*core.Transaction{tx})
	if len(bc.GenerateBlock(0).Body) != 1 {
		t.Fatal("failed preparation leaked hold")
	}
	p, err := bc.CommitGroupSourcePreparation(c, 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Members) != 2 || p.PrepareBlock != bc.CurrentBlock.Header.Number {
		t.Fatal("not one complete committed group")
	}
	head = append([]byte(nil), bc.CurrentBlock.Hash...)
	if _, err = bc.CommitGroupSourcePreparation(c, 3, make([]byte, 32), nil); err != nil || !bytes.Equal(head, bc.CurrentBlock.Hash) {
		t.Fatal("duplicate not idempotent", err)
	}
	if bc.CheckPreparedTransactions([]*core.Transaction{tx}) == nil {
		t.Fatal("group fence missing")
	}
	if err = bc.ValidateSourcePreparation(c.Members[0]); err == nil {
		t.Fatal("legacy command admitted during group")
	}
}

func TestGroupSourceAllMembersHeldAndRestart(t *testing.T) {
	bc, c, path := groupFixture(t)
	if _, err := bc.CommitGroupSourcePreparation(c, 2, make([]byte, 32), nil); err != nil {
		t.Fatal(err)
	}
	other := "0000000000000000000000000000000000000030"
	check := func(chain *BlockChain) {
		for i, m := range c.Members {
			tx := core.NewTransaction(m.Account, other, big.NewInt(1), uint64(i+1), time.Unix(1, 0))
			chain.SendTx2Pool([]*core.Transaction{tx})
			state, err := chain.ExportAccountState(m.Account)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = chain.ImportAccountState(state); err == nil {
				t.Fatal("import bypasses group fence")
			}
		}
		independent := core.NewTransaction(other, other, big.NewInt(1), 9, time.Unix(1, 0))
		chain.SendTx2Pool([]*core.Transaction{independent})
		b := chain.GenerateBlock(0)
		if len(b.Body) != 1 || b.Body[0] != independent {
			t.Fatal("wrong held or independent transaction behavior")
		}
		chain.AddBlock(b)
	}
	check(bc)
	bc.CloseBlockChain()
	recovered := newMigrationChain(t, path, 0)
	defer recovered.CloseBlockChain()
	if p := recovered.GroupSourcePreparation(); p == nil || !sameGroupCommand(p.Command, c) {
		t.Fatal("group lost on restart")
	}
	check(recovered)
}

func TestGroupSourceRejectsMixedAndDuplicateMembers(t *testing.T) {
	bc, c, _ := groupFixture(t)
	defer bc.CloseBlockChain()
	for name, mutate := range map[string]func(*core.GroupPrepareCommand){
		"duplicate": func(x *core.GroupPrepareCommand) { x.Members[1] = x.Members[0] },
		"proposal":  func(x *core.GroupPrepareCommand) { x.Members[1].ProposalID = "other" },
		"version":   func(x *core.GroupPrepareCommand) { x.Members[1].MappingVersion++ },
		"snapshot":  func(x *core.GroupPrepareCommand) { x.Members[1].ExpectedStateRoot = "other" },
		"target":    func(x *core.GroupPrepareCommand) { x.Members[1].TargetShard = 0 },
	} {
		x := core.GroupPrepareCommand{Members: append([]core.SourcePrepareCommand(nil), c.Members...)}
		mutate(&x)
		if err := bc.ValidateGroupSourcePreparation(x); err == nil {
			t.Fatal("accepted", name)
		}
	}
}

func TestGroupSourceRejectsCachedRootAndMappingBypass(t *testing.T) {
	bc, c, _ := groupFixture(t)
	defer bc.CloseBlockChain()
	oldRoot := append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
	if _, err := bc.CommitGroupSourcePreparation(c, 2, make([]byte, 32), nil); err != nil {
		t.Fatal(err)
	}
	if err := bc.checkPreparedRoot(oldRoot); err == nil {
		t.Fatal("old root omitted group marker")
	}
	p := bc.GroupSourcePreparation()
	p.Members[0].State[0] ^= 1
	p.Command.Members[0].Account = "bad"
	if !sameGroupCommand(bc.GroupSourcePreparation().Command, c) {
		t.Fatal("external mutation of cached group")
	}
	if err := bc.checkPreparedRoot(bc.CurrentBlock.Header.StateRoot); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("group mapping bypass accepted")
		}
	}()
	bc.Update_PartitionMap(c.Members[1].Account, 1)
}
