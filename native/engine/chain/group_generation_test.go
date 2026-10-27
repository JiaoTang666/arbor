package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"
)

func TestSixSourceGenerationsRestoreAndServeFirstGenerationLateWork(t *testing.T) {
	source, target, path, _ := serialChainFixture(t)
	var accounts []string
	var states []*core.AccountState
	for i := 0; i < 12; i++ {
		a := fmt.Sprintf("%040x", 0x100+i)
		accounts = append(accounts, a)
		states = append(states, &core.AccountState{Balance: big.NewInt(100)})
		source.Update_PartitionMap(a, 0)
		target.Update_PartitionMap(a, 0)
	}
	source.AddAccounts(accounts, states, 0)
	for r := 0; r < 6; r++ {
		id := fmt.Sprintf("generation-%d", r+1)
		p, e := source.CommitGroupSourcePreparation(serialGroupCommand(t, source, id, accounts[r*2:r*2+2], uint64(r)), uint64(2*r+2), make([]byte, 32), nil)
		if e != nil {
			t.Fatal("prepare", r, e)
		}
		g, e := source.SourceMigrationGeneration()
		if e != nil || g.Generation != uint64(r+1) || g.ActiveProposal != id || g.RetiredGroups != uint64(r) {
			t.Fatal("wrong active slot", g, e)
		}
		a := serialActivate(t, target, p, uint64(2*r+1))
		serialRetire(t, source, target, a, uint64(2*r+3))
		g, e = source.SourceMigrationGeneration()
		if e != nil || g.ActiveProposal != "" || g.RetiredGroups != uint64(r+1) {
			t.Fatal("retirement did not free slot", g, e)
		}
	}
	source.CloseBlockChain()
	target.CloseBlockChain()
	source = newMigrationChain(t, path, 0)
	defer source.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	g, e := source.SourceMigrationGeneration()
	if e != nil || g.Generation != 6 || g.RetiredGroups != 6 || g.ActiveProposal != "" {
		t.Fatal("generation head not recovered", g, e)
	}
	if len(source.GroupSourcePreparations()) != 6 {
		t.Fatal("archive lost")
	}
	tx := core.NewTransaction(accounts[0], accounts[1], big.NewInt(7), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	if source.CheckPreparedTransactions([]*core.Transaction{tx}) == nil {
		t.Fatal("first source fence removed")
	}
	h, e := core.BuildGroupMigrationHandoff(tx, "generation-1", accounts[:2], 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = target.CommitMigrationHandoff(*h); e != nil {
		t.Fatal("late first-generation handoff rejected", e)
	}
	before := append([]byte(nil), target.CurrentBlock.Hash...)
	if _, e = target.CommitMigrationHandoff(*h); e != nil || !bytes.Equal(before, target.CurrentBlock.Hash) {
		t.Fatal("late replay not idempotent", e)
	}
	for i, a := range accounts[:2] {
		state, e := target.ExportAccountState(a)
		want := int64(93)
		if i == 1 {
			want = 107
		}
		if e != nil || state.State.Balance.Int64() != want {
			t.Fatal("late transfer incorrect", a, e)
		}
	}
	// Reusing an account is a separate unsupported ownership transition, not
	// something made safe by freeing the active migration slot.
	if e = source.ValidateGroupSourcePreparation(serialGroupCommand(t, source, "unsafe-repeat", accounts[:2], 6)); e == nil {
		t.Fatal("remigration fence bypassed")
	}
}

func TestLegacyGenerationUpgradeAndCorruptHeadRejection(t *testing.T) {
	source, target, _, accounts := serialChainFixture(t)
	defer source.CloseBlockChain()
	defer target.CloseBlockChain()
	p, e := source.CommitGroupSourcePreparation(serialGroupCommand(t, source, "old", accounts[:2], 0), 2, make([]byte, 32), nil)
	if e != nil {
		t.Fatal(e)
	}
	serialRetire(t, source, target, serialActivate(t, target, p, 1), 3)
	st, e := source.stateTrie()
	if e != nil {
		t.Fatal(e)
	}
	if e = st.Delete(groupGenerationKey); e != nil {
		t.Fatal(e)
	}
	if _, e = source.commitStateTrie(st); e != nil {
		t.Fatal(e)
	}
	g, e := source.SourceMigrationGeneration()
	if e != nil || g.Generation != 1 || g.RetiredGroups != 1 {
		t.Fatal("legacy history not derived", e)
	}
	p, e = source.CommitGroupSourcePreparation(serialGroupCommand(t, source, "new", accounts[2:], 1), 4, make([]byte, 32), nil)
	if e != nil {
		t.Fatal("legacy upgrade", e)
	}
	st, e = source.stateTrie()
	if e != nil {
		t.Fatal(e)
	}
	raw, e := st.Get(groupGenerationKey)
	if e != nil || len(raw) == 0 {
		t.Fatal("upgrade head missing", e)
	}
	var head SourceGeneration
	if e = json.Unmarshal(raw, &head); e != nil {
		t.Fatal(e)
	}
	head.ActiveProposal = ""
	head.RetiredGroups = 2
	if e = writeSourceGeneration(st, head); e != nil {
		t.Fatal(e)
	}
	if _, e = sourceGenerationAt(st, source.GroupSourcePreparations()); e == nil {
		t.Fatal("forged retired head accepted")
	}
	// Commit corruption ONLY into this test's temporary chain, then exercise
	// the real startup restore hook: it must fail, never fabricate a free slot.
	if _, e = source.commitStateTrie(st); e != nil {
		t.Fatal(e)
	}
	if e = source.restoreGroupSourcePreparation(); e == nil {
		t.Fatal("startup accepted corrupt generation head")
	}
}
