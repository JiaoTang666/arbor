package chain

import (
	"blockEmulator/core"
	"fmt"
	"math/big"
	"testing"
)

// The grouped protocol is the same transaction/state protocol for one member;
// no dummy account or synthetic migration state is needed to fill a batch.
func TestSingletonMigrationGenerationsCommitRetireAndRestore(t *testing.T) {
	source, target, path, accounts := serialChainFixture(t)
	for i, account := range accounts[:2] {
		id := fmt.Sprintf("singleton-%d", i)
		p, err := source.CommitGroupSourcePreparation(serialGroupCommand(t, source, id, []string{account}, uint64(i)), uint64(i*2+2), make([]byte, 32), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Members) != 1 {
			t.Fatal("singleton unexpectedly padded")
		}
		serialRetire(t, source, target, serialActivate(t, target, p, uint64(i*2+1)), uint64(i*2+3))
	}
	source.CloseBlockChain()
	target.CloseBlockChain()
	source = newMigrationChain(t, path, 0)
	defer source.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	g, err := source.SourceMigrationGeneration()
	if err != nil || g.Generation != 2 || g.RetiredGroups != 2 || g.ActiveProposal != "" {
		t.Fatal("singleton generation restart mismatch", g, err)
	}
	for _, account := range accounts[:2] {
		if ok, err := target.TargetActivated(account); err != nil || !ok {
			t.Fatal("singleton activation missing", err)
		}
	}
}

func TestNewCommittedAccountCanEnterMigrationGroup(t *testing.T) {
	source, target, _, _ := serialChainFixture(t)
	defer source.CloseBlockChain()
	defer target.CloseBlockChain()
	account := "0000000000000000000000000000000000000098"
	if _, err := source.ExportAccountState(account); err == nil {
		t.Fatal("absent account synthesized")
	}
	// This unit setup commits an account after chain initialization. Production
	// obtains the same actual trie record from completed native transactions.
	source.Update_PartitionMap(account, 0)
	target.Update_PartitionMap(account, 0)
	source.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(37)}}, 0)
	p, err := source.CommitGroupSourcePreparation(serialGroupCommand(t, source, "late-account", []string{account}, 0), 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	serialRetire(t, source, target, serialActivate(t, target, p, 1), 3)
	state, err := target.ExportAccountState(account)
	if err != nil || state.State.Balance.Int64() != 37 {
		t.Fatal("late account state not preserved", err)
	}
}
