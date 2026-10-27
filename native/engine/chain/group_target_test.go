package chain

import (
	"bytes"
	"testing"
)

func TestGroupTargetAtomicInstallActivateAndRestart(t *testing.T) {
	source, c, path := groupFixture(t)
	defer source.CloseBlockChain()
	p, err := source.CommitGroupSourcePreparation(c, 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	for _, m := range c.Members {
		target.Update_PartitionMap(m.Account, 0)
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	bad := cloneGroup(p)
	bad.Members[1].State = []byte("corrupt")
	if _, err = target.CommitGroupTargetInstallation(*bad, 1, make([]byte, 32)); err == nil {
		t.Fatal("corrupt second member accepted")
	}
	if !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("partial target commit")
	}
	if state, err := target.TargetInstallation(c.Members[0].Account); err != nil || state != nil {
		t.Fatal("first member leaked")
	}
	record, err := target.CommitGroupTargetInstallation(*p, 1, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	if target.CurrentBlock.Header.Number != record.InstallBlock {
		t.Fatal("group installation not one block")
	}
	head = append([]byte(nil), target.CurrentBlock.Hash...)
	if _, err = target.CommitGroupTargetInstallation(*p, 1, make([]byte, 32)); err != nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("non-idempotent target install", err)
	}
	activation := GroupTargetActivation{}
	for _, m := range p.Members {
		state, err := target.ExportAccountState(m.Command.Account)
		if err != nil || !bytes.Equal(state.EncodedState, m.State) {
			t.Fatal("wrong installed state", err)
		}
		activation.Members = append(activation.Members, TargetActivation{TargetShard: 1, ProposalID: m.Command.ProposalID, Account: m.Command.Account, MappingVersion: 1, InstallDigest: record.CommitDigest, StateHash: m.StateHash})
	}
	if err = target.CommitTargetActivation(activation.Members[0]); err == nil {
		t.Fatal("single activation bypassed group")
	}
	if err = target.CommitGroupTargetActivation(GroupTargetActivation{Members: activation.Members[:1]}); err == nil {
		t.Fatal("partial activation accepted")
	}
	if !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("rejected activation mutated chain")
	}
	target.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	for _, m := range activation.Members {
		active, err := target.TargetActivated(m.Account)
		if err != nil || active {
			t.Fatal("restart prematurely activated member")
		}
	}
	if err = target.CommitGroupTargetActivation(activation); err != nil {
		t.Fatal(err)
	}
	head = append([]byte(nil), target.CurrentBlock.Hash...)
	if err = target.CommitGroupTargetActivation(activation); err != nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("non-idempotent group activation", err)
	}
	target.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	for _, m := range activation.Members {
		active, err := target.TargetActivated(m.Account)
		if err != nil || !active || target.Get_PartitionMap(m.Account) != 1 {
			t.Fatal("group activation recovery failed", err)
		}
	}
}

func TestGroupTargetRejectsMixedVersionActivation(t *testing.T) {
	source, c, path := groupFixture(t)
	defer source.CloseBlockChain()
	p, err := source.CommitGroupSourcePreparation(c, 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	record, err := target.CommitGroupTargetInstallation(*p, 1, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	a := GroupTargetActivation{}
	for _, m := range p.Members {
		a.Members = append(a.Members, TargetActivation{TargetShard: 1, ProposalID: m.Command.ProposalID, Account: m.Command.Account, MappingVersion: 1, InstallDigest: record.CommitDigest, StateHash: m.StateHash})
	}
	a.Members[1].MappingVersion = 2
	if err = target.CommitGroupTargetActivation(a); err == nil {
		t.Fatal("mixed generation activation accepted")
	}
	for _, m := range a.Members {
		active, err := target.TargetActivated(m.Account)
		if err != nil || active {
			t.Fatal("partial activation")
		}
	}
}
