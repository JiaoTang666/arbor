package chain

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestPreparedTargetAtomicIdentityAndIdempotence(t *testing.T) {
	source, c, path := preparationFixture(t)
	defer source.CloseBlockChain()
	p, err := source.CommitSourcePreparation(c, 2, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	target.Update_PartitionMap(c.Account, 0)
	record, err := target.CommitPreparedTarget(*p, 1, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	installed, err := target.ExportAccountState(c.Account)
	if err != nil || !bytes.Equal(installed.State.Encode(), p.State) {
		t.Fatal("installed bytes differ")
	}
	stored, err := target.TargetInstallation(c.Account)
	a, _ := json.Marshal(stored)
	b, _ := json.Marshal(record)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatal("identity not stored alongside account")
	}
	if _, err = target.CommitPreparedTarget(*p, 1, make([]byte, 32)); err != nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("duplicate installation changed chain")
	}
	if _, err = target.CommitPreparedTarget(*p, 2, make([]byte, 32)); err == nil {
		t.Fatal("conflicting commit identity accepted")
	}
	bad := *p
	bad.State = []byte("corrupt")
	if _, err = target.CommitPreparedTarget(bad, 1, make([]byte, 32)); err == nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("corrupt installation mutated state")
	}
	if target.Get_PartitionMap(c.Account) != 0 {
		t.Fatal("installation activated routing")
	}
}
