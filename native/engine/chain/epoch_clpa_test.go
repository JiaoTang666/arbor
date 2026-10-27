package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"math/big"
	"path/filepath"
	"testing"
)

func TestCLPAEpochRoundTripAndStrictImport(t *testing.T) {
	oldPath, oldShards := params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() { params.DatabaseWrite_path = oldPath; params.ShardNum = oldShards })
	params.ShardNum = 2
	params.DatabaseWrite_path = filepath.Join(t.TempDir(), "database") + string(filepath.Separator)
	a := "0000000000000000000000000000000000000002"
	source := newMigrationChain(t, params.DatabaseWrite_path, 0)
	target := newMigrationChain(t, params.DatabaseWrite_path, 1)
	defer source.CloseBlockChain()
	defer target.CloseBlockChain()
	source.AddAccounts([]string{a}, []*core.AccountState{{Balance: big.NewInt(77), Nonce: 3}}, 0)
	states, err := source.ExportCLPAEpochAccounts([]string{a})
	if err != nil {
		t.Fatal(err)
	}
	before := append([]byte(nil), target.CurrentBlock.Header.StateRoot...)
	if target.InstallCLPAEpochAccounts(map[string]uint64{a: 1}, nil, nil, 0) == nil {
		t.Fatal("missing state accepted")
	}
	if !bytes.Equal(before, target.CurrentBlock.Header.StateRoot) || target.Get_PartitionMap(a) != 0 {
		t.Fatal("failed import mutated state or route")
	}
	if err := target.InstallCLPAEpochAccounts(map[string]uint64{a: 1}, []string{a}, states, 0); err != nil {
		t.Fatal(err)
	}
	if err := source.InstallCLPAEpochAccounts(map[string]uint64{a: 1}, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExportCLPAEpochAccounts([]string{a}); err == nil {
		t.Fatal("non-owner exported stale physical state")
	}
	// A later epoch may return the same account; it overwrites the old physical
	// copy with current owner state rather than forbidding all remigration.
	target.AddAccounts([]string{a}, []*core.AccountState{{Balance: big.NewInt(99), Nonce: 4}}, 0)
	states, err = target.ExportCLPAEpochAccounts([]string{a})
	if err != nil {
		t.Fatal(err)
	}
	if err = source.InstallCLPAEpochAccounts(map[string]uint64{a: 0}, []string{a}, states, 0); err != nil {
		t.Fatal(err)
	}
	got, err := source.ExportCLPAEpochAccounts([]string{a})
	if err != nil || got[0].Balance.Int64() != 99 || got[0].Nonce != 4 {
		t.Fatalf("round trip lost current state: %v", err)
	}
	missing := "0000000000000000000000000000000000000004"
	if _, err = source.ExportCLPAEpochAccounts([]string{missing}); err == nil {
		t.Fatal("absent account fabricated")
	}
}
