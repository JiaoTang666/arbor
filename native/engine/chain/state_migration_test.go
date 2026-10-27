package chain

import (
	"bytes"
	"math/big"
	"path/filepath"
	"testing"

	"blockEmulator/core"
	"blockEmulator/params"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

func newMigrationChain(t *testing.T, root string, shard uint64) *BlockChain {
	t.Helper()
	cc := &params.ChainConfig{
		ChainID: 0, NodeID: 0, ShardID: shard, Nodes_perShard: 1,
		ShardNums: 2, BlockSize: 4, BlockInterval: 1, InjectSpeed: 4,
	}
	dbPath := filepath.Join(root, "mptDB", "ldb", "s"+string(rune('0'+shard)), "N0")
	db, err := rawdb.NewLevelDBDatabase(dbPath, 0, 1, "migration-test", false)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(cc, db)
	if err != nil {
		t.Fatal(err)
	}
	return bc
}

func TestAccountStateMigrationPrepareImportFinalize(t *testing.T) {
	oldRoot := params.ExpDataRootDir
	oldDBPath := params.DatabaseWrite_path
	oldShardNum := params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDBPath
		params.ShardNum = oldShardNum
	})

	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + string(filepath.Separator)
	params.ShardNum = 2

	source := newMigrationChain(t, params.DatabaseWrite_path, 0)
	target := newMigrationChain(t, params.DatabaseWrite_path, 1)

	address := "0x0000000000000000000000000000000000000011"
	source.Update_PartitionMap(address, 0)
	source.AddAccounts([]string{address}, []*core.AccountState{
		{Nonce: 4, Balance: big.NewInt(77)},
	}, 0)

	migration, err := source.ExportAccountState(address)
	if err != nil {
		t.Fatal(err)
	}
	if migration.State.Balance.Int64() != 77 || migration.State.Nonce != 4 {
		t.Fatalf("exported state = balance %s nonce %d", migration.State.Balance, migration.State.Nonce)
	}

	target.Update_PartitionMap(address, 1)
	targetRoot, err := target.ImportAccountState(migration)
	if err != nil {
		t.Fatal(err)
	}
	if len(targetRoot) == 0 {
		t.Fatal("target root is empty after import")
	}
	imported := target.FetchAccounts([]string{address})[0]
	if imported.Balance.Int64() != 77 || imported.Nonce != 4 {
		t.Fatalf("imported state = balance %s nonce %d", imported.Balance, imported.Nonce)
	}
	duplicateRoot, err := target.ImportAccountState(migration)
	if err != nil {
		t.Fatalf("idempotent duplicate import failed: %v", err)
	}
	if !bytes.Equal(targetRoot, duplicateRoot) {
		t.Fatal("duplicate import changed target root")
	}

	sourceRoot, err := source.FinalizeAccountStateMigration(target, migration)
	if err != nil {
		t.Fatal(err)
	}
	duplicateSourceRoot, err := source.FinalizeAccountStateMigration(target, migration)
	if err != nil {
		t.Fatalf("idempotent finalize retry failed: %v", err)
	}
	if !bytes.Equal(sourceRoot, duplicateSourceRoot) {
		t.Fatal("finalize retry changed source root")
	}
	if len(sourceRoot) == 0 {
		t.Fatal("source root is empty after finalize")
	}
	sourceState := source.FetchAccounts([]string{address})[0]
	if sourceState.Balance.Cmp(params.Init_Balance) != 0 || sourceState.Nonce != 0 {
		t.Fatalf("source still has exported state: balance %s nonce %d", sourceState.Balance, sourceState.Nonce)
	}
	sourceRootAfter := append([]byte(nil), source.CurrentBlock.Header.StateRoot...)
	targetRootAfter := append([]byte(nil), target.CurrentBlock.Header.StateRoot...)
	source.CloseBlockChain()
	target.CloseBlockChain()
	sourceDB, err := rawdb.NewLevelDBDatabase(filepath.Join(params.DatabaseWrite_path, "mptDB", "ldb", "s0", "N0"), 0, 1, "migration-reopen-source", false)
	if err != nil {
		t.Fatal(err)
	}
	reopenedSource, err := NewBlockChain(&params.ChainConfig{ChainID: 0, NodeID: 0, ShardID: 0, Nodes_perShard: 1, ShardNums: 2, BlockSize: 4, BlockInterval: 1, InjectSpeed: 4}, sourceDB)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedSource.CloseBlockChain()
	if !bytes.Equal(sourceRootAfter, reopenedSource.CurrentBlock.Header.StateRoot) {
		t.Fatal("reopened source root differs after finalize")
	}
	targetDB, err := rawdb.NewLevelDBDatabase(filepath.Join(params.DatabaseWrite_path, "mptDB", "ldb", "s1", "N0"), 0, 1, "migration-reopen-target", false)
	if err != nil {
		t.Fatal(err)
	}
	reopenedTarget, err := NewBlockChain(&params.ChainConfig{ChainID: 0, NodeID: 0, ShardID: 1, Nodes_perShard: 1, ShardNums: 2, BlockSize: 4, BlockInterval: 1, InjectSpeed: 4}, targetDB)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedTarget.CloseBlockChain()
	if !bytes.Equal(targetRootAfter, reopenedTarget.CurrentBlock.Header.StateRoot) {
		t.Fatal("reopened target root differs after import")
	}
	persisted := reopenedTarget.FetchAccounts([]string{address})[0]
	if persisted.Balance.Int64() != 77 || persisted.Nonce != 4 {
		t.Fatalf("reopened target state = balance %s nonce %d", persisted.Balance, persisted.Nonce)
	}

}

func TestAccountStateMigrationRejectsChangedSource(t *testing.T) {
	oldRoot := params.ExpDataRootDir
	oldDBPath := params.DatabaseWrite_path
	oldShardNum := params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDBPath
		params.ShardNum = oldShardNum
	})

	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + string(filepath.Separator)
	params.ShardNum = 1

	source := newMigrationChain(t, params.DatabaseWrite_path, 0)
	target := newMigrationChain(t, params.DatabaseWrite_path, 1)
	defer source.CloseBlockChain()
	defer target.CloseBlockChain()
	address := "0x0000000000000000000000000000000000000022"
	source.Update_PartitionMap(address, 0)
	source.AddAccounts([]string{address}, []*core.AccountState{
		{Nonce: 1, Balance: big.NewInt(50)},
	}, 0)

	migration, err := source.ExportAccountState(address)
	if err != nil {
		t.Fatal(err)
	}

	source.AddAccounts([]string{address}, []*core.AccountState{
		{Nonce: 2, Balance: big.NewInt(49)},
	}, 0)
	if _, err := target.ImportAccountState(migration); err != nil {
		t.Fatal(err)
	}
	if _, err := source.FinalizeAccountStateMigration(target, migration); err == nil {
		t.Fatal("finalize accepted stale source state")
	}
	current := source.FetchAccounts([]string{address})[0]
	if current.Balance.Int64() != 49 || current.Nonce != 2 {
		t.Fatalf("stale finalize changed current source state: balance %s nonce %d", current.Balance, current.Nonce)
	}
}

func TestAccountStateMigrationRejectsUnconfirmedAndConflictingTarget(t *testing.T) {
	oldRoot := params.ExpDataRootDir
	oldDBPath := params.DatabaseWrite_path
	oldShardNum := params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDBPath
		params.ShardNum = oldShardNum
	})

	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + string(filepath.Separator)
	params.ShardNum = 2

	source := newMigrationChain(t, params.DatabaseWrite_path, 0)
	target := newMigrationChain(t, params.DatabaseWrite_path, 1)
	defer source.CloseBlockChain()
	defer target.CloseBlockChain()

	address := "0x0000000000000000000000000000000000000033"
	source.Update_PartitionMap(address, 0)
	source.AddAccounts([]string{address}, []*core.AccountState{
		{Nonce: 3, Balance: big.NewInt(88)},
	}, 0)
	migration, err := source.ExportAccountState(address)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := source.FinalizeAccountStateMigration(target, migration); err == nil {
		t.Fatal("finalize succeeded before target import")
	}
	before := source.FetchAccounts([]string{address})[0]
	if before.Balance.Int64() != 88 || before.Nonce != 3 {
		t.Fatalf("unconfirmed finalize changed source: balance %s nonce %d", before.Balance, before.Nonce)
	}

	target.Update_PartitionMap(address, 1)
	target.AddAccounts([]string{address}, []*core.AccountState{
		{Nonce: 99, Balance: big.NewInt(1)},
	}, 1)
	if _, err := target.ImportAccountState(migration); err == nil {
		t.Fatal("conflicting target state accepted migration")
	}
	after := source.FetchAccounts([]string{address})[0]
	if after.Balance.Int64() != 88 || after.Nonce != 3 {
		t.Fatalf("conflict handling changed source: balance %s nonce %d", after.Balance, after.Nonce)
	}
}

func TestAccountStateMigrationFinalizeAfterSourceRestart(t *testing.T) {
	oldRoot := params.ExpDataRootDir
	oldDBPath := params.DatabaseWrite_path
	oldShardNum := params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDBPath
		params.ShardNum = oldShardNum
	})

	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + string(filepath.Separator)
	params.ShardNum = 2

	source := newMigrationChain(t, params.DatabaseWrite_path, 0)
	target := newMigrationChain(t, params.DatabaseWrite_path, 1)
	defer target.CloseBlockChain()

	address := "0x0000000000000000000000000000000000000044"
	source.Update_PartitionMap(address, 0)
	source.AddAccounts([]string{address}, []*core.AccountState{
		{Nonce: 5, Balance: big.NewInt(123)},
	}, 0)
	migration, err := source.ExportAccountState(address)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.ImportAccountState(migration); err != nil {
		t.Fatal(err)
	}

	source.CloseBlockChain()
	source = newMigrationChain(t, params.DatabaseWrite_path, 0)
	defer source.CloseBlockChain()
	if _, err := source.FinalizeAccountStateMigration(target, migration); err != nil {
		t.Fatal(err)
	}
	state := source.FetchAccounts([]string{address})[0]
	if state.Balance.Cmp(params.Init_Balance) != 0 || state.Nonce != 0 {
		t.Fatalf("restarted source still has exported state: balance %s nonce %d", state.Balance, state.Nonce)
	}
}

func TestAccountStateMigrationFinalizeAfterTargetRestart(t *testing.T) {
	oldRoot := params.ExpDataRootDir
	oldDBPath := params.DatabaseWrite_path
	oldShardNum := params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDBPath
		params.ShardNum = oldShardNum
	})
	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + string(filepath.Separator)
	params.ShardNum = 2
	source := newMigrationChain(t, params.DatabaseWrite_path, 0)
	target := newMigrationChain(t, params.DatabaseWrite_path, 1)
	address := "0x0000000000000000000000000000000000000055"
	source.Update_PartitionMap(address, 0)
	source.AddAccounts([]string{address}, []*core.AccountState{{Nonce: 6, Balance: big.NewInt(144)}}, 0)
	migration, err := source.ExportAccountState(address)
	if err != nil {
		t.Fatal(err)
	}
	target.Update_PartitionMap(address, 1)
	if _, err := target.ImportAccountState(migration); err != nil {
		t.Fatal(err)
	}
	target.CloseBlockChain()
	targetDB, err := rawdb.NewLevelDBDatabase(filepath.Join(params.DatabaseWrite_path, "mptDB", "ldb", "s1", "N0"), 0, 1, "migration-target-reopen", false)
	if err != nil {
		t.Fatal(err)
	}
	target, err = NewBlockChain(&params.ChainConfig{ChainID: 0, NodeID: 0, ShardID: 1, Nodes_perShard: 1, ShardNums: 2, BlockSize: 4, BlockInterval: 1, InjectSpeed: 4}, targetDB)
	if err != nil {
		t.Fatal(err)
	}
	defer source.CloseBlockChain()
	defer target.CloseBlockChain()
	if _, err := source.FinalizeAccountStateMigration(target, migration); err != nil {
		t.Fatal(err)
	}
	state := source.FetchAccounts([]string{address})[0]
	if state.Balance.Cmp(params.Init_Balance) != 0 || state.Nonce != 0 {
		t.Fatalf("source state not removed after target restart: balance %s nonce %d", state.Balance, state.Nonce)
	}
}
