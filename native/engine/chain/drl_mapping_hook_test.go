package chain

import (
	"math/big"
	"path/filepath"
	"testing"

	"blockEmulator/core"
	"blockEmulator/params"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestExternalMappingUpdateIsRouteOnly(t *testing.T) {
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

	cc := &params.ChainConfig{
		ChainID: 0, NodeID: 0, ShardID: 0, Nodes_perShard: 1,
		ShardNums: 2, BlockSize: 4, BlockInterval: 1, InjectSpeed: 4,
	}
	dbPath := filepath.Join(params.DatabaseWrite_path, "mptDB", "ldb", "s0", "N0")
	db, err := rawdb.NewLevelDBDatabase(dbPath, 0, 1, "mapping-route-only", false)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(cc, db)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.CloseBlockChain()

	address := "0x0000000000000000000000000000000000000001"
	bc.Update_PartitionMap(address, 0)
	bc.AddAccounts(
		[]string{address},
		[]*core.AccountState{{Nonce: 3, Balance: big.NewInt(77)}},
		0,
	)

	applied := bc.ApplyExternalObjectMappingUpdate(&ExternalObjectMappingUpdate{
		ProposalID:     "route-only-test",
		EffectiveEpoch: 1,
		ObjectType:     "stable",
		ObjectID:       "stable:test",
		SourceShard:    0,
		TargetShard:    1,
		Addresses:      []string{address},
		ApplyMode:      "lightweight_stable_active_slice_hook",
	})
	if applied != 1 {
		t.Fatalf("applied accounts = %d, want 1", applied)
	}
	if got := bc.Get_PartitionMap(address); got != 1 {
		t.Fatalf("route shard = %d, want 1", got)
	}

	state := bc.FetchAccounts([]string{address})[0]
	if state.Balance.Int64() != 77 || state.Nonce != 3 {
		t.Fatalf("route update changed source trie state: balance=%s nonce=%d", state.Balance, state.Nonce)
	}
}
