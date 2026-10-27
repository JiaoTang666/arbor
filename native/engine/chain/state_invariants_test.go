package chain

import (
	"bytes"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"blockEmulator/core"
	"blockEmulator/params"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestAccountStateInvariantsAndTrieRecovery(t *testing.T) {
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

	cc := &params.ChainConfig{
		ChainID: 0, NodeID: 0, ShardID: 0, Nodes_perShard: 1,
		ShardNums: 1, BlockSize: 16, BlockInterval: 1, InjectSpeed: 16,
	}
	mptPath := filepath.Join(params.DatabaseWrite_path, "mptDB", "ldb", "s0", "N0")
	db, err := rawdb.NewLevelDBDatabase(mptPath, 0, 1, "state-invariant", false)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(cc, db)
	if err != nil {
		t.Fatal(err)
	}

	const sender = "0000000000000000000000000000000000000001"
	const recipient = "0000000000000000000000000000000000000002"
	bc.AddAccounts(
		[]string{sender, recipient},
		[]*core.AccountState{
			{Nonce: 0, Balance: big.NewInt(100)},
			{Nonce: 0, Balance: big.NewInt(20)},
		},
		0,
	)
	rootBefore := append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)

	txs := []*core.Transaction{
		core.NewTransaction(sender, recipient, big.NewInt(10), 101, time.Unix(1, 0)),
		core.NewTransaction(sender, recipient, big.NewInt(15), 205, time.Unix(2, 0)),
		core.NewTransaction(sender, recipient, big.NewInt(1000), 999, time.Unix(3, 0)),
	}
	bc.SendTx2Pool(txs)
	block := bc.GenerateBlock(0)
	bc.AddBlock(block)

	states := bc.FetchAccounts([]string{sender, recipient})
	if got := states[0].Balance.Int64(); got != 75 {
		t.Fatalf("sender balance = %d, want 75", got)
	}
	if got := states[1].Balance.Int64(); got != 45 {
		t.Fatalf("recipient balance = %d, want 45", got)
	}
	if got := states[0].Nonce; got != 2 {
		t.Fatalf("sender nonce = %d, want 2 successful outgoing transactions", got)
	}
	if got := states[1].Nonce; got != 0 {
		t.Fatalf("recipient nonce = %d, want 0", got)
	}
	total := new(big.Int).Add(new(big.Int).Set(states[0].Balance), states[1].Balance)
	if total.Cmp(big.NewInt(120)) != 0 {
		t.Fatalf("total balance = %s, want 120", total)
	}
	rootAfter := append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
	if bytes.Equal(rootBefore, rootAfter) {
		t.Fatal("state root did not change after successful transfers")
	}
	bc.CloseBlockChain()

	reopenedDB, err := rawdb.NewLevelDBDatabase(mptPath, 0, 1, "state-invariant-reopen", false)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewBlockChain(cc, reopenedDB)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseBlockChain()
	if !bytes.Equal(rootAfter, reopened.CurrentBlock.Header.StateRoot) {
		t.Fatal("reopened chain state root differs from committed root")
	}
	reopenedStates := reopened.FetchAccounts([]string{sender, recipient})
	if reopenedStates[0].Balance.Int64() != 75 || reopenedStates[0].Nonce != 2 {
		t.Fatalf("reopened sender state = balance %s nonce %d", reopenedStates[0].Balance, reopenedStates[0].Nonce)
	}
	if reopenedStates[1].Balance.Int64() != 45 || reopenedStates[1].Nonce != 0 {
		t.Fatalf("reopened recipient state = balance %s nonce %d", reopenedStates[1].Balance, reopenedStates[1].Nonce)
	}
}
