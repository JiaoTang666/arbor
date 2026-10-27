package chain

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Production trie and pool, deterministic local execution; not PBFT voting.
func TestLocalBatchSequentialBalanceDependencies(t *testing.T) {
	oldDB, oldShards := params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() { params.DatabaseWrite_path = oldDB; params.ShardNum = oldShards })
	params.ShardNum = 2
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	raw, _ := json.Marshal(initialledger.Manifest{Schema: "synthetic_initial_ledger_v1", Shards: 2, Accounts: []initialledger.Account{{Address: a, Balance: "50", Shard: 0}, {Address: b, Balance: "0", Shard: 0}, {Address: c, Balance: "0", Shard: 0}}})
	path := filepath.Join(t.TempDir(), "ledger.json")
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	h := sha256.Sum256(raw)
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", path)
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", hex.EncodeToString(h[:]))
	params.DatabaseWrite_path = t.TempDir() + string(filepath.Separator)
	bc := newMigrationChain(t, params.DatabaseWrite_path, 0)
	defer bc.CloseBlockChain()
	txs := []*core.Transaction{core.NewTransaction(a, b, big.NewInt(10), 0, time.Now()), core.NewTransaction(b, c, big.NewInt(7), 1, time.Now()), core.NewTransaction(c, a, big.NewInt(2), 2, time.Now())}
	for _, tx := range txs {
		tx.RoutingFrozen = true
		tx.RoutedSenderShard = 0
		tx.RoutedRecipientShard = 0
		tx.SenderRequiresExistingState = true
		tx.RecipientRequiresExistingState = true
	}
	bc.Txpool.AddTxs2Pool(txs)
	packed := bc.Txpool.PackTxs(1000)
	root := bc.GetUpdateStatusTrie(packed)
	st, e := trie.New(trie.TrieID(common.BytesToHash(root.Bytes())), bc.triedb)
	if e != nil {
		t.Fatal(e)
	}
	for account, want := range map[string]int64{a: 42, b: 3, c: 5} {
		raw, e := st.Get([]byte(account))
		if e != nil {
			t.Fatal(e)
		}
		state := core.DecodeAS(raw)
		if state.Balance.Cmp(big.NewInt(want)) != 0 || state.Nonce != 1 {
			t.Fatal(account, state, want)
		}
	}
}
