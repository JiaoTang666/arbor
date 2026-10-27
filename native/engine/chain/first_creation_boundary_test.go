package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"encoding/hex"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
	"math/big"
	"path/filepath"
	"testing"
	"time"
)

// Deterministic native-chain boundary test, not a PBFT/network experiment.
func TestFirstCreationRequiresFirstTransaction(t *testing.T) {
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	defer func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	}()
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "database") + string(filepath.Separator)
	params.ShardNum = 2
	bc := newMigrationChain(t, params.DatabaseWrite_path, 0)
	defer bc.CloseBlockChain()
	const sender = "0000000000000000000000000000000000000010"
	const recipient = "0000000000000000000000000000000000000020"
	exists := func(root []byte, account string) bool {
		t.Helper()
		st, e := trie.New(trie.TrieID(common.BytesToHash(root)), bc.triedb)
		if e != nil {
			t.Fatal(e)
		}
		raw, e := st.Get([]byte(account))
		if e != nil {
			t.Fatal(e)
		}
		return len(raw) > 0
	}
	initial := append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
	absent := bc.ObserveAccountPresence(sender, time.Unix(90, 0))
	if absent.Status != AccountAbsent || absent.StateRoot == "" || absent.BlockHeight != bc.CurrentBlock.Header.Number {
		t.Fatal("committed absence not observed", absent)
	}
	bc.CurrentBlock.Header.StateRoot = bytes.Repeat([]byte{1}, 32)
	unavailable := bc.ObserveAccountPresence(sender, time.Unix(91, 0))
	if unavailable.Status != AccountUnavailable || unavailable.FailureCause == "" {
		t.Fatal("missing root was not fail-closed", unavailable)
	}
	bc.CurrentBlock.Header.StateRoot = initial
	if exists(initial, sender) || exists(initial, recipient) {
		t.Fatal("fixture not empty")
	}
	returned := bc.FetchAccounts([]string{sender, recipient})
	if len(returned) != 2 || returned[0].Balance.Cmp(params.Init_Balance) != 0 {
		t.Fatal("unexpected FetchAccounts behavior")
	}
	if exists(initial, sender) || exists(initial, recipient) {
		t.Fatal("read synthesized committed state")
	}
	tx := core.NewTransaction(sender, recipient, big.NewInt(1), 0, time.Unix(100, 0))
	tx.RoutingFrozen = true
	tx.RoutedSenderShard = 0
	tx.RoutedRecipientShard = 0
	bc.SendTx2Pool([]*core.Transaction{tx})
	block := bc.GenerateBlock(0)
	if len(block.Body) != 1 {
		t.Fatal("fixture transaction not selected")
	}
	if !bytes.Equal(initial, bc.CurrentBlock.Header.StateRoot) {
		t.Fatal("proposal advanced committed head")
	}
	if exists(bc.CurrentBlock.Header.StateRoot, sender) {
		t.Fatal("committed state appeared before AddBlock")
	}
	// Proposal state is now private, not a persisted root that can be opened
	// through the database before commit. Both accounts must exist there.
	for _, account := range []string{sender, recipient} {
		raw, err := bc.pendingExecution.state.Get([]byte(account))
		if err != nil || len(raw) == 0 {
			t.Fatal("private execution failed to create account", account, err)
		}
	}
	if err := bc.AddBlock(block); err != nil {
		t.Fatal(err)
	}
	present := bc.ObserveAccountPresence(sender, time.Unix(110, 0))
	if present.Status != AccountPresent || present.StateRoot != hex.EncodeToString(bc.CurrentBlock.Header.StateRoot) {
		t.Fatal("committed presence not observed", present)
	}
	if !exists(bc.CurrentBlock.Header.StateRoot, sender) || !exists(bc.CurrentBlock.Header.StateRoot, recipient) {
		t.Fatal("created state absent after block installation")
	}
	finalRoot := append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
	bc.AddBlock(block)
	if !bytes.Equal(finalRoot, bc.CurrentBlock.Header.StateRoot) {
		t.Fatal("same block replay mutated state")
	}
	t.Log("PASS: FetchAccounts returns synthetic defaults; first transaction execution creates state; AddBlock advances committed head; duplicate block leaves head unchanged. Not proof of request idempotency or PBFT consensus.")
}
