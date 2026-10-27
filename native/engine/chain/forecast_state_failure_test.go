package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
)

func TestForecastRejectsMissingTrieAndMismatchedReplayRoot(t *testing.T) {
	t.Setenv("SAA_INTEGRATED_CONTROLLER", "forecast")
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	defer func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	}()
	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + string(filepath.Separator)
	params.ShardNum = 2
	source := newMigrationChain(t, params.DatabaseWrite_path, 0)
	defer source.CloseBlockChain()
	expectPanic := func(stage string, run func()) {
		t.Helper()
		defer func() {
			v := recover()
			if v == nil || !strings.Contains(fmt.Sprint(v), stage) {
				t.Fatalf("expected %s failure, got %v", stage, v)
			}
		}()
		run()
	}
	oldStateRoot := source.CurrentBlock.Header.StateRoot
	expectPanic("migrated sender state missing", func() {
		source.GetUpdateStatusTrie([]*core.Transaction{{Sender: "0x0000000000000000000000000000000000000011", Recipient: "0x0000000000000000000000000000000000000022", RoutingFrozen: true, RoutedSenderShard: 0, RoutedRecipientShard: 1, SenderRequiresExistingState: true, Value: big.NewInt(1)}})
	})
	expectPanic("migrated recipient state missing", func() {
		source.GetUpdateStatusTrie([]*core.Transaction{{Sender: "0x0000000000000000000000000000000000000011", Recipient: "0x0000000000000000000000000000000000000022", RoutingFrozen: true, RoutedSenderShard: 1, RoutedRecipientShard: 0, RecipientRequiresExistingState: true, Value: big.NewInt(1)}})
	})
	source.CurrentBlock.Header.StateRoot = bytes.Repeat([]byte{1}, 32)
	// SEF target/source guards may read the same invalid committed root during
	// precheck before transaction execution opens it. Both are the same
	// fail-closed forecast contract; the exact first reader is not stable.
	expectPanic("forecast state application failed", func() { source.GetUpdateStatusTrie([]*core.Transaction{{Value: big.NewInt(1)}}) })
	source.CurrentBlock.Header.StateRoot = oldStateRoot
	oldBlock := source.CurrentBlock
	block := &core.Block{Header: &core.BlockHeader{Number: oldBlock.Header.Number + 1, ParentBlockHash: oldBlock.Hash, StateRoot: bytes.Repeat([]byte{2}, 32)}}
	block.Header.TxRoot = GetTxTreeRoot(nil)
	block.Hash = block.Header.Hash()
	if err := source.AddBlock(block); err == nil || !strings.Contains(err.Error(), "replayed_root_mismatch") {
		t.Fatalf("mismatched replay was not rejected: %v", err)
	}
	if source.CurrentBlock != oldBlock {
		t.Fatal("invalid replay advanced chain head")
	}
}
