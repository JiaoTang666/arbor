package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"math/big"
	"path/filepath"
	"testing"
	"time"
)

// Regressions promoted from the isolated whole-chain review probes.
func reviewEpochChain(t *testing.T) *BlockChain {
	t.Helper()
	t.Setenv("BLOCK_EMULATOR_CLPA_EPOCH_V1", "1")
	t.Setenv(NativeEffectIndexEnv, "0")
	t.Setenv("SAA_INTEGRATED_CONTROLLER", "")
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", "")
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", "")
	oldPath, oldShards := params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() { params.DatabaseWrite_path = oldPath; params.ShardNum = oldShards })
	params.ShardNum = 2
	params.DatabaseWrite_path = filepath.Join(t.TempDir(), "database") + string(filepath.Separator)
	bc := newMigrationChain(t, params.DatabaseWrite_path, 0)
	t.Cleanup(bc.CloseBlockChain)
	return bc
}

func reviewEpochTx() *core.Transaction {
	return core.NewTransaction("0000000000000000000000000000000000000002",
		"0000000000000000000000000000000000000004", big.NewInt(1), 1, time.Unix(1, 0))
}

func TestReviewEpochMissingStateRootMustAbort(t *testing.T) {
	bc := reviewEpochChain(t)
	bc.CurrentBlock.Header.StateRoot = bytes.Repeat([]byte{0x77}, 32)
	defer func() {
		if failure := recover(); failure == nil {
			t.Error("epoch mode returned normally after missing state trie; state failure was silently downgraded")
		} else {
			t.Logf("missing root rejected: %v", failure)
		}
	}()
	root := bc.GetUpdateStatusTrie([]*core.Transaction{reviewEpochTx()})
	t.Logf("returned root = %x", root.Bytes())
}

func TestReviewEpochReplayMismatchMustAbort(t *testing.T) {
	bc := reviewEpochChain(t)
	bc.SendTx2Pool([]*core.Transaction{reviewEpochTx()})
	b := bc.GenerateBlock(0)
	b.Header.StateRoot = bytes.Repeat([]byte{0x78}, 32)
	b.Hash = b.Header.Hash()
	if err := bc.IsValidBlock(b); err == nil {
		t.Fatal("pre-prepare accepted a mismatched state root")
	}
	if err := bc.AddBlock(b); err == nil {
		t.Fatal("commit accepted a mismatched state root")
	}
	if bc.CurrentBlock.Header.Number != 0 {
		t.Fatal("invalid replay advanced the ledger")
	}
}

func TestReviewEpochRejectedDebitMustNotBecomeRelay(t *testing.T) {
	bc := reviewEpochChain(t)
	tx := reviewEpochTx()
	tx.Value = new(big.Int).Add(params.Init_Balance, big.NewInt(1))
	defer func() {
		if recover() == nil {
			t.Fatal("rejected debit returned as ordinary execution; caller could relay/report it")
		}
	}()
	bc.GetUpdateStatusTrie([]*core.Transaction{tx})
}
