package chain

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"path/filepath"
	"testing"
	"time"
)

func nativeOwnerFixture(t *testing.T) func(uint64) *BlockChain {
	t.Helper()
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() { params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum = oldRoot, oldDB, oldShards })
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "blocks") + "/"
	params.ShardNum = 16
	t.Setenv("SAA_INTEGRATED_CONTROLLER", "forecast")
	return func(shard uint64) *BlockChain {
		db, err := rawdb.NewLevelDBDatabase(filepath.Join(params.ExpDataRootDir, fmt.Sprintf("state-%d", shard)), 0, 1, "native-owner", false)
		if err != nil {
			t.Fatal(err)
		}
		bc, err := NewBlockChain(&params.ChainConfig{ShardID: shard, ShardNums: 16, Nodes_perShard: 1, BlockSize: 4}, db)
		if err != nil {
			t.Fatal(err)
		}
		return bc
	}
}

func nativeOwnerTx(a, b string, source, target uint64, seq uint64) *core.Transaction {
	tx := core.NewTransaction(a, b, big.NewInt(1), seq, time.Unix(1, 0))
	tx.RoutingFrozen, tx.RoutedSenderShard, tx.RoutedRecipientShard, tx.IngressSequence = true, source, target, seq
	return tx
}

// The exact three new accounts from the failed run: default hash shards 5, 1,
// 12, but native first placement created their states on shard 3. No test-only
// Update_PartitionMap call is allowed to hide the original integration defect.
func TestFrozenNativeCreationOwnershipSurvivesRestartAndMigrates(t *testing.T) {
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprintf("members-%d", count), func(t *testing.T) {
			open := nativeOwnerFixture(t)
			source, target := open(3), open(1)
			defer func() { source.CloseBlockChain(); target.CloseBlockChain() }()
			accounts := []string{"624d367c74077bb65e1c84786e0859fc1f676c25", "9b2d3bf10004f009dac2d74f53174dabf1fd49b1", "c1d942f2f5cbeefc6bfdbb173670e2aa7630963c"}
			for _, a := range accounts {
				if source.Get_PartitionMap(a) == 3 {
					t.Fatal("fixture no longer reproduces hash mismatch")
				}
			}
			source.SendTx2Pool([]*core.Transaction{nativeOwnerTx(accounts[0], accounts[1], 3, 3, 0), nativeOwnerTx(accounts[2], accounts[1], 3, 3, 1)})
			block := source.GenerateBlock(0)
			for _, a := range accounts {
				if r, err := source.NativeFirstOwner(a); err != nil || r != nil {
					t.Fatal("proposal exposed uncommitted owner", r, err)
				}
				if _, err := source.ExportAccountState(a); err == nil {
					t.Fatal("proposal exposed uncommitted state")
				}
			}
			source.AddBlock(block)
			root := append([]byte(nil), source.CurrentBlock.Header.StateRoot...)
			replica, err := NewBlockChain(&params.ChainConfig{ShardID: 3, NodeID: 1, ShardNums: 16, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
			if err != nil {
				t.Fatal(err)
			}
			// Replays the stored block body against an independent trie. Forecast
			// mode rejects differing replay roots instead of silently accepting.
			replica.AddBlock(block)
			for _, a := range accounts {
				if r, err := replica.NativeFirstOwner(a); err != nil || r == nil || r.Shard != 3 {
					t.Fatal("replica owner differs", err)
				}
			}
			replica.CloseBlockChain()
			source.CloseBlockChain()
			source = open(3)
			if !bytes.Equal(root, source.CurrentBlock.Header.StateRoot) {
				t.Fatal("restart changed root")
			}
			var command core.GroupPrepareCommand
			for _, a := range accounts[:count] {
				r, err := source.NativeFirstOwner(a)
				if err != nil || r == nil || r.Shard != 3 || r.CreationHeight != block.Header.Number {
					t.Fatal("committed owner missing", r, err)
				}
				if source.Get_PartitionMap(a) == 3 {
					t.Fatal("test accidentally supplied a map override")
				}
				state, err := source.ExportAccountState(a)
				if err != nil {
					t.Fatal(err)
				}
				command.Members = append(command.Members, core.SourcePrepareCommand{ProposalID: "native-first-owner", Account: a, SourceShard: 3, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(source.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(root), ExpectedStateHash: hex.EncodeToString(state.StateHash)})
			}
			p, err := source.CommitGroupSourcePreparation(command, 2, make([]byte, 32), nil)
			if err != nil {
				t.Fatal("actual committed creation cannot migrate", err)
			}
			serialRetire(t, source, target, serialActivate(t, target, p, 1), 3)
			for _, m := range p.Members {
				got, err := target.ExportAccountState(m.Command.Account)
				if err != nil || !bytes.Equal(got.EncodedState, m.State) {
					t.Fatal("migration changed actual state", err)
				}
			}
		})
	}
}

func TestNativeOwnerOnlyActualCreationAndNoOverride(t *testing.T) {
	open := nativeOwnerFixture(t)
	bc := open(3)
	defer bc.CloseBlockChain()
	a, b := "624d367c74077bb65e1c84786e0859fc1f676c25", "9b2d3bf10004f009dac2d74f53174dabf1fd49b1"
	tx := nativeOwnerTx(a, b, 5, 3, 0)
	tx.Relayed = true
	bc.SendTx2Pool([]*core.Transaction{tx})
	bc.AddBlock(bc.GenerateBlock(0))
	if owner, err := bc.NativeFirstOwner(a); err != nil || owner != nil {
		t.Fatal("relay sender falsely acquired owner", err)
	}
	owner, err := bc.NativeFirstOwner(b)
	if err != nil || owner == nil || owner.Role != "recipient" {
		t.Fatal("actual recipient creation missing", err)
	}
	st, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	if err := bc.recordNativeFirstOwner(st, b, tx, "recipient"); err == nil {
		t.Fatal("existing creation provenance overwritten")
	}
	bc.initialLedger = &initialledger.Ledger{Routes: map[string]uint64{b: 12}}
	if _, err := bc.sourcePreparationOwner(b, 0); err == nil {
		t.Fatal("native record overrode initial ledger")
	}
	if err := bc.recordNativeFirstOwner(st, b, tx, "recipient"); err == nil {
		t.Fatal("wrong-shard creator overrode initial ledger")
	}
	bc.initialLedger = nil
	if _, err := bc.sourcePreparationOwner(b, 0); err != nil {
		t.Fatal(err)
	}
}

func TestSkippedNativeDebitCreatesNoOwnership(t *testing.T) {
	open := nativeOwnerFixture(t)
	bc := open(3)
	defer bc.CloseBlockChain()
	a, b := "624d367c74077bb65e1c84786e0859fc1f676c25", "9b2d3bf10004f009dac2d74f53174dabf1fd49b1"
	tx := nativeOwnerTx(a, b, 3, 3, 0)
	tx.Value = new(big.Int).Add(params.Init_Balance, big.NewInt(1))
	bc.SendTx2Pool([]*core.Transaction{tx})
	bc.AddBlock(bc.GenerateBlock(0))
	for _, account := range []string{a, b} {
		if owner, err := bc.NativeFirstOwner(account); err != nil || owner != nil {
			t.Fatal("skipped debit created owner", err)
		}
		if _, err := bc.ExportAccountState(account); err == nil {
			t.Fatal("skipped debit created state")
		}
	}
}
