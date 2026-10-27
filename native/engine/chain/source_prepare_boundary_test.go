package chain

import (
	"bytes"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"blockEmulator/core"
	"blockEmulator/params"
)

// Characterization, NOT a passing migration-preparation acceptance test.
// ExportAccountState supplies a snapshot, not a write fence. Both schedules
// below are deterministic and show why that snapshot cannot yet be labelled
// PREPARED. They deliberately use the real queue, proposal and commit helpers.
func TestSourceExportDoesNotEstablishPreparationBoundary(t *testing.T) {
	for _, alreadyProposed := range []bool{true, false} {
		name := "arrives_after_export"
		if alreadyProposed {
			name = "already_removed_from_queue"
		}
		t.Run(name, func(t *testing.T) {
			oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
			t.Cleanup(func() {
				params.ExpDataRootDir = oldRoot
				params.DatabaseWrite_path = oldDB
				params.ShardNum = oldShards
			})
			t.Setenv("SAA_INTEGRATED_CONTROLLER", "forecast")
			params.ExpDataRootDir = t.TempDir()
			params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "database") + string(filepath.Separator)
			params.ShardNum = 2
			source := newMigrationChain(t, params.DatabaseWrite_path, 0)
			defer source.CloseBlockChain()
			account := "0000000000000000000000000000000000000010"
			peer := "0000000000000000000000000000000000000020"
			source.Update_PartitionMap(account, 0)
			source.Update_PartitionMap(peer, 0)
			source.AddAccounts([]string{account, peer}, []*core.AccountState{
				{Nonce: 4, Balance: big.NewInt(100)}, {Balance: big.NewInt(0)},
			}, 0)
			tx := core.NewTransaction(account, peer, big.NewInt(7), 1, time.Unix(1, 0))
			tx.RoutingFrozen = true
			tx.MappingVersion = 0
			tx.RoutedSenderShard = 0
			tx.RoutedRecipientShard = 0
			var block *core.Block
			if alreadyProposed {
				source.SendTx2Pool([]*core.Transaction{tx})
				block = source.GenerateBlock(0)
				if source.Txpool.GetTxQueueLen() != 0 {
					t.Fatal("transaction did not leave queue")
				}
			}
			exported, err := source.ExportAccountState(account)
			if err != nil {
				t.Fatal(err)
			}
			if exported.State.Balance.Int64() != 100 || exported.State.Nonce != 4 {
				t.Fatal("unexpected export")
			}
			if !alreadyProposed {
				source.SendTx2Pool([]*core.Transaction{tx})
				block = source.GenerateBlock(0)
			}
			if len(block.Body) != 1 {
				t.Fatal("transaction not proposed exactly once")
			}
			source.AddBlock(block)
			current, err := source.ExportAccountState(account)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(exported.StateHash, current.StateHash) {
				t.Fatal("behavior changed: review whether preparation fencing now exists")
			}
			if current.State.Balance.Int64() != 93 || current.State.Nonce != 5 {
				t.Fatal("unexpected committed state")
			}
			if source.Get_PartitionMap(account) != 0 {
				t.Fatal("unexpected mapping change")
			}
			t.Logf("PREPARED_CONTRACT=FAIL schedule=%s exported_balance=100 exported_nonce=4 committed_balance=93 committed_nonce=5 mapping_unchanged=true exported_hash=%x committed_hash=%x", name, exported.StateHash, current.StateHash)
		})
	}
}
