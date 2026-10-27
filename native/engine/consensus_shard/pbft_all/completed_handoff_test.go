package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/hex"
	"encoding/json"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPendingHandoffOrderFiltersBeforeDecode(t *testing.T) {
	n := &PbftConsensusNode{ShardID: 1}
	makeRecord := func(id string, seq uint64) *core.MigrationHandoff {
		tx := core.NewTransaction("a", "a", big.NewInt(1), seq, time.Unix(1, 0))
		tx.IngressSequence = seq
		return &core.MigrationHandoff{ID: id, TargetShard: 1, Original: tx.Encode()}
	}
	n.sourcePrepareSeen.Store("handoff:done", true)
	records := []*core.MigrationHandoff{{ID: "other", TargetShard: 0, Original: []byte("not decoded")}, {ID: "done", TargetShard: 1, Original: []byte("not decoded")}, makeRecord("z", 7), makeRecord("b", 2), makeRecord("a", 2)}
	got := n.pendingHandoffOrder(records)
	if len(got) != 3 || got[0].ID != "a" || got[1].ID != "b" || got[2].ID != "z" {
		t.Fatal("pending order changed")
	}
	fresh := &PbftConsensusNode{ShardID: 1}
	// Restart must not persist a process-local skip: replay remains available.
	if len(fresh.pendingHandoffOrder([]*core.MigrationHandoff{makeRecord("done", 1)})) != 1 {
		t.Fatal("restart omitted replay")
	}
}

func TestRetiredHandoffIdleAndLateRecovery(t *testing.T) {
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	})
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "db") + "/"
	params.ShardNum = 2
	account := "0000000000000000000000000000000000000010"
	journal, outbox, acks := t.TempDir(), t.TempDir(), t.TempDir()
	activationPath, receiptPath := filepath.Join(t.TempDir(), "activation.json"), filepath.Join(t.TempDir(), "receipt.json")
	t.Setenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR", outbox)
	t.Setenv("SAA_NATIVE_HANDOFF_ACK_DIR", acks)
	t.Setenv("SAA_NATIVE_TARGET_ACTIVATION_PATH", activationPath)
	t.Setenv("SAA_NATIVE_TARGET_ACTIVATED_PATH", receiptPath)
	t.Setenv("SAA_NATIVE_HANDOFF_BATCH_V1", "true")
	open := func() *chain.BlockChain {
		db, e := rawdb.NewLevelDBDatabase(filepath.Join(params.ExpDataRootDir, "state"), 0, 1, "idle-test", false)
		if e != nil {
			t.Fatal(e)
		}
		bc, e := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, db)
		if e != nil {
			t.Fatal(e)
		}
		return bc
	}
	bc := open()
	defer func() { bc.CloseBlockChain() }()
	bc.Update_PartitionMap(account, 0)
	bc.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	state, e := bc.ExportAccountState(account)
	if e != nil {
		t.Fatal(e)
	}
	cmd := core.SourcePrepareCommand{ProposalID: "idle", Account: account, SourceShard: 0, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(bc.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(state.SourceRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)}
	p, e := bc.CommitSourcePreparation(cmd, 2, make([]byte, 32))
	if e != nil {
		t.Fatal(e)
	}
	if e = bc.Txpool.EnableMigrationQueueJournal(journal, account); e != nil {
		t.Fatal(e)
	}
	n := &PbftConsensusNode{CurChain: bc}
	if e = os.WriteFile(activationPath, []byte("invalid"), 0600); e != nil {
		t.Fatal(e)
	}
	if n.pollSourceHandoff() == nil {
		t.Fatal("unretired path bypassed activation validation")
	}
	active := chain.TargetActivation{ProposalID: cmd.ProposalID, Account: account, TargetShard: 1, MappingVersion: 1, StateHash: p.StateHash, InstallDigest: strings.Repeat("1", 64)}
	retirement := chain.SourceRetirementCommand{Activation: active, ActivationReceiptHash: strings.Repeat("2", 64), Drain: core.MigrationDrainBoundary{Account: account, Generation: 1, SnapshotHash: strings.Repeat("3", 64)}}
	if _, e = bc.CommitSourceRetirement(retirement, 3, make([]byte, 32)); e != nil {
		t.Fatal(e)
	}
	if e = n.pollSourceHandoff(); e != nil {
		t.Fatal("retired empty path scanned activation", e)
	}
	bc.CloseBlockChain()
	bc = open()
	n = &PbftConsensusNode{CurChain: bc}
	if e = bc.Txpool.EnableMigrationQueueJournal(journal, account); e != nil {
		t.Fatal(e)
	}
	if e = n.pollSourceHandoff(); e != nil {
		t.Fatal("reopened retired empty path scanned", e)
	}
	late := core.NewTransaction(account, account, big.NewInt(1), 1, time.Unix(1, 0))
	late.RoutingFrozen = true
	late.SenderRequiresExistingState = true
	late.RecipientRequiresExistingState = true
	bc.SendTx2Pool([]*core.Transaction{late})
	bc.CloseBlockChain()
	bc = open()
	n = &PbftConsensusNode{CurChain: bc}
	if e = bc.Txpool.EnableMigrationQueueJournal(journal, account); e != nil {
		t.Fatal(e)
	}
	if n.pollSourceHandoff() == nil {
		t.Fatal("recovered late transaction was skipped")
	}
	raw, _ := json.Marshal(active)
	if e = os.WriteFile(activationPath, raw, 0600); e != nil {
		t.Fatal(e)
	}
	raw, _ = json.Marshal(map[string]interface{}{"activation": active, "block": 2, "block_hash": strings.Repeat("a", 64), "state_root": strings.Repeat("b", 64)})
	if e = os.WriteFile(receiptPath, raw, 0600); e != nil {
		t.Fatal(e)
	}
	if e = n.pollSourceHandoff(); e != nil {
		t.Fatal(e)
	}
	records, e := core.LoadMigrationOutbox(outbox)
	if e != nil || len(records) != 1 {
		t.Fatal("late forwarding missing", e)
	}
	if len(bc.Txpool.PackTxs(4)) != 0 || len(bc.Txpool.SnapshotMigrationTransactions(account)) != 1 {
		t.Fatal("late source execution or lost hold")
	}
	if e = n.pollSourceHandoff(); e != nil {
		t.Fatal("late forwarding replay failed", e)
	}
}
