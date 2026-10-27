package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

func TestSourcePrepareIdleIngressAndCommitBoundary(t *testing.T) {
	t.Setenv("SAA_NATIVE_SOURCE_QUEUE_ROOT", t.TempDir())
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	})
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "db") + "/"
	params.ShardNum = 2
	bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
	if err != nil {
		t.Fatal(err)
	}
	defer bc.CloseBlockChain()
	account := "0000000000000000000000000000000000000010"
	bc.Update_PartitionMap(account, 0)
	bc.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	state, err := bc.ExportAccountState(account)
	if err != nil {
		t.Fatal(err)
	}
	c := core.SourcePrepareCommand{ProposalID: "p", Account: account, SourceShard: 0, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(bc.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(state.SourceRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)}
	n := &PbftConsensusNode{CurChain: bc, requestPool: make(map[string]*message.Request)}
	h := &RawRelayPbftExtraHandleMod{pbftNode: n}
	n.ihm = h
	path := filepath.Join(t.TempDir(), "command.json")
	t.Setenv("SAA_NATIVE_SOURCE_PREPARE_PATH", path)
	// Optional test-only bridge: retain the real Commit handler's output for
	// the independent committee test process. Normal package tests stay isolated.
	t.Setenv("SAA_NATIVE_SOURCE_PREPARED_PATH", os.Getenv("SAA_TEST_PREPARED_BRIDGE_OUTPUT"))
	if delay, _ := n.shouldDelayEmptyProposal(); !delay {
		t.Fatal("empty node not idle")
	}
	data, _ := json.Marshal(c)
	if err = core.PublishSourcePrepareFile(path, data); err != nil {
		t.Fatal(err)
	}
	if delay, _ := n.shouldDelayEmptyProposal(); delay {
		t.Fatal("command did not wake idle node")
	}
	r, err := n.sourcePrepareProposal()
	observationDir := t.TempDir()
	t.Setenv("SAA_NATIVE_SOURCE_STATE_DIR", observationDir)
	t.Setenv("SAA_NATIVE_SOURCE_STATE_ACCOUNT", account)
	beforeObservation := hex.EncodeToString(bc.CurrentBlock.Hash)
	if err := n.exportSourceStateObservation(); err != nil {
		t.Fatal(err)
	}
	rawObservation, readErr := os.ReadFile(filepath.Join(observationDir, "s0-n0-"+beforeObservation+".json"))
	var observation sourceStateObservation
	if readErr != nil || json.Unmarshal(rawObservation, &observation) != nil || observation.BlockHash != c.ExpectedBlockHash || observation.StateRoot != c.ExpectedStateRoot || observation.StateHash != c.ExpectedStateHash || hex.EncodeToString(bc.CurrentBlock.Hash) != beforeObservation {
		t.Fatal("source observation did not match unchanged chain state")
	}
	if err != nil || r == nil {
		t.Fatalf("proposal: %v", err)
	}
	digest := getDigest(r)
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: r, Digest: digest}) {
		t.Fatal("preprepare rejected")
	}
	if bc.SourcePreparation() != nil {
		t.Fatal("preprepare prematurely fenced source")
	}
	// A retry at the proposal boundary must retain request identity.
	retry, retryErr := n.sourcePrepareProposal()
	if retryErr != nil || retry == nil || hex.EncodeToString(getDigest(retry)) != hex.EncodeToString(digest) {
		t.Fatal("uncommitted request lost or changed on retry")
	}
	// Discard volatile consensus state, retaining the committed chain and inbox.
	// This is handler reconstruction, not a full process/view-change test.
	n = &PbftConsensusNode{CurChain: bc, requestPool: make(map[string]*message.Request)}
	h = &RawRelayPbftExtraHandleMod{pbftNode: n}
	n.ihm = h
	recovered, recoverErr := n.sourcePrepareProposal()
	if recoverErr != nil || recovered == nil || hex.EncodeToString(getDigest(recovered)) != hex.EncodeToString(digest) {
		t.Fatal("reconstructed request identity changed")
	}
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: recovered, Digest: digest}) {
		t.Fatal("recovered preprepare rejected")
	}
	// Characterize the precommit crash boundary: durable local queue intent
	// alone is not a PBFT commit certificate and must not advance chain state.
	if err := n.prepareSourceQueue(c); err != nil {
		t.Fatal(err)
	}
	beforeCommitHead := hex.EncodeToString(bc.CurrentBlock.Hash)
	precommitRestart := &PbftConsensusNode{CurChain: bc}
	if err := precommitRestart.restoreSourceQueue(); err == nil {
		t.Fatal("uncommitted journal accepted as committed preparation")
	}
	if bc.SourcePreparation() != nil || precommitRestart.sourceQueueReady || hex.EncodeToString(bc.CurrentBlock.Hash) != beforeCommitHead {
		t.Fatal("precommit recovery fabricated progress")
	}
	if !h.HandleinCommit(&message.Commit{Digest: digest, SeqID: 2}) {
		t.Fatal("commit rejected")
	}
	p := bc.SourcePreparation()
	if p == nil || p.Command != c || p.PrepareSequence != 2 || p.CommitDigest != hex.EncodeToString(digest) {
		t.Fatal("committed identity mismatch")
	}
	head := hex.EncodeToString(bc.CurrentBlock.Hash)
	if !h.HandleinCommit(&message.Commit{Digest: digest, SeqID: 2}) || head != hex.EncodeToString(bc.CurrentBlock.Hash) {
		t.Fatal("duplicate commit changed chain")
	}
	if n.hasUnattemptedSourcePreparation() {
		t.Fatal("same command repeatedly wakes node")
	}
	if bc.Get_PartitionMap(account) != 0 {
		t.Fatal("preparation changed routing")
	}
	if !n.sourceQueueReady {
		t.Fatal("commit did not enable queue journal")
	}
	bc.SendTx2Pool([]*core.Transaction{core.NewTransaction(account, account, big.NewInt(1), 1, r.ReqTime)})
	bc.Txpool = core.NewTxPool()
	restarted := &PbftConsensusNode{CurChain: bc}
	if err := restarted.restoreSourceQueue(); err != nil || bc.Txpool.GetTxQueueLen() != 1 || len(bc.Txpool.PackTxs(1)) != 0 {
		t.Fatalf("startup queue recovery failed: %v", err)
	}
}

func TestSourcePreparedPublicationRecoveryFromDisk(t *testing.T) {
	t.Setenv("SAA_NATIVE_SOURCE_QUEUE_ROOT", t.TempDir())
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	})
	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "blocks") + "/"
	params.ShardNum = 2
	openChain := func() *chain.BlockChain {
		db, err := rawdb.NewLevelDBDatabase(filepath.Join(root, "trie"), 0, 1, "prepare-recovery", false)
		if err != nil {
			t.Fatal(err)
		}
		bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, db)
		if err != nil {
			t.Fatal(err)
		}
		return bc
	}
	bc := openChain()
	account := "0000000000000000000000000000000000000010"
	bc.Update_PartitionMap(account, 0)
	bc.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	state, err := bc.ExportAccountState(account)
	if err != nil {
		t.Fatal(err)
	}
	c := core.SourcePrepareCommand{ProposalID: "recovery", Account: account, SourceShard: 0, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(bc.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(state.SourceRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)}
	inbox := filepath.Join(root, "command.json")
	data, _ := json.Marshal(c)
	if err = core.PublishSourcePrepareFile(inbox, data); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAA_NATIVE_SOURCE_PREPARE_PATH", inbox)
	// Missing parent forces publication failure AFTER the durable state commit.
	out := filepath.Join(root, "missing", "prepared.json")
	t.Setenv("SAA_NATIVE_SOURCE_PREPARED_PATH", out)
	n := &PbftConsensusNode{CurChain: bc}
	r, err := n.sourcePrepareProposal()
	if err != nil || r == nil {
		t.Fatalf("proposal: %v", err)
	}
	queued := core.NewTransaction(account, account, big.NewInt(1), 7, r.ReqTime)
	queued.RoutingFrozen = true
	bc.SendTx2Pool([]*core.Transaction{queued})
	if err = n.commitSourcePrepare(r, 2, getDigest(r)); err == nil {
		t.Fatal("publication failure not surfaced")
	}
	saved := bc.SourcePreparation()
	if saved == nil {
		t.Fatal("failure occurred before commit")
	}
	expected, _ := json.Marshal(saved)
	expected = append(expected, '\n')
	head := hex.EncodeToString(bc.CurrentBlock.Hash)
	if _, err = os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("unexpected result file")
	}
	bc.CloseBlockChain()
	bc = openChain()
	defer bc.CloseBlockChain()
	n = &PbftConsensusNode{CurChain: bc}
	if err = n.restorePreparedRequestProgress(); err != nil {
		t.Fatal(err)
	}
	t.Run("missing-journal-rejects-startup", func(t *testing.T) {
		t.Setenv("SAA_NATIVE_SOURCE_QUEUE_ROOT", t.TempDir())
		if err := n.restoreSourceQueue(); err == nil {
			t.Fatal("missing journal silently accepted")
		}
		if n.sourceQueueReady || bc.Txpool.GetTxQueueLen() != 0 || hex.EncodeToString(bc.CurrentBlock.Hash) != head {
			t.Fatal("failed recovery mutated state")
		}
	})
	if err = n.restoreSourceQueue(); err != nil {
		t.Fatal(err)
	}
	if bc.Txpool.GetTxQueueLen() != 1 || !bytes.Equal(bc.Txpool.SnapshotMigrationTransactions(account)[0].Encode(), queued.Encode()) || len(bc.Txpool.PackTxs(1)) != 0 {
		t.Fatal("disk reopen lost or released prepared backlog")
	}
	n.ihm = &RawRelayPbftExtraHandleMod{pbftNode: n}
	if !n.hasUnattemptedSourcePreparation() {
		t.Fatal("restart did not expose pending publication")
	}
	// Failure must remain retryable without another state transition.
	if request, err := n.sourcePrepareProposal(); err == nil || request != nil {
		t.Fatal("missing output parent was not reported")
	}
	if !n.hasUnattemptedSourcePreparation() {
		t.Fatal("failed publication incorrectly marked complete")
	}
	if err = os.Mkdir(filepath.Dir(out), 0700); err != nil {
		t.Fatal(err)
	}
	if request, err := n.sourcePrepareProposal(); err != nil || request != nil {
		t.Fatalf("repair: request=%v err=%v", request, err)
	}
	actual, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(actual, expected) {
		t.Fatal("repaired evidence differs from original committed record")
	}
	if head != hex.EncodeToString(bc.CurrentBlock.Hash) || bc.Get_PartitionMap(account) != 0 {
		t.Fatal("repair mutated chain or routing")
	}
	if n.hasUnattemptedSourcePreparation() {
		t.Fatal("successful repair keeps waking")
	}
	// A further handler reconstruction republishes idempotently, not a new prepare.
	n = &PbftConsensusNode{CurChain: bc}
	if request, err := n.sourcePrepareProposal(); err != nil || request != nil {
		t.Fatal("idempotent republish failed")
	}
	if head != hex.EncodeToString(bc.CurrentBlock.Hash) {
		t.Fatal("republish added a block")
	}
}
