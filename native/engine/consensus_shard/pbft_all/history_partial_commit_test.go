package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"encoding/hex"
	"encoding/json"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"path/filepath"
	"testing"
	"time"
)

// Regression for a successful prefix followed by a rejected preparation.
func TestHistoryPartialCommitRetainsSuccessfulProgress(t *testing.T) {
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
	t.Setenv("SAA_NATIVE_SOURCE_PREPARED_PATH", "")
	open := func() *chain.BlockChain {
		db, err := rawdb.NewLevelDBDatabase(filepath.Join(root, "trie"), 0, 1, "partial-history", false)
		if err != nil {
			t.Fatal(err)
		}
		bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, db)
		if err != nil {
			t.Fatal(err)
		}
		return bc
	}
	bc := open()
	defer func() {
		if bc != nil {
			bc.CloseBlockChain()
		}
	}()
	const account = "0000000000000000000000000000000000000010"
	bc.Update_PartitionMap(account, 0)
	bc.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	before := bc.CurrentBlock.Header.Number
	oldHead := hex.EncodeToString(bc.CurrentBlock.Hash)
	state, err := bc.ExportAccountState(account)
	if err != nil {
		t.Fatal(err)
	}
	command := core.SourcePrepareCommand{ProposalID: "first", Account: account, TargetShard: 1, ExpectedBlockHash: oldHead, ExpectedStateRoot: hex.EncodeToString(state.SourceRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)}
	makeRequest := func(c core.SourcePrepareCommand) *message.Request {
		data, _ := json.Marshal(c)
		return &message.Request{RequestType: sourcePrepareRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: data}}
	}
	first := makeRequest(command)
	command.ProposalID = "conflicting-second"
	second := makeRequest(command)
	n := historyIngressNode(&historyTestHook{})
	n.CurChain = bc
	n.ihm = &RawRelayPbftExtraHandleMod{pbftNode: n}
	data := historyResponseForTest(n, &message.SendOldMessage{SeqStartHeight: 2, SeqEndHeight: 3, OldRequest: []*message.Request{first, second}})
	var failure any
	func() { defer func() { failure = recover() }(); n.handleSendOldSeq(data) }()
	if failure != nil {
		t.Fatalf("unexpected failure: %v", failure)
	}
	prepared := bc.SourcePreparation()
	if prepared == nil || prepared.Command.ProposalID != "first" || bc.CurrentBlock.Header.Number != before+1 || hex.EncodeToString(bc.CurrentBlock.Hash) == oldHead {
		t.Fatal("first durable write not reproduced")
	}
	if n.sequenceID != 3 || len(n.requestPool) != 1 || len(n.height2Digest) != 1 || len(n.isReply) != 1 || !n.hasCommittedBlock.Load() {
		t.Fatal("unexpected progress tracking behavior")
	}
	if n.historyPending != nil {
		t.Fatal("failed attempt not retired")
	}
	persistedHead := hex.EncodeToString(bc.CurrentBlock.Hash)
	bc.CloseBlockChain()
	bc = nil
	bc = open()
	if bc.SourcePreparation() == nil || bc.SourcePreparation().Command.ProposalID != "first" || hex.EncodeToString(bc.CurrentBlock.Hash) != persistedHead {
		t.Fatal("partial write did not survive reopen")
	}
	// Simulate missing volatile progress after durable state application.
	// Exercise the same startup hook called by the production constructor,
	// with its actual initial sequence (chain height + 1), without replay input.
	startup := historyIngressNode(&historyTestHook{})
	startup.CurChain = bc
	startup.sequenceID = bc.CurrentBlock.Header.Number + 1
	if err := startup.restorePreparedRequestProgress(); err != nil {
		t.Fatal(err)
	}
	digestKey := string(getDigest(first))
	if startup.sequenceID != 3 || startup.height2Digest[2] != digestKey || startup.requestPool[digestKey] == nil || !startup.isReply[digestKey] || !startup.hasCommittedBlock.Load() {
		t.Fatal("production startup hook did not restore committed request")
	}
	if err := startup.restorePreparedRequestProgress(); err != nil || len(startup.requestPool) != 1 || hex.EncodeToString(bc.CurrentBlock.Hash) != persistedHead {
		t.Fatal("startup restoration not idempotent")
	}
	recovered := historyIngressNode(&historyTestHook{})
	recovered.CurChain = bc
	recovered.ihm = &RawRelayPbftExtraHandleMod{pbftNode: recovered}
	replay := historyResponseForTest(recovered, &message.SendOldMessage{SeqStartHeight: 2, SeqEndHeight: 2, OldRequest: []*message.Request{first}})
	recovered.handleSendOldSeq(replay)
	if recovered.sequenceID != 3 || len(recovered.height2Digest) != 1 || hex.EncodeToString(bc.CurrentBlock.Hash) != persistedHead {
		t.Fatal("durable marker replay failed or repeated state write")
	}
	// A different sequence cannot reuse the same prepared proposal as a new commit.
	if recovered.ihm.HandleforSequentialRequest(&message.SendOldMessage{SeqStartHeight: 3, SeqEndHeight: 3, OldRequest: []*message.Request{first}}) {
		t.Fatal("same preparation accepted under a different sequence")
	}
	t.Log("successful prefix retained; failed suffix not indexed; durable marker replay rebuilt progress without rewriting state")
}
