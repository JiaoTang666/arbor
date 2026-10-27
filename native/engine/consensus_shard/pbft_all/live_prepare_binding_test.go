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
)

func TestLivePrepareBindsAfterHeadAdvance(t *testing.T) {
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	})
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "db") + "/"
	params.ShardNum = 2
	bc, e := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
	if e != nil {
		t.Fatal(e)
	}
	defer bc.CloseBlockChain()
	a := "0000000000000000000000000000000000000010"
	bc.Update_PartitionMap(a, 0)
	bc.AddAccounts([]string{a}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	before := hex.EncodeToString(bc.CurrentBlock.Hash)
	c := core.SourcePrepareCommand{ProposalID: "live", Account: a, SourceShard: 0, TargetShard: 1, BindAtProposal: true}
	path := filepath.Join(t.TempDir(), "intent.json")
	t.Setenv("SAA_NATIVE_SOURCE_PREPARE_PATH", path)
	t.Setenv("SAA_NATIVE_SOURCE_PREPARED_PATH", "")
	t.Setenv("SAA_NATIVE_SOURCE_QUEUE_ROOT", t.TempDir())
	raw, _ := json.Marshal(c)
	if e = core.PublishSourcePrepareFile(path, append(raw, '\n')); e != nil {
		t.Fatal(e)
	}
	bc.AddAccounts([]string{a}, []*core.AccountState{{Balance: big.NewInt(101)}}, 0)
	if before == hex.EncodeToString(bc.CurrentBlock.Hash) {
		t.Fatal("fixture did not advance")
	}
	n := &PbftConsensusNode{CurChain: bc, requestPool: make(map[string]*message.Request)}
	h := &RawRelayPbftExtraHandleMod{pbftNode: n}
	n.ihm = h
	r, e := n.sourcePrepareProposal()
	if e != nil || r == nil {
		t.Fatal(e)
	}
	bound, e := decodeSourcePrepare(r)
	if e != nil || bound.ExpectedBlockHash != hex.EncodeToString(bc.CurrentBlock.Hash) || bound.ExpectedStateHash == "" {
		t.Fatal("not bound to latest committed snapshot")
	}
	if bc.SourcePreparation() != nil {
		t.Fatal("proposal fenced before commit")
	}
	retry, e := n.sourcePrepareProposal()
	if e != nil || hex.EncodeToString(getDigest(retry)) != hex.EncodeToString(getDigest(r)) {
		t.Fatal("retry changed request")
	}
	bad := bound
	bad.ExpectedStateHash = before
	if bc.ValidateSourcePreparation(bad) == nil {
		t.Fatal("hash mismatch accepted")
	}
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: r, Digest: getDigest(r)}) {
		t.Fatal("preprepare failed")
	}
	if !h.HandleinCommit(&message.Commit{Digest: getDigest(r), SeqID: 2}) {
		t.Fatal("commit failed")
	}
	if n.hasUnattemptedSourcePreparation() {
		t.Fatal("committed intent still wakes")
	}
	fresh := &PbftConsensusNode{CurChain: bc, requestPool: make(map[string]*message.Request)}
	if rr, e := fresh.sourcePrepareProposal(); e != nil || rr != nil {
		t.Fatal("committed intent recovery failed", e)
	}
}
