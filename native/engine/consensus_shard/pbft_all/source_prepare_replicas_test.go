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
	"path/filepath"
	"testing"
	"time"
)

// Two independent stores, same ordered request; not a quorum/network test.
func TestSourcePreparationDeterministicAcrossReplicas(t *testing.T) {
	runSourcePreparationReplicaComparison(t, false)
}

func TestSourcePreparationHistoricalCatchupMatchesCommit(t *testing.T) {
	runSourcePreparationReplicaComparison(t, true)
}

func runSourcePreparationReplicaComparison(t *testing.T, catchup bool) {
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
	const account = "0000000000000000000000000000000000000010"
	var nodes []*PbftConsensusNode
	for id := uint64(0); id < 2; id++ {
		dir := filepath.Join(root, []string{"replica0", "replica1"}[id])
		db, err := rawdb.NewLevelDBDatabase(dir, 0, 1, "replica", false)
		if err != nil {
			t.Fatal(err)
		}
		bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, NodeID: id, ShardNums: 2, Nodes_perShard: 2, BlockSize: 4}, db)
		if err != nil {
			t.Fatal(err)
		}
		defer bc.CloseBlockChain()
		bc.Update_PartitionMap(account, 0)
		bc.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(100), Nonce: 4}}, 0)
		n := &PbftConsensusNode{CurChain: bc, NodeID: id, requestPool: make(map[string]*message.Request)}
		n.ihm = &RawRelayPbftExtraHandleMod{pbftNode: n}
		nodes = append(nodes, n)
	}
	left, right := nodes[0].CurChain, nodes[1].CurChain
	if !bytes.Equal(left.CurrentBlock.Hash, right.CurrentBlock.Hash) || !bytes.Equal(left.CurrentBlock.Header.StateRoot, right.CurrentBlock.Header.StateRoot) {
		t.Fatal("replicas do not start from identical committed state")
	}
	state, err := left.ExportAccountState(account)
	if err != nil {
		t.Fatal(err)
	}
	c := core.SourcePrepareCommand{ProposalID: "replica-p", Account: account, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(left.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(state.SourceRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)}
	data, _ := json.Marshal(c)
	request := &message.Request{RequestType: sourcePrepareRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: data}}
	digest := getDigest(request)
	for _, n := range nodes {
		h := n.ihm.(*RawRelayPbftExtraHandleMod)
		if catchup && n.NodeID == 1 {
			if n.CurChain.SourcePreparation() != nil {
				t.Fatal("lagging replica already prepared")
			}
			// Use the existing historical-request hook, not the normal Commit hook.
			history := &message.SendOldMessage{SeqStartHeight: 2, SeqEndHeight: 2, OldRequest: []*message.Request{request}}
			if !h.HandleforSequentialRequest(history) || n.sequenceID != 3 {
				t.Fatal("catchup did not advance sequence")
			}
			continue
		}
		if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: request, Digest: digest}) {
			t.Fatal("preprepare rejected")
		}
		if n.CurChain.SourcePreparation() != nil {
			t.Fatal("preprepare mutated source")
		}
		if !h.HandleinCommit(&message.Commit{Digest: digest, SeqID: 2}) {
			t.Fatal("commit rejected")
		}
	}
	lp, _ := json.Marshal(left.SourcePreparation())
	rp, _ := json.Marshal(right.SourcePreparation())
	if !bytes.Equal(lp, rp) || !bytes.Equal(left.CurrentBlock.Header.StateRoot, right.CurrentBlock.Header.StateRoot) || !bytes.Equal(left.CurrentBlock.Hash, right.CurrentBlock.Hash) {
		t.Fatal("same request produced different replica state")
	}
	head := append([]byte(nil), right.CurrentBlock.Hash...)
	if catchup {
		nodes[1].ihm.HandleforSequentialRequest(&message.SendOldMessage{SeqStartHeight: 2, SeqEndHeight: 2, OldRequest: []*message.Request{request}})
		if nodes[1].sequenceID != 3 {
			t.Fatal("duplicate history changed next sequence")
		}
	} else {
		nodes[1].ihm.(*RawRelayPbftExtraHandleMod).HandleinCommit(&message.Commit{Digest: digest, SeqID: 2})
	}
	if !bytes.Equal(head, right.CurrentBlock.Hash) {
		t.Fatal("duplicate commit changed follower state")
	}
	for _, n := range nodes {
		actual, err := n.CurChain.ExportAccountState(account)
		if err != nil || !bytes.Equal(actual.StateHash, state.StateHash) || n.CurChain.Get_PartitionMap(account) != 0 {
			t.Fatal("preparation changed account or route")
		}
	}
	t.Logf("replicas=2 identical block=%x state_root=%x preparation_bytes=%d", left.CurrentBlock.Hash, left.CurrentBlock.Header.StateRoot, len(lp))
}
