package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"encoding/json"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

func TestSingletonBindAtProposalRequiresActualCommittedAccount(t *testing.T) {
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() { params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum = oldRoot, oldDB, oldShards })
	root := t.TempDir()
	params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum = root, filepath.Join(root, "blocks")+"/", 2
	path := filepath.Join(root, "prepare.json")
	t.Setenv("SAA_GROUP_SOURCE_PREPARE_PATH", path)
	bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
	if err != nil {
		t.Fatal(err)
	}
	defer bc.CloseBlockChain()
	n := &PbftConsensusNode{ShardID: 0, CurChain: bc, sequenceID: 1, requestPool: make(map[string]*message.Request)}
	account := "0000000000000000000000000000000000000098"
	bc.Update_PartitionMap(account, 0)
	c := core.GroupPrepareCommand{Members: []core.SourcePrepareCommand{{ProposalID: "new-singleton", Account: account, SourceShard: 0, TargetShard: 1, BindAtProposal: true}}}
	raw, _ := json.Marshal(c)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if request, err := n.groupProposal(); err == nil || request != nil {
		t.Fatal("missing account silently synthesized for migration")
	}
	if bc.GroupSourcePreparation() != nil {
		t.Fatal("failed binding committed a migration")
	}
	bc.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(73)}}, 0)
	request, err := n.groupProposal()
	if err != nil || request == nil {
		t.Fatal("newly committed account cannot bind", err)
	}
	var bound core.GroupPrepareCommand
	if err := json.Unmarshal(request.Msg.Content, &bound); err != nil {
		t.Fatal(err)
	}
	if len(bound.Members) != 1 || bound.Members[0].ExpectedStateHash == "" || bound.Members[0].ExpectedBlockHash == "" {
		t.Fatal("actual state and block bindings missing")
	}
	if bc.GroupSourcePreparation() != nil {
		t.Fatal("proposal observation committed state before PBFT")
	}
}
