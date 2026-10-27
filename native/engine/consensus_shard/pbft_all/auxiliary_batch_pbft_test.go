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

func auxiliaryPBFTFixture(t *testing.T) (*chain.BlockChain, *chain.BlockChain, core.SourcePrepareCommand) {
	t.Helper()
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() { params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum = oldRoot, oldDB, oldShards })
	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "blocks") + "/"
	params.ShardNum = 2
	newChain := func(id uint64) *chain.BlockChain {
		bc, e := chain.NewBlockChain(&params.ChainConfig{ShardID: id, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(bc.CloseBlockChain)
		return bc
	}
	source, target := newChain(0), newChain(1)
	account := "0000000000000000000000000000000000000010"
	source.Update_PartitionMap(account, 0)
	source.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	state, e := source.ExportAccountState(account)
	if e != nil {
		t.Fatal(e)
	}
	cmd := core.SourcePrepareCommand{ProposalID: "batch-pbft", Account: account, SourceShard: 0, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(source.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(state.SourceRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)}
	p, e := source.CommitSourcePreparation(cmd, 2, make([]byte, 32))
	if e != nil {
		t.Fatal(e)
	}
	installed, e := target.CommitPreparedTarget(*p, 1, make([]byte, 32))
	if e != nil {
		t.Fatal(e)
	}
	if e = target.CommitTargetActivation(chain.TargetActivation{TargetShard: 1, ProposalID: cmd.ProposalID, Account: cmd.Account, MappingVersion: 1, InstallDigest: installed.CommitDigest, StateHash: p.StateHash}); e != nil {
		t.Fatal(e)
	}
	return source, target, cmd
}

func verifyAuxiliaryPBFT(t *testing.T, bc *chain.BlockChain, kind message.RequestType, body interface{}) {
	t.Helper()
	payload, e := json.Marshal(body)
	if e != nil {
		t.Fatal(e)
	}
	r := &message.Request{RequestType: kind, Msg: message.RawMessage{Content: payload}}
	n := &PbftConsensusNode{ShardID: bc.ChainConfig.ShardID, CurChain: bc, requestPool: map[string]*message.Request{}}
	h := &RawRelayPbftExtraHandleMod{pbftNode: n}
	n.ihm = h
	digest := getDigest(r)
	head := append([]byte(nil), bc.CurrentBlock.Hash...)
	height := bc.CurrentBlock.Header.Number
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: r, Digest: digest, SeqID: height + 1}) {
		t.Fatal("batch preprepare rejected")
	}
	if !bytes.Equal(head, bc.CurrentBlock.Hash) {
		t.Fatal("preprepare wrote state")
	}
	h.HandleinCommit(&message.Commit{Digest: digest, SeqID: height + 1})
	if bc.CurrentBlock.Header.Number != height+1 {
		t.Fatal("batch did not commit exactly one block")
	}
	head = append([]byte(nil), bc.CurrentBlock.Hash...)
	h.HandleinCommit(&message.Commit{Digest: digest, SeqID: height + 1})
	if !bytes.Equal(head, bc.CurrentBlock.Hash) {
		t.Fatal("duplicate PBFT commit changed root")
	}
}

func TestAuxiliaryBatchPBFTPrePrepareCommitAndReplay(t *testing.T) {
	source, target, cmd := auxiliaryPBFTFixture(t)
	sender := "0000000000000000000000000000000000000020"
	source.Update_PartitionMap(sender, 0)
	source.AddAccounts([]string{sender}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	items := []chain.InboundDebitItem{}
	for i := 0; i < 2; i++ {
		tx := core.NewTransaction(sender, cmd.Account, big.NewInt(7), uint64(i+1), time.Unix(1, 0))
		tx.RoutingFrozen = true
		items = append(items, chain.InboundDebitItem{Transaction: tx, MappingVersion: 1})
	}
	verifyAuxiliaryPBFT(t, source, inboundDebitBatchRequest, items)
	for _, item := range items {
		r, e := source.FindInboundDebit(item.Transaction, 1)
		if e != nil || r == nil {
			t.Fatal("durable inbound credit missing")
		}
		if _, e = target.CommitMigrationHandoff(r.Credit); e != nil {
			t.Fatal(e)
		}
	}

	recipient := "0000000000000000000000000000000000000030"
	firsts := []chain.HandoffExecution{}
	for i := 0; i < 2; i++ {
		tx := core.NewTransaction(cmd.Account, recipient, big.NewInt(7), uint64(i+3), time.Unix(1, 0))
		tx.RoutingFrozen = true
		h, e := core.BuildMigrationHandoff(tx, cmd.ProposalID, cmd.Account, 0, 1, 1)
		if e != nil {
			t.Fatal(e)
		}
		first, e := target.CommitMigrationHandoff(*h)
		if e != nil {
			t.Fatal(e)
		}
		firsts = append(firsts, *first)
	}
	verifyAuxiliaryPBFT(t, source, handoffRelayBatchRequest, firsts)
	credited, e := source.ExportAccountState(recipient)
	if e != nil || credited.State.Balance.Cmp(new(big.Int).Add(new(big.Int).Set(params.Init_Balance), big.NewInt(14))) != 0 {
		t.Fatal("PBFT relay credit lost")
	}
}
