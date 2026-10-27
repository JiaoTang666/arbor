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
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This tests production handlers, not multi-process consensus.
func TestRelayRedirectPBFTReservationCreditAndParentClosure(t *testing.T) {
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() { params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum = oldRoot, oldDB, oldShards })
	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "blocks") + "/"
	params.ShardNum = 2
	for _, key := range []string{"SAA_NATIVE_HANDOFF_ACK_DIR", "SAA_NATIVE_HANDOFF_RELAY_DIR"} {
		dir := filepath.Join(root, key)
		if e := os.MkdirAll(dir, 0700); e != nil {
			t.Fatal(e)
		}
		t.Setenv(key, dir)
	}
	nodes := make([]*PbftConsensusNode, 2)
	for i := range nodes {
		bc, e := chain.NewBlockChain(&params.ChainConfig{ShardID: uint64(i), ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(bc.CloseBlockChain)
		nodes[i] = &PbftConsensusNode{ShardID: uint64(i), CurChain: bc, requestPool: map[string]*message.Request{}}
	}
	source, target := nodes[0], nodes[1]
	accounts := []string{"0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020", "0000000000000000000000000000000000000030", "0000000000000000000000000000000000000040"}
	for _, a := range accounts {
		source.CurChain.Update_PartitionMap(a, 0)
		target.CurChain.Update_PartitionMap(a, 0)
	}
	source.CurChain.AddAccounts(accounts, []*core.AccountState{{Balance: big.NewInt(100)}, {Balance: big.NewInt(100)}, {Balance: big.NewInt(100)}, {Balance: big.NewInt(100)}}, 0)
	prepare := func(id string, as []string, version, seq uint64) *core.GroupPreparation {
		c := core.GroupPrepareCommand{}
		for _, a := range as {
			s, e := source.CurChain.ExportAccountState(a)
			if e != nil {
				t.Fatal(e)
			}
			c.Members = append(c.Members, core.SourcePrepareCommand{ProposalID: id, Account: a, SourceShard: 0, TargetShard: 1, MappingVersion: version, ExpectedBlockHash: hex.EncodeToString(source.CurChain.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(s.SourceRoot), ExpectedStateHash: hex.EncodeToString(s.StateHash)})
		}
		p, e := source.CurChain.CommitGroupSourcePreparation(c, seq, make([]byte, 32), nil)
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	activate := func(p *core.GroupPreparation, seq uint64) chain.GroupTargetActivation {
		r, e := target.CurChain.CommitGroupTargetInstallation(*p, seq, make([]byte, 32))
		if e != nil {
			t.Fatal(e)
		}
		a := chain.GroupTargetActivation{}
		for _, m := range p.Members {
			a.Members = append(a.Members, chain.TargetActivation{ProposalID: m.Command.ProposalID, Account: m.Command.Account, TargetShard: 1, MappingVersion: m.Command.MappingVersion + 1, StateHash: m.StateHash, InstallDigest: r.CommitDigest})
		}
		if e = target.CurChain.CommitGroupTargetActivation(a); e != nil {
			t.Fatal(e)
		}
		return a
	}
	p := prepare("first", accounts[:2], 0, 2)
	a := activate(p, 1)
	receipt, _ := json.Marshal(map[string]interface{}{"record": a, "committed": true, "block": target.CurChain.CurrentBlock.Header.Number, "block_hash": hex.EncodeToString(target.CurChain.CurrentBlock.Hash), "state_root": hex.EncodeToString(target.CurChain.CurrentBlock.Header.StateRoot), "shard": 1})
	drain, _ := json.Marshal(map[string]interface{}{"Accounts": accounts[:2], "Account": "", "Kind": "full", "Sequence": 3, "Transactions": [][]byte{}})
	if _, e := source.CurChain.CommitGroupRetirement(chain.GroupRetirementCommand{ActivationReceipt: receipt, DrainSnapshot: drain}, 3, make([]byte, 32)); e != nil {
		t.Fatal(e)
	}
	tx := core.NewTransaction(accounts[0], accounts[2], big.NewInt(7), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, e := core.BuildGroupMigrationHandoff(tx, "first", accounts[:2], 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	first, e := target.CurChain.CommitMigrationHandoff(*h)
	if e != nil {
		t.Fatal(e)
	}
	q := prepare("second", accounts[2:], 1, 4)
	raw, _ := json.Marshal(first)
	if e = core.PublishSourcePrepareFile(filepath.Join(os.Getenv("SAA_NATIVE_HANDOFF_RELAY_DIR"), h.ID+".relay.json"), raw); e != nil {
		t.Fatal(e)
	}
	req, e := source.relayBatchProposal()
	if e != nil || req == nil || req.RequestType != relayReserveRequest {
		t.Fatal("reservation not selected", e)
	}
	verifyAuxiliaryPBFT(t, source.CurChain, relayReserveRequest, first)
	req, e = target.redirectCreditProposal()
	if e != nil || req != nil {
		t.Fatal("credit did not wait for activation", e)
	}
	activate(q, 3)
	req, e = target.redirectCreditProposal()
	if e != nil || req == nil || req.RequestType != relayCreditRequest {
		t.Fatal("credit not selected", e)
	}
	var report relayRedirectReport
	if e = json.Unmarshal(req.Msg.Content, &report); e != nil {
		t.Fatal(e)
	}
	bad := report
	bad.Shard = 1
	malformed, _ := makeRedirectRequest(relayCreditRequest, bad)
	if target.validateRedirect(malformed) == nil {
		t.Fatal("wrong source report accepted")
	}
	verifyAuxiliaryPBFT(t, target.CurChain, relayCreditRequest, report)
	state, e := target.CurChain.ExportAccountState(accounts[2])
	if e != nil || state.State.Balance.Int64() != 107 {
		t.Fatal("wrong target balance", e)
	}
	raw, e = os.ReadFile(filepath.Join(os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR"), h.ID+".relay-ack.json"))
	if e != nil {
		t.Fatal(e)
	}
	var ack core.HandoffRelayAck
	if e = json.Unmarshal(raw, &ack); e != nil {
		t.Fatal(e)
	}
	if ack.Redirect == nil {
		t.Fatal("missing source-to-target lineage")
	}
	if e = core.ValidateHandoffRelayAck(h, ack); e != nil {
		t.Fatal("parent completion rejected", e)
	}
}
