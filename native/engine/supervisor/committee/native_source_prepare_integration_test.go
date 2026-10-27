package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

// Real chain writes and exported bytes; the commit context is supplied by this
// test, not obtained from a network quorum. This is NOT native PBFT evidence.
func TestChainPreparedResultThroughSEFWithoutTargetReceipt(t *testing.T) {
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
	db, err := rawdb.NewLevelDBDatabase(filepath.Join(root, "trie"), 0, 1, "sef-integration", false)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, db)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.CloseBlockChain()
	account := "0000000000000000000000000000000000000010"
	canonical := "0x" + account
	bc.Update_PartitionMap(account, 0)
	bc.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(77), Nonce: 4}}, 0)
	before, err := bc.ExportAccountState(account)
	if err != nil {
		t.Fatal(err)
	}
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, softDecisionMode: saaIntegratedDecisionMode, stableShardByAccount: map[string]uint64{canonical: 0}}
	s := m.integratedStateLocked()
	a := s.authority
	a.enableInstallationReceiptGate()
	if d := a.submit(mappingAuthorityProposal{ProposalID: "chain-sef", Addresses: []string{canonical}, TargetShard: 1, EffectiveEpoch: 2}, 1); d.Status != mappingAccepted {
		t.Fatal(d)
	}
	c := core.SourcePrepareCommand{ProposalID: "chain-sef", Account: account, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(bc.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(before.SourceRoot), ExpectedStateHash: hex.EncodeToString(before.StateHash)}
	inbox := filepath.Join(root, "command.json")
	if err = m.PublishNativeSourcePreparation(c, inbox); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(inbox)
	if err != nil {
		t.Fatal(err)
	}
	var delivered core.SourcePrepareCommand
	if err = json.Unmarshal(data, &delivered); err != nil || delivered != c {
		t.Fatal("command changed in transport")
	}
	digest := sha256.Sum256(data)
	prepared, err := bc.CommitSourcePreparation(delivered, 2, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(prepared.State, before.State.Encode()) || prepared.StateHash != hex.EncodeToString(before.StateHash) {
		t.Fatal("committed export differs from real source state")
	}
	head := hex.EncodeToString(bc.CurrentBlock.Hash)
	output, err := json.Marshal(prepared)
	if err != nil {
		t.Fatal(err)
	}
	output = append(output, '\n')
	s.nativeSourcePreparedPath = filepath.Join(root, "prepared.json")
	if err = core.PublishSourcePrepareFile(s.nativeSourcePreparedPath, output); err != nil {
		t.Fatal(err)
	}
	for _, epoch := range []int{2, 2, 3} {
		m.lock.Lock()
		m.applySAAIntegratedDueLocked(epoch)
		m.lock.Unlock()
		e, ok := a.preparedTransfers[c.ProposalID][canonical]
		if !ok || e.ProposalID != c.ProposalID || e.Account != canonical || e.SourceShard != c.SourceShard || e.TargetShard != c.TargetShard || e.MappingVersion != c.MappingVersion || e.AnchorVersion != c.AnchorVersion || e.StateHash != prepared.StateHash || e.SourceStateRoot != prepared.SourceStateRoot {
			t.Fatal("SEF binding differs from actual source record")
		}
		if a.routeVersion != 0 || a.decisionByID[c.ProposalID].Status == mappingApplied || m.stableShardByAccount[canonical] != 0 || bc.Get_PartitionMap(account) != 0 {
			t.Fatal("source evidence activated routing")
		}
		if a.canFinalizeMigration(c.ProposalID, canonical, prepared.StateHash) {
			t.Fatal("source evidence authorized cleanup")
		}
	}
	after, err := bc.ExportAccountState(account)
	if err != nil || !bytes.Equal(after.StateHash, before.StateHash) || head != hex.EncodeToString(bc.CurrentBlock.Hash) {
		t.Fatal("SEF consumption changed source chain")
	}
	t.Logf("actual source export matched SEF: proposal=%s state_hash=%s route_version=%d; target receipt absent", c.ProposalID, prepared.StateHash, a.routeVersion)
}
