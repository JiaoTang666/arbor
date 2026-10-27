package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/hex"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"path/filepath"
	"testing"
)

func TestPreparedStartupRejectsLegacyAndMalformedWithoutProgress(t *testing.T) {
	for _, kind := range []string{"legacy", "malformed"} {
		t.Run(kind, func(t *testing.T) {
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
			bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
			if err != nil {
				t.Fatal(err)
			}
			defer bc.CloseBlockChain()
			account := "0000000000000000000000000000000000000010"
			bc.Update_PartitionMap(account, 0)
			bc.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
			s, err := bc.ExportAccountState(account)
			if err != nil {
				t.Fatal(err)
			}
			c := core.SourcePrepareCommand{ProposalID: "old", Account: account, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(bc.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(s.SourceRoot), ExpectedStateHash: hex.EncodeToString(s.StateHash)}
			var raw []byte
			if kind == "malformed" {
				raw = []byte("not-json")
			}
			if _, err = bc.CommitSourcePreparation(c, 2, make([]byte, 32), raw); err != nil {
				t.Fatal(err)
			}
			n := historyIngressNode(&historyTestHook{})
			n.CurChain = bc
			n.sequenceID = bc.CurrentBlock.Header.Number + 1
			head := hex.EncodeToString(bc.CurrentBlock.Hash)
			seq := n.sequenceID
			err = n.restorePreparedRequestProgress()
			if err == nil {
				t.Fatal("unsafe startup accepted")
			}
			if n.sequenceID != seq || len(n.requestPool) != 0 || len(n.height2Digest) != 0 || len(n.isReply) != 0 || n.hasCommittedBlock.Load() || hex.EncodeToString(bc.CurrentBlock.Hash) != head || bc.SourcePreparation() == nil {
				t.Fatal("failed startup mutated progress or source")
			}
			t.Logf("%s rejected without state/index mutation: %v", kind, err)
		})
	}
}
