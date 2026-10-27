package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"encoding/hex"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommittedReceiptReadsInstalledTrieInsteadOfPayload(t *testing.T) {
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	})
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "db") + "/"
	params.ShardNum = 2
	cfg := &params.ChainConfig{ShardID: 1, Nodes_perShard: 1, ShardNums: 2, BlockSize: 4, BlockInterval: 1, InjectSpeed: 4}
	bc, err := chain.NewBlockChain(cfg, rawdb.NewMemoryDatabase())
	if err != nil {
		t.Fatal(err)
	}
	defer bc.CloseBlockChain()
	account := strings.Repeat("a", 40)
	state := &core.AccountState{AcAddress: account, Nonce: 4, Balance: big.NewInt(77)}
	e := message.StateTransferEnvelope{ProposalID: "unit-proposal", Account: account, TargetShard: 1,
		SourceStateRoot: strings.Repeat("1", 64), StateHash: hex.EncodeToString(state.Hash())}
	digest := make([]byte, 32) // Unit commit context, not a native consensus certificate.
	if _, err = committedInstallationRecord(bc, e, 1, 5, digest); err == nil {
		t.Fatal("uninstalled payload acknowledged")
	}
	root, err := bc.ImportAccountState(&chain.AccountStateMigration{Address: account, State: state, StateHash: state.Hash()})
	if err != nil {
		t.Fatal(err)
	}
	record, err := committedInstallationRecord(bc, e, 1, 5, digest)
	if err != nil {
		t.Fatal(err)
	}
	if record["target_state_root"] != hex.EncodeToString(root) || record["install_block"] != bc.CurrentBlock.Header.Number || record["account"] != "0x"+account {
		t.Fatalf("wrong receipt: %v", record)
	}
	e.StateHash = strings.Repeat("2", 64)
	if _, err = committedInstallationRecord(bc, e, 1, 5, digest); err == nil {
		t.Fatal("wrong installed state acknowledged")
	}
}
