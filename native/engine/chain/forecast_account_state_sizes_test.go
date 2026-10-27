package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"math/big"
	"path/filepath"
	"testing"
)

func TestForecastStateSizesDoNotSynthesizeMissingAccounts(t *testing.T) {
	t.Setenv("SAA_INTEGRATED_CONTROLLER", "forecast")
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	defer func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	}()
	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + string(filepath.Separator)
	params.ShardNum = 2
	source := newMigrationChain(t, params.DatabaseWrite_path, 0)
	defer source.CloseBlockChain()
	account := "0x0000000000000000000000000000000000000011"
	missing := "0x0000000000000000000000000000000000000022"
	source.Update_PartitionMap(account, 0)
	state := &core.AccountState{Nonce: 4, Balance: big.NewInt(77)}
	source.AddAccounts([]string{account}, []*core.AccountState{state}, 0)
	block := &core.Block{Header: source.CurrentBlock.Header, Body: []*core.Transaction{{Sender: account, Recipient: missing}}}
	sizes, stateRoot, status := source.ForecastBlockAccountStateSizes(block)
	if sizes[account] != uint64(len(state.Encode())) || stateRoot == "" || status != "committed_touched_account_trie_payload_only" {
		t.Fatal(sizes, stateRoot, status)
	}
	if _, ok := sizes[missing]; ok {
		t.Fatal("missing state synthesized")
	}
}
