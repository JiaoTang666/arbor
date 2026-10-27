package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"path/filepath"
	"testing"
)

func TestBlockApplicationRejectsGapAndWrongParentWithoutAdvancing(t *testing.T) {
	oldDB, oldShards := params.DatabaseWrite_path, params.ShardNum
	defer func() { params.DatabaseWrite_path, params.ShardNum = oldDB, oldShards }()
	params.DatabaseWrite_path = filepath.Join(t.TempDir(), "database") + string(filepath.Separator)
	params.ShardNum = 2
	bc := newMigrationChain(t, params.DatabaseWrite_path, 0)
	defer bc.CloseBlockChain()
	head := bc.CurrentBlock
	for _, b := range []*core.Block{
		{Header: &core.BlockHeader{Number: head.Header.Number + 2, ParentBlockHash: head.Hash}},
		{Header: &core.BlockHeader{Number: head.Header.Number + 1, ParentBlockHash: []byte("wrong-parent")}},
	} {
		if err := bc.IsValidBlock(b); err == nil {
			t.Fatal("invalid block passed proposal validation")
		}
		if err := bc.AddBlock(b); err == nil {
			t.Fatal("invalid block application reported success")
		}
		if bc.CurrentBlock != head {
			t.Fatal("rejected block changed ledger head")
		}
		stored, err := bc.Storage.GetNewestBlockHash()
		if err != nil || string(stored) != string(head.Hash) {
			t.Fatal("rejected block changed durable ledger head")
		}
	}
}
