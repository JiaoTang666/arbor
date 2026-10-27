package chain

import (
	"blockEmulator/core"
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestSourceRetirementPersistsWithoutReleasingExecution(t *testing.T) {
	bc, c, path := preparationFixture(t)
	p, err := bc.CommitSourcePreparation(c, 2, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	cmd := SourceRetirementCommand{Activation: TargetActivation{ProposalID: c.ProposalID, Account: c.Account, TargetShard: c.TargetShard, MappingVersion: c.MappingVersion + 1, StateHash: p.StateHash, InstallDigest: strings.Repeat("1", 64)}, ActivationReceiptHash: strings.Repeat("2", 64), Drain: core.MigrationDrainBoundary{Account: c.Account, Generation: 1, SnapshotHash: strings.Repeat("3", 64)}}
	before, _ := bc.ExportAccountState(c.Account)
	if _, err = bc.CommitSourceRetirement(cmd, 3, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	head := append([]byte(nil), bc.CurrentBlock.Hash...)
	if _, err = bc.CommitSourceRetirement(cmd, 3, make([]byte, 32)); err != nil || !bytes.Equal(head, bc.CurrentBlock.Hash) {
		t.Fatal("replay changed state")
	}
	bad := cmd
	bad.Drain.Generation++
	if _, err = bc.CommitSourceRetirement(bad, 4, make([]byte, 32)); err == nil {
		t.Fatal("conflicting retirement accepted")
	}
	bc.CloseBlockChain()
	bc = newMigrationChain(t, path, 0)
	defer bc.CloseBlockChain()
	record, err := bc.SourceRetirement()
	if err != nil || record == nil || record.Command != cmd {
		t.Fatal("retirement not recovered")
	}
	after, _ := bc.ExportAccountState(c.Account)
	if !bytes.Equal(before.StateHash, after.StateHash) {
		t.Fatal("archival source changed")
	}
	late := core.NewTransaction(c.Account, c.Account, big.NewInt(1), 1, time.Unix(1, 0))
	bc.SendTx2Pool([]*core.Transaction{late})
	if len(bc.Txpool.PackTxs(1)) != 0 || bc.CheckPreparedTransactions([]*core.Transaction{late}) == nil {
		t.Fatal("retired source regained execution")
	}
}
