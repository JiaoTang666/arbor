package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestGroupHandoffBothEndpointsAndLateRelay(t *testing.T) {
	a, b := "0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020"
	tx := NewTransaction(a, b, big.NewInt(10), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, e := BuildGroupMigrationHandoff(tx, "g", []string{a, b}, 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	ex := DecodeTx(h.Execution)
	if ex.RoutedSenderShard != 1 || ex.RoutedRecipientShard != 1 || !bytes.Equal(ex.TxHash, tx.TxHash) {
		t.Fatal("incorrect group placement")
	}
	if e = ValidateMigrationHandoff(h); e != nil {
		t.Fatal(e)
	}
	other, e := BuildGroupMigrationHandoff(tx, "g", []string{a, b}, 0, 1, 1)
	if e != nil || h.ID != other.ID {
		t.Fatal("unstable group identity")
	}
	bad := *h
	bad.GroupAccounts = []string{a, "0000000000000000000000000000000000000030"}
	if ValidateMigrationHandoff(&bad) == nil {
		t.Fatal("changed membership accepted")
	}
	tx.Relayed = true
	credit, e := BuildGroupMigrationHandoff(tx, "g", []string{a, b}, 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	if credit.Account != b || credit.Leg != "credit" || !DecodeTx(credit.Execution).Relayed {
		t.Fatal("late relay became debit")
	}
}

func TestGroupHandoffConfirmationRejectsMissingRelay(t *testing.T) {
	a, b, c := "0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020", "0000000000000000000000000000000000000030"
	tx := NewTransaction(a, c, big.NewInt(1), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, e := BuildGroupMigrationHandoff(tx, "g", []string{a, b}, 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256(h.Execution)
	ack := HandoffAck{ID: h.ID, ProposalID: "g", TargetShard: 1, ExecutionHash: hex.EncodeToString(hash[:]), Block: 1, BlockHash: strings.Repeat("1", 64), StateRoot: strings.Repeat("2", 64), Executed: true}
	if ValidateHandoffAck(h, ack) == nil {
		t.Fatal("cross-shard first leg claimed complete")
	}
	ack.RelayPending = true
	p := NewTxPool()
	p.AddTx2Pool(tx)
	dir := t.TempDir()
	if e = PersistMigrationHandoff(dir, h); e != nil {
		t.Fatal(e)
	}
	if p.ConfirmMigrationHandoff(dir, h, ack) == nil || p.GetTxQueueLen() != 1 {
		t.Fatal("dequeued before relay completion")
	}
}
