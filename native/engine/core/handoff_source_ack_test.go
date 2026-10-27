package core

import (
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSourceDequeueRequiresPersistedCompleteMatchingAck(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	b := "0000000000000000000000000000000000000011"
	tx := NewTransaction(a, b, big.NewInt(3), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.RoutedRecipientShard = 1
	h, err := BuildMigrationHandoff(tx, "p", a, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	pool := NewTxPool()
	pool.AddTx2Pool(tx)
	pool.HoldSourceAccount(a)
	dir := t.TempDir()
	sum := sha256.Sum256(h.Execution)
	ack := HandoffAck{ID: h.ID, ProposalID: "p", TargetShard: 1, ExecutionHash: hex.EncodeToString(sum[:]), Block: 3, BlockHash: strings.Repeat("1", 64), StateRoot: strings.Repeat("2", 64), Executed: true}
	if pool.ConfirmMigrationHandoff(dir, h, ack) == nil || pool.GetTxQueueLen() != 1 {
		t.Fatal("missing outbox accepted")
	}
	if err = PersistMigrationHandoff(dir, h); err != nil {
		t.Fatal(err)
	}
	bad := ack
	bad.RelayPending = true
	if pool.ConfirmMigrationHandoff(dir, h, bad) == nil || pool.GetTxQueueLen() != 1 {
		t.Fatal("pending relay dequeued")
	}
	bad = ack
	bad.ExecutionHash = strings.Repeat("3", 64)
	if pool.ConfirmMigrationHandoff(dir, h, bad) == nil || pool.GetTxQueueLen() != 1 {
		t.Fatal("wrong execution dequeued")
	}
	if err = pool.ConfirmMigrationHandoff(dir, h, ack); err != nil || pool.GetTxQueueLen() != 0 {
		t.Fatal("confirmed item not dequeued")
	}
	if _, err = os.Stat(filepath.Join(dir, h.ID+".confirmed.json")); err != nil {
		t.Fatal(err)
	}
	if err = pool.ConfirmMigrationHandoff(dir, h, ack); err != nil {
		t.Fatal("ack replay failed")
	}
	pool.AddTx2Pool(tx)
	pool.AddTx2Pool(tx)
	if pool.ConfirmMigrationHandoff(dir, h, ack) == nil || pool.GetTxQueueLen() != 2 {
		t.Fatal("ambiguous occurrences removed")
	}
}
