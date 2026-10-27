package core

import (
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEmptyRestartQueueDoesNotProveHandoffCompletion(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	b := "0000000000000000000000000000000000000011"
	tx := NewTransaction(a, b, big.NewInt(3), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.RoutedRecipientShard = 1
	h, err := BuildMigrationHandoff(tx, "p", a, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = PersistMigrationHandoff(dir, h); err != nil {
		t.Fatal(err)
	}
	restartedPool := NewTxPool()
	audit, err := AuditMigrationHandoffs(dir, "p", a)
	if err != nil || restartedPool.GetTxQueueLen() != 0 || audit.Persisted != 1 || audit.Confirmed != 0 || len(audit.Pending) != 1 {
		t.Fatalf("empty memory queue concealed durable obligation: %+v %v", audit, err)
	}
	if err = os.WriteFile(filepath.Join(dir, h.ID+".confirmed.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = AuditMigrationHandoffs(dir, "p", a); err == nil {
		t.Fatal("invalid confirmation accepted as completion")
	}
}
