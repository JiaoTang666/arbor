package pbft_all_test

import (
	"os"
	"path/filepath"
	"testing"

	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/supervisor/committee"
)

func TestNativeReceiptCrossPackageRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipts.jsonl")
	t.Setenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH", path)
	pbft_all.EmitNativeInstallationReceipt("proposal-x", "acct-x", "hash-x")
	m := &committee.RuntimeUnknownAdmissionManager{}
	var offset int64
	if n, err := m.ConsumeNativeInstallationReceipts(path, &offset); err != nil || n != 1 {
		t.Fatalf("consume n=%d err=%v", n, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
