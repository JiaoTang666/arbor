package committee

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConsumeNativeInstallationReceiptsIncremental(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "receipts.jsonl")
	if err := os.WriteFile(path, []byte(`{"proposal_id":"p1"}
{"proposal_id":"p2"}`), 0644); err != nil {
		t.Fatal(err)
	}
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, stableShardByAccount: map[string]uint64{}}
	var offset int64
	got, err := m.ConsumeNativeInstallationReceipts(path, &offset)
	if err != nil || got != 1 {
		t.Fatalf("first consume got=%d err=%v", got, err)
	}
	if offset <= 0 {
		t.Fatal("offset did not advance")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("\n")
	_ = f.Close()
	got, err = m.ConsumeNativeInstallationReceipts(path, &offset)
	if err != nil || got != 1 {
		t.Fatalf("second consume got=%d err=%v", got, err)
	}
	state := m.integratedStateLocked()
	if !state.authority.installationReceipts["p1"] || !state.authority.installationReceipts["p2"] {
		t.Fatal("receipts missing")
	}
}

func TestConsumeNativeInstallationReceiptsRejectsUncommittedWhenGated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "receipts.jsonl")
	if err := os.WriteFile(path, []byte(`{"proposal_id":"pending","committed":false}
`), 0644); err != nil {
		t.Fatal(err)
	}
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, stableShardByAccount: map[string]uint64{}}
	s := m.integratedStateLocked()
	s.authority.enableInstallationReceiptGate()
	var offset int64
	if n, err := m.ConsumeNativeInstallationReceipts(path, &offset); err != nil || n != 0 {
		t.Fatalf("uncommitted receipt consumed: n=%d err=%v", n, err)
	}
}
