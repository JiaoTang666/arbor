package committee

import "testing"

func TestRecordNativeInstallationReceiptValidatesInput(t *testing.T) {
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, stableShardByAccount: map[string]uint64{}}
	if err := m.RecordNativeInstallationReceipt(""); err == nil {
		t.Fatal("empty proposal id accepted")
	}
	if err := m.RecordNativeInstallationReceipt("p1"); err != nil {
		t.Fatalf("valid receipt rejected: %v", err)
	}
	state := m.integratedStateLocked()
	if !state.authority.installationReceipts["p1"] {
		t.Fatal("receipt not recorded")
	}
}
