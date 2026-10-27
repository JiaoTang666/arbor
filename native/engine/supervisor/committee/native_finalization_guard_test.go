package committee

import "testing"

func TestCanFinalizeMigrationRequiresExactCommittedReceipt(t *testing.T) {
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, stableShardByAccount: map[string]uint64{}}
	if err := m.CanFinalizeMigration("p", "a", "h"); err == nil {
		t.Fatal("missing receipt accepted")
	}
	s := m.integratedStateLocked()
	s.authority.recordInstallationReceiptMeta(nativeInstallationReceiptMeta{ProposalID: "p", Account: "a", StateHash: "h", Committed: true})
	if err := m.CanFinalizeMigration("p", "a", "wrong"); err == nil {
		t.Fatal("wrong hash accepted")
	}
	if err := m.CanFinalizeMigration("p", "a", "h"); err != nil {
		t.Fatalf("matching receipt rejected: %v", err)
	}
}
