package committee

import (
	"blockEmulator/chain"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestTargetActivationPublishedOnlyAfterAppliedReceipt(t *testing.T) {
	m, _ := sourceResultFixture(t)
	s := m.integratedStateLocked()
	a := s.authority
	account := authorityAccount('a')
	receipt := prepareReceiptForTest(t, a, "p", account, 1)
	path := filepath.Join(t.TempDir(), "activate.json")
	t.Setenv("SAA_NATIVE_TARGET_ACTIVATION_PATH", path)
	if err := m.publishTargetActivationLocked("p", account, path); err == nil {
		t.Fatal("activation authorized before receipt")
	}
	a.recordInstallationReceiptMeta(receipt)
	if err := m.publishTargetActivationLocked("p", account, path); err == nil {
		t.Fatal("activation authorized before effective epoch")
	}
	for i := 0; i < 3; i++ {
		m.pollNativeControlAtObservedEpoch(1)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) || a.routeVersion != 0 {
		t.Fatal("repeated observed-epoch polling prematurely activated future route")
	}
	m.pollNativeControlAtObservedEpoch(2)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var c chain.TargetActivation
	if err = json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	if c.ProposalID != "p" || c.MappingVersion != 1 || c.InstallDigest != receipt.CommitDigest || c.StateHash != receipt.StateHash {
		t.Fatal("activation binding mismatch")
	}
	m.pollNativeControlAtObservedEpoch(2)
	if a.routeVersion != 1 {
		t.Fatal("repeated epoch advanced route version")
	}
	if err = m.publishDueTargetActivationLocked(); err != nil {
		t.Fatal("activation publication not idempotent")
	}
}
