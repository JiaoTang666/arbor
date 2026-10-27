package committee

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTargetCommandRequiresAcceptedPreparedBinding(t *testing.T) {
	m, p := sourceResultFixture(t)
	path := filepath.Join(t.TempDir(), "target.json")
	if m.PublishNativeTargetInstallation(p, path) == nil {
		t.Fatal("unprepared proposal published")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("rejected publication created command")
	}
	m.lock.Lock()
	err := m.consumeNativeSourcePreparedLocked(writeSourceResult(t, p))
	m.lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = m.PublishNativeTargetInstallation(p, path); err != nil {
		t.Fatal(err)
	}
	if err = m.PublishNativeTargetInstallation(p, path); err != nil {
		t.Fatal("idempotent publication failed")
	}
	p.Command.AnchorVersion++
	if m.PublishNativeTargetInstallation(p, filepath.Join(t.TempDir(), "bad.json")) == nil {
		t.Fatal("mismatched anchor published")
	}
}
