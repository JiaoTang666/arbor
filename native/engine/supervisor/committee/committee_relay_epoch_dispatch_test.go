package committee

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEpochAwareDispatchWaitsForCompleteWatermark(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "epoch_aware_dispatch_audit.csv")
	module := &RelayCommitteeModule{
		epochAwareDispatchEnabled:   true,
		epochAwareDispatchTimeout:   250 * time.Millisecond,
		epochAwareDispatchAuditPath: auditPath,
	}
	go func() {
		time.Sleep(15 * time.Millisecond)
		atomic.StoreInt64(&module.observedEpoch, 4)
	}()

	if !module.waitForObservedEpochAtLeast(2, 1000, 3) {
		t.Fatal("expected complete watermark wait to succeed")
	}
	audit, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(audit), "COMPLETE_WATERMARK_REACHED") ||
		!strings.Contains(string(audit), "2,1000,3,4,4") {
		t.Fatalf("unexpected dispatch audit: %s", audit)
	}
}

func TestEpochAwareDispatchTimeoutIsAudited(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "epoch_aware_dispatch_audit.csv")
	module := &RelayCommitteeModule{
		epochAwareDispatchEnabled:   true,
		epochAwareDispatchTimeout:   20 * time.Millisecond,
		epochAwareDispatchAuditPath: auditPath,
	}

	if module.waitForObservedEpochAtLeast(3, 8000, 1) {
		t.Fatal("expected complete watermark wait to time out")
	}
	audit, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(audit), "TIMEOUT") ||
		!strings.Contains(string(audit), "3,8000,1,2,0") {
		t.Fatalf("unexpected dispatch timeout audit: %s", audit)
	}
}

func TestParseEpochAwareDispatchConfiguration(t *testing.T) {
	if !parseEpochAwareDispatchEnabled("1") {
		t.Fatal("expected 1 to enable epoch-aware dispatch")
	}
	if parseEpochAwareDispatchEnabled("invalid") {
		t.Fatal("invalid boolean must disable epoch-aware dispatch")
	}
	if got := parseEpochAwareDispatchTimeout("750"); got != 60*time.Second {
		t.Fatalf("short timeout=%s, want default 60s", got)
	}
	if got := parseEpochAwareDispatchTimeout("120000"); got != 120*time.Second {
		t.Fatalf("parsed timeout=%s, want 120s", got)
	}
}
