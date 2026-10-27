package initialledger

import (
	"strings"
	"testing"
)

func TestSealedRoutesPreserveLegacyAndRejectAliasing(t *testing.T) {
	account := strings.Repeat("0", 39) + "1"
	alias := map[string]uint64{account: 1}
	ledger := &Ledger{Manifest: Manifest{Shards: 2}, Routes: alias}
	if owner, ok := ledger.HistoricalOwner(account); !ok || owner != 1 || ledger.Routes == nil {
		t.Fatal("legacy map changed before sealing")
	}
	base, err := ledger.SealRoutes()
	if err != nil || ledger.Routes != nil {
		t.Fatalf("sealing failed: %v", err)
	}
	alias[account] = 0
	alias[strings.Repeat("f", 40)] = 0
	if owner, ok := ledger.HistoricalOwner(account); !ok || owner != 1 || base.Len() != 1 {
		t.Fatal("retained legacy alias mutated the sealed base")
	}
	if _, ok := ledger.HistoricalOwner(strings.Repeat("f", 40)); ok {
		t.Fatal("unverified account appeared in the sealed base")
	}
	if next, err := ledger.SealRoutes(); err != nil || next != base {
		t.Fatal("bootstrap rebuilt its immutable index")
	}
}

func TestSealedRoutesFailureLeavesInputUntouched(t *testing.T) {
	ledger := &Ledger{Manifest: Manifest{Shards: 2}, Routes: map[string]uint64{"bad": 1}}
	if base, err := ledger.SealRoutes(); err == nil || base != nil || ledger.Routes["bad"] != 1 || ledger.sealedRoutes != nil {
		t.Fatal("invalid base was adopted or discarded its input")
	}
}
