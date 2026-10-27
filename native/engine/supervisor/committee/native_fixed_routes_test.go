package committee

import (
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"fmt"
	"testing"
)

func TestCompletionFixedInputRoutes(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 16
	t.Cleanup(func() { params.ShardNum = old })
	known := fmt.Sprintf("%040x", 1)
	unknown := fmt.Sprintf("%040x", 2)
	m := &RelayCommitteeModule{nativeFixedRoutes: true, nativeBatch: true,
		initialLedger: &initialledger.Ledger{Routes: map[string]uint64{known: 7}}, nativeRoutes: map[string]uint64{}}
	for _, pair := range [][2]string{{known, unknown}, {unknown, known}, {unknown, fmt.Sprintf("%040x", 3)}} {
		a, b := m.nativePlacement(pair[0], pair[1])
		want := func(account string) uint64 {
			if account == known {
				return 7
			}
			var v uint64
			fmt.Sscanf(account[32:], "%x", &v)
			return v % 16
		}
		if a != want(pair[0]) || b != want(pair[1]) || m.nativePlacementVersion != 0 || len(m.nativeRoutes) != 0 || m.newNewWork != nil {
			t.Fatalf("placement or mutable policy leaked into fixed route input: %d %d", a, b)
		}
	}
}

func TestCompletionFixedInputEnvironment(t *testing.T) {
	for _, key := range []string{"SAA_NEWNEW_COMPENSATION_V1", "SAA_NEWNEW_CUMULATIVE_LAYOUT_V1", "SAA_ATTRIBUTION_INDEPENDENT_NEW_V1"} {
		t.Setenv(key, "")
	}
	for _, value := range []string{"", "false", "true", "typo"} {
		t.Setenv("BLOCK_EMULATOR_NATIVE_FIXED_ROUTES_V1", value)
		enabled, err := fixedNativeRoutesFromEnv()
		if (err != nil) != (value == "typo") || enabled != (value == "true") {
			t.Fatalf("incorrect config result for %q", value)
		}
	}
	t.Setenv("BLOCK_EMULATOR_NATIVE_FIXED_ROUTES_V1", "true")
	t.Setenv("SAA_NEWNEW_COMPENSATION_V1", "true")
	if _, err := fixedNativeRoutesFromEnv(); err == nil {
		t.Fatal("mixed policy accepted")
	}
}
