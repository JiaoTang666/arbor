package committee

import (
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestCumulativeLayoutPolicy(t *testing.T) {
	t.Setenv("SAA_NEWNEW_COMPENSATION_V1", "true")
	t.Setenv("SAA_NEWNEW_CUMULATIVE_LAYOUT_V1", "true")
	old := params.ShardNum
	params.ShardNum = 4
	defer func() { params.ShardNum = old }()
	addr := func(i int) string { return fmt.Sprintf("%040x", i) }
	m := &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: map[string]uint64{addr(1): 0, addr(2): 1}}, nativeRoutes: map[string]uint64{}, offeredLoadTPS: 2000}
	pairs := [][2]int{{3, 4}, {5, 2}, {4, 5}, {1, 2}, {6, 6}, {7, 3}}
	want := [][2]uint64{{2, 2}, {1, 1}, {2, 1}, {0, 1}, {3, 3}, {2, 2}}
	for i, p := range pairs {
		a, b := m.nativePlacement(addr(p[0]), addr(p[1]))
		if a != want[i][0] || b != want[i][1] {
			t.Fatalf("%d: %d,%d", i, a, b)
		}
	}
	m.newNewWork.advance(1e9)
	if m.newNewWork.work[2] != 3 || len(m.newNewWork.items) != 0 {
		t.Fatal("cumulative work expired or allocated rolling history")
	}
	if m.newNewWork.population[3] != 1 {
		t.Fatal("self address double counted")
	}
}

// Exact native-placement parity against the fixed offline 500k route artifact.
// This is not a consensus experiment and must not be reported as one.
func TestCumulativeLayoutFullPrefixParity(t *testing.T) {
	path := os.Getenv("LAYOUT_PARITY_FIXTURE")
	if path == "" {
		t.Skip("full data fixture is run separately in native preflight")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pos := 0
	read := func() uint32 {
		if pos+4 > len(raw) {
			t.Fatal("short fixture")
		}
		v := binary.LittleEndian.Uint32(raw[pos : pos+4])
		pos += 4
		return v
	}
	n, total, shards := int(read()), int(read()), uint64(read())
	old := params.ShardNum
	params.ShardNum = int(shards)
	defer func() { params.ShardNum = old }()
	t.Setenv("SAA_NEWNEW_COMPENSATION_V1", "true")
	t.Setenv("SAA_NEWNEW_CUMULATIVE_LAYOUT_V1", "true")
	addr := func(i uint32) string { return fmt.Sprintf("%040x", i) }
	routes := make(map[string]uint64, n)
	for i := 0; i < n; i++ {
		routes[addr(uint32(i))] = uint64(read())
	}
	m := &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: routes}, nativeRoutes: map[string]uint64{}, offeredLoadTPS: 2000}
	for i := 0; i < total; i++ {
		u, v, s, d := read(), read(), read(), read()
		a, b := m.nativePlacement(addr(u), addr(v))
		if a != uint64(s) || b != uint64(d) {
			t.Fatalf("route mismatch at %d: %d,%d != %d,%d", i, a, b, s, d)
		}
		if (i+1)%1000 == 0 {
			m.nativeCount += 1000
		}
	}
	if pos != len(raw) {
		t.Fatal("fixture trailing bytes")
	}
}
