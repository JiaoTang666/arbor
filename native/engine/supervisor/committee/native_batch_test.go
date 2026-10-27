package committee

import (
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"strings"
	"testing"
)

func TestNativeBatchStableFirstPlacement(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 16
	defer func() { params.ShardNum = old }()
	a := strings.Repeat("0", 39) + "1"
	b := strings.Repeat("0", 39) + "2"
	c := strings.Repeat("0", 39) + "3"
	m := &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: map[string]uint64{a: 7}}, nativeRoutes: map[string]uint64{}}
	s, d := m.nativePlacement(a, b)
	if s != 7 || d != 7 {
		t.Fatal(s, d)
	}
	s, d = m.nativePlacement(b, c)
	if s != 7 || d != 7 {
		t.Fatal(s, d)
	}
	m.initialLedger.Routes[a] = 9
	s, d = m.nativePlacement(b, a)
	if s != 7 || d != 9 {
		t.Fatal("assignment changed", s, d)
	}
}
func TestNativeBatchBothUnknown(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 16
	defer func() { params.ShardNum = old }()
	a := strings.Repeat("0", 39) + "3"
	b := strings.Repeat("0", 39) + "4"
	m := &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: map[string]uint64{}}, nativeRoutes: map[string]uint64{}}
	s, d := m.nativePlacement(a, b)
	if s != 3 || d != 3 {
		t.Fatal(s, d)
	}
	s, d = m.nativePlacement(b, a)
	if s != 3 || d != 3 {
		t.Fatal(s, d)
	}
}
