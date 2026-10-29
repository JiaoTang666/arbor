package committee

import (
	"os"
	"testing"
)

func TestNewNewWorkBoundary(t *testing.T) {
	l := &newNewWorkLedger{work: make([]int64, 16)}
	if l.choose(5) != 5 {
		t.Fatal("empty tie")
	}
	l.add(0, 5, 5)
	if l.choose(5) != 6 {
		t.Fatal("cyclic tie")
	}
	l.add(0, 1, 2)
	if l.work[1] != 1 || l.work[2] != 1 || l.work[5] != 1 {
		t.Fatal("stage accounting")
	}
	l.advance(59.5)
	if l.work[5] != 1 {
		t.Fatal("early expiry")
	}
	l.advance(60)
	for _, v := range l.work {
		if v != 0 {
			t.Fatal("expiry")
		}
	}
}
func TestNewNewWorkMillionParity(t *testing.T) {
	path := os.Getenv("SAA_NEWNEW_PARITY_FIXTURE")
	if path == "" {
		t.Skip("explicit offline parity fixture required")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if len(raw) != 4000000 {
		t.Fatal("fixture length")
	}
	l := &newNewWorkLedger{work: make([]int64, 16)}
	decisions := 0
	for i := 0; i < len(raw)/4; i++ {
		p := raw[i*4 : i*4+4]
		now := float64(i/1000) * .5
		l.advance(now)
		if p[2] == 2 {
			decisions++
			if l.choose(uint64(p[3])) != uint64(p[0]) {
				t.Fatalf("decision mismatch %d", i)
			}
		}
		l.add(now, uint64(p[0]), uint64(p[1]))
	}
	if decisions != 49456 {
		t.Fatal("decision count", decisions)
	}
}
