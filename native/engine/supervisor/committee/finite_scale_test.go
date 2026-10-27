package committee

import (
	"testing"
	"time"
)

func TestFiniteMillionCountAndDeadline(t *testing.T) {
	origin := time.Now()
	p := &finitePlan{count: 1000000, afterLast: 48 * time.Hour, now: func() time.Time { return origin }}
	p.start(origin)
	if p.totalCount() != 1000000 || p.cutoff.Sub(origin) != 48*time.Hour+999999*time.Millisecond {
		t.Fatal(p.totalCount(), p.cutoff.Sub(origin))
	}
	if p.closed() {
		t.Fatal("premature cutoff")
	}
	p.now = func() time.Time { return p.cutoff }
	if !p.closed() || p.begin("admit", 999999) {
		t.Fatal("cutoff bypass")
	}
}
func TestFiniteLegacyDefaultCount(t *testing.T) {
	if (&finitePlan{}).totalCount() != 12000 {
		t.Fatal("legacy test default changed")
	}
}
