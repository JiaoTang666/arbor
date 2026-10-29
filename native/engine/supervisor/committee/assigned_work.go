package committee

// Single input producer owns this ledger. It measures assigned execution work,
// not native queue occupancy, capacity or completion. No admission gating.
type newNewWorkItem struct {
	at             float64
	source, target uint64
}
type newNewWorkLedger struct {
	work       []int64
	items      []newNewWorkItem
	head       int
	cumulative bool
	population []int64
}

func (l *newNewWorkLedger) advance(now float64) {
	if l.cumulative {
		return
	}
	for l.head < len(l.items) && l.items[l.head].at <= now-60 {
		x := l.items[l.head]
		l.work[x.source]--
		if x.source != x.target {
			l.work[x.target]--
		}
		l.head++
	}
	if l.head > 65536 {
		copy(l.items, l.items[l.head:])
		l.items = l.items[:len(l.items)-l.head]
		l.head = 0
	}
}
func (l *newNewWorkLedger) choose(hash uint64) uint64 {
	if l.cumulative {
		best := 0
		for s := 1; s < len(l.work); s++ {
			if l.work[s] < l.work[best] || (l.work[s] == l.work[best] && l.population[s] < l.population[best]) {
				best = s
			}
		}
		return uint64(best)
	}
	best := hash
	for k := uint64(1); k < uint64(len(l.work)); k++ {
		s := (hash + k) % uint64(len(l.work))
		if l.work[s] < l.work[best] {
			best = s
		}
	}
	return best
}
func (l *newNewWorkLedger) add(now float64, s, t uint64) {
	l.work[s]++
	if s != t {
		l.work[t]++
	}
	if !l.cumulative {
		l.items = append(l.items, newNewWorkItem{now, s, t})
	}
}
