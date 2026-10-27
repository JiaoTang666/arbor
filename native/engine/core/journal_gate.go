package core

import "sync"

// journalGate preserves one durable writer, but does not put completion behind
// the entire network-admission backlog. FIFO within each class; alternate the
// classes when both wait. A control caller waits for at most one admission
// after the active holder, plus earlier control callers. No I/O is preempted.
// The zero value is ready to use; never copy after first use.
type journalGate struct {
	mu                                    sync.Mutex
	cond                                  *sync.Cond
	busy, ownerAdmission, preferAdmission bool
	next, head                            [2]uint64 // 0: control/confirmation; 1: durable admission
}

func (g *journalGate) Lock()          { g.acquire(0) }
func (g *journalGate) LockAdmission() { g.acquire(1) }

func (g *journalGate) acquire(class int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cond == nil {
		g.cond = sync.NewCond(&g.mu)
	}
	ticket := g.next[class]
	g.next[class]++
	for g.busy || ticket != g.head[class] || g.otherTurn(class) {
		g.cond.Wait()
	}
	g.head[class]++
	g.busy, g.ownerAdmission = true, class == 1
}

func (g *journalGate) otherTurn(class int) bool {
	both := g.next[0] != g.head[0] && g.next[1] != g.head[1]
	return both && (class == 1) != g.preferAdmission
}

func (g *journalGate) TryLock() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.busy || g.next != g.head {
		return false
	}
	g.busy, g.ownerAdmission = true, false
	return true
}

func (g *journalGate) Unlock() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.busy {
		panic("unlock of unlocked journal gate")
	}
	g.preferAdmission = !g.ownerAdmission
	g.busy = false
	if g.cond != nil {
		g.cond.Broadcast()
	}
}
