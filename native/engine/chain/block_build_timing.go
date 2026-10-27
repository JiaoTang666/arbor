package chain

import (
	"fmt"
	"time"
)

const (
	buildPack = iota
	buildPrecheck
	buildOpenTrie
	buildExecute
	buildTrieCommit
	buildTrieUpdate
	buildTriePersist
	buildFinalize
	buildPhaseCount
)

// Timings are local diagnostics only. No clocks or counters enter a block,
// transaction or state root. Indexed execution adds diagnostic subtotals only.
type blockBuildTiming struct {
	started, last time.Time
	phases        [buildPhaseCount]time.Duration
	transactions  int
	complete      bool
	execution     indexedExecutionTiming
}

func (t *blockBuildTiming) executionTiming() *indexedExecutionTiming {
	if t == nil {
		return nil
	}
	return &t.execution
}

func newBlockBuildTiming() *blockBuildTiming {
	now := time.Now()
	return &blockBuildTiming{started: now, last: now}
}

func (t *blockBuildTiming) mark(phase int) {
	if t == nil {
		return
	}
	now := time.Now()
	t.phases[phase] += now.Sub(t.last)
	t.last = now
}

func (t *blockBuildTiming) report(shard, height uint64) {
	p := t.phases
	fmt.Printf("block_build_detail shard=%d height=%d txs=%d packUs=%d precheckUs=%d openTrieUs=%d executeUs=%d trieCommitUs=%d trieUpdateUs=%d triePersistUs=%d finalizeUs=%d totalUs=%d complete=%t\n",
		shard, height, t.transactions, p[buildPack].Microseconds(), p[buildPrecheck].Microseconds(),
		p[buildOpenTrie].Microseconds(), p[buildExecute].Microseconds(), p[buildTrieCommit].Microseconds(),
		p[buildTrieUpdate].Microseconds(), p[buildTriePersist].Microseconds(), p[buildFinalize].Microseconds(),
		time.Since(t.started).Microseconds(), t.complete)
	t.execution.report(shard, height, p[buildExecute])
}
