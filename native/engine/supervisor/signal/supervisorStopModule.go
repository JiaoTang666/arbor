package signal

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// to judge when the listener to send the stop message to the leaders
type StopSignal struct {
	stoplock sync.Mutex // check the stopGap will not be modified by other processes

	stopGap       int // record how many empty txLists from leaders in a row
	stopThreshold int // the threshold

	requiredShards         int
	perShardEmptyThreshold int
	quietPeriod            time.Duration
	degradedQuietPeriod    time.Duration
	degradedIdleTarget     int
	terminalQuietPeriod    time.Duration
	terminalIdleTarget     int
	waitSecondLegDrain     bool
	pendingSecondLegTx     int
	lastMeaningfulActivity time.Time
	hasSeenNonEmptyBlock   bool
	shardEmptyStreak       map[uint64]int
	shardSeen              map[uint64]bool
	untouchedAfterDrain    map[uint64]bool // exclusive fenced input proof, NOT a block report
}

type StopSnapshot struct {
	GlobalStopGap          int
	GlobalStopThreshold    int
	RequiredShards         int
	PerShardEmptyThreshold int
	IdleShardCount         int
	PerShardReady          bool
	DegradedReady          bool
	DegradedIdleTarget     int
	TerminalReady          bool
	TerminalIdleTarget     int
	TerminalQuietPeriodMs  int64
	PendingSecondLegTx     int
	SecondLegDrainReady    bool
	QuietFor               time.Duration
	HasSeenNonEmptyBlock   bool
}

func NewStopSignal(stop_Threshold int, shardCount int) *StopSignal {
	perShardThreshold := 1
	if shardCount <= 1 {
		perShardThreshold = stop_Threshold
		if perShardThreshold <= 0 {
			perShardThreshold = 1
		}
	}
	quietPeriod := 3 * time.Second
	if shardCount > 8 {
		quietPeriod = 4 * time.Second
	}
	degradedQuietPeriod := 10 * quietPeriod
	if degradedQuietPeriod < 15*time.Second {
		degradedQuietPeriod = 15 * time.Second
	}
	terminalQuietPeriod := 4 * degradedQuietPeriod
	if terminalQuietPeriod < 120*time.Second {
		terminalQuietPeriod = 120 * time.Second
	}
	quietPeriod = durationFromEnvMs("BLOCK_EMULATOR_STOP_QUIET_MS", quietPeriod)
	degradedQuietPeriod = durationFromEnvMs("BLOCK_EMULATOR_STOP_DEGRADED_QUIET_MS", degradedQuietPeriod)
	terminalQuietPeriod = durationFromEnvMs("BLOCK_EMULATOR_STOP_TERMINAL_QUIET_MS", terminalQuietPeriod)
	allowance := shardCount / 4
	if allowance < 1 {
		allowance = 1
	}
	degradedIdleTarget := shardCount - allowance
	if degradedIdleTarget < 1 {
		degradedIdleTarget = 1
	}
	terminalIdleTarget := shardCount / 2
	if terminalIdleTarget < 1 {
		terminalIdleTarget = 1
	}
	return &StopSignal{
		stopGap:                0,
		stopThreshold:          stop_Threshold,
		requiredShards:         shardCount,
		perShardEmptyThreshold: perShardThreshold,
		quietPeriod:            quietPeriod,
		degradedQuietPeriod:    degradedQuietPeriod,
		degradedIdleTarget:     degradedIdleTarget,
		terminalQuietPeriod:    terminalQuietPeriod,
		terminalIdleTarget:     terminalIdleTarget,
		waitSecondLegDrain:     boolFromEnv("BLOCK_EMULATOR_STOP_WAIT_SECOND_LEG_DRAIN_ENABLED", true),
		pendingSecondLegTx:     0,
		lastMeaningfulActivity: time.Now(),
		hasSeenNonEmptyBlock:   false,
		shardEmptyStreak:       make(map[uint64]int),
		shardSeen:              make(map[uint64]bool),
	}
}

func boolFromEnv(name string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if raw == "" {
		return fallback
	}
	return raw == "1" || raw == "true" || raw == "yes" || raw == "y"
}

func durationFromEnvMs(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return time.Duration(value) * time.Millisecond
}

func (ss *StopSignal) ObserveSecondLegProgress(firstLegCount int, secondLegCount int) {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.pendingSecondLegTx += firstLegCount - secondLegCount
	if ss.pendingSecondLegTx < 0 {
		ss.pendingSecondLegTx = 0
	}
}

func (ss *StopSignal) secondLegDrainReadyLocked() bool {
	return !ss.waitSecondLegDrain || ss.pendingSecondLegTx <= 0
}

// when receiving a message with an empty txList, then call this function to increase stopGap
func (ss *StopSignal) StopGap_Inc() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.stopGap++
}

// when receiving a message with txs excuted, then call this function to reset stopGap
func (ss *StopSignal) StopGap_Reset() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.resetOnMeaningfulActivityLocked()
}

func (ss *StopSignal) resetOnMeaningfulActivityLocked() {
	ss.stopGap = 0
	ss.lastMeaningfulActivity = time.Now()
	for sid := range ss.shardEmptyStreak {
		ss.shardEmptyStreak[sid] = 0
	}
}

func (ss *StopSignal) ObserveBlockInfo(shardID uint64, blockBodyLength int) {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()

	ss.shardSeen[shardID] = true
	if ss.untouchedAfterDrain[shardID] {
		delete(ss.untouchedAfterDrain, shardID)
		ss.resetOnMeaningfulActivityLocked() // late traffic revokes proof and restarts quiet wait
	}
	if blockBodyLength == 0 {
		ss.stopGap++
		ss.shardEmptyStreak[shardID]++
		return
	}

	ss.stopGap = 0
	ss.shardEmptyStreak[shardID] = 0
	ss.hasSeenNonEmptyBlock = true
	ss.lastMeaningfulActivity = time.Now()
}

func (ss *StopSignal) idleShardCountLocked() int {
	if ss.requiredShards <= 0 {
		return 0
	}
	count := 0
	for sid := 0; sid < ss.requiredShards; sid++ {
		if ss.untouchedAfterDrain[uint64(sid)] || ss.shardEmptyStreak[uint64(sid)] >= ss.perShardEmptyThreshold {
			count++
		}
	}
	return count
}

func (ss *StopSignal) perShardReadyLocked() bool {
	if ss.requiredShards <= 0 {
		return false
	}
	if !ss.allShardsCoveredLocked() {
		return false
	}
	if !ss.hasSeenNonEmptyBlock {
		return false
	}
	for sid := 0; sid < ss.requiredShards; sid++ {
		shardID := uint64(sid)
		if ss.untouchedAfterDrain[shardID] {
			continue
		}
		if !ss.shardSeen[shardID] {
			return false
		}
		if ss.shardEmptyStreak[shardID] < ss.perShardEmptyThreshold {
			return false
		}
	}
	return time.Since(ss.lastMeaningfulActivity) >= ss.quietPeriod
}

func (ss *StopSignal) degradedReadyLocked() bool {
	if ss.requiredShards <= 0 {
		return false
	}
	if !ss.allShardsCoveredLocked() {
		return false
	}
	if !ss.hasSeenNonEmptyBlock {
		return false
	}
	if ss.idleShardCountLocked() < ss.degradedIdleTarget {
		return false
	}
	return time.Since(ss.lastMeaningfulActivity) >= ss.degradedQuietPeriod
}

func (ss *StopSignal) terminalReadyLocked() bool {
	if ss.requiredShards <= 0 {
		return false
	}
	if !ss.allShardsCoveredLocked() {
		return false
	}
	idleShardCount := ss.idleShardCountLocked()
	if !ss.hasSeenNonEmptyBlock {
		// In some strict runs the system can drain into all-empty block reports
		// without ever flipping hasSeenNonEmptyBlock, which previously caused
		// the supervisor to wait forever despite every shard being idle and the
		// global gap already carrying enough empty evidence. Keep this fallback
		// strict: require all shards to be idle, at least two empty rounds of
		// global evidence, and the full terminal quiet period before closeout.
		minEmptyEvidence := ss.stopThreshold
		if ss.requiredShards > 0 {
			twoEmptyRounds := ss.requiredShards * 2
			if minEmptyEvidence <= 0 || twoEmptyRounds < minEmptyEvidence {
				minEmptyEvidence = twoEmptyRounds
			}
		}
		if minEmptyEvidence < ss.requiredShards {
			minEmptyEvidence = ss.requiredShards
		}
		if ss.stopGap < minEmptyEvidence {
			return false
		}
		if idleShardCount < ss.requiredShards {
			return false
		}
		return time.Since(ss.lastMeaningfulActivity) >= ss.terminalQuietPeriod
	}
	if idleShardCount < ss.terminalIdleTarget {
		// Some native runs finish without every shard emitting explicit empty
		// block reports. Treat a full terminal quiet period after real block
		// activity as terminal evidence once every shard has appeared.
		return time.Since(ss.lastMeaningfulActivity) >= ss.terminalQuietPeriod
	}
	return time.Since(ss.lastMeaningfulActivity) >= ss.terminalQuietPeriod
}

func (ss *StopSignal) Snapshot() StopSnapshot {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	return StopSnapshot{
		GlobalStopGap:          ss.stopGap,
		GlobalStopThreshold:    ss.stopThreshold,
		RequiredShards:         ss.requiredShards,
		PerShardEmptyThreshold: ss.perShardEmptyThreshold,
		IdleShardCount:         ss.idleShardCountLocked(),
		PerShardReady:          ss.perShardReadyLocked(),
		DegradedReady:          ss.degradedReadyLocked(),
		DegradedIdleTarget:     ss.degradedIdleTarget,
		TerminalReady:          ss.terminalReadyLocked(),
		TerminalIdleTarget:     ss.terminalIdleTarget,
		TerminalQuietPeriodMs:  ss.terminalQuietPeriod.Milliseconds(),
		PendingSecondLegTx:     ss.pendingSecondLegTx,
		SecondLegDrainReady:    ss.secondLegDrainReadyLocked(),
		QuietFor:               time.Since(ss.lastMeaningfulActivity),
		HasSeenNonEmptyBlock:   ss.hasSeenNonEmptyBlock,
	}
}

// Check the stopGap is enough or not
// if StopGap is not less than stopThreshold, then the stop message should be sent to leaders.
func (ss *StopSignal) GapEnough() bool {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	if !ss.secondLegDrainReadyLocked() {
		return false
	}
	if !ss.hasSeenNonEmptyBlock {
		return ss.terminalReadyLocked()
	}
	if ss.perShardReadyLocked() {
		return true
	}
	if ss.degradedReadyLocked() {
		return true
	}
	return ss.terminalReadyLocked()
}
