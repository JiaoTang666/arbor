package core

import (
	"container/heap"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// This selector changes only the order of a bounded legacy-queue prefix.
// Completion is a stage property, NOT a successful execution prediction.
// No clock, score, or diagnostic changes a transaction or its state effects.
type CompletionScheduleConfig struct {
	Mode            string
	LookaheadBlocks int
	FairEvery       uint64
}

func CompletionSchedulingFromEnv() (*CompletionScheduleConfig, error) {
	mode := strings.TrimSpace(os.Getenv("BLOCK_EMULATOR_COMPLETION_SCHEDULER"))
	if mode == "" || mode == "off" {
		return nil, nil
	}
	c := &CompletionScheduleConfig{Mode: mode, LookaheadBlocks: 4, FairEvery: 4}
	if raw := os.Getenv("BLOCK_EMULATOR_COMPLETION_LOOKAHEAD_BLOCKS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid completion lookahead: %w", err)
		}
		c.LookaheadBlocks = value
	}
	if raw := os.Getenv("BLOCK_EMULATOR_COMPLETION_FAIR_EVERY"); raw != "" {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid completion fairness period: %w", err)
		}
		c.FairEvery = value
	}
	return c, c.validate()
}

func (c CompletionScheduleConfig) validate() error {
	if c.Mode != "fifo" && c.Mode != "phase_priority" && c.Mode != "completion_frontier" {
		return fmt.Errorf("unknown completion scheduler mode %q", c.Mode)
	}
	if c.LookaheadBlocks < 1 || c.LookaheadBlocks > 64 || c.FairEvery == 0 {
		return fmt.Errorf("completion lookahead must be 1..64 and fairness period must be positive")
	}
	return nil
}

type CompletionStageCounts struct {
	Inner       int `json:"inner"`
	Relay1      int `json:"relay1"`
	Relay2      int `json:"relay2"`
	Unsupported int `json:"unsupported"`
}

func (c *CompletionStageCounts) add(stage int) {
	switch stage {
	case 0:
		c.Inner++
	case 1:
		c.Relay1++
	case 2:
		c.Relay2++
	default:
		c.Unsupported++
	}
}

type CompletionScheduleAudit struct {
	Schema                   string                `json:"schema"`
	Mode                     string                `json:"mode"`
	EffectiveMode            string                `json:"effective_mode"`
	PackCall                 uint64                `json:"pack_call"`
	QueueBefore              int                   `json:"queue_before"`
	Examined                 int                   `json:"examined"`
	Prefix                   CompletionStageCounts `json:"prefix"`
	InitiallyReady           CompletionStageCounts `json:"initially_ready"`
	InitiallyBlocked         int                   `json:"initially_blocked"`
	Selected                 CompletionStageCounts `json:"selected"`
	SelectedCount            int                   `json:"selected_count"`
	RemainingReadyCompletion int                   `json:"remaining_ready_completion"`
	FairFIFO                 bool                  `json:"fair_fifo"`
	FallbackReason           string                `json:"fallback_reason,omitempty"`
	SelectionMicroseconds    int64                 `json:"selection_us"`
}

type completionScheduler struct {
	config      CompletionScheduleConfig
	shard, size uint64
	calls       uint64
	last        CompletionScheduleAudit
}

func (p *TxPool) EnableCompletionScheduling(c CompletionScheduleConfig, shard, shards uint64) error {
	if err := c.validate(); err != nil {
		return err
	}
	if shards == 0 || shard >= shards {
		return fmt.Errorf("invalid completion scheduler shard topology")
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.completionScheduling != nil {
		return fmt.Errorf("completion scheduling is startup-only and already configured")
	}
	p.completionScheduling = &completionScheduler{config: c, shard: shard, size: shards}
	return nil
}

func (p *TxPool) CompletionSchedulingAudit() CompletionScheduleAudit {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.completionScheduling == nil {
		return CompletionScheduleAudit{Schema: "completion_schedule_v1", Mode: "off", EffectiveMode: "fifo"}
	}
	return p.completionScheduling.last
}

// Only frozen RawRelay stages have an exact, immutable local footprint here.
// Unsupported input falls back as a whole prefix; it is never silently skipped.
func completionFootprint(tx *Transaction, shard, shards uint64) (int, []string, string) {
	if tx == nil {
		return -1, nil, "nil_transaction"
	}
	if tx.HasBroker || tx.SenderIsBroker || tx.OriginalSender != "" || tx.FinalRecipient != "" {
		return -1, nil, "broker_or_non_raw_relay"
	}
	// First creation has a separate binding/lease contract. Do not reorder its
	// transport in this first version; falling back does not introduce a HOLD.
	if tx.FirstPlacement != nil {
		return -1, nil, "first_placement_binding"
	}
	if !tx.RoutingFrozen {
		return -1, nil, "unfrozen_route"
	}
	if tx.RoutedSenderShard >= shards || tx.RoutedRecipientShard >= shards {
		return -1, nil, "invalid_route"
	}
	sender, recipient := canonicalPrepareAccount(tx.Sender), canonicalPrepareAccount(tx.Recipient)
	if sender == "" || recipient == "" {
		return -1, nil, "missing_account"
	}
	if tx.Relayed {
		if tx.RoutedRecipientShard != shard || tx.RoutedSenderShard == shard {
			return -1, nil, "invalid_relay_stage"
		}
		return 2, []string{recipient}, ""
	}
	if tx.RoutedSenderShard != shard {
		return -1, nil, "stage_not_local"
	}
	if tx.RoutedRecipientShard != shard {
		return 1, []string{sender}, ""
	}
	if sender == recipient {
		return 0, []string{sender}, ""
	}
	return 0, []string{sender, recipient}, ""
}

type completionNode struct {
	stage, degree, position int
	preds, next             []int
	score                   int // twice the score: indegree is at most two.
	chosen                  bool
}

type completionReadyHeap struct {
	nodes []completionNode
	items []int
}

func (h completionReadyHeap) Len() int { return len(h.items) }
func (h completionReadyHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if h.nodes[a].score != h.nodes[b].score {
		return h.nodes[a].score > h.nodes[b].score
	}
	return a < b // local queue order, never global ingress or transaction nonce.
}
func (h completionReadyHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.nodes[h.items[i]].position, h.nodes[h.items[j]].position = i, j
}
func (h *completionReadyHeap) Push(x interface{}) {
	i := x.(int)
	h.nodes[i].position = len(h.items)
	h.items = append(h.items, i)
}
func (h *completionReadyHeap) Pop() interface{} {
	i := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	h.nodes[i].position = -1
	return i
}

func completionScore(nodes []completionNode, i int, mode string) int {
	if mode == "fifo" {
		return 0
	}
	score := 0
	if nodes[i].stage != 1 {
		score = 2
	}
	if mode == "completion_frontier" {
		for _, j := range nodes[i].next {
			if !nodes[j].chosen && nodes[j].stage != 1 && nodes[j].degree > 0 {
				score += 2 / nodes[j].degree
			}
		}
	}
	return score
}

// Caller holds p.lock. At most LookaheadBlocks*blockCapacity entries are
// inspected. Residual prefix is written behind the selected slice boundary;
// the potentially huge suffix is neither traversed nor copied.
func (p *TxPool) packCompletionLocked(limit uint64) []*Transaction {
	s := p.completionScheduling
	if limit == 0 || len(p.TxQueue) == 0 {
		s.last = CompletionScheduleAudit{Schema: "completion_schedule_v1", Mode: s.config.Mode, EffectiveMode: s.config.Mode, QueueBefore: len(p.TxQueue)}
		return nil
	}
	started := time.Now()
	s.calls++
	a := CompletionScheduleAudit{Schema: "completion_schedule_v1", Mode: s.config.Mode,
		EffectiveMode: s.config.Mode, PackCall: s.calls, QueueBefore: len(p.TxQueue)}
	defer func() { a.SelectionMicroseconds = time.Since(started).Microseconds(); s.last = a }()
	count := len(p.TxQueue)
	if limit < uint64(count) {
		count = int(limit)
	}
	n := len(p.TxQueue)
	if count <= n/s.config.LookaheadBlocks {
		n = count * s.config.LookaheadBlocks
	}
	if s.config.Mode == "fifo" || s.calls%s.config.FairEvery == 0 {
		a.FairFIFO = s.config.Mode != "fifo"
		a.EffectiveMode = "fifo"
	}
	if a.EffectiveMode == "fifo" {
		n = count
	}
	a.Examined = n
	nodes := make([]completionNode, n)
	last := make(map[string]int, 2*n)
	for i, tx := range p.TxQueue[:n] {
		stage, accounts, reason := completionFootprint(tx, s.shard, s.size)
		nodes[i] = completionNode{stage: stage, position: -1}
		a.Prefix.add(stage)
		if reason != "" && a.FallbackReason == "" {
			a.FallbackReason = reason
		}
		for _, account := range accounts {
			if prev, ok := last[account]; ok {
				if len(nodes[i].preds) == 0 || nodes[i].preds[0] != prev {
					nodes[i].preds = append(nodes[i].preds, prev)
					nodes[prev].next = append(nodes[prev].next, i)
				}
			}
			last[account] = i
		}
		nodes[i].degree = len(nodes[i].preds)
	}
	if a.FallbackReason != "" {
		a.EffectiveMode = "fifo"
		out := p.TxQueue[:count]
		for _, tx := range out {
			stage, _, _ := completionFootprint(tx, s.shard, s.size)
			a.Selected.add(stage)
		}
		a.SelectedCount = count
		p.TxQueue = p.TxQueue[count:]
		return out
	}
	h := &completionReadyHeap{nodes: nodes}
	for i := range nodes {
		if nodes[i].degree == 0 {
			nodes[i].score = completionScore(nodes, i, a.EffectiveMode)
			h.items = append(h.items, i)
			nodes[i].position = len(h.items) - 1
			a.InitiallyReady.add(nodes[i].stage)
		} else {
			a.InitiallyBlocked++
		}
	}
	heap.Init(h)
	out := make([]*Transaction, 0, count)
	for len(out) < count {
		i := heap.Pop(h).(int) // forward-only dependency edges guarantee a ready node.
		nodes[i].chosen = true
		out = append(out, p.TxQueue[i])
		a.Selected.add(nodes[i].stage)
		for _, j := range nodes[i].next {
			nodes[j].degree--
			if nodes[j].degree == 0 {
				nodes[j].score = completionScore(nodes, j, a.EffectiveMode)
				heap.Push(h, j)
			}
			// Removing one prerequisite increases the unlock share of the
			// other still-ready prerequisites. Each node has at most two.
			for _, pred := range nodes[j].preds {
				if nodes[pred].position >= 0 && !nodes[pred].chosen {
					nodes[pred].score = completionScore(nodes, pred, a.EffectiveMode)
					heap.Fix(h, nodes[pred].position)
				}
			}
		}
	}
	retained := make([]*Transaction, 0, n-count)
	for i := range nodes {
		if !nodes[i].chosen {
			retained = append(retained, p.TxQueue[i])
			if nodes[i].degree == 0 && nodes[i].stage != 1 {
				a.RemainingReadyCompletion++
			}
		}
	}
	copy(p.TxQueue[count:n], retained)
	for i := 0; i < count; i++ {
		p.TxQueue[i] = nil
	}
	p.TxQueue = p.TxQueue[count:]
	a.SelectedCount = len(out)
	return out
}

func (p *TxPool) completionBypassLocked(reason string) {
	if s := p.completionScheduling; s != nil {
		s.last = CompletionScheduleAudit{Schema: "completion_schedule_v1", Mode: s.config.Mode,
			EffectiveMode: "legacy_migration", QueueBefore: p.PendingCountLocked(), FallbackReason: reason}
	}
}
