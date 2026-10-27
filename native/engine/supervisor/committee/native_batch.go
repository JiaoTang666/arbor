package committee

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func fixedNativeRoutesFromEnv() (bool, error) {
	var enabled bool
	switch os.Getenv("BLOCK_EMULATOR_NATIVE_FIXED_ROUTES_V1") {
	case "", "false":
		return false, nil
	case "true":
		enabled = true
	default:
		return false, fmt.Errorf("invalid fixed native route mode")
	}
	for _, key := range []string{"SAA_NEWNEW_COMPENSATION_V1", "SAA_NEWNEW_CUMULATIVE_LAYOUT_V1", "SAA_ATTRIBUTION_INDEPENDENT_NEW_V1"} {
		if os.Getenv(key) == "true" {
			return false, fmt.Errorf("fixed native route mode conflicts with %s", key)
		}
	}
	return enabled, nil
}

// Initialize history-only counts before releasing paced input. Keep the lazy
// call for small fixtures which construct the committee without its constructor.
func (m *RelayCommitteeModule) initializeNewNewWork() {
	if m.newNewWork != nil || os.Getenv("SAA_NEWNEW_COMPENSATION_V1") != "true" {
		return
	}
	if m.offeredLoadTPS <= 0 {
		panic("newnew accounting requires offered clock")
	}
	m.newNewWork = &newNewWorkLedger{work: make([]int64, params.ShardNum)}
	if os.Getenv("SAA_NEWNEW_CUMULATIVE_LAYOUT_V1") != "true" {
		return
	}
	m.newNewWork.cumulative = true
	m.newNewWork.population = make([]int64, params.ShardNum)
	for _, shard := range m.initialLedger.Routes {
		m.newNewWork.population[shard]++
	}
	for account, shard := range m.nativeRoutes {
		if _, exists := m.initialLedger.Routes[account]; !exists {
			m.newNewWork.population[shard]++
		}
	}
}

// Only the source producer mutates these routes. Assignment is stable and
// independent of delivery/completion order. Native execution creates state.
func (m *RelayCommitteeModule) nativePlacement(sender, recipient string) (uint64, uint64) {
	if m.livePlan != nil {
		return m.liveNativePlacement(sender, recipient)
	}
	if m.nativeFixedRoutes {
		m.nativePlacementVersion = 0
		route := func(account string) uint64 {
			account = nativeAdmissionAccount(account)
			if shard, ok := m.initialLedger.Routes[account]; ok {
				return shard
			}
			return uint64(utils.Addr2Shard(account))
		}
		sa, sb := route(sender), route(recipient)
		m.checkAuthorizedPlacement(sender, recipient, sa, sb)
		return sa, sb
	}
	if m.groupControl != nil {
		m.groupControl.mu.Lock()
		defer m.groupControl.mu.Unlock()
	}
	m.nativePlacementVersion = 0
	if m.groupControl != nil && m.groupControl.serial != nil {
		m.nativePlacementVersion = m.groupControl.serial.version
	} else if m.groupControl != nil && m.groupControl.active {
		m.nativePlacementVersion = m.groupControl.authority.routeVersion
	}
	a, b := nativeAdmissionAccount(sender), nativeAdmissionAccount(recipient)
	lookup := func(a string) (uint64, bool) {
		if m.groupControl != nil && m.groupControl.serial != nil {
			if s, ok := m.groupControl.serial.routes[a]; ok {
				return s, true
			}
		} else if m.groupControl != nil && m.groupControl.active {
			if s, ok := m.groupControl.authority.resolve("0x" + a); ok {
				return s, true
			}
		}
		if s, ok := m.initialLedger.Routes[a]; ok {
			return s, true
		}
		s, ok := m.nativeRoutes[a]
		return s, ok
	}
	sa, ka := lookup(a)
	sb, kb := lookup(b)
	m.initializeNewNewWork()
	now := float64(m.nativeCount) / m.offeredLoadTPS
	if m.groupControl != nil && m.groupControl.serial != nil && m.newNewWork != nil && m.newNewWork.cumulative {
		s := m.groupControl.serial
		if s.populationApplied == nil {
			s.populationApplied = map[string]bool{}
		}
		for account, target := range s.routes {
			if !s.populationApplied[account] {
				source, exists := s.originalRoute(account)
				if !exists {
					panic("activated migration has no original native placement")
				}
				m.newNewWork.population[source]--
				m.newNewWork.population[target]++
				s.populationApplied[account] = true
			}
		}
	}
	if m.newNewWork != nil {
		m.newNewWork.advance(now)
	}
	target := uint64(utils.Addr2Shard(a))
	if ka {
		target = sa
	} else if kb {
		target = sb
	} else if m.newNewWork != nil && (m.newNewPolicy == nil || m.newNewPolicy.mode != "association_hash") {
		target = m.newNewWork.choose(target)
	}
	if (!ka || !kb) && m.newNewPolicy != nil && m.newNewPolicy.share == nil {
		c := entryContext{KnownSender: ka, KnownRecipient: kb, Self: a == b, PartnerShard: -1}
		if !ka {
			c.Novel++
		}
		if !kb && a != b {
			c.Novel++
		}
		if ka {
			c.PartnerShard = int(sa)
			c.Partner = a
		} else if kb {
			c.PartnerShard = int(sb)
			c.Partner = b
		}
		target = m.chooseNewNewPolicy(target, c)
	}
	var shareAt placementClockReading
	if m.newNewPolicy != nil && m.newNewPolicy.share != nil {
		if !ka && !kb && !lightModeV2(m.newNewPolicy.mode) {
			m.queryForestNativeV2()
		}
		shareAt = m.placementSignals.clock.now()
		if m.newNewPolicy.share.originNS < 0 {
			m.newNewPolicy.share.originNS = shareAt.MonoNS
		}
		if !ka && !kb {
			if m.newNewPolicy.mode == "association_hash" {
				// Association-only control: real sender hash, no pressure or model query.
				// Binding, family evidence and execution accounting remain shared.
				m.nativeAudit.write(map[string]interface{}{"event": "association_placement", "sequence": m.placementSequence, "sender": a, "action": target, "binding_mono_ns": shareAt.MonoNS, "clock_id": shareAt.ClockID})
			} else {
				target, shareAt = m.chooseSharePolicy()
			}
		}
	}
	// The same placement policy proposes the target. Public authorization is
	// checked BEFORE either new binding or work/population increment is stored.
	proposedSender, proposedRecipient := sa, sb
	if !ka {
		proposedSender = target
	}
	if !kb {
		proposedRecipient = target
	}
	m.checkAuthorizedPlacement(a, b, proposedSender, proposedRecipient)
	if !ka {
		sa = target
		m.nativeRoutes[a] = sa
		if m.newNewPolicy != nil {
			m.newNewPolicy.bindingSequence[a] = m.newNewPolicy.base + int64(m.placementSequence)
		}
		if m.newNewWork != nil && m.newNewWork.cumulative {
			m.newNewWork.population[sa]++
		}
	}
	if !kb {
		sb = target
		m.nativeRoutes[b] = sb
		if m.newNewPolicy != nil {
			m.newNewPolicy.bindingSequence[b] = m.newNewPolicy.base + int64(m.placementSequence)
		}
		if m.newNewWork != nil && m.newNewWork.cumulative && (ka || a != b) {
			m.newNewWork.population[sb]++
		}
	}
	if m.newNewWork != nil {
		m.newNewWork.add(now, sa, sb)
	}
	if m.newNewPolicy != nil && m.newNewPolicy.share != nil {
		m.recordSharePlacement(a, b, ka, kb, sa, sb, shareAt)
	}
	return sa, sb
}

func (m *RelayCommitteeModule) sendNativeBatch(txs []*core.Transaction) {
	if m.nativeClock.IsZero() {
		m.nativeClock = time.Now()
	}
	// Original batch offered-load model: 1000 transactions each second, not
	// 1000 sequential RPCs each second. The cumulative schedule makes any
	// delivery lateness visible rather than resetting the plan clock.
	deadline := m.nativeClock.Add(time.Duration(float64(m.nativeCount) / m.offeredLoadTPS * float64(time.Second)))
	if delay := time.Until(deadline); delay > 0 {
		time.Sleep(delay)
	}
	at := time.Now()
	var before runtime.MemStats
	if m.placementSignals != nil {
		runtime.ReadMemStats(&before)
	}
	for i, tx := range txs {
		if tx.IngressSequence != uint64(m.nativeCount+i) {
			panic("native batch source identity gap")
		}
	}
	if err := m.bindNativeEffectAdmissions(txs); err != nil {
		panic(err)
	}
	boundAt := time.Now()
	if err := m.prepareLivePlacement(txs); err != nil {
		panic(err)
	}
	planAt := time.Now()
	m.nativeAudit.write(map[string]interface{}{"event": "batch_input", "at": at.UTC(), "start": m.nativeCount, "count": len(txs), "planned_at": deadline.UTC(), "target_tps": m.offeredLoadTPS,
		"planned_elapsed_ns": deadline.Sub(m.nativeClock).Nanoseconds()})
	m.txSendingUnpaced(txs)
	if m.placementSignals != nil {
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		m.nativeAudit.write(map[string]interface{}{"event": "batch_preparation_timing", "start": m.nativeCount,
			"count": len(txs), "effect_binding_ns": boundAt.Sub(at).Nanoseconds(),
			"live_plan_ns": planAt.Sub(boundAt).Nanoseconds(), "route_and_dispatch_ns": time.Since(planAt).Nanoseconds(),
			"gc_pause_total_delta_ns": after.PauseTotalNs - before.PauseTotalNs,
			"gc_cycles":               after.NumGC - before.NumGC, "heap_alloc_bytes": after.HeapAlloc,
			"diagnostic_cost_included": true})
	}
	if m.livePlan != nil {
		if m.livePlan.cursor != len(txs) {
			panic("live placement plan was not consumed exactly once")
		}
		m.livePlan = nil
	}
	// Sender goroutines have been scheduled, not necessarily written to their
	// sockets. The auditor joins each occurrence's first_socket_write event to
	// this batch; that later boundary includes asynchronous transport waiting.
	m.nativeAudit.write(map[string]interface{}{"event": "batch_dispatch_scheduled", "start": m.nativeCount,
		"count": len(txs), "started_at": at.UTC(), "ended_at": time.Now().UTC(), "planned_at": deadline.UTC()})
	m.nativeCount += len(txs)
	if m.groupControl != nil {
		atomic.StoreInt64(&m.groupControl.inputCount, int64(m.nativeCount))
		// Use the controller's real admission watermark, after every occurrence
		// in this batch has passed txSendingUnpaced. This only closes NEW
		// commands; already accepted migrations and execution still drain.
		if len(txs) > 0 && params.TotalDataSize > 0 && m.nativeCount >= params.TotalDataSize {
			if err := m.groupControl.publishNativeInputEOP(); err != nil {
				panic(err)
			}
		}
	}
	// Retain cumulative plan lateness in batch_input; no attempt to erase it.
}

// A persistent buffered audit stream: no per-transaction open/close/fsync.
// Flush errors invalidate the run rather than fabricating successful evidence.
type batchAudit struct {
	mu       sync.Mutex
	f        *os.File
	w        *bufio.Writer
	observer *observerProjection
	async    *auditWriter
}

func newBatchAudit(path string, observerInput ...string) *batchAudit {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		panic(e)
	}
	a := &batchAudit{f: f, w: bufio.NewWriterSize(f, 1024*1024)}
	if len(observerInput) > 0 && observerInput[0] != "" {
		a.observer = newObserverProjection(path, observerInput[0])
	}
	a.startWriter()
	return a
}
func (a *batchAudit) write(x interface{}) {
	if a.async != nil {
		raw, err := json.Marshal(x)
		if err != nil {
			panic(err)
		}
		a.enqueueAudit(auditRecord{data: append(raw, '\n')})
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if e := json.NewEncoder(a.w).Encode(x); e != nil {
		panic(e)
	}
}
func (a *batchAudit) flush() {
	if a.async != nil {
		done := make(chan struct{})
		a.enqueueAudit(auditRecord{barrier: done})
		<-done
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if e := a.w.Flush(); e != nil {
		panic(e)
	}
	if a.observer != nil {
		a.observer.flush()
	}
}
