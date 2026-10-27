package committee

// One journal consumer owns the Forest provider. Planning reads an immutable
// earlier snapshot; transport cuts are sealed only when a ready plan installs.
// This is fresh-run execution, not a cross-store crash/restart protocol.
import (
	"blockEmulator/params"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type forestAsyncJob struct {
	generation, baseEpoch uint64
	overrides             map[string]uint64
	eligible              []string
	reason                string
}

type forestAsyncResult struct {
	job     forestAsyncJob
	reply   forestEpochReply
	report  map[string]interface{}
	started time.Time
}

type forestAsyncPipeline struct {
	e                  *clpaEpochController
	p                  *forestEpochProvider
	journal, reader    *os.File
	notify, stop, done chan struct{}
	intents            chan forestAsyncJob
	results            chan *forestAsyncResult
	closeOnce          sync.Once
	closeErr           error
	observed           atomic.Uint64
	written, durable   atomic.Uint64 // Dispatch may lead durability; observation and planning may not.
	// The run goroutine alone owns scheduling, results and the write handle.
	majorRounds, nextStart        int
	generation                    uint64
	busy, inputDone, finalPlanned bool
	finalArchived                 bool // A full EOF candidate is evidence, not a new ownership epoch.
	ready                         *forestAsyncResult
}

func newForestAsyncPipeline(e *clpaEpochController, p *forestEpochProvider, path string) (*forestAsyncPipeline, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	r, err := os.Open(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	major := e.rounds * epochPositiveEnv("BLOCK_EMULATOR_CLPA_MAJOR_EPOCHS", 20)
	a := &forestAsyncPipeline{e: e, p: p, journal: f, reader: r, majorRounds: major,
		nextStart: asyncHalf(major), notify: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		intents: make(chan forestAsyncJob, 1), results: make(chan *forestAsyncResult, 1)}
	go a.consume()
	return a, nil
}

func asyncHalf(value int) int {
	if value/2 < 1 {
		return 1
	}
	return value / 2
}

func (a *forestAsyncPipeline) wake() {
	select {
	case a.notify <- struct{}{}:
	default:
	}
}
func (a *forestAsyncPipeline) append(rows []forestEpochObservation) error {
	if len(rows) == 0 {
		return fmt.Errorf("empty journal batch")
	}
	started := time.Now()
	b, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	n, err := a.journal.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err == nil {
		a.written.Add(uint64(len(rows)))
		a.e.record("observation_journal_append", map[string]interface{}{"rows": len(rows), "bytes": n, "seconds": time.Since(started).Seconds()})
	}
	return err
}

// One durability barrier covers all rows already appended when it begins.
// Dispatch never waits for this barrier; evidence consumption must wait.
// Concurrent appends can persist too, but only the conservative written cut
// captured before Sync is published. Only the consumer (or joined Close) calls.
func (a *forestAsyncPipeline) syncThrough(needed uint64) error {
	previous := a.durable.Load()
	if previous >= needed {
		return nil
	}
	target := a.written.Load()
	if target < needed {
		return fmt.Errorf("journal written prefix precedes requested evidence")
	}
	started := time.Now()
	if err := a.journal.Sync(); err != nil {
		return err
	}
	a.e.record("observation_journal_sync", map[string]interface{}{
		"previous_durable_prefix": previous, "durable_prefix": target,
		"rows_covered": target - previous, "seconds": time.Since(started).Seconds()})
	a.durable.Store(target)
	return nil
}

func (a *forestAsyncPipeline) fail(err error) {
	a.e.mu.Lock()
	defer a.e.mu.Unlock()
	a.e.failLocked(fmt.Errorf("async Forest pipeline: %w", err))
}

func (a *forestAsyncPipeline) wait() bool {
	select {
	case <-a.stop:
		return false
	case <-a.notify:
	case <-time.After(50 * time.Millisecond):
	}
	return true
}

type forestObservationBatch struct {
	rows  []forestEpochObservation
	bytes int
}

// The single consumer retains a partial line or an undispatched complete
// batch. EOF ends a group immediately: it never waits to fill eight batches.
type forestObservationJournalReader struct {
	r       *bufio.Reader
	partial []byte
	pending *forestObservationBatch
}

func (j *forestObservationJournalReader) next() (*forestObservationBatch, error) {
	for {
		part, err := j.r.ReadSlice('\n')
		if len(j.partial)+len(part) > forestEpochObserveGroupBytes-1024 {
			return nil, fmt.Errorf("journal batch exceeds bounded observation group size")
		}
		j.partial = append(j.partial, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		batch := &forestObservationBatch{bytes: len(j.partial)}
		if err = json.Unmarshal(j.partial, &batch.rows); err != nil {
			return nil, err
		}
		j.partial = nil
		if len(batch.rows) == 0 {
			return nil, fmt.Errorf("empty journal batch")
		}
		return batch, nil
	}
}

func (j *forestObservationJournalReader) group(dispatched uint64) ([][]forestEpochObservation, error) {
	var batches [][]forestEpochObservation
	bytes := 0
	for len(batches) < forestEpochObserveGroupBatches {
		if j.pending == nil {
			batch, err := j.next()
			if err != nil {
				return nil, err
			}
			j.pending = batch
		}
		if j.pending == nil || j.pending.rows[len(j.pending.rows)-1].Sequence >= dispatched || bytes+j.pending.bytes > forestEpochObserveGroupBytes-1024 {
			break
		}
		batches = append(batches, j.pending.rows)
		bytes += j.pending.bytes
		j.pending = nil
	}
	return batches, nil
}

func (a *forestAsyncPipeline) consume() {
	defer close(a.done)
	defer a.reader.Close()
	j := forestObservationJournalReader{r: bufio.NewReaderSize(a.reader, 256<<10)}
	var active *forestAsyncResult
	lastPoll := time.Time{}
	for {
		select {
		case <-a.stop:
			return
		default:
		}
		if active == nil {
			select {
			case job := <-a.intents:
				request := forestEpochRequest{Op: "plan_start", Epoch: job.generation, ObservedPrefix: a.p.observed,
					CurrentMapping: job.overrides, CurrentMappingSHA256: clpaEpochMappingDigest(job.overrides),
					InitialLedgerSHA256: a.p.initialLedgerHash, Eligible: job.eligible}
				started := time.Now()
				reply, err := a.p.call(request)
				if err != nil {
					a.fail(err)
					return
				}
				if reply.Epoch != job.generation || reply.ObservedPrefix != a.p.observed || reply.PrefixSHA256 != a.p.prefixHash ||
					reply.CurrentMappingSHA256 != request.CurrentMappingSHA256 || reply.InitialLedgerSHA256 != a.p.initialLedgerHash || !reply.Running {
					a.fail(fmt.Errorf("async snapshot start binding mismatch"))
					return
				}
				active = &forestAsyncResult{job: job, reply: reply, started: started}
				a.e.record("forest_plan_started", map[string]interface{}{"generation": job.generation, "base_mapping_epoch": job.baseEpoch,
					"observed_count": reply.ObservedPrefix, "prefix_sha256": reply.PrefixSHA256, "current_mapping_sha256": request.CurrentMappingSHA256,
					"reason": job.reason, "cadence_rounds": a.majorRounds})
				lastPoll = time.Time{}
			default:
			}
		}
		if active != nil && time.Since(lastPoll) >= 250*time.Millisecond {
			reply, err := a.p.call(forestEpochRequest{Op: "plan_poll", Epoch: active.job.generation})
			if err != nil {
				a.fail(err)
				return
			}
			lastPoll = time.Now()
			if !reply.Running {
				if err := a.validateResult(active, reply); err != nil {
					a.fail(err)
					return
				}
				active.reply = reply
				a.e.record("forest_plan_ready", map[string]interface{}{"generation": active.job.generation, "base_mapping_epoch": active.job.baseEpoch,
					"observed_count": reply.ObservedPrefix, "prefix_sha256": reply.PrefixSHA256, "delta_accounts": len(reply.Delta),
					"current_mapping_sha256": reply.CurrentMappingSHA256,
					"reason":                 active.job.reason, "planning_wall_seconds": time.Since(active.started).Seconds()})
				select {
				case a.results <- active:
				case <-a.stop:
					return
				}
				active = nil
			}
		}
		a.e.mu.Lock()
		dispatched := uint64(a.e.dispatched)
		a.e.mu.Unlock()
		batches, err := j.group(dispatched)
		if err != nil {
			a.fail(err)
			return
		}
		if len(batches) == 0 {
			if !a.wait() {
				return
			}
			continue
		}
		last := batches[len(batches)-1]
		if err := a.syncThrough(last[len(last)-1].Sequence + 1); err != nil {
			a.fail(err)
			return
		}
		if err := a.p.ObserveGroup(batches); err != nil {
			a.fail(err)
			return
		}
		a.observed.Store(a.p.observed)
		a.e.record("forest_watermark", map[string]interface{}{"observed_count": a.p.observed, "dispatched": dispatched,
			"observation_lag": dispatched - a.p.observed, "prefix_sha256": a.p.prefixHash, "group_batches": len(batches)})
	}
}

func (a *forestAsyncPipeline) validateResult(result *forestAsyncResult, reply forestEpochReply) error {
	snapshot := result.reply
	if reply.Epoch != result.job.generation || reply.ObservedPrefix != snapshot.ObservedPrefix || reply.PrefixSHA256 != snapshot.PrefixSHA256 ||
		reply.CurrentMappingSHA256 != clpaEpochMappingDigest(result.job.overrides) || reply.InitialLedgerSHA256 != a.p.initialLedgerHash || reply.Delta == nil {
		return fmt.Errorf("async plan differs from frozen prefix/mapping/ledger")
	}
	eligible := map[string]bool{}
	for _, account := range result.job.eligible {
		eligible[account] = true
	}
	for account, target := range reply.Delta {
		_, initial := a.p.initialRoutes[account]
		owner, ok := result.job.overrides[account]
		if !ok {
			owner = a.p.baseOwner(account)
		}
		if !forestCanonicalAccount(account) || target >= uint64(params.ShardNum) || (!initial && !eligible[account]) || target == owner {
			return fmt.Errorf("invalid or uncreated async move %s", account)
		}
	}
	var explanation, evidence map[string]interface{}
	if json.Unmarshal(reply.Report, &explanation) != nil || explanation == nil || json.Unmarshal(reply.Evidence, &evidence) != nil || evidence == nil {
		return fmt.Errorf("async plan omitted explanatory evidence")
	}
	result.report = map[string]interface{}{"provider": "forest", "epoch": reply.Epoch, "observed_count": reply.ObservedPrefix,
		"input_sha256": a.p.inputHash, "initial_ledger_sha256": a.p.initialLedgerHash, "current_mapping_sha256": reply.CurrentMappingSHA256,
		"prefix_sha256": reply.PrefixSHA256, "override_count": len(result.job.overrides), "report": explanation, "evidence": evidence,
		"base_mapping_epoch": result.job.baseEpoch, "proposal_generation": result.job.generation, "reason": result.job.reason,
		"planning_wall_seconds": time.Since(result.started).Seconds(), "snapshot_relation": "evidence prefix <= eventual sealed transport prefix"}
	return nil
}

func (a *forestAsyncPipeline) minRoundsLocked() int {
	n := int(^uint(0) >> 1)
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		if a.e.counts[shard] < n {
			n = a.e.counts[shard]
		}
	}
	return n
}

func (a *forestAsyncPipeline) tick() {
	e := a.e
	e.mu.Lock()
	defer e.mu.Unlock()
	select {
	case result := <-a.results:
		a.ready = result
	default:
	}
	// Already broadcast installations must finish. Only a proposal that has
	// not entered native transport may be archived after the last transaction.
	if e.plan != nil {
		return
	}
	if a.ready != nil && !e.nativeCompleteAt.IsZero() {
		if _, err := a.proposalLocked(); err != nil {
			e.failLocked(fmt.Errorf("archive candidate ownership: %w", err))
			return
		}
		r := a.ready
		final := r.job.reason == "EOF_ARCHIVE"
		e.record("forest_plan_archived", map[string]interface{}{
			"provider": "forest", "generation": r.job.generation, "proposal_generation": r.job.generation,
			"base_mapping_epoch": r.job.baseEpoch, "observed_count": r.reply.ObservedPrefix,
			"evidence_prefix": r.reply.ObservedPrefix, "prefix_sha256": r.reply.PrefixSHA256,
			"evidence_prefix_sha256": r.reply.PrefixSHA256, "current_mapping_sha256": r.reply.CurrentMappingSHA256,
			"dispatched": e.dispatched, "mapping": r.reply.Delta, "moved_accounts": len(r.reply.Delta),
			"planner_report": r.report, "reason": r.job.reason, "archive_reason": "native_complete",
			"disposition": "NOT_APPLIED", "final_snapshot": final, "native_complete": true,
		})
		a.finalArchived = final
		a.ready, a.busy = nil, false
	}
	if a.ready != nil && len(a.ready.reply.Delta) == 0 {
		e.record("forest_plan_skipped", map[string]interface{}{"generation": a.ready.job.generation, "reason": "unchanged_layout", "observed_count": a.ready.reply.ObservedPrefix})
		a.ready = nil
		a.busy = false
		a.nextStart = a.minRoundsLocked() + asyncHalf(a.majorRounds)
	}
	if a.busy {
		return
	}
	reason := "PERIODIC_PRECOMPUTE"
	if !e.nativeCompleteAt.IsZero() {
		if !a.inputDone || a.finalPlanned || a.observed.Load() != uint64(e.dispatched) {
			return
		}
		reason = "EOF_ARCHIVE"
		a.finalPlanned = true
	} else if a.minRoundsLocked() < a.nextStart || a.observed.Load() == 0 {
		return
	}
	a.generation++
	job := forestAsyncJob{generation: a.generation, baseEpoch: e.epoch, overrides: copyEpochMapping(e.committedOverrides),
		eligible: make([]string, 0, len(e.created)), reason: reason}
	for account := range e.created {
		job.eligible = append(job.eligible, account)
	}
	sort.Strings(job.eligible)
	a.busy = true
	a.intents <- job
	a.wake()
}

func (a *forestAsyncPipeline) installReadyLocked() bool {
	return a.ready != nil && a.ready.job.reason != "EOF_ARCHIVE" &&
		a.e.nativeCompleteAt.IsZero() && a.minRoundsLocked() >= a.majorRounds
}

func (a *forestAsyncPipeline) proposalLocked() (epochMappingProposal, error) {
	r := a.ready
	if r == nil || r.job.baseEpoch != a.e.epoch || clpaEpochMappingDigest(r.job.overrides) != clpaEpochMappingDigest(a.e.committedOverrides) || r.reply.ObservedPrefix > uint64(a.e.dispatched) {
		return epochMappingProposal{}, fmt.Errorf("ready plan no longer matches committed ownership")
	}
	p := epochMappingProposal{delta: r.reply.Delta, moves: map[string]map[string]uint64{}}
	for account, target := range p.delta {
		_, initial := a.e.initialRoutes[account]
		if !initial && !a.e.created[account] {
			return p, fmt.Errorf("ready account lacks terminal state evidence")
		}
		source := a.e.ccm.fetchModifiedMap(account)
		owner, ok := r.job.overrides[account]
		if !ok {
			owner = a.p.baseOwner(account)
		}
		if source != owner || source == target {
			return p, fmt.Errorf("ready move source changed")
		}
		p.moves[account] = map[string]uint64{"from": source, "to": target}
	}
	return p, nil
}

func (a *forestAsyncPipeline) installed() {
	a.ready = nil
	a.busy = false
	a.nextStart = asyncHalf(a.majorRounds)
}

func (a *forestAsyncPipeline) finishInput() {
	a.e.mu.Lock()
	a.inputDone = true
	a.e.mu.Unlock()
	a.wake()
}
func (a *forestAsyncPipeline) finished() bool {
	a.e.mu.Lock()
	defer a.e.mu.Unlock()
	return a.finishedLocked()
}
func (a *forestAsyncPipeline) finishedLocked() bool {
	return a.inputDone && !a.e.nativeCompleteAt.IsZero() && a.finalArchived &&
		!a.busy && a.ready == nil && a.e.plan == nil && a.observed.Load() == uint64(a.e.dispatched)
}

func (a *forestAsyncPipeline) Close() error {
	a.closeOnce.Do(func() {
		close(a.stop)
		a.wake()
		<-a.done
		if err := a.syncThrough(a.written.Load()); err != nil {
			a.closeErr = err
		}
		if err := a.journal.Close(); err != nil {
			if a.closeErr == nil {
				a.closeErr = err
			}
		}
		if a.observed.Load() == uint64(a.e.dispatched) {
			a.e.record("forest_observation_end", map[string]interface{}{
				"observed_count": a.observed.Load(), "prefix_sha256": a.p.prefixHash, "input_sha256": a.p.inputHash})
		}
	})
	return a.closeErr
}
