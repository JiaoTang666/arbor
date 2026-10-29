package committee

// Forest facts are an additive, local observation interface. They never select
// a shard, certify distributed ownership, predict demand, or write a receipt.
import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

const ForestFactsSchemaV2 = "forest_facts_v2"
const forestUnknownV2 = "unknown"

type ForestAccountFactV2 struct {
	Parent           string `json:"parent,omitempty"`
	PermanentOrigin  string `json:"permanent_origin"`
	PlanningOrigin   string `json:"planning_origin"`
	FirstSequence    uint64 `json:"first_sequence"`
	SourceTime       int64  `json:"source_time"`
	Shard            uint64 `json:"shard"`
	Historical       bool   `json:"historical"`
	BirthKnown       bool   `json:"birth_known"`
	PositionKnown    bool   `json:"position_known"`
	CommittedCreated bool   `json:"committed_created"`
	RuntimeNewNew    bool   `json:"runtime_newnew"`
}

type forestCreditV2 struct {
	family   string
	units    int64
	category int
}
type forestWorkKeyV2 struct {
	family string
	shard  uint64
}
type forestFamilyWorkV2 struct {
	assigned, confirmed int64
	category            int
}
type forestBoundV2 struct {
	a, b   string
	sa, sb uint64
	at     int64
}
type forestPendingV2 struct {
	hash, a, b string
	sa, sb     uint64
	at         int64
	credits    []forestCreditV2
	done       uint8
}
type forestBirthV2 struct {
	at    int64
	shard uint64
}
type forestWindowWorkV2 struct {
	at     int64
	shard  uint64
	family string
	units  int64
}

// ForestFactsV2 serializes its own state. The seed map is transferred to this
// object and must remain immutable afterwards: copying 4M address keys is not
// necessary. Only runtime accounts and touched historical positions are added.
// Completed receipt identities are deliberately not retained: the existing
// lifecycle verifier must validate and deduplicate before calling Confirm.
type ForestFactsV2 struct {
	mu                                      sync.Mutex
	seed                                    map[string]string
	accounts                                map[string]ForestAccountFactV2
	namespace, clockID                      string
	cutoff                                  uint64
	shards                                  int
	boundPrefix, assignedPrefix             uint64
	sourceTime, now, confirmedReceiptMonoNS int64
	bound                                   map[uint64]forestBoundV2
	pending                                 map[uint64]*forestPendingV2
	assigned, confirmed                     []int64
	categoryAssigned, categoryConfirmed     [3][]int64
	familyWork                              map[forestWorkKeyV2]forestFamilyWorkV2
	new20, work5, mass20                    []int64
	square20                                []float64
	active20                                []map[string]int64
	births                                  []forestBirthV2
	short, long                             []forestWindowWorkV2
	birthHead, shortHead, longHead          int
	completed                               uint64
}

type ForestFactsSnapshotV2 struct {
	Schema                      string             `json:"schema"`
	SemanticVersion             int                `json:"semantic_version"`
	Namespace                   string             `json:"namespace"`
	ClockID                     string             `json:"clock_id"`
	RuntimeMonoNS               int64              `json:"runtime_mono_ns"`
	SourceTime                  int64              `json:"source_time"`
	HistoryCutoff               uint64             `json:"history_cutoff"`
	BoundPrefix                 uint64             `json:"bound_prefix_exclusive"`
	AssignedPrefix              uint64             `json:"assigned_prefix_exclusive"`
	ConfirmedReceiptMonoNS      int64              `json:"confirmed_receipt_mono_ns"`
	AttributionView             string             `json:"attribution_view"`
	NewOrigins20s               []int64            `json:"new_origins_20s"`
	ActiveOrigins20s            []int64            `json:"active_origins_20s"`
	AssignedNNHalfStages5s      []int64            `json:"assigned_nn_half_stages_5s"`
	Concentration20s            []float64          `json:"concentration_20s"`
	NoActivity20s               []bool             `json:"no_activity_20s"`
	AssignedStages              []int64            `json:"assigned_stages"`
	ConfirmedStages             []int64            `json:"confirmed_stages"`
	AssignedCategoryHalfStages  map[string][]int64 `json:"assigned_category_half_stages"`
	ConfirmedCategoryHalfStages map[string][]int64 `json:"confirmed_category_half_stages"`
	OutstandingOccurrences      int                `json:"outstanding_occurrences"`
	RetainedRecentEvents        int                `json:"retained_recent_events"`
	TimeBoundary                string             `json:"time_boundary"`
}

type ForestWorkAuditV2 struct {
	Schema                          string `json:"schema"`
	Status                          string `json:"status"`
	Bound, Assigned, Completed      uint64
	OutstandingOccurrences          int `json:"outstanding_occurrences"`
	AssignedStages, ConfirmedStages []int64
	AttributionView                 string `json:"attribution_view"`
	FamilyShardEntries              int    `json:"family_shard_entries"`
}

func NewForestFactsV2(seed map[string]string, namespace string, historicalCutoff uint64, clockID string, shards int) (*ForestFactsV2, error) {
	if seed == nil || namespace == "" || clockID == "" || shards < 1 {
		return nil, fmt.Errorf("forest v2 requires seed, namespace, clock and shards")
	}
	f := &ForestFactsV2{seed: seed, namespace: namespace, clockID: clockID, cutoff: historicalCutoff, shards: shards,
		accounts: map[string]ForestAccountFactV2{}, bound: map[uint64]forestBoundV2{}, pending: map[uint64]*forestPendingV2{},
		familyWork: map[forestWorkKeyV2]forestFamilyWorkV2{}, sourceTime: -1, now: -1, confirmedReceiptMonoNS: -1,
		assigned: make([]int64, shards), confirmed: make([]int64, shards), new20: make([]int64, shards), work5: make([]int64, shards),
		mass20: make([]int64, shards), square20: make([]float64, shards), active20: make([]map[string]int64, shards)}
	for i := 0; i < 3; i++ {
		f.categoryAssigned[i] = make([]int64, shards)
		f.categoryConfirmed[i] = make([]int64, shards)
	}
	for s := range f.active20 {
		f.active20[s] = map[string]int64{}
	}
	return f, nil
}

func (f *ForestFactsV2) account(a string) (ForestAccountFactV2, bool) {
	if v, ok := f.accounts[a]; ok {
		return v, true
	}
	root, ok := f.seed[a]
	if !ok {
		return ForestAccountFactV2{}, false
	}
	if root == "" {
		root = forestUnknownV2
	}
	return ForestAccountFactV2{PermanentOrigin: root, PlanningOrigin: "historical:" + a, Historical: true, CommittedCreated: true}, true
}

func (f *ForestFactsV2) Source(account string) (ForestAccountFactV2, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.account(account)
}

func (f *ForestFactsV2) validTime(at int64, clock string) error {
	if clock != f.clockID || at < 0 || at < f.now {
		return fmt.Errorf("forest v2 missing, foreign or regressed monotonic clock")
	}
	return nil
}

// advance expires each event once. Confirmation delivery does not call this:
// a delayed receipt need not precede the latest local assignment timestamp.
func (f *ForestFactsV2) advance(at int64) {
	f.now = at
	for f.birthHead < len(f.births) && f.births[f.birthHead].at <= at-20*int64(time.Second) {
		e := f.births[f.birthHead]
		f.new20[e.shard]--
		f.birthHead++
	}
	for f.shortHead < len(f.short) && f.short[f.shortHead].at <= at-5*int64(time.Second) {
		e := f.short[f.shortHead]
		f.work5[e.shard] -= e.units
		f.short[f.shortHead] = forestWindowWorkV2{}
		f.shortHead++
	}
	for f.longHead < len(f.long) && f.long[f.longHead].at <= at-20*int64(time.Second) {
		e := f.long[f.longHead]
		f.changeMass(e.shard, e.family, -e.units)
		f.long[f.longHead] = forestWindowWorkV2{}
		f.longHead++
	}
	if f.birthHead > 4096 && f.birthHead*2 > len(f.births) {
		f.births = append([]forestBirthV2(nil), f.births[f.birthHead:]...)
		f.birthHead = 0
	}
	compact := func(events *[]forestWindowWorkV2, head *int) {
		if *head > 4096 && *head*2 > len(*events) {
			*events = append([]forestWindowWorkV2(nil), (*events)[*head:]...)
			*head = 0
		}
	}
	compact(&f.short, &f.shortHead)
	compact(&f.long, &f.longHead)
}

func (f *ForestFactsV2) changeMass(s uint64, family string, delta int64) {
	old := f.active20[s][family]
	next := old + delta
	f.mass20[s] += delta
	f.square20[s] += float64(next)*float64(next) - float64(old)*float64(old)
	if next == 0 {
		delete(f.active20[s], family)
	} else {
		f.active20[s][family] = next
	}
}

// Bind observes the already selected actual placement. It does not constrain
// new-old co-location: lineage and execution position are different facts.
// Sequence is local zero-based accepted input order, not a transaction hash.
func (f *ForestFactsV2) Bind(seq uint64, a, b string, knownA, knownB bool, sa, sb uint64, sourceTime, monoNS int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if seq != f.boundPrefix || a == "" || b == "" || sa >= uint64(f.shards) || sb >= uint64(f.shards) || sourceTime < 0 || sourceTime < f.sourceTime {
		return fmt.Errorf("forest v2 invalid binding identity, sequence, route or source time")
	}
	if err := f.validTime(monoNS, f.clockID); err != nil {
		return err
	}
	va, ka := f.account(a)
	vb, kb := f.account(b)
	if ka != knownA || kb != knownB || (a == b && (knownA != knownB || sa != sb)) {
		return fmt.Errorf("forest v2 binding visibility/self mismatch")
	}
	if ka && va.PositionKnown && va.Shard != sa || kb && vb.PositionKnown && vb.Shard != sb {
		return fmt.Errorf("forest v2 existing account rebound")
	}
	if f.cutoff > uint64(^uint64(0)>>1)-2 || seq > uint64(^uint64(0)>>1)-2-f.cutoff {
		return fmt.Errorf("forest v2 canonical signed global occurrence overflow")
	}
	f.advance(monoNS)
	if !ka && !kb {
		root := "newnew:" + f.namespace + ":" + strconv.FormatUint(f.cutoff+seq, 10)
		permanent := f.namespace + ":" + strconv.FormatInt(-int64(f.cutoff+seq)-2, 10)
		va = ForestAccountFactV2{PermanentOrigin: permanent, PlanningOrigin: root, FirstSequence: f.cutoff + seq, SourceTime: sourceTime, BirthKnown: true, RuntimeNewNew: true}
		vb = va
		f.births = append(f.births, forestBirthV2{monoNS, sa})
		f.new20[sa]++
		// One source event can be present on two shards under native hash.
		// Count each (origin, first-host shard) once; do not invent co-location.
		if sa != sb {
			f.births = append(f.births, forestBirthV2{monoNS, sb})
			f.new20[sb]++
		}
	} else if !ka {
		va = ForestAccountFactV2{Parent: b, PermanentOrigin: vb.PermanentOrigin, PlanningOrigin: vb.PlanningOrigin, FirstSequence: f.cutoff + seq, SourceTime: sourceTime, BirthKnown: true, RuntimeNewNew: vb.RuntimeNewNew}
	} else if !kb {
		vb = ForestAccountFactV2{Parent: a, PermanentOrigin: va.PermanentOrigin, PlanningOrigin: va.PlanningOrigin, FirstSequence: f.cutoff + seq, SourceTime: sourceTime, BirthKnown: true, RuntimeNewNew: va.RuntimeNewNew}
	}
	va.Shard = sa
	va.PositionKnown = true
	vb.Shard = sb
	vb.PositionKnown = true
	f.accounts[a] = va
	if b != a {
		f.accounts[b] = vb
	}
	f.bound[seq] = forestBoundV2{a, b, sa, sb, monoNS}
	f.boundPrefix++
	f.sourceTime = sourceTime
	return nil
}

func forestCategoryV2(v ForestAccountFactV2) int {
	if v.PlanningOrigin == "" || v.PlanningOrigin == forestUnknownV2 {
		return 2
	}
	if v.RuntimeNewNew {
		return 1
	}
	return 0
}

func forestSplitV2(a, b ForestAccountFactV2) []forestCreditV2 {
	root := func(v ForestAccountFactV2) string {
		if v.PlanningOrigin == "" {
			return forestUnknownV2
		}
		return v.PlanningOrigin
	}
	if root(a) == root(b) {
		return []forestCreditV2{{root(a), 2, forestCategoryV2(a)}}
	}
	return []forestCreditV2{{root(a), 1, forestCategoryV2(a)}, {root(b), 1, forestCategoryV2(b)}}
}

func (f *ForestFactsV2) Assign(seq uint64, hash, a, b string, sa, sb uint64, monoNS int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	x, ok := f.bound[seq]
	if seq != f.assignedPrefix || !ok || hash == "" || x.a != a || x.b != b || x.sa != sa || x.sb != sb || monoNS < x.at {
		return fmt.Errorf("forest v2 assignment differs from observed binding")
	}
	if err := f.validTime(monoNS, f.clockID); err != nil {
		return err
	}
	va, _ := f.account(a)
	vb, _ := f.account(b)
	split := forestSplitV2(va, vb)
	f.advance(monoNS)
	r := &forestPendingV2{hash: hash, a: a, b: b, sa: sa, sb: sb, at: monoNS, credits: split}
	f.pending[seq] = r
	shards := []uint64{sa}
	if sa != sb {
		shards = append(shards, sb)
	}
	for _, s := range shards {
		f.assigned[s]++
		for _, c := range split {
			key := forestWorkKeyV2{c.family, s}
			w := f.familyWork[key]
			w.assigned += c.units
			w.category = c.category
			f.familyWork[key] = w
			f.categoryAssigned[c.category][s] += c.units
			if c.category == 1 {
				e := forestWindowWorkV2{monoNS, s, c.family, c.units}
				f.short = append(f.short, e)
				f.long = append(f.long, e)
				f.work5[s] += c.units
				f.changeMass(s, c.family, c.units)
			}
		}
	}
	delete(f.bound, seq)
	f.assignedPrefix++
	return nil
}

// Confirm is for the already validated, deduplicated local lifecycle hook.
// Use ConfirmWithClock when reading externally serialized evidence.
func (f *ForestFactsV2) Confirm(seq uint64, hash, leg string, shard uint64, monoNS int64) (bool, error) {
	return f.ConfirmWithClock(seq, hash, leg, shard, monoNS, f.clockID)
}

func (f *ForestFactsV2) ConfirmWithClock(seq uint64, hash, leg string, shard uint64, monoNS int64, clockID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.pending[seq]
	if !ok {
		return false, fmt.Errorf("forest v2 receipt absent: completed identities require lifecycle deduplication")
	}
	if hash != r.hash || clockID != f.clockID || monoNS < r.at {
		return false, fmt.Errorf("forest v2 receipt identity/clock mismatch")
	}
	var bit, all uint8
	if r.sa == r.sb {
		if leg != "inner" || shard != r.sa {
			return false, fmt.Errorf("forest v2 inner receipt mismatch")
		}
		bit = 1
		all = 1
	} else {
		all = 3
		if leg == "relay1" && shard == r.sa {
			bit = 1
		} else if leg == "relay2" && shard == r.sb {
			bit = 2
		} else {
			return false, fmt.Errorf("forest v2 relay receipt mismatch")
		}
	}
	if r.done&bit != 0 {
		return false, nil
	}
	r.done |= bit
	f.confirmed[shard]++
	if monoNS > f.confirmedReceiptMonoNS {
		f.confirmedReceiptMonoNS = monoNS
	}
	for _, c := range r.credits {
		key := forestWorkKeyV2{c.family, shard}
		w := f.familyWork[key]
		w.confirmed += c.units
		f.familyWork[key] = w
		f.categoryConfirmed[c.category][shard] += c.units
	}
	if leg == "inner" || leg == "relay1" {
		v, _ := f.account(r.a)
		v.CommittedCreated = true
		f.accounts[r.a] = v
	}
	if leg == "inner" || leg == "relay2" {
		v, _ := f.account(r.b)
		v.CommittedCreated = true
		f.accounts[r.b] = v
	}
	if r.done == all {
		delete(f.pending, seq)
		f.completed++
	}
	return true, nil
}

func (f *ForestFactsV2) Snapshot(monoNS int64, clockID string) (ForestFactsSnapshotV2, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshotLocked(monoNS, clockID)
}

// SnapshotNow captures its clock inside the same critical section as the facts.
// A query timestamp captured before locking could include a later confirmation.
func (f *ForestFactsV2) SnapshotNow(clock *placementClock) (ForestFactsSnapshotV2, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if clock == nil {
		return ForestFactsSnapshotV2{}, fmt.Errorf("forest v2 missing query clock")
	}
	at := clock.now()
	return f.snapshotLocked(at.MonoNS, at.ClockID)
}

func (f *ForestFactsV2) snapshotLocked(monoNS int64, clockID string) (ForestFactsSnapshotV2, error) {
	if err := f.validTime(monoNS, clockID); err != nil {
		return ForestFactsSnapshotV2{}, err
	}
	if monoNS < f.confirmedReceiptMonoNS {
		return ForestFactsSnapshotV2{}, fmt.Errorf("forest v2 snapshot predates already visible confirmation")
	}
	f.advance(monoNS)
	copyN := func(x []int64) []int64 { return append([]int64(nil), x...) }
	v := ForestFactsSnapshotV2{Schema: ForestFactsSchemaV2, SemanticVersion: 2, Namespace: f.namespace, ClockID: f.clockID, RuntimeMonoNS: monoNS,
		SourceTime: f.sourceTime, HistoryCutoff: f.cutoff, BoundPrefix: f.boundPrefix, AssignedPrefix: f.assignedPrefix, ConfirmedReceiptMonoNS: f.confirmedReceiptMonoNS, AttributionView: "planning_cutoff",
		NewOrigins20s: copyN(f.new20), ActiveOrigins20s: make([]int64, f.shards), AssignedNNHalfStages5s: copyN(f.work5),
		Concentration20s: make([]float64, f.shards), NoActivity20s: make([]bool, f.shards), AssignedStages: copyN(f.assigned), ConfirmedStages: copyN(f.confirmed),
		AssignedCategoryHalfStages: map[string][]int64{}, ConfirmedCategoryHalfStages: map[string][]int64{}, OutstandingOccurrences: len(f.pending),
		RetainedRecentEvents: len(f.births) - f.birthHead + len(f.short) - f.shortHead + len(f.long) - f.longHead,
		TimeBoundary:         "(now-window,now], only consumed sequence events; assigned relay2 may not yet be queued"}
	for s := 0; s < f.shards; s++ {
		v.ActiveOrigins20s[s] = int64(len(f.active20[s]))
		v.NoActivity20s[s] = f.mass20[s] == 0
		if f.mass20[s] > 0 {
			v.Concentration20s[s] = f.square20[s] / (float64(f.mass20[s]) * float64(f.mass20[s]))
		}
	}
	for i, name := range []string{"historical_entrance", "runtime_newnew", "unknown"} {
		v.AssignedCategoryHalfStages[name] = copyN(f.categoryAssigned[i])
		v.ConfirmedCategoryHalfStages[name] = copyN(f.categoryConfirmed[i])
	}
	return v, nil
}

// Audit is explicitly O(number of family/shard entries), never a decision hook.
// Audit checks internal conservation at an arbitrary prefix, which may be
// incomplete. CompleteAudit additionally ties closeout to the input contract.
func (f *ForestFactsV2) CompleteAudit(expected uint64) (ForestWorkAuditV2, error) {
	a, err := f.Audit()
	if err != nil {
		return a, err
	}
	if a.Bound != expected || a.Assigned != expected || a.Completed != expected || a.OutstandingOccurrences != 0 {
		return a, fmt.Errorf("forest v2 incomplete closeout: expected=%d bound=%d assigned=%d completed=%d outstanding=%d", expected, a.Bound, a.Assigned, a.Completed, a.OutstandingOccurrences)
	}
	return a, nil
}

func (f *ForestFactsV2) Audit() (ForestWorkAuditV2, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.assignedPrefix > f.boundPrefix || f.completed+uint64(len(f.pending)) != f.assignedPrefix || uint64(len(f.bound)) != f.boundPrefix-f.assignedPrefix {
		return ForestWorkAuditV2{}, fmt.Errorf("forest v2 occurrence conservation failure")
	}
	a, c := make([]int64, f.shards), make([]int64, f.shards)
	var ca, cc [3][]int64
	for i := 0; i < 3; i++ {
		ca[i] = make([]int64, f.shards)
		cc[i] = make([]int64, f.shards)
	}
	for k, w := range f.familyWork {
		if k.shard >= uint64(f.shards) || w.category < 0 || w.category > 2 || w.assigned < 0 || w.confirmed < 0 || w.confirmed > w.assigned {
			return ForestWorkAuditV2{}, fmt.Errorf("forest v2 invalid family ledger")
		}
		a[k.shard] += w.assigned
		c[k.shard] += w.confirmed
		ca[w.category][k.shard] += w.assigned
		cc[w.category][k.shard] += w.confirmed
	}
	for s := 0; s < f.shards; s++ {
		if a[s] != 2*f.assigned[s] || c[s] != 2*f.confirmed[s] || f.confirmed[s] > f.assigned[s] {
			return ForestWorkAuditV2{}, fmt.Errorf("forest v2 half-stage conservation failure shard %d", s)
		}
		for j := 0; j < 3; j++ {
			if ca[j][s] != f.categoryAssigned[j][s] || cc[j][s] != f.categoryConfirmed[j][s] {
				return ForestWorkAuditV2{}, fmt.Errorf("forest v2 category conservation failure")
			}
		}
	}
	return ForestWorkAuditV2{Schema: ForestFactsSchemaV2, Status: "PASS", Bound: f.boundPrefix, Assigned: f.assignedPrefix, Completed: f.completed,
		OutstandingOccurrences: len(f.pending), AssignedStages: append([]int64(nil), f.assigned...), ConfirmedStages: append([]int64(nil), f.confirmed...),
		AttributionView: "planning_cutoff", FamilyShardEntries: len(f.familyWork)}, nil
}
