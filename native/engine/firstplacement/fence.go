package firstplacement

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Fence is the single admission registry for one fresh, exclusive RawRelay run.
// Every occurrence (including identical transaction hashes) gets a distinct ID.
// Terminal reports are trusted node facts; timeout never means terminal.
type Fence struct {
	ordinaryPipeline bool // ordinary work waits for transport, not consensus completion
	mu               sync.Mutex
	session          string
	next, generation uint64
	flights          map[string]*Flight
	leases           map[string]*Lease
	owners           map[string]string
	changed          chan struct{}
}

// CompleteCreator removes ownership only after the designated occurrence is
// terminal. Caller must first validate all account creation receipts.
func (f *Fence) CompleteCreator(request string) ([]Flight, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.leases[request]
	if l == nil || l.CreatorID == "" || !f.flights[l.CreatorID].Terminal {
		return nil, fmt.Errorf("creator_not_terminal")
	}
	for _, a := range l.Accounts {
		if f.owners[a] != request {
			return nil, fmt.Errorf("lease_not_owned")
		}
	}
	for _, a := range l.Accounts {
		delete(f.owners, a)
	}
	f.signal()
	return nil, nil // creation eligibility is separate from queue scheduling
}

func (f *Fence) earlier(x *Flight) bool {
	for _, p := range f.flights {
		if p.Sequence < x.Sequence && !p.Terminal && (p.Sender == x.Sender || p.Sender == x.Recipient || p.Recipient == x.Sender || p.Recipient == x.Recipient) {
			return true
		}
	}
	return false
}

// Creation still uses earlier/remaining to drain FULL lifecycles. Ordinary
// traffic only waits for earlier dispatches sharing an endpoint. Uncertain
// transport is deliberately not treated as written or terminal.
func (f *Fence) dispatchBlocked(x *Flight) bool {
	if !f.ordinaryPipeline {
		return f.earlier(x)
	}
	for _, p := range f.flights {
		if p.Sequence < x.Sequence && !p.Terminal && p.State != "IN_FLIGHT" &&
			(p.Sender == x.Sender || p.Sender == x.Recipient || p.Recipient == x.Sender || p.Recipient == x.Recipient) {
			return true
		}
	}
	return false
}
func (f *Fence) gateBlocks(x *Flight) bool {
	for _, a := range []string{x.Sender, x.Recipient} {
		if owner := f.owners[a]; owner != "" && x.Sequence > f.leases[owner].Cut {
			return true
		}
	}
	return false
}

// Reserve both endpoint positions before any network I/O. All accounts share
// one admission sequence. Pre-cut queued work must remain drainable.
func (f *Fence) TakeReady() []Flight {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []Flight{}
	for _, x := range f.flights {
		if x.Held && !x.Routed && !x.Terminal && !f.dispatchBlocked(x) && !f.gateBlocks(x) {
			x.Held = false
			x.State = "READY"
			out = append(out, *x)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	return out
}
func (f *Fence) MarkSend(id string, failure string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if x := f.flights[id]; x != nil && x.Routed && !x.Terminal {
		x.State = "IN_FLIGHT"
		if failure != "" {
			x.State = "SEND_UNCERTAIN"
		}
		f.signal()
	}
}
func (f *Fence) Flight(id string) (Flight, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	x, ok := f.flights[id]
	if !ok {
		return Flight{}, false
	}
	return *x, true
}

func (f *Fence) CreatorTerminal(request string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.leases[request]
	return l != nil && l.CreatorID != "" && f.flights[l.CreatorID].Terminal
}

type Flight struct {
	SourceReported, TargetReported   bool
	State                            string
	ID, Sender, Recipient            string
	Sequence                         uint64
	Held, Routed, Terminal           bool
	SourceShard, TargetShard         int
	AcceptedAt, RoutedAt, TerminalAt time.Time
}
type Lease struct {
	CreatorID                string
	RequestID, Session       string
	Generation, Cut          uint64
	Accounts                 []string
	OpenedAt, QueryStartedAt time.Time
	Querying                 bool
}

// RouteCreator atomically authorizes exactly one held occurrence. commit must
// either fail without mutation or commit the route synchronously; it must not
// call back into Fence. Ownership remains installed for all other work.
func (f *Fence) RouteCreator(request, occurrence string, source, target int, at time.Time, commit func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, x := f.leases[request], f.flights[occurrence]
	if l == nil || !l.Querying || l.CreatorID != "" || f.remaining(l) != 0 || x == nil || !x.Held || x.Routed || x.Terminal || at.Before(x.AcceptedAt) || source < 0 || target < 0 || commit == nil {
		return fmt.Errorf("creator_not_releasable")
	}
	for _, a := range l.Accounts {
		if f.owners[a] != request || (x.Sender != a && x.Recipient != a) {
			return fmt.Errorf("creator_does_not_cover_lease")
		}
	}
	for _, a := range []string{x.Sender, x.Recipient} {
		if owner := f.owners[a]; owner != "" && owner != request {
			return fmt.Errorf("creator_blocked_by_other_lease")
		}
	}
	if f.earlier(x) {
		return fmt.Errorf("creator_has_earlier_account_work")
	}
	if err := commit(); err != nil {
		return err
	}
	l.CreatorID = occurrence
	x.Held = false
	x.Routed = true
	x.State = "SENDING"
	x.SourceShard = source
	x.TargetShard = target
	x.RoutedAt = at
	f.signal()
	return nil
}

// WaitIdle observes completion without acquiring a creation lease. The single
// source producer calls it before admitting a dependent creator. Nil accounts
// drains the complete finite stream. Uncertain sends never become terminal here.
func (f *Fence) WaitIdle(ctx context.Context, accounts []string) error {
	for {
		f.mu.Lock()
		busy := false
		if len(accounts) == 0 {
			busy = len(f.owners) > 0
		} else {
			for _, a := range accounts {
				if f.owners[a] != "" {
					busy = true
				}
			}
		}
		for _, x := range f.flights {
			if !x.Terminal && (len(accounts) == 0 || touches(x, accounts)) {
				busy = true
				break
			}
		}
		changed := f.changed
		f.mu.Unlock()
		if !busy {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Idle is an eligibility snapshot, not a lease or absence proof. The stream
// adapter has one admitting writer; reports only complete work.
func (f *Fence) Idle(accounts []string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range accounts {
		if f.owners[a] != "" {
			return false
		}
	}
	for _, x := range f.flights {
		if !x.Terminal && touches(x, accounts) {
			return false
		}
	}
	return true
}

func (f *Fence) Outstanding() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, x := range f.flights {
		if !x.Terminal {
			n++
		}
	}
	return n
}

func NewFence(session string) *Fence {
	return &Fence{session: session, flights: map[string]*Flight{}, leases: map[string]*Lease{}, owners: map[string]string{}, changed: make(chan struct{})}
}

// NewPipelinedFence keeps creation leases and full drain accounting, without
// imposing full-lifecycle serialization on ordinary RawRelay transactions.
func NewPipelinedFence(session string) *Fence {
	f := NewFence(session)
	f.ordinaryPipeline = true
	return f
}

// PendingDispatch excludes successfully written ordinary in-flight work from
// the entry buffer limit. Outstanding remains unchanged for final drain.
func (f *Fence) PendingDispatch() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, x := range f.flights {
		if !x.Terminal && x.State != "IN_FLIGHT" {
			n++
		}
	}
	return n
}
func (f *Fence) signal() { close(f.changed); f.changed = make(chan struct{}) }
func (f *Fence) Admit(sender, recipient string, at time.Time) (Flight, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.session == "" || !canonical(sender) || !canonical(recipient) || at.IsZero() {
		return Flight{}, fmt.Errorf("invalid_admission")
	}
	f.next++
	id := fmt.Sprintf("%s/%d", f.session, f.next)
	x := Flight{ID: id, Sender: sender, Recipient: recipient, Sequence: f.next, AcceptedAt: at, Held: f.owners[sender] != "" || f.owners[recipient] != ""}
	x.Held = x.Held || f.dispatchBlocked(&x)
	x.State = "READY"
	if x.Held {
		x.State = "HOLD"
	}
	f.flights[id] = &x
	f.signal()
	return x, nil
}

// Called after route selection, BEFORE insertion in the outbound send buffer.
// Begin and Admit share the same mutex. An accepted pre-cut occurrence remains
// drainable even if Begin happens before route selection or network delivery.
func (f *Fence) Route(id string, source, target int, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	x := f.flights[id]
	if x == nil || x.Held || x.Terminal || x.Routed || source < 0 || target < 0 || at.Before(x.AcceptedAt) {
		return fmt.Errorf("not_admitted_for_routing")
	}
	x.Routed = true
	x.State = "SENDING"
	x.SourceShard = source
	x.TargetShard = target
	x.RoutedAt = at
	return nil
}
func copyLease(l *Lease) Lease { c := *l; c.Accounts = append([]string(nil), l.Accounts...); return c }
func (f *Fence) Begin(id string, accounts []string, at time.Time) (Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id == "" || len(accounts) == 0 || at.IsZero() {
		return Lease{}, fmt.Errorf("invalid_lease")
	}
	if _, ok := f.leases[id]; ok {
		return Lease{}, fmt.Errorf("query_request_already_owned")
	}
	seen := map[string]bool{}
	for _, a := range accounts {
		if !canonical(a) || seen[a] {
			return Lease{}, fmt.Errorf("invalid_or_duplicate_account")
		}
		seen[a] = true
		if f.owners[a] != "" {
			return Lease{}, fmt.Errorf("query_lease_conflict")
		}
	}
	f.generation++
	l := &Lease{RequestID: id, Session: f.session, Generation: f.generation, Cut: f.next, Accounts: append([]string(nil), accounts...), OpenedAt: at}
	f.leases[id] = l
	for _, a := range accounts {
		f.owners[a] = id
	}
	f.signal()
	return copyLease(l), nil
}
func touches(x *Flight, accounts []string) bool {
	for _, a := range accounts {
		if x.Sender == a || x.Recipient == a {
			return true
		}
	}
	return false
}
func (f *Fence) remaining(l *Lease) int {
	n := 0
	for _, x := range f.flights {
		if x.Sequence <= l.Cut && !x.Terminal && touches(x, l.Accounts) {
			n++
		}
	}
	return n
}
func (f *Fence) WaitDrained(ctx context.Context, id string) (Lease, error) {
	for {
		if err := ctx.Err(); err != nil {
			return Lease{}, err
		}
		f.mu.Lock()
		l := f.leases[id]
		if l == nil {
			f.mu.Unlock()
			return Lease{}, fmt.Errorf("lease_missing")
		}
		if f.remaining(l) == 0 {
			l.Querying = true
			if l.QueryStartedAt.IsZero() {
				l.QueryStartedAt = time.Now()
			}
			out := copyLease(l)
			f.mu.Unlock()
			return out, nil
		}
		ch := f.changed
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return Lease{}, ctx.Err()
		case <-ch:
		}
	}
}

// Both endpoints and both frozen shards must match the registered occurrence.
// A relay first leg does not complete the occurrence, even if called 'commit'.
func (f *Fence) Report(id, sender, recipient string, source, target, reportShard int, leg string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	x := f.flights[id]
	if x == nil || !x.Routed || x.Held || x.Sender != sender || x.Recipient != recipient || x.SourceShard != source || x.TargetShard != target || at.IsZero() {
		return fmt.Errorf("unregistered_or_mismatched_report")
	}
	terminal := false
	switch leg {
	case "inner":
		if source != target || reportShard != source {
			return fmt.Errorf("invalid_inner_report")
		}
		terminal = true
		x.SourceReported = true
		x.TargetReported = true
	case "relay1":
		if source == target || reportShard != source {
			return fmt.Errorf("invalid_relay1_report")
		}
		x.SourceReported = true
		terminal = x.TargetReported
	case "relay2":
		if source == target || reportShard != target {
			return fmt.Errorf("invalid_relay2_report")
		}
		x.TargetReported = true
		terminal = x.SourceReported
	default:
		return fmt.Errorf("unsupported_terminal_evidence")
	}
	if terminal && !x.Terminal {
		x.Terminal = true
		x.State = "TERMINAL"
		x.TerminalAt = at
		f.signal()
	}
	return nil
}

// The caller retains HOLD after success or failure. No expiry/retry can release
// held work; creator/creation-receipt release is a separate, subsequent stage.
func (f *Fence) Lease(id string) (Lease, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.leases[id]
	if l == nil {
		return Lease{}, 0, fmt.Errorf("lease_missing")
	}
	return copyLease(l), f.remaining(l), nil
}
func (f *Fence) HeldCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, x := range f.flights {
		if x.Held {
			n++
		}
	}
	return n
}
