// Package firstplacement defines an offline, fail-closed preparation interface.
// It does not create native state, activate routes, or estimate service capacity.
package firstplacement

import (
	"fmt"
	"math"
	"time"
)

type Endpoint struct {
	Account          string // canonical native 40-character lowercase hex, without 0x
	Exists           bool
	AbsenceConfirmed bool // must come from native authority, never window first-seen
	Shard            int
}

type Queue struct {
	Operations             float64
	ObservedAt, ReceivedAt time.Time
	IncludedReservation    uint64 // explicit watermark, not inferred from timestamp
}

type WorkReservation struct {
	ID   uint64
	Work []float64
}

type Request struct {
	ID                                                             string
	Sender, Recipient                                              Endpoint
	HasHistoricalCandidates                                        bool
	MappingVersion                                                 uint64
	AuthorityObservedAt, AuthorityReceivedAt, DecisionAt, Deadline time.Time
	MaxAge                                                         time.Duration // explicitly chosen by caller; no silent default
	BaselineSender, BaselineRecipient                              int
	Queues                                                         []Queue
	Reservations                                                   []WorkReservation
}

type Choice struct {
	Sender, Recipient int
	Work              []float64
	PathPressure      float64 // operations proxy, NOT seconds or predicted workload
}

type Decision struct {
	Request        Request // frozen value copy for decision-time features
	Policy, Reason string
	Choice         Choice
}

func canonical(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func Prepare(r Request, policy string) (Decision, error) {
	fail := func(s string) (Decision, error) { return Decision{}, fmt.Errorf("%s", s) }
	n := len(r.Queues)
	if r.ID == "" || n == 0 || r.MaxAge <= 0 || r.DecisionAt.IsZero() || r.Deadline.Before(r.DecisionAt) {
		return fail("invalid_request_or_deadline")
	}
	if r.AuthorityObservedAt.IsZero() || r.AuthorityReceivedAt.Before(r.AuthorityObservedAt) || r.AuthorityReceivedAt.After(r.DecisionAt) || r.DecisionAt.Sub(r.AuthorityObservedAt) > r.MaxAge {
		return fail("stale_or_future_authority")
	}
	if r.HasHistoricalCandidates {
		return fail("outside_no_candidate_scope")
	}
	if r.Sender.Exists && r.Recipient.Exists {
		return fail("not_first_creation")
	}
	for _, e := range []Endpoint{r.Sender, r.Recipient} {
		if !canonical(e.Account) || (!e.Exists && !e.AbsenceConfirmed) || (e.Exists && (e.Shard < 0 || e.Shard >= n)) {
			return fail("invalid_endpoint_or_unconfirmed_absence")
		}
	}
	if r.BaselineSender < 0 || r.BaselineSender >= n || r.BaselineRecipient < 0 || r.BaselineRecipient >= n {
		return fail("invalid_baseline")
	}
	if policy != "baseline" && policy != "follow_partner" && policy != "shortest_queue" && policy != "path_queue" {
		return fail("unknown_policy")
	}
	// Copy all mutable features: outcomes must never rewrite the input record.
	r.Queues = append([]Queue(nil), r.Queues...)
	rs := make([]WorkReservation, len(r.Reservations))
	seen := map[uint64]bool{}
	for i, x := range r.Reservations {
		if x.ID == 0 || seen[x.ID] || len(x.Work) != n {
			return fail("invalid_reservation")
		}
		seen[x.ID] = true
		rs[i] = WorkReservation{x.ID, append([]float64(nil), x.Work...)}
	}
	r.Reservations = rs
	q := make([]float64, n)
	fresh := true
	for i, x := range r.Queues {
		if math.IsNaN(x.Operations) || math.IsInf(x.Operations, 0) || x.Operations < 0 {
			return fail("invalid_queue")
		}
		if x.ObservedAt.IsZero() || x.ReceivedAt.Before(x.ObservedAt) || x.ReceivedAt.After(r.DecisionAt) || r.DecisionAt.Sub(x.ObservedAt) > r.MaxAge {
			fresh = false
		}
		q[i] = x.Operations
		for _, pending := range rs {
			w := pending.Work[i]
			if math.IsNaN(w) || math.IsInf(w, 0) || w < 0 {
				return fail("invalid_pending_work")
			}
			if pending.ID > x.IncludedReservation {
				q[i] += w
			}
		}
	}
	effective := policy
	reason := "selected"
	if !fresh && (policy == "shortest_queue" || policy == "path_queue") {
		effective = "follow_partner"
		reason = "queue_unavailable_follow_partner_or_baseline"
	}
	bs, br := r.BaselineSender, r.BaselineRecipient
	if r.Sender.Exists {
		bs = r.Sender.Shard
	}
	if r.Recipient.Exists {
		br = r.Recipient.Shard
	}
	if effective == "follow_partner" {
		if r.Sender.Exists {
			br = bs
		}
		if r.Recipient.Exists {
			bs = br
		}
	}
	choices := []Choice{}
	for s := 0; s < n; s++ {
		for t := 0; t < n; t++ {
			if r.Sender.Exists && s != r.Sender.Shard || r.Recipient.Exists && t != r.Recipient.Shard {
				continue
			}
			if r.Sender.Account == r.Recipient.Account && s != t {
				continue
			}
			if (effective == "baseline" || effective == "follow_partner") && (s != bs || t != br) {
				continue
			}
			w := make([]float64, n)
			w[s]++
			if t != s {
				w[t]++
			}
			pressure := q[s] + 1
			if t != s {
				pressure += q[t] + 1
			}
			if effective == "shortest_queue" {
				pressure = 0
				if !r.Sender.Exists {
					pressure += q[s]
				}
				if !r.Recipient.Exists {
					pressure += q[t]
				}
			}
			choices = append(choices, Choice{s, t, w, pressure})
		}
	}
	if len(choices) == 0 {
		return fail("inconsistent_joint_endpoints")
	}
	best := choices[0]
	for _, c := range choices[1:] {
		if c.PathPressure < best.PathPressure {
			best = c
		}
	}
	return Decision{r, policy, reason, best}, nil
}

// Confirmation must be emitted by an atomic native creation-and-route adapter.
// This check is not an implementation or proof of that adapter's atomicity.
type Confirmation struct {
	RequestID                  string
	BaseVersion, ActiveVersion uint64
	Sender, Recipient          int
	CreatedAndActiveAt         time.Time
}

func (d Decision) CanBind(c Confirmation, bindAt time.Time) error {
	if c.RequestID != d.Request.ID || c.BaseVersion != d.Request.MappingVersion || c.ActiveVersion <= c.BaseVersion || c.Sender != d.Choice.Sender || c.Recipient != d.Choice.Recipient {
		return fmt.Errorf("activation_mismatch")
	}
	if c.CreatedAndActiveAt.Before(d.Request.DecisionAt) || bindAt.Before(c.CreatedAndActiveAt) || bindAt.After(d.Request.Deadline) {
		return fmt.Errorf("activation_not_ready_or_deadline_exceeded")
	}
	return nil
}

// Outcome is a separate later record, joined by request ID. It is not a feature.
type Outcome struct {
	RequestID                                                   string
	EnqueuedAt, SourceCommittedAt, RelayEnqueuedAt, CompletedAt time.Time
	Operations, RemainingOperations                             int
}
