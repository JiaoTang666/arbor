package firstplacement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"
)

type PresenceProof struct {
	Session, RequestID     string
	Generation             uint64
	CommittedHeight        uint64
	Account                string `json:"account"`
	Shard                  int    `json:"shard"`
	BoundaryEpoch          int    `json:"boundary_epoch"`
	StateRoot              string `json:"state_root"`
	Status                 string `json:"status"`
	ObservedAt, ReceivedAt time.Time
}
type Claim struct {
	Account     string `json:"account"`
	TargetShard int    `json:"target_shard"`
}
type ReservationRequest struct {
	PresenceSession               string
	PresenceGeneration            uint64
	ID                            string `json:"id"`
	BaseRouteVersion              uint64 `json:"base_route_version"`
	EffectiveEpoch, DeadlineEpoch int
	SubmittedAt                   time.Time
	MaxEvidenceAge                time.Duration
	Claims                        []Claim
	Proofs                        []PresenceProof
}
type ReservationStatus string

const (
	Reserved          ReservationStatus = "RESERVED"
	RouteActive       ReservationStatus = "ROUTE_ACTIVE"
	CreationCommitted ReservationStatus = "CREATION_COMMITTED"
	Expired           ReservationStatus = "EXPIRED"
	Rejected          ReservationStatus = "REJECTED"
)

type Reservation struct {
	Request       ReservationRequest   `json:"request"`
	Fingerprint   string               `json:"fingerprint"`
	Status        ReservationStatus    `json:"status"`
	Reason        string               `json:"reason"`
	ActiveVersion uint64               `json:"active_version"`
	ActivatedAt   time.Time            `json:"activated_at"`
	Committed     map[string]time.Time `json:"committed"`
}
type Authority struct {
	mu             sync.Mutex
	ShardNum       int                    `json:"shard_num"`
	RouteVersion   uint64                 `json:"route_version"`
	Routes         map[string]int         `json:"routes"`
	AccountVersion map[string]uint64      `json:"account_version"`
	Owner          map[string]string      `json:"owner"`
	Requests       map[string]Reservation `json:"requests"`
}

func NewAuthority(shards int, routes map[string]int, version uint64) *Authority {
	a := &Authority{ShardNum: shards, RouteVersion: version, Routes: map[string]int{}, AccountVersion: map[string]uint64{}, Owner: map[string]string{}, Requests: map[string]Reservation{}}
	for k, v := range routes {
		a.Routes[k] = v
		a.AccountVersion[k] = version
	}
	return a
}
func validRoot(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
func normalizedRequest(r ReservationRequest) (ReservationRequest, string, error) {
	r.Claims = append([]Claim(nil), r.Claims...)
	r.Proofs = append([]PresenceProof(nil), r.Proofs...)
	sort.Slice(r.Claims, func(i, j int) bool { return r.Claims[i].Account < r.Claims[j].Account })
	sort.Slice(r.Proofs, func(i, j int) bool {
		if r.Proofs[i].Account != r.Proofs[j].Account {
			return r.Proofs[i].Account < r.Proofs[j].Account
		}
		return r.Proofs[i].Shard < r.Proofs[j].Shard
	})
	b, e := json.Marshal(r)
	if e != nil {
		return r, "", e
	}
	h := sha256.Sum256(b)
	return r, hex.EncodeToString(h[:]), nil
}
func cloneReservation(r Reservation) Reservation {
	r.Request.Claims = append([]Claim(nil), r.Request.Claims...)
	r.Request.Proofs = append([]PresenceProof(nil), r.Request.Proofs...)
	committed := r.Committed
	r.Committed = map[string]time.Time{}
	for k, v := range committed {
		r.Committed[k] = v
	}
	return r
}
func (a *Authority) Reserve(raw ReservationRequest, currentEpoch int) Reservation {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, fp, e := normalizedRequest(raw)
	reject := func(reason string) Reservation {
		return Reservation{Request: r, Fingerprint: fp, Status: Rejected, Reason: reason, Committed: map[string]time.Time{}}
	}
	if old, ok := a.Requests[r.ID]; ok {
		if old.Fingerprint == fp {
			return cloneReservation(old)
		}
		return reject("duplicate_request_conflict")
	}
	if e != nil || a.ShardNum < 1 || r.ID == "" || r.BaseRouteVersion != a.RouteVersion || r.EffectiveEpoch <= currentEpoch || r.DeadlineEpoch < r.EffectiveEpoch || r.SubmittedAt.IsZero() || r.MaxEvidenceAge <= 0 || len(r.Claims) == 0 {
		return reject("invalid_request_or_version")
	}
	claimSet := map[string]Claim{}
	for _, c := range r.Claims {
		if !canonical(c.Account) || c.TargetShard < 0 || c.TargetShard >= a.ShardNum {
			return reject("invalid_claim")
		}
		if _, ok := claimSet[c.Account]; ok {
			return reject("duplicate_claim")
		}
		if _, ok := a.Routes[c.Account]; ok {
			return reject("account_already_routed")
		}
		if _, ok := a.Owner[c.Account]; ok {
			return reject("account_already_reserved")
		}
		claimSet[c.Account] = c
	}
	proofCount := map[string]int{}
	seen := map[string]bool{}
	for _, p := range r.Proofs {
		if r.PresenceGeneration > 0 && (p.Generation != r.PresenceGeneration || p.Session != r.PresenceSession || p.RequestID != r.ID || r.PresenceSession == "") {
			return reject("presence_generation_mismatch")
		}
		key := fmt.Sprintf("%s/%d", p.Account, p.Shard)
		_, claimed := claimSet[p.Account]
		if !claimed || p.Shard < 0 || p.Shard >= a.ShardNum || seen[key] || (r.PresenceGeneration == 0 && p.BoundaryEpoch != currentEpoch) || !validRoot(p.StateRoot) || p.Status != "ABSENT" || p.ObservedAt.IsZero() || p.ReceivedAt.Before(p.ObservedAt) || p.ReceivedAt.After(r.SubmittedAt) || r.SubmittedAt.Sub(p.ObservedAt) > r.MaxEvidenceAge {
			return reject("incomplete_stale_or_non_absent_proof")
		}
		seen[key] = true
		proofCount[p.Account]++
	}
	for account := range claimSet {
		if proofCount[account] != a.ShardNum {
			return reject("incomplete_stale_or_non_absent_proof")
		}
	}
	res := Reservation{Request: r, Fingerprint: fp, Status: Reserved, Reason: "complete_absence_proof_reserved", Committed: map[string]time.Time{}}
	a.Requests[r.ID] = res
	for account := range claimSet {
		a.Owner[account] = r.ID
	}
	return cloneReservation(res)
}
func (a *Authority) ActivateDue(epoch int, at time.Time) []Reservation {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := []string{}
	for id, r := range a.Requests {
		if r.Status == Reserved && r.Request.EffectiveEpoch <= epoch {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := []Reservation{}
	for _, id := range ids {
		r := a.Requests[id]
		if epoch > r.Request.DeadlineEpoch {
			r.Status = Expired
			r.Reason = "activation_deadline_exceeded"
			for _, c := range r.Request.Claims {
				delete(a.Owner, c.Account)
			}
			a.Requests[id] = r
			out = append(out, cloneReservation(r))
			continue
		}
		valid := true
		for _, c := range r.Request.Claims {
			if a.Owner[c.Account] != id {
				valid = false
			}
			if _, ok := a.Routes[c.Account]; ok {
				valid = false
			}
		}
		if !valid {
			r.Status = Rejected
			r.Reason = "reservation_conflict_at_activation"
			for _, c := range r.Request.Claims {
				if a.Owner[c.Account] == id {
					delete(a.Owner, c.Account)
				}
			}
			a.Requests[id] = r
			out = append(out, cloneReservation(r))
			continue
		}
		a.RouteVersion++
		r.ActiveVersion = a.RouteVersion
		r.ActivatedAt = at
		r.Status = RouteActive
		r.Reason = "route_activated"
		for _, c := range r.Request.Claims {
			a.Routes[c.Account] = c.TargetShard
			a.AccountVersion[c.Account] = a.RouteVersion
		}
		a.Requests[id] = r
		out = append(out, cloneReservation(r))
	}
	return out
}
func (a *Authority) Bind(id string, version uint64, claims []Claim, epoch int, at time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.Requests[id]
	if !ok || r.Status != RouteActive {
		return fmt.Errorf("route_not_active")
	}
	if version != r.ActiveVersion || epoch < r.Request.EffectiveEpoch || epoch > r.Request.DeadlineEpoch || at.Before(r.ActivatedAt) {
		return fmt.Errorf("activation_mismatch_or_expired")
	}
	want := append([]Claim(nil), r.Request.Claims...)
	got := append([]Claim(nil), claims...)
	sort.Slice(got, func(i, j int) bool { return got[i].Account < got[j].Account })
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("claim_mismatch")
	}
	for _, c := range got {
		if a.Routes[c.Account] != c.TargetShard || a.AccountVersion[c.Account] != version {
			return fmt.Errorf("route_version_mismatch")
		}
	}
	return nil
}
func (a *Authority) Commit(id, account string, shard int, at time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.Requests[id]
	if !ok || (r.Status != RouteActive && r.Status != CreationCommitted) {
		return fmt.Errorf("route_not_active")
	}
	target := -1
	for _, c := range r.Request.Claims {
		if c.Account == account {
			target = c.TargetShard
		}
	}
	if target < 0 || target != shard || at.Before(r.ActivatedAt) {
		return fmt.Errorf("commit_mismatch")
	}
	if old, ok := r.Committed[account]; ok {
		if old.Equal(at) {
			return nil
		}
		return fmt.Errorf("conflicting_commit")
	}
	r.Committed[account] = at
	if len(r.Committed) == len(r.Request.Claims) {
		r.Status = CreationCommitted
		r.Reason = "all_first_states_committed"
		for _, c := range r.Request.Claims {
			delete(a.Owner, c.Account)
		}
	}
	a.Requests[id] = r
	return nil
}
func (a *Authority) Get(id string) (Reservation, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.Requests[id]
	return cloneReservation(r), ok
}

type authoritySnapshot struct {
	ShardNum       int                    `json:"shard_num"`
	RouteVersion   uint64                 `json:"route_version"`
	Routes         map[string]int         `json:"routes"`
	AccountVersion map[string]uint64      `json:"account_version"`
	Owner          map[string]string      `json:"owner"`
	Requests       map[string]Reservation `json:"requests"`
}

func (a *Authority) Snapshot() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return json.Marshal(authoritySnapshot{a.ShardNum, a.RouteVersion, a.Routes, a.AccountVersion, a.Owner, a.Requests})
}

func RestoreAuthority(payload []byte) (*Authority, error) {
	var s authoritySnapshot
	if err := json.Unmarshal(payload, &s); err != nil || s.ShardNum < 1 || s.Routes == nil || s.AccountVersion == nil || s.Owner == nil || s.Requests == nil {
		return nil, fmt.Errorf("invalid_snapshot")
	}
	a := &Authority{ShardNum: s.ShardNum, RouteVersion: s.RouteVersion, Routes: s.Routes, AccountVersion: s.AccountVersion, Owner: s.Owner, Requests: s.Requests}
	for account, shard := range a.Routes {
		if !canonical(account) || shard < 0 || shard >= a.ShardNum || a.AccountVersion[account] > a.RouteVersion {
			return nil, fmt.Errorf("invalid_route_snapshot")
		}
	}
	for id, r := range a.Requests {
		normalized, fingerprint, err := normalizedRequest(r.Request)
		if err != nil || id != r.Request.ID || !reflect.DeepEqual(normalized, r.Request) || fingerprint != r.Fingerprint || r.Committed == nil {
			return nil, fmt.Errorf("invalid_request_snapshot")
		}
		for _, c := range r.Request.Claims {
			switch r.Status {
			case Reserved:
				if a.Owner[c.Account] != id {
					return nil, fmt.Errorf("lost_reservation_owner")
				}
			case RouteActive, CreationCommitted:
				if a.Routes[c.Account] != c.TargetShard || a.AccountVersion[c.Account] != r.ActiveVersion {
					return nil, fmt.Errorf("lost_active_route")
				}
			case Expired, Rejected:
			default:
				return nil, fmt.Errorf("invalid_reservation_status")
			}
		}
	}
	for account, id := range a.Owner {
		r, ok := a.Requests[id]
		owned := false
		if ok && (r.Status == Reserved || r.Status == RouteActive) {
			for _, c := range r.Request.Claims {
				owned = owned || c.Account == account
			}
		}
		if !owned {
			return nil, fmt.Errorf("orphan_reservation_owner")
		}
	}
	return a, nil
}
