package firstplacement

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

const PresenceWireType = "first_presence_v1"
const PresencePrefixBytes = 30

type PresenceQuery struct {
	MinAppliedHeight   uint64
	Session, RequestID string
	Generation         uint64
	Account            string
	Shard              int
	Hello              bool
}
type PresenceResponse struct {
	Query                  PresenceQuery
	Status                 string
	Height                 uint64
	StateRoot              string
	ObservedAt, ReceivedAt time.Time
	Failure                string
}

func QueryPresence(ctx context.Context, address string, q PresenceQuery) (PresenceResponse, error) {
	var r PresenceResponse
	deadline, ok := ctx.Deadline()
	if !ok {
		return r, fmt.Errorf("presence_deadline_required")
	}
	c, e := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if e != nil {
		return r, e
	}
	defer c.Close()
	c.SetDeadline(deadline)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			c.Close()
		case <-stop:
		}
	}()
	body, e := json.Marshal(q)
	if e != nil {
		return r, e
	}
	prefix := make([]byte, PresencePrefixBytes)
	copy(prefix, PresenceWireType)
	payload := append(append(prefix, body...), '\n')
	for len(payload) > 0 {
		n, err := c.Write(payload)
		if err != nil {
			return r, err
		}
		if n == 0 {
			return r, io.ErrShortWrite
		}
		payload = payload[n:]
	}
	line, e := bufio.NewReader(io.LimitReader(c, 1<<20)).ReadBytes('\n')
	if e != nil {
		return r, e
	}
	if e = json.Unmarshal(line, &r); e != nil {
		return r, e
	}
	r.ReceivedAt = time.Now()
	if r.Query != q {
		return r, fmt.Errorf("stale_or_mismatched_presence_generation")
	}
	return r, nil
}

type ProofSet struct {
	Session, RequestID string
	Generation         uint64
	Proofs             []PresenceResponse
}

// Shard heights are independent. Generation identifies the admission fence.
func ValidateProofSet(l Lease, shards int, proofs []PresenceResponse) error {
	if !l.Querying || l.QueryStartedAt.IsZero() || shards < 1 {
		return fmt.Errorf("query_before_drain")
	}
	accounts := map[string]bool{}
	for _, a := range l.Accounts {
		accounts[a] = true
	}
	seen := map[string]bool{}
	existing := map[string]int{}
	unavailable := false
	for _, p := range proofs {
		q := p.Query
		if p.Height < q.MinAppliedHeight {
			return fmt.Errorf("presence_below_applied_floor")
		}
		key := fmt.Sprintf("%s/%d", q.Account, q.Shard)
		if q.Session != l.Session || q.RequestID != l.RequestID || q.Generation != l.Generation || q.Hello || !accounts[q.Account] || q.Shard < 0 || q.Shard >= shards || seen[key] {
			return fmt.Errorf("duplicate_or_stale_presence_response")
		}
		seen[key] = true
		if p.ObservedAt.IsZero() || p.ReceivedAt.Before(l.QueryStartedAt) || !validRoot(p.StateRoot) {
			unavailable = true
		}
		switch p.Status {
		case "EXISTS":
			existing[q.Account]++
		case "ABSENT":
		case "UNAVAILABLE":
			unavailable = true
		default:
			return fmt.Errorf("invalid_presence_status")
		}
	}
	for _, n := range existing {
		if n > 1 {
			return fmt.Errorf("multiple_shard_state_invariant_violation")
		}
	}
	if len(existing) > 0 {
		return fmt.Errorf("existing_state_reconciliation_required")
	}
	if unavailable {
		return fmt.Errorf("presence_unavailable")
	}
	if len(seen) != len(accounts)*shards {
		return fmt.Errorf("presence_incomplete")
	}
	return nil
}
