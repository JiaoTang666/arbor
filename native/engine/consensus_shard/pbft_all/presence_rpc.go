package pbft_all

import (
	"blockEmulator/firstplacement"
	"encoding/json"
	"net"
	"os"
	"strings"
	"time"
)

type presenceCached struct {
	Query    firstplacement.PresenceQuery
	Response firstplacement.PresenceResponse
}

// Read-only RPC on the existing listener; serialized with PBFT commit/head
// changes. Replies on the request connection, not an unbounded async send queue.
func (p *PbftConsensusNode) tryPresenceRequest(c net.Conn, raw []byte) bool {
	if len(raw) < 30 || strings.TrimRight(string(raw[:30]), "\x00") != firstplacement.PresenceWireType {
		return false
	}
	var q firstplacement.PresenceQuery
	if json.Unmarshal(raw[30:], &q) != nil {
		return true
	}
	response := p.presenceResponse(q)
	c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_ = json.NewEncoder(c).Encode(response)
	return true
}
func (p *PbftConsensusNode) presenceResponse(q firstplacement.PresenceQuery) firstplacement.PresenceResponse {
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	r := firstplacement.PresenceResponse{Query: q, Status: "UNAVAILABLE", ObservedAt: time.Now()}
	if os.Getenv("SAA_FIRST_PLACEMENT_FENCE") != "true" || p.stopSignal.Load() || p.chainClosed.Load() || p.CurChain == nil || q.Session == "" || q.Shard != int(p.ShardID) {
		r.Failure = "presence_disabled_or_invalid_session"
		return r
	}
	if _, ok := p.ohm.(*RawRelayOutsideModule); !ok {
		r.Failure = "raw_relay_required"
		return r
	}
	if q.Hello {
		for _, name := range []string{"SAA_NATIVE_TARGET_INSTALL_PATH", "SAA_NATIVE_TARGET_ACTIVATION_PATH", "SAA_NATIVE_SOURCE_PREPARE_PATH"} {
			if os.Getenv(name) != "" {
				r.Failure = "external_state_writer_enabled"
				return r
			}
		}
		if q.RequestID != "" || q.Generation != 0 || q.Account != "" || q.MinAppliedHeight != 0 {
			r.Failure = "invalid_hello"
			return r
		}
		if p.presenceSession == "" {
			if p.CurChain.CurrentBlock.Header.Number != 0 || p.hasSeenWorkload.Load() {
				r.Failure = "fresh_run_required"
				return r
			}
			p.presenceSession = q.Session
			p.presenceCache = map[string]presenceCached{}
		}
		if p.presenceSession != q.Session {
			r.Failure = "supervisor_session_already_owned"
			return r
		}
		r.Status = "READY"
		return r
	}
	if p.presenceSession != q.Session || q.RequestID == "" || q.Generation == 0 {
		r.Failure = "session_or_generation_invalid"
		return r
	}
	key := q.RequestID + "/" + q.Account
	if p.CurChain.CurrentBlock.Header.Number < q.MinAppliedHeight {
		r.Height = p.CurChain.CurrentBlock.Header.Number
		r.Failure = "applied_height_below_query_floor"
		return r // a lagging response is not cached as absence
	}
	if old, ok := p.presenceCache[key]; ok {
		if old.Query != q {
			r.Failure = "query_identity_conflict"
			return r
		}
		return old.Response
	}
	observation := p.CurChain.ObserveAccountPresence(q.Account, time.Now())
	r.Status = string(observation.Status)
	r.Height = observation.BlockHeight
	r.StateRoot = observation.StateRoot
	r.ObservedAt = observation.ObservedAt
	r.Failure = observation.FailureCause
	p.presenceCache[key] = presenceCached{q, r}
	return r
}
