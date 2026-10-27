package committee

import "encoding/hex"

// forecastAuthorityBridge validates controller decisions but owns no route map.
// The caller must hold the same lock used for the supplied authority.
type forecastAuthorityEnvelope struct {
	DecisionID     string `json:"decision_id"`
	InputHash      string `json:"input_sha256"`
	MappingVersion uint64 `json:"mapping_version"`
	AnchorVersion  uint64 `json:"anchor_version"`
	Account        string `json:"account"`
	SourceShard    uint64 `json:"source_shard"`
	TargetShard    uint64 `json:"target_shard"`
	EffectiveEpoch int    `json:"effective_epoch"`
	Keep           bool   `json:"keep"`
	QueueAvailable bool   `json:"queue_available"`
	QueueEpoch     int    `json:"queue_epoch"`
}

type forecastAuthorityBridge struct {
	authority *mappingAuthority
	seen      map[string]forecastAuthorityEnvelope
	results   map[string]mappingAuthorityDecision
}

func newForecastAuthorityBridge(a *mappingAuthority) *forecastAuthorityBridge {
	return &forecastAuthorityBridge{authority: a, seen: make(map[string]forecastAuthorityEnvelope), results: make(map[string]mappingAuthorityDecision)}
}

func (b *forecastAuthorityBridge) submit(e forecastAuthorityEnvelope, epoch int, anchorVersion uint64, maxQueueAge int) mappingAuthorityDecision {
	reject := func(reason string) mappingAuthorityDecision {
		return mappingAuthorityDecision{ProposalID: e.DecisionID, Status: mappingRejected, Reason: reason}
	}
	if b == nil || b.authority == nil {
		return reject("authority_missing")
	}
	if previous, ok := b.seen[e.DecisionID]; ok {
		if previous != e {
			return reject("decision_id_payload_conflict")
		}
		if latest, ok := b.authority.decisionByID[e.DecisionID]; ok {
			return latest
		}
		return b.results[e.DecisionID]
	}
	if b.authority == nil {
		return reject("authority_missing")
	}
	_, hashError := hex.DecodeString(e.InputHash)
	if len(e.InputHash) != 64 || hashError != nil || e.DecisionID == "" {
		return reject("decision_provenance_missing")
	}
	if e.MappingVersion != b.authority.routeVersion {
		return reject("mapping_version_mismatch")
	}
	if anchorVersion == 0 || e.AnchorVersion != anchorVersion {
		return reject("anchor_version_mismatch")
	}
	shard, known := b.authority.resolve(e.Account)
	if !known || shard != e.SourceShard {
		return reject("source_route_mismatch")
	}
	if e.Keep {
		if e.TargetShard != shard {
			return reject("keep_target_mismatch")
		}
		d := mappingAuthorityDecision{ProposalID: e.DecisionID, Status: mappingNoop, Reason: "controller_keep", BaseRouteVersion: e.MappingVersion, NewRouteVersion: b.authority.routeVersion, SubmittedEpoch: epoch}
		b.seen[e.DecisionID] = e
		b.results[e.DecisionID] = d
		return d
	}
	if !e.QueueAvailable || maxQueueAge < 0 || e.QueueEpoch > epoch || epoch-e.QueueEpoch > maxQueueAge {
		return reject("queue_missing_or_stale")
	}
	d := b.authority.submit(mappingAuthorityProposal{ProposalID: e.DecisionID, Source: "relation_work_controller", Addresses: []string{e.Account}, SourceShard: shard, TargetShard: e.TargetShard, BaseRouteVersion: e.MappingVersion, EffectiveEpoch: e.EffectiveEpoch}, epoch)
	b.seen[e.DecisionID] = e
	b.results[e.DecisionID] = d
	return d
}
