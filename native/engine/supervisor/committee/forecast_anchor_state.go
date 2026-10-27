package committee

import (
	"encoding/hex"
	"fmt"
	"math"
)

type forecastAnchorAccount struct {
	SoftMembership        map[string]float64 `json:"soft_membership"`
	KnownRelationMass     float64            `json:"known_relation_mass"`
	UnresolvedPartnerMass float64            `json:"unresolved_partner_mass"`
}

type forecastAnchorState struct {
	StreamID                 string                           `json:"stream_id"`
	ObservationKind          string                           `json:"observation_kind"`
	AssignmentHash           string                           `json:"assignment_sha256"`
	PredictedUncoveredCounts []float64                        `json:"predicted_uncovered_counts"`
	MembershipSemantics      string                           `json:"membership_semantics"`
	AnchorVersion            uint64                           `json:"anchor_version"`
	EvidenceStart            uint64                           `json:"evidence_start"`
	EvidenceEnd              uint64                           `json:"evidence_end_exclusive"`
	CheckpointHash           string                           `json:"checkpoint_sha256"`
	Accounts                 map[string]forecastAnchorAccount `json:"accounts"`
}

// installForecastAnchorLocked does not infer a route or advance mapping version.
// observedSequence must be supplied from the same event stream as the forecast.
func (m *RuntimeUnknownAdmissionManager) installForecastAnchorLocked(next forecastAnchorState, observedSequence uint64) error {
	if m == nil || m.softDecisionMode != saaIntegratedDecisionMode {
		return fmt.Errorf("integrated runtime unavailable")
	}
	state := m.integratedStateLocked()
	if state.controllerMode == forecastControllerMode {
		if next.StreamID == "" || next.StreamID != state.forecastStreamID || next.ObservationKind != "relay_routed_input_not_commit" {
			return fmt.Errorf("Anchor stream mismatch")
		}
		if state.forecastCheckpointHash == "" || next.CheckpointHash != state.forecastCheckpointHash {
			return fmt.Errorf("unexpected model checkpoint")
		}
		if observedSequence != state.forecastObservedSequence {
			return fmt.Errorf("watermark not runtime owned")
		}
		if next.MembershipSemantics != "conditional_on_candidates" || len(next.PredictedUncoveredCounts) != 4 {
			return fmt.Errorf("missing coverage semantics")
		}
		if b, e := hex.DecodeString(next.AssignmentHash); e != nil || len(b) != 32 {
			return fmt.Errorf("invalid assignment hash")
		}
		for _, v := range next.PredictedUncoveredCounts {
			if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
				return fmt.Errorf("invalid uncovered forecast")
			}
		}
	}
	if next.AnchorVersion != state.forecastAnchorVersion+1 {
		return fmt.Errorf("anchor version not sequential")
	}
	if next.EvidenceEnd <= next.EvidenceStart || next.EvidenceEnd > observedSequence {
		return fmt.Errorf("invalid or future evidence")
	}
	if state.modelAnchor != nil && next.EvidenceEnd <= state.modelAnchor.EvidenceEnd {
		return fmt.Errorf("stale evidence")
	}
	if b, e := hex.DecodeString(next.CheckpointHash); e != nil || len(b) != 32 {
		return fmt.Errorf("invalid checkpoint hash")
	}
	clone := next
	clone.PredictedUncoveredCounts = append([]float64(nil), next.PredictedUncoveredCounts...)
	clone.Accounts = make(map[string]forecastAnchorAccount, len(next.Accounts))
	for account, a := range next.Accounts {
		if normalizeMappingAuthorityAccount(account) != account {
			return fmt.Errorf("invalid account")
		}
		if math.IsNaN(a.KnownRelationMass) || math.IsInf(a.KnownRelationMass, 0) || a.KnownRelationMass <= 0 {
			return fmt.Errorf("invalid relation mass")
		}
		sum := a.UnresolvedPartnerMass
		if math.IsNaN(sum) || math.IsInf(sum, 0) || sum < 0 || sum > 1 {
			return fmt.Errorf("invalid unresolved mass")
		}
		weights := make(map[string]float64, len(a.SoftMembership))
		for id, w := range a.SoftMembership {
			if id == "" || math.IsNaN(w) || math.IsInf(w, 0) || w < 0 || w > 1 {
				return fmt.Errorf("invalid membership")
			}
			sum += w
			weights[id] = w
		}
		if math.Abs(sum-1) > 1e-6 {
			return fmt.Errorf("membership mass not conserved")
		}
		a.SoftMembership = weights
		clone.Accounts[account] = a
	}
	state.modelAnchor = &clone
	state.forecastAnchorVersion = next.AnchorVersion
	m.appendSAAIntegratedAuditLocked(map[string]interface{}{"record_type": "forecast_anchor_installed", "anchor_version": next.AnchorVersion, "evidence_end_exclusive": next.EvidenceEnd, "checkpoint_sha256": next.CheckpointHash, "mapping_version": state.authority.routeVersion})
	return nil
}
