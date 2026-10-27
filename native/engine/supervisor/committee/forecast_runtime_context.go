package committee

import (
	"blockEmulator/utils"
	"encoding/json"
	"fmt"
	"os"
)

// Snapshot after due activations, under the same manager lock. Unknown routes
// use the same hash fallback as relay injection, explicitly labelled below.
func (m *RuntimeUnknownAdmissionManager) forecastContextLocked(round int) map[string]interface{} {
	s := m.integratedStateLocked()
	routes := map[string]uint64{}
	sources := map[string]string{}
	if s.modelAnchor != nil {
		for account := range s.modelAnchor.Accounts {
			shard, ok := s.authority.resolve(account)
			source := "mapping_authority"
			if !ok {
				shard = uint64(utils.Addr2Shard(account))
				source = "relay_hash_fallback"
			}
			routes[account] = shard
			sources[account] = source
		}
	}
	anchors := map[string]uint64{}
	for id, shard := range m.stableShardByCluster {
		anchors[fmt.Sprintf("stable:%d", id)] = shard
	}
	queues := map[uint64]float64{}
	rates := map[uint64]float64{}
	serviceEvidence := map[uint64]forecastServiceObservation{}
	for shard, sample := range s.forecastService {
		if sample.Epoch < round && round-sample.Epoch <= 2 {
			rates[shard] = sample.Rate
			serviceEvidence[shard] = sample
		}
	}
	for shard, value := range m.liveQueueByShard {
		queues[shard] = value
	}
	// Opt-in cost profile: measured committed trie payload, NOT total network
	// handoff. Preserve the strict legacy contract unless explicitly selected.
	payloads := m.forecastPayloadEvidenceLocked(routes, round)
	var stateBytes interface{}
	scope := "full_handoff_unavailable_payload_observations_separate"
	profile := os.Getenv("SAA_FORECAST_STATE_COST_SCOPE")
	if profile != "" && profile != "committed_payload_v1" {
		panic("invalid SAA_FORECAST_STATE_COST_SCOPE")
	}
	if profile == "committed_payload_v1" {
		measured := map[string]uint64{}
		for account, sample := range payloads {
			measured[account] = sample.Bytes
		}
		stateBytes = measured
		scope = "committed_trie_payload_only_excludes_protocol_queue_retries"
	}
	missing := []string{"full_handoff_bytes", "real_effect_delay"}
	if len(rates) != int(m.shardNum) {
		missing = append(missing, "service_rate_estimate")
	}
	if stateBytes == nil {
		missing = append(missing, "state_bytes")
	}
	return map[string]interface{}{
		"schema": "forecast_runtime_context_v1", "stream_id": s.forecastStreamID,
		"shard_count":                    m.shardNum,
		"account_state_payload_evidence": payloads,
		"state_bytes_scope":              scope,
		"observed_sequence":              s.forecastObservedSequence, "logical_round": round,
		"mapping_version": s.authority.routeVersion, "anchor_version": s.forecastAnchorVersion,
		"routes": routes, "route_sources": sources, "anchor_shards": anchors,
		"queues": queues, "queue_epoch": m.pressureHintEpoch, "queue_age_epochs": round - m.pressureHintEpoch,
		"service_rates": rates, "service_rate_evidence": serviceEvidence, "service_rate_semantics": "last_consecutive_commit_throughput_estimate_not_capacity", "baseline_arrivals": nil, "seconds": nil, "state_bytes": stateBytes,
		"missing":   append(missing, "full_arrival_projection", "forecast_interval_seconds"),
		"semantics": "runtime snapshot; queue samples are not service capacity",
	}
}

func (m *RuntimeUnknownAdmissionManager) exportForecastContextLocked(round int) error {
	s := m.integratedStateLocked()
	if s.controllerMode != forecastControllerMode || s.forecastContextPath == "" || s.modelAnchor == nil || s.forecastContextVersion == s.forecastAnchorVersion {
		return nil
	}
	data, err := json.Marshal(m.forecastContextLocked(round))
	if err != nil {
		return err
	}
	flags := os.O_WRONLY | os.O_APPEND
	if s.forecastContextVersion == 0 {
		flags |= os.O_CREATE | os.O_EXCL
	}
	file, err := os.OpenFile(s.forecastContextPath, flags, 0600)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	n, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if n != len(data) {
		return fmt.Errorf("short context write")
	}
	if closeErr != nil {
		return closeErr
	}
	s.forecastContextVersion = s.forecastAnchorVersion
	s.forecastContextCount++
	return nil
}
