package committee

import (
	"blockEmulator/message"
	"encoding/hex"
)

type forecastStatePayloadObservation struct {
	Bytes     uint64 `json:"bytes"`
	Shard     uint64 `json:"source_shard"`
	Epoch     int    `json:"commit_height"`
	StateRoot string `json:"state_root"`
}

func (m *RuntimeUnknownAdmissionManager) observeForecastStatePayloadsLocked(b *message.BlockInfoMsg) {
	s := m.integratedStateLocked()
	if s.forecastStatePayloads == nil {
		s.forecastStatePayloads = map[uint64]map[string]forecastStatePayloadObservation{}
	}
	// Update touched accounts only. Unrelated commits must not erase evidence
	// or refresh its original height/root. An unscoped read failure invalidates
	// the shard cache conservatively because the affected accounts are unknown.
	root, err := hex.DecodeString(b.AccountStateRoot)
	if err != nil || len(root) != 32 || b.AccountStateSizeStatus != "committed_touched_account_trie_payload_only" {
		delete(s.forecastStatePayloads, b.SenderShardID)
		return
	}
	samples := s.forecastStatePayloads[b.SenderShardID]
	if samples == nil {
		samples = map[string]forecastStatePayloadObservation{}
		s.forecastStatePayloads[b.SenderShardID] = samples
	}
	updates := map[string]forecastStatePayloadObservation{}
	for account, size := range b.AccountStateBytes {
		account = normalizeAdmissionAddress(account)
		if normalizeMappingAuthorityAccount(account) != account {
			continue
		}
		if size == 0 {
			delete(samples, account)
			continue
		}
		samples[account] = forecastStatePayloadObservation{Bytes: size, Shard: b.SenderShardID, Epoch: b.Epoch, StateRoot: b.AccountStateRoot}
		updates[account] = samples[account]
	}
	m.appendSAAIntegratedAuditLocked(map[string]interface{}{"record_type": "forecast_state_payload_observation", "stream_id": s.forecastStreamID, "shard_id": b.SenderShardID, "commit_height": b.Epoch, "state_root": b.AccountStateRoot, "accounts": updates, "scope": "account_trie_payload_only_not_total_handoff"})
}

func (m *RuntimeUnknownAdmissionManager) forecastPayloadEvidenceLocked(routes map[string]uint64, round int) map[string]forecastStatePayloadObservation {
	s := m.integratedStateLocked()
	result := map[string]forecastStatePayloadObservation{}
	for account, source := range routes {
		sample, ok := s.forecastStatePayloads[source][account]
		if ok && sample.Epoch < round && round-sample.Epoch <= 2 {
			result[account] = sample
		}
	}
	return result
}
