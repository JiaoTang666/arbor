package committee

import (
	"blockEmulator/message"
	"time"
)

type forecastCommitObservation struct {
	Epoch int
	Time  time.Time
}
type forecastServiceObservation struct {
	Epoch             int     `json:"epoch"`
	PreviousEpoch     int     `json:"previous_epoch"`
	Seconds           float64 `json:"seconds"`
	BlockTransactions int     `json:"block_transactions"`
	Rate              float64 `json:"transactions_per_second"`
}

// Relay BlockInfo.Epoch is block height; BlockBodyLength is len(block.Body).
// Consecutive same-shard commit times identify throughput, not saturation.
func (m *RuntimeUnknownAdmissionManager) observeForecastCommit(b *message.BlockInfoMsg) {
	if m == nil || b == nil || m.softDecisionMode != saaIntegratedDecisionMode {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	s := m.integratedStateLocked()
	if s.controllerMode != forecastControllerMode {
		return
	}
	if b.SenderShardID >= m.shardNum || b.Epoch < 0 || b.CommitTime.IsZero() || b.BlockBodyLength < 0 {
		return
	}
	if s.forecastCommits == nil {
		s.forecastCommits = map[uint64]forecastCommitObservation{}
		s.forecastService = map[uint64]forecastServiceObservation{}
	}
	previous, known := s.forecastCommits[b.SenderShardID]
	if known && b.Epoch <= previous.Epoch {
		return
	}
	if known && !b.CommitTime.After(previous.Time) {
		delete(s.forecastService, b.SenderShardID)
		return
	}
	s.forecastCommits[b.SenderShardID] = forecastCommitObservation{b.Epoch, b.CommitTime}
	m.observeForecastStatePayloadsLocked(b)
	if !known || b.Epoch != previous.Epoch+1 {
		delete(s.forecastService, b.SenderShardID)
		return
	}
	seconds := b.CommitTime.Sub(previous.Time).Seconds()
	sample := forecastServiceObservation{Epoch: b.Epoch, PreviousEpoch: previous.Epoch, Seconds: seconds, BlockTransactions: b.BlockBodyLength, Rate: float64(b.BlockBodyLength) / seconds}
	s.forecastService[b.SenderShardID] = sample
	m.appendSAAIntegratedAuditLocked(map[string]interface{}{"record_type": "forecast_service_observation", "stream_id": s.forecastStreamID, "shard_id": b.SenderShardID, "sample": sample, "semantics": "consecutive_commit_throughput_not_capacity"})
}
