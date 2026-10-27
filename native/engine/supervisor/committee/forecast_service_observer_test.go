package committee

import (
	"blockEmulator/message"
	"testing"
	"time"
)

func TestForecastServiceUsesConsecutiveCommitCounts(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2}
	defer saaIntegratedRuntimeStates.Delete(m)
	start := time.Unix(100, 0)
	send := func(epoch, count, seconds int) {
		m.observeForecastCommit(&message.BlockInfoMsg{Epoch: epoch, BlockBodyLength: count, CommitTime: start.Add(time.Duration(seconds) * time.Second)})
	}
	send(1, 100, 0)
	send(2, 20, 2)
	s := m.integratedStateLocked()
	if s.forecastService[0].Rate != 10 {
		t.Fatal("block count confused with cumulative count")
	}
	send(2, 999, 3)
	if s.forecastService[0].Rate != 10 {
		t.Fatal("duplicate changed estimate")
	}
	snapshot := m.forecastContextLocked(3)
	if snapshot["service_rates"].(map[uint64]float64)[0] != 10 {
		t.Fatal("fresh estimate not exported")
	}
	if len(m.forecastContextLocked(5)["service_rates"].(map[uint64]float64)) != 0 {
		t.Fatal("stale estimate exported")
	}
	send(4, 100, 4)
	if len(s.forecastService) != 0 {
		t.Fatal("gap treated as consecutive")
	}
}
