package committee

import (
	"blockEmulator/message"
	"strings"
	"testing"
	"time"
)

func TestForecastPayloadUnrelatedCommitPreservesOriginalEvidence(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2}
	defer saaIntegratedRuntimeStates.Delete(m)
	a, other := authorityAccount('a'), authorityAccount('b')
	originalRoot := strings.Repeat("1", 64)
	m.observeForecastCommit(&message.BlockInfoMsg{SenderShardID: 0, Epoch: 1, CommitTime: time.Unix(100, 0), AccountStateRoot: originalRoot, AccountStateSizeStatus: "committed_touched_account_trie_payload_only", AccountStateBytes: map[string]uint64{a: 200}})
	for epoch := 2; epoch <= 3; epoch++ {
		payload := map[string]uint64{}
		if epoch == 3 {
			payload[other] = 300
		}
		m.observeForecastCommit(&message.BlockInfoMsg{SenderShardID: 0, Epoch: epoch, CommitTime: time.Unix(int64(100+epoch), 0), AccountStateRoot: strings.Repeat("2", 64), AccountStateSizeStatus: "committed_touched_account_trie_payload_only", AccountStateBytes: payload})
		cached := m.integratedStateLocked().forecastStatePayloads[0][a]
		if cached.Bytes != 200 || cached.Epoch != 1 || cached.StateRoot != originalRoot {
			t.Fatal("unrelated commit erased or refreshed account evidence", cached)
		}
	}
	if len(m.forecastPayloadEvidenceLocked(map[string]uint64{a: 0}, 4)) != 0 {
		t.Fatal("retention bypassed freshness gate")
	}
}

func TestForecastPayloadEvidenceRequiresSourceAndFreshCommit(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2}
	defer saaIntegratedRuntimeStates.Delete(m)
	account := authorityAccount('a')
	b := &message.BlockInfoMsg{SenderShardID: 0, Epoch: 1, CommitTime: time.Unix(100, 0), AccountStateRoot: strings.Repeat("1", 64), AccountStateSizeStatus: "committed_touched_account_trie_payload_only", AccountStateBytes: map[string]uint64{account: 200}}
	m.observeForecastCommit(b)
	if len(m.forecastPayloadEvidenceLocked(map[string]uint64{account: 1}, 2)) != 0 {
		t.Fatal("wrong source accepted")
	}
	if m.forecastPayloadEvidenceLocked(map[string]uint64{account: 0}, 2)[account].Bytes != 200 {
		t.Fatal("native payload lost")
	}
	if len(m.forecastPayloadEvidenceLocked(map[string]uint64{account: 0}, 4)) != 0 {
		t.Fatal("stale payload accepted")
	}
	b.Epoch = 2
	b.CommitTime = time.Unix(102, 0)
	b.AccountStateSizeStatus = "state_read_failed"
	m.observeForecastCommit(b)
	if len(m.forecastPayloadEvidenceLocked(map[string]uint64{account: 0}, 3)) != 0 {
		t.Fatal("failed read retained old evidence")
	}
}
