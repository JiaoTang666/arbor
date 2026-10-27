package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestForecastStateRequirementSurvivesTransactionAndRelayEncoding(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	t.Setenv("SAA_FORECAST_STREAM_ID", "run")
	t.Setenv("SAA_FORECAST_STREAM_PATH", filepath.Join(t.TempDir(), "input.jsonl"))
	account := authorityAccount('a')
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2, stableShardByAccount: map[string]uint64{account: 0}}
	defer saaIntegratedRuntimeStates.Delete(m)
	s := m.integratedStateLocked()
	s.authority.submit(mappingAuthorityProposal{ProposalID: "move", Addresses: []string{account}, SourceShard: 0, TargetShard: 1, EffectiveEpoch: 2}, 1)
	s.authority.applyDue(2)
	tx := &core.Transaction{Sender: strings.TrimPrefix(account, "0x"), Recipient: strings.TrimPrefix(account, "0x"), RoutingFrozen: true, MappingVersion: s.authority.routeVersion, RoutedSenderShard: 1, RoutedRecipientShard: 1}
	if err := m.recordForecastRoutedInput(tx); err != nil {
		t.Fatal(err)
	}
	assert := func(decoded *core.Transaction) {
		t.Helper()
		if !decoded.SenderRequiresExistingState || !decoded.RecipientRequiresExistingState || decoded.MappingVersion != 1 {
			t.Fatalf("state requirement lost: %+v", decoded)
		}
	}
	assert(core.DecodeTx(tx.Encode()))
	tx.Relayed = true
	payload, err := json.Marshal(message.Relay{Txs: []*core.Transaction{tx}})
	if err != nil {
		t.Fatal(err)
	}
	var relay message.Relay
	if err = json.Unmarshal(payload, &relay); err != nil {
		t.Fatal(err)
	}
	assert(relay.Txs[0])
	proofPayload, err := json.Marshal(message.RelayWithProof{Txs: []*core.Transaction{tx}})
	if err != nil {
		t.Fatal(err)
	}
	var proof message.RelayWithProof
	if err = json.Unmarshal(proofPayload, &proof); err != nil {
		t.Fatal(err)
	}
	assert(proof.Txs[0])
}
