package committee

import (
	"blockEmulator/core"
	"encoding/json"
	"os"
	"testing"
)

// Optional artifact bridge from TestSourcePrepareIdleIngressAndCommitBoundary.
// No production API is exported just to bypass package-private handlers.
func TestSEFConsumesActualPBFTHandlerPreparedArtifact(t *testing.T) {
	path := os.Getenv("SAA_TEST_PREPARED_BRIDGE_OUTPUT")
	if path == "" {
		t.Skip("requires PBFT handler artifact from paired integration run")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var p core.SourcePreparation
	if err = json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	const account = "0x0000000000000000000000000000000000000010"
	// Independent expectations: do not build the proposal from received identity.
	if p.Command.ProposalID != "p" || "0x"+p.Command.Account != account || p.PrepareSequence != 2 || p.Command.SourceShard != 0 || p.Command.TargetShard != 1 || p.Command.MappingVersion != 0 {
		t.Fatal("producer identity differs from expected contract")
	}
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, softDecisionMode: saaIntegratedDecisionMode, stableShardByAccount: map[string]uint64{account: 0}}
	s := m.integratedStateLocked()
	a := s.authority
	a.enableInstallationReceiptGate()
	if d := a.submit(mappingAuthorityProposal{ProposalID: "p", Addresses: []string{account}, TargetShard: 1, EffectiveEpoch: 2}, 1); d.Status != mappingAccepted {
		t.Fatal(d)
	}
	s.nativeSourcePreparedPath = path
	for _, epoch := range []int{2, 2, 3} {
		m.lock.Lock()
		m.applySAAIntegratedDueLocked(epoch)
		m.lock.Unlock()
		e, ok := a.preparedTransfers["p"][account]
		if !ok || e.StateHash != p.StateHash || e.SourceStateRoot != p.SourceStateRoot || e.AnchorVersion != p.Command.AnchorVersion {
			t.Fatal("PBFT output not bound in SEF")
		}
		if a.routeVersion != 0 || a.decisionByID["p"].Status == mappingApplied || m.stableShardByAccount[account] != 0 || a.canFinalizeMigration("p", account, p.StateHash) {
			t.Fatal("source-only PBFT artifact activated or finalized")
		}
	}
	t.Logf("PBFT handler artifact consumed: proposal=p sequence=%d digest=%s state=%s route_version=0", p.PrepareSequence, p.CommitDigest, p.StateHash)
}
