package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/initialledger"
	"testing"
)

func TestOrdinaryPipelineDefaultAndLegacyReference(t *testing.T) {
	t.Setenv("SAA_LEGACY_SERIAL_ADMISSION", "")
	t.Setenv("SAA_ORDERED_LOCAL_BATCH_V1", "true")
	f := newFirstAdmissionRuntime(&RelayCommitteeModule{})
	if !f.ordinaryPipeline || f.localBatchEnabled {
		t.Fatal("new default not active")
	}
	t.Setenv("SAA_LEGACY_SERIAL_ADMISSION", "true")
	f = newFirstAdmissionRuntime(&RelayCommitteeModule{})
	if f.ordinaryPipeline || !f.localBatchEnabled {
		t.Fatal("reference mode broken")
	}
}
func TestOrdinaryPipelineRealWireWithoutTerminal(t *testing.T) {
	m, received, ctx := dispatchFixture(t)
	f := m.firstAdmission
	f.ordinaryPipeline = true
	f.localBatchEnabled = false
	f.ledger = firstplacement.NewPipelinedFence(f.session)
	a, b := queueAddress(1), queueAddress(2)
	m.initialLedger = &initialledger.Ledger{Routes: map[string]uint64{a: 0, b: 1}}
	first, second := queueTx(1, 2), queueTx(1, 2)
	second.IngressSequence = 1
	f.submitOrdered([]*core.Transaction{first, second})
	for i := 0; i < 2; i++ {
		select {
		case packet := <-received:
			if len(packet.Txs) != 1 {
				t.Fatal("wrong packet")
			}
		case <-ctx.Done():
			t.Fatal("ordinary follower waited for consensus", ctx.Err())
		}
	}
	// No block reports were supplied. Full lifecycle accounting must remain live.
	if f.ledger.Outstanding() != 2 {
		t.Fatal("write incorrectly counted as terminal")
	}
}
