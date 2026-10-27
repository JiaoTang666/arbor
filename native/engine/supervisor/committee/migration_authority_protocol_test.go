package committee

import (
	"strings"
	"testing"
)

func TestNativeStateTransferRequiresAuthorityAndInstallReceipt(t *testing.T) {
	d := mappingAuthorityDecision{ProposalID: "p", Status: mappingApplied}
	e := NativeStateTransferEnvelope{ProposalID: "p", SourceShard: 0, TargetShard: 1, SourceStateRoot: strings.Repeat("1", 64), StateHash: strings.Repeat("2", 64)}
	if err := e.Validate(d, false); err == nil {
		t.Fatal("unacknowledged install accepted")
	}
	if err := e.Validate(d, true); err != nil {
		t.Fatal(err)
	}
	d.Status = mappingAccepted
	if err := e.Validate(d, true); err != nil {
		t.Fatalf("installation must precede activation: %v", err)
	}
	d.Status = mappingRejected
	if err := e.Validate(d, true); err == nil {
		t.Fatal("rejected proposal accepted")
	}
	d.Status = mappingApplied
	e.ProposalID = "other"
	if err := e.Validate(d, true); err == nil {
		t.Fatal("proposal mismatch accepted")
	}
	if len(StateTransferReceiptHash("p", "a", strings.Repeat("2", 64))) != 64 {
		t.Fatal("receipt hash malformed")
	}
}
