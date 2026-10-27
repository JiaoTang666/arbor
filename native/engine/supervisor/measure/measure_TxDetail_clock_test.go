package measure

import (
	"blockEmulator/core"
	"testing"
)

func TestCaptureRoutingMetadata(t *testing.T) {
	tx := &core.Transaction{
		Sender:               "0xaaaa",
		Recipient:            "0xbbbb",
		IngressSequence:      11,
		RoutingEpoch:         4,
		MappingVersion:       3,
		RoutedSenderShard:    5,
		RoutedRecipientShard: 6,
	}
	detail := &txMetricDetailTime{}
	captureRoutingMetadata(detail, tx)
	if detail.IngressSequence != 11 || detail.RoutingEpoch != 4 || detail.MappingVersion != 3 {
		t.Fatalf("clock metadata mismatch: %+v", detail)
	}
	if detail.RoutedSenderShard != 5 || detail.RoutedRecipientShard != 6 {
		t.Fatalf("route metadata mismatch: %+v", detail)
	}
}
