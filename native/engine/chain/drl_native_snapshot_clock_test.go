package chain

import (
	"blockEmulator/core"
	"testing"
)

func TestCompletedInteractionRecordsUseFinalStages(t *testing.T) {
	direct := &core.Transaction{
		Sender:               "0xaaaa",
		Recipient:            "0xbbbb",
		TxHash:               []byte{0x01},
		IngressSequence:      7,
		RoutingEpoch:         3,
		MappingVersion:       2,
		RoutedSenderShard:    1,
		RoutedRecipientShard: 1,
	}
	relayFinal := &core.Transaction{
		Sender:               "0xcccc",
		Recipient:            "0xdddd",
		TxHash:               []byte{0x02},
		IngressSequence:      8,
		RoutingEpoch:         3,
		MappingVersion:       2,
		RoutedSenderShard:    1,
		RoutedRecipientShard: 2,
	}
	records := completedInteractionRecords([]*core.Transaction{direct}, []*core.Transaction{relayFinal})
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if records[0].CompletionStage != "direct_final" || records[1].CompletionStage != "relay_final" {
		t.Fatalf("unexpected stages: %q %q", records[0].CompletionStage, records[1].CompletionStage)
	}
	if records[1].IngressSequence != 8 || records[1].MappingVersion != 2 {
		t.Fatalf("route metadata was not preserved: %+v", records[1])
	}
}
