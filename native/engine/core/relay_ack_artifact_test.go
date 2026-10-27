package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRelayAckNativeFailureArtifact(t *testing.T) {
	d := os.Getenv("SAA_RELAY_FAILURE_ARTIFACT")
	if d == "" {
		t.Skip("set artifact directory for frozen native replay")
	}
	files, e := filepath.Glob(filepath.Join(d, "SAA_NATIVE_HANDOFF_RELAY_DIR", "*.relay.json"))
	if e != nil || len(files) != 1 {
		t.Fatalf("expected one frozen relay: %v %v", files, e)
	}
	raw, e := os.ReadFile(files[0])
	if e != nil {
		t.Fatal(e)
	}
	var first struct {
		Handoff MigrationHandoff
		Relay   []byte
	}
	if e = json.Unmarshal(raw, &first); e != nil {
		t.Fatal(e)
	}
	raw, e = os.ReadFile(filepath.Join(d, "SAA_NATIVE_HANDOFF_ACK_DIR", first.Handoff.ID+".relay-ack.json"))
	if e != nil {
		t.Fatal(e)
	}
	var original HandoffRelayAck
	if e = json.Unmarshal(raw, &original); e != nil {
		t.Fatal(e)
	}
	expected := DecodeTx(first.Handoff.Execution)
	expected.Relayed = true
	local := sha256.Sum256(expected.Encode())
	wire := sha256.Sum256(first.Relay)
	if original.RelayHash != hex.EncodeToString(wire[:]) {
		t.Fatal("original ACK does not bind wire bytes")
	}
	if local == wire {
		t.Fatal("fixture no longer reproduces gob re-encoding mismatch")
	}
	if ValidateHandoffRelayAck(&first.Handoff, original) == nil {
		t.Fatal("old payload-free ACK must fail closed")
	}
	fixed := original
	fixed.RelayPayload = first.Relay
	if e := ValidateHandoffRelayAck(&first.Handoff, fixed); e != nil {
		t.Fatal(e)
	}
	for name, mutate := range map[string]func(*HandoffRelayAck){
		"id": func(a *HandoffRelayAck) { a.ID = "wrong" }, "hash": func(a *HandoffRelayAck) { a.RelayHash = "wrong" }, "shard": func(a *HandoffRelayAck) { a.RecipientShard++ }, "uncommitted": func(a *HandoffRelayAck) { a.Committed = false }, "block": func(a *HandoffRelayAck) { a.Block = 0 }, "root": func(a *HandoffRelayAck) { a.StateRoot = "bad" }, "blockhash": func(a *HandoffRelayAck) { a.BlockHash = "bad" }, "malformed": func(a *HandoffRelayAck) { a.RelayPayload = []byte("bad") },
	} {
		t.Run(name, func(t *testing.T) {
			a := fixed
			mutate(&a)
			if ValidateHandoffRelayAck(&first.Handoff, a) == nil {
				t.Fatal("invalid ACK accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*Transaction){"value": func(tx *Transaction) { tx.Value.SetInt64(1) }, "recipient": func(tx *Transaction) { tx.Recipient = "wrong" }, "occurrence": func(tx *Transaction) { tx.IngressSequence++ }, "version": func(tx *Transaction) { tx.MappingVersion++ }, "relayflag": func(tx *Transaction) { tx.Relayed = false }, "route": func(tx *Transaction) { tx.RoutedRecipientShard++ }} {
		t.Run(name, func(t *testing.T) {
			tx := DecodeTx(first.Relay)
			mutate(tx)
			a := fixed
			a.RelayPayload = tx.Encode()
			h := sha256.Sum256(a.RelayPayload)
			a.RelayHash = hex.EncodeToString(h[:])
			if ValidateHandoffRelayAck(&first.Handoff, a) == nil {
				t.Fatal("changed transaction accepted despite matching byte hash")
			}
		})
	}
}
