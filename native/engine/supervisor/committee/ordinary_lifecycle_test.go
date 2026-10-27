package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOrdinaryLifecycleLegContract(t *testing.T) {
	m := &RelayCommitteeModule{offeredAuditPath: filepath.Join(t.TempDir(), "load")}
	tx := &core.Transaction{Sender: "a", Recipient: "b", IngressSequence: 4, TxHash: []byte{1}, RoutingFrozen: true, RoutedSenderShard: 0, RoutedRecipientShard: 1, MappingVersion: 2, RoutingEpoch: 3}
	m.auditAccepted(tx)
	m.auditRoute(tx)
	report := func(shard uint64, leg string, v *core.Transaction) {
		b := &message.BlockInfoMsg{SenderShardID: shard, Epoch: 1, CommitTime: time.Now()}
		if leg == "relay1" {
			b.Relay1Txs = []*core.Transaction{v}
		} else {
			b.Relay2Txs = []*core.Transaction{v}
		}
		m.auditBlock(b)
	}
	bad := *tx
	bad.MappingVersion = 1
	report(1, "relay2", &bad)
	report(0, "relay2", tx)
	if m.lifecycle[occurrenceKey(tx)].Target {
		t.Fatal("invalid report advanced lifecycle")
	}
	report(1, "relay2", tx) // target notification may arrive first
	if m.lifecycle[occurrenceKey(tx)].Done {
		t.Fatal("one leg completed cross-shard tx")
	}
	report(0, "relay1", tx)
	report(0, "relay1", tx)
	report(1, "relay2", tx)
	if !m.lifecycle[occurrenceKey(tx)].Done {
		t.Fatal("not complete")
	}
	raw, e := os.ReadFile(m.offeredAuditPath + ".lifecycle.v1.jsonl")
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Count(raw, []byte(`"event":"completed"`)) != 1 {
		t.Fatal(string(raw))
	}
}

func TestOrdinaryLifecycleOccurrencesAndLocal(t *testing.T) {
	m := &RelayCommitteeModule{offeredAuditPath: filepath.Join(t.TempDir(), "load")}
	a := &core.Transaction{Sender: "a", Recipient: "b", TxHash: []byte{1}, RoutingFrozen: true}
	b := *a
	b.IngressSequence = 1
	m.auditAccepted(a)
	m.auditRoute(a)
	m.auditAccepted(&b)
	m.auditRoute(&b)
	m.auditBlock(&message.BlockInfoMsg{Epoch: 1, CommitTime: time.Now(), InnerShardTxs: []*core.Transaction{a}})
	if !m.lifecycle[occurrenceKey(a)].Done || m.lifecycle[occurrenceKey(&b)].Done {
		t.Fatal("occurrences merged")
	}
	m.auditTransport([]*core.Transaction{a}, "first_socket_write", time.Now(), nil)
	m.auditTransport([]*core.Transaction{a}, "first_socket_write", time.Now(), nil)
	raw, _ := os.ReadFile(m.offeredAuditPath + ".lifecycle.v1.jsonl")
	if bytes.Count(raw, []byte(`"event":"first_socket_write"`)) != 1 {
		t.Fatal("duplicate first write")
	}
}
