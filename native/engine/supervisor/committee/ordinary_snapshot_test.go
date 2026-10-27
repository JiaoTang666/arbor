package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOrdinarySnapshotAllCheckedFieldsAreIndependent(t *testing.T) {
	cases := map[string]func(*core.Transaction){
		"sender":    func(x *core.Transaction) { x.Sender = "changed" },
		"recipient": func(x *core.Transaction) { x.Recipient = "changed" },
		"version":   func(x *core.Transaction) { x.MappingVersion++ },
		"epoch":     func(x *core.Transaction) { x.RoutingEpoch++ },
		"source":    func(x *core.Transaction) { x.RoutedSenderShard++ },
		"target":    func(x *core.Transaction) { x.RoutedRecipientShard++ },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := &RelayCommitteeModule{offeredAuditPath: filepath.Join(t.TempDir(), "audit")}
			tx := &core.Transaction{Sender: "a", Recipient: "b", TxHash: []byte{1}, RoutingFrozen: true, MappingVersion: 2, RoutingEpoch: 3}
			m.auditAccepted(tx)
			m.auditRoute(tx)
			original := *tx
			mutate(tx)
			report := func(x *core.Transaction) {
				m.auditBlock(&message.BlockInfoMsg{Epoch: 1, CommitTime: time.Now(), InnerShardTxs: []*core.Transaction{x}})
			}
			report(tx)
			if m.lifecycle[occurrenceKey(tx)].Done {
				t.Fatal("mutated route accepted")
			}
			report(&original)
			if !m.lifecycle[occurrenceKey(tx)].Done {
				t.Fatal("original route rejected")
			}
		})
	}
}

func TestOrdinaryBufferedAuditCompleteEventContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	m := &RelayCommitteeModule{offeredAuditPath: path, nativeAudit: &batchAudit{f: f, w: bufio.NewWriterSize(f, 4096)}}
	tx := &core.Transaction{Sender: "a", Recipient: "b", TxHash: []byte{1}, RoutingFrozen: true, RoutedRecipientShard: 1, MappingVersion: 2, RoutingEpoch: 3, IngressSequence: 7}
	m.auditAccepted(tx)
	m.auditRoute(tx)
	m.auditTransport([]*core.Transaction{tx}, "first_socket_write", time.Now(), nil)
	m.auditTransport([]*core.Transaction{tx}, "first_socket_write", time.Now(), nil)
	now := time.Now()
	target := &message.BlockInfoMsg{SenderShardID: 1, Epoch: 1, CommitTime: now, Relay2Txs: []*core.Transaction{tx}}
	source := &message.BlockInfoMsg{SenderShardID: 0, Epoch: 1, CommitTime: now, Relay1Txs: []*core.Transaction{tx}}
	m.auditBlock(target)
	m.auditBlock(source)
	m.auditBlock(source)
	m.auditBlock(target)
	m.nativeAudit.flush()
	raw, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	dec := json.NewDecoder(raw)
	want := []string{"accepted", "route_bound", "first_socket_write", "leg_registered", "leg_registered", "completed"}
	for _, ev := range want {
		var x map[string]interface{}
		if e := dec.Decode(&x); e != nil {
			t.Fatal(e)
		}
		if x["event"] != ev || x["occurrence"] != "7/01" || x["tx_hash"] != "01" || x["schema"] != "occurrence_lifecycle_v1" || x["ingress_sequence"] != float64(7) {
			t.Fatalf("invalid event: %v", x)
		}
		if ev == "route_bound" {
			d := x["details"].(map[string]interface{})
			if d["version"] != float64(2) || d["epoch"] != float64(3) || d["target"] != float64(1) || d["source"] != float64(0) {
				t.Fatal(d)
			}
		}
	}
	if dec.More() {
		t.Fatal("duplicate event")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate ingress accepted")
		}
	}()
	m.auditAccepted(tx)
}
