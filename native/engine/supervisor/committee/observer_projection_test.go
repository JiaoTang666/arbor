package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func observerAuditFixture(t *testing.T) (*RelayCommitteeModule, string, string) {
	t.Helper()
	path, input := filepath.Join(t.TempDir(), "audit.jsonl"), strings.Repeat("a", 64)
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	p := newObserverProjection(path, input)
	if p.failed {
		t.Fatal("projection creation failed")
	}
	t.Cleanup(func() { f.Close(); p.f.Close() })
	m := &RelayCommitteeModule{offeredAuditPath: path, nativeAudit: &batchAudit{f: f, w: bufio.NewWriterSize(f, 4096), observer: p}}
	return m, path, input
}

func TestObserverProjectionMatchesFullNativeFieldsAndDuplicatePayloads(t *testing.T) {
	m, path, input := observerAuditFixture(t)
	for seq := uint64(0); seq < 2; seq++ {
		// Same payload hash does not collapse two admitted occurrences.
		tx := &core.Transaction{Sender: "a", Recipient: "b", TxHash: bytes.Repeat([]byte{1}, 32),
			IngressSequence: seq, AdmissionID: fmt.Sprintf("native-input/%s/%d", input, seq),
			RoutingFrozen: true, RoutedRecipientShard: 1, MappingVersion: seq, RoutingEpoch: 3}
		m.auditAccepted(tx)
		m.auditRoute(tx)
		now := time.Now()
		m.auditBlock(&message.BlockInfoMsg{SenderShardID: 1, Epoch: 1, CommitTime: now, Relay2Txs: []*core.Transaction{tx}})
		m.auditBlock(&message.BlockInfoMsg{SenderShardID: 0, Epoch: 1, CommitTime: now, Relay1Txs: []*core.Transaction{tx}})
	}
	m.nativeAudit.flush()
	full, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	accepted := map[uint64]map[string]interface{}{}
	routed := map[uint64]map[string]interface{}{}
	completed := map[uint64]map[string]interface{}{}
	decoder := json.NewDecoder(bytes.NewReader(full))
	for {
		var row map[string]interface{}
		if e = decoder.Decode(&row); e == io.EOF {
			break
		} else if e != nil {
			t.Fatal(e)
		}
		seq := uint64(row["ingress_sequence"].(float64))
		switch row["event"] {
		case "accepted":
			accepted[seq] = row
		case "route_bound":
			routed[seq] = row
		case "completed":
			completed[seq] = row
		}
	}
	raw, e := os.ReadFile(path + ".observer.v1.jsonl")
	if e != nil {
		t.Fatal(e)
	}
	decoder = json.NewDecoder(bytes.NewReader(raw))
	var header map[string]interface{}
	if e = decoder.Decode(&header); e != nil {
		t.Fatal(e)
	}
	if header["schema"] != "occurrence_observer_v1" || header["input_sha256"] != input || header["lifecycle_file"] != filepath.Base(path) {
		t.Fatal(header)
	}
	for seq := uint64(0); seq < 2; seq++ {
		var route, done []interface{}
		if e = decoder.Decode(&route); e != nil {
			t.Fatal(e)
		}
		if e = decoder.Decode(&done); e != nil {
			t.Fatal(e)
		}
		d := routed[seq]["details"].(map[string]interface{})
		if len(route) != 8 || route[0] != "r" || route[1] != float64(seq) || route[2] != accepted[seq]["tx_hash"] || route[3] != accepted[seq]["at"] || route[4] != d["source"] || route[5] != d["target"] || route[6] != d["version"] || route[7] != d["epoch"] {
			t.Fatal(route, accepted[seq], d)
		}
		if len(done) != 4 || done[0] != "c" || done[1] != float64(seq) || done[2] != completed[seq]["tx_hash"] || done[3] != d["version"] {
			t.Fatal(done)
		}
	}
	var extra interface{}
	if e = decoder.Decode(&extra); e != io.EOF {
		t.Fatal("extra projected event", extra, e)
	}
}

func TestObserverProjectionFailureIsExplicitAndFullAuditContinues(t *testing.T) {
	m, path, input := observerAuditFixture(t)
	if e := m.nativeAudit.observer.f.Close(); e != nil {
		t.Fatal(e)
	}
	m.nativeAudit.flush()
	if !m.nativeAudit.observer.failed {
		t.Fatal("projection flush failure ignored")
	}
	if _, e := os.Stat(path + ".observer.v1.jsonl.failed.json"); e != nil {
		t.Fatal(e)
	}
	tx := &core.Transaction{TxHash: bytes.Repeat([]byte{1}, 32), AdmissionID: "native-input/" + input + "/0"}
	m.auditAccepted(tx)
	m.auditRoute(tx)
	m.nativeAudit.flush()
	raw, e := os.ReadFile(path)
	if e != nil || !bytes.Contains(raw, []byte("route_bound")) {
		t.Fatal("full audit stopped", e)
	}
}

func TestObserverProjectionIdentityMismatchDoesNotRewriteFullAudit(t *testing.T) {
	m, path, _ := observerAuditFixture(t)
	tx := &core.Transaction{TxHash: bytes.Repeat([]byte{1}, 32), AdmissionID: "wrong"}
	m.auditAccepted(tx)
	m.auditRoute(tx)
	m.nativeAudit.flush()
	if !m.nativeAudit.observer.failed {
		t.Fatal("foreign admission identity projected")
	}
	raw, e := os.ReadFile(path)
	if e != nil || !bytes.Contains(raw, []byte(`"admission_id":"wrong"`)) {
		t.Fatal("full facts changed", e)
	}
}

func TestObserverProjectionOrdinaryPathNeedsNoEffectIndex(t *testing.T) {
	m, path, _ := observerAuditFixture(t)
	tx := &core.Transaction{TxHash: bytes.Repeat([]byte{2}, 32), IngressSequence: 8, RoutingFrozen: true}
	m.auditAccepted(tx)
	m.auditRoute(tx)
	m.nativeAudit.flush()
	if m.nativeAudit.observer.failed { t.Fatal("ordinary occurrence was rejected") }
	raw, err := os.ReadFile(path + ".observer.v1.jsonl")
	if err != nil || !bytes.Contains(raw, []byte(`["r",8,`)) { t.Fatal(string(raw), err) }
}
