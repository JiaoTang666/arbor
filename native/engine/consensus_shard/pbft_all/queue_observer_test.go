package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestQueueObserverOneShotReadOnlyAndBoundedService(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "request.json")
	out := filepath.Join(dir, "snapshots")
	t.Setenv("SAA_DEFERRED_GROUP_REQUEST_PATH", path)
	t.Setenv("SAA_DEFERRED_GROUP_SNAPSHOT_DIR", out)
	n := &PbftConsensusNode{CurChain: &chain.BlockChain{Txpool: core.NewTxPool(), CurrentBlock: &core.Block{Header: &core.BlockHeader{Number: 9, StateRoot: make([]byte, 32)}, Hash: make([]byte, 32)}}}
	tx := core.NewTransaction("0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020", big.NewInt(1), 1, time.Unix(1, 0))
	tx.IngressSequence = 200123
	tx.RoutingFrozen = true
	n.CurChain.Txpool.AddTx2Pool(tx)
	for i := 0; i < 10; i++ {
		n.observeQueueService(uint64(i), 1000, time.Unix(int64(i*5), 0))
	}
	request := core.QueueObservationRequest{Schema: "saa_queue_request_v1", RequestID: "one", RequestedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute), AfterOccurrences: 200000, ObservedOccurrences: 200000}
	raw, _ := json.Marshal(request)
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	n.NodeID = 1
	if e := n.observeRequestedQueue(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(out); !os.IsNotExist(e) {
		t.Fatal("follower wrote leader snapshot")
	}
	n.NodeID = 0
	if e := n.observeRequestedQueue(); e != nil {
		t.Fatal(e)
	}
	file := filepath.Join(out, "snapshot_s0.json")
	before, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	var record struct {
		RequestID   string               `json:"request_id"`
		QueueLength int                  `json:"queue_length"`
		IngressMin  uint64               `json:"ingress_min"`
		Service     []queueServiceSample `json:"service_samples"`
	}
	if e = json.Unmarshal(before, &record); e != nil || record.RequestID != "one" || record.QueueLength != 1 || record.IngressMin != 200123 || len(record.Service) != 8 || record.Service[0].Height != 2 {
		t.Fatal("snapshot evidence mismatch", e)
	}
	n.CurChain.Txpool.AddTx2Pool(tx)
	if e = n.observeRequestedQueue(); e != nil {
		t.Fatal(e)
	}
	after, e := os.ReadFile(file)
	if e != nil || string(before) != string(after) || n.CurChain.Txpool.GetTxQueueLen() != 2 {
		t.Fatal("observer overwrote snapshot or changed queue")
	}
}
