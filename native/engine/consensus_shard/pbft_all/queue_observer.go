package pbft_all

import (
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type queueServiceSample struct {
	At                    time.Time `json:"committed_at"`
	Height                uint64    `json:"block_height"`
	BlockTransactions     int       `json:"tx_count"`
	PostcommitQueueLength int       `json:"queue_after"`
}
type queueObserverState struct {
	mu      sync.Mutex
	seen    string
	service []queueServiceSample
}

func (n *PbftConsensusNode) observeQueueService(height uint64, count int, at time.Time) {
	if os.Getenv("SAA_DEFERRED_GROUP_REQUEST_PATH") == "" {
		return
	}
	n.queueObserver.mu.Lock()
	defer n.queueObserver.mu.Unlock()
	n.queueObserver.service = append(n.queueObserver.service, queueServiceSample{at, height, count, n.CurChain.Txpool.GetTxQueueLen()})
	if len(n.queueObserver.service) > 8 {
		n.queueObserver.service = n.queueObserver.service[len(n.queueObserver.service)-8:]
	}
}

// Runs in the existing serialized proposal path. Leaders sample independently:
// this is NOT a globally atomic state/queue cut and NOT a queue certificate.
func (n *PbftConsensusNode) observeRequestedQueue() error {
	path, dir := os.Getenv("SAA_DEFERRED_GROUP_REQUEST_PATH"), os.Getenv("SAA_DEFERRED_GROUP_SNAPSHOT_DIR")
	if path == "" || dir == "" || n.NodeID != uint64(n.view.Load()) {
		return nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var request core.QueueObservationRequest
	if err = json.Unmarshal(raw, &request); err != nil {
		return err
	}
	if request.Schema != "saa_queue_request_v1" || request.RequestID == "" || request.RequestedAt.IsZero() || !request.ExpiresAt.After(request.RequestedAt) {
		return fmt.Errorf("invalid queue request")
	}
	n.queueObserver.mu.Lock()
	defer n.queueObserver.mu.Unlock()
	if n.queueObserver.seen == request.RequestID {
		return nil
	}
	n.queueObserver.seen = request.RequestID // one attempt, even on storage failure
	if time.Now().After(request.ExpiresAt) {
		return nil
	}
	output := filepath.Join(dir, fmt.Sprintf("snapshot_s%d.json", n.ShardID))
	if _, err := os.Stat(output); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	started := time.Now()
	rows, relayCount := n.CurChain.Txpool.SnapshotPendingQueueObservation()
	var ingressMin, ingressMax *uint64
	for _, row := range rows {
		if ingressMin == nil {
			lo, hi := row.IngressSequence, row.IngressSequence
			ingressMin = &lo
			ingressMax = &hi
		}
		if row.IngressSequence < *ingressMin {
			*ingressMin = row.IngressSequence
		}
		if row.IngressSequence > *ingressMax {
			*ingressMax = row.IngressSequence
		}
	}
	block := n.CurChain.CurrentBlock
	record := map[string]interface{}{
		"schema": "saa_queue_snapshot_v1", "request_id": request.RequestID,
		"shard_id": n.ShardID, "node_id": n.NodeID, "observed_at": started.UTC(),
		"copy_finished_at": time.Now().UTC(), "committed_height": block.Header.Number,
		"committed_root": hex.EncodeToString(block.Header.StateRoot), "committed_hash": hex.EncodeToString(block.Hash),
		"queue_length": len(rows), "relay_pool_length": relayCount, "transactions": rows,
		"ingress_min": ingressMin, "ingress_max": ingressMax,
		"service_samples": append([]queueServiceSample(nil), n.queueObserver.service...),
		"semantics":       "leader-local pending queue before proposal; committed head bound; shards sampled at different times; service samples are observed throughput not saturation capacity; no input pause",
	}
	raw, err = json.Marshal(record)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	return core.PublishSourcePrepareFile(output, append(raw, '\n'))
}
