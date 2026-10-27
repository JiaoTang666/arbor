package core

import (
	"encoding/hex"
	"time"
)

// QueueObservationRequest is a one-shot diagnostic boundary, not an input fence.
type QueueObservationRequest struct {
	Schema              string    `json:"schema"`
	RequestID           string    `json:"request_id"`
	RequestedAt         time.Time `json:"requested_at"`
	ExpiresAt           time.Time `json:"expires_at"`
	AfterOccurrences    int64     `json:"after_occurrences"`
	ObservedOccurrences int64     `json:"observed_occurrences"`
}

type QueueObservedTransaction struct {
	QueueIndex           int       `json:"queue_index"`
	IngressSequence      uint64    `json:"ingress_sequence"`
	TxHash               string    `json:"tx_hash"`
	Sender               string    `json:"sender"`
	Recipient            string    `json:"recipient"`
	Relayed              bool      `json:"relayed"`
	RoutingFrozen        bool      `json:"routing_frozen"`
	RoutedSenderShard    uint64    `json:"routed_sender_shard"`
	RoutedRecipientShard uint64    `json:"routed_recipient_shard"`
	MappingVersion       uint64    `json:"mapping_version"`
	HasBroker            bool      `json:"has_broker"`
	Value                string    `json:"value"`
	QueueTime            time.Time `json:"queue_time"`
	Held                 bool      `json:"held"`
}

// Copies scalar evidence under the existing queue lock. It never removes,
// reorders, holds, encodes mutable gob objects, or changes transaction times.
func (p *TxPool) SnapshotPendingQueueObservation() ([]QueueObservedTransaction, int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	rows := make([]QueueObservedTransaction, 0, p.PendingCountLocked())
	for index, tx := range p.pendingQueueLocked() {
		if tx == nil {
			continue
		}
		// Contact alone is not a hold: a valid new-route inbound debit may
		// execute here even though its recipient is frozen for migration.
		held := p.heldForMigration(tx)
		value := ""
		if tx.Value != nil {
			value = tx.Value.String()
		}
		rows = append(rows, QueueObservedTransaction{index, tx.IngressSequence, hex.EncodeToString(tx.TxHash), canonicalPrepareAccount(tx.Sender), canonicalPrepareAccount(tx.Recipient), tx.Relayed, tx.RoutingFrozen, tx.RoutedSenderShard, tx.RoutedRecipientShard, tx.MappingVersion, tx.HasBroker || tx.SenderIsBroker || tx.OriginalSender != "" || tx.FinalRecipient != "", value, tx.Time, held})
	}
	relays := 0
	for _, entries := range p.RelayPool {
		relays += len(entries)
	}
	return rows, relays
}
