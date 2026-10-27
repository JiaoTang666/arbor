package ownership

import (
	"encoding/json"
	"fmt"
)

const PlacementWitnessSchema = "coolchain/placement-witness-v1"
const MaxPlacementTransactions = 4096

// PlacementWitness binds the collector's ordered input to an independently
// checked placement contract. The collector remains trusted: this is not proof
// of input provenance, optimizer optimality, or authenticated graph contents.
type PlacementWitness struct {
	Schema       string                 `json:"schema"`
	InputDigest  string                 `json:"input_digest"`
	Transactions []PlacementTransaction `json:"transactions"`
}

type PlacementTransaction struct {
	OccurrenceID   string `json:"occurrence_id"`
	Sequence       uint64 `json:"sequence"`
	Sender         string `json:"sender"`
	Recipient      string `json:"recipient"`
	SenderShard    uint64 `json:"sender_shard"`
	RecipientShard uint64 `json:"recipient_shard"`
}

type placementInput struct {
	OccurrenceID string `json:"occurrence_id"`
	Sequence     uint64 `json:"sequence"`
	Sender       string `json:"sender"`
	Recipient    string `json:"recipient"`
}

// PlacementInputDigest excludes chosen routes; those are separately bound by
// the signed Batch. Field order and the domain tag are shared with Python.
func PlacementInputDigest(transactions []PlacementTransaction) string {
	input := make([]placementInput, len(transactions))
	for i, transaction := range transactions {
		input[i] = placementInput{transaction.OccurrenceID, transaction.Sequence, transaction.Sender, transaction.Recipient}
	}
	raw, _ := json.Marshal(input)
	return digestBytes(append([]byte("coolchain:placement-input:v1\x00"), raw...))
}

// PlacementWitnessDigest commits to the ordered input AND chosen routes.
// The exact struct JSON order is shared with Python; no map canonicalization
// or route omission is permitted here.
func PlacementWitnessDigest(witness *PlacementWitness) string {
	raw, _ := json.Marshal(witness)
	return digestBytes(raw)
}

func (w *PlacementWitness) checkShape(batch Batch) error {
	if w.Schema != PlacementWitnessSchema || !validDigest(w.InputDigest) ||
		len(w.Transactions) == 0 || len(w.Transactions) > MaxPlacementTransactions ||
		batch.EvidenceVersion == 0 || !validDigest(batch.EvidenceDigest) ||
		batch.AdmissionStart >= batch.AdmissionEnd || batch.EvidenceCutoff > batch.AdmissionStart ||
		batch.AdmissionEnd-batch.AdmissionStart != uint64(len(w.Transactions)) {
		return fmt.Errorf("placement witness context/size mismatch")
	}
	occurrences := make(map[string]bool, len(w.Transactions))
	for i, transaction := range w.Transactions {
		if transaction.OccurrenceID == "" || len(transaction.OccurrenceID) > 128 || occurrences[transaction.OccurrenceID] ||
			transaction.Sequence != batch.AdmissionStart+uint64(i) ||
			!canonicalAccount(transaction.Sender) || !canonicalAccount(transaction.Recipient) ||
			transaction.SenderShard >= batch.Context.Shards || transaction.RecipientShard >= batch.Context.Shards {
			return fmt.Errorf("placement witness input/order mismatch")
		}
		occurrences[transaction.OccurrenceID] = true
	}
	if PlacementInputDigest(w.Transactions) != w.InputDigest {
		return fmt.Errorf("placement witness input digest mismatch")
	}
	return nil
}

// ValidatePlacement checks OO immutability, NO following and NN colocation in
// original input order. New owners from earlier rows become old for later rows.
// Work and allocation are O(T+B), without copying the entire owner registry.
func ValidatePlacement(batch Batch, owner func(string) (uint64, bool)) error {
	w := batch.PlacementWitness
	if w == nil || owner == nil {
		return fmt.Errorf("placement witness required")
	}
	if err := w.checkShape(batch); err != nil {
		return err
	}
	if batch.Context.AuthorizationSchema == CompactAuthorizationSchema && PlacementWitnessDigest(w) != batch.PlacementWitnessDigest {
		return fmt.Errorf("placement witness commitment mismatch")
	}
	return validatePlacementRelations(batch, owner)
}

// Called only after checkedBatch/Verify established shape and commitment.
func validatePlacementRelations(batch Batch, owner func(string) (uint64, bool)) error {
	w := batch.PlacementWitness
	if w == nil || owner == nil {
		return fmt.Errorf("placement witness required")
	}
	created := make(map[string]uint64, len(batch.Bindings))
	lookup := func(account string) (uint64, bool) {
		if shard, exists := created[account]; exists {
			return shard, true
		}
		return owner(account)
	}
	for _, transaction := range w.Transactions {
		sender, senderOld := lookup(transaction.Sender)
		recipient, recipientOld := lookup(transaction.Recipient)
		if senderOld && sender != transaction.SenderShard || recipientOld && recipient != transaction.RecipientShard {
			return fmt.Errorf("placement witness rewrites an existing owner")
		}
		if !senderOld || !recipientOld {
			if transaction.SenderShard != transaction.RecipientShard {
				return fmt.Errorf("placement witness violates NO follow or NN colocation")
			}
		}
		if !senderOld {
			created[transaction.Sender] = transaction.SenderShard
		}
		if !recipientOld {
			created[transaction.Recipient] = transaction.RecipientShard
		}
	}
	if len(created) != len(batch.Bindings) {
		return fmt.Errorf("placement witness binding coverage mismatch")
	}
	for _, binding := range batch.Bindings {
		if shard, exists := created[binding.Account]; !exists || shard != binding.Shard {
			return fmt.Errorf("placement witness disagrees with signed bindings")
		}
	}
	return nil
}
