package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// Every transaction-bearing attack is a mutation of the exact transaction
// subsequently injected through TCP. Its admission identity is never renewed.
type issuedNetworkAttack struct {
	Name string
	Kind message.MessageType
	Body []byte
}

type issuedAttackCompletion struct {
	Name       string              `json:"name"`
	Kind       message.MessageType `json:"kind"`
	BodySHA256 string              `json:"body_sha256"`
}

type issuedRejectedObservation struct {
	Observation                 issuedNodeObservation    `json:"observation"`
	Cases                       []issuedAttackCompletion `json:"cases"`
	TransportCallbacksCompleted int                      `json:"transport_callbacks_completed"`
}

// This list is finite and identical in the parent and corresponding children.
// Shard 0 exercises injection; shard 1 exercises the recipient Relay stage.
// The latter is only an invalid-input fixture, never a valid target delivery.
func issuedNetworkAttacks(t *testing.T, shard uint64, tx *core.Transaction) []issuedNetworkAttack {
	t.Helper()
	if shard > 1 || tx == nil {
		t.Fatal("issued attack list requires one of two shards and the valid transaction")
	}
	copyTx := func() *core.Transaction {
		copy := *tx
		copy.TxHash = append([]byte(nil), tx.TxHash...)
		copy.Relayed = shard == 1
		return &copy
	}
	kind := message.CInject
	if shard == 1 {
		kind = message.CRelay
	}
	envelope := func(txs []*core.Transaction, envelopeShard uint64) []byte {
		var value interface{}
		if shard == 0 {
			value = message.InjectTxs{Txs: txs, ToShardID: envelopeShard}
		} else {
			value = message.Relay{Txs: txs, SenderShardID: envelopeShard, SenderSeq: 99}
		}
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal("issued attack encoding failed", err)
		}
		return raw
	}
	wrongOwner, unboundRecipient, stageSpoof := copyTx(), copyTx(), copyTx()
	wrongOwner.RoutedRecipientShard = 0
	unboundRecipient.Recipient = issuedAccount(3)
	stageSpoof.Relayed = !stageSpoof.Relayed
	return []issuedNetworkAttack{
		{Name: "wrong_owner", Kind: kind, Body: envelope([]*core.Transaction{wrongOwner}, 0)},
		{Name: "unbound_recipient", Kind: kind, Body: envelope([]*core.Transaction{unboundRecipient}, 0)},
		{Name: "wrong_envelope_shard", Kind: kind, Body: envelope([]*core.Transaction{copyTx()}, 1)},
		{Name: "stage_spoof", Kind: kind, Body: envelope([]*core.Transaction{stageSpoof}, 0)},
		{Name: "mixed_valid_nil", Kind: kind, Body: envelope([]*core.Transaction{copyTx(), nil}, 0)},
		{Name: "malformed_json", Kind: kind, Body: []byte("{")},
		{Name: "null_json", Kind: kind, Body: []byte("null")},
		{Name: "null_relay_with_proof", Kind: message.CRelayWithProof, Body: []byte("null")},
	}
}

func issuedAttackFingerprint(kind message.MessageType, content []byte) (string, string) {
	// The actual TCP reader retains the line delimiter. Strip only that framing
	// byte; pass the original bytes unchanged to the production handler.
	body := bytes.TrimSuffix(content, []byte{'\n'})
	digest := sha256.Sum256(body)
	hash := hex.EncodeToString(digest[:])
	return string(kind) + "/" + hash, hash
}

// The immutable expected set is read before production handles each message.
// The mutex protects only completion accounting, never production execution.
type issuedAttackObserver struct {
	inner     OpInterShards
	t         *testing.T
	node      *PbftConsensusNode
	path      string
	expected  map[string]issuedAttackCompletion
	ordered   []issuedAttackCompletion
	mu        sync.Mutex
	completed map[string]bool
	published bool
}

var _ OpInterShards = (*issuedAttackObserver)(nil)

func installIssuedAttackObserver(t *testing.T, node *PbftConsensusNode, root string, id int, tx *core.Transaction) {
	t.Helper()
	observer := &issuedAttackObserver{
		inner: node.ohm, t: t, node: node,
		path:      filepath.Join(root, fmt.Sprintf("node_%d.rejected.json", id)),
		expected:  map[string]issuedAttackCompletion{},
		completed: map[string]bool{},
	}
	for _, attack := range issuedNetworkAttacks(t, node.ShardID, tx) {
		key, hash := issuedAttackFingerprint(attack.Kind, attack.Body)
		if _, exists := observer.expected[key]; exists {
			t.Fatal("issued attack cases must have distinct transport fingerprints")
		}
		item := issuedAttackCompletion{Name: attack.Name, Kind: attack.Kind, BodySHA256: hash}
		observer.expected[key] = item
		observer.ordered = append(observer.ordered, item)
	}
	node.ohm = observer
}

func (o *issuedAttackObserver) HandleMessageOutsidePBFT(kind message.MessageType, content []byte) bool {
	key, _ := issuedAttackFingerprint(kind, content)
	_, expected := o.expected[key]
	result := o.inner.HandleMessageOutsidePBFT(kind, content)
	if !expected {
		return result
	}

	o.mu.Lock()
	o.completed[key] = true
	if o.published || len(o.completed) != len(o.expected) {
		o.mu.Unlock()
		return result
	}
	o.published = true
	cases := append([]issuedAttackCompletion(nil), o.ordered...)
	completed := len(o.completed)
	o.mu.Unlock()

	// Reaching this point requires every distinct attack's production callback
	// to have returned. The parent waits for all eight atomic snapshots before
	// injecting legal work, so the negative proof has no timed quiet interval.
	observation := func() issuedNodeObservation {
		o.node.pbftLock.Lock()
		defer o.node.pbftLock.Unlock()
		return issuedObserve(o.t, o.node)
	}()
	issuedEvidence(o.t, o.path, issuedRejectedObservation{
		Observation: observation, Cases: cases, TransportCallbacksCompleted: completed,
	})
	return result
}
