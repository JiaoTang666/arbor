// Definition of transaction

package core

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"time"
)

type Transaction struct {
	FirstPlacement *FirstPlacementBinding `json:"first_placement,omitempty"`
	Sender         utils.Address
	Recipient      utils.Address
	Nonce          uint64
	Signature      []byte // not implemented now.
	Value          *big.Int
	TxHash         []byte

	Time time.Time // TimeStamp the tx proposed.

	// used in transaction relaying
	Relayed bool
	// used in broker, if the tx is not a broker1 or broker2 tx, these values should be empty.
	HasBroker      bool
	SenderIsBroker bool
	OriginalSender utils.Address
	FinalRecipient utils.Address
	RawTxHash      []byte

	// RoutingFrozen records the future-effective mapping observed when the
	// supervisor injected this transaction. In-flight transactions must retain
	// these route endpoints even if a newer account mapping is committed before
	// the transaction leaves a shard queue.
	RoutingFrozen bool
	// One supervisor admission occurrence; preserved across both relay legs.
	AdmissionID     string
	IngressSequence uint64
	// Explicitly assigned zero-based ingress is distinct from a legacy zero
	// value. This flag is preserved with the original across relay/handoff.
	IngressSequenceAssigned        bool
	RoutingEpoch                   int
	MappingVersion                 uint64
	RoutedSenderShard              uint64
	RoutedRecipientShard           uint64
	SenderRequiresExistingState    bool
	RecipientRequiresExistingState bool
}

func (tx *Transaction) PrintTx() string {
	vals := []interface{}{
		tx.Sender[:],
		tx.Recipient[:],
		tx.Value,
		string(tx.TxHash[:]),
	}
	res := fmt.Sprintf("%v\n", vals)
	return res
}

// Encode transaction for storing
func (tx *Transaction) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// Decode transaction
func DecodeTx(to_decode []byte) *Transaction {
	var tx Transaction

	decoder := gob.NewDecoder(bytes.NewReader(to_decode))
	err := decoder.Decode(&tx)
	if err != nil {
		log.Panic(err)
	}

	return &tx
}

// new a transaction
func NewTransaction(sender, recipient string, value *big.Int, nonce uint64, proposeTime time.Time) *Transaction {
	tx := &Transaction{
		Sender:    sender,
		Recipient: recipient,
		Value:     value,
		Nonce:     nonce,
		Time:      proposeTime,
	}

	hash := sha256.Sum256(tx.Encode())
	tx.TxHash = hash[:]
	tx.Relayed = false
	tx.FinalRecipient = ""
	tx.OriginalSender = ""
	tx.RawTxHash = nil
	tx.HasBroker = false
	tx.SenderIsBroker = false
	return tx
}
