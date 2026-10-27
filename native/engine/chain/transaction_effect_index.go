package chain

import (
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

const NativeEffectIndexEnv = "SAA_NATIVE_EFFECT_INDEX"

type EffectIndexTrie interface {
	Get([]byte) ([]byte, error)
	Update([]byte, []byte) error
}

type TransactionEffectStatus string

const (
	EffectIndexDisabled TransactionEffectStatus = "DISABLED"
	EffectUnknown       TransactionEffectStatus = "UNKNOWN"
	EffectUnseen        TransactionEffectStatus = "UNSEEN"
	EffectSeen          TransactionEffectStatus = "SEEN"
)

type TransactionEffectCoverage struct {
	Version              uint64 `json:"version"`
	StartBlock           uint64 `json:"start_block"`
	FirstIngressSequence uint64 `json:"first_ingress_sequence"`
}

type TransactionEffectReceipt struct {
	Effect              core.TransactionEffect `json:"effect"`
	Block               uint64                 `json:"block"`
	Relay               []byte                 `json:"relay,omitempty"`
	Transaction         []byte                 `json:"transaction,omitempty"`
	ImportedSourceRoot  string                 `json:"imported_source_root,omitempty"`
	ImportedSourceBlock uint64                 `json:"imported_source_block,omitempty"`
	ImportedSourceShard uint64                 `json:"imported_source_shard,omitempty"`
}

var transactionEffectCoverageKey = []byte("__sef_transaction_effect_coverage_v1__")

func NativeEffectIndexConfiguration() (bool, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(NativeEffectIndexEnv))) {
	case "", "false", "0":
		return false, nil
	case "true", "1":
		return true, nil
	default:
		return false, fmt.Errorf("invalid %s configuration", NativeEffectIndexEnv)
	}
}

func transactionEffectCoverage(st EffectIndexTrie) (*TransactionEffectCoverage, error) {
	raw, err := st.Get(transactionEffectCoverageKey)
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	// A missing/null first-ingress field cannot impersonate explicit zero.
	coverage := TransactionEffectCoverage{FirstIngressSequence: ^uint64(0)}
	if err = json.Unmarshal(raw, &coverage); err != nil {
		return nil, err
	}
	if coverage.Version != 1 || coverage.StartBlock != 1 || coverage.FirstIngressSequence > 1 {
		return nil, fmt.Errorf("invalid transaction effect coverage")
	}
	return &coverage, nil
}

// InitializeTransactionEffectIndex writes only to the caller's working trie.
// It must share the first actual state commit, not be claimed after the fact.
// Existing non-genesis ledgers need a separately verified backfill protocol;
// this implementation deliberately cannot certify their historical effects.
func InitializeTransactionEffectIndex(st EffectIndexTrie, currentBlock uint64) error {
	enabled, err := NativeEffectIndexConfiguration()
	if err != nil {
		return err
	}
	coverage, err := transactionEffectCoverage(st)
	if err != nil {
		return err
	}
	if !enabled {
		if coverage != nil {
			return fmt.Errorf("cannot disable persisted transaction effect index")
		}
		return nil
	}
	if coverage != nil {
		return nil
	}
	if currentBlock != 0 {
		return fmt.Errorf("transaction effect index cannot start on non-genesis ledger")
	}
	raw, _ := json.Marshal(TransactionEffectCoverage{Version: 1, StartBlock: 1, FirstIngressSequence: 0})
	return st.Update(transactionEffectCoverageKey, raw)
}

func RequireMigrationEffectIndex(st EffectIndexTrie) error {
	enabled, err := NativeEffectIndexConfiguration()
	if err != nil {
		return err
	}
	coverage, err := transactionEffectCoverage(st)
	if err != nil {
		return err
	}
	if !enabled || coverage == nil {
		return fmt.Errorf("migration requires complete transaction effect coverage")
	}
	return nil
}

func transactionEffectKey(e core.TransactionEffect) []byte {
	return []byte("__sef_transaction_effect_v1__/" + e.Account + "/" + e.Effect + "/" + e.InstanceID)
}

func transactionAdmissionBindingKey(admission string) []byte {
	h := sha256.Sum256([]byte(admission))
	return []byte("__sef_transaction_admission_binding_v1__/" + hex.EncodeToString(h[:]))
}

type transactionAdmissionBinding struct {
	AdmissionID, TxHash, PayloadHash string
	IngressSequence                  uint64
}

func effectAdmissionBinding(e core.TransactionEffect) transactionAdmissionBinding {
	return transactionAdmissionBinding{e.AdmissionID, e.TxHash, e.PayloadHash, e.IngressSequence}
}

// LookupTransactionEffect reports local committed effects, not a proof that
// an external debit occurred. The caller still needs the originating execution
// authorization. UNKNOWN must never be converted into permission to execute.
func LookupTransactionEffect(st EffectIndexTrie, tx *core.Transaction, effect string) (TransactionEffectStatus, error) {
	enabled, err := NativeEffectIndexConfiguration()
	if err != nil {
		return EffectUnknown, err
	}
	coverage, err := transactionEffectCoverage(st)
	if err != nil {
		return EffectUnknown, err
	}
	if !enabled {
		if coverage != nil {
			return EffectUnknown, fmt.Errorf("cannot disable persisted transaction effect index")
		}
		return EffectIndexDisabled, nil
	}
	if coverage == nil {
		return EffectUnknown, nil
	}
	e, err := core.BuildTransactionEffect(tx, effect)
	if errors.Is(err, core.ErrTransactionEffectIdentityMissing) {
		return EffectUnknown, nil
	}
	if err != nil {
		return EffectUnknown, err
	}
	if e.IngressSequence < coverage.FirstIngressSequence {
		return EffectUnknown, nil // Persisted positive-only coverage is not expanded.
	}
	bindingRaw, err := st.Get(transactionAdmissionBindingKey(e.AdmissionID))
	if err != nil {
		return EffectUnknown, err
	}
	if len(bindingRaw) > 0 {
		var binding transactionAdmissionBinding
		if err = json.Unmarshal(bindingRaw, &binding); err != nil {
			return EffectUnknown, err
		}
		if binding != effectAdmissionBinding(e) {
			return EffectUnknown, fmt.Errorf("conflicting admission payload")
		}
	}
	raw, err := st.Get(transactionEffectKey(e))
	if err != nil {
		return EffectUnknown, err
	}
	if len(raw) == 0 {
		return EffectUnseen, nil
	}
	var receipt TransactionEffectReceipt
	if err = json.Unmarshal(raw, &receipt); err != nil {
		return EffectUnknown, err
	}
	if len(bindingRaw) == 0 || receipt.Effect != e || receipt.Block < coverage.StartBlock {
		return EffectUnknown, fmt.Errorf("conflicting or incomplete transaction effect marker")
	}
	if len(receipt.Relay) > 0 {
		var relay core.Transaction
		if err = gob.NewDecoder(bytes.NewReader(receipt.Relay)).Decode(&relay); err != nil {
			return EffectUnknown, err
		}
		bound, bindErr := core.BuildTransactionEffect(&relay, core.TransactionDebit)
		if bindErr != nil || bound != e || !relay.Relayed || !relay.RoutingFrozen || relay.RoutedSenderShard == relay.RoutedRecipientShard {
			return EffectUnknown, fmt.Errorf("invalid transaction effect relay obligation")
		}
	}
	return EffectSeen, nil
}

// ReadTransactionEffectReceipt makes committed cross-shard debit obligations
// recoverable without relying on a transient network send or transaction pool.
// A nil receipt is not execution permission; use Lookup to distinguish statuses.
func ReadTransactionEffectReceipt(st EffectIndexTrie, tx *core.Transaction, effect string) (*TransactionEffectReceipt, error) {
	status, err := LookupTransactionEffect(st, tx, effect)
	if err != nil || status != EffectSeen {
		return nil, err
	}
	e, _ := core.BuildTransactionEffect(tx, effect)
	raw, err := st.Get(transactionEffectKey(e))
	if err != nil {
		return nil, err
	}
	var receipt TransactionEffectReceipt
	if err = json.Unmarshal(raw, &receipt); err != nil {
		return nil, err
	}
	return &receipt, nil
}

// RecordTransactionEffect does not commit independently: the balance/nonce
// change, admission binding and marker must share one state root. On any error,
// discard the working trie instead of committing a partly updated transaction.
// false means a matching effect already exists, not permission to apply it again.
func RecordTransactionEffect(st EffectIndexTrie, tx *core.Transaction, effect string, block uint64) (bool, error) {
	if block == 0 {
		return false, fmt.Errorf("transaction effect commit block missing")
	}
	status, err := LookupTransactionEffect(st, tx, effect)
	if err != nil {
		return false, err
	}
	if status == EffectSeen {
		return false, nil
	}
	if status != EffectUnseen {
		return false, fmt.Errorf("transaction effect cannot be recorded without coverage and identity")
	}
	if effect == core.TransactionDebit && tx.Relayed {
		return false, fmt.Errorf("relay credit cannot create a debit effect")
	}
	e, err := core.BuildTransactionEffect(tx, effect)
	if err != nil {
		return false, err
	}
	binding, _ := json.Marshal(effectAdmissionBinding(e))
	record := TransactionEffectReceipt{Effect: e, Block: block, Transaction: tx.Encode()}
	if effect == core.TransactionDebit && !tx.Relayed && tx.RoutingFrozen && tx.RoutedSenderShard != tx.RoutedRecipientShard {
		relay := core.DecodeTx(tx.Encode())
		relay.Relayed = true
		record.Relay = relay.Encode()
	}
	receipt, _ := json.Marshal(record)
	if err = st.Update(transactionAdmissionBindingKey(e.AdmissionID), binding); err != nil {
		return false, err
	}
	if err = st.Update(transactionEffectKey(e), receipt); err != nil {
		return false, err
	}
	return true, nil
}
