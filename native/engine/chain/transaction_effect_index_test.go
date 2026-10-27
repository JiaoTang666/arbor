package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"
)

type effectMemoryTrie map[string][]byte

func (st effectMemoryTrie) Get(key []byte) ([]byte, error) {
	return append([]byte(nil), st[string(key)]...), nil
}
func (st effectMemoryTrie) Update(key, value []byte) error {
	st[string(key)] = append([]byte(nil), value...)
	return nil
}

func indexedEffectFixture(t *testing.T) (effectMemoryTrie, *core.Transaction) {
	t.Helper()
	t.Setenv(NativeEffectIndexEnv, "true")
	st := effectMemoryTrie{}
	if err := InitializeTransactionEffectIndex(st, 0); err != nil {
		t.Fatal(err)
	}
	tx := core.NewTransaction(strings.Repeat("1", 40), strings.Repeat("2", 40), big.NewInt(7), 9, time.Unix(10, 0))
	tx.AdmissionID, tx.IngressSequence = "run/9", 9
	return st, tx
}

func TestEffectIndexRequiresGenesisCoverageAndKnownIdentity(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	st := effectMemoryTrie{}
	if err := InitializeTransactionEffectIndex(st, 5); err == nil || len(st) != 0 {
		t.Fatal("existing ledger was falsely certified")
	}
	if status, err := LookupTransactionEffect(st, nil, core.TransactionCredit); err != nil || status != EffectUnknown {
		t.Fatal("missing coverage inferred unseen", status, err)
	}
	if err := RequireMigrationEffectIndex(st); err == nil {
		t.Fatal("migration accepted missing coverage")
	}
	st, tx := indexedEffectFixture(t)
	tx.AdmissionID = ""
	if status, err := LookupTransactionEffect(st, tx, core.TransactionCredit); err != nil || status != EffectUnknown {
		t.Fatal("legacy nonce treated as admission identity", status, err)
	}
	if _, err := RecordTransactionEffect(st, tx, core.TransactionCredit, 1); err == nil {
		t.Fatal("legacy effect recorded as covered")
	}
	if err := InitializeTransactionEffectIndex(st, 100); err != nil {
		t.Fatal("valid persisted coverage not idempotent", err)
	}
	t.Setenv(NativeEffectIndexEnv, "false")
	if _, err := LookupTransactionEffect(st, tx, core.TransactionCredit); err == nil {
		t.Fatal("persisted index silently disabled")
	}
}

func TestEffectIndexSeenReplayAndAccountScopedStages(t *testing.T) {
	st, tx := indexedEffectFixture(t)
	if status, err := LookupTransactionEffect(st, tx, core.TransactionCredit); err != nil || status != EffectUnseen {
		t.Fatal(status, err)
	}
	if added, err := RecordTransactionEffect(st, tx, core.TransactionCredit, 2); err != nil || !added {
		t.Fatal(added, err)
	}
	effect, _ := core.BuildTransactionEffect(tx, core.TransactionCredit)
	original := append([]byte(nil), st[string(transactionEffectKey(effect))]...)
	other := core.DecodeTx(tx.Encode())
	other.Relayed, other.MappingVersion, other.RoutedRecipientShard = true, 8, 3
	if added, err := RecordTransactionEffect(st, other, core.TransactionCredit, 9); err != nil || added {
		t.Fatal("route replay executed again", added, err)
	}
	if !bytes.Equal(original, st[string(transactionEffectKey(effect))]) {
		t.Fatal("replay overwrote original execution block")
	}
	if status, err := LookupTransactionEffect(st, tx, core.TransactionDebit); err != nil || status != EffectUnseen {
		t.Fatal("credit implied debit", status, err)
	}
	if added, err := RecordTransactionEffect(st, tx, core.TransactionDebit, 3); err != nil || !added {
		t.Fatal(added, err)
	}
}

func TestEffectIndexRejectsChangedHashPayloadAndAccount(t *testing.T) {
	st, tx := indexedEffectFixture(t)
	if _, err := RecordTransactionEffect(st, tx, core.TransactionCredit, 2); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*core.Transaction){
		func(tx *core.Transaction) { tx.TxHash[0] ^= 1 },
		func(tx *core.Transaction) { tx.Value = big.NewInt(8) },
		func(tx *core.Transaction) { tx.Sender = strings.Repeat("3", 40) },
		func(tx *core.Transaction) { tx.Recipient = strings.Repeat("3", 40) },
		func(tx *core.Transaction) { tx.IngressSequence++ },
	} {
		other := core.DecodeTx(tx.Encode())
		mutate(other)
		before := len(st)
		if _, err := RecordTransactionEffect(st, other, core.TransactionCredit, 3); err == nil {
			t.Fatal("conflicting admission accepted")
		}
		if len(st) != before {
			t.Fatal("failed validation wrote an effect")
		}
	}
}

func TestEffectIndexDetectsCorruptMarkerAndInvalidConfiguration(t *testing.T) {
	st, tx := indexedEffectFixture(t)
	if _, err := RecordTransactionEffect(st, tx, core.TransactionCredit, 2); err != nil {
		t.Fatal(err)
	}
	effect, _ := core.BuildTransactionEffect(tx, core.TransactionCredit)
	st[string(transactionEffectKey(effect))] = []byte("null")
	if _, err := LookupTransactionEffect(st, tx, core.TransactionCredit); err == nil {
		t.Fatal("corrupt marker became unseen")
	}
	t.Setenv(NativeEffectIndexEnv, "maybe")
	if _, err := NativeEffectIndexConfiguration(); err == nil {
		t.Fatal("invalid configuration silently disabled index")
	}
}

func TestEffectIndexDisabledIsNotUnseen(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "false")
	st := effectMemoryTrie{}
	if err := InitializeTransactionEffectIndex(st, 5); err != nil || len(st) != 0 {
		t.Fatal("disabled mode wrote coverage", err)
	}
	if status, err := LookupTransactionEffect(st, nil, core.TransactionCredit); err != nil || status != EffectIndexDisabled {
		t.Fatal("disabled mode granted execution", status, err)
	}
	if _, err := RecordTransactionEffect(st, nil, core.TransactionCredit, 1); err == nil {
		t.Fatal("disabled mode recorded an effect")
	}
}

func TestEffectIndexRetainsCommittedRelayObligation(t *testing.T) {
	st, tx := indexedEffectFixture(t)
	tx.RoutingFrozen, tx.RoutedSenderShard, tx.RoutedRecipientShard = true, 0, 1
	if _, err := RecordTransactionEffect(st, tx, core.TransactionDebit, 2); err != nil {
		t.Fatal(err)
	}
	receipt, err := ReadTransactionEffectReceipt(st, tx, core.TransactionDebit)
	if err != nil || receipt == nil || len(receipt.Relay) == 0 {
		t.Fatal("committed debit lost its next leg", err)
	}
	relay := core.DecodeTx(receipt.Relay)
	if !relay.Relayed || relay.AdmissionID != tx.AdmissionID || relay.RoutedRecipientShard != 1 {
		t.Fatal("relay obligation changed admission or destination")
	}
	// New placement metadata cannot overwrite the already committed next leg.
	other := core.DecodeTx(tx.Encode())
	other.RoutedRecipientShard = 3
	if added, err := RecordTransactionEffect(st, other, core.TransactionDebit, 4); err != nil || added {
		t.Fatal(added, err)
	}
	again, err := ReadTransactionEffectReceipt(st, other, core.TransactionDebit)
	if err != nil || !bytes.Equal(again.Relay, receipt.Relay) {
		t.Fatal("replay rewrote relay responsibility", err)
	}
	st[string(transactionEffectKey(receipt.Effect))] = bytes.Replace(st[string(transactionEffectKey(receipt.Effect))], []byte(`"block":2`), []byte(`"block":0`), 1)
	if _, err := ReadTransactionEffectReceipt(st, tx, core.TransactionDebit); err == nil {
		t.Fatal("invalid committed obligation accepted")
	}
}

func TestEffectIndexDoesNotInferDebitFromRelayCredit(t *testing.T) {
	st, tx := indexedEffectFixture(t)
	tx.Relayed = true
	if _, err := RecordTransactionEffect(st, tx, core.TransactionDebit, 2); err == nil {
		t.Fatal("relay credit manufactured debit evidence")
	}
	if status, err := LookupTransactionEffect(st, tx, core.TransactionDebit); err != nil || status != EffectUnseen {
		t.Fatal("rejected relay wrote a debit marker", status, err)
	}
}

func TestEffectIndexZeroIngressAndPersistedPositiveOnlyCoverage(t *testing.T) {
	st, tx := indexedEffectFixture(t)
	coverage, err := transactionEffectCoverage(st)
	if err != nil || coverage.FirstIngressSequence != 0 {
		t.Fatal("new genesis is not zero-based", err)
	}
	tx.AdmissionID, tx.IngressSequence, tx.IngressSequenceAssigned = "run/0", 0, true
	tx.RoutingFrozen, tx.RoutedSenderShard, tx.RoutedRecipientShard = true, 0, 1
	if added, err := RecordTransactionEffect(st, tx, core.TransactionDebit, 1); err != nil || !added {
		t.Fatal("explicit first ingress not indexed", added, err)
	}
	receipt, err := ReadTransactionEffectReceipt(st, tx, core.TransactionDebit)
	if err != nil || receipt == nil {
		t.Fatal("first debit not retained", err)
	}
	relay := core.DecodeTx(receipt.Relay)
	if relay.IngressSequence != 0 || !relay.IngressSequenceAssigned || relay.AdmissionID != tx.AdmissionID {
		t.Fatal("first Relay lost ingress identity")
	}
	if added, err := RecordTransactionEffect(st, core.DecodeTx(tx.Encode()), core.TransactionDebit, 2); err != nil || added {
		t.Fatal("retransmitted first ingress executed again", added, err)
	}
	legacy := effectMemoryTrie{}
	raw, _ := json.Marshal(TransactionEffectCoverage{Version: 1, StartBlock: 1, FirstIngressSequence: 1})
	legacy[string(transactionEffectCoverageKey)] = raw
	if err := InitializeTransactionEffectIndex(legacy, 9); err != nil || !bytes.Equal(raw, legacy[string(transactionEffectCoverageKey)]) {
		t.Fatal("positive-only persisted coverage changed", err)
	}
	if status, err := LookupTransactionEffect(legacy, tx, core.TransactionDebit); err != nil || status != EffectUnknown {
		t.Fatal("old coverage falsely includes ingress zero", status, err)
	}
	if _, err := RecordTransactionEffect(legacy, tx, core.TransactionDebit, 10); err == nil || len(legacy) != 1 {
		t.Fatal("old coverage recorded an uncovered zero effect", err)
	}
	legacy[string(transactionEffectCoverageKey)] = []byte(`{"version":1,"start_block":1}`)
	if _, err := transactionEffectCoverage(legacy); err == nil {
		t.Fatal("missing coverage start inferred as explicit zero")
	}
}

func TestIndexedNativeFirstZeroIngressCommitsOnce(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	bc, command, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	tx := indexedAdmission(command.Account, "0000000000000000000000000000000000000020", 7, 0)
	tx.IngressSequenceAssigned = true
	committedIndexedNative(t, bc, tx, core.DecodeTx(tx.Encode()))
	a, err := bc.ExportAccountState(tx.Sender)
	if err != nil {
		t.Fatal(err)
	}
	b, err := bc.ExportAccountState(tx.Recipient)
	if err != nil {
		t.Fatal(err)
	}
	if a.State.Balance.Int64() != 93 || a.State.Nonce != 1 || b.State.Balance.Int64() != 107 {
		t.Fatal("first zero ingress was dropped or executed repeatedly")
	}
	st, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	if status, err := LookupTransactionEffect(st, tx, core.TransactionCredit); err != nil || status != EffectSeen {
		t.Fatal("first real native credit lacks committed evidence", status, err)
	}
}
