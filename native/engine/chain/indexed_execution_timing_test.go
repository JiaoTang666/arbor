package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"errors"
	"math/big"
	"testing"
)

type executionReadTrie struct {
	effectMemoryTrie
	reads int
	err   error
}

func (st *executionReadTrie) Get(key []byte) ([]byte, error) {
	st.reads++
	if st.err != nil {
		return nil, st.err
	}
	return st.effectMemoryTrie.Get(key)
}

func TestExecutionAccountUsesOneReadAndPreservesErrors(t *testing.T) {
	st := &executionReadTrie{effectMemoryTrie: effectMemoryTrie{"a": (&core.AccountState{Balance: big.NewInt(93), Nonce: 2}).Encode()}}
	timing := &indexedExecutionTiming{}
	state, exists, err := readExecutionAccount(st, "a", true, timing)
	if err != nil || !exists || state.Balance.Int64() != 93 || state.Nonce != 2 || st.reads != 1 || timing.stateReads != 1 {
		t.Fatal("existing state was not read exactly once", state, exists, st.reads, err)
	}
	state, exists, err = readExecutionAccount(st, "new", false, nil)
	if err != nil || exists || state.Balance.Cmp(params.Init_Balance) != 0 || st.reads != 2 {
		t.Fatal("lazy state initialization changed", exists, st.reads, err)
	}
	initial := new(big.Int).Set(params.Init_Balance)
	state.Balance.SetInt64(1)
	if params.Init_Balance.Cmp(initial) != 0 {
		t.Fatal("lazy balance aliased the shared initial balance")
	}
	if _, _, err = readExecutionAccount(st, "new", true, nil); err == nil || st.reads != 3 {
		t.Fatal("required absence was not rejected", st.reads, err)
	}
	injected := errors.New("injected state read failure")
	st.err = injected
	if _, _, err = readExecutionAccount(st, "a", false, nil); !errors.Is(err, injected) || st.reads != 4 {
		t.Fatal("read failure was converted to absence", st.reads, err)
	}
}

func TestIndexedExecutionOneReadPerEffectAndReplay(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	bc, c, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	st, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	head := append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
	tx := indexedAdmission(c.Account, c.Account, 7, 1)
	timing := &indexedExecutionTiming{}
	changed, err := bc.applyIndexedRawRelayObserved(st, tx, false, timing)
	if err != nil || changed != 2 || timing.stateReads != 2 || timing.applied != 2 {
		t.Fatal("self-transfer did not use one immediate read per effect", changed, timing, err)
	}
	raw, err := st.Get([]byte(c.Account))
	if err != nil {
		t.Fatal(err)
	}
	state := core.DecodeAS(raw)
	if state.Balance.Int64() != 100 || state.Nonce != 1 {
		t.Fatal("credit did not read the newly debited self-transfer state", state)
	}
	root := st.Hash()
	changed, err = bc.applyIndexedRawRelayObserved(st, core.DecodeTx(tx.Encode()), false, timing)
	if err != nil || changed != 0 || timing.stateReads != 2 || timing.applied != 2 || st.Hash() != root {
		t.Fatal("duplicate effects reread or changed account state", changed, timing, err)
	}
	for _, effect := range []string{core.TransactionDebit, core.TransactionCredit} {
		if status, err := LookupTransactionEffect(st, tx, effect); err != nil || status != EffectSeen {
			t.Fatal("effect marker missing", effect, status, err)
		}
	}
	if !bytes.Equal(head, bc.CurrentBlock.Header.StateRoot) {
		t.Fatal("private execution published a root")
	}
}

func TestIndexedExecutionInsufficientDebitDoesNotCredit(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(map[bool]string{false: "native_skip", true: "strict_error"}[strict], func(t *testing.T) {
			t.Setenv(NativeEffectIndexEnv, "true")
			bc, c, _ := preparationFixture(t)
			defer bc.CloseBlockChain()
			st, err := bc.stateTrie()
			if err != nil {
				t.Fatal(err)
			}
			root := st.Hash()
			tx := indexedAdmission(c.Account, "0000000000000000000000000000000000000020", 101, 1)
			timing := &indexedExecutionTiming{}
			changed, err := bc.applyIndexedRawRelayObserved(st, tx, strict, timing)
			if (err != nil) != strict || changed != 0 || timing.stateReads != 1 || timing.applied != 0 || st.Hash() != root {
				t.Fatal("insufficient debit changed state or continued to credit", changed, timing, err)
			}
			for _, effect := range []string{core.TransactionDebit, core.TransactionCredit} {
				if status, err := LookupTransactionEffect(st, tx, effect); err != nil || status != EffectUnseen {
					t.Fatal("insufficient debit recorded an effect", effect, status, err)
				}
			}
		})
	}
}

func TestIndexedExecutionSeenDebitAllowsOutstandingCredit(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	bc, c, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	st, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	tx := indexedAdmission(c.Account, "0000000000000000000000000000000000000020", 7, 1)
	// Model the exact debit state and receipt already installed before replay.
	if err = st.Update([]byte(c.Account), (&core.AccountState{Balance: big.NewInt(93), Nonce: 1}).Encode()); err != nil {
		t.Fatal(err)
	}
	if _, err = RecordTransactionEffect(st, tx, core.TransactionDebit, bc.CurrentBlock.Header.Number+1); err != nil {
		t.Fatal(err)
	}
	timing := &indexedExecutionTiming{}
	changed, err := bc.applyIndexedRawRelayObserved(st, tx, false, timing)
	if err != nil || changed != 1 || timing.stateReads != 1 || timing.applied != 1 {
		t.Fatal("seen debit blocked the outstanding credit", changed, timing, err)
	}
	raw, err := st.Get([]byte(tx.Recipient))
	if err != nil || core.DecodeAS(raw).Balance.Int64() != 107 {
		t.Fatal("outstanding credit was not applied", err)
	}
}

func TestIndexedExecutionDiagnosticsDoNotChangeRoot(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	bc, c, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	plain, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	observed, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	peer := "0000000000000000000000000000000000000020"
	first := indexedAdmission(c.Account, peer, 7, 1)
	self := indexedAdmission(c.Account, c.Account, 5, 2)
	relay := indexedAdmission(peer, c.Account, 2, 3)
	relay.Relayed, relay.RoutedSenderShard, relay.RoutedRecipientShard = true, 1, 0
	timing := &indexedExecutionTiming{}
	for _, tx := range []*core.Transaction{first, self, core.DecodeTx(first.Encode()), relay} {
		changed, err := bc.applyIndexedRawRelay(plain, tx, false)
		observedChanged, observedErr := bc.applyIndexedRawRelayObserved(observed, tx, false, timing)
		if err != nil || observedErr != nil || changed != observedChanged {
			t.Fatal("diagnostics changed effect execution", changed, observedChanged, err, observedErr)
		}
	}
	if observed.Hash() != plain.Hash() || timing.stateReads != 5 || timing.applied != 5 || timing.transactions != 4 {
		t.Fatal("diagnostics changed the private root or reread account state", timing)
	}
	raw, err := observed.Get([]byte(c.Account))
	if err != nil {
		t.Fatal(err)
	}
	state := core.DecodeAS(raw)
	if state.Balance.Int64() != 95 || state.Nonce != 2 {
		t.Fatal("ordered native, self-transfer and Relay effects changed", state)
	}
}
