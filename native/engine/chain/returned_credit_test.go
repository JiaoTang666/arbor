package chain

import (
	"blockEmulator/core"
	"testing"
)

func TestReturnedCreditWaitsForHandoffNotLocalDebit(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	bc, c, _ := groupFixture(t)
	defer bc.CloseBlockChain()
	accounts := []string{c.Members[0].Account, c.Members[1].Account}
	tx := indexedAdmission(accounts[0], "0000000000000000000000000000000000000030", 7, 41)
	if _, err := bc.CommitGroupSourcePreparation(c, 2, make([]byte, 32), nil); err != nil {
		t.Fatal(err)
	}
	if err := bc.Txpool.EnableGroupMigrationQueueJournal(t.TempDir(), accounts); err != nil {
		t.Fatal(err)
	}
	bc.Txpool.AddTx2Pool(tx)
	h, err := core.BuildGroupMigrationHandoff(tx, c.Members[0].ProposalID, accounts, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	relay := core.DecodeTx(h.Execution)
	relay.Relayed = true
	committedIndexedNative(t, bc, relay)
	items, err := bc.CollectSourceAppliedCredits([]*core.Transaction{tx}, 1)
	if err != nil || len(items) != 0 {
		t.Fatalf("returned credit is not local completion: items=%d err=%v", len(items), err)
	}
	if bc.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("classification prematurely removed pending debit")
	}
	status, err := bc.TransactionEffectStatus(tx, core.TransactionDebit)
	if err != nil || status != EffectUnseen {
		t.Fatal("source debit invented", status, err)
	}
	st, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := ReadTransactionEffectReceipt(st, tx, core.TransactionCredit)
	if err != nil {
		t.Fatal(err)
	}
	if !bc.isReturnedHandoffCredit(tx, receipt) {
		t.Fatal("exact transformation not recognized")
	}
	for _, kind := range []string{"wrong-version", "wrong-payload", "not-relay", "missing-transport"} {
		t.Run(kind, func(t *testing.T) {
			bad := *receipt
			actual := core.DecodeTx(receipt.Transaction)
			switch kind {
			case "wrong-version":
				actual.MappingVersion++
			case "wrong-payload":
				actual.Nonce++
			case "not-relay":
				actual.Relayed = false
			}
			bad.Transaction = actual.Encode()
			if kind == "missing-transport" {
				bad.Transaction = nil
			}
			if bc.isReturnedHandoffCredit(tx, &bad) {
				t.Fatal("unbound credit classified as returned handoff")
			}
		})
	}
}
