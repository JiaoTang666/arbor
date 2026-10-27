package chain

import (
	"blockEmulator/core"
	"bytes"
	"math/big"
	"testing"
)

func TestInactiveTargetHoldUsesLocalWriteEffects(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	source, command, path := groupFixture(t)
	defer source.CloseBlockChain()
	prepared, err := source.CommitGroupSourcePreparation(command, 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	outsider := "0000000000000000000000000000000000000041"
	target.Update_PartitionMap(outsider, 1)
	target.AddAccounts([]string{outsider}, []*core.AccountState{{Balance: big.NewInt(20)}}, 0)
	if _, err = target.CommitGroupTargetInstallation(*prepared, 1, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	a, b := command.Members[0].Account, command.Members[1].Account
	beforeA, err := target.ExportAccountState(a)
	if err != nil {
		t.Fatal(err)
	}
	beforeB, err := target.ExportAccountState(b)
	if err != nil {
		t.Fatal(err)
	}

	// Installation is not activation: genuine writes to the group stay fenced.
	localHeld := indexedAdmission(a, b, 1, 1)
	localHeld.RoutedSenderShard, localHeld.RoutedRecipientShard = 1, 1
	if err = target.CheckPreparedTransactions([]*core.Transaction{localHeld}); err == nil {
		t.Fatal("inactive target admitted a local group transfer")
	}
	relayHeld := indexedAdmission(outsider, a, 7, 2)
	relayHeld.Relayed = true
	relayHeld.RoutedSenderShard, relayHeld.RoutedRecipientShard = 1, 1
	if err = target.CheckPreparedTransactions([]*core.Transaction{relayHeld}); err == nil {
		t.Fatal("inactive target admitted a local Relay credit")
	}

	// Unrelated work and a debit addressed to the group's old source remain live.
	independent := indexedAdmission(outsider, outsider, 1, 3)
	independent.RoutedSenderShard, independent.RoutedRecipientShard = 1, 1
	if err = target.CheckPreparedTransactions([]*core.Transaction{independent}); err != nil {
		t.Fatal("inactive group blocked unrelated work", err)
	}
	committedIndexedNative(t, target, independent)
	crossDebit := indexedAdmission(outsider, a, 7, 4)
	crossDebit.RoutedSenderShard, crossDebit.RoutedRecipientShard = 1, 0
	if err = target.CheckPreparedTransactions([]*core.Transaction{crossDebit}); err != nil {
		t.Fatal("contact with old source was mistaken for a local target write", err)
	}
	committedIndexedNative(t, target, crossDebit)
	d, err := target.ExportAccountState(outsider)
	if err != nil || d.State.Balance.Int64() != 13 || d.State.Nonce != 2 {
		t.Fatal("native debit did not progress exactly once", d, err)
	}
	afterA, err := target.ExportAccountState(a)
	if err != nil || !bytes.Equal(beforeA.State.Encode(), afterA.State.Encode()) {
		t.Fatal("old-route debit changed inactive target recipient", err)
	}
	afterB, err := target.ExportAccountState(b)
	if err != nil || !bytes.Equal(beforeB.State.Encode(), afterB.State.Encode()) {
		t.Fatal("unrelated execution changed inactive target group", err)
	}
	if status, err := target.TransactionEffectStatus(crossDebit, core.TransactionDebit); err != nil || status != EffectSeen {
		t.Fatal("committed debit effect missing", status, err)
	}
	if status, err := target.TransactionEffectStatus(crossDebit, core.TransactionCredit); err != nil || status != EffectUnseen {
		t.Fatal("remote credit was recorded as a local effect", status, err)
	}
	st, err := target.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := ReadTransactionEffectReceipt(st, crossDebit, core.TransactionDebit)
	if err != nil || receipt == nil || len(receipt.Relay) == 0 {
		t.Fatal("cross-shard debit lost its durable Relay obligation", receipt, err)
	}
	relay := core.DecodeTx(receipt.Relay)
	if !relay.Relayed || relay.RoutedRecipientShard != 0 {
		t.Fatal("cross-shard debit changed its old-route Relay destination")
	}
	if block, err := target.GroupTargetActivationBlock(command.Members[0].ProposalID); err != nil || block != 0 {
		t.Fatal("ordinary progress implicitly activated the installed group", block, err)
	}
	if err = target.CheckPreparedTransactions([]*core.Transaction{relayHeld}); err == nil {
		t.Fatal("ordinary progress released the inactive target fence")
	}
}
