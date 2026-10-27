package chain

import (
	"blockEmulator/core"
	"bytes"
	"testing"
)

func TestCommittedNativeEffectsRejectUnexecutedDebitAndCountOnce(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	bc, command, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	recipient := "0000000000000000000000000000000000000031"
	good := indexedAdmission(command.Account, recipient, 7, 1)
	bad := indexedAdmission(command.Account, recipient, 200, 2)
	good.RoutedRecipientShard, bad.RoutedRecipientShard = 1, 1
	duplicate := core.DecodeTx(good.Encode())
	committedIndexedNative(t, bc, good, duplicate, bad)
	block := *bc.CurrentBlock
	before := good.Encode()
	executed, err := bc.ExecutedNativeTransactions(&block)
	if err != nil || len(executed) != 1 || executed[0].AdmissionID != good.AdmissionID {
		t.Fatal("failed/duplicate debit was eligible for Relay1 or success metrics", len(executed), err)
	}
	executed[0].Relayed = true
	if !bytes.Equal(before, good.Encode()) {
		t.Fatal("classification mutated committed body")
	}
	oldBlock := block
	committedIndexedNative(t, bc, core.DecodeTx(good.Encode()))
	replay := *bc.CurrentBlock
	if executed, err = bc.ExecutedNativeTransactions(&replay); err != nil || len(executed) != 0 {
		t.Fatal("earlier debit was reported as new service", len(executed), err)
	}
	if executed, err = bc.ExecutedNativeTransactions(&oldBlock); err != nil || len(executed) != 1 {
		t.Fatal("report used latest root instead of requested committed block", len(executed), err)
	}
}

func TestCommittedNativeEffectsUnknownIsNotSuccess(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	bc, command, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	legacy := indexedAdmission(command.Account, "0000000000000000000000000000000000000020", 1, 1)
	legacy.AdmissionID = ""
	block := *bc.CurrentBlock
	block.Body = []*core.Transaction{legacy}
	if _, err := bc.ExecutedNativeTransactions(&block); err == nil {
		t.Fatal("unknown admission was reported as executed")
	}
}
