package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"
	"time"
)

func committedIndexedNative(t *testing.T, bc *BlockChain, txs ...*core.Transaction) {
	t.Helper()
	bc.SendTx2Pool(txs)
	block := bc.GenerateBlock(int32(bc.ChainConfig.NodeID))
	if len(block.Body) != len(txs) {
		t.Fatal("native fixture failed to include all supplied transactions")
	}
	bc.AddBlock(block)
}

func indexedAdmission(sender, recipient string, value int64, sequence uint64) *core.Transaction {
	tx := core.NewTransaction(sender, recipient, big.NewInt(value), sequence, time.Unix(1, 0))
	tx.AdmissionID, tx.IngressSequence = fmt.Sprintf("test/%d", sequence), sequence
	tx.RoutingFrozen = true
	return tx
}

func activatedIndexedGroup(t *testing.T) (*BlockChain, *BlockChain, core.GroupPrepareCommand) {
	t.Helper()
	t.Setenv(NativeEffectIndexEnv, "true")
	source, c, path := groupFixture(t)
	t.Cleanup(source.CloseBlockChain)
	p, err := source.CommitGroupSourcePreparation(c, 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	target := newMigrationChain(t, path, 1)
	t.Cleanup(target.CloseBlockChain)
	installed, err := target.CommitGroupTargetInstallation(*p, 1, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	activation := GroupTargetActivation{}
	for _, member := range p.Members {
		activation.Members = append(activation.Members, TargetActivation{
			ProposalID: member.Command.ProposalID, Account: member.Command.Account,
			TargetShard: 1, MappingVersion: 1, StateHash: member.StateHash, InstallDigest: installed.CommitDigest,
		})
	}
	if err = target.CommitGroupTargetActivation(activation); err != nil {
		t.Fatal(err)
	}
	return source, target, c
}

func TestIndexedActualNativeDuplicateDoesNotRepeatBalanceOrNonce(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	bc, command, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	sender, recipient := command.Account, "0000000000000000000000000000000000000020"
	tx := indexedAdmission(sender, recipient, 7, 1)
	committedIndexedNative(t, bc, tx, core.DecodeTx(tx.Encode()))
	committedIndexedNative(t, bc, core.DecodeTx(tx.Encode()))
	a, _ := bc.ExportAccountState(sender)
	b, _ := bc.ExportAccountState(recipient)
	if a.State.Balance.Int64() != 93 || a.State.Nonce != 1 || b.State.Balance.Int64() != 107 {
		t.Fatal("native repeated the same admission effects")
	}
}

func TestIndexedExecutionStageOwnership(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		relayed                     bool
		senderShard, recipientShard uint64
		valid                       bool
	}{
		{"ordinary_wrong_owner", false, 1, 0, false},
		{"relay_wrong_owner", true, 0, 1, false},
		{"frozen_source_debit_remote_credit", false, 0, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(NativeEffectIndexEnv, "true")
			bc, command, _ := preparationFixture(t)
			defer bc.CloseBlockChain()
			tx := indexedAdmission(command.Account, "0000000000000000000000000000000000000041", 7, 1)
			tx.Relayed, tx.RoutedSenderShard, tx.RoutedRecipientShard = tc.relayed, tc.senderShard, tc.recipientShard
			// Frozen Execution, not a newer map, decides the stage owner.
			if tc.valid {
				bc.Update_PartitionMap(command.Account, 1)
			}
			st, err := bc.stateTrie()
			if err != nil {
				t.Fatal(err)
			}
			root, head := st.Hash(), append([]byte(nil), bc.CurrentBlock.Hash...)
			changed, err := bc.applyIndexedRawRelay(st, tx, false)
			if tc.valid {
				if err != nil || changed != 1 {
					t.Fatal("valid source debit rejected", changed, err)
				}
				raw, err := st.Get([]byte(tx.Sender))
				if err != nil {
					t.Fatal(err)
				}
				state := core.DecodeAS(raw)
				if state.Balance.Int64() != 93 || state.Nonce != 1 {
					t.Fatal("wrong source debit")
				}
				receipt, err := ReadTransactionEffectReceipt(st, tx, core.TransactionDebit)
				if err != nil || receipt == nil || len(receipt.Relay) == 0 {
					t.Fatal("Relay obligation lost", err)
				}
			} else if err == nil || changed != 0 || st.Hash() != root {
				t.Fatal("wrong stage was not rejected before state/marker writes", changed, err)
			}
			if raw, err := st.Get([]byte(tx.Recipient)); err != nil || len(raw) != 0 {
				t.Fatal("wrong stage or source debit created recipient", err)
			}
			if !bytes.Equal(head, bc.CurrentBlock.Hash) || !bytes.Equal(root.Bytes(), bc.CurrentBlock.Header.StateRoot) {
				t.Fatal("private execution published state")
			}
		})
	}
}

func TestIndexedHandoffBatchOverdraftDoesNotPublishPrefix(t *testing.T) {
	_, target, c := activatedIndexedGroup(t)
	accounts := []string{c.Members[0].Account, c.Members[1].Account}
	var items []core.MigrationHandoff
	for i := uint64(1); i <= 2; i++ {
		tx := indexedAdmission(accounts[0], accounts[1], 60, i)
		h, err := core.BuildGroupMigrationHandoff(tx, c.Members[0].ProposalID, accounts, 0, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, *h)
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	if err := target.ValidateMigrationHandoffBatch(items); err == nil {
		t.Fatal("cumulative overdraft passed validation")
	}
	if err := target.CommitMigrationHandoffBatch(items); err == nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("failed indexed batch published a prefix", err)
	}
	st, _ := target.stateTrie()
	if raw, _ := st.Get(handoffExecutionKey(items[0].ID)); len(raw) != 0 {
		t.Fatal("failed batch exposed envelope marker")
	}
	if status, err := LookupTransactionEffect(st, core.DecodeTx(items[0].Execution), core.TransactionDebit); err != nil || status != EffectUnseen {
		t.Fatal("failed batch exposed balance-effect marker", status, err)
	}
	a, _ := target.ExportAccountState(accounts[0])
	if a.State.Balance.Int64() != 100 {
		t.Fatal("failed batch changed balance")
	}
}

func TestIndexedNativeThenDifferentHandoffEnvelopeDoesNotRepeatEffects(t *testing.T) {
	_, target, c := activatedIndexedGroup(t)
	accounts := []string{c.Members[0].Account, c.Members[1].Account}
	tx := indexedAdmission(accounts[0], accounts[1], 7, 1)
	tx.RoutedSenderShard, tx.RoutedRecipientShard = 1, 1
	committedIndexedNative(t, target, tx)
	old := core.DecodeTx(tx.Encode())
	old.RoutedSenderShard, old.RoutedRecipientShard = 0, 0
	h, err := core.BuildGroupMigrationHandoff(old, c.Members[0].ProposalID, accounts, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = target.CommitMigrationHandoff(*h); err != nil {
		t.Fatal(err)
	}
	a, _ := target.ExportAccountState(accounts[0])
	b, _ := target.ExportAccountState(accounts[1])
	if a.State.Balance.Int64() != 93 || a.State.Nonce != 1 || b.State.Balance.Int64() != 107 {
		t.Fatal("new handoff envelope repeated native effects")
	}
}

func TestIndexedInboundRejectsCreditAlreadyContainedInFrozenSnapshot(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	source, c, _ := groupFixture(t)
	defer source.CloseBlockChain()
	sender := "0000000000000000000000000000000000000030"
	tx := indexedAdmission(sender, c.Members[0].Account, 7, 1)
	committedIndexedNative(t, source, tx)
	for i := range c.Members {
		state, err := source.ExportAccountState(c.Members[i].Account)
		if err != nil {
			t.Fatal(err)
		}
		c.Members[i].ExpectedBlockHash = hex.EncodeToString(source.CurrentBlock.Hash)
		c.Members[i].ExpectedStateRoot = hex.EncodeToString(source.CurrentBlock.Header.StateRoot)
		c.Members[i].ExpectedStateHash = hex.EncodeToString(state.StateHash)
	}
	if _, err := source.CommitGroupSourcePreparation(c, 3, make([]byte, 32), nil); err != nil {
		t.Fatal(err)
	}
	head := append([]byte(nil), source.CurrentBlock.Hash...)
	if _, err := source.CommitInboundDebit(tx, 1); err == nil {
		t.Fatal("already credited source-local transaction produced a new credit handoff")
	}
	if err := source.CommitInboundDebitBatch([]InboundDebitItem{{Transaction: tx, MappingVersion: 1}}); err == nil {
		t.Fatal("batch bypassed source credit check")
	}
	legacy := core.DecodeTx(tx.Encode())
	legacy.AdmissionID = ""
	if _, err := source.CommitInboundDebit(legacy, 1); err == nil {
		t.Fatal("unknown source credit coverage allowed handoff")
	}
	if !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("rejected inbound replay published state")
	}
	a, _ := source.ExportAccountState(c.Members[0].Account)
	b, _ := source.ExportAccountState(sender)
	if a.State.Balance.Int64() != 107 || b.State.Balance.Int64() != 93 {
		t.Fatal("rejected inbound changed balances")
	}
}

func TestIndexedNativeCreditThenAuxiliaryRelayDoesNotRepeat(t *testing.T) {
	source, target, c := activatedIndexedGroup(t)
	accounts := []string{c.Members[0].Account, c.Members[1].Account}
	recipient := "0000000000000000000000000000000000000030"
	original := indexedAdmission(accounts[0], recipient, 7, 1)
	actual := core.DecodeTx(original.Encode())
	actual.MappingVersion, actual.RoutedSenderShard, actual.RoutedRecipientShard = 1, 1, 0
	actual.SenderRequiresExistingState = true
	committedIndexedNative(t, target, actual)
	h, err := core.BuildGroupMigrationHandoff(original, c.Members[0].ProposalID, accounts, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	first, err := target.CommitMigrationHandoff(*h)
	if err != nil || first == nil || len(first.Relay) == 0 {
		t.Fatal("native debit next leg not retained through envelope replay", err)
	}
	committedIndexedNative(t, source, core.DecodeTx(first.Relay))
	if _, err = source.CommitHandoffRelay(*first); err != nil {
		t.Fatal(err)
	}
	a, _ := target.ExportAccountState(accounts[0])
	b, _ := source.ExportAccountState(recipient)
	if a.State.Balance.Int64() != 93 || a.State.Nonce != 1 || b.State.Balance.Int64() != 107 {
		t.Fatal("auxiliary envelope repeated an already committed native effect")
	}
}
