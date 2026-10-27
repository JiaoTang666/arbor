package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"encoding/hex"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/trie"
)

func TestCompletedLocalOriginalDrainsWithoutReexecutionAndRecovers(t *testing.T) {
	for _, outgoing := range []bool{false, true} {
		name := "outside-to-group"
		if outgoing {
			name = "group-to-outside"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv(NativeEffectIndexEnv, "true")
			bc, command, _ := groupFixture(t)
			defer bc.CloseBlockChain()
			member, outside := command.Members[0].Account, "0000000000000000000000000000000000000030"
			sender, recipient := outside, member
			if outgoing {
				sender, recipient = member, outside
			}
			tx := indexedAdmission(sender, recipient, 7, 21)
			committedIndexedNative(t, bc, tx)
			for i := range command.Members {
				state, err := bc.ExportAccountState(command.Members[i].Account)
				if err != nil {
					t.Fatal(err)
				}
				command.Members[i].ExpectedBlockHash = hex.EncodeToString(bc.CurrentBlock.Hash)
				command.Members[i].ExpectedStateRoot = hex.EncodeToString(bc.CurrentBlock.Header.StateRoot)
				command.Members[i].ExpectedStateHash = hex.EncodeToString(state.StateHash)
			}
			if _, err := bc.CommitGroupSourcePreparation(command, 3, make([]byte, 32), nil); err != nil {
				t.Fatal(err)
			}
			accounts := []string{command.Members[0].Account, command.Members[1].Account}
			dir := t.TempDir()
			if err := bc.Txpool.EnableGroupMigrationQueueJournal(dir, accounts); err != nil {
				t.Fatal(err)
			}
			unknown := indexedAdmission(sender, recipient, 3, 22)
			bc.Txpool.AddTxs2Pool([]*core.Transaction{core.DecodeTx(tx.Encode()), unknown})
			head, root := append([]byte(nil), bc.CurrentBlock.Hash...), append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
			if removed, err := bc.ConfirmSourceAppliedCredits([]*core.Transaction{tx, unknown}); err != nil || removed != 1 {
				t.Fatal(removed, err)
			}
			if !bytes.Equal(head, bc.CurrentBlock.Hash) || !bytes.Equal(root, bc.CurrentBlock.Header.StateRoot) {
				t.Fatal("completion altered committed state")
			}
			a, _ := bc.ExportAccountState(sender)
			b, _ := bc.ExportAccountState(recipient)
			if a.State.Balance.Int64() != 93 || a.State.Nonce != 1 || b.State.Balance.Int64() != 107 {
				t.Fatal("completed original executed twice")
			}
			recovered := core.NewTxPool()
			recovered.EnableIndexedMigrationAdmission()
			if err := recovered.EnableGroupMigrationQueueJournal(dir, accounts); err != nil {
				t.Fatal(err)
			}
			if err := recovered.ValidateIndexedMigrationAdmission(); err != nil {
				t.Fatal(err)
			}
			pending := recovered.SnapshotGroupMigrationTransactions(accounts)
			if len(pending) != 1 || pending[0].AdmissionID != unknown.AdmissionID {
				t.Fatal("recovery resurrected completion or dropped unseen work")
			}
		})
	}
}

func TestOrdinaryCompletionCannotUseCreditWithoutDebit(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	bc, command, _ := preparationFixture(t)
	defer bc.CloseBlockChain()
	tx := indexedAdmission("0000000000000000000000000000000000000030", command.Account, 7, 23)
	credit := core.DecodeTx(tx.Encode())
	credit.Relayed = true
	committedIndexedNative(t, bc, credit)
	if err := bc.Txpool.EnableGroupMigrationQueueJournal(t.TempDir(), []string{command.Account}); err != nil {
		t.Fatal(err)
	}
	bc.Txpool.AddTx2Pool(tx)
	if removed, err := bc.ConfirmSourceAppliedCredits([]*core.Transaction{tx}); err == nil || removed != 0 || bc.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("credit-only evidence cleared ordinary transaction", removed, err)
	}
}

func TestSourceCreditCompletionUsesCommittedCreditAndDoesNotChangeState(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "true")
	oldRoot, oldDB := params.ExpDataRootDir, params.DatabaseWrite_path
	t.Cleanup(func() { params.ExpDataRootDir, params.DatabaseWrite_path = oldRoot, oldDB })
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "database") + "/"
	bc := newMigrationChain(t, params.DatabaseWrite_path, 0)
	defer bc.CloseBlockChain()
	accounts := []string{"0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020"}
	bc.AddAccounts(accounts, []*core.AccountState{{Balance: big.NewInt(100)}, {Balance: big.NewInt(100)}}, 0)
	tx := core.NewTransaction("0000000000000000000000000000000000000030", accounts[0], big.NewInt(7), 9, time.Unix(1, 0))
	tx.AdmissionID, tx.IngressSequence = "run/9", 9
	tx.Relayed, tx.RoutingFrozen = true, true
	tx.RoutedSenderShard, tx.RoutedRecipientShard = 1, 0
	// Use the actual state-update path and commit its root before any cleanup.
	root := bc.GetUpdateStatusTrie([]*core.Transaction{tx})
	st, err := trie.New(trie.TrieID(root), bc.triedb)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = bc.commitStateTrie(st); err != nil {
		t.Fatal(err)
	}
	unknown := core.DecodeTx(tx.Encode())
	unknown.AdmissionID, unknown.IngressSequence = "run/10", 10
	bc.Txpool.AddTx2Pool(tx)
	bc.Txpool.AddTx2Pool(core.DecodeTx(tx.Encode()))
	bc.Txpool.AddTx2Pool(unknown)
	if err = bc.Txpool.EnableGroupMigrationQueueJournal(t.TempDir(), accounts); err != nil {
		t.Fatal(err)
	}
	head, stateRoot := append([]byte(nil), bc.CurrentBlock.Hash...), append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
	if removed, err := bc.ConfirmSourceAppliedCredits([]*core.Transaction{tx, unknown}); err != nil || removed != 2 {
		t.Fatal("committed credit cleanup failed", removed, err)
	}
	if !bytes.Equal(head, bc.CurrentBlock.Hash) || !bytes.Equal(stateRoot, bc.CurrentBlock.Header.StateRoot) {
		t.Fatal("read-only completion changed ledger")
	}
	state, err := bc.ExportAccountState(accounts[0])
	if err != nil || state.State.Balance.Int64() != 107 || bc.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("balance or unknown admission changed", err)
	}
}
