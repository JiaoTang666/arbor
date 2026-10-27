package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"errors"
	"math/big"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// Faults are enabled only after genesis. Embedding the real in-memory database
// retains production trie/batch behavior without a separate fake trie engine.
type contractWriteFailureDB struct {
	ethdb.Database
	fail bool
}

func (db *contractWriteFailureDB) Put(key, value []byte) error {
	if db.fail {
		return errors.New("injected state persistence failure")
	}
	return db.Database.Put(key, value)
}

func (db *contractWriteFailureDB) NewBatch() ethdb.Batch {
	return &contractWriteFailureBatch{Batch: db.Database.NewBatch(), db: db}
}

func (db *contractWriteFailureDB) NewBatchWithSize(size int) ethdb.Batch {
	return &contractWriteFailureBatch{Batch: db.Database.NewBatchWithSize(size), db: db}
}

type contractWriteFailureBatch struct {
	ethdb.Batch
	db *contractWriteFailureDB
}

func (batch *contractWriteFailureBatch) Write() error {
	if batch.db.fail {
		return errors.New("injected state batch persistence failure")
	}
	return batch.Batch.Write()
}

func contractUnchangedHead(t *testing.T, bc *BlockChain, head *core.Block) {
	t.Helper()
	if bc.CurrentBlock != head {
		t.Fatal("rejected execution advanced the in-memory chain head")
	}
	stored, err := bc.Storage.GetNewestBlockHash()
	if err != nil || !bytes.Equal(stored, head.Hash) {
		t.Fatal("rejected execution advanced the durable chain head", err)
	}
}

func contractAccount(t *testing.T, bc *BlockChain, account string, balance *big.Int, nonce uint64) {
	t.Helper()
	state, err := bc.ExportAccountState(account)
	if err != nil || state.State.Balance.Cmp(balance) != 0 || state.State.Nonce != nonce {
		t.Fatal("unexpected committed account", account, state, err)
	}
}

func TestBlockExecutionContractStateFailureAllModes(t *testing.T) {
	for _, mode := range []string{"baseline", "ownership", "forecast", "epoch", "indexed"} {
		for _, fault := range []string{"missing_root", "persist_failure"} {
			t.Run(mode+"/"+fault, func(t *testing.T) {
				ownershipSecurityFixture(t)
				t.Setenv(NativeEffectIndexEnv, "false")
				if mode != "ownership" {
					t.Setenv("SAA_OWNERSHIP_SECURITY_V1", "false")
					for _, key := range []string{"SAA_OWNERSHIP_CONFIG_PATH", "SAA_OWNERSHIP_CONFIG_SHA256", "SAA_OWNERSHIP_CERTIFICATES_PATH", "SAA_OWNERSHIP_CERTIFICATES_SHA256", "SAA_OWNERSHIP_RUNTIME_JOURNAL_PATH"} {
						t.Setenv(key, "")
					}
				}
				switch mode {
				case "forecast":
					t.Setenv("SAA_INTEGRATED_CONTROLLER", "forecast")
				case "epoch":
					t.Setenv("BLOCK_EMULATOR_CLPA_EPOCH_V1", "1")
				case "indexed":
					t.Setenv(NativeEffectIndexEnv, "true")
				}
				params.DatabaseWrite_path = t.TempDir() + string(filepath.Separator)
				db := &contractWriteFailureDB{Database: rawdb.NewMemoryDatabase()}
				bc, err := NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 4, BlockSize: 4}, db)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(bc.CloseBlockChain)
				head := bc.CurrentBlock
				originalRoot := append([]byte(nil), head.Header.StateRoot...)
				tx := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 1)
				tx.AdmissionID = "state-failure/1"
				bc.SendTx2Pool([]*core.Transaction{tx})
				if fault == "missing_root" {
					head.Header.StateRoot = bytes.Repeat([]byte{0x77}, 32)
				} else {
					db.fail = true
				}
				if fault == "missing_root" {
					requireSecurityPanic(t, func() { bc.GenerateBlock(0) })
				} else {
					proposal := bc.GenerateBlock(0)
					contractUnchangedHead(t, bc, head)
					contractAccount(t, bc, securityOldAccount, big.NewInt(100), 0)
					if err := bc.AddBlock(proposal); err == nil {
						t.Fatal("state persistence failure was accepted as committed execution")
					}
				}
				db.fail = false
				head.Header.StateRoot = originalRoot
				contractUnchangedHead(t, bc, head)
				contractAccount(t, bc, securityOldAccount, big.NewInt(100), 0)
				if _, err := bc.ExportAccountState(securityLocalNew); err == nil {
					t.Fatal("failed proposal published a recipient state")
				}
			})
		}
	}
}

func TestBlockExecutionContractRejectsAuthorizedExistingWrongRoot(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "false")
	bc := ownershipSecurityFixture(t)(0)
	head := bc.CurrentBlock
	tx := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 1)
	block := core.NewBlock(&core.BlockHeader{Number: 1, ParentBlockHash: head.Hash,
		StateRoot: append([]byte(nil), head.Header.StateRoot...), TxRoot: GetTxTreeRoot([]*core.Transaction{tx})}, []*core.Transaction{tx})
	block.ExecutionResults = []bool{true}
	block.Hash = block.Header.Hash()
	if err := bc.IsValidBlock(block); err == nil {
		t.Fatal("authorized transaction accepted an existing but incorrect state root")
	}
	if err := bc.AddBlock(block); err == nil {
		t.Fatal("commit bypass accepted an existing but incorrect state root")
	}
	contractUnchangedHead(t, bc, head)
	contractAccount(t, bc, securityOldAccount, big.NewInt(100), 0)
	if _, err := bc.ExportAccountState(securityLocalNew); err == nil {
		t.Fatal("wrong-root rejection published recipient creation")
	}
}

func TestBlockExecutionContractReportsOnlySuccessfulStages(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "false")
	bc := ownershipSecurityFixture(t)(0)
	local := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 1)
	cross := nativeOwnerTx(securityOldAccount, securityRemoteNew, 0, 1, 2)
	bad := nativeOwnerTx(securityOldAccount, securityOtherNew, 0, 1, 3)
	bad.Value = big.NewInt(101)
	bc.SendTx2Pool([]*core.Transaction{local, cross, bad})
	block := bc.GenerateBlock(0)
	if !reflect.DeepEqual(block.ExecutionResults, []bool{true, true, false}) {
		t.Fatal("execution outcomes confused block occupancy with success", block.ExecutionResults)
	}
	if _, err := bc.ExecutedNativeTransactions(block); err == nil {
		t.Fatal("uncommitted proposal was reported as committed service")
	}
	if err := bc.AddBlock(block); err != nil {
		t.Fatal(err)
	}
	before := block.Encode()
	executed, err := bc.ExecutedNativeTransactions(block)
	if err != nil || len(executed) != 2 || !bytes.Equal(executed[0].TxHash, local.TxHash) || !bytes.Equal(executed[1].TxHash, cross.TxHash) {
		t.Fatal("failed debit was eligible for Relay or success reporting", executed, err)
	}
	executed[1].Relayed = true
	executed[0].Value.SetInt64(99)
	if !bytes.Equal(before, block.Encode()) {
		t.Fatal("classification mutated the committed block or its big.Int payload")
	}
	// Reporting must survive eviction and trust the atomically stored outcomes,
	// not the result mask supplied by a caller with an otherwise identical block.
	bc.committedExecution = nil
	caller := core.DecodeB(block.Encode())
	caller.ExecutionResults = []bool{false, false, true}
	reported, err := bc.ExecutedNativeTransactions(caller)
	if err != nil || len(reported) != 2 || !bytes.Equal(reported[0].TxHash, local.TxHash) || !bytes.Equal(reported[1].TxHash, cross.TxHash) {
		t.Fatal("caller result bits replaced locally committed execution outcomes", reported, err)
	}
	contractAccount(t, bc, securityOldAccount, big.NewInt(98), 2)
	for _, remote := range []string{securityRemoteNew, securityOtherNew} {
		if _, err := bc.ExportAccountState(remote); err == nil {
			t.Fatal("source debit created a remote recipient", remote)
		}
	}
}

func TestBlockExecutionContractReplicaLocalSelfAndRelay(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "false")
	open := ownershipSecurityFixture(t)
	leader, follower, target, targetFollower := open(0), open(0), open(1), open(1)
	local := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 1)
	cross := nativeOwnerTx(securityOldAccount, securityRemoteNew, 0, 1, 2)
	self := nativeOwnerTx(securityOldAccount, securityOldAccount, 0, 0, 3)
	leader.SendTx2Pool([]*core.Transaction{local, cross, self})
	block := leader.GenerateBlock(0)
	for _, bc := range []*BlockChain{leader, follower} {
		copy := core.DecodeB(block.Encode())
		if err := bc.IsValidBlock(copy); err != nil {
			t.Fatal("valid proposal rejected", err)
		}
		contractAccount(t, bc, securityOldAccount, big.NewInt(100), 0)
		if _, err := bc.ExportAccountState(securityLocalNew); err == nil {
			t.Fatal("proposal validation published uncommitted creation")
		}
		if err := bc.AddBlock(copy); err != nil {
			t.Fatal(err)
		}
		contractAccount(t, bc, securityOldAccount, big.NewInt(98), 3)
		contractAccount(t, bc, securityLocalNew, new(big.Int).Add(params.Init_Balance, big.NewInt(1)), 0)
	}
	if !bytes.Equal(leader.CurrentBlock.Header.StateRoot, follower.CurrentBlock.Header.StateRoot) {
		t.Fatal("leader and independently replayed follower roots differ")
	}
	relay := core.DecodeTx(cross.Encode())
	relay.Relayed = true
	target.SendTx2Pool([]*core.Transaction{relay})
	credit := target.GenerateBlock(0)
	for _, bc := range []*BlockChain{target, targetFollower} {
		copy := core.DecodeB(credit.Encode())
		if err := bc.IsValidBlock(copy); err != nil {
			t.Fatal(err)
		}
		if err := bc.AddBlock(copy); err != nil {
			t.Fatal(err)
		}
		contractAccount(t, bc, securityRemoteNew, new(big.Int).Add(params.Init_Balance, big.NewInt(1)), 0)
		owner, err := bc.NativeFirstOwner(securityRemoteNew)
		if err != nil || owner == nil || owner.Role != "recipient" || owner.Shard != 1 {
			t.Fatal("Relay2 creation provenance changed", owner, err)
		}
		if _, err := bc.ExportAccountState(securityOldAccount); err == nil {
			t.Fatal("Relay2 spuriously created its remote sender")
		}
	}
	if !bytes.Equal(target.CurrentBlock.Header.StateRoot, targetFollower.CurrentBlock.Header.StateRoot) {
		t.Fatal("Relay2 replay roots differ")
	}
}

func TestBlockExecutionContractCacheBindsOrderedPayload(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "false")
	bc := ownershipSecurityFixture(t)(0)
	head := bc.CurrentBlock
	first := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 1)
	second := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 2)
	first.Value, second.Value = big.NewInt(30), big.NewInt(80)
	bc.SendTx2Pool([]*core.Transaction{first, second})
	proposal := bc.GenerateBlock(0)
	if err := bc.IsValidBlock(proposal); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"order", "value", "multiplicity", "result_mask"} {
		t.Run(change, func(t *testing.T) {
			bad := core.DecodeB(proposal.Encode())
			switch change {
			case "order":
				bad.Body[0], bad.Body[1] = bad.Body[1], bad.Body[0]
			case "value":
				bad.Body[0].Value = big.NewInt(20)
				bad.ExecutionResults = []bool{true, true}
			case "multiplicity":
				bad.Body = append(bad.Body, core.DecodeTx(bad.Body[0].Encode()))
				bad.ExecutionResults = []bool{true, false, true}
			case "result_mask":
				bad.ExecutionResults = []bool{false, true}
			}
			if !bytes.Equal(GetTxTreeRoot(bad.Body), proposal.Header.TxRoot) {
				t.Fatal("fixture must preserve the legacy unordered transaction root")
			}
			if err := bc.IsValidBlock(bad); err == nil {
				t.Fatal("changed ordered execution input reused an unrelated cached root")
			}
			if err := bc.AddBlock(bad); err == nil {
				t.Fatal("commit accepted a mutated execution input with the original root")
			}
			contractUnchangedHead(t, bc, head)
			contractAccount(t, bc, securityOldAccount, big.NewInt(100), 0)
		})
	}
	if err := bc.AddBlock(proposal); err != nil {
		t.Fatal("rejected alternatives poisoned the original valid proposal", err)
	}
	contractAccount(t, bc, securityOldAccount, big.NewInt(70), 1)
}

func TestBlockExecutionContractBlockStorageFailureDoesNotAdvance(t *testing.T) {
	t.Setenv(NativeEffectIndexEnv, "false")
	bc := ownershipSecurityFixture(t)(0)
	head := bc.CurrentBlock
	bc.SendTx2Pool([]*core.Transaction{nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 1)})
	proposal := bc.GenerateBlock(0)
	contractUnchangedHead(t, bc, head)
	if err := bc.Storage.DataBase.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bc.AddBlock(proposal); err == nil || bc.CurrentBlock != head {
		t.Fatal("failed block storage advanced the ledger head", err)
	}
	// The database was intentionally closed: do not mistake a failed head read
	// from that closed database for evidence that the durable head was changed.
	contractAccount(t, bc, securityOldAccount, big.NewInt(100), 0)
	if _, err := bc.ExportAccountState(securityLocalNew); err == nil {
		t.Fatal("unreferenced persisted trie became committed account state")
	}
}
