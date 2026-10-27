package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/hex"
	"fmt"
	"math/big"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

// These tests exercise native block construction and committed state, not PBFT
// votes, network relay authentication, migration, or performance.
func completionChainFixture(t *testing.T, mode string) *BlockChain {
	t.Helper()
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	oldBytes, oldBalance := params.UseBlocksizeInBytes, params.Init_Balance
	t.Cleanup(func() {
		params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum = oldRoot, oldDB, oldShards
		params.UseBlocksizeInBytes, params.Init_Balance = oldBytes, oldBalance
	})
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "database") + string(filepath.Separator)
	params.ShardNum, params.UseBlocksizeInBytes, params.Init_Balance = 2, 0, big.NewInt(100)
	t.Setenv(NativeEffectIndexEnv, "true")
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", "")
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", "")
	t.Setenv("BLOCK_EMULATOR_COMPLETION_SCHEDULER", "")
	bc, err := NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 1}, rawdb.NewMemoryDatabase())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bc.CloseBlockChain)
	if mode != "fifo" {
		if err := bc.Txpool.EnableCompletionScheduling(core.CompletionScheduleConfig{Mode: mode, LookaheadBlocks: 8, FairEvery: 100}, 0, 2); err != nil {
			t.Fatal(err)
		}
	}
	return bc
}

func completionNativeTx(sender, recipient string, value int64, sequence, source, target uint64, relay bool) *core.Transaction {
	tx := core.NewTransaction(sender, recipient, big.NewInt(value), sequence, time.Unix(1, 0))
	tx.AdmissionID, tx.IngressSequence, tx.IngressSequenceAssigned = fmt.Sprintf("completion/%d", sequence), sequence, true
	tx.RoutingFrozen, tx.RoutedSenderShard, tx.RoutedRecipientShard, tx.MappingVersion = true, source, target, 3
	tx.Relayed = relay
	return tx
}

func completionAccount(id int) string { return fmt.Sprintf("%040x", id) }

func completionCommitPool(t *testing.T, bc *BlockChain) []string {
	t.Helper()
	order := []string{}
	for remaining := bc.Txpool.GetTxQueueLen(); remaining > 0; remaining = bc.Txpool.GetTxQueueLen() {
		block := bc.GenerateBlock(0)
		if len(block.Body) != 1 {
			t.Fatalf("nonempty executable pool did not fill its one-slot block: %d", len(block.Body))
		}
		if err := bc.AddBlock(block); err != nil {
			t.Fatal(err)
		}
		order = append(order, block.Body[0].AdmissionID)
		if bc.Txpool.GetTxQueueLen() >= remaining {
			t.Fatal("native packing did not advance the pending queue")
		}
	}
	return order
}

type completionBusinessAccount struct {
	Exists                     bool
	Balance                    string
	Nonce, Shard, Mapping, Seq uint64
	CreatorHash, CreatorRole   string
}

func completionBusinessSnapshot(t *testing.T, bc *BlockChain, accounts []string) map[string]completionBusinessAccount {
	t.Helper()
	st, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]completionBusinessAccount{}
	for _, account := range accounts {
		raw, err := st.Get([]byte(account))
		if err != nil {
			t.Fatal(err)
		}
		owner, err := bc.NativeFirstOwner(account)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) == 0 {
			if owner != nil {
				t.Fatal("absent state has a first-owner record", account)
			}
			result[account] = completionBusinessAccount{}
			continue
		}
		state := core.DecodeAS(raw)
		if owner == nil || owner.CreationHeight == 0 || owner.CreationHeight > bc.CurrentBlock.Header.Number {
			t.Fatal("created account lacks valid committed provenance", account, owner)
		}
		// Block height may change under legitimate cross-block scheduling. The
		// creator identity and business state, unlike the full root, must agree.
		result[account] = completionBusinessAccount{true, state.Balance.String(), state.Nonce,
			owner.Shard, owner.MappingVersion, owner.IngressSequence, owner.TxHash, owner.Role}
	}
	return result
}

func completionEffects(t *testing.T, bc *BlockChain, txs []*core.Transaction) map[string]core.TransactionEffect {
	t.Helper()
	st, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]core.TransactionEffect{}
	for _, tx := range txs {
		for _, kind := range []string{core.TransactionDebit, core.TransactionCredit} {
			want := kind == core.TransactionDebit && !tx.Relayed || kind == core.TransactionCredit && tx.RoutedRecipientShard == 0
			status, err := LookupTransactionEffect(st, tx, kind)
			if err != nil || want && status != EffectSeen || !want && status != EffectUnseen {
				t.Fatal("wrong native stage effect coverage", tx.AdmissionID, kind, status, err)
			}
			if !want {
				continue
			}
			receipt, err := ReadTransactionEffectReceipt(st, tx, kind)
			identity, identityErr := core.BuildTransactionEffect(tx, kind)
			if err != nil || identityErr != nil || receipt == nil || receipt.Effect != identity || receipt.Block == 0 || receipt.Block > bc.CurrentBlock.Header.Number {
				t.Fatal("committed effect receipt disagrees with its admission", receipt, err, identityErr)
			}
			result[identity.ID] = identity
		}
	}
	return result
}

func TestCompletionSchedulingPreservesNativeBusinessState(t *testing.T) {
	a, b, c, d := completionAccount(0x10), completionAccount(0x20), completionAccount(0x30), completionAccount(0x40)
	e, f, g := completionAccount(0x50), completionAccount(0x60), completionAccount(0x70)
	m, n, p, q := completionAccount(0x11), completionAccount(0x21), completionAccount(0x31), completionAccount(0x41)
	txs := []*core.Transaction{
		completionNativeTx(a, m, 5, 0, 0, 1, false),
		completionNativeTx(c, b, 60, 1, 0, 0, false),
		completionNativeTx(b, n, 150, 2, 0, 1, false), // Cannot debit before the local funding credit.
		completionNativeTx(p, d, 9, 3, 1, 0, true),
		completionNativeTx(d, e, 108, 4, 0, 0, false), // Cannot debit before the admitted Relay2 credit.
		completionNativeTx(q, d, 11, 5, 1, 0, true),
		completionNativeTx(f, g, 3, 6, 0, 0, false),
	}
	accounts := []string{a, b, c, d, e, f, g, m, n, p, q}
	var baseline map[string]completionBusinessAccount
	var baselineEffects map[string]core.TransactionEffect
	for _, mode := range []string{"fifo", "phase_priority", "completion_frontier"} {
		t.Run(mode, func(t *testing.T) {
			bc := completionChainFixture(t, mode)
			originals := make([]*core.Transaction, len(txs))
			for i, tx := range txs {
				originals[i] = core.DecodeTx(tx.Encode())
			}
			// Relay2 objects represent already admitted relay work; this native
			// state test does not certify their remote source or network ingress.
			bc.SendTx2Pool(originals)
			order := completionCommitPool(t, bc)
			if mode != "fifo" && order[0] == txs[0].AdmissionID {
				t.Fatal("fixture did not exercise non-FIFO block selection")
			}
			state := completionBusinessSnapshot(t, bc, accounts)
			for account, expected := range map[string]struct {
				balance string
				nonce   uint64
			}{
				a: {"95", 1}, b: {"10", 1}, c: {"40", 1}, d: {"12", 1}, e: {"208", 0}, f: {"97", 1}, g: {"103", 0},
			} {
				if got := state[account]; !got.Exists || got.Balance != expected.balance || got.Nonce != expected.nonce || got.Shard != 0 || got.Mapping != 3 {
					t.Fatal("wrong final business account", account, got, expected)
				}
			}
			for account, creator := range map[string]int{a: 0, b: 1, c: 1, d: 3, e: 4, f: 6, g: 6} {
				if got := state[account]; got.CreatorHash != hex.EncodeToString(txs[creator].TxHash) || got.Seq != txs[creator].IngressSequence {
					t.Fatal("scheduling changed the original creator", account, got)
				}
			}
			for _, remote := range []string{m, n, p, q} {
				if state[remote].Exists {
					t.Fatal("source leg or Relay2 wrote a nonlocal endpoint", remote)
				}
			}
			effects := completionEffects(t, bc, originals)
			if len(effects) != 10 {
				t.Fatal("wrong unique local effect count", len(effects))
			}
			if mode == "fifo" {
				baseline, baselineEffects = state, effects
			} else if !reflect.DeepEqual(baseline, state) || !reflect.DeepEqual(baselineEffects, effects) {
				t.Fatal("non-FIFO scheduling changed business state or effect identities")
			}
			// Replayed inner and Relay2 admissions occupy blocks but must produce
			// no additional debit, credit, nonce, or creator changes.
			for _, index := range []int{1, 3} {
				bc.SendTx2Pool([]*core.Transaction{core.DecodeTx(originals[index].Encode())})
				block := bc.GenerateBlock(0)
				if err := bc.AddBlock(block); err != nil {
					t.Fatal(err)
				}
				fresh, err := bc.ExecutedNativeTransactions(block)
				if err != nil || len(fresh) != 0 {
					t.Fatal("replay was incorrectly reported as new service", fresh, err)
				}
			}
			if !reflect.DeepEqual(state, completionBusinessSnapshot(t, bc, accounts)) || !reflect.DeepEqual(effects, completionEffects(t, bc, originals)) {
				t.Fatal("replay changed business state or the unique effect set")
			}
		})
	}
}

func TestCompletionSchedulingKeepsLocalArrivalBeforeOlderIngressRelay(t *testing.T) {
	a, b, remote := completionAccount(0x10), completionAccount(0x20), completionAccount(0x11)
	for _, mode := range []string{"fifo", "phase_priority", "completion_frontier"} {
		t.Run(mode, func(t *testing.T) {
			bc := completionChainFixture(t, mode)
			debit := completionNativeTx(a, b, 150, 100, 0, 0, false)
			laterCredit := completionNativeTx(remote, a, 100, 1, 1, 0, true)
			bc.SendTx2Pool([]*core.Transaction{debit, laterCredit})
			order := completionCommitPool(t, bc)
			if !reflect.DeepEqual(order, []string{debit.AdmissionID, laterCredit.AdmissionID}) {
				t.Fatal("global ingress sequence displaced a local predecessor", order)
			}
			state := completionBusinessSnapshot(t, bc, []string{a, b, remote})
			if got := state[a]; got.Balance != "200" || got.Nonce != 0 || got.CreatorRole != "recipient" || got.CreatorHash != hex.EncodeToString(laterCredit.TxHash) || state[b].Exists || state[remote].Exists {
				t.Fatal("late credit changed an earlier rejected debit or creator", state)
			}
			st, err := bc.stateTrie()
			if err != nil {
				t.Fatal(err)
			}
			status, err := LookupTransactionEffect(st, debit, core.TransactionDebit)
			if err != nil || status != EffectUnseen {
				t.Fatal("rejected predecessor acquired a debit effect", status, err)
			}
		})
	}
}
