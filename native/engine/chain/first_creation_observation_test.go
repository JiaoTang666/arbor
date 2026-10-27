package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func creationObservationChains(t *testing.T) (*BlockChain, *BlockChain) {
	t.Helper()
	root, db, shards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	params.ExpDataRootDir = t.TempDir()
	params.DatabaseWrite_path = filepath.Join(params.ExpDataRootDir, "database") + "/"
	params.ShardNum = 2
	a := newMigrationChain(t, params.DatabaseWrite_path, 0)
	b := newMigrationChain(t, params.DatabaseWrite_path, 1)
	t.Cleanup(func() {
		a.CloseBlockChain()
		b.CloseBlockChain()
		params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum = root, db, shards
	})
	return a, b
}

func creationTx() (*core.Transaction, CreatorExpectation) {
	tx := core.NewTransaction(strings.Repeat("a", 40), strings.Repeat("b", 40), big.NewInt(1), 0, time.Now())
	tx.AdmissionID = "creation-run/1"
	tx.RoutingFrozen = true
	tx.MappingVersion = 7
	tx.RoutedSenderShard, tx.RoutedRecipientShard = 0, 1
	return tx, CreatorExpectation{AdmissionID: tx.AdmissionID, TxHash: hex.EncodeToString(tx.TxHash), Account: tx.Recipient, Shard: 1, MappingVersion: 7}
}

func TestFirstCreationObservationRelayCommitBoundary(t *testing.T) {
	source, target := creationObservationChains(t)
	tx, expected := creationTx()
	source.SendTx2Pool([]*core.Transaction{tx})
	first := source.GenerateBlock(0)
	source.AddBlock(first)
	wrong := expected
	wrong.Shard = 0
	if _, e := source.ObserveFirstCreation(first.Hash, wrong, time.Now()); e == nil {
		t.Fatal("source relay1 certified target creation")
	}
	relay := core.DecodeTx(tx.Encode())
	relay.Relayed = true
	target.SendTx2Pool([]*core.Transaction{relay})
	second := target.GenerateBlock(0)
	if _, e := target.ObserveFirstCreation(second.Hash, expected, time.Now()); e == nil {
		t.Fatal("proposal certified as committed")
	}
	target.AddBlock(second)
	observation, e := target.ObserveFirstCreation(second.Hash, expected, time.Now())
	if e != nil || observation.StateHash == "" || observation.StateRoot == observation.ParentStateRoot {
		t.Fatal(observation, e)
	}
	if source.ObserveAccountPresence(expected.Account, time.Now()).Status != AccountAbsent {
		t.Fatal("recipient exists on source")
	}
	for name, mutate := range map[string]func(*CreatorExpectation){
		"wrong_occurrence": func(x *CreatorExpectation) { x.AdmissionID = "creation-run/2" },
		"wrong_hash":       func(x *CreatorExpectation) { x.TxHash = strings.Repeat("0", 64) },
		"wrong_version":    func(x *CreatorExpectation) { x.MappingVersion++ },
		"wrong_shard":      func(x *CreatorExpectation) { x.Shard = 0 },
		"wrong_account":    func(x *CreatorExpectation) { x.Account = strings.Repeat("c", 40) },
		"noncanonical":     func(x *CreatorExpectation) { x.Account = "0x" + x.Account },
	} {
		t.Run(name, func(t *testing.T) {
			x := expected
			mutate(&x)
			if _, e := target.ObserveFirstCreation(second.Hash, x, time.Now()); e == nil {
				t.Fatal("invalid binding accepted")
			}
		})
	}
	if _, e := target.ObserveFirstCreation(second.Hash, expected, time.Time{}); e == nil {
		t.Fatal("missing observation time accepted")
	}
	// A read retry is idempotent in identity/root/hash, but not its timestamp.
	again, e := target.ObserveFirstCreation(second.Hash, expected, time.Now())
	if e != nil || again.StateHash != observation.StateHash || again.BlockHash != observation.BlockHash {
		t.Fatal("retry changed committed evidence", e)
	}
	if path := os.Getenv("SAA_CREATION_OBSERVATION_EVIDENCE"); path != "" {
		f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			t.Fatal(e)
		}
		e = json.NewEncoder(f).Encode(map[string]interface{}{"status": "PASS", "observation": observation, "scope": "two real chain instances; direct AddBlock; no TCP, PBFT voting, authority activation or HOLD release"})
		ce := f.Close()
		if e != nil || ce != nil {
			t.Fatal(e, ce)
		}
	}
}

func TestFirstCreationObservationRejectsExistingAndAmbiguous(t *testing.T) {
	for _, mode := range []string{"existing", "ambiguous", "no_state"} {
		t.Run(mode, func(t *testing.T) {
			source, _ := creationObservationChains(t)
			tx, expected := creationTx()
			tx.RoutedRecipientShard = 0
			expected.Shard = 0
			if mode == "existing" {
				source.Update_PartitionMap(expected.Account, 0)
				source.AddAccounts([]string{expected.Account}, []*core.AccountState{{Balance: big.NewInt(2)}}, 0)
			}
			txs := []*core.Transaction{tx}
			if mode == "ambiguous" {
				other := core.DecodeTx(tx.Encode())
				other.AdmissionID = "creation-run/2"
				txs = append(txs, other)
			}
			if mode == "no_state" {
				tx.Value = new(big.Int).Add(params.Init_Balance, big.NewInt(1))
			}
			source.SendTx2Pool(txs)
			block := source.GenerateBlock(0)
			source.AddBlock(block)
			_, e := source.ObserveFirstCreation(block.Hash, expected, time.Now())
			if e == nil {
				t.Fatal("invalid creation certified", mode)
			}
			want := map[string]string{"existing": "account_already_existed", "ambiguous": "missing_or_ambiguous_creator", "no_state": "account_not_created"}[mode]
			if !strings.Contains(e.Error(), want) {
				t.Fatal("unexpected failure", e)
			}
		})
	}
}
