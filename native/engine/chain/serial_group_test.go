package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"
)

func serialGroupCommand(t *testing.T, bc *BlockChain, id string, accounts []string, version uint64) core.GroupPrepareCommand {
	t.Helper()
	var c core.GroupPrepareCommand
	for _, account := range accounts {
		state, err := bc.ExportAccountState(account)
		if err != nil {
			t.Fatal(err)
		}
		c.Members = append(c.Members, core.SourcePrepareCommand{ProposalID: id, Account: account, SourceShard: 0, TargetShard: 1, MappingVersion: version,
			ExpectedBlockHash: hex.EncodeToString(bc.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(state.SourceRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)})
	}
	return c
}

func serialActivate(t *testing.T, target *BlockChain, p *core.GroupPreparation, seq uint64) GroupTargetActivation {
	t.Helper()
	record, err := target.CommitGroupTargetInstallation(*p, seq, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	a := GroupTargetActivation{}
	for _, m := range p.Members {
		a.Members = append(a.Members, TargetActivation{TargetShard: 1, ProposalID: m.Command.ProposalID, Account: m.Command.Account, MappingVersion: m.Command.MappingVersion + 1, InstallDigest: record.CommitDigest, StateHash: m.StateHash})
	}
	if err = target.CommitGroupTargetActivation(a); err != nil {
		t.Fatal(err)
	}
	return a
}

func serialRetire(t *testing.T, source, target *BlockChain, a GroupTargetActivation, seq uint64) {
	t.Helper()
	receipt, _ := json.Marshal(map[string]interface{}{"record": a, "committed": true, "block": target.CurrentBlock.Header.Number, "block_hash": hex.EncodeToString(target.CurrentBlock.Hash), "state_root": hex.EncodeToString(target.CurrentBlock.Header.StateRoot), "shard": 1})
	var accounts []string
	for _, m := range a.Members {
		accounts = append(accounts, m.Account)
	}
	drain, _ := json.Marshal(map[string]interface{}{"Accounts": accounts, "Account": "", "Kind": "full", "Sequence": seq, "Transactions": [][]byte{}})
	if _, err := source.CommitGroupRetirement(GroupRetirementCommand{ActivationReceipt: receipt, DrainSnapshot: drain}, seq, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
}

func serialChainFixture(t *testing.T) (*BlockChain, *BlockChain, string, []string) {
	t.Helper()
	source, _, path := preparationFixture(t)
	accounts := []string{"0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020", "0000000000000000000000000000000000000030", "0000000000000000000000000000000000000040"}
	source.Update_PartitionMap(accounts[3], 0)
	source.AddAccounts([]string{accounts[3]}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	target := newMigrationChain(t, path, 1)
	for _, a := range accounts {
		target.Update_PartitionMap(a, 0)
	}
	return source, target, path, accounts
}

func TestSerialGroupsSameSourceTargetRestoreAllFences(t *testing.T) {
	source, target, path, accounts := serialChainFixture(t)
	first := serialGroupCommand(t, source, "round-1", accounts[:2], 0)
	p, err := source.CommitGroupSourcePreparation(first, 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	a := serialActivate(t, target, p, 1)
	next := serialGroupCommand(t, source, "round-2", accounts[2:], 1)
	if err = source.ValidateGroupSourcePreparation(next); err == nil {
		t.Fatal("second group accepted before first retirement")
	}
	serialRetire(t, source, target, a, 3)
	next = serialGroupCommand(t, source, "round-2", accounts[2:], 1)
	q, err := source.CommitGroupSourcePreparation(next, 4, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	b := serialActivate(t, target, q, 3)
	// Wrong-generation receipt cannot retire the current group through an ID
	// substitution, and historical idempotent activation cannot advance the head.
	bad := b
	bad.Members = append([]TargetActivation(nil), b.Members...)
	bad.Members[0].MappingVersion = 1
	if err = target.ValidateGroupTargetActivation(bad); err == nil {
		t.Fatal("wrong generation accepted")
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	if err = target.CommitGroupTargetActivation(a); err != nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("old activation changed current state", err)
	}
	serialRetire(t, source, target, b, 5)
	source.CloseBlockChain()
	target.CloseBlockChain()
	source = newMigrationChain(t, path, 0)
	defer source.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	if len(source.GroupSourcePreparations()) != 2 {
		t.Fatal("historical source group lost")
	}
	for _, id := range []string{"round-1", "round-2"} {
		if source.GroupSourcePreparation(id) == nil {
			t.Fatal("missing source record", id)
		}
		if r, e := source.GroupSourceRetirement(id); e != nil || r == nil {
			t.Fatal("missing retirement", id, e)
		}
		if r, e := target.GroupTargetInstallation(id); e != nil || r == nil {
			t.Fatal("missing target record", id, e)
		}
	}
	for _, account := range accounts {
		tx := core.NewTransaction(account, account, big.NewInt(1), 1, time.Unix(1, 0))
		tx.RoutingFrozen = true
		if source.CheckPreparedTransactions([]*core.Transaction{tx}) == nil {
			t.Fatal("old source fence lost", account)
		}
		if ok, e := target.TargetActivated(account); e != nil || !ok {
			t.Fatal("target activation lost", account, e)
		}
	}
	if err = source.checkPreparedRoot(source.CurrentBlock.Header.StateRoot); err != nil {
		t.Fatal(err)
	}
	// Late first-group outgoing work still binds to first-group installation,
	// even though the target's latest installation belongs to round two.
	tx := core.NewTransaction(accounts[0], accounts[1], big.NewInt(1), 10, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, err := core.BuildGroupMigrationHandoff(tx, "round-1", accounts[:2], 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = target.CommitMigrationHandoff(*h); err != nil {
		t.Fatal("old group handoff lost after new installation", err)
	}
}

// This is an explicit UNSUPPORTED-path regression, not evidence of serial
// end-to-end success. Removing the source fence to make this pass is unsafe.
func TestSerialCrossGroupRelayRemainsUnsupported(t *testing.T) {
	source, target, _, accounts := serialChainFixture(t)
	defer source.CloseBlockChain()
	defer target.CloseBlockChain()
	p, err := source.CommitGroupSourcePreparation(serialGroupCommand(t, source, "first", accounts[:2], 0), 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	a := serialActivate(t, target, p, 1)
	serialRetire(t, source, target, a, 3)
	tx := core.NewTransaction(accounts[0], accounts[2], big.NewInt(7), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, err := core.BuildGroupMigrationHandoff(tx, "first", accounts[:2], 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	firstLeg, err := target.CommitMigrationHandoff(*h)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstLeg.Relay) == 0 {
		t.Fatal("missing first-group credit obligation")
	}
	if err = source.ValidateHandoffRelay(*firstLeg); err != nil {
		t.Fatal("relay was not executable before second group", err)
	}
	// An ordinary queued first-group transaction may reach the old source after
	// its historical drain. Retirement is not a permanent no-more-old-work proof.
	q, err := source.CommitGroupSourcePreparation(serialGroupCommand(t, source, "second", accounts[2:], 1), 4, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	serialActivate(t, target, q, 3)
	head := append([]byte(nil), source.CurrentBlock.Hash...)
	_, err = source.CommitHandoffRelay(*firstLeg)
	if err == nil || !strings.Contains(err.Error(), "prepared group") {
		t.Fatalf("expected protected old-destination rejection, got %v", err)
	}
	if !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("rejected relay changed source state")
	}
	state, err := target.ExportAccountState(accounts[2])
	if err != nil || state.State.Balance.Int64() != 100 {
		t.Fatal("credit applied without valid redirect", err)
	}
	t.Log("NATIVE MULTI-ROUND NOT READY: first-group credit still addresses second-group frozen source; validated redirect/ACK lineage is missing")
}
