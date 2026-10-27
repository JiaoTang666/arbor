package chain

import (
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"testing"
	"time"
)

func redirectFixture(t *testing.T) (*BlockChain, *BlockChain, string, []string, *HandoffExecution, *core.GroupPreparation) {
	t.Helper()
	source, target, path, accounts := serialChainFixture(t)
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
	first, err := target.CommitMigrationHandoff(*h)
	if err != nil {
		t.Fatal(err)
	}
	q, err := source.CommitGroupSourcePreparation(serialGroupCommand(t, source, "second", accounts[2:], 1), 4, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	return source, target, path, accounts, first, q
}

func TestRedirectCreditWaitsForActivationAndSurvivesRestart(t *testing.T) {
	source, target, path, accounts, first, q := redirectFixture(t)
	old, err := source.ExportAccountState(accounts[2])
	if err != nil {
		t.Fatal(err)
	}
	r, err := source.CommitRelayRedirect(*first)
	if err != nil {
		t.Fatal(err)
	}
	now, err := source.ExportAccountState(accounts[2])
	if err != nil || !bytes.Equal(old.State.Encode(), now.State.Encode()) {
		t.Fatal("redirect modified old balance", err)
	}
	if _, err = target.CommitRedirectCredit(*r); err == nil {
		t.Fatal("uninstalled recipient credited")
	}
	serialActivate(t, target, q, 3)
	receipt, err := target.CommitRedirectCredit(*r)
	if err != nil {
		t.Fatal(err)
	}
	state, err := target.ExportAccountState(accounts[2])
	if err != nil || state.State.Balance.Int64() != 107 {
		t.Fatal("credit not applied once", err)
	}
	if _, err = source.CommitHandoffRelay(*first); err == nil {
		t.Fatal("old path accepted after redirect reservation")
	}
	// Parent completion retains its identity and binds the actual child credit.
	executed := sha256.Sum256(r.Child.Execution)
	relayHash := sha256.Sum256(first.Relay)
	ack := core.HandoffAck{ID: r.Child.ID, ProposalID: r.Child.ProposalID, TargetShard: 1, ExecutionHash: hex.EncodeToString(executed[:]), Block: receipt.Block, BlockHash: hex.EncodeToString(target.CurrentBlock.Hash), StateRoot: hex.EncodeToString(target.CurrentBlock.Header.StateRoot), Executed: true}
	proof := core.RelayRedirectProof{ParentID: first.Handoff.ID, RelayHash: hex.EncodeToString(relayHash[:]), Child: r.Child, SourceShard: 0, SourceBlock: r.Block, SourceBlockHash: hex.EncodeToString(source.CurrentBlock.Hash), SourceStateRoot: hex.EncodeToString(source.CurrentBlock.Header.StateRoot), SourceCommitted: true, TargetAck: ack}
	final := core.HandoffRelayAck{RelayPayload: first.Relay, ID: first.Handoff.ID, RelayHash: proof.RelayHash, RecipientShard: 1, Block: ack.Block, BlockHash: ack.BlockHash, StateRoot: ack.StateRoot, Committed: true, Redirect: &proof}
	if err = core.ValidateHandoffRelayAck(&first.Handoff, final); err != nil {
		t.Fatal(err)
	}
	bad := proof
	bad.Child = r.Child
	bad.Child.MappingVersion = 1
	final.Redirect = &bad
	if core.ValidateHandoffRelayAck(&first.Handoff, final) == nil {
		t.Fatal("wrong generation accepted")
	}
	source.CloseBlockChain()
	target.CloseBlockChain()
	source = newMigrationChain(t, path, 0)
	defer source.CloseBlockChain()
	target = newMigrationChain(t, path, 1)
	defer target.CloseBlockChain()
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	again, err := source.CommitRelayRedirect(*first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = target.CommitRedirectCredit(*again); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("replay committed twice")
	}
	state, err = target.ExportAccountState(accounts[2])
	if err != nil || state.State.Balance.Int64() != 107 {
		t.Fatal("replay changed balance", err)
	}
}

func TestRedirectCannotFollowAlreadyCommittedOldCredit(t *testing.T) {
	source, target, _, accounts := serialChainFixture(t)
	defer source.CloseBlockChain()
	defer target.CloseBlockChain()
	p, err := source.CommitGroupSourcePreparation(serialGroupCommand(t, source, "first", accounts[:2], 0), 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	serialActivate(t, target, p, 1)
	tx := core.NewTransaction(accounts[0], accounts[2], big.NewInt(7), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, err := core.BuildGroupMigrationHandoff(tx, "first", accounts[:2], 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	first, err := target.CommitMigrationHandoff(*h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = source.CommitHandoffRelay(*first); err != nil {
		t.Fatal(err)
	}
	head := append([]byte(nil), source.CurrentBlock.Hash...)
	if _, err = source.CommitRelayRedirect(*first); err == nil {
		t.Fatal("already credited parent redirected")
	}
	if !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("rejection mutated state")
	}
}
