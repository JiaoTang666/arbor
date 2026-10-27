package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"bytes"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func artifactRequest(t *testing.T, kind message.RequestType, items interface{}) *message.Request {
	t.Helper()
	raw, e := json.Marshal(items)
	if e != nil {
		t.Fatal(e)
	}
	return &message.Request{RequestType: kind, Msg: message.RawMessage{Content: raw}}
}
func assertBundle(t *testing.T, dir, kind string, count int) core.MigrationArtifacts {
	t.Helper()
	files, e := filepath.Glob(filepath.Join(dir, "*"+kind+".bundle"))
	if e != nil || len(files) != 1 {
		t.Fatal("expected one durable batch", files, e)
	}
	singles, _ := filepath.Glob(filepath.Join(dir, "*"+kind))
	if len(singles) != 0 {
		t.Fatal("per-item publication remained", singles)
	}
	items, e := core.LoadMigrationArtifacts(dir, kind, false)
	if e != nil || len(items) != count {
		t.Fatal("batch unreadable", len(items), e)
	}
	return items
}

func TestBatchPublicationCommitFailureRetryAndRelayClosure(t *testing.T) {
	source, target, cmd := auxiliaryPBFTFixture(t)
	root := t.TempDir()
	acks := filepath.Join(root, "a-acks")
	relays := filepath.Join(root, "z-relays")
	outbox := t.TempDir()
	if e := os.Mkdir(acks, 0700); e != nil {
		t.Fatal(e)
	}
	// First group succeeds, second group fails: state is committed, but no seen
	// marker may suppress retry, and no source dequeue is legal yet.
	if e := os.WriteFile(relays, []byte("not a directory"), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("SAA_NATIVE_HANDOFF_ACK_DIR", acks)
	t.Setenv("SAA_NATIVE_HANDOFF_RELAY_DIR", relays)
	t.Setenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR", outbox)
	hs := []core.MigrationHandoff{}
	txs := []*core.Transaction{}
	for i := 0; i < 2; i++ {
		tx := core.NewTransaction(cmd.Account, "0000000000000000000000000000000000000030", big.NewInt(7), uint64(i+1), time.Unix(1, 0))
		tx.RoutingFrozen = true
		tx.IngressSequence = uint64(i + 1)
		h, e := core.BuildMigrationHandoff(tx, cmd.ProposalID, cmd.Account, 0, 1, 1)
		if e != nil {
			t.Fatal(e)
		}
		if e = core.PersistMigrationHandoff(outbox, h); e != nil {
			t.Fatal(e)
		}
		hs = append(hs, *h)
		txs = append(txs, tx)
	}
	n := &PbftConsensusNode{ShardID: 1, CurChain: target}
	r := artifactRequest(t, migrationHandoffBatchRequest, hs)
	if e := n.commitMigrationHandoffBatch(r, getDigest(r)); e == nil {
		t.Fatal("expected publication failure")
	}
	committed := append([]byte(nil), target.CurrentBlock.Hash...)
	for _, h := range hs {
		if _, seen := n.sourcePrepareSeen.Load("handoff:" + h.ID); seen {
			t.Fatal("failed flush marked done")
		}
	}
	ackRows := assertBundle(t, acks, ".ack.json", 2)
	pool := new(core.TxPool)
	pool.AddTxs2Pool(txs)
	completed := []core.CompletedHandoffConfirmation{}
	for i := range hs {
		var ack core.HandoffAck
		if e := json.Unmarshal(ackRows[hs[i].ID], &ack); e != nil {
			t.Fatal(e)
		}
		completed = append(completed, core.CompletedHandoffConfirmation{H: &hs[i], Ack: ack})
	}
	if e := pool.ConfirmCompletedHandoffBatch(outbox, completed); e == nil {
		t.Fatal("source removed pending relay")
	}
	if e := os.Remove(relays); e != nil {
		t.Fatal(e)
	}
	if e := os.Mkdir(relays, 0700); e != nil {
		t.Fatal(e)
	}
	// Fresh process-local state; durable chain marker must prevent second debit.
	n = &PbftConsensusNode{ShardID: 1, CurChain: target}
	if e := n.commitMigrationHandoffBatch(r, getDigest(r)); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(committed, target.CurrentBlock.Hash) {
		t.Fatal("retry reapplied state")
	}
	assertBundle(t, acks, ".ack.json", 2)
	assertBundle(t, relays, ".relay.json", 2)
	relayNode := &PbftConsensusNode{ShardID: 0, CurChain: source}
	rr, e := relayNode.relayBatchProposal()
	if e != nil || rr == nil {
		t.Fatal("relay bundle not proposed", e)
	}
	if e = relayNode.commitAuxiliaryBatch(rr, getDigest(rr)); e != nil {
		t.Fatal(e)
	}
	relayRoot := append([]byte(nil), source.CurrentBlock.Hash...)
	relayNode = &PbftConsensusNode{ShardID: 0, CurChain: source}
	if e = relayNode.commitAuxiliaryBatch(rr, getDigest(rr)); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(relayRoot, source.CurrentBlock.Hash) {
		t.Fatal("replayed relay changed state")
	}
	finals := assertBundle(t, acks, ".relay-ack.json", 2)
	for i := range completed {
		var a core.HandoffRelayAck
		if e = json.Unmarshal(finals[hs[i].ID], &a); e != nil {
			t.Fatal(e)
		}
		completed[i].Relay = &a
	}
	if e = pool.ConfirmCompletedHandoffBatch(outbox, completed); e != nil {
		t.Fatal(e)
	}
	if len(pool.SnapshotMigrationTransactions(cmd.Account)) != 0 {
		t.Fatal("source not drained")
	}
}

func TestInboundBatchPublicationAndRetry(t *testing.T) {
	source, _, cmd := auxiliaryPBFTFixture(t)
	dir := t.TempDir()
	t.Setenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR", dir)
	sender := "0000000000000000000000000000000000000020"
	source.Update_PartitionMap(sender, 0)
	source.AddAccounts([]string{sender}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	items := []chain.InboundDebitItem{}
	for i := 0; i < 2; i++ {
		tx := core.NewTransaction(sender, cmd.Account, big.NewInt(7), uint64(i+1), time.Unix(1, 0))
		tx.RoutingFrozen = true
		items = append(items, chain.InboundDebitItem{Transaction: tx, MappingVersion: 1})
	}
	n := &PbftConsensusNode{CurChain: source}
	r := artifactRequest(t, inboundDebitBatchRequest, items)
	if e := n.commitAuxiliaryBatch(r, getDigest(r)); e != nil {
		t.Fatal(e)
	}
	root := append([]byte(nil), source.CurrentBlock.Hash...)
	existing := assertBundle(t, dir, ".handoff.json", 2)
	for _, item := range items {
		record, e := source.FindInboundDebit(item.Transaction, 1)
		if e != nil || record == nil {
			t.Fatal(e)
		}
		if e = n.ensureInboundPublication(dir, existing, &record.Credit); e != nil {
			t.Fatal(e)
		}
	}
	if e := n.commitAuxiliaryBatch(r, getDigest(r)); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(root, source.CurrentBlock.Hash) {
		t.Fatal("inbound retry changed state")
	}
	assertBundle(t, dir, ".handoff.json", 2)
}
