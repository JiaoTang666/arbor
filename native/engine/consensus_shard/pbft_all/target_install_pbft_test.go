package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

func TestTargetPBFTInstallationCommitAndReceipt(t *testing.T) {
	activationReceiptPath := filepath.Join(t.TempDir(), "activated.json")
	t.Setenv("SAA_NATIVE_TARGET_ACTIVATED_PATH", activationReceiptPath)
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	})
	root := t.TempDir()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "blocks") + "/"
	params.ShardNum = 2
	chains := make([]*chain.BlockChain, 2)
	for id := uint64(0); id < 2; id++ {
		bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: id, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
		if err != nil {
			t.Fatal(err)
		}
		defer bc.CloseBlockChain()
		chains[id] = bc
	}
	account := "0000000000000000000000000000000000000010"
	source, target := chains[0], chains[1]
	source.Update_PartitionMap(account, 0)
	source.AddAccounts([]string{account}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	state, err := source.ExportAccountState(account)
	if err != nil {
		t.Fatal(err)
	}
	p, err := source.CommitSourcePreparation(core.SourcePrepareCommand{ProposalID: "target-test", Account: account, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(source.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(state.SourceRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)}, 2, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(p)
	n := &PbftConsensusNode{ShardID: 1, CurChain: target, requestPool: make(map[string]*message.Request)}
	h := &RawRelayPbftExtraHandleMod{pbftNode: n}
	n.ihm = h
	n.sequenceID = 1
	input := filepath.Join(root, "target-command.json")
	if err = core.PublishSourcePrepareFile(input, data); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAA_NATIVE_TARGET_INSTALL_PATH", input)
	if delay, _ := n.shouldDelayEmptyProposal(); delay {
		t.Fatal("target command did not wake idle shard")
	}
	r, err := n.targetInstallProposal()
	if err != nil || r == nil {
		t.Fatalf("proposal: %v", err)
	}
	digest := getDigest(r)
	out := filepath.Join(root, "receipt.jsonl")
	t.Setenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH", out)
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: r, Digest: digest, SeqID: 1}) {
		t.Fatal("preprepare rejected")
	}
	if _, err = target.ExportAccountState(account); err == nil {
		t.Fatal("preprepare installed account")
	}
	if _, err = os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("premature receipt")
	}
	h.HandleinCommit(&message.Commit{Digest: digest, SeqID: 1})
	actual, err := target.ExportAccountState(account)
	if err != nil || !bytes.Equal(actual.StateHash, state.StateHash) {
		t.Fatal("wrong installed state")
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]interface{}
	if err = json.Unmarshal(bytes.TrimSpace(raw), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt["committed"] != true || receipt["installed"] != true || receipt["target_state_root"] != hex.EncodeToString(target.CurrentBlock.Header.StateRoot) || receipt["commit_digest"] != hex.EncodeToString(digest) {
		t.Fatal("receipt does not describe actual commit")
	}
	if target.CheckPreparedTransactions([]*core.Transaction{{Sender: account}}) == nil {
		t.Fatal("installation prematurely enabled execution")
	}
	head := append([]byte(nil), target.CurrentBlock.Hash...)
	if next, err := n.targetInstallProposal(); err != nil || next != nil || !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("installed input was proposed again")
	}
	if n.hasTargetInstallInput() {
		t.Fatal("completed input keeps waking shard")
	}
	activation := chain.TargetActivation{TargetShard: 1, ProposalID: p.Command.ProposalID, Account: account, MappingVersion: 1, InstallDigest: hex.EncodeToString(digest), StateHash: p.StateHash}
	activationData, _ := json.Marshal(activation)
	activationPath := filepath.Join(root, "activation.json")
	if err = core.PublishSourcePrepareFile(activationPath, activationData); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAA_NATIVE_TARGET_ACTIVATION_PATH", activationPath)
	n.sequenceID = 2
	ar, err := n.targetActivationProposal()
	if err != nil || ar == nil {
		t.Fatalf("activation proposal: %v", err)
	}
	ad := getDigest(ar)
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: ar, Digest: ad, SeqID: 2}) {
		t.Fatal("activation preprepare rejected")
	}
	if target.CheckPreparedTransactions([]*core.Transaction{{Sender: account}}) == nil {
		t.Fatal("preprepare released account")
	}
	h.HandleinCommit(&message.Commit{Digest: ad, SeqID: 2})
	if target.CheckPreparedTransactions([]*core.Transaction{{Sender: account}}) != nil || target.Get_PartitionMap(account) != 1 {
		t.Fatal("activation commit ineffective")
	}
	if source.CheckPreparedTransactions([]*core.Transaction{{Sender: account}}) == nil {
		t.Fatal("source execution restored")
	}
	// A self-transfer changes nonce once and leaves balance conserved, exercising
	// both debit and credit without inventing a second target account.
	tx := core.NewTransaction(account, account, big.NewInt(7), 1, r.ReqTime)
	tx.RoutingFrozen = true
	journalRoot := t.TempDir()
	t.Setenv("SAA_NATIVE_SOURCE_QUEUE_ROOT", journalRoot)
	journalDir := filepath.Join(journalRoot, "s0-n0")
	if err := os.Mkdir(journalDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := source.Txpool.EnableMigrationQueueJournal(journalDir, account); err != nil {
		t.Fatal(err)
	}
	handoff, err := core.BuildMigrationHandoff(tx, p.Command.ProposalID, account, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	before, err := target.ExportAccountState(account)
	if err != nil {
		t.Fatal(err)
	}
	ackDir := t.TempDir()
	t.Setenv("SAA_NATIVE_HANDOFF_ACK_DIR", ackDir)
	t.Setenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR", t.TempDir())
	source.SendTx2Pool([]*core.Transaction{tx})
	sourceNode := &PbftConsensusNode{CurChain: source, ShardID: 0}
	if err = sourceNode.pollSourceHandoff(); err != nil || source.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("source poll did not retain queued handoff")
	}
	hr, err := n.handoffProposal()
	if err != nil || hr == nil {
		t.Fatalf("handoff ingress: %v", err)
	}
	hash := getDigest(hr)
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: hr, Digest: hash, SeqID: 3}) {
		t.Fatal("handoff preprepare rejected")
	}
	unchanged, _ := target.ExportAccountState(account)
	if !bytes.Equal(unchanged.StateHash, before.StateHash) {
		t.Fatal("preprepare executed handoff")
	}
	h.HandleinCommit(&message.Commit{Digest: hash, SeqID: 3})
	after, _ := target.ExportAccountState(account)
	if after.State.Nonce != before.State.Nonce+1 || after.State.Balance.Cmp(before.State.Balance) != 0 {
		t.Fatal("handoff debit/credit incorrect")
	}
	head = append([]byte(nil), target.CurrentBlock.Hash...)
	h.HandleinCommit(&message.Commit{Digest: hash, SeqID: 3})
	if !bytes.Equal(head, target.CurrentBlock.Hash) {
		t.Fatal("duplicate handoff changed chain")
	}
	ackBytes, err := os.ReadFile(filepath.Join(ackDir, handoff.ID+".ack.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ack core.HandoffAck
	if err = json.Unmarshal(ackBytes, &ack); err != nil {
		t.Fatal(err)
	}
	if err = core.ValidateHandoffAck(handoff, ack); err != nil || ack.RelayPending || ack.BlockHash != hex.EncodeToString(head) {
		t.Fatal("handoff ACK does not match actual execution")
	}
	if err = sourceNode.pollSourceHandoff(); err != nil || source.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("source did not consume execution ACK")
	}
	if next, err := n.handoffProposal(); err != nil || next != nil {
		t.Fatal("completed handoff was re-proposed")
	}
	// A second-leg arrival after activation must remain credit-only.
	late := core.NewTransaction("0000000000000000000000000000000000000091", account, big.NewInt(5), 2, r.ReqTime)
	late.Relayed = true
	late.RoutingFrozen = true
	late.RoutedSenderShard = 1
	late.RoutedRecipientShard = 0
	late.IngressSequence = 2
	sourceBefore, _ := source.ExportAccountState(account)
	source.SendTx2Pool([]*core.Transaction{late})
	if err = sourceNode.pollSourceHandoff(); err != nil || source.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("late credit was not retained pending execution")
	}
	creditRequest, err := n.handoffProposal()
	if err != nil || creditRequest == nil {
		t.Fatalf("late credit proposal: %v", err)
	}
	creditDigest := getDigest(creditRequest)
	creditBefore, _ := target.ExportAccountState(account)
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: creditRequest, Digest: creditDigest, SeqID: 4}) {
		t.Fatal("late credit preprepare rejected")
	}
	h.HandleinCommit(&message.Commit{Digest: creditDigest, SeqID: 4})
	creditAfter, _ := target.ExportAccountState(account)
	wantBalance := new(big.Int).Add(creditBefore.State.Balance, big.NewInt(5))
	if creditAfter.State.Balance.Cmp(wantBalance) != 0 || creditAfter.State.Nonce != creditBefore.State.Nonce {
		t.Fatal("late relay credited incorrectly or performed debit")
	}
	creditHead := append([]byte(nil), target.CurrentBlock.Hash...)
	h.HandleinCommit(&message.Commit{Digest: creditDigest, SeqID: 4})
	if !bytes.Equal(creditHead, target.CurrentBlock.Hash) {
		t.Fatal("duplicate late credit executed")
	}
	if err = sourceNode.pollSourceHandoff(); err != nil || source.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("late credit acknowledgement did not dequeue")
	}
	sourceAfter, _ := source.ExportAccountState(account)
	if !bytes.Equal(sourceBefore.StateHash, sourceAfter.StateHash) {
		t.Fatal("late relay changed frozen source state")
	}
	peer := "0000000000000000000000000000000000000020"
	source.Update_PartitionMap(peer, 0)
	source.AddAccounts([]string{peer}, []*core.AccountState{{Balance: big.NewInt(10)}}, 0)
	outgoing := core.NewTransaction(account, peer, big.NewInt(3), 3, r.ReqTime)
	outgoing.RoutingFrozen = true
	outgoing.IngressSequence = 3
	source.SendTx2Pool([]*core.Transaction{outgoing})
	relayDir := t.TempDir()
	t.Setenv("SAA_NATIVE_HANDOFF_RELAY_DIR", relayDir)
	if err = sourceNode.pollSourceHandoff(); err != nil {
		t.Fatal(err)
	}
	or, err := n.handoffProposal()
	if err != nil || or == nil {
		t.Fatal("outgoing handoff missing")
	}
	od := getDigest(or)
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: or, Digest: od, SeqID: 5}) {
		t.Fatal("outgoing rejected")
	}
	h.HandleinCommit(&message.Commit{Digest: od, SeqID: 5})
	decoded, err := decodeMigrationHandoff(or)
	if err != nil {
		t.Fatal(err)
	}
	relayData, err := os.ReadFile(filepath.Join(relayDir, decoded.ID+".relay.json"))
	if err != nil {
		t.Fatal(err)
	}
	rr := &message.Request{RequestType: handoffRelayRequest, Msg: message.RawMessage{Content: relayData}}
	rd := getDigest(rr)
	sourceNode.requestPool = make(map[string]*message.Request)
	sourceHandler := &RawRelayPbftExtraHandleMod{pbftNode: sourceNode}
	if !sourceHandler.HandleinPrePrepare(&message.PrePrepare{RequestMsg: rr, Digest: rd, SeqID: 4}) {
		t.Fatal("relay preprepare rejected")
	}
	creditState, _ := source.ExportAccountState(peer)
	if creditState.State.Balance.Int64() != 10 {
		t.Fatal("relay preprepare credited")
	}
	sourceHandler.HandleinCommit(&message.Commit{Digest: rd, SeqID: 4})
	creditState, _ = source.ExportAccountState(peer)
	if creditState.State.Balance.Int64() != 13 {
		t.Fatal("relay commit did not credit")
	}
	relayHead := append([]byte(nil), source.CurrentBlock.Hash...)
	sourceHandler.HandleinCommit(&message.Commit{Digest: rd, SeqID: 4})
	if !bytes.Equal(relayHead, source.CurrentBlock.Hash) {
		t.Fatal("relay duplicate wrote again")
	}
	if err = sourceNode.pollSourceHandoff(); err != nil || source.Txpool.GetTxQueueLen() != 0 {
		t.Fatalf("final relay ACK did not complete source handoff: %v", err)
	}
	if next, err := sourceNode.relayProposal(); err != nil || next != nil {
		t.Fatal("completed relay proposed again")
	}
	incoming := core.NewTransaction(peer, account, big.NewInt(2), 4, r.ReqTime)
	incoming.RoutingFrozen = true
	incoming.IngressSequence = 4
	source.SendTx2Pool([]*core.Transaction{incoming})
	ir, err := sourceNode.inboundProposal()
	if err != nil || ir == nil || ir.RequestType != inboundDebitRequest {
		t.Fatalf("queued inbound debit was not proposed: %v", err)
	}
	idigest := getDigest(ir)
	if !sourceHandler.HandleinPrePrepare(&message.PrePrepare{RequestMsg: ir, Digest: idigest, SeqID: 5}) {
		t.Fatal("inbound preprepare rejected")
	}
	beforeDebit, _ := source.ExportAccountState(peer)
	if beforeDebit.State.Balance.Int64() != 13 {
		t.Fatal("inbound preprepare debited")
	}
	sourceHandler.HandleinCommit(&message.Commit{Digest: idigest, SeqID: 5})
	afterDebit, _ := source.ExportAccountState(peer)
	if afterDebit.State.Balance.Int64() != 11 {
		t.Fatal("inbound debit incorrect")
	}
	if next, err := sourceNode.inboundProposal(); err != nil || next != nil || source.Txpool.GetTxQueueLen() != 1 {
		t.Fatalf("inbound must remain queued without credit acknowledgement: %v", err)
	}
	inboundHead := append([]byte(nil), source.CurrentBlock.Hash...)
	sourceHandler.HandleinCommit(&message.Commit{Digest: idigest, SeqID: 5})
	if !bytes.Equal(inboundHead, source.CurrentBlock.Hash) {
		t.Fatal("inbound debit repeated")
	}
	credit, err := n.handoffProposal()
	if err != nil || credit == nil {
		t.Fatalf("inbound credit not published: %v", err)
	}
	cdigest := getDigest(credit)
	beforeCredit, _ := target.ExportAccountState(account)
	if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: credit, Digest: cdigest, SeqID: 6}) {
		t.Fatal("inbound credit rejected")
	}
	h.HandleinCommit(&message.Commit{Digest: cdigest, SeqID: 6})
	afterCredit, _ := target.ExportAccountState(account)
	if afterCredit.State.Balance.Cmp(new(big.Int).Add(beforeCredit.State.Balance, big.NewInt(2))) != 0 {
		t.Fatal("inbound credit incorrect")
	}
	if next, err := sourceNode.inboundProposal(); err != nil || next != nil || source.Txpool.GetTxQueueLen() != 0 {
		t.Fatalf("inbound acknowledgement did not remove original non-relay transaction: %v", err)
	}
	if next, err := sourceNode.inboundProposal(); err != nil || next != nil {
		t.Fatalf("completed inbound transaction was re-proposed: %v", err)
	}
	audit, err := core.AuditMigrationHandoffs(os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR"), p.Command.ProposalID, account, journalDir)
	firstActivationReceipt, receiptErr := os.ReadFile(activationReceiptPath)
	if receiptErr != nil {
		t.Fatal(receiptErr)
	}
	if err := n.publishTargetActivationReceipt(activation); err != nil {
		t.Fatal(err)
	}
	replayedActivationReceipt, receiptErr := os.ReadFile(activationReceiptPath)
	if receiptErr != nil || !bytes.Equal(firstActivationReceipt, replayedActivationReceipt) {
		t.Fatal("activation receipt changed after later transaction commits")
	}
	restoredPool := core.NewTxPool()
	if err := restoredPool.EnableMigrationQueueJournal(journalDir, account); err != nil || restoredPool.GetTxQueueLen() != 0 {
		t.Fatalf("confirmed transactions revived after journal recovery: %v", err)
	}
	if err != nil || audit.Persisted != 4 || audit.Confirmed != 4 || len(audit.Pending) != 0 {
		t.Fatalf("four transaction paths lack durable confirmation: %+v %v", audit, err)
	}
	boundary, err := source.Txpool.MigrationDrainBoundary(account)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := os.ReadFile(filepath.Join(journalDir, fmt.Sprintf("%020d.queue.json", boundary.Generation)))
	if err != nil {
		t.Fatal(err)
	}
	receiptHash := sha256.Sum256(firstActivationReceipt)
	envelope := sourceRetirementEnvelope{Command: chain.SourceRetirementCommand{Activation: activation, ActivationReceiptHash: hex.EncodeToString(receiptHash[:]), Drain: boundary}, ActivationReceipt: firstActivationReceipt, DrainSnapshot: snapshot}
	retiredPath := filepath.Join(t.TempDir(), "retired.json")
	t.Setenv("SAA_NATIVE_SOURCE_RETIRED_PATH", retiredPath)
	retireRequest, err := sourceNode.sourceRetirementProposal()
	if err != nil || retireRequest == nil {
		t.Fatalf("automatic retirement not proposed: %v", err)
	}
	retireDigest := getDigest(retireRequest)
	if !sourceHandler.HandleinPrePrepare(&message.PrePrepare{RequestMsg: retireRequest, Digest: retireDigest, SeqID: 6}) {
		t.Fatal("retirement preprepare rejected")
	}
	if record, err := source.SourceRetirement(); err != nil || record != nil {
		t.Fatal("preprepare retired source")
	}
	sourceHandler.HandleinCommit(&message.Commit{Digest: retireDigest, SeqID: 6})
	if record, err := source.SourceRetirement(); err != nil || record == nil || record.Command != envelope.Command {
		t.Fatal("retirement not committed")
	}
	sourceHandler.HandleinCommit(&message.Commit{Digest: retireDigest, SeqID: 6})
	retiredBytes, err := os.ReadFile(retiredPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAA_NATIVE_SOURCE_RETIRED_PATH", filepath.Join(t.TempDir(), "repaired.json"))
	retiredHead := append([]byte(nil), source.CurrentBlock.Hash...)
	if next, err := sourceNode.sourceRetirementProposal(); err != nil || next != nil {
		t.Fatalf("receipt repair failed: %v", err)
	}
	repairedBytes, err := os.ReadFile(os.Getenv("SAA_NATIVE_SOURCE_RETIRED_PATH"))
	if err != nil || !bytes.Equal(retiredBytes, repairedBytes) || !bytes.Equal(retiredHead, source.CurrentBlock.Hash) {
		t.Fatal("receipt repair changed state or evidence")
	}
}
