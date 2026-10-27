package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"io"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type groupConfirmationStageWriter func([]byte) (int, error)

func (w groupConfirmationStageWriter) Write(raw []byte) (int, error) { return w(raw) }

// Polls only the I/O lanes: a deadline turns a stuck publisher/confirmation into a
// failure instead of weakening any execution, ACK, or retirement assertion.
func awaitGroupSourcePublication(t *testing.T, n *PbftConsensusNode) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if err := n.pollSourceHandoff(); err != nil {
			t.Fatal("source publication poll", err)
		}
		lane := n.sourcePublicationLane()
		lane.mu.Lock()
		pending := lane.job != nil
		lane.mu.Unlock()
		if !pending && !n.sourceConfirmationLane().active() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("source publication did not finish")
		case <-tick.C:
		}
	}
}

// Wait for a result without consuming it: the real proposal function must
// validate its binding and current empty-queue proof itself.
func awaitRetirementAuditResult(t *testing.T, lane *retirementAuditLane) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		lane.mu.Lock()
		ready := lane.done != nil && len(lane.done) > 0
		lane.mu.Unlock()
		if ready {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("retirement audit did not finish")
		case <-tick.C:
		}
	}
}

func TestGroupPBFTCommitEvidenceAndRecovery(t *testing.T) {
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
	for _, key := range []string{"SAA_GROUP_SOURCE_PREPARE_PATH", "SAA_GROUP_SOURCE_PREPARED_PATH", "SAA_GROUP_TARGET_INSTALL_PATH", "SAA_GROUP_TARGET_INSTALLED_PATH", "SAA_GROUP_TARGET_ACTIVATION_PATH", "SAA_GROUP_TARGET_ACTIVATED_PATH"} {
		t.Setenv(key, filepath.Join(root, key+".json"))
	}
	t.Setenv("SAA_NATIVE_SOURCE_QUEUE_ROOT", filepath.Join(root, "queue"))
	t.Setenv("SAA_GROUP_SOURCE_RETIRED_PATH", filepath.Join(root, "group-retired.json"))
	for _, key := range []string{"SAA_NATIVE_HANDOFF_OUTBOX_DIR", "SAA_NATIVE_HANDOFF_ACK_DIR", "SAA_NATIVE_HANDOFF_RELAY_DIR"} {
		dir := filepath.Join(root, key)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv(key, dir)
	}
	nodes := []*PbftConsensusNode{}
	for id := uint64(0); id < 2; id++ {
		bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: id, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
		if err != nil {
			t.Fatal(err)
		}
		defer bc.CloseBlockChain()
		n := &PbftConsensusNode{ShardID: id, CurChain: bc, requestPool: make(map[string]*message.Request)}
		n.sequenceID = 1
		n.ihm = &RawRelayPbftExtraHandleMod{pbftNode: n}
		nodes = append(nodes, n)
	}
	addresses := []string{"0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020"}
	source, target := nodes[0], nodes[1]
	var prepareLog bytes.Buffer
	source.pl = &pbft_log.PbftLog{Plog: log.New(&prepareLog, "", 0)}
	source.CurChain.AddAccounts(addresses, []*core.AccountState{{Balance: big.NewInt(100)}, {Balance: big.NewInt(100)}}, 0)
	outsider := "0000000000000000000000000000000000000030"
	source.CurChain.Update_PartitionMap(outsider, 0)
	source.CurChain.AddAccounts([]string{outsider}, []*core.AccountState{{Balance: big.NewInt(100)}}, 0)
	c := core.GroupPrepareCommand{}
	for _, a := range addresses {
		source.CurChain.Update_PartitionMap(a, 0)
		c.Members = append(c.Members, core.SourcePrepareCommand{ProposalID: "group", Account: a, SourceShard: 0, TargetShard: 1, BindAtProposal: true})
	}
	write := func(key string, v interface{}) {
		raw, _ := json.Marshal(v)
		if err := core.PublishSourcePrepareFile(os.Getenv(key), raw); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(n *PbftConsensusNode, out string) *message.Request {
		if !n.hasGroupInput() {
			t.Fatal("group command does not wake idle node")
		}
		r, err := n.groupProposal()
		if err != nil || r == nil {
			t.Fatal("missing proposal", err)
		}
		d := getDigest(r)
		h := n.ihm.(*RawRelayPbftExtraHandleMod)
		head := append([]byte(nil), n.CurChain.CurrentBlock.Hash...)
		bad := append([]byte(nil), d...)
		bad[0] ^= 1
		if h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: r, Digest: bad, SeqID: n.sequenceID}) {
			t.Fatal("bad digest accepted")
		}
		if !h.HandleinPrePrepare(&message.PrePrepare{RequestMsg: r, Digest: d, SeqID: n.sequenceID}) {
			t.Fatal("valid group rejected")
		}
		if !bytes.Equal(head, n.CurChain.CurrentBlock.Hash) {
			t.Fatal("preprepare changed state")
		}
		if _, err = os.Stat(os.Getenv(out)); !os.IsNotExist(err) {
			t.Fatal("premature group receipt")
		}
		h.HandleinCommit(&message.Commit{Digest: d, SeqID: n.sequenceID})
		n.sequenceID++
		raw, err := os.ReadFile(os.Getenv(out))
		if err != nil {
			t.Fatal(err)
		}
		var proof struct {
			Committed bool   `json:"committed"`
			StateRoot string `json:"state_root"`
		}
		if err = json.Unmarshal(raw, &proof); err != nil || !proof.Committed || proof.StateRoot != hex.EncodeToString(n.CurChain.CurrentBlock.Header.StateRoot) {
			t.Fatal("wrong group receipt", err)
		}
		return r
	}
	queued := core.NewTransaction(addresses[0], addresses[1], big.NewInt(1), 1, time.Unix(1, 0))
	queued.RoutingFrozen = true
	source.CurChain.Txpool.AddTxs2Pool([]*core.Transaction{queued})
	write("SAA_GROUP_SOURCE_PREPARE_PATH", c)
	commit(source, "SAA_GROUP_SOURCE_PREPARED_PATH")
	for _, field := range []string{"group_source_prepare_timing", "proposal=group", "status=committed", "prepareQueueUs=", "marshalUs=", "chainCommitUs=", "evidencePublishUs=", "totalUs="} {
		if !bytes.Contains(prepareLog.Bytes(), []byte(field)) {
			t.Fatalf("prepare timing missing %s", field)
		}
	}
	late := core.NewTransaction("0000000000000000000000000000000000000030", addresses[1], big.NewInt(1), 2, time.Unix(1, 0))
	late.Relayed = true
	late.RoutingFrozen = true
	source.CurChain.Txpool.AddTxs2Pool([]*core.Transaction{late})
	source.CurChain.Txpool = core.NewTxPool()
	recoveredSource := &PbftConsensusNode{ShardID: 0, CurChain: source.CurChain}
	if err := recoveredSource.restoreSourceQueue(); err != nil {
		t.Fatal("group queue recovery", err)
	}
	if source.CurChain.Txpool.GetTxQueueLen() != 2 || len(source.CurChain.Txpool.PackTxs(100)) != 0 {
		t.Fatal("group queue missing/duplicated or holds lost")
	}
	p := source.CurChain.GroupSourcePreparation()
	if p == nil || len(p.Members) != 2 {
		t.Fatal("group missing")
	}
	write("SAA_GROUP_TARGET_INSTALL_PATH", p)
	r := commit(target, "SAA_GROUP_TARGET_INSTALLED_PATH")
	a := chain.GroupTargetActivation{}
	for _, m := range p.Members {
		a.Members = append(a.Members, chain.TargetActivation{TargetShard: 1, ProposalID: "group", Account: m.Command.Account, MappingVersion: 1, InstallDigest: hex.EncodeToString(getDigest(r)), StateHash: m.StateHash})
	}
	write("SAA_GROUP_TARGET_ACTIVATION_PATH", a)
	commit(target, "SAA_GROUP_TARGET_ACTIVATED_PATH")
	// Retirement must carry the same bytes that passed the activation check.
	// A later file replacement cannot mutate that already-read evidence.
	activationPath := os.Getenv("SAA_GROUP_TARGET_ACTIVATED_PATH")
	activation, verified, err := source.readConfirmedGroupActivation()
	if err != nil || activation == nil || len(verified) == 0 {
		t.Fatal("missing validated activation bytes", err)
	}
	expected, err := os.ReadFile(activationPath)
	if err != nil || !bytes.Equal(verified, expected) {
		t.Fatal("receipt bytes changed", err)
	}
	if err := os.WriteFile(activationPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.readConfirmedGroupActivation(); err == nil {
		t.Fatal("replacement bypassed validation")
	}
	if !bytes.Equal(verified, expected) {
		t.Fatal("validated bytes alias later file contents")
	}
	if err := os.WriteFile(activationPath, expected, 0600); err != nil {
		t.Fatal(err)
	}
	// A target may observe a linked bundle before the source publisher reports
	// durable success. Hold that completion while exercising the real group
	// polling, target execution, ACK validation, and ordinary queue paths.
	var handoffs []*core.MigrationHandoff
	var handoffIDs []string
	for _, tx := range []*core.Transaction{queued, late} {
		h, err := core.BuildGroupMigrationHandoff(tx, "group", addresses, 0, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		handoffs = append(handoffs, h)
		handoffIDs = append(handoffIDs, h.ID)
	}
	lane := source.sourcePublicationLane()
	outboxDir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR")
	visible, release, workerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var publicationCalls atomic.Int32
	if !lane.start(handoffIDs, func() error {
		defer close(workerDone)
		publicationCalls.Add(1)
		if err := core.PublishSourceHandoffBatch(outboxDir, handoffs); err != nil {
			return err
		}
		close(visible)
		<-release
		return nil
	}) {
		t.Fatal("first publication was not scheduled")
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		select {
		case <-workerDone:
		case <-time.After(10 * time.Second):
			t.Error("publication worker did not stop")
		}
	}()
	select {
	case <-visible:
	case <-time.After(10 * time.Second):
		t.Fatal("publication never became visible")
	}
	for i := 0; i < 3; i++ {
		if err := source.pollSourceHandoff(); err != nil {
			t.Fatal("group outbox while publisher is busy", err)
		}
		if lane.start(handoffIDs, func() error { publicationCalls.Add(1); return nil }) {
			t.Fatal("a second publication worker was admitted")
		}
	}
	if publicationCalls.Load() != 1 {
		t.Fatal("polling duplicated publication work")
	}
	ordinary := core.NewTransaction(outsider, outsider, big.NewInt(1), 99, time.Unix(1, 0))
	ordinary.RoutingFrozen = true
	source.CurChain.Txpool.AddTx2Pool(ordinary)
	packed := source.CurChain.Txpool.PackTxs(100)
	if len(packed) != 1 || !bytes.Equal(packed[0].Encode(), ordinary.Encode()) {
		t.Fatal("busy publication blocked ordinary work or released held group work")
	}
	if source.CurChain.Txpool.GetTxQueueLen() != 2 {
		t.Fatal("source dequeued before target execution")
	}
	if r, err := source.groupRetirementProposal(); err != nil || r != nil {
		t.Fatal("in-flight source publication allowed retirement", err)
	}
	for i := 0; i < 2; i++ {
		hr, err := target.handoffProposal()
		if err != nil || hr == nil {
			t.Fatal("group handoff proposal", err)
		}
		d := getDigest(hr)
		handler := target.ihm.(*RawRelayPbftExtraHandleMod)
		if !handler.HandleinPrePrepare(&message.PrePrepare{RequestMsg: hr, Digest: d, SeqID: target.sequenceID}) {
			t.Fatal("group handoff preprepare")
		}
		handler.HandleinCommit(&message.Commit{Digest: d, SeqID: target.sequenceID})
		target.sequenceID++
	}
	if err := source.pollSourceHandoff(); err != nil {
		t.Fatal("group ACK before publication success", err)
	}
	if source.CurChain.Txpool.GetTxQueueLen() != 2 {
		t.Fatal("visible outbox and real ACK bypassed in-flight durability")
	}
	// Recovery loses the in-memory publication lane. It must recover both
	// obligations despite visible outbox/ACK files, then confirm them through
	// one durable confirm-remove generation in the replica queue directory.
	// The completion proof and removal must be synced before durable dequeue.
	// This is distinct from the live lane's completion wait checked above.
	confirmations, err := core.CompletedMigrationGenerationFiles(source.sourceQueueDirectory())
	if err != nil || len(confirmations) != 0 {
		t.Fatal("premature source confirmation", err)
	}
	source.CurChain.Txpool = core.NewTxPool()
	restarted := &PbftConsensusNode{ShardID: source.ShardID, NodeID: source.NodeID, CurChain: source.CurChain}
	if err := restarted.restoreSourceQueue(); err != nil || source.CurChain.Txpool.GetTxQueueLen() != 2 {
		t.Fatal("visible target ACK discarded source recovery obligations", err)
	}
	// Fail the actual joint publication after receipt validation. The stage
	// observer is diagnostic only; this test moves its private journal directory
	// so publication cannot create/sync a generation, without weakening proofs.
	queueDir := restarted.sourceQueueDirectory()
	parkedQueueDir := queueDir + ".confirmation-unavailable"
	t.Cleanup(func() {
		if _, err := os.Stat(parkedQueueDir); err == nil {
			if err := os.Rename(parkedQueueDir, queueDir); err != nil {
				t.Error("restore private confirmation journal", err)
			}
		}
	})
	var moveOnce sync.Once
	moved := make(chan error, 1)
	restarted.pl = &pbft_log.PbftLog{Plog: log.New(groupConfirmationStageWriter(func(raw []byte) (int, error) {
		if bytes.Contains(raw, []byte("phase=confirm_persist")) {
			moveOnce.Do(func() { moved <- os.Rename(queueDir, parkedQueueDir) })
		}
		return len(raw), nil
	}), "", 0)}
	if err := restarted.pollSourceHandoff(); err != nil {
		t.Fatal("recovered source confirmation", err)
	}
	failed := publicationFinish(t, restarted.sourceConfirmationLane())
	select {
	case err := <-moved:
		if err != nil {
			t.Fatal("inject joint publication failure", err)
		}
	default:
		t.Fatal("joint publication stage was not reached")
	}
	if err := os.Rename(parkedQueueDir, queueDir); err != nil {
		t.Fatal("restore joint publication directory", err)
	}
	if failed.err == nil || source.CurChain.Txpool.GetTxQueueLen() != 2 || !restarted.sourceConfirmationLane().active() {
		t.Fatal("failed joint publication discarded recovered obligations or exact retry", failed.err)
	}
	awaitGroupSourcePublication(t, restarted)
	confirmations, err = core.CompletedMigrationGenerationFiles(restarted.sourceQueueDirectory())
	if err != nil || len(confirmations) != 1 || source.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("recovery bypassed durable batch confirmation", err)
	}
	legacyConfirmations, err := filepath.Glob(filepath.Join(outboxDir, "batch-confirmed-*.json"))
	if err != nil || len(legacyConfirmations) != 0 {
		t.Fatal("recovered confirmation dual-wrote legacy receipts", err)
	}
	recoveredQueue := core.NewTxPool()
	if err := recoveredQueue.EnableGroupMigrationQueueJournal(restarted.sourceQueueDirectory(), addresses); err != nil || recoveredQueue.GetTxQueueLen() != 0 {
		t.Fatal("confirmed recovery resurrected source obligations", err)
	}
	close(release)
	awaitGroupSourcePublication(t, source)
	if source.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("acknowledged group entries retained")
	}
	first, _ := target.CurChain.ExportAccountState(addresses[0])
	second, _ := target.CurChain.ExportAccountState(addresses[1])
	if first.State.Balance.Int64() != 99 || second.State.Balance.Int64() != 102 {
		t.Fatal("group execution repeated debit or missed late credit")
	}
	inbound := core.NewTransaction(outsider, addresses[1], big.NewInt(3), 3, time.Unix(1, 0))
	inbound.RoutingFrozen = true
	source.CurChain.Txpool.AddTxs2Pool([]*core.Transaction{inbound})
	if r, err := source.groupRetirementProposal(); err != nil || r != nil {
		t.Fatal("retired with pending inbound", err)
	}
	dr, err := source.inboundProposal()
	if err != nil || dr == nil {
		t.Fatal("group inbound proposal", err)
	}
	digest := getDigest(dr)
	sh := source.ihm.(*RawRelayPbftExtraHandleMod)
	if !sh.HandleinPrePrepare(&message.PrePrepare{RequestMsg: dr, Digest: digest, SeqID: source.sequenceID}) {
		t.Fatal("inbound preprepare failed")
	}
	beforeDebit, _ := source.CurChain.ExportAccountState(outsider)
	if beforeDebit.State.Balance.Int64() != 100 {
		t.Fatal("preprepare debited sender")
	}
	sh.HandleinCommit(&message.Commit{Digest: digest, SeqID: source.sequenceID})
	source.sequenceID++
	debit, _ := source.CurChain.ExportAccountState(outsider)
	stillFrozen, _ := source.CurChain.ExportAccountState(addresses[1])
	if debit.State.Balance.Int64() != 97 || stillFrozen.State.Balance.Int64() != 100 || source.CurChain.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("wrong source debit/fence/dequeue")
	}
	if _, err = source.CurChain.CommitInboundDebit(inbound, 1); err != nil {
		t.Fatal(err)
	}
	debit, _ = source.CurChain.ExportAccountState(outsider)
	if debit.State.Balance.Int64() != 97 {
		t.Fatal("duplicate source debit")
	}
	cr, err := target.handoffProposal()
	if err != nil || cr == nil {
		t.Fatal("group inbound credit proposal", err)
	}
	cd := getDigest(cr)
	th := target.ihm.(*RawRelayPbftExtraHandleMod)
	if !th.HandleinPrePrepare(&message.PrePrepare{RequestMsg: cr, Digest: cd, SeqID: target.sequenceID}) {
		t.Fatal("group inbound credit preprepare")
	}
	th.HandleinCommit(&message.Commit{Digest: cd, SeqID: target.sequenceID})
	target.sequenceID++
	if r, err := source.inboundProposal(); err != nil || r != nil {
		t.Fatal("inbound confirmation", err)
	}
	awaitGroupSourcePublication(t, source)
	second, _ = target.CurChain.ExportAccountState(addresses[1])
	if second.State.Balance.Int64() != 105 || source.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("inbound credit missing or source retained")
	}
	// A stale result cannot retire this real prepared/activated group.
	if !source.retirementAudit.start("different-preparation-or-activation", func() retirementAuditResult {
		return retirementAuditResult{}
	}) {
		t.Fatal("stale audit submission")
	}
	awaitRetirementAuditResult(t, &source.retirementAudit)
	if r, err := source.groupRetirementProposal(); err != nil || r != nil {
		t.Fatal("stale audit produced a retirement", err)
	}
	// Hold the read-only audit. Exercise the actual RawRelay proposal path,
	// not just pool packing: unrelated work must still become a normal block.
	prepared := source.CurChain.GroupSourcePreparation()
	_, proof, err := source.readConfirmedGroupActivation()
	if err != nil {
		t.Fatal(err)
	}
	binding, err := retirementAuditBinding(prepared, proof, outboxDir, source.sourceQueueDirectory())
	if err != nil {
		t.Fatal(err)
	}
	auditMembers := map[string]string{}
	for _, m := range prepared.Members {
		auditMembers[m.Command.Account] = m.Command.ProposalID
	}
	index := source.retirementAudit.indexFor(outboxDir)
	// Even a completed valid audit becomes stale if a final artifact appears
	// before its result is consumed. Do not certify a file set never audited.
	if !source.retirementAudit.start(binding, func() retirementAuditResult {
		return readRetirementAudit(index, outboxDir, auditMembers, source.sourceQueueDirectory())
	}) {
		t.Fatal("fresh audit submission")
	}
	awaitRetirementAuditResult(t, &source.retirementAudit)
	unexpected := filepath.Join(outboxDir, "unexpected.handoff.json")
	if err := os.WriteFile(unexpected, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if r, err := source.groupRetirementProposal(); err != nil || r != nil {
		t.Fatal("changed final files accepted", err)
	}
	if err := os.Remove(unexpected); err != nil {
		t.Fatal(err)
	}
	readFailure := errors.New("read-only audit failed")
	if !source.retirementAudit.start(binding, func() retirementAuditResult {
		return retirementAuditResult{err: readFailure}
	}) {
		t.Fatal("error audit submission")
	}
	awaitRetirementAuditResult(t, &source.retirementAudit)
	if r, err := source.groupRetirementProposal(); r != nil || err != readFailure {
		t.Fatal("audit error allowed retirement", err)
	}
	auditRelease := make(chan struct{})
	var auditReleaseOnce sync.Once
	defer auditReleaseOnce.Do(func() { close(auditRelease) })
	if !source.retirementAudit.start(binding, func() retirementAuditResult {
		<-auditRelease
		return readRetirementAudit(index, outboxDir, auditMembers, source.sourceQueueDirectory())
	}) {
		t.Fatal("held audit submission")
	}
	ordinary = core.NewTransaction(outsider, outsider, big.NewInt(1), 100, time.Unix(1, 0))
	ordinary.RoutingFrozen = true
	source.CurChain.Txpool.AddTx2Pool(ordinary)
	source.pl = &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}
	normalDone := make(chan *message.Request, 1)
	go func() { _, request := sh.HandleinPropose(); normalDone <- request }()
	select {
	case normal := <-normalDone:
		if normal == nil || normal.RequestType != message.BlockRequest {
			t.Fatal("busy audit blocked ordinary proposal")
		}
		block := core.DecodeB(normal.Msg.Content)
		if len(block.Body) != 1 || !bytes.Equal(block.Body[0].Encode(), ordinary.Encode()) {
			t.Fatal("ordinary block lost independent transaction")
		}
	case <-time.After(2 * time.Second):
		auditReleaseOnce.Do(func() { close(auditRelease) })
		<-normalDone
		t.Fatal("ordinary proposal waited for read-only audit")
	}
	auditReleaseOnce.Do(func() { close(auditRelease) })
	awaitRetirementAuditResult(t, &source.retirementAudit)
	rr, err := source.groupRetirementProposal()
	if err != nil || rr == nil {
		t.Fatal("missing group retirement after async audit", err)
	}
	rd := getDigest(rr)
	if !sh.HandleinPrePrepare(&message.PrePrepare{RequestMsg: rr, Digest: rd, SeqID: source.sequenceID}) {
		t.Fatal("retirement preprepare")
	}
	sh.HandleinCommit(&message.Commit{Digest: rd, SeqID: source.sequenceID})
	source.sequenceID++
	retired, err := source.CurChain.GroupSourceRetirement()
	if err != nil || retired == nil {
		t.Fatal("retirement absent", err)
	}
	if source.CurChain.CheckPreparedTransactions([]*core.Transaction{queued}) == nil {
		t.Fatal("retirement released source execution")
	}
	post := core.NewTransaction(outsider, addresses[1], big.NewInt(2), 4, time.Unix(1, 0))
	post.RoutingFrozen = true
	post.Relayed = true
	source.CurChain.Txpool.AddTxs2Pool([]*core.Transaction{post})
	if err = source.pollSourceHandoff(); err != nil {
		t.Fatal("late input after retirement", err)
	}
	awaitGroupSourcePublication(t, source)
	if source.CurChain.Txpool.GetTxQueueLen() != 1 {
		t.Fatal("late handoff was removed before target execution")
	}
	lateRequest, err := target.handoffProposal()
	if err != nil || lateRequest == nil {
		t.Fatal("late forwarding after retirement", err)
	}
	ld := getDigest(lateRequest)
	if !th.HandleinPrePrepare(&message.PrePrepare{RequestMsg: lateRequest, Digest: ld, SeqID: target.sequenceID}) {
		t.Fatal("late relay preprepare")
	}
	th.HandleinCommit(&message.Commit{Digest: ld, SeqID: target.sequenceID})
	target.sequenceID++
	if err = source.pollSourceHandoff(); err != nil {
		t.Fatal(err)
	}
	awaitGroupSourcePublication(t, source)
	second, _ = target.CurChain.ExportAccountState(addresses[1])
	if second.State.Balance.Int64() != 107 || source.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("post-retirement forwarding incomplete")
	}
	if r, err := source.groupRetirementProposal(); err != nil || r != nil {
		t.Fatal("retirement reproposed", err)
	}
	before, _ := os.ReadFile(os.Getenv("SAA_GROUP_TARGET_INSTALLED_PATH"))
	// A fresh node wrapper simulates loss of the in-memory inbox cache; chain
	// restart persistence itself is covered by the chain-layer tests.
	fresh := &PbftConsensusNode{ShardID: 1, CurChain: target.CurChain}
	fresh.sequenceID = 99
	if r, err := fresh.groupProposal(); err != nil || r != nil {
		t.Fatal("recovery reproposed installation", err)
	}
	after, _ := os.ReadFile(os.Getenv("SAA_GROUP_TARGET_INSTALLED_PATH"))
	if !bytes.Equal(before, after) {
		t.Fatal("republication changed original commit identity")
	}
}
