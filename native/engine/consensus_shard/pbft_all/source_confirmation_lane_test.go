package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"io"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const confirmationMemberA = "0000000000000000000000000000000000000010"
const confirmationMemberB = "0000000000000000000000000000000000000020"
const confirmationOutsider = "0000000000000000000000000000000000000030"

func confirmationAck(h *core.MigrationHandoff) core.HandoffAck {
	sum := sha256.Sum256(h.Execution)
	return core.HandoffAck{ID: h.ID, ProposalID: h.ProposalID, TargetShard: h.TargetShard, ExecutionHash: hex.EncodeToString(sum[:]), Block: 1, BlockHash: strings.Repeat("1", 64), StateRoot: strings.Repeat("2", 64), Executed: true}
}

// A committed source preparation and a validated trusted-local activation are
// enough to exercise the real proposal/retirement boundaries. Target execution
// and receipt validation are separately covered by the existing PBFT tests.
func confirmationPreparedNode(t *testing.T) (*PbftConsensusNode, string) {
	t.Helper()
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() {
		params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum = oldRoot, oldDB, oldShards
	})
	root := t.TempDir()
	params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum = root, filepath.Join(root, "blocks")+"/", 2
	t.Setenv(chain.NativeEffectIndexEnv, "")
	t.Setenv("SAA_SERIAL_GROUP_CONTROL", "")
	for _, key := range []string{"SAA_GROUP_SOURCE_PREPARE_PATH", "SAA_GROUP_SOURCE_PREPARED_PATH", "SAA_GROUP_TARGET_INSTALL_PATH", "SAA_GROUP_TARGET_INSTALLED_PATH", "SAA_GROUP_TARGET_ACTIVATION_PATH", "SAA_GROUP_TARGET_ACTIVATED_PATH", "SAA_GROUP_SOURCE_RETIRED_PATH"} {
		t.Setenv(key, filepath.Join(root, key+".json"))
	}
	t.Setenv("SAA_NATIVE_SOURCE_QUEUE_ROOT", filepath.Join(root, "queue"))
	for _, key := range []string{"SAA_NATIVE_HANDOFF_OUTBOX_DIR", "SAA_NATIVE_HANDOFF_ACK_DIR", "SAA_NATIVE_HANDOFF_RELAY_DIR"} {
		dir := filepath.Join(root, key)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv(key, dir)
	}
	bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bc.CloseBlockChain)
	n := &PbftConsensusNode{ShardID: 0, CurChain: bc, sequenceID: 2, pl: &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}}
	addresses := []string{confirmationMemberA, confirmationMemberB, confirmationOutsider}
	bc.AddAccounts(addresses, []*core.AccountState{{Balance: big.NewInt(100)}, {Balance: big.NewInt(100)}, {Balance: big.NewInt(100)}}, 0)
	for _, a := range addresses {
		bc.Update_PartitionMap(a, 0)
	}
	command := core.GroupPrepareCommand{}
	for _, a := range addresses[:2] {
		state, err := bc.ExportAccountState(a)
		if err != nil {
			t.Fatal(err)
		}
		command.Members = append(command.Members, core.SourcePrepareCommand{ProposalID: "confirmation", Account: a, SourceShard: 0, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(bc.CurrentBlock.Hash), ExpectedStateRoot: hex.EncodeToString(bc.CurrentBlock.Header.StateRoot), ExpectedStateHash: hex.EncodeToString(state.StateHash)})
	}
	if err := n.prepareGroupQueue(command); err != nil {
		t.Fatal(err)
	}
	prepared, err := bc.CommitGroupSourcePreparation(command, 1, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	activation := chain.GroupTargetActivation{}
	for _, m := range prepared.Members {
		activation.Members = append(activation.Members, chain.TargetActivation{ProposalID: m.Command.ProposalID, Account: m.Command.Account, TargetShard: 1, MappingVersion: 1, StateHash: m.StateHash, InstallDigest: strings.Repeat("3", 64)})
	}
	raw, err := json.Marshal(map[string]interface{}{"record": activation, "committed": true, "block": 1, "block_hash": strings.Repeat("4", 64), "state_root": strings.Repeat("5", 64), "shard": 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.PublishSourcePrepareFile(os.Getenv("SAA_GROUP_TARGET_ACTIVATED_PATH"), raw); err != nil {
		t.Fatal(err)
	}
	if active, _, err := n.readConfirmedGroupActivation(); err != nil || active == nil {
		t.Fatal("fixture activation does not bind to committed preparation", err)
	}
	return n, os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR")
}

func TestSourceConfirmationBusyAllowsRealOrdinaryProposalAndPreservesLateWork(t *testing.T) {
	n, dir := confirmationPreparedNode(t)
	pool := n.CurChain.Txpool
	held := core.NewTransaction(confirmationMemberA, confirmationMemberB, big.NewInt(1), 1, time.Unix(1, 0))
	held.RoutingFrozen = true
	pool.AddTx2Pool(held)
	h, err := core.BuildGroupMigrationHandoff(held, "confirmation", []string{confirmationMemberA, confirmationMemberB}, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.PersistMigrationHandoff(dir, h); err != nil {
		t.Fatal(err)
	}
	items := []core.CompletedHandoffConfirmation{{H: h, Ack: confirmationAck(h)}}
	entered, release, workerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	lane := n.sourceConfirmationLane()
	if !lane.start([]string{h.ID}, func() error {
		defer close(workerDone)
		close(entered)
		<-release
		return pool.ConfirmCompletedHandoffBatch(dir, items)
	}) {
		t.Fatal("confirmation was not admitted")
	}
	defer func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-workerDone:
		case <-time.After(2 * time.Second):
			t.Error("confirmation worker did not stop")
		}
		// The real proposal may launch a late forwarding writer. Join it
		// before fixture cleanup even when an assertion above fails.
		awaitGroupSourcePublication(t, n)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("confirmation worker did not enter")
	}
	if queued, err := n.queueCompletedHandoffs(dir, items); err != nil || queued {
		t.Fatal("a second confirmation task was admitted", err)
	}
	late := core.NewTransaction(confirmationMemberA, confirmationMemberB, big.NewInt(2), 2, time.Unix(1, 0))
	late.RoutingFrozen = true
	ordinary := core.NewTransaction(confirmationOutsider, confirmationOutsider, big.NewInt(1), 3, time.Unix(1, 0))
	ordinary.RoutingFrozen = true
	pool.AddTxs2Pool([]*core.Transaction{late, ordinary})
	if request, err := n.groupRetirementProposal(); err != nil || request != nil {
		t.Fatal("in-flight confirmation allowed source retirement", err)
	}
	proposalDone := make(chan *message.Request, 1)
	go func() {
		// Match the proposal loop's locks: a slow confirmation must not keep
		// HandleinPropose inside the consensus critical section.
		n.sequenceLock.Lock()
		n.pbftLock.Lock()
		_, request := (&RawRelayPbftExtraHandleMod{pbftNode: n}).HandleinPropose()
		n.pbftLock.Unlock()
		n.sequenceLock.Unlock()
		proposalDone <- request
	}()
	select {
	case request := <-proposalDone:
		if request == nil || request.RequestType != message.BlockRequest {
			t.Fatal("slow confirmation prevented an ordinary block")
		}
		block := core.DecodeB(request.Msg.Content)
		if len(block.Body) != 1 || !bytes.Equal(block.Body[0].Encode(), ordinary.Encode()) {
			t.Fatal("ordinary proposal lost independent work or executed held work")
		}
	case <-time.After(2 * time.Second):
		releaseOnce.Do(func() { close(release) })
		select {
		case <-proposalDone:
		case <-time.After(2 * time.Second):
		}
		t.Fatal("HandleinPropose waited for confirmation I/O")
	}
	if pool.GetTxQueueLen() != 2 {
		t.Fatal("source obligations disappeared before durable confirmation")
	}
	releaseOnce.Do(func() { close(release) })
	if result := publicationFinish(t, lane); result.err != nil {
		t.Fatal("confirmation failed", result.err)
	}
	remaining := pool.SnapshotGroupMigrationTransactions([]string{confirmationMemberA, confirmationMemberB})
	if len(remaining) != 1 || !bytes.Equal(remaining[0].Encode(), late.Encode()) {
		t.Fatal("an old ACK removed late related work")
	}
	if request, err := n.groupRetirementProposal(); err != nil || request != nil {
		t.Fatal("late related work allowed source retirement", err)
	}
	if packed := pool.PackTxs(10); len(packed) != 0 {
		t.Fatal("late related work lost the migration fence")
	}
}

func TestSourceConfirmationOwnsInputsAndRetainsFailedOrStoppedQueue(t *testing.T) {
	pool := core.NewTxPool()
	dir := filepath.Join(t.TempDir(), "outbox") // Missing on the first attempt.
	accounts := []string{confirmationMemberA, confirmationMemberB}
	var items []core.CompletedHandoffConfirmation
	for i := 0; i < 3; i++ {
		tx := core.NewTransaction(confirmationMemberA, confirmationMemberB, big.NewInt(1), uint64(i), time.Unix(1, 0))
		tx.RoutingFrozen = true
		var inbound *core.Transaction
		switch i {
		case 1:
			tx.Recipient, tx.RoutedRecipientShard = confirmationOutsider, 2
		case 2:
			tx.Sender, tx.Recipient = confirmationOutsider, confirmationMemberA
			inbound = core.DecodeTx(tx.Encode())
		}
		// Pool ownership is separate from the mutable caller's proof inputs.
		pool.AddTx2Pool(core.DecodeTx(tx.Encode()))
		wire := core.DecodeTx(tx.Encode())
		if inbound != nil {
			wire.Relayed = true
		}
		h, err := core.BuildGroupMigrationHandoff(wire, "owned", accounts, 0, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		item := core.CompletedHandoffConfirmation{H: h, Ack: confirmationAck(h), InboundOriginal: inbound}
		if i == 1 {
			item.Ack.RelayPending = true
			relay := core.DecodeTx(h.Execution)
			relay.Relayed = true
			raw := relay.Encode()
			sum := sha256.Sum256(raw)
			item.Relay = &core.HandoffRelayAck{RelayPayload: raw, ID: h.ID, RelayHash: hex.EncodeToString(sum[:]), RecipientShard: 2, Block: 1, BlockHash: strings.Repeat("6", 64), StateRoot: strings.Repeat("7", 64), Committed: true}
		}
		items = append(items, item)
	}
	journal := t.TempDir()
	if err := pool.EnableGroupMigrationQueueJournal(journal, accounts); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	var original []core.CompletedHandoffConfirmation
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	n := &PbftConsensusNode{CurChain: &chain.BlockChain{Txpool: pool, ChainConfig: &params.ChainConfig{BlockSize: 4}}}
	n.stopSignal.Store(true)
	if queued, err := n.queueCompletedHandoffs(dir, items); err != nil || queued || pool.GetTxQueueLen() != 3 {
		t.Fatal("stopped node scheduled confirmation or lost its queue", err)
	}
	n.stopSignal.Store(false)
	if queued, err := n.queueCompletedHandoffs(dir, items); err != nil || !queued {
		t.Fatal("confirmation was not scheduled", err)
	}
	lane := n.sourceConfirmationLane()
	if result := publicationFinish(t, lane); result.err == nil || !lane.active() || pool.GetTxQueueLen() != 3 {
		t.Fatal("failed confirmation released source obligations")
	}
	items[0].H.Original[0] ^= 1
	items[0].H.Execution[0] ^= 1
	items[0].H.GroupAccounts[0] = confirmationOutsider
	items[1].Relay.RelayPayload[0] ^= 1
	items[1].Ack.ExecutionHash = "caller replaced receipt"
	items[2].InboundOriginal.Value.SetInt64(999)
	items[2].InboundOriginal.TxHash[0] ^= 1
	items[0] = core.CompletedHandoffConfirmation{}
	if queued, err := n.queueCompletedHandoffs(dir, original); err != nil || queued {
		t.Fatal("failed immutable confirmation was overwritten", err)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, item := range original {
		if err := core.PersistMigrationHandoff(dir, item.H); err != nil {
			t.Fatal(err)
		}
	}
	if result := publicationFinish(t, lane); result.err != nil || lane.active() || pool.GetTxQueueLen() != 0 {
		t.Fatal("retry did not use its owned original proof inputs", result.err)
	}
	fresh := core.NewTxPool()
	if err := fresh.EnableGroupMigrationQueueJournal(journal, accounts); err != nil || fresh.GetTxQueueLen() != 0 {
		t.Fatal("successful owned retry did not durably remove obligations", err)
	}
}
