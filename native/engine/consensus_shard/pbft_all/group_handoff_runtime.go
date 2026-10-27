package pbft_all

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Only a committed whole-group activation receipt unlocks source forwarding.
// Source queues remain held until the matching full execution ACK is present.
func (n *PbftConsensusNode) pollGroupSourceHandoff() error {
	for _, p := range n.CurChain.GroupSourcePreparations() {
		if err := n.pollOneGroupSourceHandoff(p); err != nil {
			return err
		}
	}
	return nil
}

func (n *PbftConsensusNode) pollOneGroupSourceHandoff(p *core.GroupPreparation) error {
	if n.stopSignal.Load() {
		return nil
	}
	started := time.Now()
	var completed []core.CompletedHandoffConfirmation
	var snapshotTime, indexTime, buildTime, confirmTime time.Duration
	contactPending, recoveryDue, submitted, confirmationSubmitted := -1, -1, 0, 0
	creditSubmitted := false
	defer func() {
		if n.pl != nil && (len(completed) > 0 || submitted > 0 || creditSubmitted || time.Since(started) > time.Second) {
			n.pl.Plog.Printf("group_source_handoff_timing contact_pending=%d recovery_due=%d publication_submitted=%d confirmation_ready=%d confirmation_submitted=%d source_credit_submitted=%t snapshotMs=%d indexMs=%d buildAckMs=%d confirmationQueueMs=%d totalMs=%d", contactPending, recoveryDue, submitted, len(completed), confirmationSubmitted, creditSubmitted, snapshotTime.Milliseconds(), indexTime.Milliseconds(), buildTime.Milliseconds(), confirmTime.Milliseconds(), time.Since(started).Milliseconds())
		}
	}()
	lane := n.sourcePublicationLane()
	busy, result := lane.poll()
	if result != nil {
		if n.pl != nil {
			n.pl.Plog.Printf("group_source_publication_complete elapsedMs=%d success=%t", result.elapsed.Milliseconds(), result.err == nil)
		}
		if result.err != nil {
			return result.err
		}
	}
	if p == nil {
		return nil
	}
	dir, ackDir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR"), os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR")
	if dir == "" || ackDir == "" {
		return nil
	}
	activation, err := n.confirmedGroupActivation(p.Command.Members[0].ProposalID)
	if err != nil || activation == nil {
		return err
	}
	accounts := []string{}
	members := map[string]bool{}
	for _, a := range activation.Members {
		accounts = append(accounts, a.Account)
		members[a.Account] = true
	}
	// Snapshot the union from one locked queue, preserving queue order.
	stageStart := time.Now()
	work := n.CurChain.Txpool.SnapshotGroupMigrationWork(accounts)
	pending := work.Transactions
	contactPending, recoveryDue = work.ContactPending, work.RecoveryDue
	snapshotTime = time.Since(stageStart)
	// Retired groups keep forwarding fences for late old-route work, but an
	// empty queue has no ACK to consume. Poll the publication lane above first;
	// never cache emptiness, so a later arrival is handled on the next poll.
	if len(pending) == 0 {
		return nil
	}
	// Clean completed Relay credits and source-local originals using durable
	// evidence. Ordinary completion requires debit AND credit at one root.
	queued, err := n.queueSourceCompletedCredits(pending)
	if err != nil {
		return err
	}
	if queued {
		creditSubmitted = true
		// Proof collected is not durable removal. Do not forward the same
		// already-completed effects while their exact confirmation is in flight.
		return nil
	}
	// The outbox is immutable in this trusted local transport. Validate its
	// indexed contents once per proposal, rather than reopening and resyncing
	// every already-published handoff while the source queue grows.
	stageStart = time.Now()
	existingRecords, err := n.indexedHandoffs(dir, nil)
	if err != nil {
		return err
	}
	existing := make(map[string]*core.MigrationHandoff, len(existingRecords))
	acks, err := n.indexedArtifacts(ackDir, ".ack.json")
	if err != nil {
		return err
	}
	relays, err := n.indexedArtifacts(ackDir, ".relay-ack.json")
	if err != nil {
		return err
	}
	for _, h := range existingRecords {
		existing[h.ID] = h
	}
	indexTime = time.Since(stageStart)
	stageStart = time.Now()
	var publish []*core.MigrationHandoff
	bytesQueued := 512
	maxItems := int(n.CurChain.ChainConfig.BlockSize)
	if maxItems <= 0 {
		maxItems = 1
	}
	for _, tx := range pending {
		// A committed local effect is being durably removed, not forwarded.
		if effect, e := core.BuildTransactionEffect(tx, core.TransactionCredit); e == nil &&
			n.sourceConfirmationLane().blocks("source-credit/"+effect.ID) {
			continue
		}
		owner := tx.Sender
		if tx.Relayed {
			owner = tx.Recipient
		}
		if !members[strings.TrimPrefix(strings.ToLower(strings.TrimSpace(owner)), "0x")] {
			continue
		} // The separate inbound path commits the source debit first.
		first := p.Members[0].Command
		h, e := core.BuildGroupMigrationHandoff(tx, first.ProposalID, accounts, first.SourceShard, first.TargetShard, activation.Members[0].MappingVersion)
		if e != nil {
			return e
		}
		if lane.blocks(h.ID) {
			continue
		} // visible link is not yet durable success
		if previous := existing[h.ID]; previous != nil {
			a, _ := json.Marshal(previous)
			b, _ := json.Marshal(h)
			if !bytes.Equal(a, b) {
				return fmt.Errorf("immutable group handoff conflict %s", h.ID)
			}
		} else {
			if busy {
				continue
			} // bounded single flight; do not queue another job
			raw, e := json.Marshal(h)
			if e != nil {
				return e
			}
			if len(raw)+1024 > 8<<20 {
				return fmt.Errorf("source handoff exceeds publication batch size")
			}
			if len(publish) < maxItems && bytesQueued+len(raw)+128 <= 8<<20 {
				publish = append(publish, h)
				bytesQueued += len(raw) + 128
			}
			continue // never acknowledge an unpublished or in-flight job
		}
		if n.sourceConfirmationLane().active() {
			continue // Do not accumulate an unbounded confirmation backlog.
		}
		raw, e := acks.Read(h.ID)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		var ack core.HandoffAck
		if e = json.Unmarshal(raw, &ack); e != nil {
			return e
		}
		if ack.RelayPending {
			raw, e = relays.Read(h.ID)
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return e
			}
			var final core.HandoffRelayAck
			if e = json.Unmarshal(raw, &final); e != nil {
				return e
			}
			completed = append(completed, core.CompletedHandoffConfirmation{H: h, Ack: ack, Relay: &final})
		} else {
			completed = append(completed, core.CompletedHandoffConfirmation{H: h, Ack: ack})
		}
	}
	buildTime = time.Since(stageStart)
	stageStart = time.Now()
	if queued, err := n.queueCompletedHandoffs(dir, completed); err != nil {
		return err
	} else if queued {
		confirmationSubmitted = len(completed)
		if confirmationSubmitted > maxItems {
			confirmationSubmitted = maxItems
		}
	}
	confirmTime = time.Since(stageStart)
	if len(publish) > 0 {
		// h is built from a deep-copied queue transaction. The worker captures
		// only owned records and the transport directory, never the node/chain.
		if n.stopSignal.Load() {
			return nil
		}
		ids := make([]string, len(publish))
		for i, h := range publish {
			ids[i] = h.ID
		}
		if !lane.start(ids, func() error { return core.PublishSourceHandoffBatch(dir, publish) }) {
			if n.stopSignal.Load() {
				return nil
			}
			return fmt.Errorf("concurrent source publication submission")
		}
		submitted = len(publish)
	}
	return nil
}
