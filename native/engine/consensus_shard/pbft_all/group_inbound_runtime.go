package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"os"
	"strings"
	"time"
)

func groupInboundOnSource(tx *core.Transaction, members map[string]bool, source uint64) bool {
	canonical := func(s string) string { return strings.TrimPrefix(strings.ToLower(strings.TrimSpace(s)), "0x") }
	return tx != nil && !tx.Relayed && !tx.HasBroker && !members[canonical(tx.Sender)] &&
		members[canonical(tx.Recipient)] && tx.RoutingFrozen &&
		tx.RoutedSenderShard == source && tx.RoutedRecipientShard == source
}

func (n *PbftConsensusNode) groupInboundProposal() (*message.Request, error) {
	for _, p := range n.CurChain.GroupSourcePreparations() {
		r, err := n.oneGroupInboundProposal(p.Command.Members[0].ProposalID)
		if err != nil || r != nil {
			return r, err
		}
	}
	return nil, nil
}

func (n *PbftConsensusNode) oneGroupInboundProposal(id string) (*message.Request, error) {
	started := time.Now()
	var completed []core.CompletedHandoffConfirmation
	defer func() {
		if n.pl != nil && (len(completed) > 0 || time.Since(started) > time.Second) {
			n.pl.Plog.Printf("group inbound ACK batch count=%d elapsedMs=%d", len(completed), time.Since(started).Milliseconds())
		}
	}()
	dir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR")
	if dir == "" {
		return nil, nil
	}
	a, err := n.confirmedGroupActivation(id)
	if err != nil || a == nil {
		return nil, err
	}
	accounts := []string{}
	members := map[string]bool{}
	for _, m := range a.Members {
		accounts = append(accounts, m.Account)
		members[m.Account] = true
	}
	version := a.Members[0].MappingVersion
	pending := n.CurChain.Txpool.SnapshotGroupMigrationTransactions(accounts)
	if len(pending) == 0 {
		return nil, nil
	}
	acks, err := n.indexedArtifacts(os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR"), ".ack.json")
	if err != nil {
		return nil, err
	}
	outbox, err := n.indexedArtifacts(dir, ".handoff.json")
	if err != nil {
		return nil, err
	}
	batch := []chain.InboundDebitItem{}
	heldSenders := map[string]bool{}
	for _, p := range n.CurChain.GroupSourcePreparations() {
		for _, m := range p.Members {
			heldSenders[m.Command.Account] = true
		}
	}
	size := 2
	for _, tx := range pending {
		if !groupInboundOnSource(tx, members, n.ShardID) || heldSenders[strings.TrimPrefix(strings.ToLower(tx.Sender), "0x")] {
			continue
		}
		record, err := n.CurChain.FindInboundDebit(tx, version)
		if err != nil {
			return nil, err
		}
		if record != nil {
			if err = n.ensureInboundPublication(dir, outbox, &record.Credit); err != nil {
				return nil, err
			}
			if n.sourceConfirmationLane().active() {
				continue
			}
			ackDir := os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR")
			if ackDir == "" {
				continue
			}
			raw, err := acks.Read(record.Credit.ID)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			var ack core.HandoffAck
			if err = json.Unmarshal(raw, &ack); err != nil {
				return nil, err
			}
			completed = append(completed, core.CompletedHandoffConfirmation{H: &record.Credit, Ack: ack, InboundOriginal: tx})
			continue
		}
		if err = n.CurChain.ValidateInboundDebit(tx, version); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(inboundDebitCommand{Transaction: tx, MappingVersion: version})
		if err != nil {
			return nil, err
		}
		if os.Getenv("SAA_NATIVE_HANDOFF_BATCH_V1") != "true" {
			if _, err = n.queueCompletedHandoffs(dir, completed); err != nil {
				return nil, err
			}
			return &message.Request{RequestType: inboundDebitRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: raw}}, nil
		}
		if size+len(raw)+1 > 8<<20 {
			break
		}
		batch = append(batch, chain.InboundDebitItem{Transaction: tx, MappingVersion: version})
		size += len(raw) + 1
		if uint64(len(batch)) >= n.CurChain.ChainConfig.BlockSize {
			break
		}
	}
	if _, err = n.queueCompletedHandoffs(dir, completed); err != nil {
		return nil, err
	}
	if len(batch) > 0 {
		if err = n.CurChain.ValidateInboundDebitBatch(batch); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(batch)
		if err != nil {
			return nil, err
		}
		return &message.Request{RequestType: inboundDebitBatchRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: raw}}, nil
	}
	return nil, nil
}
