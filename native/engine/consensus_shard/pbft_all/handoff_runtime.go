package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func (n *PbftConsensusNode) pollSourceHandoff() error {
	if err := n.pollSourceConfirmation(); err != nil {
		return err
	}
	if n.CurChain.GroupSourcePreparation() != nil {
		return n.pollGroupSourceHandoff()
	}
	dir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR")
	if dir == "" {
		return nil
	}
	p := n.CurChain.SourcePreparation()
	if p == nil {
		return nil
	}
	// Retirement preserves forwarding responsibility for late old-route input.
	// Only an empty current queue plus a matching committed retirement may idle.
	pending := n.CurChain.Txpool.SnapshotMigrationTransactions(p.Command.Account)
	if len(pending) == 0 {
		retired, err := n.CurChain.SourceRetirement()
		if err != nil {
			return err
		}
		if retired != nil {
			a := retired.Command.Activation
			if a.ProposalID != p.Command.ProposalID || a.Account != p.Command.Account || a.TargetShard != p.Command.TargetShard || a.StateHash != p.StateHash {
				return fmt.Errorf("source retirement/preparation mismatch")
			}
			return nil
		}
	}
	raw, err := os.ReadFile(os.Getenv("SAA_NATIVE_TARGET_ACTIVATION_PATH"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var active chain.TargetActivation
	if err = json.Unmarshal(raw, &active); err != nil {
		return err
	}
	c := p.Command
	if active.ProposalID != c.ProposalID || active.Account != c.Account || active.TargetShard != c.TargetShard || active.StateHash != p.StateHash || active.MappingVersion <= c.MappingVersion {
		return fmt.Errorf("source handoff activation mismatch")
	}
	if confirmed, err := targetActivationConfirmed(active); err != nil || !confirmed {
		return err
	}
	pendingIDs := make(map[string]bool, len(pending))
	for _, tx := range pending {
		if isInboundCandidate(tx, c.Account) {
			continue
		}
		h, err := core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, c.SourceShard, c.TargetShard, active.MappingVersion)
		if err != nil {
			return err
		}
		if err = n.publishSourceHandoff(dir, h); err != nil {
			return err
		}
		pendingIDs[h.ID] = true
	}
	ackDir := os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR")
	if ackDir == "" {
		return nil
	}
	records, err := n.indexedHandoffs(dir, func(h *core.MigrationHandoff) bool {
		return pendingIDs[h.ID] && h.ProposalID == c.ProposalID && h.SourceShard == n.ShardID
	})
	if err != nil {
		return err
	}
	var completedBatch []core.HandoffConfirmation
	acks, err := n.indexedArtifacts(ackDir, ".ack.json")
	if err != nil {
		return err
	}
	relays, err := n.indexedArtifacts(ackDir, ".relay-ack.json")
	if err != nil {
		return err
	}
	for _, h := range records {
		if h.ProposalID != c.ProposalID || h.SourceShard != n.ShardID {
			continue
		}
		raw, err := acks.Read(h.ID)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		var ack core.HandoffAck
		if err = json.Unmarshal(raw, &ack); err != nil {
			return err
		}
		if ack.RelayPending {
			raw, err := relays.Read(h.ID)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			var final core.HandoffRelayAck
			if err = json.Unmarshal(raw, &final); err != nil {
				return err
			}
			if err = n.CurChain.Txpool.ConfirmMigrationHandoff(dir, h, ack, final); err != nil {
				return err
			}
			continue
		}
		if os.Getenv("SAA_NATIVE_HANDOFF_BATCH_V1") == "true" {
			completedBatch = append(completedBatch, core.HandoffConfirmation{H: h, Ack: ack})
			continue
		}
		if err = n.CurChain.Txpool.ConfirmMigrationHandoff(dir, h, ack); err != nil {
			return err
		}
	}
	return n.CurChain.Txpool.ConfirmMigrationHandoffBatch(dir, completedBatch)
}
func (n *PbftConsensusNode) nextHandoffInput() (*core.MigrationHandoff, error) {
	dir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR")
	if dir == "" {
		return nil, nil
	}
	records, err := n.indexedHandoffs(dir, func(h *core.MigrationHandoff) bool {
		if h.TargetShard != n.ShardID {
			return false
		}
		v, seen := n.sourcePrepareSeen.Load("handoff:" + h.ID)
		return !seen || v != true
	})
	if err != nil {
		return nil, err
	}
	for _, h := range records {
		if h.TargetShard != n.ShardID {
			continue
		}
		if v, seen := n.sourcePrepareSeen.Load("handoff:" + h.ID); !seen || v != true {
			return h, nil
		}
	}
	return nil, nil
}
func (n *PbftConsensusNode) handoffProposal() (*message.Request, error) {
	if os.Getenv("SAA_NATIVE_HANDOFF_BATCH_V1") == "true" {
		return n.fairHandoffProposal()
	}
	if r, err := n.inboundProposal(); err != nil || r != nil {
		return r, err
	}
	if r, err := n.relayProposal(); err != nil || r != nil {
		return r, err
	}
	return n.migrationExecutionProposal()
}

func (n *PbftConsensusNode) migrationExecutionProposal() (*message.Request, error) {
	if os.Getenv("SAA_NATIVE_HANDOFF_BATCH_V1") == "true" {
		return n.handoffBatchProposal()
	}
	h, err := n.nextHandoffInput()
	if err != nil || h == nil {
		return nil, err
	}
	active, err := n.CurChain.TargetActivated(h.Account)
	if err != nil || !active {
		return nil, err
	}
	if err = n.CurChain.ValidateMigrationHandoffExecution(*h); err != nil {
		return nil, err
	}
	data, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	return &message.Request{RequestType: migrationHandoffRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: data}}, nil
}
