package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

func isInboundCandidate(tx *core.Transaction, account string) bool {
	return tx != nil && !tx.Relayed && !tx.HasBroker && strings.TrimPrefix(tx.Recipient, "0x") == account && strings.TrimPrefix(tx.Sender, "0x") != account
}
func (n *PbftConsensusNode) inboundProposal() (*message.Request, error) {
	if n.CurChain.GroupSourcePreparation() != nil {
		return n.groupInboundProposal()
	}
	dir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR")
	if dir == "" {
		return nil, nil
	}
	p := n.CurChain.SourcePreparation()
	if p == nil {
		return nil, nil
	}
	raw, err := os.ReadFile(os.Getenv("SAA_NATIVE_TARGET_ACTIVATION_PATH"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var a chain.TargetActivation
	if err = json.Unmarshal(raw, &a); err != nil {
		return nil, err
	}
	if a.ProposalID != p.Command.ProposalID || a.Account != p.Command.Account || a.StateHash != p.StateHash || a.TargetShard != p.Command.TargetShard || a.MappingVersion <= p.Command.MappingVersion {
		return nil, fmt.Errorf("inbound activation mismatch")
	}
	if confirmed, err := targetActivationConfirmed(a); err != nil || !confirmed {
		return nil, err
	}
	acks, err := n.indexedArtifacts(os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR"), ".ack.json")
	if err != nil {
		return nil, err
	}
	outbox, err := n.indexedArtifacts(dir, ".handoff.json")
	if err != nil {
		return nil, err
	}
	for _, tx := range n.CurChain.Txpool.SnapshotMigrationTransactions(p.Command.Account) {
		if !isInboundCandidate(tx, p.Command.Account) {
			continue
		}
		record, err := n.CurChain.FindInboundDebit(tx, a.MappingVersion)
		if err != nil {
			return nil, err
		}
		if record != nil {
			if err = n.ensureInboundPublication(dir, outbox, &record.Credit); err != nil {
				return nil, err
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
			if err = n.CurChain.Txpool.ConfirmInboundHandoff(dir, &record.Credit, ack, tx); err != nil {
				return nil, err
			}
			continue
		}
		if err = n.CurChain.ValidateInboundDebit(tx, a.MappingVersion); err != nil {
			return nil, err
		}
		data, err := json.Marshal(inboundDebitCommand{Transaction: tx, MappingVersion: a.MappingVersion})
		if err != nil {
			return nil, err
		}
		return &message.Request{RequestType: inboundDebitRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: data}}, nil
	}
	return nil, nil
}
