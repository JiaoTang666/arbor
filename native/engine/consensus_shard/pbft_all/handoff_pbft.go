package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const migrationHandoffRequest message.RequestType = "SEFMigrationHandoffV1"

func decodeMigrationHandoff(r *message.Request) (core.MigrationHandoff, error) {
	var h core.MigrationHandoff
	if r == nil || r.RequestType != migrationHandoffRequest {
		return h, fmt.Errorf("wrong handoff request")
	}
	err := json.Unmarshal(r.Msg.Content, &h)
	return h, err
}
func (n *PbftConsensusNode) commitMigrationHandoff(r *message.Request, digest []byte) error {
	return n.commitMigrationHandoffPublishing(r, digest, core.PublishSourcePrepareFile, func(key string) { n.sourcePrepareSeen.Store(key, true) })
}
func (n *PbftConsensusNode) commitMigrationHandoffPublishing(r *message.Request, digest []byte, publish func(string, []byte) error, mark func(string)) error {
	if string(getDigest(r)) != string(digest) {
		return fmt.Errorf("handoff digest mismatch")
	}
	h, err := decodeMigrationHandoff(r)
	if err != nil {
		return err
	}
	record, err := n.CurChain.CommitMigrationHandoff(h)
	if err != nil {
		return err
	}
	directory := os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR")
	if relayDir := os.Getenv("SAA_NATIVE_HANDOFF_RELAY_DIR"); relayDir != "" && len(record.Relay) > 0 && n.NodeID == uint64(n.view.Load()) {
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err = publish(filepath.Join(relayDir, h.ID+".relay.json"), append(raw, '\n')); err != nil {
			return err
		}
	}
	if directory == "" || n.NodeID != uint64(n.view.Load()) {
		mark("handoff:" + h.ID)
		return nil
	}
	block := n.CurChain.CurrentBlock
	for block.Header.Number > record.Block {
		block, err = n.CurChain.Storage.GetBlock(block.Header.ParentBlockHash)
		if err != nil {
			return err
		}
	}
	if block.Header.Number != record.Block {
		return fmt.Errorf("handoff execution block unavailable")
	}
	hash := sha256.Sum256(h.Execution)
	ack := core.HandoffAck{ID: h.ID, ProposalID: h.ProposalID, TargetShard: h.TargetShard, ExecutionHash: hex.EncodeToString(hash[:]), Block: record.Block, BlockHash: hex.EncodeToString(block.Hash), StateRoot: hex.EncodeToString(block.Header.StateRoot), Executed: true, RelayPending: len(record.Relay) > 0}
	data, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	if err = publish(filepath.Join(directory, h.ID+".ack.json"), append(data, '\n')); err != nil {
		return err
	}
	mark("handoff:" + h.ID)
	return nil
}
