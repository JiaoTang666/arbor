package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const handoffRelayRequest message.RequestType = "SEFHandoffRelayV1"

func decodeHandoffRelay(r *message.Request) (chain.HandoffExecution, error) {
	var first chain.HandoffExecution
	if r == nil || r.RequestType != handoffRelayRequest {
		return first, fmt.Errorf("wrong relay request")
	}
	err := json.Unmarshal(r.Msg.Content, &first)
	return first, err
}
func (n *PbftConsensusNode) commitHandoffRelay(r *message.Request, digest []byte) error {
	return n.commitHandoffRelayPublishing(r, digest, core.PublishSourcePrepareFile, func(key string) { n.sourcePrepareSeen.Store(key, true) })
}
func (n *PbftConsensusNode) commitHandoffRelayPublishing(r *message.Request, digest []byte, publish func(string, []byte) error, mark func(string)) error {
	if string(getDigest(r)) != string(digest) {
		return fmt.Errorf("relay digest mismatch")
	}
	first, err := decodeHandoffRelay(r)
	if err != nil {
		return err
	}
	record, err := n.CurChain.CommitHandoffRelay(first)
	if err != nil {
		return err
	}
	dir := os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR")
	if dir != "" && n.NodeID == uint64(n.view.Load()) {
		block := n.CurChain.CurrentBlock
		for block.Header.Number > record.Block {
			block, err = n.CurChain.Storage.GetBlock(block.Header.ParentBlockHash)
			if err != nil {
				return err
			}
		}
		hash := sha256.Sum256(first.Relay)
		if block.Header.Number != record.Block {
			return fmt.Errorf("relay execution block unavailable")
		}
		ack := core.HandoffRelayAck{RelayPayload: first.Relay, ID: first.Handoff.ID, RelayHash: hex.EncodeToString(hash[:]), RecipientShard: n.ShardID, Block: record.Block, BlockHash: hex.EncodeToString(block.Hash), StateRoot: hex.EncodeToString(block.Header.StateRoot), Committed: true}
		data, err := json.Marshal(ack)
		if err != nil {
			return err
		}
		if err = publish(filepath.Join(dir, ack.ID+".relay-ack.json"), append(data, '\n')); err != nil {
			return err
		}
	}
	mark("relay:" + first.Handoff.ID)
	return nil
}
