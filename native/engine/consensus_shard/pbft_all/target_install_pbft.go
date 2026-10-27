package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

const targetInstallRequest message.RequestType = "SEFTargetInstallV1"

func decodeTargetInstall(r *message.Request) (core.SourcePreparation, error) {
	var p core.SourcePreparation
	if r == nil || r.RequestType != targetInstallRequest {
		return p, fmt.Errorf("wrong target request type")
	}
	err := json.Unmarshal(r.Msg.Content, &p)
	return p, err
}
func (n *PbftConsensusNode) commitTargetInstall(r *message.Request, sequence uint64, digest []byte) error {
	if string(getDigest(r)) != string(digest) {
		return fmt.Errorf("target request digest mismatch")
	}
	p, err := decodeTargetInstall(r)
	if err != nil {
		return err
	}
	installed, err := n.CurChain.CommitPreparedTarget(p, sequence, digest)
	if err != nil {
		return err
	}
	path := os.Getenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH")
	if path == "" || n.NodeID != uint64(n.view.Load()) {
		return nil
	}
	c := p.Command
	record, err := committedInstallationRecord(n.CurChain, message.StateTransferEnvelope{ProposalID: c.ProposalID, Account: c.Account, SourceShard: c.SourceShard, TargetShard: c.TargetShard, MappingVersion: c.MappingVersion, AnchorVersion: c.AnchorVersion, SourceStateRoot: p.SourceStateRoot, StateHash: p.StateHash}, n.ShardID, sequence, digest)
	if err != nil {
		return err
	}
	// Duplicate publication must name the ORIGINAL installation block/root.
	block := n.CurChain.CurrentBlock
	for block.Header.Number > installed.InstallBlock {
		block, err = n.CurChain.Storage.GetBlock(block.Header.ParentBlockHash)
		if err != nil {
			return err
		}
	}
	if block.Header.Number != installed.InstallBlock {
		return fmt.Errorf("installation block unavailable")
	}
	record["install_block"] = installed.InstallBlock
	record["target_state_root"] = hex.EncodeToString(block.Header.StateRoot)
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return appendCommittedReceipt(path, append(data, '\n'))
}
