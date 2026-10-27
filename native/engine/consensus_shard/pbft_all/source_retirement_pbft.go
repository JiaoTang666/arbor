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
)

const sourceRetirementRequest message.RequestType = "SEFSourceRetireV1"

type sourceRetirementEnvelope struct {
	Command           chain.SourceRetirementCommand `json:"command"`
	ActivationReceipt []byte                        `json:"activation_receipt"`
	DrainSnapshot     []byte                        `json:"drain_snapshot"`
}

// Proofs are trusted-local witnesses, not Byzantine cross-shard certificates.
// Carry immutable bytes so replay does not depend on today's queue contents.
func (n *PbftConsensusNode) validateSourceRetirementRequest(r *message.Request) (sourceRetirementEnvelope, error) {
	var e sourceRetirementEnvelope
	if r == nil || r.RequestType != sourceRetirementRequest {
		return e, fmt.Errorf("wrong source retirement request")
	}
	if err := json.Unmarshal(r.Msg.Content, &e); err != nil {
		return e, err
	}
	receiptHash, snapshotHash := sha256.Sum256(e.ActivationReceipt), sha256.Sum256(e.DrainSnapshot)
	if hex.EncodeToString(receiptHash[:]) != e.Command.ActivationReceiptHash || hex.EncodeToString(snapshotHash[:]) != e.Command.Drain.SnapshotHash {
		return e, fmt.Errorf("retirement proof hash mismatch")
	}
	var receipt struct {
		Activation chain.TargetActivation `json:"activation"`
		Block      uint64                 `json:"block"`
		BlockHash  string                 `json:"block_hash"`
		StateRoot  string                 `json:"state_root"`
	}
	if err := json.Unmarshal(e.ActivationReceipt, &receipt); err != nil {
		return e, err
	}
	if receipt.Activation != e.Command.Activation || receipt.Block == 0 {
		return e, fmt.Errorf("retirement activation mismatch")
	}
	for _, s := range []string{receipt.BlockHash, receipt.StateRoot} {
		b, err := hex.DecodeString(s)
		if err != nil || len(b) != 32 {
			return e, fmt.Errorf("activation proof digest invalid")
		}
	}
	var snapshot struct {
		Account      string
		Sequence     uint64
		Transactions [][]byte
	}
	if err := json.Unmarshal(e.DrainSnapshot, &snapshot); err != nil {
		return e, err
	}
	if snapshot.Account != e.Command.Drain.Account || snapshot.Sequence != e.Command.Drain.Generation || len(snapshot.Transactions) != 0 {
		return e, fmt.Errorf("retirement snapshot not matching drained generation")
	}
	return e, n.CurChain.ValidateSourceRetirement(e.Command)
}

func (n *PbftConsensusNode) commitSourceRetirement(r *message.Request, sequence uint64, digest []byte) error {
	if r == nil || string(getDigest(r)) != string(digest) {
		return fmt.Errorf("retirement request digest mismatch")
	}
	e, err := n.validateSourceRetirementRequest(r)
	if err != nil {
		return err
	}
	record, err := n.CurChain.CommitSourceRetirement(e.Command, sequence, digest)
	if err != nil {
		return err
	}
	return n.publishSourceRetirement(record)
}

func (n *PbftConsensusNode) publishSourceRetirement(record *chain.SourceRetirement) error {
	var err error
	path := os.Getenv("SAA_NATIVE_SOURCE_RETIRED_PATH")
	if path == "" || n.NodeID != uint64(n.view.Load()) {
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
		return fmt.Errorf("retirement block unavailable")
	}
	raw, err := json.Marshal(struct {
		Retirement *chain.SourceRetirement `json:"retirement"`
		BlockHash  string                  `json:"block_hash"`
		StateRoot  string                  `json:"state_root"`
	}{record, hex.EncodeToString(block.Hash), hex.EncodeToString(block.Header.StateRoot)})
	if err != nil {
		return err
	}
	return core.PublishSourcePrepareFile(path, raw)
}
