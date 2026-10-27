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
	"time"
)

const targetActivationRequest message.RequestType = "SEFTargetActivateV1"

func decodeTargetActivation(r *message.Request) (chain.TargetActivation, error) {
	var c chain.TargetActivation
	if r == nil || r.RequestType != targetActivationRequest {
		return c, fmt.Errorf("wrong activation request")
	}
	err := json.Unmarshal(r.Msg.Content, &c)
	return c, err
}
func (n *PbftConsensusNode) targetActivationInput() ([]byte, bool) {
	path := os.Getenv("SAA_NATIVE_TARGET_ACTIVATION_PATH")
	if path == "" {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var c chain.TargetActivation
	if json.Unmarshal(data, &c) != nil || c.TargetShard != n.ShardID {
		return nil, false
	}
	return data, true
}
func (n *PbftConsensusNode) hasTargetActivationInput() bool {
	data, ok := n.targetActivationInput()
	if !ok {
		return false
	}
	v, seen := n.sourcePrepareSeen.Load(sha256.Sum256(data))
	return !seen || v != true
}
func (n *PbftConsensusNode) targetActivationProposal() (*message.Request, error) {
	data, ok := n.targetActivationInput()
	if !ok {
		return nil, nil
	}
	key := sha256.Sum256(data)
	if v, seen := n.sourcePrepareSeen.Load(key); seen && v == true {
		return nil, nil
	}
	r := &message.Request{RequestType: targetActivationRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: data}}
	c, err := decodeTargetActivation(r)
	if err != nil {
		return nil, err
	}
	if err = n.CurChain.ValidateTargetActivation(c); err != nil {
		return nil, err
	}
	active, err := n.CurChain.TargetActivated(c.Account)
	if err != nil {
		return nil, err
	}
	if active {
		if err := n.publishTargetActivationReceipt(c); err != nil {
			return nil, err
		}
		n.sourcePrepareSeen.Store(key, true)
		return nil, nil
	}
	return r, nil
}
func (n *PbftConsensusNode) commitTargetActivation(r *message.Request, digest []byte) error {
	if string(getDigest(r)) != string(digest) {
		return fmt.Errorf("activation digest mismatch")
	}
	c, err := decodeTargetActivation(r)
	if err != nil {
		return err
	}
	if c.TargetShard != n.ShardID {
		return fmt.Errorf("activation target mismatch")
	}
	if err = n.CurChain.CommitTargetActivation(c); err != nil {
		return err
	}
	if err = n.publishTargetActivationReceipt(c); err != nil {
		return err
	}
	n.sourcePrepareSeen.Store(sha256.Sum256(r.Msg.Content), true)
	return nil
}

func (n *PbftConsensusNode) publishTargetActivationReceipt(c chain.TargetActivation) error {
	path := os.Getenv("SAA_NATIVE_TARGET_ACTIVATED_PATH")
	if path == "" || n.NodeID != uint64(n.view.Load()) {
		return nil
	}
	block, err := n.CurChain.TargetActivationCommit(c)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(struct {
		Activation chain.TargetActivation `json:"activation"`
		Block      uint64                 `json:"block"`
		BlockHash  string                 `json:"block_hash"`
		StateRoot  string                 `json:"state_root"`
	}{c, block.Header.Number, hex.EncodeToString(block.Hash), hex.EncodeToString(block.Header.StateRoot)})
	if err != nil {
		return err
	}
	return core.PublishSourcePrepareFile(path, raw)
}
