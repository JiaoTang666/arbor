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
	"time"
)

const relayReserveRequest message.RequestType = "SEFRelayRedirectReserveV1"
const relayCreditRequest message.RequestType = "SEFRelayRedirectCreditV1"

// Same trusted-reporter boundary as installation receipts, not a quorum certificate.
type relayRedirectReport struct {
	Record    chain.RelayRedirect
	Shard     uint64
	BlockHash string
	StateRoot string
	Committed bool
}

func redirectRequest(r *message.Request) bool {
	return r != nil && (r.RequestType == relayReserveRequest || r.RequestType == relayCreditRequest)
}
func makeRedirectRequest(kind message.RequestType, value interface{}) (*message.Request, error) {
	raw, e := json.Marshal(value)
	if e != nil {
		return nil, e
	}
	return &message.Request{RequestType: kind, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: raw}}, nil
}
func (p relayRedirectReport) validate() error {
	r := p.Record
	valid := func(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
	if !p.Committed || r.Block == 0 || p.Shard != r.Child.SourceShard || !valid(p.BlockHash) || !valid(p.StateRoot) {
		return fmt.Errorf("invalid redirect source report")
	}
	return core.ValidateRelayRedirectLink(&r.First.Handoff, r.First.Relay, &r.Child)
}
func (n *PbftConsensusNode) validateRedirect(r *message.Request) error {
	if !redirectRequest(r) || len(r.Msg.Content) > 8<<20 {
		return fmt.Errorf("invalid redirect request")
	}
	if r.RequestType == relayReserveRequest {
		var first chain.HandoffExecution
		if e := json.Unmarshal(r.Msg.Content, &first); e != nil {
			return e
		}
		return n.CurChain.ValidateRelayRedirect(first)
	}
	var p relayRedirectReport
	if e := json.Unmarshal(r.Msg.Content, &p); e != nil {
		return e
	}
	if e := p.validate(); e != nil {
		return e
	}
	return n.CurChain.ValidateMigrationHandoffExecution(p.Record.Child)
}
func (n *PbftConsensusNode) commitRedirect(r *message.Request, digest []byte) error {
	if string(getDigest(r)) != string(digest) {
		return fmt.Errorf("redirect digest mismatch")
	}
	if e := n.validateRedirect(r); e != nil {
		return e
	}
	var height uint64
	var p relayRedirectReport
	var execution *chain.HandoffExecution
	var e error
	if r.RequestType == relayReserveRequest {
		var first chain.HandoffExecution
		json.Unmarshal(r.Msg.Content, &first)
		record, err := n.CurChain.CommitRelayRedirect(first)
		if err != nil {
			return err
		}
		p.Record = *record
		p.Shard = n.ShardID
		p.Committed = true
		height = record.Block
	} else {
		json.Unmarshal(r.Msg.Content, &p)
		execution, e = n.CurChain.CommitRedirectCredit(p.Record)
		if e != nil {
			return e
		}
		height = execution.Block
	}
	block := n.CurChain.CurrentBlock
	for block.Header.Number > height {
		block, e = n.CurChain.Storage.GetBlock(block.Header.ParentBlockHash)
		if e != nil {
			return e
		}
	}
	if block.Header.Number != height {
		return fmt.Errorf("redirect committed block unavailable")
	}
	var path string
	var value interface{}
	var mark string
	if r.RequestType == relayReserveRequest {
		p.BlockHash = hex.EncodeToString(block.Hash)
		p.StateRoot = hex.EncodeToString(block.Header.StateRoot)
		path = filepath.Join(os.Getenv("SAA_NATIVE_HANDOFF_RELAY_DIR"), p.Record.First.Handoff.ID+".redirect.json")
		value = p
		mark = "relay:" + p.Record.First.Handoff.ID
	} else {
		child := p.Record.Child
		first := p.Record.First
		hash := sha256.Sum256(child.Execution)
		ack := core.HandoffAck{ID: child.ID, ProposalID: child.ProposalID, TargetShard: child.TargetShard, ExecutionHash: hex.EncodeToString(hash[:]), Block: height, BlockHash: hex.EncodeToString(block.Hash), StateRoot: hex.EncodeToString(block.Header.StateRoot), Executed: true}
		hash = sha256.Sum256(first.Relay)
		proof := core.RelayRedirectProof{ParentID: first.Handoff.ID, RelayHash: hex.EncodeToString(hash[:]), Child: child, SourceShard: p.Shard, SourceBlock: p.Record.Block, SourceBlockHash: p.BlockHash, SourceStateRoot: p.StateRoot, SourceCommitted: p.Committed, TargetAck: ack}
		result := core.HandoffRelayAck{RelayPayload: first.Relay, ID: first.Handoff.ID, RelayHash: proof.RelayHash, RecipientShard: n.ShardID, Block: height, BlockHash: ack.BlockHash, StateRoot: ack.StateRoot, Committed: true, Redirect: &proof}
		if e = core.ValidateHandoffRelayAck(&first.Handoff, result); e != nil {
			return e
		}
		path = filepath.Join(os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR"), first.Handoff.ID+".relay-ack.json")
		value = result
		mark = "redirect-credit:" + first.Handoff.ID
	}
	if n.NodeID == uint64(n.view.Load()) {
		if filepath.Dir(path) == "." {
			return fmt.Errorf("missing redirect output directory")
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if e = core.PublishSourcePrepareFile(path, append(raw, '\n')); e != nil {
			return e
		}
	}
	n.sourcePrepareSeen.Store(mark, true)
	return nil
}

func (n *PbftConsensusNode) redirectCreditProposal() (*message.Request, error) {
	dir := os.Getenv("SAA_NATIVE_HANDOFF_RELAY_DIR")
	if dir == "" {
		return nil, nil
	}
	entries, e := n.indexedArtifacts(dir, ".redirect.json")
	if e != nil {
		return nil, e
	}
	for _, id := range entries.IDs() {
		var p relayRedirectReport
		if e = json.Unmarshal(entries[id], &p); e != nil {
			return nil, e
		}
		if e = p.validate(); e != nil {
			return nil, e
		}
		if p.Record.Child.TargetShard != n.ShardID {
			continue
		}
		if done, _ := n.sourcePrepareSeen.Load("redirect-credit:" + p.Record.First.Handoff.ID); done == true {
			continue
		}
		height, err := n.CurChain.GroupTargetActivationBlock(p.Record.Child.ProposalID)
		if err != nil {
			return nil, err
		}
		if height == 0 {
			continue
		}
		r, err := makeRedirectRequest(relayCreditRequest, p)
		if err != nil {
			return nil, err
		}
		if err = n.validateRedirect(r); err != nil {
			return nil, err
		}
		return r, nil
	}
	return nil, nil
}
