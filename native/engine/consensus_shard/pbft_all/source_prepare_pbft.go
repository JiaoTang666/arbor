package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const sourcePrepareRequest message.RequestType = "SEFSourcePrepareV1"

// Idle polling reads only immutable input, never the mutable chain head.
// Pending inputs remain eligible; only rejected or completed inputs stop waking.
func (n *PbftConsensusNode) hasUnattemptedSourcePreparation() bool {
	if _, ok := n.ihm.(*RawRelayPbftExtraHandleMod); !ok {
		return false
	}
	if n.hasGroupInput() {
		return true
	}
	if n.hasTargetInstallInput() {
		return true
	}
	if n.hasTargetActivationInput() {
		return true
	}
	if h, err := n.nextHandoffInput(); err == nil && h != nil {
		return true
	}
	if r, err := n.nextRelayInput(); err == nil && r != nil {
		return true
	}
	path := os.Getenv("SAA_NATIVE_SOURCE_PREPARE_PATH")
	if path == "" {
		return false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var c core.SourcePrepareCommand
	if json.Unmarshal(data, &c) != nil || c.SourceShard != n.ShardID {
		return false
	}
	if retiredPath := os.Getenv("SAA_NATIVE_SOURCE_RETIRED_PATH"); retiredPath != "" {
		if _, err := os.Stat(retiredPath); os.IsNotExist(err) {
			return true
		}
	}
	value, seen := n.sourcePrepareSeen.Load(sha256.Sum256(data))
	_, pending := value.(*message.Request)
	return !seen || pending
}

func decodeSourcePrepare(r *message.Request) (core.SourcePrepareCommand, error) {
	var c core.SourcePrepareCommand
	if r == nil || r.RequestType != sourcePrepareRequest {
		return c, fmt.Errorf("wrong preparation request type")
	}
	err := json.Unmarshal(r.Msg.Content, &c)
	return c, err
}

// Called under the normal PBFT proposal locks, only at a completed-sequence
// boundary. No route mutation or state export is performed by the file reader.
func (n *PbftConsensusNode) sourcePrepareProposal() (*message.Request, error) {
	if err := n.exportSourceStateObservation(); err != nil {
		return nil, err
	}
	path := os.Getenv("SAA_NATIVE_SOURCE_PREPARE_PATH")
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var c core.SourcePrepareCommand
	if err = json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.SourceShard != n.ShardID {
		return nil, nil
	}
	fingerprint := sha256.Sum256(data)
	if value, seen := n.sourcePrepareSeen.Load(fingerprint); seen && value == true {
		return nil, nil
	}
	// Keep a bound request immutable across retries. New snapshots must never
	// change the identity of a request already proposed in this sequence.
	if value, seen := n.sourcePrepareSeen.Load(fingerprint); seen {
		if request, ok := value.(*message.Request); ok {
			return request, nil
		}
	}
	if c.BindAtProposal {
		if c.ExpectedBlockHash != "" || c.ExpectedStateRoot != "" || c.ExpectedStateHash != "" {
			return nil, fmt.Errorf("proposal-bound intent must not supply snapshot hashes")
		}
		if old := n.CurChain.SourcePreparation(); old != nil {
			intent := old.Command
			intent.ExpectedBlockHash, intent.ExpectedStateRoot, intent.ExpectedStateHash = "", "", ""
			if intent != c {
				return nil, fmt.Errorf("prepared intent conflict")
			}
			c = old.Command
		} else {
			state, err := n.CurChain.ExportAccountState(c.Account)
			if err != nil {
				return nil, err
			}
			c.ExpectedBlockHash = hex.EncodeToString(n.CurChain.CurrentBlock.Hash)
			c.ExpectedStateRoot = hex.EncodeToString(state.SourceRoot)
			c.ExpectedStateHash = hex.EncodeToString(state.StateHash)
		}
		data, err = json.Marshal(c)
		if err != nil {
			return nil, err
		}
	}
	if err = n.CurChain.ValidateSourcePreparation(c); err != nil {
		n.sourcePrepareSeen.Store(fingerprint, true)
		return nil, err
	}
	if old := n.CurChain.SourcePreparation(); old != nil {
		// Repair a crash between committed preparation and result publication.
		err = n.publishSourcePrepared(old)
		if err == nil {
			n.sourcePrepareSeen.Store(fingerprint, true)
		}
		return nil, err
	}
	if value, seen := n.sourcePrepareSeen.Load(fingerprint); seen {
		if request, ok := value.(*message.Request); ok {
			return request, nil
		}
	}
	// Control-request time is a fixed identity field, not a workload timestamp.
	// Reconstructing the same immutable inbox must reproduce its digest.
	request := &message.Request{RequestType: sourcePrepareRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: data}}
	n.sourcePrepareSeen.Store(fingerprint, request)
	return request, nil
}

func (n *PbftConsensusNode) publishSourcePrepared(p *core.SourcePreparation) error {
	if n.NodeID != uint64(n.view.Load()) {
		return nil
	}
	path := os.Getenv("SAA_NATIVE_SOURCE_PREPARED_PATH")
	if path == "" {
		return nil
	}
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return core.PublishSourcePrepareFile(path, append(data, '\n'))
}

func (n *PbftConsensusNode) commitSourcePrepare(r *message.Request, sequence uint64, digest []byte) error {
	c, err := decodeSourcePrepare(r)
	if err != nil {
		return err
	}
	if string(getDigest(r)) != string(digest) {
		return fmt.Errorf("preparation request digest mismatch")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if err = n.CurChain.ValidateSourcePreparation(c); err != nil {
		return err
	}
	if err = n.prepareSourceQueue(c); err != nil {
		return err
	}
	p, err := n.CurChain.CommitSourcePreparation(c, sequence, digest, raw)
	if err != nil {
		return err
	}
	if err = n.publishSourcePrepared(p); err != nil {
		return err
	}
	n.sourcePrepareSeen.Store(sha256.Sum256(r.Msg.Content), true)
	if c.BindAtProposal {
		intent := c
		intent.ExpectedBlockHash, intent.ExpectedStateRoot, intent.ExpectedStateHash = "", "", ""
		rawIntent, err := json.Marshal(intent)
		if err != nil {
			return err
		}
		// The original inbox may contain insignificant JSON whitespace; bind by
		// decoded identity as well as the exact bytes used by the ingress cache.
		n.sourcePrepareSeen.Store(sha256.Sum256(rawIntent), true)
		if rawInbox, e := os.ReadFile(os.Getenv("SAA_NATIVE_SOURCE_PREPARE_PATH")); e == nil {
			var inbox core.SourcePrepareCommand
			if json.Unmarshal(rawInbox, &inbox) == nil && inbox == intent {
				n.sourcePrepareSeen.Store(sha256.Sum256(rawInbox), true)
			}
		}
	}
	return nil
}
