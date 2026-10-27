package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const groupPrepareRequest message.RequestType = "SEFGroupPrepareV1"
const groupInstallRequest message.RequestType = "SEFGroupInstallV1"
const groupActivateRequest message.RequestType = "SEFGroupActivateV1"

func groupKind(k message.RequestType) bool {
	return k == groupPrepareRequest || k == groupInstallRequest || k == groupActivateRequest || k == groupRetireRequest
}

func groupInbox(k message.RequestType) string {
	switch k {
	case groupPrepareRequest:
		return os.Getenv("SAA_GROUP_SOURCE_PREPARE_PATH")
	case groupInstallRequest:
		return os.Getenv("SAA_GROUP_TARGET_INSTALL_PATH")
	case groupActivateRequest:
		return os.Getenv("SAA_GROUP_TARGET_ACTIVATION_PATH")
	}
	return ""
}

// Idle check only reads immutable inboxes, never the mutable chain head.
func (n *PbftConsensusNode) hasGroupInput() bool {
	for _, k := range []message.RequestType{groupPrepareRequest, groupInstallRequest, groupActivateRequest} {
		for _, path := range core.GroupTransportPaths(groupInbox(k)) {
			raw, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			if !n.groupForShard(k, raw) {
				continue
			}
			if k == groupPrepareRequest && os.Getenv("SAA_GROUP_SOURCE_RETIRED_PATH") != "" {
				if _, e := os.Stat(core.GroupTransportPath(os.Getenv("SAA_GROUP_SOURCE_RETIRED_PATH"), groupRequestID(k, raw))); os.IsNotExist(e) {
					return true
				}
			}
			v, seen := n.sourcePrepareSeen.Load(sha256.Sum256(raw))
			if !seen || v != true {
				return true
			}
		}
	}
	return false
}

func (n *PbftConsensusNode) groupForShard(k message.RequestType, raw []byte) bool {
	switch k {
	case groupPrepareRequest:
		var c core.GroupPrepareCommand
		return json.Unmarshal(raw, &c) == nil && len(c.Members) > 0 && c.Members[0].SourceShard == n.ShardID
	case groupInstallRequest:
		var p core.GroupPreparation
		return json.Unmarshal(raw, &p) == nil && len(p.Members) > 0 && p.Members[0].Command.TargetShard == n.ShardID
	case groupActivateRequest:
		var c chain.GroupTargetActivation
		return json.Unmarshal(raw, &c) == nil && len(c.Members) > 0 && c.Members[0].TargetShard == n.ShardID
	}
	return false
}

func (n *PbftConsensusNode) validateGroupRequest(r *message.Request, seq uint64, digest []byte) error {
	if r == nil || !groupKind(r.RequestType) || !bytes.Equal(getDigest(r), digest) {
		return fmt.Errorf("invalid group request digest/type")
	}
	switch r.RequestType {
	case groupRetireRequest:
		var c chain.GroupRetirementCommand
		if e := json.Unmarshal(r.Msg.Content, &c); e != nil {
			return e
		}
		return n.CurChain.ValidateGroupRetirement(c)
	case groupPrepareRequest:
		var c core.GroupPrepareCommand
		if err := json.Unmarshal(r.Msg.Content, &c); err != nil {
			return err
		}
		return n.CurChain.ValidateGroupSourcePreparation(c)
	case groupInstallRequest:
		var p core.GroupPreparation
		if err := json.Unmarshal(r.Msg.Content, &p); err != nil {
			return err
		}
		return n.CurChain.ValidateGroupTargetInstallation(p, seq, digest)
	case groupActivateRequest:
		var c chain.GroupTargetActivation
		if err := json.Unmarshal(r.Msg.Content, &c); err != nil {
			return err
		}
		return n.CurChain.ValidateGroupTargetActivation(c)
	}
	return fmt.Errorf("unsupported group request")
}

func (n *PbftConsensusNode) groupProposal() (*message.Request, error) {
	for _, k := range []message.RequestType{groupPrepareRequest, groupInstallRequest, groupActivateRequest} {
		for _, path := range core.GroupTransportPaths(groupInbox(k)) {
			if path == "" {
				continue
			}
			raw, err := os.ReadFile(path)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if !n.groupForShard(k, raw) {
				continue
			}
			id := groupRequestID(k, raw)
			if core.SerialGroupEnabled() && path != core.GroupTransportPath(groupInbox(k), id) {
				return nil, fmt.Errorf("group inbox identity/path mismatch")
			}
			key := sha256.Sum256(raw)
			if v, seen := n.sourcePrepareSeen.Load(key); seen {
				if v == true {
					continue
				}
				if r, ok := v.(*message.Request); ok {
					return r, nil
				}
			}
			content := raw
			if k == groupPrepareRequest {
				var c core.GroupPrepareCommand
				if err = json.Unmarshal(raw, &c); err != nil {
					return nil, err
				}
				old := n.CurChain.GroupSourcePreparation(id)
				if old != nil {
					if len(old.Command.Members) != len(c.Members) {
						return nil, fmt.Errorf("group intent conflict")
					}
					for i, m := range c.Members {
						previous := old.Command.Members[i]
						if m.BindAtProposal {
							previous.ExpectedBlockHash = ""
							previous.ExpectedStateRoot = ""
							previous.ExpectedStateHash = ""
						}
						if m != previous {
							return nil, fmt.Errorf("group intent conflict")
						}
					}
					if err = n.publishGroupEvidence("SAA_GROUP_SOURCE_PREPARED_PATH", old, old.PrepareBlock); err != nil {
						return nil, err
					}
					n.sourcePrepareSeen.Store(key, true)
					continue
				}
				for i, m := range c.Members {
					if !m.BindAtProposal {
						continue
					}
					if m.ExpectedBlockHash != "" || m.ExpectedStateRoot != "" || m.ExpectedStateHash != "" {
						return nil, fmt.Errorf("group intent contains snapshot")
					}
					state, err := n.CurChain.ExportAccountState(m.Account)
					if err != nil {
						return nil, err
					}
					c.Members[i].ExpectedBlockHash = hex.EncodeToString(n.CurChain.CurrentBlock.Hash)
					c.Members[i].ExpectedStateRoot = hex.EncodeToString(state.SourceRoot)
					c.Members[i].ExpectedStateHash = hex.EncodeToString(state.StateHash)
				}
				content, err = json.Marshal(c)
				if err != nil {
					return nil, err
				}
			}
			r := &message.Request{RequestType: k, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: content}}
			validationSeq := n.sequenceID
			if k == groupInstallRequest {
				if old, e := n.CurChain.GroupTargetInstallation(id); e != nil {
					return nil, e
				} else if old != nil {
					validationSeq = old.InstallSequence
				}
			}
			if err = n.validateGroupRequest(r, validationSeq, getDigest(r)); err != nil {
				return nil, err
			}
			// Recovered target records repair publication without another PBFT commit.
			if k == groupInstallRequest {
				if p, e := n.CurChain.GroupTargetInstallation(id); e != nil {
					return nil, e
				} else if p != nil {
					if e = n.publishGroupEvidence("SAA_GROUP_TARGET_INSTALLED_PATH", p, p.InstallBlock); e != nil {
						return nil, e
					}
					n.sourcePrepareSeen.Store(key, true)
					continue
				}
			}
			if k == groupActivateRequest {
				if block, e := n.CurChain.GroupTargetActivationBlock(id); e != nil {
					return nil, e
				} else if block > 0 {
					var c chain.GroupTargetActivation
					_ = json.Unmarshal(content, &c)
					if e = n.publishGroupEvidence("SAA_GROUP_TARGET_ACTIVATED_PATH", c, block); e != nil {
						return nil, e
					}
					n.sourcePrepareSeen.Store(key, true)
					continue
				}
			}
			n.sourcePrepareSeen.Store(key, r)
			return r, nil
		}
	}
	return nil, nil
}

func (n *PbftConsensusNode) publishGroupEvidence(env string, record interface{}, height uint64) error {
	id, identityError := groupEvidenceID(record)
	if identityError != nil {
		return identityError
	}
	path := core.GroupTransportPath(os.Getenv(env), id)
	view := uint64(n.view.Load())
	if path == "" || n.NodeID != view {
		return nil
	}
	if env == "SAA_GROUP_SOURCE_RETIRED_PATH" {
		recordRaw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		binding := retirementEvidenceBinding{path: path, record: sha256.Sum256(recordRaw), height: height, shard: n.ShardID, node: n.NodeID, view: view}
		value, _ := n.sourcePrepareSeen.LoadOrStore("retirement-evidence-publication-v1", &retirementEvidencePublication{})
		return value.(*retirementEvidencePublication).publish(binding, func() ([]byte, error) {
			return n.groupEvidenceBytes(record, height)
		}, core.PublishSourcePrepareFile)
	}
	raw, err := n.groupEvidenceBytes(record, height)
	if err != nil {
		return err
	}
	return core.PublishSourcePrepareFile(path, raw)
}

func (n *PbftConsensusNode) groupEvidenceBytes(record interface{}, height uint64) ([]byte, error) {
	block := n.CurChain.CurrentBlock
	for block.Header.Number > height {
		var err error
		block, err = n.CurChain.Storage.GetBlock(block.Header.ParentBlockHash)
		if err != nil {
			return nil, err
		}
	}
	if block.Header.Number != height {
		return nil, fmt.Errorf("group commit block unavailable")
	}
	raw, err := json.Marshal(map[string]interface{}{"record": record, "block": height, "block_hash": hex.EncodeToString(block.Hash), "state_root": hex.EncodeToString(block.Header.StateRoot), "shard": n.ShardID, "committed": true})
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func (n *PbftConsensusNode) commitGroupRequest(r *message.Request, seq uint64, digest []byte) error {
	if err := n.validateGroupRequest(r, seq, digest); err != nil {
		return err
	}
	switch r.RequestType {
	case groupRetireRequest:
		var c chain.GroupRetirementCommand
		_ = json.Unmarshal(r.Msg.Content, &c)
		retired, e := n.CurChain.CommitGroupRetirement(c, seq, digest)
		if e != nil {
			return e
		}
		return n.publishGroupEvidence("SAA_GROUP_SOURCE_RETIRED_PATH", retired, retired.Block)
	case groupPrepareRequest:
		var c core.GroupPrepareCommand
		_ = json.Unmarshal(r.Msg.Content, &c)
		if err := n.commitGroupSourcePrepareTimed(c, r, seq, digest); err != nil {
			return err
		}
	case groupInstallRequest:
		var p core.GroupPreparation
		_ = json.Unmarshal(r.Msg.Content, &p)
		installed, err := n.CurChain.CommitGroupTargetInstallation(p, seq, digest)
		if err != nil {
			return err
		}
		if err = n.publishGroupEvidence("SAA_GROUP_TARGET_INSTALLED_PATH", installed, installed.InstallBlock); err != nil {
			return err
		}
	case groupActivateRequest:
		var c chain.GroupTargetActivation
		_ = json.Unmarshal(r.Msg.Content, &c)
		if err := n.CurChain.CommitGroupTargetActivation(c); err != nil {
			return err
		}
		block, err := n.CurChain.GroupTargetActivationBlock(c.Members[0].ProposalID)
		if err != nil {
			return err
		}
		if err = n.publishGroupEvidence("SAA_GROUP_TARGET_ACTIVATED_PATH", c, block); err != nil {
			return err
		}
	}
	// Match the exact original immutable inbox, including bound-source intents.
	if raw, err := os.ReadFile(core.GroupTransportPath(groupInbox(r.RequestType), groupRequestID(r.RequestType, r.Msg.Content))); err == nil {
		matches := bytes.Equal(bytes.TrimSpace(raw), bytes.TrimSpace(r.Msg.Content))
		if r.RequestType == groupPrepareRequest {
			var intent, bound core.GroupPrepareCommand
			if json.Unmarshal(raw, &intent) == nil && json.Unmarshal(r.Msg.Content, &bound) == nil && len(intent.Members) == len(bound.Members) {
				matches = true
				for i, m := range bound.Members {
					if intent.Members[i].BindAtProposal {
						m.ExpectedBlockHash = ""
						m.ExpectedStateRoot = ""
						m.ExpectedStateHash = ""
					}
					matches = matches && m == intent.Members[i]
				}
			}
		}
		if matches {
			n.sourcePrepareSeen.Store(sha256.Sum256(raw), true)
		}
	}
	return nil
}

// Measures this commit handler's local work, not PBFT voting or total account
// unavailability. Diagnostics do not alter queue, commit or evidence ordering.
func (n *PbftConsensusNode) commitGroupSourcePrepareTimed(c core.GroupPrepareCommand, r *message.Request, seq uint64, digest []byte) (err error) {
	started := time.Now()
	var prepareQueue, marshal, chainCommit, evidencePublish time.Duration
	stage := "prepareQueue"
	defer func() {
		if n.pl == nil || n.pl.Plog == nil {
			return
		}
		status := "committed"
		if err != nil {
			status = "error"
		}
		n.pl.Plog.Printf("group_source_prepare_timing proposal=%s seq=%d status=%s stage=%s prepareQueueUs=%d marshalUs=%d chainCommitUs=%d evidencePublishUs=%d totalUs=%d", c.Members[0].ProposalID, seq, status, stage, prepareQueue.Microseconds(), marshal.Microseconds(), chainCommit.Microseconds(), evidencePublish.Microseconds(), time.Since(started).Microseconds())
	}()
	stageStart := time.Now()
	err = n.prepareGroupQueue(c)
	prepareQueue = time.Since(stageStart)
	if err != nil {
		return err
	}
	stage, stageStart = "marshal", time.Now()
	raw, err := json.Marshal(r)
	marshal = time.Since(stageStart)
	if err != nil {
		return err
	}
	stage, stageStart = "chainCommit", time.Now()
	p, err := n.CurChain.CommitGroupSourcePreparationObserved(c, seq, digest, raw, func(phase string) {
		if n.pl != nil && n.pl.Plog != nil {
			n.pl.Plog.Printf("group_source_prepare_stage proposal=%s phase=%s elapsedMs=%d", c.Members[0].ProposalID, phase, time.Since(stageStart).Milliseconds())
		}
	})
	chainCommit = time.Since(stageStart)
	if err != nil {
		return err
	}
	stage, stageStart = "evidencePublish", time.Now()
	err = n.publishGroupEvidence("SAA_GROUP_SOURCE_PREPARED_PATH", p, p.PrepareBlock)
	evidencePublish = time.Since(stageStart)
	return err
}

func (n *PbftConsensusNode) prepareGroupQueue(c core.GroupPrepareCommand) error {
	dir := n.sourceQueueDirectory()
	if dir == "" {
		return fmt.Errorf("group source requires durable queue directory")
	}
	if n.CurChain.GroupSourcePreparation(c.Members[0].ProposalID) != nil && n.sourceQueueReady {
		return nil
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err = core.PublishSourcePrepareFile(core.GroupTransportPath(filepath.Join(dir, "group-binding.json"), c.Members[0].ProposalID), raw); err != nil {
		return err
	}
	accounts := []string{}
	for _, m := range c.Members {
		accounts = append(accounts, m.Account)
	}
	if n.sourceQueueReady {
		err = n.CurChain.Txpool.ExtendGroupMigrationQueueJournal(accounts)
	} else {
		err = n.CurChain.Txpool.EnableGroupMigrationQueueJournal(dir, accounts)
	}
	if err != nil {
		return err
	}
	if err = n.CurChain.Txpool.ValidateIndexedMigrationAdmission(); err != nil {
		return err
	}
	n.sourceQueueReady = true
	return nil
}
