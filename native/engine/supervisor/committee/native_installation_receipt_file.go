package committee

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type nativeInstallationReceiptEvent struct {
	ProposalID      string `json:"proposal_id"`
	Account         string `json:"account"`
	StateHash       string `json:"state_hash"`
	SourceShard     uint64 `json:"source_shard"`
	TargetShard     uint64 `json:"target_shard"`
	MappingVersion  uint64 `json:"mapping_version"`
	AnchorVersion   uint64 `json:"anchor_version"`
	Committed       bool   `json:"committed"`
	Installed       bool   `json:"installed"`
	SourceStateRoot string `json:"source_state_root"`
	TargetStateRoot string `json:"target_state_root"`
	InstallBlock    uint64 `json:"install_block"`
	InstallRound    uint64 `json:"install_round"`
	CommitDigest    string `json:"commit_digest"`
}

// ConsumeNativeInstallationReceipts consumes newline-terminated PBFT receipt
// events from a shared JSONL file. The caller should invoke it from its normal
// supervisor polling loop; offsets advance only for valid events.
func (m *RuntimeUnknownAdmissionManager) ConsumeNativeInstallationReceipts(path string, offset *int64) (int, error) {
	if m == nil {
		return 0, fmt.Errorf("manager required")
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.consumeNativeInstallationReceiptsLocked(path, offset)
}

// The runtime polling loop already holds m.lock; never call the public wrapper
// from that loop, or acquire it again through RecordNativeInstallationReceipt.
func (m *RuntimeUnknownAdmissionManager) consumeNativeInstallationReceiptsLocked(path string, offset *int64) (int, error) {
	if m == nil || path == "" || offset == nil {
		return 0, fmt.Errorf("receipt path and offset are required")
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if *offset < 0 || *offset > info.Size() {
		return 0, fmt.Errorf("receipt stream truncated or invalid offset")
	}
	if _, err = f.Seek(*offset, io.SeekStart); err != nil {
		return 0, err
	}
	count := 0
	r := bufio.NewReader(f)
	for {
		line, e := r.ReadBytes('\n')
		if e == io.EOF {
			break
		}
		if e != nil {
			return count, e
		}
		var event nativeInstallationReceiptEvent
		if json.Unmarshal(line, &event) != nil || event.ProposalID == "" {
			return count, fmt.Errorf("invalid receipt record at offset %d", *offset)
		}
		// Advance for every complete, parsed record, including rejected evidence.
		// Otherwise skipping a record and accepting the next one corrupts offsets.
		*offset += int64(len(line))
		state := m.integratedStateLocked()
		if state != nil && state.authority != nil && state.authority.requireInstallationReceipt && !event.Committed {
			continue
		}
		state = m.integratedStateLocked()
		if state == nil || state.authority == nil {
			return count, fmt.Errorf("integrated authority is unavailable")
		}
		if event.Account != "" || event.StateHash != "" {
			meta := nativeInstallationReceiptMeta{ProposalID: event.ProposalID, Account: event.Account, StateHash: event.StateHash, SourceShard: event.SourceShard, TargetShard: event.TargetShard, MappingVersion: event.MappingVersion, AnchorVersion: event.AnchorVersion, Committed: event.Committed, Installed: event.Installed, SourceStateRoot: event.SourceStateRoot, TargetStateRoot: event.TargetStateRoot, InstallBlock: event.InstallBlock, InstallRound: event.InstallRound, CommitDigest: event.CommitDigest}
			if state.authority.requireInstallationReceipt {
				// Installation can arrive before the source-export adapter registers
				// preparation. Leave this complete record unread, rather than losing
				// a valid receipt through an asynchronous ordering race.
				p, pending := state.authority.pending[meta.ProposalID]
				member := false
				for _, account := range p.Addresses {
					member = member || account == meta.Account
				}
				_, prepared := state.authority.preparedTransfers[meta.ProposalID][meta.Account]
				if pending && member && !prepared && meta.Installed && meta.Committed && meta.InstallBlock > 0 && validTransferDigest(meta.StateHash) && validTransferDigest(meta.SourceStateRoot) && validTransferDigest(meta.TargetStateRoot) && validTransferDigest(meta.CommitDigest) {
					*offset -= int64(len(line))
					return count, nil
				}
				if !state.authority.matchesPreparedReceipt(meta) {
					continue
				}
			}
			state.authority.recordInstallationReceiptMeta(meta)
		} else {
			if state.authority.requireInstallationReceipt {
				continue
			}
			state.authority.recordInstallationReceipt(event.ProposalID)
		}
		count++
	}
	return count, nil
}
