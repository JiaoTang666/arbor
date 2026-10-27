package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/message"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"syscall"
)

// This builder reads installed trie state; it does not trust AccountTransferMsg
// payload equality as evidence of installation. The caller must be the PBFT
// commit handler, after the request has passed the existing commit threshold.
func committedInstallationRecord(bc *chain.BlockChain, e message.StateTransferEnvelope, shard, round uint64, digest []byte) (map[string]interface{}, error) {
	root, err := hex.DecodeString(e.SourceStateRoot)
	if err != nil || len(root) != 32 || len(digest) != 32 || e.ProposalID == "" || e.TargetShard != shard || e.SourceShard == shard {
		return nil, fmt.Errorf("invalid committed transfer identity/root/digest")
	}
	if bc == nil || bc.CurrentBlock == nil || bc.CurrentBlock.Header.Number == 0 {
		return nil, fmt.Errorf("target install block missing")
	}
	installed, err := bc.ExportAccountState(e.Account)
	if err != nil {
		return nil, err
	}
	if hex.EncodeToString(installed.StateHash) != e.StateHash {
		return nil, fmt.Errorf("installed state differs from prepared transfer")
	}
	if len(installed.SourceRoot) != 32 {
		return nil, fmt.Errorf("target root missing")
	}
	account := "0x" + strings.TrimPrefix(strings.ToLower(e.Account), "0x")
	decoded, err := hex.DecodeString(strings.TrimPrefix(account, "0x"))
	if err != nil || len(decoded) != 20 {
		return nil, fmt.Errorf("invalid account")
	}
	return map[string]interface{}{
		"proposal_id": e.ProposalID, "account": account, "source_shard": e.SourceShard, "target_shard": e.TargetShard,
		"mapping_version": e.MappingVersion, "anchor_version": e.AnchorVersion, "state_hash": e.StateHash,
		"source_state_root": e.SourceStateRoot, "target_state_root": hex.EncodeToString(installed.SourceRoot),
		"install_block": bc.CurrentBlock.Header.Number, "install_round": round, "commit_digest": hex.EncodeToString(digest),
		"installed": true, "committed": true,
	}, nil
}

func (n *PbftConsensusNode) emitCommittedMigrationReceipts(atm *message.AccountTransferMsg, commit *message.Commit) error {
	path := os.Getenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH")
	if path == "" || n.NodeID != uint64(n.view.Load()) {
		return nil
	}
	for _, e := range atm.MigrationEnvelopes {
		// Old Broker-generated IDs lack source preparation evidence. Preserve
		// their compatibility path, but never upgrade them to strong receipts.
		if e.SourceStateRoot == "" {
			continue
		}
		record, err := committedInstallationRecord(n.CurChain, e, n.ShardID, commit.SeqID, commit.Digest)
		if err != nil {
			return err
		}
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err = appendCommittedReceipt(path, append(data, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func appendCommittedReceipt(path string, data []byte) error {
	nativeReceiptWriteMu.Lock()
	defer nativeReceiptWriteMu.Unlock()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("short receipt write")
	}
	return f.Sync()
}
