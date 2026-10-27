package pbft_all

import (
	"blockEmulator/message"
	"encoding/json"
	"os"
	"sync"
)

var nativeReceiptWriteMu sync.Mutex

type nativeInstallationReceiptEvent struct {
	ProposalID     string `json:"proposal_id"`
	Account        string `json:"account"`
	StateHash      string `json:"state_hash"`
	Committed      bool   `json:"committed"`
	SourceShard    uint64 `json:"source_shard"`
	TargetShard    uint64 `json:"target_shard"`
	MappingVersion uint64 `json:"mapping_version"`
	AnchorVersion  uint64 `json:"anchor_version"`
}

func emitNativeInstallationReceipt(proposalID, account, stateHash string) {
	path := os.Getenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH")
	if path == "" || proposalID == "" {
		return
	}
	b, err := json.Marshal(nativeInstallationReceiptEvent{ProposalID: proposalID, Account: account, StateHash: stateHash, Committed: true})
	if err != nil {
		return
	}
	nativeReceiptWriteMu.Lock()
	defer nativeReceiptWriteMu.Unlock()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(b, '\n'))
}

// EmitNativeInstallationReceipt is the cross-package ingress used by
// integration wiring; production CLPA paths use the internal helper above.
func EmitNativeInstallationReceipt(proposalID, account, stateHash string) {
	emitNativeInstallationReceipt(proposalID, account, stateHash)
}

func emitNativeInstallationReceiptWithEnvelope(e message.StateTransferEnvelope) {
	path := os.Getenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH")
	if path == "" || e.ProposalID == "" {
		return
	}
	b, err := json.Marshal(nativeInstallationReceiptEvent{ProposalID: e.ProposalID, Account: e.Account, StateHash: e.StateHash, Committed: true, SourceShard: e.SourceShard, TargetShard: e.TargetShard, MappingVersion: e.MappingVersion, AnchorVersion: e.AnchorVersion})
	if err != nil {
		return
	}
	nativeReceiptWriteMu.Lock()
	defer nativeReceiptWriteMu.Unlock()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(b, '\n'))
}
