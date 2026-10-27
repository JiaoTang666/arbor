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

func (n *PbftConsensusNode) sourceRetirementProposal() (*message.Request, error) {
	if os.Getenv("SAA_NATIVE_SOURCE_RETIRED_PATH") == "" {
		return nil, nil
	}
	p := n.CurChain.SourcePreparation()
	if p == nil {
		return nil, nil
	}
	old, err := n.CurChain.SourceRetirement()
	if err != nil {
		return nil, err
	}
	if old != nil {
		return nil, n.publishSourceRetirement(old)
	}
	// Empty memory alone is never a drain certificate.
	if len(n.CurChain.Txpool.SnapshotMigrationTransactions(p.Command.Account)) > 0 {
		return nil, nil
	}
	boundary, err := n.CurChain.Txpool.MigrationDrainBoundary(p.Command.Account)
	if err != nil {
		return nil, err
	}
	dir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR")
	if dir == "" || os.Getenv("SAA_NATIVE_TARGET_ACTIVATED_PATH") == "" || n.sourceQueueDirectory() == "" {
		return nil, fmt.Errorf("retirement requires durable queue, outbox and activation confirmation")
	}
	audit, err := core.AuditMigrationHandoffs(dir, p.Command.ProposalID, p.Command.Account, n.sourceQueueDirectory())
	if err != nil {
		return nil, err
	}
	if len(audit.Pending) > 0 {
		return nil, nil
	}
	raw, err := os.ReadFile(os.Getenv("SAA_NATIVE_TARGET_ACTIVATED_PATH"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var receipt struct {
		Activation chain.TargetActivation `json:"activation"`
	}
	if err = json.Unmarshal(raw, &receipt); err != nil {
		return nil, err
	}
	if ok, err := targetActivationConfirmed(receipt.Activation); err != nil || !ok {
		return nil, err
	}
	snapshot, err := os.ReadFile(filepath.Join(n.sourceQueueDirectory(), fmt.Sprintf("%020d.queue.json", boundary.Generation)))
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(raw)
	e := sourceRetirementEnvelope{Command: chain.SourceRetirementCommand{Activation: receipt.Activation, ActivationReceiptHash: hex.EncodeToString(h[:]), Drain: boundary}, ActivationReceipt: raw, DrainSnapshot: snapshot}
	data, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	r := &message.Request{RequestType: sourceRetirementRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: data}}
	if _, err = n.validateSourceRetirementRequest(r); err != nil {
		return nil, err
	}
	return r, nil
}
