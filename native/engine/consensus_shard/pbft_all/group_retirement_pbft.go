package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const groupRetireRequest message.RequestType = "SEFGroupRetireV1"

func (n *PbftConsensusNode) groupRetirementProposal() (*message.Request, error) {
	if n.stopSignal.Load() || os.Getenv("SAA_GROUP_SOURCE_RETIRED_PATH") == "" {
		return nil, nil
	}
	t := newRetirementTrace()
	defer t.log(n)
	if n.sourceConfirmationLane().active() {
		t.status = "confirmation_running"
		return nil, nil
	}
	p := n.CurChain.GroupSourcePreparation()
	t.mark(&t.lookup)
	if p == nil {
		t.status = "no_preparation"
		return nil, nil
	}
	old, err := n.CurChain.GroupSourceRetirement()
	t.mark(&t.lookup)
	if err != nil {
		return nil, err
	}
	if old != nil {
		err = n.publishGroupEvidence("SAA_GROUP_SOURCE_RETIRED_PATH", old, old.Block)
		t.mark(&t.publish)
		if err == nil {
			t.status = "already_retired"
		}
		return nil, err
	}

	activation, receipt, err := n.readConfirmedGroupActivation()
	t.mark(&t.activation)
	if err != nil {
		return nil, err
	}
	if activation == nil {
		t.status = "await_activation"
		return nil, nil
	}
	accounts := make([]string, 0, len(p.Members))
	members := make(map[string]string, len(p.Members))
	for _, m := range p.Members {
		accounts = append(accounts, m.Command.Account)
		members[m.Command.Account] = m.Command.ProposalID
	}
	dir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR")
	if dir == "" {
		return nil, fmt.Errorf("group retirement needs durable outbox")
	}
	journalDir := n.sourceQueueDirectory()
	binding, err := retirementAuditBinding(p, receipt, dir, journalDir)
	if err != nil {
		return nil, err
	}
	busy, result := n.retirementAudit.poll()
	t.mark(&t.audit)
	if busy {
		t.status = "audit_running"
		return nil, nil // Ordinary block proposals continue while file audit runs.
	}
	if result != nil {
		t.backgroundAudit = result.elapsed
		if result.binding != binding || result.filesChanged {
			t.status = "audit_stale"
			return nil, nil
		}
		if result.err != nil {
			return nil, result.err
		}
		for account := range members {
			audit, exists := result.audits[account]
			if !exists {
				return nil, fmt.Errorf("group audit missing member %s", account)
			}
			if len(audit.Pending) > 0 {
				t.status = "pending_handoff"
				return nil, nil
			}
		}
	}
	snapshot, detail, err := n.CurChain.Txpool.TryGroupDrainSnapshot(accounts)
	t.detail = detail
	t.mark(&t.drain)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		t.status = "deferred_" + detail.Status
		return nil, nil
	}

	if result == nil {
		index := n.retirementAudit.indexFor(dir)
		// members is owned by this proposal. The worker captures only immutable
		// input and its dedicated index, never n, CurChain or TxPool.
		if !n.stopSignal.Load() && n.retirementAudit.start(binding, func() retirementAuditResult {
			return readRetirementAudit(index, dir, members, journalDir)
		}) {
			t.status = "audit_started"
		} else {
			t.status = "audit_unavailable"
		}
		t.mark(&t.audit)
		return nil, nil
	}
	// This is the CURRENT empty-group proof. Do not compare its global sequence
	// to the launch snapshot: obtaining a union projection itself advances it.
	// File-set equality independently binds the returned audit to the current
	// final outbox/proof set. A new publication forces a fresh audit.
	files, err := readRetirementAuditFiles(dir, journalDir)
	t.mark(&t.audit)
	if err != nil {
		return nil, err
	}
	if !result.files.same(files) {
		t.status = "audit_stale_files"
		return nil, nil
	}
	c := chain.GroupRetirementCommand{ActivationReceipt: receipt, DrainSnapshot: snapshot}
	err = n.CurChain.ValidateGroupRetirement(c)
	t.mark(&t.validate)
	if err != nil {
		return nil, err
	}
	content, err := json.Marshal(c)
	t.mark(&t.marshal)
	if err != nil {
		return nil, err
	}
	t.status = "proposed"
	return &message.Request{RequestType: groupRetireRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: content}}, nil
}

// A local timing record, not a second protocol state machine. Stage intervals
// include the small bookkeeping between calls; core drain timings split its
// actual lock/scan/I/O work. Diagnostics never decide whether to retire.
type retirementTrace struct {
	started, last                                                time.Time
	status                                                       string
	detail                                                       core.GroupDrainTiming
	lookup, activation, drain, audit, validate, marshal, publish time.Duration
	backgroundAudit                                              time.Duration
}

func newRetirementTrace() *retirementTrace {
	now := time.Now()
	return &retirementTrace{started: now, last: now, status: "error", detail: core.GroupDrainTiming{Status: "not_observed", ContactPending: -1, RecoveryDue: -1}}
}

func (t *retirementTrace) mark(dst *time.Duration) {
	now := time.Now()
	*dst += now.Sub(t.last)
	t.last = now
}

func (t *retirementTrace) log(n *PbftConsensusNode) {
	if n.pl == nil || n.pl.Plog == nil {
		return
	}
	if t.status == "no_preparation" && time.Since(t.started) < 100*time.Millisecond {
		return
	}
	d := t.detail
	n.pl.Plog.Printf("group_retirement_timing height=%d status=%s contact_pending=%d recovery_due=%d lookupUs=%d activationUs=%d drainUs=%d gateUs=%d poolUs=%d scanUs=%d readSnapshotUs=%d publishSnapshotUs=%d drainStatus=%s auditUs=%d backgroundAuditUs=%d receiptReadUs=0 validateUs=%d marshalUs=%d evidencePublishUs=%d totalUs=%d", n.CurChain.CurrentBlock.Header.Number, t.status, d.ContactPending, d.RecoveryDue, t.lookup.Microseconds(), t.activation.Microseconds(), t.drain.Microseconds(), d.Gate.Microseconds(), d.Pool.Microseconds(), d.Scan.Microseconds(), d.Read.Microseconds(), d.Publish.Microseconds(), d.Status, t.audit.Microseconds(), t.backgroundAudit.Microseconds(), t.validate.Microseconds(), t.marshal.Microseconds(), t.publish.Microseconds(), time.Since(t.started).Microseconds())
}
