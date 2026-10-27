package committee

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

type mappingProposalStatus string

const (
	mappingAccepted mappingProposalStatus = "accepted"
	mappingRejected mappingProposalStatus = "rejected"
	mappingApplied  mappingProposalStatus = "applied"
	mappingNoop     mappingProposalStatus = "noop"
)

type mappingAuthorityProposal struct {
	ProposalID       string   `json:"proposal_id"`
	Source           string   `json:"source"`
	Addresses        []string `json:"addresses"`
	SourceShard      uint64   `json:"source_shard"`
	TargetShard      uint64   `json:"target_shard"`
	BaseRouteVersion uint64   `json:"base_route_version"`
	EffectiveEpoch   int      `json:"effective_epoch"`
}

type mappingAuthorityDecision struct {
	SubmittedEpoch             int                   `json:"submitted_epoch"`
	AppliedEpoch               *int                  `json:"applied_epoch,omitempty"`
	RouteActivationDelayEpochs *int                  `json:"route_activation_delay_epochs,omitempty"`
	ProposalID                 string                `json:"proposal_id"`
	Status                     mappingProposalStatus `json:"status"`
	Reason                     string                `json:"reason"`
	AppliedAccounts            int                   `json:"applied_accounts"`
	BaseRouteVersion           uint64                `json:"base_route_version"`
	NewRouteVersion            uint64                `json:"new_route_version"`
	EffectiveEpoch             int                   `json:"effective_epoch"`
}

// mappingAuthority is the only owner of an active route map in learned-SAA
// mode. Policies submit proposals; they never mutate the active map directly.
// Account versions allow one non-conflicting batch to share a global base
// version while still rejecting stale writes to an account changed since then.
type mappingAuthority struct {
	shardNum                    uint64
	active                      map[string]uint64
	accountVersion              map[string]uint64
	migratedAccounts            map[string]bool
	pending                     map[string]mappingAuthorityProposal
	decisionByID                map[string]mappingAuthorityDecision
	routeVersion                uint64
	installationReceipts        map[string]bool
	installationReceiptMeta     map[string]map[string]nativeInstallationReceiptMeta
	preparedTransfers           map[string]map[string]NativeStateTransferEnvelope
	installationReceiptAccounts map[string]map[string]bool
	requireInstallationReceipt  bool
}

type nativeInstallationReceiptMeta struct {
	ProposalID      string
	Account         string
	SourceShard     uint64
	TargetShard     uint64
	MappingVersion  uint64
	AnchorVersion   uint64
	StateHash       string
	Committed       bool
	Installed       bool
	SourceStateRoot string
	TargetStateRoot string
	InstallBlock    uint64
	InstallRound    uint64
	CommitDigest    string
}

func newMappingAuthority(shardNum uint64, initial map[string]uint64) *mappingAuthority {
	a := &mappingAuthority{
		shardNum:                    shardNum,
		active:                      make(map[string]uint64, len(initial)),
		accountVersion:              make(map[string]uint64, len(initial)),
		migratedAccounts:            make(map[string]bool),
		pending:                     make(map[string]mappingAuthorityProposal),
		decisionByID:                make(map[string]mappingAuthorityDecision),
		installationReceipts:        make(map[string]bool),
		installationReceiptMeta:     make(map[string]map[string]nativeInstallationReceiptMeta),
		preparedTransfers:           make(map[string]map[string]NativeStateTransferEnvelope),
		installationReceiptAccounts: make(map[string]map[string]bool),
	}
	for raw, shardID := range initial {
		account := normalizeMappingAuthorityAccount(raw)
		if account != "" && shardID < shardNum {
			a.active[account] = shardID
		}
	}
	return a
}

// recordInstallationReceipt gates route activation on consensus-committed
// destination installation. The receipt is keyed by proposal ID and is
// intentionally idempotent.
func (a *mappingAuthority) recordInstallationReceipt(proposalID string) {
	if a != nil && strings.TrimSpace(proposalID) != "" {
		a.installationReceipts[proposalID] = true
	}
}

func (a *mappingAuthority) recordInstallationReceiptMeta(r nativeInstallationReceiptMeta) {
	if a == nil || strings.TrimSpace(r.ProposalID) == "" || !r.Committed {
		return
	}
	if a.requireInstallationReceipt && !a.matchesPreparedReceipt(r) {
		return
	}
	a.installationReceipts[r.ProposalID] = true
	if a.installationReceiptMeta[r.ProposalID] == nil {
		a.installationReceiptMeta[r.ProposalID] = make(map[string]nativeInstallationReceiptMeta)
	}
	// Replays are idempotent; conflicting evidence must not overwrite a receipt.
	if previous, exists := a.installationReceiptMeta[r.ProposalID][r.Account]; exists && previous != r {
		return
	}
	a.installationReceiptMeta[r.ProposalID][r.Account] = r
	if r.Account != "" {
		if a.installationReceiptAccounts[r.ProposalID] == nil {
			a.installationReceiptAccounts[r.ProposalID] = make(map[string]bool)
		}
		a.installationReceiptAccounts[r.ProposalID][r.Account] = true
	}
}

// canFinalizeMigration is the source-side guard: finalization is allowed only
// after a committed target receipt for the exact proposal/account/hash.
func (a *mappingAuthority) canFinalizeMigration(proposalID, account, stateHash string) bool {
	if a == nil || proposalID == "" || account == "" || stateHash == "" {
		return false
	}
	r, ok := a.installationReceiptMeta[proposalID][account]
	if !ok || !r.Committed || r.StateHash != stateHash {
		return false
	}
	if a.requireInstallationReceipt {
		return a.decisionByID[proposalID].Status == mappingApplied && a.matchesPreparedReceipt(r)
	}
	return true
}
func (a *mappingAuthority) enableInstallationReceiptGate() {
	if a != nil {
		a.requireInstallationReceipt = true
	}
}

func normalizeMappingAuthorityAccount(raw string) string {
	account := strings.ToLower(strings.TrimSpace(raw))
	if len(account) != 42 || !strings.HasPrefix(account, "0x") {
		return ""
	}
	if _, err := hex.DecodeString(account[2:]); err != nil {
		return ""
	}
	return account
}

func (a *mappingAuthority) resolve(raw string) (uint64, bool) {
	if a == nil {
		return 0, false
	}
	shardID, ok := a.active[normalizeMappingAuthorityAccount(raw)]
	return shardID, ok
}

func (a *mappingAuthority) submit(proposal mappingAuthorityProposal, currentEpoch int) mappingAuthorityDecision {
	if a == nil {
		return mappingAuthorityDecision{ProposalID: proposal.ProposalID, Status: mappingRejected, Reason: "authority_missing"}
	}
	if previous, ok := a.decisionByID[proposal.ProposalID]; ok {
		return previous
	}
	decision := mappingAuthorityDecision{
		SubmittedEpoch: currentEpoch,
		ProposalID:     proposal.ProposalID, BaseRouteVersion: proposal.BaseRouteVersion,
		NewRouteVersion: a.routeVersion, EffectiveEpoch: proposal.EffectiveEpoch,
	}
	reject := func(reason string) mappingAuthorityDecision {
		decision.Status = mappingRejected
		decision.Reason = reason
		a.decisionByID[proposal.ProposalID] = decision
		return decision
	}
	if strings.TrimSpace(proposal.ProposalID) == "" {
		return reject("proposal_id_missing")
	}
	if proposal.TargetShard >= a.shardNum {
		return reject("target_shard_out_of_range")
	}
	if proposal.EffectiveEpoch <= currentEpoch {
		return reject("effective_epoch_not_future")
	}
	if proposal.BaseRouteVersion > a.routeVersion {
		return reject("base_route_version_from_future")
	}
	seen := make(map[string]bool)
	normalized := make([]string, 0, len(proposal.Addresses))
	for _, raw := range proposal.Addresses {
		account := normalizeMappingAuthorityAccount(raw)
		if account == "" {
			return reject("invalid_account")
		}
		if seen[account] {
			continue
		}
		seen[account] = true
		if a.accountVersion[account] > proposal.BaseRouteVersion {
			return reject("stale_account_version")
		}
		for _, pending := range a.pending {
			if pending.ProposalID == proposal.ProposalID {
				continue
			}
			for _, pendingAccount := range pending.Addresses {
				if pendingAccount == account {
					return reject("account_has_pending_proposal")
				}
			}
		}
		normalized = append(normalized, account)
	}
	if len(normalized) == 0 {
		return reject("addresses_empty")
	}
	proposal.Addresses = normalized
	a.pending[proposal.ProposalID] = proposal
	decision.Status = mappingAccepted
	decision.Reason = "queued_for_effective_epoch"
	a.decisionByID[proposal.ProposalID] = decision
	return decision
}

func (a *mappingAuthority) applyDue(epoch int) []mappingAuthorityDecision {
	if a == nil {
		return nil
	}
	due := make([]mappingAuthorityProposal, 0)
	for _, proposal := range a.pending {
		if proposal.EffectiveEpoch <= epoch {
			due = append(due, proposal)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].EffectiveEpoch != due[j].EffectiveEpoch {
			return due[i].EffectiveEpoch < due[j].EffectiveEpoch
		}
		return due[i].ProposalID < due[j].ProposalID
	})
	decisions := make([]mappingAuthorityDecision, 0, len(due))
	for _, proposal := range due {
		decision := a.decisionByID[proposal.ProposalID]
		if a.requireInstallationReceipt && proposal.SourceShard != proposal.TargetShard && !a.installationReceipts[proposal.ProposalID] {
			decision.Status = mappingAccepted
			decision.Reason = "state_installation_receipt_missing"
			decisions = append(decisions, decision)
			continue
		}
		if a.requireInstallationReceipt && proposal.SourceShard != proposal.TargetShard {
			accounts := a.installationReceiptAccounts[proposal.ProposalID]
			complete := true
			matched := true
			for _, account := range proposal.Addresses {
				if !accounts[account] {
					complete = false
					continue
				}
				r := a.installationReceiptMeta[proposal.ProposalID][account]
				if r.SourceShard != proposal.SourceShard || r.TargetShard != proposal.TargetShard || r.MappingVersion != proposal.BaseRouteVersion || !a.matchesPreparedReceipt(r) {
					matched = false
				}
			}
			if !complete {
				decision.Status = mappingAccepted
				decision.Reason = "state_installation_receipt_incomplete"
				decisions = append(decisions, decision)
				continue
			}
			if !matched {
				decision.Status = mappingAccepted
				decision.Reason = "state_installation_receipt_mismatch"
				decisions = append(decisions, decision)
				continue
			}
		}
		stale := false
		for _, account := range proposal.Addresses {
			if a.accountVersion[account] > proposal.BaseRouteVersion {
				stale = true
				break
			}
		}
		if stale {
			decision.Status = mappingRejected
			decision.Reason = "stale_account_version_at_apply"
		} else {
			changed := 0
			for _, account := range proposal.Addresses {
				if shardID, ok := a.active[account]; !ok || shardID != proposal.TargetShard {
					if proposal.SourceShard != proposal.TargetShard {
						a.migratedAccounts[account] = true
					}
					a.active[account] = proposal.TargetShard
					changed++
				}
			}
			if changed == 0 {
				decision.Status = mappingNoop
				decision.Reason = "already_at_target"
			} else {
				a.routeVersion++
				for _, account := range proposal.Addresses {
					a.accountVersion[account] = a.routeVersion
				}
				decision.Status = mappingApplied
				appliedEpoch := epoch
				delay := epoch - decision.SubmittedEpoch
				decision.AppliedEpoch = &appliedEpoch
				decision.RouteActivationDelayEpochs = &delay
				decision.Reason = "applied_at_effective_epoch"
				decision.AppliedAccounts = changed
			}
		}
		decision.NewRouteVersion = a.routeVersion
		a.decisionByID[proposal.ProposalID] = decision
		delete(a.pending, proposal.ProposalID)
		decisions = append(decisions, decision)
	}
	return decisions
}

func (a *mappingAuthority) stateDigest() string {
	if a == nil {
		return ""
	}
	accounts := make([]string, 0, len(a.active))
	for account := range a.active {
		accounts = append(accounts, account)
	}
	sort.Strings(accounts)
	digest := sha256.New()
	for _, account := range accounts {
		_, _ = fmt.Fprintf(digest, "%s=%d@%d\n", account, a.active[account], a.accountVersion[account])
	}
	_, _ = fmt.Fprintf(digest, "route_version=%d\n", a.routeVersion)
	return fmt.Sprintf("%x", digest.Sum(nil))
}
