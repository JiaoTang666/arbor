package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// HandoffAudit covers persisted records only. Even Pending == 0 does NOT
// certify ingress exhaustion, queue durability, or permission to release a fence.
type HandoffAudit struct {
	Persisted int
	Confirmed int
	Pending   []string
}

func AuditMigrationHandoffs(directory, proposalID, account string, journalDirs ...string) (HandoffAudit, error) {
	if proposalID == "" || account == "" {
		return HandoffAudit{}, fmt.Errorf("proposal and account required")
	}
	all, err := AuditMigrationHandoffsForMembers(directory, map[string]string{account: proposalID}, journalDirs...)
	return all[account], err
}

// A group retirement audits the outbox and batch receipts once, instead of
// repeating the same whole-directory scan for every member on each proposal.
func AuditMigrationHandoffsForMembers(directory string, members map[string]string, journalDirs ...string) (map[string]HandoffAudit, error) {
	records, err := LoadMigrationOutbox(directory)
	if err != nil {
		return nil, err
	}
	return auditValidatedHandoffs(directory, members, records, validateCompletedAuditReceipt, journalDirs...)
}

// Same complete receipt audit, using the already validated immutable outbox
// index. No completed ACK, relay ACK, orphan or conflict check is skipped.
func (x *HandoffIndex) AuditMembers(members map[string]string, journalDirs ...string) (map[string]HandoffAudit, error) {
	records, _, err := x.Load(nil)
	if err != nil {
		return nil, err
	}
	x.auditMu.Lock()
	defer x.auditMu.Unlock()
	if x.auditConfirmed == nil {
		x.auditConfirmed = map[string][32]byte{}
	}
	verify := func(h *MigrationHandoff, receipt completedBatchReceiptItem) error {
		// Bind BOTH immutable work and the exact full completion proof. A new
		// or altered ACK must pass the original validator; no negative cache.
		raw, err := json.Marshal(struct {
			H *MigrationHandoff
			R completedBatchReceiptItem
		}{h, receipt})
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		if old, ok := x.auditConfirmed[h.ID]; ok && old == digest {
			return nil
		}
		if err := validateCompletedAuditReceipt(h, receipt); err != nil {
			return err
		}
		// Bounded accounting for this additional digest index, not RSS.
		if int64(len(x.auditConfirmed)+1)*256 <= x.limit {
			x.auditConfirmed[h.ID] = digest
		}
		return nil
	}
	return auditValidatedHandoffs(x.directory, members, records, verify, journalDirs...)
}

func validateCompletedAuditReceipt(h *MigrationHandoff, r completedBatchReceiptItem) error {
	if err := ValidateHandoffAck(h, r.Ack); err != nil {
		return err
	}
	if r.Ack.RelayPending {
		if r.Relay == nil {
			return fmt.Errorf("confirmed handoff missing relay evidence %s", h.ID)
		}
		return ValidateHandoffRelayAck(h, *r.Relay)
	}
	if r.Relay != nil {
		return fmt.Errorf("unexpected relay confirmation %s", h.ID)
	}
	return nil
}

func auditValidatedHandoffs(directory string, members map[string]string, records []*MigrationHandoff, verify func(*MigrationHandoff, completedBatchReceiptItem) error, journalDirs ...string) (map[string]HandoffAudit, error) {
	result := make(map[string]HandoffAudit, len(members))
	if len(members) == 0 {
		return nil, fmt.Errorf("audit members required")
	}
	for account, proposal := range members {
		if account == "" || proposal == "" {
			return nil, fmt.Errorf("proposal and account required")
		}
		result[account] = HandoffAudit{}
	}
	batch, err := loadCompletedBatchReceipts(directory, journalDirs...)
	if err != nil {
		return nil, err
	}
	known := make(map[string]*MigrationHandoff, len(records))
	for _, h := range records {
		known[h.ID] = h
	}
	for id, receipt := range batch {
		h := known[id]
		if h == nil {
			return nil, fmt.Errorf("batch receipt without outbox %s", id)
		}
		if err := verify(h, receipt); err != nil {
			return nil, err
		}
	}
	for _, h := range records {
		proposal, selected := members[h.Account]
		if !selected || h.ProposalID != proposal {
			continue
		}
		audit := result[h.Account]
		audit.Persisted++
		receipt, inBatch := batch[h.ID]
		raw, err := os.ReadFile(filepath.Join(directory, h.ID+".confirmed.json"))
		if os.IsNotExist(err) {
			if !inBatch {
				audit.Pending = append(audit.Pending, h.ID)
				result[h.Account] = audit
				continue
			}
		} else if err != nil {
			return nil, err
		} else {
			var legacy HandoffAck
			if err := json.Unmarshal(raw, &legacy); err != nil {
				return nil, err
			}
			if inBatch {
				a, _ := json.Marshal(legacy)
				b, _ := json.Marshal(receipt.Ack)
				if !bytes.Equal(a, b) {
					return nil, fmt.Errorf("legacy/batch confirmation conflict %s", h.ID)
				}
			} else {
				receipt.Ack = legacy
			}
		}
		if receipt.Ack.RelayPending {
			raw, err := os.ReadFile(filepath.Join(directory, h.ID+".relay-confirmed.json"))
			if err == nil {
				var legacy HandoffRelayAck
				if err := json.Unmarshal(raw, &legacy); err != nil {
					return nil, err
				}
				if inBatch {
					a, _ := json.Marshal(legacy)
					b, _ := json.Marshal(receipt.Relay)
					if !bytes.Equal(a, b) {
						return nil, fmt.Errorf("legacy/batch relay conflict %s", h.ID)
					}
				} else {
					receipt.Relay = &legacy
				}
			} else if !os.IsNotExist(err) {
				return nil, err
			}
			if receipt.Relay == nil {
				return nil, fmt.Errorf("confirmed handoff missing relay evidence %s", h.ID)
			}
		} else if receipt.Relay != nil {
			return nil, fmt.Errorf("unexpected relay confirmation %s", h.ID)
		}
		if err := verify(h, receipt); err != nil {
			return nil, err
		}
		audit.Confirmed++
		result[h.Account] = audit
	}
	return result, nil
}
