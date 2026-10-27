package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
)

// Requires a previously persisted outbox record. Acknowledgement persistence
// precedes dequeue; source account ownership/fences are never released here.
func (p *TxPool) ConfirmMigrationHandoff(directory string, h *MigrationHandoff, ack HandoffAck, relay ...HandoffRelayAck) error {
	return p.confirmMigrationOriginal(directory, h, ack, h.Original, relay...)
}
func (p *TxPool) ConfirmInboundHandoff(directory string, h *MigrationHandoff, ack HandoffAck, original *Transaction) error {
	if original == nil || original.Relayed {
		return fmt.Errorf("inbound original required")
	}
	copyTx := DecodeTx(original.Encode())
	copyTx.Relayed = true
	if !bytes.Equal(copyTx.Encode(), h.Original) {
		return fmt.Errorf("inbound original does not produce acknowledged credit")
	}
	return p.confirmMigrationOriginal(directory, h, ack, original.Encode())
}
func (p *TxPool) confirmMigrationOriginal(directory string, h *MigrationHandoff, ack HandoffAck, original []byte, relay ...HandoffRelayAck) error {
	if err := ValidateHandoffAck(h, ack); err != nil {
		return err
	}
	if ack.RelayPending {
		if len(relay) != 1 {
			return fmt.Errorf("handoff still has an unconfirmed relay leg")
		}
		if err := ValidateHandoffRelayAck(h, relay[0]); err != nil {
			return err
		}
		data, err := json.Marshal(relay[0])
		if err != nil {
			return err
		}
		if err = PublishSourcePrepareFile(filepath.Join(directory, h.ID+".relay-confirmed.json"), append(data, '\n')); err != nil {
			return err
		}
	}
	raw, err := ReadMigrationArtifact(directory, h.ID, ".handoff.json")
	if err != nil {
		return err
	}
	var stored MigrationHandoff
	if err = json.Unmarshal(raw, &stored); err != nil {
		return err
	}
	expected, _ := json.Marshal(h)
	actual, _ := json.Marshal(stored)
	if !bytes.Equal(expected, actual) {
		return fmt.Errorf("ack does not match persisted outbox")
	}
	p.journalGate.Lock()
	defer p.journalGate.Unlock()
	if p.lanes != nil {
		return p.confirmIsolatedLocked(map[string]bool{string(original): true}, func() error {
			data, err := json.Marshal(ack)
			if err != nil {
				return err
			}
			return PublishSourcePrepareFile(filepath.Join(directory, h.ID+".confirmed.json"), append(data, '\n'))
		})
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	index := -1
	for i, tx := range p.TxQueue {
		if bytes.Equal(tx.Encode(), original) {
			if index != -1 {
				return fmt.Errorf("ambiguous duplicate source occurrence; refusing dequeue")
			}
			index = i
		}
	}
	data, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	if err = PublishSourcePrepareFile(filepath.Join(directory, h.ID+".confirmed.json"), append(data, '\n')); err != nil {
		return err
	}
	if index >= 0 {
		remaining := append([]*Transaction(nil), p.TxQueue[:index]...)
		remaining = append(remaining, p.TxQueue[index+1:]...)
		if err = p.persistMigrationQueueLocked(remaining); err != nil {
			return err
		}
		copy(p.TxQueue[index:], p.TxQueue[index+1:])
		p.TxQueue[len(p.TxQueue)-1] = nil
		p.TxQueue = p.TxQueue[:len(p.TxQueue)-1]
	}
	return nil
}
