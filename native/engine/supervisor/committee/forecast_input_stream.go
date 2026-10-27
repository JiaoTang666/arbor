package committee

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// recordForecastRoutedInput records relay input after route freezing, not a
// commit or a successful network delivery. The sequence is relay-owned.
func (m *RuntimeUnknownAdmissionManager) recordForecastRoutedInput(tx *core.Transaction) error {
	if m == nil || m.softDecisionMode != saaIntegratedDecisionMode {
		return nil
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	state := m.integratedStateLocked()
	if state.controllerMode != forecastControllerMode {
		return nil
	}
	if state.forecastStreamPath == "" || state.forecastStreamID == "" {
		return fmt.Errorf("forecast stream path and ID required")
	}
	if tx == nil || !tx.RoutingFrozen || tx.IngressSequence != state.forecastObservedSequence {
		return fmt.Errorf("unfrozen or noncontiguous relay input")
	}
	sender := normalizeAdmissionAddress(tx.Sender)
	recipient := normalizeAdmissionAddress(tx.Recipient)
	if normalizeMappingAuthorityAccount(sender) == "" || normalizeMappingAuthorityAccount(recipient) == "" {
		return fmt.Errorf("invalid forecast account identity")
	}
	tx.SenderRequiresExistingState = state.authority.migratedAccounts[sender]
	tx.RecipientRequiresExistingState = state.authority.migratedAccounts[recipient]
	record := map[string]interface{}{
		"schema": "forecast_routed_input_v1", "stream_id": state.forecastStreamID,
		"observation_kind": "relay_routed_input_not_commit", "accepted_sequence": tx.IngressSequence,
		"observed_at_unix_ns": time.Now().UnixNano(),
		"sender":              sender, "recipient": recipient, "logical_round": tx.RoutingEpoch,
		"mapping_version": tx.MappingVersion, "sender_shard": tx.RoutedSenderShard, "recipient_shard": tx.RoutedRecipientShard,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	flags := os.O_WRONLY | os.O_APPEND
	if state.forecastObservedSequence == 0 {
		flags |= os.O_CREATE | os.O_EXCL
	}
	file, err := os.OpenFile(state.forecastStreamPath, flags, 0600)
	if err != nil {
		return err
	}
	n, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if n != len(data) {
		return fmt.Errorf("short forecast stream write")
	}
	if closeErr != nil {
		return closeErr
	}
	state.forecastObservedSequence++
	return nil
}
