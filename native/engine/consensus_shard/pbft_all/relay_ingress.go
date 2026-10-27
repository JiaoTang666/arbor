package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"os"
	"time"
)

func (n *PbftConsensusNode) nextRelayInput() (*chain.HandoffExecution, error) {
	dir := os.Getenv("SAA_NATIVE_HANDOFF_RELAY_DIR")
	if dir == "" {
		return nil, nil
	}
	entries, err := n.indexedArtifacts(dir, ".relay.json")
	if err != nil {
		return nil, err
	}
	for _, id := range entries.IDs() {
		raw := entries[id]
		var record chain.HandoffExecution
		if err = json.Unmarshal(raw, &record); err != nil {
			return nil, err
		}
		if err = core.ValidateMigrationHandoff(&record.Handoff); err != nil {
			return nil, err
		}
		tx := core.DecodeTx(record.Handoff.Execution)
		if tx.RoutedRecipientShard != n.ShardID {
			continue
		}
		if done, ok := n.sourcePrepareSeen.Load("relay:" + record.Handoff.ID); ok && done == true {
			continue
		}
		return &record, nil
	}
	return nil, nil
}
func (n *PbftConsensusNode) relayProposal() (*message.Request, error) {
	record, err := n.nextRelayInput()
	if err != nil || record == nil {
		return nil, err
	}
	if err = n.CurChain.ValidateHandoffRelay(*record); err != nil {
		if n.CurChain.ValidateRelayRedirect(*record) == nil {
			return makeRedirectRequest(relayReserveRequest, record)
		}
		return nil, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	return &message.Request{RequestType: handoffRelayRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: data}}, nil
}
