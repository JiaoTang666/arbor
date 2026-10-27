package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const inboundDebitRequest message.RequestType = "SEFInboundDebitV1"

type inboundDebitCommand struct {
	Transaction    *core.Transaction
	MappingVersion uint64
}

func decodeInboundDebit(r *message.Request) (inboundDebitCommand, error) {
	var c inboundDebitCommand
	if r == nil || r.RequestType != inboundDebitRequest {
		return c, fmt.Errorf("wrong inbound request")
	}
	err := json.Unmarshal(r.Msg.Content, &c)
	return c, err
}
func (n *PbftConsensusNode) commitInboundDebit(r *message.Request, digest []byte) error {
	return n.commitInboundDebitPublishing(r, digest, core.PublishSourcePrepareFile)
}
func (n *PbftConsensusNode) commitInboundDebitPublishing(r *message.Request, digest []byte, publish func(string, []byte) error) error {
	if string(getDigest(r)) != string(digest) {
		return fmt.Errorf("inbound digest mismatch")
	}
	c, err := decodeInboundDebit(r)
	if err != nil {
		return err
	}
	record, err := n.CurChain.CommitInboundDebit(c.Transaction, c.MappingVersion)
	if err != nil {
		return err
	}
	if dir := os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR"); dir != "" && n.NodeID == uint64(n.view.Load()) {
		if err = core.ValidateMigrationHandoff(&record.Credit); err != nil {
			return err
		}
		raw, err := json.Marshal(record.Credit)
		if err != nil {
			return err
		}
		return publish(filepath.Join(dir, record.Credit.ID+".handoff.json"), append(raw, '\n'))
	}
	return nil
}
