package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"fmt"
)

func groupRequestID(kind message.RequestType, raw []byte) string {
	switch kind {
	case groupPrepareRequest:
		var c core.GroupPrepareCommand
		if json.Unmarshal(raw, &c) == nil && len(c.Members) > 0 {
			return c.Members[0].ProposalID
		}
	case groupInstallRequest:
		var p core.GroupPreparation
		if json.Unmarshal(raw, &p) == nil && len(p.Members) > 0 {
			return p.Members[0].Command.ProposalID
		}
	case groupActivateRequest:
		var c chain.GroupTargetActivation
		if json.Unmarshal(raw, &c) == nil && len(c.Members) > 0 {
			return c.Members[0].ProposalID
		}
	}
	return ""
}

func groupEvidenceID(record interface{}) (string, error) {
	switch p := record.(type) {
	case *core.GroupPreparation:
		if len(p.Members) > 0 {
			return p.Members[0].Command.ProposalID, nil
		}
	case *chain.GroupTargetInstallation:
		if len(p.Prepared.Members) > 0 {
			return p.Prepared.Members[0].Command.ProposalID, nil
		}
	case chain.GroupTargetActivation:
		if len(p.Members) > 0 {
			return p.Members[0].ProposalID, nil
		}
	case *chain.GroupSourceRetirement:
		var r struct {
			Record chain.GroupTargetActivation `json:"record"`
		}
		if json.Unmarshal(p.Command.ActivationReceipt, &r) == nil && len(r.Record.Members) > 0 {
			return r.Record.Members[0].ProposalID, nil
		}
	}
	return "", fmt.Errorf("group evidence has no proposal identity")
}
