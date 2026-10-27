package pbft_all

import (
	"blockEmulator/ownership"
	"encoding/json"
	"os"
)

// Emit once after chain construction, recovery and handler selection succeed.
// Security-off is explicitly observed too. No transaction path is modified.
// This log is observation, not a BFT proof or remote attestation.
func (p *PbftConsensusNode) logOwnershipStartup() {
	enabled, scope := p.CurChain.OwnershipSecurityEnabled(), "disabled"
	if enabled {
		if _, ok := p.ihm.(*RawRelayPbftExtraHandleMod); !ok {
			panic("ownership security requires the actual RawRelay consensus handler")
		}
		if _, ok := p.ohm.(*RawRelayOutsideModule); !ok {
			panic("ownership security requires the actual RawRelay network handler")
		}
		scope = "rawrelay_ingress_proposal_execution"
	}
	raw, err := json.Marshal(struct {
		Role               string                    `json:"role"`
		PID                int                       `json:"pid"`
		Shard              uint64                    `json:"shard"`
		Node               uint64                    `json:"node"`
		EnforcementEnabled bool                      `json:"enforcement_enabled"`
		GuardScope         string                    `json:"guard_scope"`
		Status             ownership.BootstrapStatus `json:"status"`
	}{"native", os.Getpid(), p.ShardID, p.NodeID, enabled, scope, p.CurChain.OwnershipSecurityStatus()})
	if err != nil {
		panic(err)
	}
	p.pl.Plog.Printf("OWNERSHIP_SECURITY_STATUS %s", raw)
}
