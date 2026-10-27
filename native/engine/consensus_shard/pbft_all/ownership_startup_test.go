package pbft_all

import (
	"blockEmulator/ownership"
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"testing"
)

func TestOwnershipStartupReportsActualHandlersAndView(t *testing.T) {
	open, _, _, _ := ownershipIngressFixture(t)
	module := open(0)
	node := module.pbftNode
	node.ihm, node.ohm = &RawRelayPbftExtraHandleMod{pbftNode: node}, module
	var output bytes.Buffer
	node.pl.Plog = log.New(&output, "", 0)
	// Changing the switch after bootstrap cannot fabricate enabled/disabled status.
	t.Setenv("SAA_OWNERSHIP_SECURITY_V1", "false")
	node.logOwnershipStartup()
	const marker = "OWNERSHIP_SECURITY_STATUS "
	line := strings.TrimSpace(output.String())
	if !strings.HasPrefix(line, marker) {
		t.Fatal("missing startup observation")
	}
	var record struct {
		Role               string                    `json:"role"`
		PID                int                       `json:"pid"`
		Shard              uint64                    `json:"shard"`
		Node               uint64                    `json:"node"`
		EnforcementEnabled bool                      `json:"enforcement_enabled"`
		GuardScope         string                    `json:"guard_scope"`
		Status             ownership.BootstrapStatus `json:"status"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, marker)), &record); err != nil {
		t.Fatal(err)
	}
	if record.Role != "native" || record.PID <= 0 || record.Shard != node.ShardID || record.Node != node.NodeID ||
		!record.EnforcementEnabled || record.GuardScope != "rawrelay_ingress_proposal_execution" ||
		!record.Status.Enabled || record.Status.Mode != "pinned_offline_bundle" || record.Status.Owners != 3 {
		t.Fatalf("wrong enforced startup observation: %+v", record)
	}
	if record.Status != node.CurChain.OwnershipSecurityStatus() {
		t.Fatal("status did not come from the actual chain")
	}
}

func TestOwnershipStartupRejectsUnsupportedActualHandler(t *testing.T) {
	open, _, _, _ := ownershipIngressFixture(t)
	for _, wrongOutside := range []bool{false, true} {
		t.Run(map[bool]string{false: "consensus", true: "network"}[wrongOutside], func(t *testing.T) {
			module := open(0)
			node := module.pbftNode
			node.ihm, node.ohm = &RawRelayPbftExtraHandleMod{pbftNode: node}, module
			if wrongOutside {
				node.ohm = &RawBrokerOutsideModule{pbftNode: node}
			} else {
				node.ihm = &RawBrokerPbftExtraHandleMod{pbftNode: node}
			}
			var output bytes.Buffer
			node.pl.Plog = log.New(&output, "", 0)
			defer func() {
				if recover() == nil || output.Len() != 0 {
					t.Fatal("unsupported handler reported security enabled")
				}
			}()
			node.logOwnershipStartup()
		})
	}
}
