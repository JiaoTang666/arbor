package chain

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestGroupRetirementRequiresFullSnapshotKind(t *testing.T) {
	bc, cmd, _ := groupFixture(t)
	defer bc.CloseBlockChain()
	p, err := bc.CommitGroupSourcePreparation(cmd, 2, make([]byte, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	a := GroupTargetActivation{}
	accounts := []string{}
	for _, m := range p.Members {
		accounts = append(accounts, m.Command.Account)
		a.Members = append(a.Members, TargetActivation{Account: m.Command.Account, ProposalID: m.Command.ProposalID, TargetShard: m.Command.TargetShard, MappingVersion: m.Command.MappingVersion + 1, StateHash: m.StateHash, InstallDigest: strings.Repeat("1", 64)})
	}
	receipt, err := json.Marshal(map[string]interface{}{"record": a, "committed": true, "block": 3, "block_hash": strings.Repeat("2", 64), "state_root": strings.Repeat("3", 64), "shard": p.Members[0].Command.TargetShard})
	if err != nil {
		t.Fatal(err)
	}
	head := append([]byte(nil), bc.CurrentBlock.Hash...)
	for _, tc := range []struct {
		name, kind     string
		omit, wantPass bool
	}{
		{"legacy_missing", "", true, true}, {"legacy_empty", "", false, true}, {"full", "full", false, true},
		{"append", "append", false, false}, {"prepend", "prepend", false, false}, {"unknown", "bogus", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := map[string]interface{}{"Account": "", "Accounts": accounts, "Sequence": uint64(1), "Transactions": nil}
			if !tc.omit {
				s["Kind"] = tc.kind
			}
			raw, _ := json.Marshal(s)
			err := bc.ValidateGroupRetirement(GroupRetirementCommand{ActivationReceipt: receipt, DrainSnapshot: raw})
			if (err == nil) != tc.wantPass {
				t.Fatalf("kind=%q wantPass=%v error=%v", tc.kind, tc.wantPass, err)
			}
			if !bytes.Equal(head, bc.CurrentBlock.Hash) {
				t.Fatal("validation changed committed state")
			}
		})
	}
}
