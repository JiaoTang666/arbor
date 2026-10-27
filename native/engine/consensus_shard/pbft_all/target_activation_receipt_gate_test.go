package pbft_all

import (
	"blockEmulator/chain"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestActivationReceiptGateMissingMatchingAndConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	t.Setenv("SAA_NATIVE_TARGET_ACTIVATED_PATH", path)
	c := chain.TargetActivation{ProposalID: "p", Account: "a", TargetShard: 1, MappingVersion: 1}
	if ok, err := targetActivationConfirmed(c); ok || err != nil {
		t.Fatal("missing receipt must wait")
	}
	raw, _ := json.Marshal(map[string]interface{}{"activation": c, "block": 1, "block_hash": strings.Repeat("1", 64), "state_root": strings.Repeat("2", 64)})
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if ok, err := targetActivationConfirmed(c); !ok || err != nil {
		t.Fatal(err)
	}
	c.MappingVersion++
	if ok, err := targetActivationConfirmed(c); ok || err == nil {
		t.Fatal("conflicting version accepted")
	}
}
