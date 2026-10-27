package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNativeGroupControlWaitsForWholeCommittedActivation(t *testing.T) {
	for _, key := range []string{"SAA_GROUP_SOURCE_PREPARE_PATH", "SAA_GROUP_SOURCE_PREPARED_PATH", "SAA_GROUP_TARGET_INSTALL_PATH", "SAA_GROUP_TARGET_INSTALLED_PATH", "SAA_GROUP_TARGET_ACTIVATION_PATH", "SAA_GROUP_TARGET_ACTIVATED_PATH", "SAA_GROUP_SOURCE_RETIRED_PATH"} {
		t.Setenv(key, filepath.Join(t.TempDir(), "evidence.json"))
	}
	accounts := []string{strings.Repeat("0", 39) + "2", strings.Repeat("0", 39) + "4"}
	spec := nativeGroupSpec{Arm: "move", AfterOccurrences: 10}
	initial := map[string]uint64{}
	for _, a := range accounts {
		spec.Command.Members = append(spec.Command.Members, core.SourcePrepareCommand{ProposalID: "group", Account: a, SourceShard: 0, TargetShard: 1, BindAtProposal: true})
		initial["0x"+a] = 0
	}
	g := &nativeGroupControl{spec: spec, authority: newMappingAuthority(2, initial)}
	g.authority.enableInstallationReceiptGate()
	if e := g.poll(); e != nil {
		t.Fatal(e)
	}
	if g.stage != 0 {
		t.Fatal("started before observed prefix")
	}
	atomic.StoreInt64(&g.inputCount, 10)
	if e := g.poll(); e != nil {
		t.Fatal(e)
	}
	if g.stage != 1 || g.active {
		t.Fatal("premature routing")
	}
	write := func(key string, record interface{}, block, shard uint64) {
		raw, _ := json.Marshal(map[string]interface{}{"record": record, "block": block, "shard": shard, "committed": true, "block_hash": strings.Repeat("a", 64), "state_root": strings.Repeat("b", 64)})
		if e := os.WriteFile(os.Getenv(key), raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	p := core.GroupPreparation{PrepareBlock: 1, PrepareSequence: 1, CommitDigest: strings.Repeat("c", 64)}
	state := []byte("opaque state fixture")
	hash := sha256.Sum256(state)
	for _, c := range spec.Command.Members {
		c.ExpectedBlockHash = strings.Repeat("d", 64)
		c.ExpectedStateRoot = strings.Repeat("e", 64)
		c.ExpectedStateHash = hex.EncodeToString(hash[:])
		p.Command.Members = append(p.Command.Members, c)
		p.Members = append(p.Members, core.SourcePreparation{Command: c, State: state, StateHash: c.ExpectedStateHash, SourceStateRoot: c.ExpectedStateRoot, PrepareBlock: 1, PrepareSequence: 1, CommitDigest: p.CommitDigest})
	}
	write("SAA_GROUP_SOURCE_PREPARED_PATH", p, 1, 0)
	if e := g.poll(); e != nil {
		t.Fatal(e)
	}
	installed := chain.GroupTargetInstallation{Prepared: p, InstallBlock: 2, InstallSequence: 2, CommitDigest: strings.Repeat("f", 64)}
	partial := installed
	partial.Prepared.Members = partial.Prepared.Members[:1]
	write("SAA_GROUP_TARGET_INSTALLED_PATH", partial, 2, 1)
	if e := g.poll(); e == nil || g.active || g.authority.routeVersion != 0 {
		t.Fatal("partial installation accepted")
	}
	write("SAA_GROUP_TARGET_INSTALLED_PATH", installed, 2, 1)
	if e := g.poll(); e != nil {
		t.Fatal(e)
	}
	if g.active || g.stage != 3 || g.authority.routeVersion != 1 {
		t.Fatal("installation prematurely routed or authority missing")
	}
	bad := g.activation
	bad.Members = append([]chain.TargetActivation(nil), bad.Members...)
	bad.Members[1].MappingVersion++
	write("SAA_GROUP_TARGET_ACTIVATED_PATH", bad, 3, 1)
	if e := g.poll(); e == nil || g.active {
		t.Fatal("mismatched activation accepted")
	}
	write("SAA_GROUP_TARGET_ACTIVATED_PATH", g.activation, 3, 1)
	if e := g.poll(); e != nil {
		t.Fatal(e)
	}
	if !g.active || g.stage != 4 {
		t.Fatal("matching commit did not enable routes")
	}
	for _, a := range accounts {
		shard, ok := g.authority.resolve("0x" + a)
		if !ok || shard != 1 {
			t.Fatal("group route mismatch")
		}
	}
}

func TestNativeGroupKeepDoesNotPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "request.json")
	t.Setenv("SAA_GROUP_SOURCE_PREPARE_PATH", path)
	g := &nativeGroupControl{spec: nativeGroupSpec{Arm: "keep", AfterOccurrences: 1, Command: core.GroupPrepareCommand{Members: []core.SourcePrepareCommand{{ProposalID: "g"}}}}}
	atomic.StoreInt64(&g.inputCount, 1)
	if e := g.poll(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("KEEP wrote migration request")
	}
}
