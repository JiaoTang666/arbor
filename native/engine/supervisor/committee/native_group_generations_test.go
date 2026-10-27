package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/params"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestControllerSixGenerationMovesRetainPriorRoutes(t *testing.T) {
	oldTotal := params.TotalDataSize
	params.TotalDataSize = 1000
	defer func() { params.TotalDataSize = oldTotal }()
	t.Setenv("SAA_SERIAL_GROUP_CONTROL", "true")
	dir := t.TempDir()
	for _, key := range []string{"SAA_GROUP_SOURCE_PREPARE_PATH", "SAA_GROUP_SOURCE_PREPARED_PATH", "SAA_GROUP_TARGET_INSTALL_PATH", "SAA_GROUP_TARGET_INSTALLED_PATH", "SAA_GROUP_TARGET_ACTIVATION_PATH", "SAA_GROUP_TARGET_ACTIVATED_PATH", "SAA_GROUP_SOURCE_RETIRED_PATH"} {
		t.Setenv(key, filepath.Join(dir, key))
	}
	initial := map[string]uint64{}
	accounts := []string{}
	for i := 1; i <= 12; i++ {
		accounts = append(accounts, fmt.Sprintf("%040x", 2*i))
	}
	for _, a := range accounts {
		initial[a] = 0
	}
	s := &serialGroupControl{directory: dir, continuous: true, next: 1, routes: map[string]uint64{}, history: map[string]nativeGroupSpec{}, used: map[string]bool{}, initial: initial, shards: 7}
	g := &nativeGroupControl{serial: s, stage: -1}
	poll := func() {
		t.Helper()
		if e := g.poll(); e != nil {
			t.Fatal(e)
		}
	}
	for round := 1; round <= 6; round++ {
		id := fmt.Sprintf("round-%d", round)
		spec := nativeGroupSpec{Arm: "move", AfterOccurrences: int64(round * 10)}
		for _, a := range accounts[(round-1)*2 : round*2] {
			spec.Command.Members = append(spec.Command.Members, core.SourcePrepareCommand{ProposalID: id, Account: a, SourceShard: 0, TargetShard: uint64(round), MappingVersion: uint64(round - 1), BindAtProposal: true})
		}
		p := serialGroupSelection{Schema: "saa_serial_selection_v2", Round: round, BaseVersion: uint64(round - 1), ObservedOccurrences: spec.AfterOccurrences, Decision: "MOVE", Spec: spec, ExpiresAt: time.Now().Add(time.Minute)}
		raw, _ := json.Marshal(p)
		if e := os.WriteFile(filepath.Join(dir, fmt.Sprintf("round-%03d.json", round)), raw, 0600); e != nil {
			t.Fatal(e)
		}
		poll()
		if s.loaded && g.spec.Command.Members[0].ProposalID == id {
			t.Fatal("future round admitted")
		}
		atomic.StoreInt64(&g.inputCount, spec.AfterOccurrences)
		poll()
		if g.stage != 1 || s.version != uint64(round-1) {
			t.Fatal("wrong pre-activation state")
		}
		write := func(key string, r interface{}, block, shard uint64) []byte {
			raw, _ := json.Marshal(groupControlEvidence{Record: mustSerialJSON(t, r), Committed: true, Block: block, Shard: shard, BlockHash: strings.Repeat("a", 64), StateRoot: strings.Repeat("b", 64)})
			if e := os.WriteFile(groupControlPath(key, id), raw, 0600); e != nil {
				t.Fatal(e)
			}
			return raw
		}
		prepared := core.GroupPreparation{PrepareBlock: 1, PrepareSequence: 1, CommitDigest: strings.Repeat("c", 64)}
		state := []byte("controller fixture, not native state")
		hash := sha256.Sum256(state)
		for _, c := range spec.Command.Members {
			c.ExpectedBlockHash = strings.Repeat("d", 64)
			c.ExpectedStateRoot = strings.Repeat("e", 64)
			c.ExpectedStateHash = hex.EncodeToString(hash[:])
			prepared.Command.Members = append(prepared.Command.Members, c)
			prepared.Members = append(prepared.Members, core.SourcePreparation{Command: c, State: state, StateHash: c.ExpectedStateHash, SourceStateRoot: c.ExpectedStateRoot, PrepareBlock: 1, PrepareSequence: 1, CommitDigest: prepared.CommitDigest})
		}
		write("SAA_GROUP_SOURCE_PREPARED_PATH", prepared, 1, 0)
		poll()
		install := chain.GroupTargetInstallation{Prepared: prepared, InstallBlock: 2, InstallSequence: 2, CommitDigest: strings.Repeat("f", 64)}
		write("SAA_GROUP_TARGET_INSTALLED_PATH", install, 2, uint64(round))
		poll()
		if s.version != uint64(round-1) {
			t.Fatal("installation exposed new version")
		}
		if round == 2 && s.routes[accounts[0]] != 1 {
			t.Fatal("earlier route disappeared")
		}
		activation := write("SAA_GROUP_TARGET_ACTIVATED_PATH", g.activation, 3, uint64(round))
		poll()
		if s.version != uint64(round) || s.routes[accounts[(round-1)*2]] != uint64(round) {
			t.Fatal("activation missing")
		}
		as := []string{}
		for _, c := range spec.Command.Members {
			as = append(as, c.Account)
		}
		drain := mustSerialJSON(t, map[string]interface{}{"Accounts": as, "Sequence": 4, "Transactions": [][]byte{}})
		retired := chain.GroupSourceRetirement{Block: 4, Sequence: 4, Digest: strings.Repeat("f", 64), Command: chain.GroupRetirementCommand{ActivationReceipt: activation, DrainSnapshot: drain}}
		write("SAA_GROUP_SOURCE_RETIRED_PATH", retired, 4, 0)
		poll()
	}
	atomic.StoreInt64(&g.inputCount, 1000)
	closeRecord := map[string]interface{}{"schema": "forest_stream_closed_v1", "last_round": 6, "observed_occurrences": 1000, "mapping_version": 6}
	if e := os.WriteFile(filepath.Join(dir, "closed.json"), mustSerialJSON(t, closeRecord), 0600); e != nil {
		t.Fatal(e)
	}
	poll()
	if !s.done || len(s.history) != 6 || len(s.routes) != 12 {
		t.Fatal("serial completion lost history")
	}
}
