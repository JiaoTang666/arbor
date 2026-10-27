package committee

import (
	"blockEmulator/core"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func deferredFixture(t *testing.T, arm string) (*nativeGroupControl, nativeGroupSpec, time.Time) {
	t.Helper()
	dir := t.TempDir()
	a, b := strings.Repeat("0", 39)+"2", strings.Repeat("0", 39)+"4"
	spec := nativeGroupSpec{Arm: arm, AfterOccurrences: 10}
	for _, account := range []string{a, b} {
		spec.Command.Members = append(spec.Command.Members, core.SourcePrepareCommand{ProposalID: "dynamic-group", Account: account, SourceShard: 0, TargetShard: 1, BindAtProposal: true})
	}
	g := &nativeGroupControl{spec: nativeGroupSpec{Arm: arm, AfterOccurrences: 10}, stage: -1, deferred: &deferredGroupControl{routes: map[string]uint64{a: 0, b: 0}, shards: 2, requestPath: filepath.Join(dir, "request.json"), selectionPath: filepath.Join(dir, "selection.json"), deadline: 60 * time.Second}}
	return g, spec, time.Unix(1000, 0)
}
func saveDeferredSelection(t *testing.T, g *nativeGroupControl, spec nativeGroupSpec, requestID string) {
	t.Helper()
	raw, _ := json.Marshal(deferredGroupSelection{Schema: "saa_queue_selection_v1", RequestID: requestID, Decision: "MOVE", Reason: "test", Spec: spec})
	if err := os.WriteFile(g.deferred.selectionPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
}
func TestDeferredGroupMissingSelectionDoesNotBlockInput(t *testing.T) {
	g, _, now := deferredFixture(t, "move")
	if proceed, e := g.pollDeferredGroup(now); e != nil || proceed || g.deferred.request != nil {
		t.Fatal("requested before prefix")
	}
	atomic.StoreInt64(&g.inputCount, 10)
	if proceed, e := g.pollDeferredGroup(now); e != nil || proceed || g.deferred.request == nil {
		t.Fatal("missing selection should return immediately")
	}
	atomic.StoreInt64(&g.inputCount, 300)
	if proceed, e := g.pollDeferredGroup(now.Add(61 * time.Second)); e != nil || proceed || !g.retired || g.active || atomic.LoadInt64(&g.inputCount) != 300 {
		t.Fatal("deadline did not choose KEEP without changing ingress")
	}
	if e := g.poll(); e != nil {
		t.Fatal(e)
	} // terminal no-member state never indexes member zero
}
func TestDeferredGroupInstallsOnceAndKeepArmNeverWritesMigration(t *testing.T) {
	g, spec, now := deferredFixture(t, "keep")
	atomic.StoreInt64(&g.inputCount, 10)
	if _, e := g.pollDeferredGroup(now); e != nil {
		t.Fatal(e)
	}
	saveDeferredSelection(t, g, spec, g.deferred.request.RequestID)
	if proceed, e := g.pollDeferredGroup(now.Add(time.Second)); e != nil || !proceed || g.deferred != nil || g.authority == nil || g.active {
		t.Fatal("valid selection not staged")
	}
	path := filepath.Join(t.TempDir(), "prepare.json")
	t.Setenv("SAA_GROUP_SOURCE_PREPARE_PATH", path)
	if e := g.poll(); e != nil || g.stage != 99 || g.active {
		t.Fatal("KEEP arm moved")
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("KEEP wrote migration")
	}
}
func TestDeferredGroupRejectsWrongRequestAndUnknownAccount(t *testing.T) {
	g, spec, now := deferredFixture(t, "move")
	atomic.StoreInt64(&g.inputCount, 10)
	_, _ = g.pollDeferredGroup(now)
	saveDeferredSelection(t, g, spec, "wrong-request")
	if proceed, e := g.pollDeferredGroup(now.Add(time.Second)); e != nil || proceed || !g.retired || g.active {
		t.Fatal("wrong request activated")
	}
	spec.Command.Members[1].Account = strings.Repeat("0", 39) + "6"
	if _, e := validateDeferredGroupSpec(spec, "move", 10, g.deferred.routes, 2); e == nil {
		t.Fatal("unobserved/noninitial member accepted")
	}
}
