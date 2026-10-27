package committee

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"strings"
	"testing"
	"time"
)

func asyncCloseoutFixture(t *testing.T) (*forestAsyncPipeline, *bytes.Buffer) {
	t.Helper()
	old := params.ShardNum
	params.ShardNum = 2
	t.Cleanup(func() { params.ShardNum = old })
	audit := &bytes.Buffer{}
	e := &clpaEpochController{audit: audit, dispatched: 10, ccm: &CLPACommitteeModule{},
		counts: map[uint64]int{0: 200, 1: 200}, committedOverrides: map[string]uint64{}}
	a := &forestAsyncPipeline{e: e, p: &forestEpochProvider{}, majorRounds: 400, nextStart: 200,
		inputDone: true, intents: make(chan forestAsyncJob, 1), results: make(chan *forestAsyncResult, 1), notify: make(chan struct{}, 1)}
	e.async = a
	return a, audit
}

func TestAsyncInputEOFDoesNotCancelPeriodicPlanning(t *testing.T) {
	a, _ := asyncCloseoutFixture(t)
	a.observed.Store(3) // Deliberately behind dispatched: no full-input gate.
	a.tick()
	select {
	case job := <-a.intents:
		if job.reason != "PERIODIC_PRECOMPUTE" || a.finalPlanned {
			t.Fatal("input EOF replaced periodic planning with finalization")
		}
	default:
		t.Fatal("periodic planning was cancelled while native work remained")
	}
}

func TestAsyncNativeCompletionWaitsForFullObservationBeforeArchiveJob(t *testing.T) {
	a, _ := asyncCloseoutFixture(t)
	a.e.nativeCompleteAt = time.Now()
	a.observed.Store(9)
	a.tick()
	if len(a.intents) != 0 || a.finishedLocked() {
		t.Fatal("incomplete evidence started or closed a final archive")
	}
	a.observed.Store(10)
	a.tick()
	job := <-a.intents
	if job.reason != "EOF_ARCHIVE" || !a.finalPlanned || a.finishedLocked() {
		t.Fatal("full observation did not start a pending evidence-only archive")
	}
}

func TestAsyncFinalCandidateIsNotAnOwnershipChange(t *testing.T) {
	a, audit := asyncCloseoutFixture(t)
	account := strings.Repeat("0", 39) + "2"
	a.p.initialRoutes = map[string]uint64{account: 0}
	a.e.initialRoutes = a.p.initialRoutes
	a.e.ccm.modifiedMap = map[string]uint64{account: 0}
	a.e.nativeCompleteAt = time.Now()
	a.observed.Store(10)
	a.finalPlanned, a.busy = true, true
	a.ready = &forestAsyncResult{job: forestAsyncJob{generation: 1, reason: "EOF_ARCHIVE", overrides: map[string]uint64{}},
		reply: forestEpochReply{ObservedPrefix: 10, Delta: map[string]uint64{account: 1},
			CurrentMappingSHA256: clpaEpochMappingDigest(nil)}}
	a.tick()
	if !a.finishedLocked() || a.e.epoch != 0 || a.e.plan != nil || len(a.e.committedOverrides) != 0 || a.e.ccm.modifiedMap[account] != 0 {
		t.Fatalf("archived candidate changed ownership or failed evidence closeout: %v", a.e.failure)
	}
	if !strings.Contains(audit.String(), `"disposition":"NOT_APPLIED"`) ||
		!strings.Contains(audit.String(), `"final_snapshot":true`) {
		t.Fatal("final candidate lacks explicit non-install evidence")
	}
}

func TestAsyncLatePeriodicCandidateIsArchivedBeforeFullEOFEvidence(t *testing.T) {
	a, audit := asyncCloseoutFixture(t)
	a.e.nativeCompleteAt = time.Now()
	a.observed.Store(9)
	a.busy = true
	a.ready = &forestAsyncResult{job: forestAsyncJob{generation: 1, reason: "PERIODIC_PRECOMPUTE", overrides: map[string]uint64{}},
		reply: forestEpochReply{ObservedPrefix: 3, Delta: map[string]uint64{}, CurrentMappingSHA256: clpaEpochMappingDigest(nil)}}
	a.tick()
	if a.ready != nil || a.busy || a.finalArchived || a.finishedLocked() || len(a.intents) != 0 {
		t.Fatal("late periodic result was installed or mistaken for full EOF evidence")
	}
	if !strings.Contains(audit.String(), `"final_snapshot":false`) {
		t.Fatal("late periodic candidate lacks a distinct archive disposition")
	}
	a.observed.Store(10)
	a.tick()
	if job := <-a.intents; job.reason != "EOF_ARCHIVE" {
		t.Fatal("full EOF evidence was not separately requested")
	}
}

func TestAsyncArchiveCannotCancelAnAlreadyBroadcastInstallation(t *testing.T) {
	a, audit := asyncCloseoutFixture(t)
	a.e.nativeCompleteAt = time.Now()
	a.e.plan = &message.CLPAEpochPlan{Epoch: 1}
	a.busy = true
	a.ready = &forestAsyncResult{}
	a.tick()
	if a.e.plan == nil || a.ready == nil || !a.busy || audit.Len() != 0 || a.finishedLocked() {
		t.Fatal("native completion cancelled an already started installation")
	}
}
