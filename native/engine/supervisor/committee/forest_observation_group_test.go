package committee

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func forestGroupRow(seq uint64) forestEpochObservation {
	return forestEpochObservation{Sequence: seq, SourceTime: 1700000000,
		Sender: forestTestA, Recipient: forestTestB}
}

func forestGroupLine(rows []forestEpochObservation) []byte {
	raw, _ := json.Marshal(rows)
	return append(raw, '\n')
}

func TestAsyncObservationGroupDrainsAvailableEightAndTail(t *testing.T) {
	var input bytes.Buffer
	for seq := uint64(0); seq < 10; seq++ {
		input.Write(forestGroupLine([]forestEpochObservation{forestGroupRow(seq)}))
	}
	j := forestObservationJournalReader{r: bufio.NewReaderSize(&input, 32)}
	first, err := j.group(10)
	if err != nil || len(first) != 8 || first[7][0].Sequence != 7 {
		t.Fatalf("first group lost boundaries: %v %v", first, err)
	}
	tail, err := j.group(10)
	if err != nil || len(tail) != 2 || tail[0][0].Sequence != 8 || tail[1][0].Sequence != 9 {
		t.Fatalf("EOF tail was lost or waited to fill: %v %v", tail, err)
	}
}

func TestAsyncObservationGroupPreservesPartialAndUndispatchedBatch(t *testing.T) {
	var input bytes.Buffer
	input.Write(forestGroupLine([]forestEpochObservation{forestGroupRow(0)}))
	line := forestGroupLine([]forestEpochObservation{forestGroupRow(1)})
	input.Write(line[:len(line)/2])
	j := forestObservationJournalReader{r: bufio.NewReader(&input)}
	group, err := j.group(2)
	if err != nil || len(group) != 1 || len(j.partial) == 0 {
		t.Fatalf("partial line consumed: %v %v", group, err)
	}
	input.Write(line[len(line)/2:])
	group, err = j.group(1)
	if err != nil || len(group) != 0 || j.pending == nil {
		t.Fatalf("undispatched complete batch consumed: %v %v", group, err)
	}
	group, err = j.group(2)
	if err != nil || len(group) != 1 || group[0][0].Sequence != 1 || j.pending != nil {
		t.Fatalf("pending batch not resumed exactly once: %v %v", group, err)
	}
}

func TestAsyncObservationGroupEncodedSizeBound(t *testing.T) {
	var input bytes.Buffer
	for batch := 0; batch < 3; batch++ {
		rows := make([]forestEpochObservation, 6000)
		for i := range rows {
			rows[i] = forestGroupRow(uint64(batch*len(rows) + i))
		}
		input.Write(forestGroupLine(rows))
	}
	j := forestObservationJournalReader{r: bufio.NewReader(&input)}
	group, err := j.group(18000)
	raw, _ := json.Marshal(forestEpochRequest{Schema: forestEpochWorkerSchema, ID: 1, Op: "observe_group", Batches: group})
	if err != nil || len(group) != 2 || len(raw) > forestEpochObserveGroupBytes || j.pending == nil {
		t.Fatalf("group transport bound not preserved: batches=%d bytes=%d err=%v", len(group), len(raw), err)
	}
	tail, err := j.group(18000)
	if err != nil || len(tail) != 1 || tail[0][0].Sequence != 12000 {
		t.Fatal("size-limited tail lost", err)
	}
	tooLarge := forestObservationJournalReader{r: bufio.NewReader(strings.NewReader(strings.Repeat(" ", forestEpochObserveGroupBytes) + "\n"))}
	if _, err := tooLarge.group(1); err == nil {
		t.Fatal("unbounded individual journal batch accepted")
	}
}

func TestForestObserveGroupRejectsWholeInvalidGroupBeforeWorker(t *testing.T) {
	for _, bad := range []string{"sequence", "time", "address", "empty", "too_many"} {
		t.Run(bad, func(t *testing.T) {
			batches := [][]forestEpochObservation{{forestGroupRow(0)}, {forestGroupRow(1)}}
			switch bad {
			case "sequence":
				batches[1][0].Sequence = 0
			case "time":
				batches[1][0].SourceTime = math.NaN()
			case "address":
				batches[1][0].Sender = "0x" + forestTestA
			case "empty":
				batches[1] = nil
			case "too_many":
				batches = make([][]forestEpochObservation, 9)
			}
			called := false
			p := &forestEpochProvider{prefixHash: strings.Repeat("a", 64), call: func(forestEpochRequest) (forestEpochReply, error) {
				called = true
				return forestEpochReply{}, nil
			}}
			if err := p.ObserveGroup(batches); err == nil || called || p.observed != 0 || p.prefixHash != strings.Repeat("a", 64) {
				t.Fatal("invalid later batch was partially published")
			}
		})
	}
}

func TestForestObserveGroupPublishesOnlyValidCommitAcknowledgement(t *testing.T) {
	for _, result := range []string{"success", "error", "prefix", "hash"} {
		t.Run(result, func(t *testing.T) {
			oldHash, newHash := strings.Repeat("a", 64), strings.Repeat("b", 64)
			p := &forestEpochProvider{prefixHash: oldHash}
			p.call = func(r forestEpochRequest) (forestEpochReply, error) {
				if r.Op != "observe_group" || len(r.Batches) != 2 || r.ObservedPrefix != 0 || p.observed != 0 || p.prefixHash != oldHash {
					t.Fatal("group exposed intermediate watermark or changed boundaries")
				}
				reply := forestEpochReply{ObservedPrefix: 2, PrefixSHA256: newHash}
				switch result {
				case "error":
					return reply, fmt.Errorf("commit failed")
				case "prefix":
					reply.ObservedPrefix = 1
				case "hash":
					reply.PrefixSHA256 = "invalid"
				}
				return reply, nil
			}
			err := p.ObserveGroup([][]forestEpochObservation{{forestGroupRow(0)}, {forestGroupRow(1)}})
			if result == "success" {
				if err != nil || p.observed != 2 || p.prefixHash != newHash {
					t.Fatal("committed group did not advance", err)
				}
			} else if err == nil || p.observed != 0 || p.prefixHash != oldHash {
				t.Fatal("failed group published a prefix", err)
			}
		})
	}
}

func newForestGroupConsumer(t *testing.T, count int, call func(forestEpochRequest) (forestEpochReply, error)) *forestAsyncPipeline {
	t.Helper()
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	r, err := os.Open(path)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	a := &forestAsyncPipeline{e: &clpaEpochController{audit: io.Discard, dispatched: count},
		p: &forestEpochProvider{call: call}, journal: f, reader: r,
		notify: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		intents: make(chan forestAsyncJob, 1), results: make(chan *forestAsyncResult, 1)}
	for seq := 0; seq < count; seq++ {
		if err = a.append([]forestEpochObservation{forestGroupRow(uint64(seq))}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { a.Close() })
	return a
}

func TestAsyncObservationGroupYieldsToPlanBetweenBoundedGroups(t *testing.T) {
	var a *forestAsyncPipeline
	var ops []string
	var sizes []int
	a = newForestGroupConsumer(t, 10, func(r forestEpochRequest) (forestEpochReply, error) {
		ops = append(ops, r.Op)
		switch r.Op {
		case "observe_group":
			sizes = append(sizes, len(r.Batches))
			if len(sizes) == 1 {
				a.intents <- forestAsyncJob{generation: 1, overrides: map[string]uint64{}}
			}
			last := r.Batches[len(r.Batches)-1]
			return forestEpochReply{ObservedPrefix: last[len(last)-1].Sequence + 1, PrefixSHA256: strings.Repeat("a", 64)}, nil
		case "plan_start":
			return forestEpochReply{Running: true, Epoch: 1, ObservedPrefix: 8, PrefixSHA256: strings.Repeat("a", 64), CurrentMappingSHA256: r.CurrentMappingSHA256}, nil
		case "plan_poll":
			return forestEpochReply{Epoch: 1, ObservedPrefix: 8, PrefixSHA256: strings.Repeat("a", 64), CurrentMappingSHA256: clpaEpochMappingDigest(nil), Delta: map[string]uint64{}, Report: []byte(`{}`), Evidence: []byte(`{}`)}, nil
		}
		return forestEpochReply{}, fmt.Errorf("unexpected op %s", r.Op)
	})
	go a.consume()
	deadline := time.Now().Add(5 * time.Second)
	for a.observed.Load() != 10 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if a.observed.Load() != 10 || !reflect.DeepEqual(sizes, []int{8, 2}) || !reflect.DeepEqual(ops, []string{"observe_group", "plan_start", "plan_poll", "observe_group"}) {
		t.Fatalf("group starved planning or lost tail: observed=%d sizes=%v ops=%v", a.observed.Load(), sizes, ops)
	}
}

func TestAsyncObservationGroupFailureDoesNotPublish(t *testing.T) {
	a := newForestGroupConsumer(t, 2, func(forestEpochRequest) (forestEpochReply, error) {
		return forestEpochReply{}, fmt.Errorf("group commit failed")
	})
	go a.consume()
	select {
	case <-a.done:
	case <-time.After(5 * time.Second):
		t.Fatal("group failure did not stop consumer")
	}
	if a.e.failure == nil || a.observed.Load() != 0 || a.p.observed != 0 || a.durable.Load() != 2 {
		t.Fatal("failed commit published evidence or consumed nondurable rows")
	}
}
