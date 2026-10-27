package committee

// Explicit opt-in read-only audit of a saved native stream. No node, simulator,
// policy, synthetic clock, or new action is created by this test.
import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

type forestReplayTopologyV1 struct {
	Schema string `json:"schema"`
	Shards int `json:"shards"`
	NodesPerShard int `json:"nodes_per_shard"`
	InjectSpeed int `json:"inject_speed"`
	ExpectedTransactions uint64 `json:"expected_transactions"`
}

func parseForestReplayTopologyV1(shardsValue string, data []byte) (forestReplayTopologyV1, error) {
	var manifest forestReplayTopologyV1
	shards, e := strconv.Atoi(shardsValue)
	if e != nil || shards < 1 {
		return manifest, fmt.Errorf("FOREST_V2_SHARDS must be an explicit positive count")
	}
	if e = json.Unmarshal(data, &manifest); e != nil {
		return manifest, e
	}
	if manifest.Schema != "full_shard_scale_v1" || manifest.Shards != shards || manifest.NodesPerShard != 4 || manifest.InjectSpeed != 125*shards || manifest.ExpectedTransactions == 0 {
		return manifest, fmt.Errorf("forest replay topology manifest mismatch")
	}
	return manifest, nil
}

func checkForestSnapshotWidthV1(snapshot ForestFactsSnapshotV2, shards int) error {
	for _, size := range []int{len(snapshot.NewOrigins20s), len(snapshot.ActiveOrigins20s), len(snapshot.AssignedNNHalfStages5s), len(snapshot.Concentration20s), len(snapshot.NoActivity20s), len(snapshot.AssignedStages), len(snapshot.ConfirmedStages)} {
		if size != shards {
			return fmt.Errorf("forest saved snapshot topology mismatch")
		}
	}
	for _, category := range []string{"historical_entrance", "runtime_newnew", "unknown"} {
		if len(snapshot.AssignedCategoryHalfStages[category]) != shards || len(snapshot.ConfirmedCategoryHalfStages[category]) != shards {
			return fmt.Errorf("forest saved category topology mismatch")
		}
	}
	return nil
}

func TestForestNativeSavedTrace(t *testing.T) {
	path := os.Getenv("FOREST_V2_TRACE")
	if path == "" {
		t.Skip("explicit existing native trace required")
	}
	began := time.Now()
	manifestBytes, e := os.ReadFile(os.Getenv("FOREST_V2_TOPOLOGY_MANIFEST"))
	if e != nil {
		t.Fatal("explicit topology manifest required", e)
	}
	topology, e := parseForestReplayTopologyV1(os.Getenv("FOREST_V2_SHARDS"), manifestBytes)
	if e != nil {
		t.Fatal(e)
	}
	manifestHash := sha256.Sum256(manifestBytes)
	seed, meta, e := loadForestSeedV2(os.Getenv("FOREST_V2_SEED"))
	if e != nil {
		t.Fatal(e)
	}
	source, e := os.ReadFile(os.Getenv("FOREST_V2_SOURCE_TIME"))
	if e != nil || len(source)%8 != 0 {
		t.Fatal("invalid source time fixture", e)
	}
	if uint64(len(source)/8) != topology.ExpectedTransactions {
		t.Fatal("source time scope differs from topology manifest")
	}
	file, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer file.Close()
	h := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(file, h))
	scanner.Buffer(make([]byte, 1<<20), 16<<20)
	var f *ForestFactsV2
	var clockID string
	var lastMono int64
	var confirmations uint64
	var recordedFinal *ForestWorkAuditV2
	for scanner.Scan() {
		var row struct {
			Event   string          `json:"event"`
			Seq     uint64          `json:"sequence"`
			Ingress uint64          `json:"ingress_sequence"`
			Hash    string          `json:"tx_hash"`
			A       string          `json:"sender"`
			B       string          `json:"recipient"`
			KA      bool            `json:"known_sender"`
			KB      bool            `json:"known_recipient"`
			SA      uint64          `json:"source_shard"`
			SB      uint64          `json:"target_shard"`
			At      int64           `json:"dispatch_mono_ns"`
			Clock   string          `json:"clock_id"`
			Details json.RawMessage `json:"details"`
			Fast *ForestFactsSnapshotV2 `json:"fast"`
			Audit *ForestWorkAuditV2 `json:"audit"`
		}
		if e = json.Unmarshal(scanner.Bytes(), &row); e != nil {
			t.Fatal(e)
		}
		if row.Fast != nil {
			if e = checkForestSnapshotWidthV1(*row.Fast, topology.Shards); e != nil {
				t.Fatal(e)
			}
		}
		switch row.Event {
		case "forest_facts_v2_final":
			if row.Audit == nil || recordedFinal != nil || len(row.Audit.AssignedStages) != topology.Shards || len(row.Audit.ConfirmedStages) != topology.Shards {
				t.Fatal("missing, repeated or wrong-width saved final forest audit")
			}
			recordedFinal = row.Audit
		case "forest_binding":
			if row.Clock == "" {
				t.Fatal("missing old monotonic evidence")
			}
			if f == nil {
				clockID = row.Clock
				f, e = NewForestFactsV2(seed, meta.Namespace, meta.Cutoff, clockID, topology.Shards)
				if e != nil {
					t.Fatal(e)
				}
			}
			if row.Clock != clockID || row.Seq >= uint64(len(source)/8) {
				t.Fatal("clock or input scope mismatch")
			}
			at := int64(binary.LittleEndian.Uint64(source[row.Seq*8 : row.Seq*8+8]))
			if e = f.Bind(row.Seq, row.A, row.B, row.KA, row.KB, row.SA, row.SB, at, row.At); e != nil {
				t.Fatalf("bind %d: %v", row.Seq, e)
			}
			if row.At > lastMono {
				lastMono = row.At
			}
		case "route_bound", "leg_registered":
			if f == nil {
				t.Fatal("work before binding")
			}
			var d struct {
				Source    uint64 `json:"source"`
				Target    uint64 `json:"target"`
				Leg       string `json:"leg"`
				Shard     uint64 `json:"shard"`
				Assigned  *int64 `json:"assigned_mono_ns"`
				Confirmed *int64 `json:"confirmed_mono_ns"`
				Clock     string `json:"clock_id"`
			}
			if e = json.Unmarshal(row.Details, &d); e != nil || d.Clock != clockID {
				t.Fatal("missing valid detail clock", e)
			}
			if row.Event == "route_bound" {
				if d.Assigned == nil {
					t.Fatal("no assigned monotonic evidence")
				}
				// Bind owns immutable endpoints; this lookup is test-only, not a second router.
				b, ok := f.bound[row.Ingress]
				if !ok {
					t.Fatal("route has no preceding binding")
				}
				if e = f.Assign(row.Ingress, row.Hash, b.a, b.b, d.Source, d.Target, *d.Assigned); e != nil {
					t.Fatalf("assign%d: %v", row.Ingress, e)
				}
				if *d.Assigned > lastMono {
					lastMono = *d.Assigned
				}
			} else {
				if d.Confirmed == nil {
					t.Fatal("no confirmation monotonic evidence")
				}
				ok, err := f.ConfirmWithClock(row.Ingress, row.Hash, d.Leg, d.Shard, *d.Confirmed, d.Clock)
				if err != nil || !ok {
					t.Fatalf("confirm %d: %v", row.Ingress, err)
				}
				confirmations++
				if *d.Confirmed > lastMono {
					lastMono = *d.Confirmed
				}
			}
		}
	}
	if e = scanner.Err(); e != nil {
		t.Fatal(e)
	}
	if f == nil {
		t.Fatal("empty saved trace")
	}
	audit, e := f.Audit()
	if e != nil {
		t.Fatal(e)
	}
	want := uint64(len(source) / 8)
	if audit.Bound != want || audit.Assigned != want || audit.Completed != want || audit.OutstandingOccurrences != 0 {
		t.Fatal("incomplete reconstruction", audit)
	}
	if recordedFinal == nil || !reflect.DeepEqual(audit.AssignedStages, recordedFinal.AssignedStages) || !reflect.DeepEqual(audit.ConfirmedStages, recordedFinal.ConfirmedStages) || recordedFinal.Bound != want || recordedFinal.Assigned != want || recordedFinal.Completed != want || recordedFinal.OutstandingOccurrences != 0 {
		t.Fatal("saved final forest evidence differs from replay")
	}
	stages := int64(0)
	for i, v := range audit.AssignedStages {
		stages += v
		if audit.ConfirmedStages[i] != v {
			t.Fatal("per-shard stage mismatch")
		}
	}
	expected, e := strconv.ParseInt(os.Getenv("FOREST_V2_EXPECTED_STAGES"), 10, 64)
	if e != nil || stages != expected {
		t.Fatal("recorded stage total differs", stages, expected, e)
	}
	if uint64(stages) != confirmations {
		t.Fatal("receipt stage conservation failed")
	}
	snapshot, e := f.Snapshot(lastMono, clockID)
	if e != nil {
		t.Fatal(e)
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	result := map[string]interface{}{"status": "PASS", "audit": audit, "snapshot": snapshot, "shards": topology.Shards, "topology_manifest_sha256": hex.EncodeToString(manifestHash[:]), "trace_sha256": hex.EncodeToString(h.Sum(nil)), "seed_sha256": meta.SHA256, "wall_seconds": time.Since(began).Seconds(), "heap_bytes": mem.HeapAlloc, "total_allocated_bytes": mem.TotalAlloc, "real_stages": stages, "new_actions": 0, "native_nodes_started": false, "evidence": "saved native Full trace replayed, not a new performance run"}
	if out := os.Getenv("FOREST_V2_REPLAY_RESULT"); out != "" {
		raw, _ := json.MarshalIndent(result, "", "  ")
		if e = os.WriteFile(out, raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("replayed %d occurrences, %d stages, %.3fs", want, stages, time.Since(began).Seconds())
}
