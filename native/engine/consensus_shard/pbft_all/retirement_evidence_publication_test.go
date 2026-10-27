package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRetirementEvidenceWarmCacheAndReplacement(t *testing.T) {
	for _, change := range []string{"removed", "file_replaced", "parent_replaced", "corrupt"} {
		t.Run(change, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "evidence")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path, raw := filepath.Join(dir, "retired.json"), []byte("immutable retirement\n")
			binding := retirementEvidenceBinding{path: path, record: sha256.Sum256(raw), height: 7, shard: 6}
			var publication retirementEvidencePublication
			builds, barriers := 0, 0
			build := func() ([]byte, error) { builds++; return raw, nil }
			persist := func(path string, raw []byte) error { barriers++; return core.PublishSourcePrepareFile(path, raw) }
			for i := 0; i < 3; i++ {
				if err := publication.publish(binding, build, persist); err != nil {
					t.Fatal(err)
				}
			}
			if builds != 1 || barriers != 1 || !publication.durable {
				t.Fatal("warm proposals repeated construction or durability barriers", builds, barriers)
			}
			switch change {
			case "removed":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "file_replaced":
				if err := os.Rename(path, filepath.Join(dir, "old.tmp")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "parent_replaced":
				if err := os.Rename(dir, filepath.Join(root, "old-evidence")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := os.WriteFile(path, []byte("different evidence\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := publication.publish(binding, build, persist)
			if change == "corrupt" {
				if err == nil || publication.durable {
					t.Fatal("changed immutable evidence remained a successful cache entry")
				}
			} else if err != nil || !publication.durable {
				t.Fatal("replacement did not restore durable evidence", err)
			}
			if builds != 1 || barriers != 2 {
				t.Fatal("replacement skipped resync or repeated historical construction", builds, barriers)
			}
		})
	}
}

func TestRetirementEvidenceUncertainRetryAndColdLifecycle(t *testing.T) {
	for _, visible := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_publication", true: "visible_uncertain"}[visible], func(t *testing.T) {
			path, raw := filepath.Join(t.TempDir(), "retired.json"), []byte("exact attempted evidence\n")
			binding := retirementEvidenceBinding{path: path, record: sha256.Sum256(raw), height: 7, shard: 6}
			var publication retirementEvidencePublication
			builds, barriers := 0, 0
			build := func() ([]byte, error) { builds++; return raw, nil }
			persist := func(path string, attempted []byte) error {
				barriers++
				if !bytes.Equal(attempted, raw) {
					t.Fatal("retry replaced the attempted evidence")
				}
				if barriers == 1 {
					if visible {
						if err := core.PublishSourcePrepareFile(path, attempted); err != nil {
							return err
						}
					}
					return errors.New("uncertain publication result")
				}
				return core.PublishSourcePrepareFile(path, attempted)
			}
			if err := publication.publish(binding, build, persist); err == nil || publication.durable {
				t.Fatal("failed publication was cached as durable")
			}
			noRebuild := func() ([]byte, error) {
				t.Fatal("uncertain retry reconstructed instead of retaining exact bytes")
				return nil, nil
			}
			if err := publication.publish(binding, noRebuild, persist); err != nil || !publication.durable {
				t.Fatal("exact retry did not finish its durability barrier", err)
			}
			if builds != 1 || barriers != 2 {
				t.Fatal("visible final file bypassed retry durability", builds, barriers)
			}
			var restarted retirementEvidencePublication
			if err := restarted.publish(binding, build, persist); err != nil {
				t.Fatal(err)
			}
			binding.view++
			if err := restarted.publish(binding, build, persist); err != nil {
				t.Fatal(err)
			}
			if builds != 3 || barriers != 4 {
				t.Fatal("restart or changed leadership inherited another binding's success", builds, barriers)
			}
		})
	}
}

func TestRetirementEvidenceBindingIsolation(t *testing.T) {
	path, raw := filepath.Join(t.TempDir(), "retired-a.json"), []byte("group a\n")
	first := retirementEvidenceBinding{path: path, record: sha256.Sum256(raw), height: 7, shard: 6}
	for _, field := range []string{"record", "height", "shard", "node", "view", "path"} {
		t.Run(field, func(t *testing.T) {
			var publication retirementEvidencePublication
			builds, barriers := 0, 0
			build := func() ([]byte, error) { builds++; return raw, nil }
			persist := func(path string, raw []byte) error { barriers++; return core.PublishSourcePrepareFile(path, raw) }
			if err := publication.publish(first, build, persist); err != nil {
				t.Fatal(err)
			}
			next := first
			switch field {
			case "record":
				next.record[0] ^= 1
			case "height":
				next.height++
			case "shard":
				next.shard++
			case "node":
				next.node++
			case "view":
				next.view++
			case "path":
				next.path = filepath.Join(filepath.Dir(path), "retired-b.json")
			}
			if err := publication.publish(next, build, persist); err != nil {
				t.Fatal(err)
			}
			if builds != 2 || barriers != 2 || publication.binding != next {
				t.Fatal("different group/path/commit/leader binding hit the old cache", builds, barriers)
			}
		})
	}
}

func TestRetirementEvidenceNodeWarmPathAvoidsAncestorReads(t *testing.T) {
	t.Setenv("SAA_SERIAL_GROUP_CONTROL", "true")
	base := filepath.Join(t.TempDir(), "retired.json")
	t.Setenv("SAA_GROUP_SOURCE_RETIRED_PATH", base)
	activation, err := json.Marshal(map[string]interface{}{"record": chain.GroupTargetActivation{Members: []chain.TargetActivation{{ProposalID: "retired-a"}}}})
	if err != nil {
		t.Fatal(err)
	}
	record := &chain.GroupSourceRetirement{Command: chain.GroupRetirementCommand{ActivationReceipt: activation}, Block: 7, Sequence: 9, Digest: "commit"}
	block := &core.Block{Header: &core.BlockHeader{Number: 7, StateRoot: bytes.Repeat([]byte{2}, 32)}, Hash: bytes.Repeat([]byte{1}, 32)}
	n := &PbftConsensusNode{ShardID: 6, NodeID: 1, CurChain: &chain.BlockChain{CurrentBlock: block}}
	path := core.GroupTransportPath(base, "retired-a")
	if err := n.publishGroupEvidence("SAA_GROUP_SOURCE_RETIRED_PATH", record, record.Block); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("nonleader publication became durable evidence", err)
	}
	n.view.Store(1)
	if err := n.publishGroupEvidence("SAA_GROUP_SOURCE_RETIRED_PATH", record, record.Block); err != nil {
		t.Fatal(err)
	}
	// No historical storage is attached to this publisher-only fixture. A warm
	// ordinary proposal beyond retirement must not try to walk the old blocks.
	n.CurChain.CurrentBlock = &core.Block{Header: &core.BlockHeader{Number: 8}}
	if err := n.publishGroupEvidence("SAA_GROUP_SOURCE_RETIRED_PATH", record, record.Block); err != nil {
		t.Fatal(err)
	}
	n.CurChain.CurrentBlock = block
	activation, err = json.Marshal(map[string]interface{}{"record": chain.GroupTargetActivation{Members: []chain.TargetActivation{{ProposalID: "retired-b"}}}})
	if err != nil {
		t.Fatal(err)
	}
	other := *record
	other.Command.ActivationReceipt = activation
	if err := n.publishGroupEvidence("SAA_GROUP_SOURCE_RETIRED_PATH", &other, other.Block); err != nil {
		t.Fatal(err)
	}
	otherPath := core.GroupTransportPath(base, "retired-b")
	firstRaw, firstErr := os.ReadFile(path)
	otherRaw, otherErr := os.ReadFile(otherPath)
	if firstErr != nil || otherErr != nil || bytes.Equal(firstRaw, otherRaw) {
		t.Fatal("another group reused the original evidence path or payload", firstErr, otherErr)
	}
	otherBase := filepath.Join(filepath.Dir(base), "relocated-retired.json")
	t.Setenv("SAA_GROUP_SOURCE_RETIRED_PATH", otherBase)
	if err := n.publishGroupEvidence("SAA_GROUP_SOURCE_RETIRED_PATH", &other, other.Block); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(core.GroupTransportPath(otherBase, "retired-b")); err != nil {
		t.Fatal("changed transport path inherited the old success", err)
	}
	restarted := &PbftConsensusNode{ShardID: 6, NodeID: 1, CurChain: n.CurChain}
	restarted.view.Store(1)
	if err := restarted.publishGroupEvidence("SAA_GROUP_SOURCE_RETIRED_PATH", &other, other.Block); err != nil {
		t.Fatal(err)
	}
	first, _ := n.sourcePrepareSeen.Load("retirement-evidence-publication-v1")
	second, _ := restarted.sourcePrepareSeen.Load("retirement-evidence-publication-v1")
	if first == nil || second == nil || first == second || !second.(*retirementEvidencePublication).durable {
		t.Fatal("new node inherited a durable-success cache instead of publishing")
	}
}
