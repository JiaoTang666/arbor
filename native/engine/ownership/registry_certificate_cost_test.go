package ownership

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

type certificateStoreCost struct {
	Mode                  string  `json:"mode"`
	VoteCommitSeconds     float64 `json:"vote_commit_seconds"`
	DatabaseFileBytes     int64   `json:"database_file_bytes"`
	AllocatedPages        int64   `json:"bolt_allocated_pages"`
	AllocatedPageBytes    int64   `json:"bolt_allocated_page_bytes"`
	WriteCalls            int64   `json:"bolt_write_calls"`
	SpillNodes            int64   `json:"bolt_spill_nodes"`
	RegistryBranchPages   int     `json:"registry_branch_pages"`
	RegistryLeafPages     int     `json:"registry_leaf_pages"`
	RegistryOverflowPages int     `json:"registry_overflow_pages"`
	OwnerIndexKeys        int     `json:"owner_index_keys"`
	CertificateKeys       int     `json:"certificate_keys"`
	ProposalKeys          int     `json:"proposal_keys"`
	VoteKeys              int     `json:"vote_keys"`
	CertificateBytes      int     `json:"logical_certificate_bytes"`
	VoteBytes             int     `json:"logical_vote_bytes"`
}

// Allocation bytes and write-call counts are Bolt counters, NOT physical disk
// bytes or fsync counts. Timing is descriptive: this single-signer fixture does
// not establish sustained four-member/native performance or a speed threshold.
func TestRegistryCertificateCostAndEquivalentRecovery(t *testing.T) {
	const batches, bindingsPerBatch = 32, 512
	f := newCommitteeFixture(t)
	certificates := make([]Certificate, batches)
	for batch := range certificates {
		bindings := make([]Binding, bindingsPerBatch)
		for item := range bindings {
			id := batch*bindingsPerBatch + item
			// Deterministically scattered addresses avoid an artificially cheap,
			// sequential address-index workload. Both modes receive identical data.
			sum := sha256.Sum256([]byte(fmt.Sprintf("certificate-cost-account-%d", id)))
			bindings[item] = Binding{hex.EncodeToString(sum[:20]), uint64(id % 2)}
		}
		sort.Slice(bindings, func(i, j int) bool { return bindings[i].Account < bindings[j].Account })
		start := uint64(batch * bindingsPerBatch)
		certificates[batch] = f.sign(t, Batch{Schema: Schema, Context: f.config.Context,
			CommitteeDigest: f.verifier.CommitteeDigest(), BaseVersion: uint64(batch),
			ProposalID: fmt.Sprintf("cost-%04d", batch), Bindings: bindings,
			EvidenceVersion: uint64(batch + 1), EvidenceDigest: digestBytes([]byte(fmt.Sprintf("causal-cutoff-%d", batch))),
			EvidenceCutoff: start, AdmissionStart: start, AdmissionEnd: start + bindingsPerBatch},
			"member-0", "member-1", "member-2", "member-3")
	}
	expected, err := BuildView(f.verifier, f.genesis, certificates)
	if err != nil {
		t.Fatal(err)
	}
	var results []certificateStoreCost
	for _, mode := range []string{registryDeltaMode, RegistryCertificateMode} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "member.db")
			signer, err := OpenSignerWithMode(path, f.verifier, f.genesis, "member-0", f.private["member-0"], mode)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if signer != nil {
					_ = signer.Close()
				}
			}()
			before := signer.registry.db.Stats()
			started := time.Now()
			for _, cert := range certificates {
				signature, err := signer.Vote(cert.Batch)
				if err != nil || !reflect.DeepEqual(signature, cert.Signatures[0]) {
					t.Fatalf("slot %d vote differs: %v", cert.Batch.BaseVersion, err)
				}
				if changed, err := signer.Commit(cert); err != nil || !changed {
					t.Fatalf("slot %d commit: changed=%v err=%v", cert.Batch.BaseVersion, changed, err)
				}
			}
			elapsed := time.Since(started).Seconds()
			after := signer.registry.db.Stats()
			difference := after.Sub(&before)
			cost := certificateStoreCost{Mode: mode, VoteCommitSeconds: elapsed,
				AllocatedPages: difference.TxStats.PageCount, AllocatedPageBytes: difference.TxStats.PageAlloc,
				WriteCalls: difference.TxStats.Write, SpillNodes: difference.TxStats.Spill}
			inspectCertificateCostStore(t, signer.registry.db, certificates, &cost)
			file, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			cost.DatabaseFileBytes = file.Size()
			wantOwners := batches * bindingsPerBatch
			if mode == RegistryCertificateMode {
				wantOwners = 0
			}
			if cost.OwnerIndexKeys != wantOwners || cost.CertificateKeys != batches || cost.ProposalKeys != batches || cost.VoteKeys != batches {
				t.Fatalf("missing evidence or unexpected address index: %+v", cost)
			}
			requireCertificateCostView(t, signer, f.genesis, expected)
			if err := signer.Close(); err != nil {
				t.Fatal(err)
			}
			// Reopen must reconstruct the complete semantic view and preserve every
			// historical vote; a live cache alone cannot pass this comparison.
			signer, err = OpenSignerWithMode(path, f.verifier, f.genesis, "member-0", f.private["member-0"], mode)
			if err != nil {
				t.Fatal(err)
			}
			requireCertificateCostView(t, signer, f.genesis, expected)
			for _, cert := range certificates {
				if signature, err := signer.Vote(cert.Batch); err != nil || !reflect.DeepEqual(signature, cert.Signatures[0]) {
					t.Fatalf("slot %d recovered vote differs: %v", cert.Batch.BaseVersion, err)
				}
				if changed, err := signer.Commit(cert); err != nil || changed {
					t.Fatalf("slot %d recovered replay changed state: %v %v", cert.Batch.BaseVersion, changed, err)
				}
			}
			results = append(results, cost)
		})
	}
	if len(results) != 2 {
		t.Fatal("one storage mode did not complete")
	}
	if results[0].CertificateBytes != results[1].CertificateBytes || results[0].VoteBytes != results[1].VoteBytes {
		t.Fatal("optimization removed or changed signed evidence")
	}
	report := struct {
		Scope            string                 `json:"scope"`
		Batches          int                    `json:"batches"`
		BindingsPerBatch int                    `json:"bindings_per_batch"`
		Semantics        string                 `json:"semantics"`
		Counters         string                 `json:"counter_limit"`
		Results          []certificateStoreCost `json:"results"`
	}{"single_signer_fixture_not_native_performance", batches, bindingsPerBatch,
		"identical_owners_proposals_context_evidence_versions_and_recovered_votes",
		"bolt_allocation_and_write_call_counters_not_physical_io_or_fsync", results}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("certificate_store_cost_json=%s", raw)
}

func inspectCertificateCostStore(t *testing.T, db *bolt.DB, certificates []Certificate, cost *certificateStoreCost) {
	t.Helper()
	if err := db.View(func(tx *bolt.Tx) error {
		registry, votes := tx.Bucket(registryBucket), tx.Bucket(signerVoteBucket)
		stats := registry.Stats()
		cost.RegistryBranchPages, cost.RegistryLeafPages = stats.BranchPageN, stats.LeafPageN
		cost.RegistryOverflowPages = stats.BranchOverflowN + stats.LeafOverflowN
		if err := registry.ForEach(func(key, value []byte) error {
			if len(key) > 0 {
				switch key[0] {
				case 'a':
					cost.OwnerIndexKeys++
				case 'p':
					cost.ProposalKeys++
				case 'c':
					if len(key) == 9 {
						cost.CertificateKeys++
						cost.CertificateBytes += len(value)
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for _, cert := range certificates {
			raw, err := json.Marshal(cert)
			if err != nil || !bytes.Equal(registry.Get(versionKey(cert.Batch.BaseVersion+1)), raw) {
				return fmt.Errorf("certificate payload changed at slot %d", cert.Batch.BaseVersion)
			}
			stored := votes.Get(voteKey(cert.Batch.BaseVersion))
			vote, err := decodeVote(stored)
			if err != nil || !reflect.DeepEqual(vote.Batch, cert.Batch) || !reflect.DeepEqual(vote.Signature, cert.Signatures[0]) {
				return fmt.Errorf("vote payload changed at slot %d", cert.Batch.BaseVersion)
			}
			cost.VoteKeys++
			cost.VoteBytes += len(stored)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func requireCertificateCostView(t *testing.T, signer *Signer, genesis map[string]uint64, expected *View) {
	t.Helper()
	got, err := signer.Snapshot(genesis)
	if err != nil {
		t.Fatal(err)
	}
	// Compare all semantic View fields; locks, file handles and verifier pointers
	// are process-local machinery, not ownership or signed-evidence results.
	if got.context != expected.context || got.version != expected.version ||
		got.committeeDigest != expected.committeeDigest || got.genesisOwners != expected.genesisOwners ||
		got.configHash != expected.configHash || got.bundleHash != expected.bundleHash ||
		!reflect.DeepEqual(got.ownerSnapshot(), expected.ownerSnapshot()) || !reflect.DeepEqual(got.proposals, expected.proposals) ||
		!reflect.DeepEqual(got.evidence, expected.evidence) {
		t.Fatal("recovered ownership view differs from signed-certificate reference")
	}
}
