package ownership

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRuntimeJournalSmallPartialTailPreservesPrefix(t *testing.T) {
	for _, size := range []int{1, 15, 16, 33, 1024, (1 << 20) - 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			_, view, journal := runtimeFixture(t)
			if err := os.WriteFile(journal, bytes.Repeat([]byte("x"), size), 0600); err != nil {
				t.Fatal(err)
			}
			metrics, err := view.RefreshMeasured()
			if err != nil || view.Version() != 0 || view.journalOffset != 0 || metrics.Certificates != 0 {
				t.Fatalf("incomplete tail changed prefix or failed: %v, %+v", err, metrics)
			}
			if metrics.TotalSeconds < metrics.ReadDecodeSeconds || metrics.TotalSeconds <= 0 {
				t.Fatalf("total refresh timing omitted measured work: %+v", metrics)
			}
		})
	}
}

func runtimeFixture(t *testing.T) (*committeeFixture, *View, string) {
	t.Helper()
	f := newCommitteeFixture(t)
	dir := t.TempDir()
	configPath, journal := filepath.Join(dir, "config.json"), filepath.Join(dir, "certificates.jsonl")
	raw, _ := json.Marshal(f.config)
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journal, nil, 0600); err != nil {
		t.Fatal(err)
	}
	view, err := LoadRuntimeView(configPath, digestBytes(raw), journal, f.genesis, f.config.Context.GenesisDigest, 2)
	if err != nil {
		t.Fatal(err)
	}
	return f, view, journal
}

func runtimeCertificate(t *testing.T, f *committeeFixture, base, start, end uint64, account string) Certificate {
	t.Helper()
	batch := Batch{Schema: Schema, Context: f.config.Context, CommitteeDigest: f.verifier.CommitteeDigest(),
		BaseVersion: base, ProposalID: account, Bindings: []Binding{{account, 1}},
		EvidenceVersion: base + 1, EvidenceDigest: digestBytes([]byte(account)), EvidenceCutoff: start, AdmissionStart: start, AdmissionEnd: end}
	return f.sign(t, batch)
}

func appendRuntime(t *testing.T, path string, cert Certificate, complete bool) []byte {
	t.Helper()
	raw, _ := json.Marshal(cert)
	if complete {
		raw = append(raw, '\n')
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(raw); err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRuntimeJournalIncrementalPartialReplayAndRecovery(t *testing.T) {
	f, view, journal := runtimeFixture(t)
	a, b := testAccount(1), testAccount(2)
	first := runtimeCertificate(t, f, 0, 0, 2, a)
	appendRuntime(t, journal, first, false)
	if err := view.Check(a, 1); err == nil || view.Version() != 0 {
		t.Fatal("partial record authorized an owner")
	}
	file, _ := os.OpenFile(journal, os.O_APPEND|os.O_WRONLY, 0600)
	file.Write([]byte("\n"))
	file.Close()
	if err := view.Check(a, 1); err != nil || view.Version() != 1 {
		t.Fatalf("complete record not adopted: %v", err)
	}
	appendRuntime(t, journal, first, true)
	appendRuntime(t, journal, runtimeCertificate(t, f, 1, 2, 4, b), true)
	if err := view.Check(b, 1); err != nil || view.Version() != 2 {
		t.Fatalf("incremental/replay application failed: %v", err)
	}
	recovered, err := LoadRuntimeView(filepath.Join(filepath.Dir(journal), "config.json"), view.configHash, journal, f.genesis, f.config.Context.GenesisDigest, 2)
	if err != nil || recovered.Version() != 2 || recovered.journalOffset != view.journalOffset {
		t.Fatalf("restart prefix mismatch: %v", err)
	}
	if err := view.CheckPlanDependency(b, 2, 2, digestBytes([]byte(b)), 2); err != nil {
		t.Fatal(err)
	}
	if err := view.CheckPlanDependency(b, 2, 1, digestBytes([]byte(a)), 0); err == nil {
		t.Fatal("mismatched prior was accepted")
	}
	if err := view.CheckAdmission(b, 1); err == nil {
		t.Fatal("future owner was accepted before its batch")
	}
}

func TestRuntimeJournalRejectsInvalidAndUnboundCertificates(t *testing.T) {
	for _, mode := range []string{"bad-signature", "wrong-context", "no-evidence", "future-evidence", "wrong-base"} {
		t.Run(mode, func(t *testing.T) {
			f, view, journal := runtimeFixture(t)
			cert := runtimeCertificate(t, f, 0, 0, 2, testAccount(1))
			switch mode {
			case "bad-signature":
				cert.Signatures[0].Value[0] ^= 1
			case "wrong-context":
				cert.Batch.Context.ChainID = "another-chain"
			case "no-evidence":
				cert = f.certificate(t, 0, "old", Binding{testAccount(1), 1})
			case "future-evidence":
				cert.Batch.EvidenceCutoff = 1
				cert = f.sign(t, cert.Batch)
			case "wrong-base":
				cert.Batch.BaseVersion = 1
				cert = f.sign(t, cert.Batch)
			}
			appendRuntime(t, journal, cert, true)
			if err := view.Refresh(); err == nil || view.Version() != 0 || view.journalOffset != 0 {
				t.Fatal("invalid journal changed authorization prefix")
			}
			if err := view.Check(testAccount(1000), 0); err != nil {
				t.Fatal("invalid unrelated certificate blocked an existing owner")
			}
		})
	}
}

func TestRuntimeJournalRejectsTruncateAndReplacement(t *testing.T) {
	for _, replace := range []bool{false, true} {
		f, view, journal := runtimeFixture(t)
		appendRuntime(t, journal, runtimeCertificate(t, f, 0, 0, 2, testAccount(1)), true)
		if err := view.Refresh(); err != nil {
			t.Fatal(err)
		}
		if replace {
			replacement := journal + ".new"
			os.WriteFile(replacement, nil, 0600)
			os.Rename(replacement, journal)
		} else {
			os.Truncate(journal, 0)
		}
		if err := view.Refresh(); err == nil {
			t.Fatal("journal identity/prefix loss not rejected")
		}
	}
}

func TestRuntimeJournalConcurrentCachedChecksAndRefresh(t *testing.T) {
	f, view, journal := runtimeFixture(t)
	appendRuntime(t, journal, runtimeCertificate(t, f, 0, 0, 2, testAccount(1)), true)
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 50; i++ {
				if err := view.Check(testAccount(1), 1); err != nil {
					t.Error(err)
				}
				view.Status()
			}
		}()
	}
	group.Wait()
	if view.Version() != 1 {
		t.Fatal("concurrent refresh applied a certificate twice")
	}
}
