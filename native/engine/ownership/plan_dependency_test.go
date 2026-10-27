package ownership

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"unsafe"
)

func planDependencyCertificate(t *testing.T, f *committeeFixture, base, start, end uint64, proposal string, bindings ...Binding) Certificate {
	t.Helper()
	bindings = append([]Binding(nil), bindings...)
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Account < bindings[j].Account })
	return f.sign(t, Batch{Schema: Schema, Context: f.config.Context,
		CommitteeDigest: f.verifier.CommitteeDigest(), BaseVersion: base, ProposalID: proposal, Bindings: bindings,
		EvidenceVersion: base + 1, EvidenceDigest: digestBytes([]byte(proposal)), EvidenceCutoff: start,
		AdmissionStart: start, AdmissionEnd: end})
}

func TestPlanDependencySharedPerCertificate(t *testing.T) {
	f := newCommitteeFixture(t)
	a, b, c, d := testAccount(1), testAccount(2), testAccount(3), testAccount(4)
	first := planDependencyCertificate(t, f, 0, 10, 20, "shared-first", Binding{a, 1}, Binding{b, 1})
	second := planDependencyCertificate(t, f, 1, 20, 30, "shared-second", Binding{c, 0}, Binding{d, 0})
	view, err := BuildView(f.verifier, f.genesis, []Certificate{first, second})
	if err != nil {
		t.Fatal(err)
	}
	firstDependency, secondDependency := view.evidence[a], view.evidence[c]
	if firstDependency == nil || firstDependency != view.evidence[b] {
		t.Fatal("first certificate did not share one dependency across its bindings")
	}
	if secondDependency == nil || secondDependency != view.evidence[d] || secondDependency == firstDependency {
		t.Fatal("second certificate did not share a separate dependency")
	}
	if _, exists := view.evidence[testAccount(1000)]; exists {
		t.Fatal("genesis owner acquired a runtime dependency")
	}
	// This retained object must contain only the five causal fields, with no
	// Batch, bindings, witness, or other complete-certificate payload reference.
	dependencyType := reflect.TypeOf(*firstDependency)
	fields := map[string]reflect.Kind{"EvidenceVersion": reflect.Uint64, "EvidenceDigest": reflect.String,
		"EvidenceCutoff": reflect.Uint64, "AdmissionStart": reflect.Uint64, "AdmissionEnd": reflect.Uint64}
	if dependencyType.NumField() != len(fields) {
		t.Fatal("dependency retains fields beyond its causal metadata")
	}
	for i := 0; i < dependencyType.NumField(); i++ {
		field := dependencyType.Field(i)
		kind, exists := fields[field.Name]
		if !exists || field.Type.Kind() != kind {
			t.Fatalf("unexpected retained dependency field %s of type %s", field.Name, field.Type)
		}
	}
	batchSize, dependencySize := unsafe.Sizeof(Batch{}), unsafe.Sizeof(planDependency{})
	t.Logf("retained value layout: Batch=%d bytes, planDependency=%d bytes", batchSize, dependencySize)
	if dependencySize >= batchSize {
		t.Fatal("dependency did not remove the complete Batch layout")
	}
	if unsafe.Sizeof(uintptr(0)) == 8 && (batchSize != 280 || dependencySize != 48) {
		t.Fatalf("unexpected 64-bit layout: Batch=%d, planDependency=%d", batchSize, dependencySize)
	}
	firstBefore, secondBefore := *firstDependency, *secondDependency
	first.Batch.EvidenceVersion++
	first.Batch.EvidenceDigest = digestBytes([]byte("caller-mutated"))
	first.Batch.EvidenceCutoff++
	first.Batch.AdmissionStart++
	first.Batch.AdmissionEnd++
	first.Batch.Bindings[0].Account = testAccount(99)
	if *firstDependency != firstBefore || *secondDependency != secondBefore {
		t.Fatal("caller mutation changed retained certificate dependencies")
	}
	if owner, exists := view.Owner(a); !exists || owner != 1 {
		t.Fatal("caller mutation changed an authorized binding")
	}
}

func TestPlanDependencyChecksMatchLegacy(t *testing.T) {
	f, runtime, journal := runtimeFixture(t)
	a, b, c, d := testAccount(1), testAccount(2), testAccount(3), testAccount(4)
	first := planDependencyCertificate(t, f, 0, 10, 20, "legacy-first", Binding{a, 1}, Binding{b, 1})
	second := planDependencyCertificate(t, f, 1, 20, 30, "legacy-second", Binding{c, 0}, Binding{d, 0})
	certificates := []Certificate{first, second}
	built, err := BuildView(f.verifier, f.genesis, certificates)
	if err != nil {
		t.Fatal(err)
	}
	for _, certificate := range certificates {
		if len(certificate.Signatures) != 3 {
			t.Fatal("fixture did not provide a real q3 certificate")
		}
		appendRuntime(t, journal, certificate, true)
	}
	if err := runtime.Refresh(); err != nil {
		t.Fatal(err)
	}
	restarted, err := LoadRuntimeView(filepath.Join(filepath.Dir(journal), "config.json"), runtime.configHash,
		journal, f.genesis, f.config.Context.GenesisDigest, 2)
	if err != nil {
		t.Fatal(err)
	}
	if built.Version() != 2 || runtime.Version() != 2 || restarted.Version() != 2 || restarted.journalOffset != runtime.journalOffset {
		t.Fatal("BuildView, incremental application, and restart disagree on the authenticated prefix")
	}
	// Independent model of the old five-field predicates, populated directly
	// from signed input rather than the View's new retained representation.
	type legacyDependency struct {
		version    uint64
		digest     string
		cutoff     uint64
		first, end uint64
	}
	owners, dependencies := map[string]bool{}, map[string]legacyDependency{}
	for account := range f.genesis {
		owners[account] = true
	}
	for _, certificate := range certificates {
		batch := certificate.Batch
		for _, binding := range batch.Bindings {
			owners[binding.Account] = true
			dependencies[binding.Account] = legacyDependency{batch.EvidenceVersion, batch.EvidenceDigest,
				batch.EvidenceCutoff, batch.AdmissionStart, batch.AdmissionEnd}
		}
	}
	legacyAdmission := func(account string, sequence uint64) string {
		if dependency, exists := dependencies[account]; exists && sequence < dependency.first {
			return "future first-owner certificate used before its input batch"
		}
		return ""
	}
	legacyPlan := func(account string, sequence, version uint64, digest string, cutoff uint64) string {
		if !owners[account] {
			return "plan account is not authorized"
		}
		dependency, exists := dependencies[account]
		if exists && dependency.first <= sequence && sequence < dependency.end &&
			(dependency.version != version || dependency.digest != digest || dependency.cutoff != cutoff) {
			return "plan and first-owner certificate use different evidence"
		}
		return legacyAdmission(account, sequence)
	}
	assertError := func(t *testing.T, err error, expected, operation string) {
		t.Helper()
		actual := ""
		if err != nil {
			actual = err.Error()
		}
		if actual != expected {
			t.Fatalf("%s: got %q, legacy expects %q", operation, actual, expected)
		}
	}
	for name, view := range map[string]*View{"BuildView": built, "runtime": runtime, "restart": restarted} {
		t.Run(name, func(t *testing.T) {
			for _, account := range []string{a, b, c, d, testAccount(999), testAccount(1000)} {
				for _, sequence := range []uint64{0, 9, 10, 19, 20, 29, 30, ^uint64(0)} {
					operation := fmt.Sprintf("account=%s sequence=%d", account, sequence)
					assertError(t, view.CheckAdmission(account, sequence), legacyAdmission(account, sequence), operation+" admission")
					for _, certificate := range certificates {
						batch := certificate.Batch
						for _, evidence := range []struct {
							name    string
							version uint64
							digest  string
							cutoff  uint64
						}{
							{"match", batch.EvidenceVersion, batch.EvidenceDigest, batch.EvidenceCutoff},
							{"wrong-version", batch.EvidenceVersion + 10, batch.EvidenceDigest, batch.EvidenceCutoff},
							{"wrong-digest", batch.EvidenceVersion, digestBytes([]byte("wrong")), batch.EvidenceCutoff},
							{"wrong-cutoff", batch.EvidenceVersion, batch.EvidenceDigest, batch.EvidenceCutoff + 1},
						} {
							expected := legacyPlan(account, sequence, evidence.version, evidence.digest, evidence.cutoff)
							err := view.CheckPlanDependency(account, sequence, evidence.version, evidence.digest, evidence.cutoff)
							assertError(t, err, expected, operation+" "+batch.ProposalID+" "+evidence.name)
						}
					}
				}
			}
		})
	}
}

func TestPlanDependencyRejectedCertificateLeavesViewUnchanged(t *testing.T) {
	f := newCommitteeFixture(t)
	a, b, novel := testAccount(10), testAccount(20), testAccount(1)
	first := planDependencyCertificate(t, f, 0, 0, 2, "atomic-first", Binding{a, 1}, Binding{b, 1})
	view, err := BuildView(f.verifier, f.genesis, []Certificate{first})
	if err != nil {
		t.Fatal(err)
	}
	owners, proposals := map[string]uint64{}, map[string]string{}
	pointers, values := map[string]*planDependency{}, map[string]planDependency{}
	for account, owner := range view.owners {
		owners[account] = owner
	}
	for proposal, digest := range view.proposals {
		proposals[proposal] = digest
	}
	for account, dependency := range view.evidence {
		pointers[account], values[account] = dependency, *dependency
	}
	version, offset := view.version, view.journalOffset
	assertUnchanged := func(t *testing.T) {
		t.Helper()
		if view.version != version || view.journalOffset != offset ||
			!reflect.DeepEqual(view.owners, owners) || !reflect.DeepEqual(view.proposals, proposals) || len(view.evidence) != len(values) {
			t.Fatal("rejected or replayed certificate changed owner, proposal, evidence, or head state")
		}
		for account, value := range values {
			if dependency := view.evidence[account]; dependency != pointers[account] || *dependency != value {
				t.Fatalf("rejected or replayed certificate changed shared dependency for %s", account)
			}
		}
	}
	// The first sorted binding is novel, but a later binding conflicts with an
	// existing owner. The entire signed certificate must fail before publication.
	rejected := planDependencyCertificate(t, f, 1, 2, 4, "atomic-rejected", Binding{novel, 0}, Binding{a, 0})
	checked, digest, err := f.verifier.Verify(rejected)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := view.apply(checked, digest); err == nil || applied {
		t.Fatal("conflicting certificate was applied")
	}
	assertUnchanged(t)
	// Identical payload with a different quorum-signature order is still a
	// replay, and must not replace or mutate the already shared object.
	replayed := first
	replayed.Signatures = []Signature{first.Signatures[2], first.Signatures[0], first.Signatures[1]}
	checked, digest, err = f.verifier.Verify(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := view.apply(checked, digest); err != nil || applied {
		t.Fatalf("same-payload replay changed the view: applied=%v err=%v", applied, err)
	}
	assertUnchanged(t)
}
