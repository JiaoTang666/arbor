package ownership

import (
	"fmt"
	"os"
	"sync"
)

// planDependency is immutable after publication in a View. All first owners
// authorized by one certificate share its causal dependency, not its payload.
type planDependency struct {
	EvidenceVersion uint64
	EvidenceDigest  string
	EvidenceCutoff  uint64
	AdmissionStart  uint64
	AdmissionEnd    uint64
}

// View is an immutable first-owner table. Authorization is not account state
// creation; the latter still occurs only in the native committed state trie.
type View struct {
	mu              sync.RWMutex
	refreshMu       sync.Mutex
	context         Context
	version         uint64
	genesis         *Genesis
	owners          map[string]uint64 // authorized additions only; never genesis
	proposals       map[string]string
	committeeDigest string
	genesisOwners   int
	configHash      string
	bundleHash      string
	verifier        *Verifier
	journalPath     string
	journalIdentity os.FileInfo
	journalOffset   int64
	runtimeStatus   *runtimeStatusWriter // diagnostic only; protected by refreshMu
	evidence        map[string]*planDependency
}

func newView(v *Verifier, genesis map[string]uint64) (*View, error) {
	base, err := NewGenesis(genesis, v.Context().Shards)
	if err != nil {
		return nil, err
	}
	return newViewWithGenesis(v, base)
}

func newViewWithGenesis(v *Verifier, genesis *Genesis) (*View, error) {
	if genesis == nil || genesis.shards != v.Context().Shards || genesis.digest != v.Context().GenesisRoutesDigest {
		return nil, fmt.Errorf("genesis routes mismatch")
	}
	view := &View{context: v.Context(), genesis: genesis, owners: map[string]uint64{}, proposals: map[string]string{},
		committeeDigest: v.CommitteeDigest(), genesisOwners: genesis.Len(), verifier: v, evidence: map[string]*planDependency{}}
	return view, nil
}

func (view *View) apply(cert Certificate, digest string) (bool, error) {
	b := cert.Batch
	if previous, exists := view.proposals[b.ProposalID]; exists {
		if previous != digest {
			return false, fmt.Errorf("proposal identity conflict")
		}
		return false, nil // Same payload, even with reordered quorum signatures.
	}
	if b.BaseVersion != view.version {
		return false, fmt.Errorf("stale registry base version")
	}
	for _, binding := range b.Bindings {
		if _, bound := view.ownerLocked(binding.Account); bound {
			return false, fmt.Errorf("first owner already bound: %s", binding.Account)
		}
	}
	var dependency *planDependency
	if b.EvidenceVersion != 0 {
		dependency = &planDependency{EvidenceVersion: b.EvidenceVersion, EvidenceDigest: b.EvidenceDigest,
			EvidenceCutoff: b.EvidenceCutoff, AdmissionStart: b.AdmissionStart, AdmissionEnd: b.AdmissionEnd}
	}
	for _, binding := range b.Bindings {
		view.owners[binding.Account] = binding.Shard
		if dependency != nil {
			view.evidence[binding.Account] = dependency
		}
	}
	view.proposals[b.ProposalID] = digest
	view.version++
	return true, nil
}

func BuildView(v *Verifier, genesis map[string]uint64, certificates []Certificate) (*View, error) {
	base, err := NewGenesis(genesis, v.Context().Shards)
	if err != nil {
		return nil, err
	}
	return buildViewWithGenesis(v, base, certificates)
}

func buildViewWithGenesis(v *Verifier, genesis *Genesis, certificates []Certificate) (*View, error) {
	view, err := newViewWithGenesis(v, genesis)
	if err != nil {
		return nil, err
	}
	for _, cert := range certificates {
		checked, digest, err := v.Verify(cert)
		if err != nil {
			return nil, err
		}
		if _, err := view.apply(checked, digest); err != nil {
			return nil, err
		}
	}
	return view, nil
}

func (view *View) Owner(account string) (uint64, bool) {
	account, err := NormalizeAccount(account)
	if err != nil {
		return 0, false
	}
	view.mu.RLock()
	defer view.mu.RUnlock()
	owner, exists := view.ownerLocked(account)
	return owner, exists
}

func (view *View) Check(account string, target uint64) error {
	owner, exists := view.Owner(account)
	if !exists && view.journalPath != "" {
		if err := view.Refresh(); err != nil {
			return err
		}
		owner, exists = view.Owner(account)
	}
	if !exists {
		return fmt.Errorf("first owner not authorized")
	}
	if owner != target {
		return fmt.Errorf("route conflicts with authorized owner")
	}
	return nil
}

// CheckAuthorizedBindings checks one transaction's canonical endpoints against
// one verified prefix. It only reads memory: callers refresh at the batch
// boundary before entering a queue lock. Published owners and dependencies are
// never rewritten, so a successful check remains valid after another refresh.
func (view *View) CheckAuthorizedBindings(bindings []Binding, sequence uint64) error {
	view.mu.RLock()
	defer view.mu.RUnlock()
	for _, binding := range bindings {
		owner, exists := view.ownerLocked(binding.Account)
		if !exists {
			return fmt.Errorf("first owner not authorized")
		}
		if owner != binding.Shard {
			return fmt.Errorf("route conflicts with authorized owner")
		}
		if dependency := view.evidence[binding.Account]; dependency != nil && sequence < dependency.AdmissionStart {
			return fmt.Errorf("future first-owner certificate used before its input batch")
		}
	}
	return nil
}

func (view *View) Context() Context { return view.context }
func (view *View) Version() uint64 {
	view.mu.RLock()
	defer view.mu.RUnlock()
	return view.version
}
