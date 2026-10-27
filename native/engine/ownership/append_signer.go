package ownership

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"os"
	"sync"

	"github.com/gofrs/flock"
)

// AppendSignerMode is explicit: a Bolt store is never converted in place.
const AppendSignerMode = "append_v1"

// AppendSigner keeps authorization semantics unchanged. One append and Sync
// durably commit the verified parent prefix AND the next immutable vote. Only
// indexes, owners and the most recent full vote remain in memory. As with the
// Bolt signer, one non-cloned, non-rolled-back store per key is an OS assumption.
type AppendSigner struct {
	mu               sync.Mutex
	file             *os.File
	lock             *flock.Flock
	path             string
	verifier         *Verifier
	memberID         string
	key              ed25519.PrivateKey
	requirePlacement bool
	fenced           bool
	closed           bool
	version          uint64
	index            uint64
	end              int64
	head             [32]byte
	genesis          *Genesis
	ownerCache       map[string]uint64
	proposals        map[string]string
	votes            map[uint64]appendVoteRef
	lastVote         *voteRecord
	lastDigest       string
	// Private fault points. No production option disables durable syncing.
	beforeAppend func() error
	afterWrite   func() error
	afterAppend  func() error
}

type appendVoteRef struct {
	offset    int64
	hash      [32]byte
	digest    string
	signature Signature
}

type appendTransition struct {
	Schema      string        `json:"schema"`
	Index       uint64        `json:"index"`
	BaseVersion uint64        `json:"base_version"`
	Parents     []Certificate `json:"parents,omitempty"`
	Vote        *voteRecord   `json:"vote,omitempty"`
}

type appendPlan struct {
	parents   []verifiedParent
	vote      *voteRecord
	digest    string
	result    Signature
	version   uint64
	owners    map[string]uint64
	proposals map[string]string
}

func OpenAppendSignerWithPolicy(path string, v *Verifier, genesis map[string]uint64, memberID string, key ed25519.PrivateKey, requirePlacement bool) (*AppendSigner, error) {
	if v == nil || len(key) != ed25519.PrivateKeySize || v.keys[memberID] == nil {
		return nil, fmt.Errorf("invalid signer identity/key")
	}
	if v.Context().AuthorizationSchema == CompactAuthorizationSchema && !requirePlacement {
		return nil, fmt.Errorf("compact authorization requires pinned placement verification")
	}
	ownedKey := append(ed25519.PrivateKey(nil), key...)
	if !bytes.Equal(ed25519.NewKeyFromSeed(ownedKey.Seed()), ownedKey) || !bytes.Equal(ownedKey.Public().(ed25519.PublicKey), v.keys[memberID]) {
		return nil, fmt.Errorf("signer key does not match pinned member")
	}
	initial, err := newView(v, genesis)
	if err != nil {
		return nil, err
	}
	file, lock, created, err := openAppendFile(path)
	if err != nil {
		return nil, err
	}
	s := &AppendSigner{file: file, lock: lock, path: path, verifier: v, memberID: memberID,
		key: ownedKey, requirePlacement: requirePlacement, genesis: initial.genesis, ownerCache: initial.owners,
		proposals: initial.proposals, votes: map[uint64]appendVoteRef{}}
	if created {
		err = s.initialize()
	} else {
		err = s.replay(true, nil)
		if err == nil {
			// A complete frame left by a process dying before Sync may still be
			// readable only from the OS page cache. Make recovered locks durable
			// BEFORE any exact retry can return their saved signatures.
			err = s.file.Sync()
			if err == nil {
				// Also covers an initializer dying after its file Sync but before
				// the first namespace barrier. This is startup-only, not per vote.
				err = syncAppendDirectory(s.path)
			}
		}
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *AppendSigner) placementRequired() bool { return s.requirePlacement }
func (s *AppendSigner) storageMode() string     { return AppendSignerMode }
func (s *AppendSigner) Vote(batch Batch) (Signature, error) {
	return s.AdvanceVote(nil, batch)
}

func (s *AppendSigner) validatePlacement(batch Batch, owner func(string) (uint64, bool)) error {
	if s.requirePlacement || batch.PlacementWitness != nil {
		return validatePlacementRelations(batch, owner)
	}
	return nil
}

func (s *AppendSigner) checkVote(vote voteRecord, slot uint64) (string, error) {
	_, digest, err := s.verifier.checkedOwnedBatch(vote.Batch)
	if err != nil || vote.Batch.BaseVersion != slot || vote.Signature.MemberID != s.memberID {
		return "", fmt.Errorf("vote context/identity corrupt")
	}
	if vote.Batch.Context.AuthorizationSchema == CompactAuthorizationSchema && vote.Batch.PlacementWitness == nil {
		return "", fmt.Errorf("compact vote lost its durable placement witness")
	}
	message, _ := hex.DecodeString(digest)
	if !ed25519.Verify(s.verifier.keys[s.memberID], message, vote.Signature.Value) {
		return "", fmt.Errorf("persisted vote signature invalid")
	}
	return digest, nil
}

func (s *AppendSigner) materialize(parent verifiedParent) (Batch, error) {
	batch := parent.certificate.Batch
	if batch.Context.AuthorizationSchema != CompactAuthorizationSchema || batch.PlacementWitness != nil {
		return batch, nil // Public Verify already checked the supplied witness.
	}
	ref, exists := s.votes[batch.BaseVersion]
	if !exists || ref.digest != parent.digest {
		return Batch{}, fmt.Errorf("compact parent requires a matching durable vote or full witness")
	}
	included := false
	for _, signature := range parent.certificate.Signatures {
		if signature.MemberID == s.memberID && bytes.Equal(signature.Value, ref.signature.Value) {
			included = true
		}
	}
	if !included {
		return Batch{}, fmt.Errorf("compact parent omits the member's durable signature; full witness required")
	}
	vote, err := s.loadVote(batch.BaseVersion, ref)
	if err != nil {
		s.fenced = true
		return Batch{}, err
	}
	return vote.Batch, nil
}

// prepare validates the WHOLE transition against a small overlay. It does not
// mutate owners, consume slots, write bytes, or expose a signature to callers.
// A supplied signature is used only by strict WAL replay, after checkVote.
func (s *AppendSigner) prepare(parents []verifiedParent, batch *Batch, digest string, supplied *Signature) (appendPlan, error) {
	plan := appendPlan{version: s.version, owners: map[string]uint64{}, proposals: map[string]string{}, digest: digest}
	owner := func(account string) (uint64, bool) {
		if shard, exists := plan.owners[account]; exists {
			return shard, true
		}
		shard, exists := s.ownerCache[account]
		if exists {
			return shard, true
		}
		return s.genesis.Owner(account)
	}
	for _, parent := range parents {
		previous := s.proposals[parent.certificate.Batch.ProposalID]
		if staged, exists := plan.proposals[parent.certificate.Batch.ProposalID]; exists {
			previous = staged
		}
		changed, err := checkCertificatePrefix(parent.certificate, parent.digest, plan.version, []byte(previous), owner)
		if err != nil {
			return plan, err
		}
		if !changed {
			continue
		}
		material, err := s.materialize(parent)
		if err != nil {
			return plan, err
		}
		if err := s.validatePlacement(material, owner); err != nil {
			return plan, err
		}
		for _, binding := range parent.certificate.Batch.Bindings {
			plan.owners[binding.Account] = binding.Shard
		}
		plan.proposals[parent.certificate.Batch.ProposalID] = parent.digest
		plan.version++
		plan.parents = append(plan.parents, parent)
	}
	if batch == nil {
		return plan, nil
	}
	if len(parents) != 0 && batch.BaseVersion != plan.version {
		return plan, fmt.Errorf("advanced vote must extend resulting committed prefix")
	}
	if ref, exists := s.votes[batch.BaseVersion]; exists {
		if supplied != nil {
			return plan, fmt.Errorf("WAL repeated a durable vote")
		}
		if ref.digest != digest {
			return plan, fmt.Errorf("slot already voted for another payload")
		}
		plan.result = ref.signature
		return plan, nil
	}
	if batch.BaseVersion != plan.version {
		return plan, fmt.Errorf("proposal must extend committed prefix")
	}
	if s.proposals[batch.ProposalID] != "" || plan.proposals[batch.ProposalID] != "" {
		return plan, fmt.Errorf("proposal identity already committed")
	}
	for _, binding := range batch.Bindings {
		if _, exists := owner(binding.Account); exists {
			return plan, fmt.Errorf("first owner already bound: %s", binding.Account)
		}
	}
	if err := s.validatePlacement(*batch, owner); err != nil {
		return plan, err
	}
	if supplied == nil {
		message, _ := hex.DecodeString(digest)
		plan.result = Signature{MemberID: s.memberID, Value: ed25519.Sign(s.key, message)}
	} else {
		plan.result = *supplied
	}
	plan.vote = &voteRecord{Batch: *batch, Signature: plan.result}
	return plan, nil
}

func verifyAppendParents(v *Verifier, parents []Certificate) ([]verifiedParent, error) {
	if len(parents) > MaxCatchupCertificates {
		return nil, fmt.Errorf("catch-up certificate bound exceeded")
	}
	verified := make([]verifiedParent, len(parents))
	for i, cert := range parents {
		checked, digest, err := v.Verify(cert)
		if err != nil {
			return nil, err
		}
		verified[i] = verifiedParent{checked, digest}
	}
	return verified, nil
}

func (s *AppendSigner) AdvanceVote(parents []Certificate, batch Batch) (Signature, error) {
	checked, digest, err := s.verifier.checkedBatch(batch)
	if err != nil {
		return Signature{}, err
	}
	if checked.Context.AuthorizationSchema == CompactAuthorizationSchema && checked.PlacementWitness == nil {
		return Signature{}, fmt.Errorf("compact vote requires a full placement witness")
	}
	verified, err := verifyAppendParents(s.verifier, parents)
	if err != nil {
		return Signature{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fenced {
		return Signature{}, fmt.Errorf("signer fenced; close and recover")
	}
	plan, err := s.prepare(verified, &checked, digest, nil)
	if err == nil {
		err = s.commitPlan(plan)
	}
	if err != nil {
		return Signature{}, err
	}
	result := plan.result
	result.Value = append([]byte(nil), result.Value...)
	return result, nil
}

func (s *AppendSigner) Commit(cert Certificate) (bool, error) {
	verified, err := verifyAppendParents(s.verifier, []Certificate{cert})
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fenced {
		return false, fmt.Errorf("signer fenced; close and recover")
	}
	plan, err := s.prepare(verified, nil, "", nil)
	if err == nil {
		err = s.commitPlan(plan)
	}
	return err == nil && len(plan.parents) != 0, err
}

func (s *AppendSigner) Version() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fenced {
		return 0, fmt.Errorf("signer fenced; close and recover")
	}
	return s.version, nil
}

// Snapshot is explicitly expensive and isolated. Only startup and a requested
// Snapshot replay history; normal Vote/Commit/Version never scan the WAL.
func (s *AppendSigner) Snapshot(genesis map[string]uint64) (*View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fenced {
		return nil, fmt.Errorf("signer fenced; close and recover")
	}
	view, err := newView(s.verifier, genesis)
	if err != nil {
		return nil, err
	}
	reader := &AppendSigner{file: s.file, path: s.path, verifier: s.verifier, memberID: s.memberID,
		key: s.key, requirePlacement: s.requirePlacement, genesis: view.genesis, ownerCache: view.owners,
		proposals: view.proposals, votes: map[uint64]appendVoteRef{}}
	if err := reader.replay(false, view); err != nil {
		s.fenced = true
		return nil, err
	}
	if reader.version != s.version || reader.index != s.index || reader.head != s.head || reader.end != s.end {
		s.fenced = true
		return nil, fmt.Errorf("signer WAL changed after opening")
	}
	return view, nil
}

func (s *AppendSigner) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fenced = true
	if s.closed {
		return nil
	}
	s.closed = true
	err := s.file.Close()
	if unlockErr := s.lock.Close(); err == nil {
		err = unlockErr
	}
	return err
}
