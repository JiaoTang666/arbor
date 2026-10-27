package ownership

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	bolt "go.etcd.io/bbolt"
)

var signerVoteBucket = []byte("first-owner-votes-v1")

// Signer owns ONE member key and durable store. Vote and Commit share the
// registry lock and transaction state. This is not PBFT: split votes remain
// locked, without timeout replacement or committee rollover. One persistent
// store per key, without cloning/rollback, is an administrative assumption.
type Signer struct {
	registry         *Registry
	memberID         string
	key              ed25519.PrivateKey
	requirePlacement bool
	beforeVoteCommit func() error // Private fault points; unset in production.
	afterVoteCommit  func() error
}

type voteRecord struct {
	Batch     Batch     `json:"batch"`
	Signature Signature `json:"signature"`
}

func voteKey(slot uint64) []byte {
	key := versionKey(slot)
	key[0] = 'v'
	return key
}

func OpenSigner(path string, v *Verifier, genesis map[string]uint64, memberID string, key ed25519.PrivateKey) (*Signer, error) {
	return OpenSignerWithMode(path, v, genesis, memberID, key, "")
}

// OpenSignerWithMode pins a storage schema, never converts an existing store.
// Vote/Commit signatures, durability, fencing and recovery are unchanged.
func OpenSignerWithMode(path string, v *Verifier, genesis map[string]uint64, memberID string, key ed25519.PrivateKey, mode string) (*Signer, error) {
	return OpenSignerWithPolicy(path, v, genesis, memberID, key, mode, false)
}

// OpenSignerWithPolicy pins placement verification at creation. A store cannot
// silently downgrade or upgrade policy on restart; legacy stores stay readable.
func OpenSignerWithPolicy(path string, v *Verifier, genesis map[string]uint64, memberID string, key ed25519.PrivateKey, mode string, requirePlacement bool) (*Signer, error) {
	if v == nil || len(key) != ed25519.PrivateKeySize || v.keys[memberID] == nil {
		return nil, fmt.Errorf("invalid signer identity/key")
	}
	if v.Context().AuthorizationSchema == CompactAuthorizationSchema && !requirePlacement {
		return nil, fmt.Errorf("compact authorization requires pinned placement verification")
	}
	ownedKey := append(ed25519.PrivateKey(nil), key...)
	derived := ed25519.NewKeyFromSeed(ownedKey.Seed())
	if !bytes.Equal(derived, ownedKey) || !bytes.Equal(ownedKey.Public().(ed25519.PublicKey), v.keys[memberID]) {
		return nil, fmt.Errorf("signer key does not match pinned member")
	}
	identity, _ := json.Marshal(Member{ID: memberID, PublicKey: v.keys[memberID]})
	registry, err := openRegistryMode(path, v, genesis, func(tx *bolt.Tx, created bool) error {
		bucket := tx.Bucket(signerVoteBucket)
		if bucket == nil {
			if !created {
				return fmt.Errorf("existing signer store has no vote bucket")
			}
			var err error
			bucket, err = tx.CreateBucket(signerVoteBucket)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte("member"), identity); err != nil {
				return err
			}
			if requirePlacement {
				if err := bucket.Put([]byte("placement-policy"), []byte(PlacementWitnessSchema)); err != nil {
					return err
				}
			}
			return bucket.Put([]byte("count"), uintBytes(0))
		}
		if !bytes.Equal(bucket.Get([]byte("member")), identity) {
			return fmt.Errorf("signer store identity mismatch")
		}
		policy := bucket.Get([]byte("placement-policy"))
		if requirePlacement && string(policy) != PlacementWitnessSchema || !requirePlacement && policy != nil {
			return fmt.Errorf("signer placement policy mismatch")
		}
		return nil
	}, mode)
	if err != nil {
		return nil, err
	}
	signer := &Signer{registry: registry, memberID: memberID, key: ownedKey, requirePlacement: requirePlacement}
	if err := signer.recoverVotes(genesis); err != nil {
		registry.Close()
		return nil, err
	}
	return signer, nil
}

func decodeVote(raw []byte) (voteRecord, error) {
	var vote voteRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&vote); err != nil {
		return vote, err
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return vote, fmt.Errorf("trailing vote data")
	}
	return vote, nil
}

func (s *Signer) checkVote(vote voteRecord, slot uint64) (string, error) {
	// decodeVote owns this object; do not clone the full witness a second time.
	_, digest, err := s.registry.verifier.checkedOwnedBatch(vote.Batch)
	if err != nil || vote.Batch.BaseVersion != slot || vote.Signature.MemberID != s.memberID {
		return "", fmt.Errorf("vote context/identity corrupt")
	}
	if vote.Batch.Context.AuthorizationSchema == CompactAuthorizationSchema && vote.Batch.PlacementWitness == nil {
		return "", fmt.Errorf("compact vote lost its durable placement witness")
	}
	message, _ := hex.DecodeString(digest)
	if !ed25519.Verify(s.registry.verifier.keys[s.memberID], message, vote.Signature.Value) {
		return "", fmt.Errorf("persisted vote signature invalid")
	}
	return digest, nil
}

// Vote never publishes a signature before its synchronous transaction commits.
// An exact old vote remains replayable after Commit, without erasing evidence.
func (s *Signer) Vote(batch Batch) (Signature, error) {
	return s.AdvanceVote(nil, batch)
}

// A minority member may accept a different valid quorum decision. Its old vote
// remains immutable. These are authorization votes, not native payment commits.
func (s *Signer) Commit(cert Certificate) (bool, error) {
	return s.registry.applyResolved(cert, s.validatePlacement, s.materializePlacement)
}

func (s *Signer) validatePlacement(batch Batch, owner func(string) (uint64, bool)) error {
	if s.requirePlacement || batch.PlacementWitness != nil {
		// The owned batch already passed shape and digest verification. Recheck
		// the policy against the CURRENT prefix without reencoding the witness.
		return validatePlacementRelations(batch, owner)
	}
	return nil
}

// A compact QC does not give a signer license to skip independent validation.
// It can reuse only its exact durable vote, included verbatim in this QC;
// otherwise the coordinator must attach the committed full witness.
func (s *Signer) materializePlacement(tx *bolt.Tx, cert Certificate, digest string) (Batch, error) {
	batch := cert.Batch
	if batch.Context.AuthorizationSchema != CompactAuthorizationSchema || batch.PlacementWitness != nil {
		return batch, nil // Full material was checked by Verifier.Verify.
	}
	votes := tx.Bucket(signerVoteBucket)
	if votes == nil {
		return Batch{}, fmt.Errorf("signer vote bucket missing")
	}
	raw := votes.Get(voteKey(batch.BaseVersion))
	if raw == nil {
		return Batch{}, fmt.Errorf("compact parent requires a matching vote or full witness")
	}
	vote, err := decodeVote(raw)
	if err != nil {
		return Batch{}, err
	}
	identity, err := s.checkVote(vote, batch.BaseVersion)
	if err != nil || identity != digest {
		return Batch{}, fmt.Errorf("compact parent does not match durable vote")
	}
	for _, signature := range cert.Signatures {
		if signature.MemberID == s.memberID && bytes.Equal(signature.Value, vote.Signature.Value) {
			return vote.Batch, nil
		}
	}
	return Batch{}, fmt.Errorf("compact parent omits the member's durable signature; full witness required")
}
func (s *Signer) Snapshot(genesis map[string]uint64) (*View, error) {
	return s.registry.Snapshot(genesis)
}
func (s *Signer) Close() error { return s.registry.Close() }

// Version reads the already-validated committed head. Full history verification
// is reserved for startup/Snapshot, not repeated on every IPC response.
func (s *Signer) Version() (uint64, error) {
	r := s.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fenced {
		return 0, fmt.Errorf("signer fenced; close and recover")
	}
	var version uint64
	err := r.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(registryBucket)
		if bucket == nil || string(bucket.Get([]byte("mode"))) != r.storageMode {
			return fmt.Errorf("signer registry missing or storage mode changed")
		}
		raw := bucket.Get([]byte("version"))
		if len(raw) != 8 {
			return fmt.Errorf("signer version corrupt")
		}
		version = binary.BigEndian.Uint64(raw)
		return nil
	})
	if err != nil {
		r.fenced = true
	}
	return version, err
}

// Check historical votes against their THEN-committed prefix, not today's
// owner table. Minority votes may differ from same-slot certificates.
func (s *Signer) recoverVotes(genesis map[string]uint64) error {
	r := s.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.db.View(func(tx *bolt.Tx) error {
		owners, votes := tx.Bucket(registryBucket), tx.Bucket(signerVoteBucket)
		if owners == nil || votes == nil {
			return fmt.Errorf("signer buckets missing")
		}
		countRaw := votes.Get([]byte("count"))
		if len(countRaw) != 8 {
			return fmt.Errorf("signer vote count corrupt")
		}
		version := binary.BigEndian.Uint64(owners.Get([]byte("version")))
		view, err := newView(r.verifier, genesis)
		if err != nil {
			return err
		}
		voteCount := 0
		for slot := uint64(0); slot <= version; slot++ {
			if raw := votes.Get(voteKey(slot)); raw != nil {
				vote, err := decodeVote(raw)
				if err != nil {
					return err
				}
				if _, err := s.checkVote(vote, slot); err != nil {
					return err
				}
				if err := s.validatePlacement(vote.Batch, view.ownerLocked); err != nil {
					return err
				}
				for _, binding := range vote.Batch.Bindings {
					if _, bound := view.ownerLocked(binding.Account); bound {
						return fmt.Errorf("vote repeated an already-bound owner")
					}
				}
				if _, reused := view.proposals[vote.Batch.ProposalID]; reused {
					return fmt.Errorf("vote reused a committed proposal identity")
				}
				voteCount++
			}
			if slot < version {
				var cert Certificate
				if err := json.Unmarshal(owners.Get(versionKey(slot+1)), &cert); err != nil {
					return err
				}
				checked, digest, err := r.verifier.Verify(cert)
				if err != nil {
					return err
				}
				materialized, err := s.materializePlacement(tx, checked, digest)
				if err != nil {
					return err
				}
				if err := s.validatePlacement(materialized, view.ownerLocked); err != nil {
					return err
				}
				if changed, err := view.apply(checked, digest); err != nil || !changed {
					return fmt.Errorf("signer certificate prefix corrupt")
				}
			}
		}
		count := 0
		if err := votes.ForEach(func(key, value []byte) error {
			count++
			if string(key) == "member" || string(key) == "count" || s.requirePlacement && string(key) == "placement-policy" {
				return nil
			}
			if len(key) != 9 || key[0] != 'v' || binary.BigEndian.Uint64(key[1:]) > version || value == nil {
				return fmt.Errorf("signer unknown vote record")
			}
			return nil
		}); err != nil {
			return err
		}
		expected := voteCount + 2
		if s.requirePlacement {
			expected++
		}
		if count != expected || binary.BigEndian.Uint64(countRaw) != uint64(voteCount) {
			return fmt.Errorf("signer extra/missing vote records")
		}
		return nil
	})
}
