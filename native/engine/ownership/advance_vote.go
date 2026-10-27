package ownership

import (
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

const MaxCatchupCertificates = 64

type verifiedParent struct {
	certificate Certificate
	digest      string
}

// AdvanceVote atomically applies a short verified QC prefix and locks the next
// vote. One synchronous bbolt commit replaces separate Apply and Vote barriers.
// It does NOT finalize the voted batch, relax slot locks, or disable fsync.
func (s *Signer) AdvanceVote(parents []Certificate, batch Batch) (Signature, error) {
	if len(parents) > MaxCatchupCertificates {
		return Signature{}, fmt.Errorf("catch-up certificate bound exceeded")
	}
	r := s.registry
	checked, digest, err := r.verifier.checkedBatch(batch)
	if err != nil {
		return Signature{}, err
	}
	if checked.Context.AuthorizationSchema == CompactAuthorizationSchema && checked.PlacementWitness == nil {
		return Signature{}, fmt.Errorf("compact vote requires a full placement witness")
	}
	verified := make([]verifiedParent, len(parents))
	for i, certificate := range parents {
		parent, identity, err := r.verifier.Verify(certificate)
		if err != nil {
			return Signature{}, err
		}
		verified[i] = verifiedParent{parent, identity}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fenced {
		return Signature{}, fmt.Errorf("signer fenced; close and recover")
	}
	var result Signature
	overlay := map[string]uint64{}
	var pending []verifiedParent
	voteChanged, committing := false, false
	err = r.db.Update(func(tx *bolt.Tx) error {
		votes, owners := tx.Bucket(signerVoteBucket), tx.Bucket(registryBucket)
		if votes == nil || owners == nil || string(owners.Get([]byte("mode"))) != r.storageMode || len(owners.Get([]byte("version"))) != 8 {
			r.fenced = true
			return fmt.Errorf("signer buckets/mode/version corrupt")
		}
		version := binary.BigEndian.Uint64(owners.Get([]byte("version")))
		owner := func(account string) (uint64, bool) {
			if shard, exists := overlay[account]; exists {
				return shard, true
			}
			shard, exists := r.ownerCache[account]
			return shard, exists
		}
		proposals := map[string]string{}
		// Validate the whole transition before writing: a bad next proposal must
		// neither install parents nor consume a vote. No full owner-table clone.
		for _, parent := range verified {
			previous := owners.Get([]byte("p" + parent.certificate.Batch.ProposalID))
			if staged, exists := proposals[parent.certificate.Batch.ProposalID]; exists {
				previous = []byte(staged)
			}
			changed, err := checkCertificatePrefix(parent.certificate, parent.digest, version, previous, owner)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			materialized, err := s.materializePlacement(tx, parent.certificate, parent.digest)
			if err != nil {
				return err
			}
			if err := s.validatePlacement(materialized, owner); err != nil {
				return err
			}
			for _, binding := range parent.certificate.Batch.Bindings {
				overlay[binding.Account] = binding.Shard
			}
			proposals[parent.certificate.Batch.ProposalID] = parent.digest
			version++
			pending = append(pending, parent)
		}
		if len(parents) != 0 && checked.BaseVersion != version {
			return fmt.Errorf("advanced vote must extend resulting committed prefix")
		}
		var voteRaw []byte
		if raw := votes.Get(voteKey(checked.BaseVersion)); raw != nil {
			previous, err := decodeVote(raw)
			if err != nil {
				r.fenced = true
				return err
			}
			oldDigest, err := s.checkVote(previous, checked.BaseVersion)
			if err != nil {
				r.fenced = true
				return err
			}
			if oldDigest != digest {
				return fmt.Errorf("slot already voted for another payload")
			}
			result = previous.Signature
		} else {
			if checked.BaseVersion != version {
				return fmt.Errorf("proposal must extend committed prefix")
			}
			if owners.Get([]byte("p"+checked.ProposalID)) != nil || proposals[checked.ProposalID] != "" {
				return fmt.Errorf("proposal identity already committed")
			}
			for _, binding := range checked.Bindings {
				if _, bound := owner(binding.Account); bound {
					return fmt.Errorf("first owner already bound: %s", binding.Account)
				}
			}
			if err := s.validatePlacement(checked, owner); err != nil {
				return err
			}
			message, _ := hex.DecodeString(digest)
			result = Signature{MemberID: s.memberID, Value: ed25519.Sign(s.key, message)}
			voteRaw, err = json.Marshal(voteRecord{Batch: checked, Signature: result})
			if err != nil {
				return err
			}
			voteChanged = true
		}
		if len(pending) == 0 && !voteChanged {
			return nil
		}
		committing = true
		for _, parent := range pending {
			if err := persistCertificate(owners, parent.certificate, parent.digest, r.storageMode); err != nil {
				return err
			}
		}
		if voteChanged {
			count := votes.Get([]byte("count"))
			if len(count) != 8 || binary.BigEndian.Uint64(count) == ^uint64(0) {
				return fmt.Errorf("signer vote count corrupt")
			}
			if err := votes.Put(voteKey(checked.BaseVersion), voteRaw); err != nil {
				return err
			}
			if err := votes.Put([]byte("count"), uintBytes(binary.BigEndian.Uint64(count)+1)); err != nil {
				return err
			}
		}
		if len(pending) != 0 && r.beforeCommit != nil {
			if err := r.beforeCommit(); err != nil {
				return err
			}
		}
		if voteChanged && s.beforeVoteCommit != nil {
			return s.beforeVoteCommit()
		}
		return nil
	})
	if err == nil && len(pending) != 0 && r.afterCommit != nil {
		err = r.afterCommit()
	}
	if err == nil && voteChanged && s.afterVoteCommit != nil {
		err = s.afterVoteCommit()
	}
	if err != nil {
		if committing {
			r.fenced = true
		}
		return Signature{}, err
	}
	// Neither an aborted transaction nor an uncertain acknowledgement publishes
	// owner changes. Reopening resolves the durable truth after a fenced error.
	for account, shard := range overlay {
		r.ownerCache[account] = shard
	}
	result.Value = append([]byte(nil), result.Value...)
	return result, nil
}
