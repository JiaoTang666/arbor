package ownership

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var registryBucket = []byte("first-owner-v1")

const registryDeltaMode = "delta_v1"

// RegistryCertificateMode stores signed ownership facts, not a second address
// index. Recovery rebuilds the private owner cache from the verified prefix.
const RegistryCertificateMode = "certificate_v1"

type Registry struct {
	mu          sync.Mutex
	db          *bolt.DB
	verifier    *Verifier
	fenced      bool
	storageMode string
	// Owned by the registry, never shared with callers or public Snapshots.
	// Verified once on open; changed only after a fully acknowledged commit.
	ownerCache map[string]uint64
	// Test-only durability fault point; unset in production. If it fires after
	// Update, the disk can already contain the event. Reopen resolves ambiguity.
	afterCommit  func() error
	beforeCommit func() error
}

func versionKey(version uint64) []byte {
	key := make([]byte, 9)
	key[0] = 'c'
	binary.BigEndian.PutUint64(key[1:], version)
	return key
}

func uintBytes(value uint64) []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, value)
	return raw
}

// Open uses Bolt's atomic transactions, file lock and synchronous commit. It
// does not add another custom WAL or disable fsync. The parent must exist.
func Open(path string, v *Verifier, genesis map[string]uint64) (*Registry, error) {
	return openRegistry(path, v, genesis, nil)
}

// initializeExtra seals an independent signer's identity and vote bucket in
// the SAME initial DB transaction as the registry.
func openRegistry(path string, v *Verifier, genesis map[string]uint64, initializeExtra func(*bolt.Tx, bool) error) (*Registry, error) {
	return openRegistryMode(path, v, genesis, initializeExtra, "")
}

// An empty requested mode preserves the legacy API: create delta_v1, or read
// the existing schema without conversion. Explicit modes must match on reopen.
func openRegistryMode(path string, v *Verifier, genesis map[string]uint64, initializeExtra func(*bolt.Tx, bool) error, requestedMode string) (*Registry, error) {
	creationMode := registryDeltaMode
	if requestedMode != "" {
		if requestedMode != registryDeltaMode && requestedMode != RegistryCertificateMode {
			return nil, fmt.Errorf("unknown requested registry storage mode")
		}
		creationMode = requestedMode
	}
	initial, err := newView(v, genesis)
	if err != nil {
		return nil, err
	}
	// An exclusive pathname reservation distinguishes a genuinely new store
	// from a damaged existing one, including concurrent initialization attempts.
	file, createErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	created := createErr == nil
	if created {
		if err := file.Close(); err != nil {
			return nil, err
		}
	} else if !os.IsExist(createErr) {
		return nil, createErr
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	r := &Registry{db: db, verifier: v}
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(registryBucket)
		if b != nil {
			r.storageMode = string(b.Get([]byte("mode")))
			if requestedMode != "" && r.storageMode != requestedMode {
				return fmt.Errorf("existing registry storage mode mismatch")
			}
			if initializeExtra != nil {
				return initializeExtra(tx, created)
			}
			return nil
		}
		if !created {
			return fmt.Errorf("existing registry has no ownership bucket")
		}
		var err error
		b, err = tx.CreateBucket(registryBucket)
		if err != nil {
			return err
		}
		config, _ := json.Marshal(v.config)
		if err := b.Put([]byte("config"), config); err != nil {
			return err
		}
		if err := b.Put([]byte("version"), uintBytes(0)); err != nil {
			return err
		}
		// The pinned genesis is immutable. certificate_v1 also omits dynamic
		// address rows: signed events already contain every committed binding.
		if err := b.Put([]byte("mode"), []byte(creationMode)); err != nil {
			return err
		}
		r.storageMode = creationMode
		if initializeExtra != nil {
			return initializeExtra(tx, created)
		}
		return nil
	})
	if err == nil {
		// Legacy durable-store caches retain their full-map representation.
		// Runtime native views instead share the sealed base and store only deltas.
		err = r.reconcile(initial, genesis)
		if err == nil {
			r.ownerCache = initial.ownerSnapshot()
		}
	}
	if err == nil && created {
		// The DB transaction syncs its contents; on Linux the new pathname
		// needs a directory barrier too. Do this once, not for every batch.
		directory, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			err = openErr
		} else {
			err = directory.Sync()
			directory.Close()
		}
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return r, nil
}

// Apply commits version, proposal identity and the full signed binding event in
// ONE transaction. Its private cache is published only after durable success.
func (r *Registry) Apply(cert Certificate) (bool, error) {
	return r.apply(cert, nil)
}

func (r *Registry) apply(cert Certificate, validate func(Batch, func(string) (uint64, bool)) error) (bool, error) {
	return r.applyResolved(cert, validate, nil)
}

// Signers resolve compact evidence under the same store transaction as Apply.
// Public registries need only quorum verification, not a signer's private vote.
func (r *Registry) applyResolved(cert Certificate, validate func(Batch, func(string) (uint64, bool)) error, resolve func(*bolt.Tx, Certificate, string) (Batch, error)) (bool, error) {
	checked, digest, err := r.verifier.Verify(cert)
	if err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fenced {
		return false, fmt.Errorf("registry fenced; close and recover")
	}
	// A repeated payload needs no new write transaction or durability barrier.
	var previous []byte
	if err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(registryBucket)
		if b == nil || string(b.Get([]byte("mode"))) != r.storageMode {
			r.fenced = true
			return fmt.Errorf("registry storage mode changed after open")
		}
		previous = append([]byte(nil), b.Get([]byte("p"+checked.Batch.ProposalID))...)
		return nil
	}); err != nil {
		return false, err
	}
	if len(previous) != 0 {
		if string(previous) != digest {
			return false, fmt.Errorf("proposal identity conflict")
		}
		return false, nil
	}
	changed, committing := false, false
	err = r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(registryBucket)
		batch := checked.Batch
		version := b.Get([]byte("version"))
		if len(version) != 8 {
			return fmt.Errorf("registry version corrupt")
		}
		owner := func(account string) (uint64, bool) { shard, exists := r.ownerCache[account]; return shard, exists }
		if _, err := checkCertificatePrefix(checked, digest, binary.BigEndian.Uint64(version), b.Get([]byte("p"+batch.ProposalID)), owner); err != nil {
			return err
		}
		if resolve != nil {
			var err error
			batch, err = resolve(tx, checked, digest)
			if err != nil {
				return err
			}
		}
		if validate != nil {
			if err := validate(batch, owner); err != nil {
				return err
			}
		}
		committing = true
		if err := persistCertificate(b, checked, digest, r.storageMode); err != nil {
			return err
		}
		if r.beforeCommit != nil {
			if err := r.beforeCommit(); err != nil {
				return err
			}
		}
		changed = true
		return nil
	})
	if err == nil && changed && r.afterCommit != nil {
		err = r.afterCommit()
	}
	if err != nil && committing {
		r.fenced = true
	}
	if err != nil {
		return false, err
	}
	if changed {
		for _, binding := range checked.Batch.Bindings {
			r.ownerCache[binding.Account] = binding.Shard
		}
	}
	return changed, nil
}

// These transaction primitives are shared by Apply and fused AdvanceVote.
// The caller validates the complete transition before persisting any item.
func checkCertificatePrefix(cert Certificate, digest string, version uint64, previous []byte, owner func(string) (uint64, bool)) (bool, error) {
	if len(previous) != 0 {
		if string(previous) != digest {
			return false, fmt.Errorf("proposal identity conflict")
		}
		return false, nil
	}
	if cert.Batch.BaseVersion != version {
		return false, fmt.Errorf("stale registry base version")
	}
	for _, binding := range cert.Batch.Bindings {
		if _, bound := owner(binding.Account); bound {
			return false, fmt.Errorf("first owner already bound: %s", binding.Account)
		}
	}
	return true, nil
}

func persistCertificate(bucket *bolt.Bucket, cert Certificate, digest, mode string) error {
	batch := cert.Batch
	if mode != RegistryCertificateMode {
		for _, binding := range batch.Bindings {
			if err := bucket.Put([]byte("a"+binding.Account), uintBytes(binding.Shard)); err != nil {
				return err
			}
		}
	}
	raw, err := json.Marshal(cert)
	if err != nil {
		return err
	}
	if err := bucket.Put(versionKey(batch.BaseVersion+1), raw); err != nil {
		return err
	}
	if err := bucket.Put([]byte("p"+batch.ProposalID), []byte(digest)); err != nil {
		return err
	}
	return bucket.Put([]byte("version"), uintBytes(batch.BaseVersion+1))
}

// Snapshot replays signed events and reconciles indexes, so a syntactically
// readable but inconsistent DB fails closed. Rollback to an older valid DB
// still requires an external trusted checkpoint to detect.
func (r *Registry) Snapshot(genesis map[string]uint64) (*View, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fenced {
		return nil, fmt.Errorf("registry fenced; close and recover")
	}
	view, err := newView(r.verifier, genesis)
	if err != nil {
		return nil, err
	}
	if err = r.reconcile(view, genesis); err != nil {
		r.fenced = true
		return nil, err
	}
	return view, nil
}

// reconcile is startup/Snapshot-only. The caller owns the lock or has not
// published this Registry; no Vote, Apply or Version path scans this history.
func (r *Registry) reconcile(view *View, genesis map[string]uint64) error {
	return r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(registryBucket)
		if b == nil || !r.verifier.configurationEqual(b.Get([]byte("config"))) || len(b.Get([]byte("version"))) != 8 {
			return fmt.Errorf("registry configuration/counter corrupt")
		}
		mode := b.Get([]byte("mode"))
		if string(mode) != r.storageMode {
			return fmt.Errorf("registry storage mode changed after open")
		}
		delta := mode != nil
		certificateOnly := string(mode) == RegistryCertificateMode
		if delta && string(mode) != registryDeltaMode && !certificateOnly {
			return fmt.Errorf("unknown registry storage mode")
		}
		version := binary.BigEndian.Uint64(b.Get([]byte("version")))
		if version > uint64(b.Stats().KeyN) {
			return fmt.Errorf("registry version corrupt")
		}
		for seq := uint64(1); seq <= version; seq++ {
			var cert Certificate
			if err := json.Unmarshal(b.Get(versionKey(seq)), &cert); err != nil {
				return fmt.Errorf("registry event corrupt: %w", err)
			}
			checked, digest, err := r.verifier.Verify(cert)
			if err != nil {
				return err
			}
			changed, err := view.apply(checked, digest)
			if err != nil || !changed || view.version != seq {
				return fmt.Errorf("registry event chain corrupt")
			}
		}
		count := 0
		if err := b.ForEach(func(key, value []byte) error {
			count++
			if len(key) > 0 && key[0] == 'a' {
				if certificateOnly {
					return fmt.Errorf("certificate registry contains redundant owner index")
				}
				account := string(key[1:])
				if _, historical := genesis[account]; delta && historical {
					return fmt.Errorf("delta registry contains genesis owner")
				}
				owner, ok := view.ownerLocked(account)
				if !ok || len(value) != 8 || binary.BigEndian.Uint64(value) != owner {
					return fmt.Errorf("registry owner index corrupt")
				}
			} else if len(key) > 0 && key[0] == 'p' {
				if view.proposals[string(key[1:])] != string(value) {
					return fmt.Errorf("registry proposal index corrupt")
				}
			} else if string(key) != "config" && string(key) != "version" && !(delta && string(key) == "mode") {
				if len(key) != 9 || key[0] != 'c' || binary.BigEndian.Uint64(key[1:]) == 0 || binary.BigEndian.Uint64(key[1:]) > version {
					return fmt.Errorf("registry unknown record")
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if !certificateOnly {
			if err := view.rangeOwnersLocked(func(account string, owner uint64) error {
				if _, historical := genesis[account]; delta && historical {
					return nil
				}
				value := b.Get([]byte("a" + account))
				if len(value) != 8 || binary.BigEndian.Uint64(value) != owner {
					return fmt.Errorf("registry missing owner")
				}
				return nil
			}); err != nil {
				return err
			}
		}
		for proposal, digest := range view.proposals {
			if string(b.Get([]byte("p"+proposal))) != digest {
				return fmt.Errorf("registry missing proposal")
			}
		}
		expected := 2 + 2*len(view.proposals)
		if delta {
			expected++
		}
		if !certificateOnly {
			expected += view.ownerCountLocked()
			if delta {
				expected -= len(genesis)
			}
		}
		if count != expected {
			return fmt.Errorf("registry extra/missing records")
		}
		return nil
	})
}

func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fenced = true
	return r.db.Close()
}
