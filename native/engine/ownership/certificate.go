// Package ownership authenticates first-owner assignments. Quorum signatures
// are NOT a PBFT commit certificate. A fixed-committee signer provides durable
// slot locking; Byzantine liveness, secure transport and rollover remain outside.
package ownership

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const Schema = "coolchain/first-owner-v1"
const CompactAuthorizationSchema = "coolchain/first-owner-compact-v2"
const MaxBindings = 4096 // Certificate size bound, not a transaction input cap.

type Context struct {
	ChainID             string `json:"chain_id"`
	CommitteeEpoch      uint64 `json:"committee_epoch"`
	Shards              uint64 `json:"shards"`
	GenesisDigest       string `json:"genesis_digest"`
	GenesisRoutesDigest string `json:"genesis_routes_digest"`
	PolicyDigest        string `json:"policy_digest"`
	SnapshotDigest      string `json:"snapshot_digest"`
	SourceRangeDigest   string `json:"source_range_digest"`
	AuthorizationSchema string `json:"authorization_schema,omitempty"`
}

type Member struct {
	ID        string `json:"id"`
	PublicKey []byte `json:"public_key"`
}

type Config struct {
	Context Context  `json:"context"`
	Faults  uint64   `json:"faults"`
	Members []Member `json:"members"`
}

type Binding struct {
	Account string `json:"account"`
	Shard   uint64 `json:"shard"`
}

type Batch struct {
	Schema          string    `json:"schema"`
	Context         Context   `json:"context"`
	CommitteeDigest string    `json:"committee_digest"`
	BaseVersion     uint64    `json:"base_version"`
	ProposalID      string    `json:"proposal_id"`
	Bindings        []Binding `json:"bindings"`
	// Runtime evidence is a causal, signed dependency, not a routing version.
	// Omitting these fields preserves the byte identity of v1 offline batches.
	EvidenceVersion        uint64            `json:"evidence_version,omitempty"`
	EvidenceDigest         string            `json:"evidence_digest,omitempty"`
	EvidenceCutoff         uint64            `json:"evidence_cutoff,omitempty"`
	AdmissionStart         uint64            `json:"admission_start,omitempty"`
	AdmissionEnd           uint64            `json:"admission_end,omitempty"`
	PlacementWitness       *PlacementWitness `json:"placement_witness,omitempty"`
	PlacementWitnessDigest string            `json:"placement_witness_digest,omitempty"`
}

type Signature struct {
	MemberID string `json:"member_id"`
	Value    []byte `json:"value"`
}

type Certificate struct {
	Batch      Batch       `json:"batch"`
	Signatures []Signature `json:"signatures"`
}

// Verifier owns a copy of its pinned public configuration. It has no private
// keys, signing method, transaction admission rule or mutable route table.
type Verifier struct {
	config Config
	keys   map[string]ed25519.PublicKey
	digest string
	quorum int
}

func digestBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validDigest(s string) bool {
	raw, err := hex.DecodeString(s)
	return err == nil && len(raw) == sha256.Size && s == strings.ToLower(s)
}

func canonicalAccount(s string) bool {
	raw, err := hex.DecodeString(s)
	return err == nil && len(raw) == 20 && s == strings.ToLower(s)
}

// NormalizeAccount accepts native/Python address spelling at the boundary.
// Signed batches themselves require the exact lowercase 40-hex representation.
func NormalizeAccount(s string) (string, error) {
	s = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(s)), "0x")
	if !canonicalAccount(s) {
		return "", fmt.Errorf("invalid account")
	}
	return s, nil
}

func NewVerifier(config Config) (*Verifier, error) {
	if len(config.Members) > 128 || len(config.Context.ChainID) > 128 {
		return nil, fmt.Errorf("oversized committee")
	}
	for _, m := range config.Members {
		if len(m.ID) > 128 || len(m.PublicKey) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("oversized member")
		}
	}
	// JSON roundtrip also removes ownership of caller-supplied key slices.
	raw, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	var pinned Config
	if err := json.Unmarshal(raw, &pinned); err != nil {
		return nil, err
	}
	c := pinned.Context
	if c.AuthorizationSchema != "" && c.AuthorizationSchema != CompactAuthorizationSchema {
		return nil, fmt.Errorf("unsupported authorization schema")
	}
	if c.ChainID == "" || len(c.ChainID) > 128 || c.Shards == 0 || len(pinned.Members) > 128 || pinned.Faults == 0 || pinned.Faults > 42 || uint64(len(pinned.Members)) != 3*pinned.Faults+1 {
		return nil, fmt.Errorf("invalid committee/topology")
	}
	for _, d := range []string{c.GenesisDigest, c.GenesisRoutesDigest, c.PolicyDigest, c.SnapshotDigest, c.SourceRangeDigest} {
		if !validDigest(d) {
			return nil, fmt.Errorf("invalid context digest")
		}
	}
	sort.Slice(pinned.Members, func(i, j int) bool { return pinned.Members[i].ID < pinned.Members[j].ID })
	v := &Verifier{config: pinned, keys: map[string]ed25519.PublicKey{}, quorum: int(2*pinned.Faults + 1)}
	seenKeys := map[string]bool{}
	for _, m := range pinned.Members {
		if m.ID == "" || len(m.ID) > 128 || len(m.PublicKey) != ed25519.PublicKeySize || v.keys[m.ID] != nil || seenKeys[string(m.PublicKey)] {
			return nil, fmt.Errorf("invalid or aliased member")
		}
		v.keys[m.ID] = append(ed25519.PublicKey(nil), m.PublicKey...)
		seenKeys[string(m.PublicKey)] = true
	}
	canonical, _ := json.Marshal(pinned)
	v.digest = digestBytes(canonical)
	return v, nil
}

func (v *Verifier) Context() Context        { return v.config.Context }
func (v *Verifier) CommitteeDigest() string { return v.digest }

// SigningBytes is public, deterministic and domain-separated. No float, map,
// wall clock, signature order or account alias participates in the payload.
func SigningBytes(batch Batch) ([]byte, error) {
	domain := "coolchain:first-owner:authorization:v1\x00"
	switch batch.Context.AuthorizationSchema {
	case "":
		if batch.PlacementWitnessDigest != "" {
			return nil, fmt.Errorf("compact witness digest in legacy authorization")
		}
	case CompactAuthorizationSchema:
		if !validDigest(batch.PlacementWitnessDigest) {
			return nil, fmt.Errorf("compact authorization requires a witness digest")
		}
		batch.PlacementWitness = nil
		domain = "coolchain:first-owner:authorization:v2\x00"
	default:
		return nil, fmt.Errorf("unsupported authorization schema")
	}
	raw, err := json.Marshal(batch)
	if err != nil {
		return nil, err
	}
	payload := append([]byte(domain), raw...)
	digest := sha256.Sum256(payload)
	return digest[:], nil
}

func RoutesDigest(routes map[string]uint64, shards uint64) (string, error) {
	bindings := make([]Binding, 0, len(routes))
	for account, shard := range routes {
		if !canonicalAccount(account) || shard >= shards {
			return "", fmt.Errorf("invalid genesis binding")
		}
		bindings = append(bindings, Binding{account, shard})
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Account < bindings[j].Account })
	raw, _ := json.Marshal(bindings)
	return digestBytes(raw), nil
}

// Verify copies the untrusted object before verification and returns that copy.
// Concurrent mutation of an input by its owner is still a caller data race.
func (v *Verifier) Verify(cert Certificate) (Certificate, string, error) {
	if len(cert.Batch.Bindings) == 0 || len(cert.Batch.Bindings) > MaxBindings || len(cert.Signatures) > len(v.keys) {
		return Certificate{}, "", fmt.Errorf("certificate size mismatch")
	}
	if cert.Batch.Context != v.config.Context || cert.Batch.Schema != Schema || len(cert.Batch.ProposalID) > 128 || len(cert.Batch.CommitteeDigest) != 64 {
		return Certificate{}, "", fmt.Errorf("certificate context/size mismatch")
	}
	for _, binding := range cert.Batch.Bindings {
		if len(binding.Account) != 40 {
			return Certificate{}, "", fmt.Errorf("oversized account")
		}
	}
	if witness := cert.Batch.PlacementWitness; witness != nil {
		if len(witness.Transactions) == 0 || len(witness.Transactions) > MaxPlacementTransactions || len(witness.Schema) > 64 || len(witness.InputDigest) != 64 {
			return Certificate{}, "", fmt.Errorf("oversized placement witness")
		}
		for _, transaction := range witness.Transactions {
			if len(transaction.OccurrenceID) > 128 || len(transaction.Sender) != 40 || len(transaction.Recipient) != 40 {
				return Certificate{}, "", fmt.Errorf("oversized placement input")
			}
		}
	}
	for _, sig := range cert.Signatures {
		if len(sig.MemberID) > 128 || len(sig.Value) != ed25519.SignatureSize {
			return Certificate{}, "", fmt.Errorf("oversized signature")
		}
	}
	// Clone only owned slices; a JSON roundtrip used to encode and decode the
	// full witness before checking and hashing it again.
	checked := Certificate{Batch: cloneBatch(cert.Batch), Signatures: make([]Signature, len(cert.Signatures))}
	for i, signature := range cert.Signatures {
		checked.Signatures[i] = Signature{MemberID: signature.MemberID, Value: append([]byte(nil), signature.Value...)}
	}
	b, digest, err := v.checkedOwnedBatch(checked.Batch)
	if err != nil {
		return Certificate{}, "", err
	}
	checked.Batch = b
	if len(checked.Signatures) < v.quorum || len(checked.Signatures) > len(v.keys) {
		return Certificate{}, "", fmt.Errorf("insufficient or excessive signatures")
	}
	message, _ := hex.DecodeString(digest)
	seen := map[string]bool{}
	for _, sig := range checked.Signatures {
		key := v.keys[sig.MemberID]
		if key == nil || seen[sig.MemberID] || !ed25519.Verify(key, message, sig.Value) {
			return Certificate{}, "", fmt.Errorf("invalid, unknown or duplicate signature")
		}
		seen[sig.MemberID] = true
	}
	return checked, digest, nil
}

// checkedBatch validates a proposal without inventing a quorum certificate.
// Both signers and certificate verification use the same payload contract.
func (v *Verifier) checkedBatch(batch Batch) (Batch, string, error) {
	if len(batch.Bindings) > MaxBindings || batch.PlacementWitness != nil && len(batch.PlacementWitness.Transactions) > MaxPlacementTransactions {
		return Batch{}, "", fmt.Errorf("proposal exceeds size bound")
	}
	return v.checkedOwnedBatch(cloneBatch(batch))
}

func cloneBatch(batch Batch) Batch {
	batch.Bindings = append([]Binding(nil), batch.Bindings...)
	if batch.PlacementWitness != nil {
		witness := *batch.PlacementWitness
		witness.Transactions = append([]PlacementTransaction(nil), witness.Transactions...)
		batch.PlacementWitness = &witness
	}
	return batch
}

// checkedOwnedBatch may borrow its argument only inside the verification call.
// Public callers always enter through the copying wrappers above.
func (v *Verifier) checkedOwnedBatch(batch Batch) (Batch, string, error) {
	if batch.Context != v.config.Context || batch.Schema != Schema ||
		batch.CommitteeDigest != v.digest || batch.ProposalID == "" || len(batch.ProposalID) > 128 ||
		batch.BaseVersion == ^uint64(0) || len(batch.Bindings) == 0 || len(batch.Bindings) > MaxBindings {
		return Batch{}, "", fmt.Errorf("proposal context/shape mismatch")
	}
	if batch.EvidenceVersion != 0 || batch.EvidenceDigest != "" || batch.EvidenceCutoff != 0 || batch.AdmissionStart != 0 || batch.AdmissionEnd != 0 {
		if batch.EvidenceVersion == 0 || !validDigest(batch.EvidenceDigest) || batch.EvidenceCutoff > batch.AdmissionStart || batch.AdmissionStart >= batch.AdmissionEnd {
			return Batch{}, "", fmt.Errorf("invalid runtime evidence or admission prefix")
		}
	}
	previous := ""
	for _, binding := range batch.Bindings {
		if len(binding.Account) != 40 || !canonicalAccount(binding.Account) || binding.Account <= previous || binding.Shard >= batch.Context.Shards {
			return Batch{}, "", fmt.Errorf("bindings must be unique, canonical and sorted")
		}
		previous = binding.Account
	}
	if batch.Context.AuthorizationSchema == CompactAuthorizationSchema {
		if !validDigest(batch.PlacementWitnessDigest) || batch.EvidenceVersion == 0 {
			return Batch{}, "", fmt.Errorf("compact authorization requires causal witness commitment")
		}
	} else if batch.PlacementWitnessDigest != "" {
		return Batch{}, "", fmt.Errorf("legacy authorization cannot contain a compact witness digest")
	}
	if batch.PlacementWitness != nil {
		if err := batch.PlacementWitness.checkShape(batch); err != nil {
			return Batch{}, "", err
		}
		if batch.Context.AuthorizationSchema == CompactAuthorizationSchema && PlacementWitnessDigest(batch.PlacementWitness) != batch.PlacementWitnessDigest {
			return Batch{}, "", fmt.Errorf("placement witness commitment mismatch")
		}
	}
	message, err := SigningBytes(batch)
	if err != nil {
		return Batch{}, "", err
	}
	return batch, hex.EncodeToString(message), nil
}

// ConfigurationEqual is used when reopening a registry, including its genesis
// and fixed committee. This version deliberately has no committee rollover.
func (v *Verifier) configurationEqual(raw []byte) bool {
	expected, _ := json.Marshal(v.config)
	return bytes.Equal(expected, raw)
}
