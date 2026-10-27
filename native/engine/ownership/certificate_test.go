package ownership

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Private signing keys exist only in these fixtures, never in the runtime API.
type committeeFixture struct {
	config   Config
	verifier *Verifier
	private  map[string]ed25519.PrivateKey
	genesis  map[string]uint64
}

func testAccount(id uint64) string { return fmt.Sprintf("%040x", id) }

func newCommitteeFixture(t *testing.T) *committeeFixture {
	t.Helper()
	f := &committeeFixture{
		private: map[string]ed25519.PrivateKey{},
		genesis: map[string]uint64{testAccount(1000): 0},
	}
	routes, err := RoutesDigest(f.genesis, 2)
	if err != nil {
		t.Fatal(err)
	}
	f.config = Config{Faults: 1, Context: Context{
		ChainID: "ownership-contract-test", CommitteeEpoch: 7, Shards: 2,
		GenesisDigest:       digestBytes([]byte("logical-genesis-state")),
		GenesisRoutesDigest: routes,
		PolicyDigest:        digestBytes([]byte("policy")),
		SnapshotDigest:      digestBytes([]byte("snapshot")),
		SourceRangeDigest:   digestBytes([]byte("contiguous-committed-ranges")),
	}}
	for i := 0; i < 4; i++ {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("member-%d", i)
		f.config.Members = append(f.config.Members, Member{ID: id, PublicKey: public})
		f.private[id] = private
	}
	f.verifier, err = NewVerifier(f.config)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *committeeFixture) sign(t *testing.T, batch Batch, members ...string) Certificate {
	t.Helper()
	if len(members) == 0 {
		members = []string{"member-0", "member-1", "member-2"}
	}
	raw, err := SigningBytes(batch)
	if err != nil {
		t.Fatal(err)
	}
	cert := Certificate{Batch: batch}
	for _, member := range members {
		private := f.private[member]
		if private == nil {
			t.Fatalf("missing test signing key %q", member)
		}
		cert.Signatures = append(cert.Signatures, Signature{MemberID: member, Value: ed25519.Sign(private, raw)})
	}
	return cert
}

func (f *committeeFixture) certificate(t *testing.T, base uint64, proposal string, bindings ...Binding) Certificate {
	t.Helper()
	bindings = append([]Binding(nil), bindings...)
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Account < bindings[j].Account })
	return f.sign(t, Batch{
		Schema: Schema, Context: f.config.Context,
		CommitteeDigest: f.verifier.CommitteeDigest(),
		BaseVersion:     base, ProposalID: proposal, Bindings: bindings,
	})
}

func cloneCertificate(t *testing.T, cert Certificate) Certificate {
	t.Helper()
	raw, err := json.Marshal(cert)
	if err != nil {
		t.Fatal(err)
	}
	var cloned Certificate
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func cloneConfig(t *testing.T, config Config) Config {
	t.Helper()
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var cloned Config
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func TestCertificateIndependentQuorumAndOwnedCopies(t *testing.T) {
	f := newCommitteeFixture(t)
	cert := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	checked, digest, err := f.verifier.Verify(cert)
	if err != nil || !validDigest(digest) {
		t.Fatalf("valid three-of-four certificate: digest=%q err=%v", digest, err)
	}
	checked.Batch.Bindings[0].Shard = 0
	checked.Signatures[0].Value[0] ^= 1
	f.config.Members[0].PublicKey[0] ^= 1
	f.config.Members[0].ID = "caller-mutated"
	if _, _, err := f.verifier.Verify(cert); err != nil {
		t.Fatalf("caller mutation changed pinned verifier/original certificate: %v", err)
	}
}

func TestVerifierRejectsAliasedOrInvalidCommittee(t *testing.T) {
	f := newCommitteeFixture(t)
	cases := []struct {
		name string
		edit func(*Config)
	}{
		{"same-key-different-ID", func(c *Config) { c.Members[1].PublicKey = append([]byte(nil), c.Members[0].PublicKey...) }},
		{"duplicate-ID", func(c *Config) { c.Members[1].ID = c.Members[0].ID }},
		{"wrong-key-size", func(c *Config) { c.Members[1].PublicKey = []byte{1} }},
		{"wrong-fault-count", func(c *Config) { c.Faults = 0 }},
		{"zero-shards", func(c *Config) { c.Context.Shards = 0 }},
		{"noncanonical-digest", func(c *Config) { c.Context.PolicyDigest = strings.ToUpper(c.Context.PolicyDigest) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := cloneConfig(t, f.config)
			tc.edit(&config)
			if _, err := NewVerifier(config); err == nil {
				t.Fatal("invalid committee accepted")
			}
		})
	}
}

func TestCertificateRejectsTamperedSignedContext(t *testing.T) {
	f := newCommitteeFixture(t)
	cert := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	cases := []struct {
		name string
		edit func(*Certificate)
	}{
		{"schema", func(c *Certificate) { c.Batch.Schema = "another-domain" }},
		{"chain", func(c *Certificate) { c.Batch.Context.ChainID = "other-chain" }},
		{"epoch", func(c *Certificate) { c.Batch.Context.CommitteeEpoch++ }},
		{"topology", func(c *Certificate) { c.Batch.Context.Shards++ }},
		{"genesis", func(c *Certificate) { c.Batch.Context.GenesisDigest = digestBytes([]byte("other-genesis")) }},
		{"genesis-routes", func(c *Certificate) { c.Batch.Context.GenesisRoutesDigest = digestBytes([]byte("other-routes")) }},
		{"policy", func(c *Certificate) { c.Batch.Context.PolicyDigest = digestBytes([]byte("other-policy")) }},
		{"snapshot", func(c *Certificate) { c.Batch.Context.SnapshotDigest = digestBytes([]byte("other-snapshot")) }},
		{"range", func(c *Certificate) { c.Batch.Context.SourceRangeDigest = digestBytes([]byte("other-range")) }},
		{"committee", func(c *Certificate) { c.Batch.CommitteeDigest = digestBytes([]byte("other-committee")) }},
		{"base-version", func(c *Certificate) { c.Batch.BaseVersion++ }},
		{"proposal", func(c *Certificate) { c.Batch.ProposalID = "other-proposal" }},
		{"account", func(c *Certificate) { c.Batch.Bindings[0].Account = testAccount(2) }},
		{"target", func(c *Certificate) { c.Batch.Bindings[0].Shard = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mutated := cloneCertificate(t, cert)
			tc.edit(&mutated)
			if _, _, err := f.verifier.Verify(mutated); err == nil {
				t.Fatal("tampered signed payload accepted")
			}
		})
	}
}

func TestCertificateRejectsInvalidQuorum(t *testing.T) {
	f := newCommitteeFixture(t)
	cert := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	cases := []struct {
		name string
		edit func(*Certificate)
	}{
		{"insufficient", func(c *Certificate) { c.Signatures = c.Signatures[:2] }},
		{"duplicate", func(c *Certificate) { c.Signatures[2] = c.Signatures[1] }},
		{"unknown", func(c *Certificate) { c.Signatures[2].MemberID = "outside-member" }},
		{"tampered-signature", func(c *Certificate) { c.Signatures[2].Value[0] ^= 1 }},
		{"short-signature", func(c *Certificate) { c.Signatures[2].Value = []byte{1} }},
		{"wrong-signing-domain", func(c *Certificate) {
			raw, err := json.Marshal(c.Batch)
			if err != nil {
				t.Fatal(err)
			}
			for i := range c.Signatures {
				c.Signatures[i].Value = ed25519.Sign(f.private[c.Signatures[i].MemberID], raw)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mutated := cloneCertificate(t, cert)
			tc.edit(&mutated)
			if _, _, err := f.verifier.Verify(mutated); err == nil {
				t.Fatal("invalid quorum accepted")
			}
		})
	}
}

func TestCertificateRequiresCanonicalUniqueSortedBindings(t *testing.T) {
	f := newCommitteeFixture(t)
	base := f.certificate(t, 0, "first", Binding{testAccount(1), 1}).Batch
	cases := []struct {
		name     string
		bindings []Binding
	}{
		{"duplicate", []Binding{{testAccount(1), 1}, {testAccount(1), 1}}},
		{"prefix-alias", []Binding{{"0x" + testAccount(1), 1}}},
		{"uppercase", []Binding{{strings.Repeat("A", 40), 1}}},
		{"short", []Binding{{"01", 1}}},
		{"unsorted", []Binding{{testAccount(2), 1}, {testAccount(1), 1}}},
		{"invalid-target", []Binding{{testAccount(1), 2}}},
		{"empty", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batch := base
			batch.Bindings = tc.bindings
			if _, _, err := f.verifier.Verify(f.sign(t, batch)); err == nil {
				t.Fatal("invalid signed binding accepted")
			}
		})
	}
	if got, err := NormalizeAccount(" 0X" + strings.Repeat("A", 40) + " "); err != nil || got != strings.Repeat("a", 40) {
		t.Fatalf("boundary normalization: %q %v", got, err)
	}
}

func writePinnedFixture(t *testing.T, path string, value interface{}) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return digestBytes(raw)
}

func TestPinnedJSONRejectsHashUnknownTrailingAndOversize(t *testing.T) {
	f := newCommitteeFixture(t)
	raw, err := json.Marshal(f.config)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		raw       []byte
		wrongHash bool
		limit     int64
	}{
		{"wrong-hash", raw, true, 1 << 20},
		{"unknown-field", append(append([]byte(nil), raw[:len(raw)-1]...), []byte(",\"unknown_authority\":true}")...), false, 1 << 20},
		{"trailing-json", append(append([]byte(nil), raw...), []byte(" {}")...), false, 1 << 20},
		{"oversize", raw, false, int64(len(raw) - 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, tc.raw, 0600); err != nil {
				t.Fatal(err)
			}
			hash := digestBytes(tc.raw)
			if tc.wrongHash {
				hash = digestBytes([]byte("not-this-file"))
			}
			var output Config
			if err := ReadPinnedJSON(path, hash, &output, tc.limit); err == nil {
				t.Fatal("untrusted JSON accepted")
			}
		})
	}
}

func TestLoadViewPinsGenesisTopologyAndAuthorizedOwners(t *testing.T) {
	f := newCommitteeFixture(t)
	dir := t.TempDir()
	configPath, bundlePath := filepath.Join(dir, "config.json"), filepath.Join(dir, "bundle.json")
	configHash := writePinnedFixture(t, configPath, f.config)
	bundleHash := writePinnedFixture(t, bundlePath, []Certificate{f.certificate(t, 0, "first", Binding{testAccount(1), 1})})
	view, err := LoadView(configPath, configHash, bundlePath, bundleHash, f.genesis, f.config.Context.GenesisDigest, 2)
	if err != nil {
		t.Fatal(err)
	}
	if view.Version() != 1 || view.Check("0x"+testAccount(1), 1) != nil {
		t.Fatal("authorized owner not loaded")
	}
	if view.Check(testAccount(1), 0) == nil || view.Check(testAccount(2), 1) == nil {
		t.Fatal("unauthorized owner accepted")
	}
	if _, err := LoadView(configPath, configHash, bundlePath, bundleHash, f.genesis, digestBytes([]byte("other-genesis")), 2); err == nil {
		t.Fatal("wrong native genesis accepted")
	}
	if _, err := LoadView(configPath, configHash, bundlePath, bundleHash, f.genesis, f.config.Context.GenesisDigest, 4); err == nil {
		t.Fatal("wrong native topology accepted")
	}
	if _, err := LoadView(configPath, configHash, bundlePath, bundleHash, map[string]uint64{}, f.config.Context.GenesisDigest, 2); err == nil {
		t.Fatal("missing genesis owners accepted")
	}
}
