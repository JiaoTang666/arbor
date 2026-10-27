package committee

import (
	"blockEmulator/initialledger"
	"blockEmulator/ownership"
	"blockEmulator/params"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
)

func securedPlacementFixture(t *testing.T, bindings []ownership.Binding) *RelayCommitteeModule {
	t.Helper()
	previous := params.ShardNum
	params.ShardNum = 4
	t.Cleanup(func() { params.ShardNum = previous })
	t.Setenv("SAA_NEWNEW_COMPENSATION_V1", "false")
	genesis := map[string]uint64{fmt.Sprintf("%040x", 1): 0, fmt.Sprintf("%040x", 2): 1}
	routesHash, err := ownership.RoutesDigest(genesis, 4)
	if err != nil {
		t.Fatal(err)
	}
	d := strings.Repeat("a", 64)
	cfg := ownership.Config{Faults: 1, Context: ownership.Context{ChainID: "placement-security-test", Shards: 4, GenesisDigest: d, GenesisRoutesDigest: routesHash, PolicyDigest: d, SnapshotDigest: d, SourceRangeDigest: d}}
	keys := map[string]ed25519.PrivateKey{}
	for i := 0; i < 4; i++ {
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("member-%d", i)
		cfg.Members = append(cfg.Members, ownership.Member{ID: id, PublicKey: pub})
		keys[id] = key
	}
	v, err := ownership.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	certs := []ownership.Certificate{}
	if len(bindings) != 0 {
		batch := ownership.Batch{Schema: ownership.Schema, Context: cfg.Context, CommitteeDigest: v.CommitteeDigest(), ProposalID: "test-batch", Bindings: bindings}
		raw, err := ownership.SigningBytes(batch)
		if err != nil {
			t.Fatal(err)
		}
		cert := ownership.Certificate{Batch: batch}
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("member-%d", i)
			cert.Signatures = append(cert.Signatures, ownership.Signature{MemberID: id, Value: ed25519.Sign(keys[id], raw)})
		}
		certs = append(certs, cert)
	}
	view, err := ownership.BuildView(v, genesis, certs)
	if err != nil {
		t.Fatal(err)
	}
	return &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: genesis, FileHash: d}, nativeRoutes: map[string]uint64{}, ownership: view, nativeBatch: true, offeredLoadTPS: 2000}
}

func TestSecurityPlacementAuthorizedPolicyUnchanged(t *testing.T) {
	a, b := fmt.Sprintf("%040x", 3), fmt.Sprintf("%040x", 4)
	m := securedPlacementFixture(t, []ownership.Binding{{Account: a, Shard: 3}, {Account: b, Shard: 3}})
	if source, target := m.nativePlacement(a, b); source != 3 || target != 3 {
		t.Fatal("authorized policy was altered")
	}
	if len(m.nativeRoutes) != 2 {
		t.Fatal("bindings missing")
	}
	if source, target := m.nativePlacement(a, b); source != 3 || target != 3 {
		t.Fatal("authorized owner changed")
	}
}

func TestSecurityPlacementRejectsBeforeEitherBinding(t *testing.T) {
	a, b := fmt.Sprintf("%040x", 3), fmt.Sprintf("%040x", 4)
	for _, bindings := range [][]ownership.Binding{
		{{Account: a, Shard: 3}},
		{{Account: a, Shard: 3}, {Account: b, Shard: 2}},
	} {
		t.Run(fmt.Sprintf("bindings-%d", len(bindings)), func(t *testing.T) {
			m := securedPlacementFixture(t, bindings)
			func() {
				defer func() {
					if recover() == nil {
						t.Error("unauthorized placement accepted")
					}
				}()
				m.nativePlacement(a, b)
			}()
			if len(m.nativeRoutes) != 0 {
				t.Fatal("partial unauthorized route published")
			}
		})
	}
}

func TestSecurityPlacementDetectsExistingRouteTampering(t *testing.T) {
	a, b := fmt.Sprintf("%040x", 3), fmt.Sprintf("%040x", 4)
	m := securedPlacementFixture(t, []ownership.Binding{{Account: a, Shard: 3}, {Account: b, Shard: 3}})
	m.nativePlacement(a, b)
	m.nativeRoutes[a] = 0
	defer func() {
		if recover() == nil {
			t.Fatal("tampered existing route accepted")
		}
	}()
	m.nativePlacement(a, b)
}
