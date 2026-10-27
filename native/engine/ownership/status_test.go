package ownership

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestBootstrapStatusUsesLoadedViewNotEnvironment(t *testing.T) {
	f := newCommitteeFixture(t)
	dir := t.TempDir()
	configPath, bundlePath := filepath.Join(dir, "config.json"), filepath.Join(dir, "bundle.json")
	configHash := writePinnedFixture(t, configPath, f.config)
	cert := f.certificate(t, 0, "status", Binding{testAccount(1), 1})
	// Replaying one valid payload does not count as another owner/version.
	bundleHash := writePinnedFixture(t, bundlePath, []Certificate{cert, cert})
	view, err := LoadView(configPath, configHash, bundlePath, bundleHash, f.genesis, f.config.Context.GenesisDigest, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAA_OWNERSHIP_SECURITY_V1", "false")
	t.Setenv("SAA_OWNERSHIP_CONFIG_SHA256", digestBytes([]byte("different-config")))
	t.Setenv("SAA_OWNERSHIP_CERTIFICATES_SHA256", digestBytes([]byte("different-bundle")))
	status := view.Status()
	if !status.Enabled || status.Mode != "pinned_offline_bundle" || status.RegistryVersion != 1 ||
		status.Owners != 2 || status.GenesisOwners != 1 || status.CertifiedOwners != 1 ||
		status.ConfigSHA256 != configHash || status.BundleSHA256 != bundleHash ||
		status.CommitteeDigest != f.verifier.CommitteeDigest() || status.Context != f.config.Context {
		t.Fatalf("incorrect loaded status: %+v", status)
	}
	status.Context.ChainID = "caller-mutated"
	if view.Context() != f.config.Context {
		t.Fatal("status leaked mutable context")
	}
	raw, err := json.Marshal(view.Status())
	if err != nil || !json.Valid(raw) {
		t.Fatal("public status is not JSON encodable")
	}
}

func TestBootstrapStatusDoesNotInventPinnedOrEnabledView(t *testing.T) {
	t.Setenv("SAA_OWNERSHIP_SECURITY_V1", "true")
	var missing *View
	if status := missing.Status(); status.Enabled || status.Mode != "disabled" || status.Owners != 0 {
		t.Fatal("environment invented an enforced view")
	}
	f := newCommitteeFixture(t)
	view, err := BuildView(f.verifier, f.genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	if status := view.Status(); !status.Enabled || status.Mode != "verified_public_view" ||
		status.ConfigSHA256 != "" || status.BundleSHA256 != "" {
		t.Fatal("in-memory verified fixture claimed pinned files")
	}
}
