package ownership

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// A reproducible PUBLIC example for ownership_audit. All signing keys stay in
// this test's memory; its output is not a distributed committee transcript.
func TestPublicAuditFixture(t *testing.T) {
	f := newCommitteeFixture(t)
	ledger := map[string]interface{}{
		"schema": "synthetic_initial_ledger_v1", "shards": 2,
		"accounts": []map[string]interface{}{{"account": testAccount(1000), "balance": "100", "nonce": 0, "storage_root": "", "code_hash": "", "shard": 0}},
	}
	ledgerRaw, err := json.Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	f.config.Context.GenesisDigest = digestBytes(ledgerRaw)
	f.verifier, err = NewVerifier(f.config)
	if err != nil {
		t.Fatal(err)
	}
	certs := []Certificate{f.certificate(t, 0, "public-fixture", Binding{testAccount(1), 1})}
	if _, err := BuildView(f.verifier, f.genesis, certs); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if requested := os.Getenv("SECURITY_PUBLIC_FIXTURE_DIR"); requested != "" {
		if !filepath.IsAbs(requested) {
			t.Fatal("fixture destination must be absolute")
		}
		if err := os.MkdirAll(requested, 0700); err != nil {
			t.Fatal(err)
		}
		directory = requested
	}
	manifest := map[string]interface{}{"schema": "coolchain/security-public-fixture-v1", "shards": 2, "private_keys_written": false, "committee_consensus": "NOT_IMPLEMENTED"}
	for name, value := range map[string]interface{}{"ledger.json": ledger, "committee.json": f.config, "certificates.json": certs} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(filepath.Join(directory, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.Write(raw)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatal(writeErr, closeErr)
		}
		manifest[name] = digestBytes(raw)
	}
	raw, _ := json.Marshal(manifest)
	file, err := os.OpenFile(filepath.Join(directory, "manifest.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
