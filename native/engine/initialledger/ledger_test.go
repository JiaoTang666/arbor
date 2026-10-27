package initialledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadFixture(t *testing.T, m Manifest) (*Ledger, error) {
	t.Helper()
	raw, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	p := filepath.Join(t.TempDir(), "ledger.json")
	if e = os.WriteFile(p, raw, 0600); e != nil {
		t.Fatal(e)
	}
	h := sha256.Sum256(raw)
	return Load(p, hex.EncodeToString(h[:]), 2)
}
func TestInitialLedgerValidation(t *testing.T) {
	a := Account{Address: strings.Repeat("0", 39) + "1", Balance: "77", Nonce: 4, Shard: 0}
	m := Manifest{Schema: "synthetic_initial_ledger_v1", Shards: 2, Accounts: []Account{a}}
	one, e := loadFixture(t, m)
	if e != nil {
		t.Fatal(e)
	}
	m.Accounts[0].Shard = 1
	two, e := loadFixture(t, m)
	if e != nil {
		t.Fatal(e)
	}
	if one.LogicalHash != two.LogicalHash || one.FileHash == two.FileHash {
		t.Fatal("placement-independent digest contract")
	}
	for _, tc := range []struct {
		name   string
		change func(*Manifest)
	}{
		{"duplicate", func(m *Manifest) { m.Accounts = append(m.Accounts, m.Accounts[0]) }},
		{"address", func(m *Manifest) { m.Accounts[0].Address = "0x" + a.Address }},
		{"balance", func(m *Manifest) { m.Accounts[0].Balance = "-1" }},
		{"shard", func(m *Manifest) { m.Accounts[0].Shard = 2 }},
		{"root", func(m *Manifest) { m.Accounts[0].StorageRoot = "ff" }},
		{"schema", func(m *Manifest) { m.Schema = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := Manifest{Schema: "synthetic_initial_ledger_v1", Shards: 2, Accounts: []Account{a}}
			tc.change(&m)
			if _, e := loadFixture(t, m); e == nil {
				t.Fatal("invalid ledger accepted")
			}
		})
	}
}
func TestInitialLedgerHashRequired(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ledger.json")
	os.WriteFile(p, []byte(`{}`), 0600)
	for _, h := range []string{"", strings.Repeat("0", 64)} {
		if _, e := Load(p, h, 2); e == nil {
			t.Fatal("missing/wrong hash accepted")
		}
	}
}
