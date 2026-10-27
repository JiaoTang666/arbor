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

func TestExplicitGenesisThenMigration(t *testing.T) {
	root := t.TempDir()
	raw, _ := json.Marshal(Manifest{Schema: "synthetic_initial_ledger_v1", Shards: 2, Accounts: []Account{{Address: strings.Repeat("0", 39) + "1", Balance: "100", Shard: 0}}})
	p := filepath.Join(root, "ledger.json")
	os.WriteFile(p, raw, 0600)
	h := sha256.Sum256(raw)
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", p)
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", hex.EncodeToString(h[:]))
	t.Setenv("SAA_NATIVE_INITIAL_LEDGER_MIGRATION_V1", "")
	for _, key := range []string{"SAA_NATIVE_SOURCE_QUEUE_ROOT", "SAA_NATIVE_SOURCE_PREPARE_PATH", "SAA_NATIVE_SOURCE_PREPARED_PATH", "SAA_NATIVE_TARGET_INSTALL_PATH", "SAA_NATIVE_INSTALLATION_RECEIPT_PATH", "SAA_NATIVE_TARGET_ACTIVATION_PATH", "SAA_NATIVE_TARGET_ACTIVATED_PATH", "SAA_NATIVE_SOURCE_RETIRED_PATH", "SAA_NATIVE_HANDOFF_OUTBOX_DIR", "SAA_NATIVE_HANDOFF_ACK_DIR", "SAA_NATIVE_HANDOFF_RELAY_DIR"} {
		t.Setenv(key, filepath.Join(root, key))
	}
	if _, e := FromEnv(2); e == nil {
		t.Fatal("default compatibility guard removed")
	}
	t.Setenv("SAA_NATIVE_INITIAL_LEDGER_MIGRATION_V1", "true")
	t.Setenv("SAA_REQUIRE_NATIVE_INSTALLATION_RECEIPT", "false")
	if _, e := FromEnv(2); e == nil {
		t.Fatal("ungated migration accepted")
	}
	t.Setenv("SAA_REQUIRE_NATIVE_INSTALLATION_RECEIPT", "true")
	if _, e := FromEnv(2); e != nil {
		t.Fatal(e)
	}
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", strings.Repeat("0", 64))
	if _, e := FromEnv(2); e == nil {
		t.Fatal("hash guard bypassed")
	}
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", hex.EncodeToString(h[:]))
	t.Setenv("BLOCK_EMULATOR_CLUSTER_SHARD_MAP_PATH", "conflict")
	if _, e := FromEnv(2); e == nil {
		t.Fatal("other layout writer allowed")
	}
	t.Setenv("BLOCK_EMULATOR_CLUSTER_SHARD_MAP_PATH", "")
	os.WriteFile(os.Getenv("SAA_NATIVE_SOURCE_PREPARE_PATH"), []byte("{}"), 0600)
	if _, e := FromEnv(2); e == nil {
		t.Fatal("preexisting command at genesis allowed")
	}
}
