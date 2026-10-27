package committee

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitialLedgerSupervisorConfiguration(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 2
	t.Cleanup(func() { params.ShardNum = old })
	raw, _ := json.Marshal(initialledger.Manifest{Schema: "synthetic_initial_ledger_v1", Shards: 2, Accounts: []initialledger.Account{{Address: strings.Repeat("0", 39) + "1", Balance: "77", Shard: 0}}})
	path := filepath.Join(t.TempDir(), "ledger.json")
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256(raw)
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", path)
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", hex.EncodeToString(hash[:]))
	t.Setenv("BLOCK_EMULATOR_RUNTIME_UNKNOWN_DYNAMIC_ENABLED", "false")
	t.Setenv("BLOCK_EMULATOR_RUNTIME_UNKNOWN_ADMISSION_ENABLED", "false")
	m := NewRelayCommitteeModule(nil, nil, nil, "", 0, 0)
	if m.initialLedger == nil || m.initialLedger.FileHash != hex.EncodeToString(hash[:]) {
		t.Fatal("supervisor manifest not loaded")
	}
	t.Setenv("BLOCK_EMULATOR_ACCOUNT_CLUSTER_MAP_PATH", "conflicting-layout.csv")
	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "initial ledger conflicts") {
			t.Fatal("conflicting layout not rejected")
		}
	}()
	NewRelayCommitteeModule(nil, nil, nil, "", 0, 0)
}

func TestInitialLedgerFixedRoute(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 2
	t.Cleanup(func() { params.ShardNum = old })
	a, b := strings.Repeat("0", 39)+"1", strings.Repeat("0", 39)+"2"
	m := &RelayCommitteeModule{admission: &RuntimeUnknownAdmissionManager{shardNum: 2}, initialLedger: &initialledger.Ledger{Routes: map[string]uint64{a: 0, b: 1}}}
	f := newFirstAdmissionRuntime(m)
	tx := &core.Transaction{Sender: a, Recipient: b}
	f.bindFixedRouteLocked(tx, 1)
	if tx.RoutedSenderShard != 0 || tx.RoutedRecipientShard != 1 || !tx.SenderRequiresExistingState || !tx.RecipientRequiresExistingState {
		t.Fatal("manifest route/state guard not preserved")
	}
	unknown := strings.Repeat("0", 38) + "03"
	tx = &core.Transaction{Sender: a, Recipient: unknown}
	f.bindFixedRouteLocked(tx, 1)
	if tx.RoutedRecipientShard != 1 || tx.RecipientRequiresExistingState {
		t.Fatal("unknown account behavior changed")
	}
}
