package chain

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/ownership"
	"blockEmulator/params"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

// This opt-in test uses public certificates from the earlier member-process
// acceptance. It neither creates signing keys nor issues a replacement vote.
type issuedPacketManifest struct {
	Schema           string `json:"schema"`
	Shards           uint64 `json:"shards"`
	RegistryVersion  uint64 `json:"registry_version"`
	CertifiedAccount string `json:"certified_account"`
	CertifiedOwner   uint64 `json:"certified_owner"`
	UnusedAccount    string `json:"unused_certified_account"`
	SourceReportHash string `json:"source_report_sha256"`
	LedgerHash       string `json:"ledger_sha256"`
	ConfigHash       string `json:"config_sha256"`
	BundleHash       string `json:"bundle_sha256"`
	MinorityHash     string `json:"minority_sha256"`
}

func issuedPacket(t *testing.T) (issuedPacketManifest, []ownership.Certificate, ownership.Certificate) {
	t.Helper()
	dir, pin := os.Getenv("SECURITY_ISSUED_FIXTURE_DIR"), os.Getenv("SECURITY_ISSUED_MANIFEST_SHA256")
	if dir == "" || pin == "" {
		t.Skip("requires pinned public member-process packet")
	}
	var m issuedPacketManifest
	if err := ownership.ReadPinnedJSON(filepath.Join(dir, "manifest.json"), pin, &m, 1<<20); err != nil {
		t.Fatal(err)
	}
	if m.Schema != "coolchain/issued-native-packet-v1" || m.Shards != 2 || m.RegistryVersion != 2 || m.CertifiedOwner != 1 {
		t.Fatal("unexpected issued packet identity")
	}
	var certs []ownership.Certificate
	var minority ownership.Certificate
	if err := ownership.ReadPinnedJSON(filepath.Join(dir, "certificates.json"), m.BundleHash, &certs, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := ownership.ReadPinnedJSON(filepath.Join(dir, "minority.json"), m.MinorityHash, &minority, 1<<20); err != nil {
		t.Fatal(err)
	}
	oldDB, oldShards, oldConsensus := params.DatabaseWrite_path, params.ShardNum, params.ConsensusMethod
	t.Cleanup(func() {
		params.DatabaseWrite_path, params.ShardNum, params.ConsensusMethod = oldDB, oldShards, oldConsensus
	})
	params.ShardNum, params.ConsensusMethod = 2, 3
	for _, key := range []string{"BLOCK_EMULATOR_CLPA_EPOCH_V1", "SAA_NATIVE_INITIAL_LEDGER_MIGRATION_V1", "SAA_INTEGRATED_CONTROLLER", "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ADMISSION_ENABLED", "BLOCK_EMULATOR_RUNTIME_UNKNOWN_DYNAMIC_ENABLED", "SAA_FIRST_PLACEMENT_FENCE", externalMappingUpdatePathEnv, runtimeUnknownMappingUpdatePathEnv} {
		t.Setenv(key, "")
	}
	t.Setenv("BLOCK_EMULATOR_COMPLETION_SCHEDULER", "fifo")
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", filepath.Join(dir, "ledger.json"))
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", m.LedgerHash)
	t.Setenv("SAA_OWNERSHIP_SECURITY_V1", "true")
	t.Setenv("SAA_OWNERSHIP_CONFIG_PATH", filepath.Join(dir, "committee.json"))
	t.Setenv("SAA_OWNERSHIP_CONFIG_SHA256", m.ConfigHash)
	t.Setenv("SAA_OWNERSHIP_CERTIFICATES_PATH", filepath.Join(dir, "certificates.json"))
	t.Setenv("SAA_OWNERSHIP_CERTIFICATES_SHA256", m.BundleHash)
	return m, certs, minority
}

func openIssuedChain(t *testing.T, shard uint64) *BlockChain {
	t.Helper()
	params.DatabaseWrite_path = t.TempDir() + string(filepath.Separator)
	bc, err := NewBlockChain(&params.ChainConfig{ShardID: shard, ShardNums: 2, Nodes_perShard: 4, BlockSize: 4}, rawdb.NewMemoryDatabase())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bc.CloseBlockChain)
	return bc
}

func TestIssuedCertificateNativeRejection(t *testing.T) {
	m, _, _ := issuedPacket(t)
	for shard := uint64(0); shard < 2; shard++ {
		bc := openIssuedChain(t, shard)
		if !bc.OwnershipSecurityEnabled() || bc.ownership.Version() != m.RegistryVersion {
			t.Fatal("native did not load issued authorization view")
		}
		for _, account := range []string{m.CertifiedAccount, m.UnusedAccount} {
			if _, err := bc.ExportAccountState(account); err == nil {
				t.Fatal("certificate alone created account state", account, shard)
			}
			if owner, err := bc.NativeFirstOwner(account); err != nil || owner != nil {
				t.Fatal("certificate alone created execution provenance", owner, err)
			}
		}
		if shard != 0 {
			continue
		}
		old := "00000000000000000000000000000000000003e8"
		valid := nativeOwnerTx(old, m.CertifiedAccount, 0, 1, 0)
		bad := core.DecodeTx(valid.Encode())
		bad.RoutedRecipientShard = 0
		root := append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
		if err := bc.TrySendAuthorizedTxs([]*core.Transaction{valid, bad}); err == nil {
			t.Fatal("wrong owner admitted after a valid prefix")
		}
		if bc.Txpool.GetTxQueueLen() != 0 || bc.CurrentBlock.Header.Number != 0 || !bytes.Equal(root, bc.CurrentBlock.Header.StateRoot) {
			t.Fatal("rejected admission changed queue or committed state")
		}
		header := &core.BlockHeader{Number: 1, ParentBlockHash: bc.CurrentBlock.Hash, StateRoot: root, TxRoot: GetTxTreeRoot([]*core.Transaction{bad})}
		block := core.NewBlock(header, []*core.Transaction{bad})
		block.Hash = header.Hash()
		if bc.IsValidBlock(block) == nil || bc.AddBlock(block) == nil {
			t.Fatal("cached state root bypassed issued authorization")
		}
		if bc.CurrentBlock.Header.Number != 0 || !bytes.Equal(root, bc.CurrentBlock.Header.StateRoot) {
			t.Fatal("invalid block advanced state")
		}
		state, err := bc.ExportAccountState(old)
		if err != nil || state.State.Balance.Int64() != 100 || state.State.Nonce != 0 {
			t.Fatal("rejection changed genesis sender", state, err)
		}
	}
}

func TestIssuedCertificateBadBundleBeforeGenesis(t *testing.T) {
	_, certs, minority := issuedPacket(t)
	for _, name := range []string{"tampered_signed_owner", "minority_conflicting_owner"} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(certs)
			if err != nil {
				t.Fatal(err)
			}
			var changed []ownership.Certificate
			if err := json.Unmarshal(raw, &changed); err != nil {
				t.Fatal(err)
			}
			if name == "tampered_signed_owner" {
				changed[0].Batch.Bindings[0].Shard = 0
			} else {
				changed = append(changed, minority)
			}
			// Pin the real mutated bytes: rejection must not be just a stale hash.
			path, hash := securityJSON(t, "invalid-certificates.json", changed)
			t.Setenv("SAA_OWNERSHIP_CERTIFICATES_PATH", path)
			t.Setenv("SAA_OWNERSHIP_CERTIFICATES_SHA256", hash)
			params.DatabaseWrite_path = t.TempDir() + string(filepath.Separator)
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			if _, err := NewBlockChain(&params.ChainConfig{ShardNums: 2, BlockSize: 4}, db); err == nil {
				t.Fatal("invalid signed bundle accepted")
			} else {
				want := "invalid, unknown or duplicate signature"
				if name == "minority_conflicting_owner" {
					want = "insufficient or excessive signatures"
				}
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("unexpected bootstrap rejection: want %q, got %v", want, err)
				}
				t.Logf("reject %s: %v", name, err)
			}
			if marked, _ := db.Has([]byte(initialledger.Marker)); marked {
				t.Fatal("failed bootstrap wrote genesis marker")
			}
			if _, err := os.Stat(filepath.Join(params.DatabaseWrite_path, "chainDB")); !os.IsNotExist(err) {
				t.Fatal("failed bootstrap opened block storage", err)
			}
		})
	}
}
