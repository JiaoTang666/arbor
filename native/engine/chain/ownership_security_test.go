package chain

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/ownership"
	"blockEmulator/params"
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

const (
	securityOldAccount = "0000000000000000000000000000000000000001"
	securityLocalNew   = "0000000000000000000000000000000000000002"
	securityRemoteNew  = "0000000000000000000000000000000000000003"
	securityOtherNew   = "0000000000000000000000000000000000000004"
)

func securityJSON(t *testing.T, name string, value interface{}) (string, string) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	return path, hex.EncodeToString(digest[:])
}

// Private keys are ephemeral test fixtures. Native startup loads public keys
// and the preauthorized bundle only; this is not a live committee experiment.
func ownershipSecurityFixture(t *testing.T) func(uint64) *BlockChain {
	t.Helper()
	oldDB, oldShards, oldConsensus := params.DatabaseWrite_path, params.ShardNum, params.ConsensusMethod
	t.Cleanup(func() {
		params.DatabaseWrite_path, params.ShardNum, params.ConsensusMethod = oldDB, oldShards, oldConsensus
	})
	params.ShardNum, params.ConsensusMethod = 2, 3
	for _, key := range []string{"BLOCK_EMULATOR_CLPA_EPOCH_V1", "SAA_NATIVE_INITIAL_LEDGER_MIGRATION_V1", "SAA_INTEGRATED_CONTROLLER", "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ADMISSION_ENABLED", "BLOCK_EMULATOR_RUNTIME_UNKNOWN_DYNAMIC_ENABLED", "SAA_FIRST_PLACEMENT_FENCE", externalMappingUpdatePathEnv, runtimeUnknownMappingUpdatePathEnv} {
		t.Setenv(key, "")
	}
	t.Setenv("BLOCK_EMULATOR_COMPLETION_SCHEDULER", "fifo")
	ledger := initialledger.Manifest{Schema: "synthetic_initial_ledger_v1", Shards: 2,
		Accounts: []initialledger.Account{{Address: securityOldAccount, Balance: "100", Shard: 0}}}
	ledgerPath, ledgerHash := securityJSON(t, "ledger.json", ledger)
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", ledgerPath)
	t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", ledgerHash)
	routesDigest, err := ownership.RoutesDigest(map[string]uint64{securityOldAccount: 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	context := ownership.Context{ChainID: "native-security-fixture", Shards: 2, CommitteeEpoch: 7,
		GenesisDigest: ledgerHash, GenesisRoutesDigest: routesDigest, PolicyDigest: strings.Repeat("1", 64),
		SnapshotDigest: strings.Repeat("2", 64), SourceRangeDigest: strings.Repeat("3", 64)}
	config := ownership.Config{Context: context, Faults: 1}
	private := make([]ed25519.PrivateKey, 4)
	for i := range private {
		private[i] = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize))
		config.Members = append(config.Members, ownership.Member{ID: string(rune('a' + i)), PublicKey: private[i].Public().(ed25519.PublicKey)})
	}
	verifier, err := ownership.NewVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	batch := ownership.Batch{Schema: ownership.Schema, Context: context, CommitteeDigest: verifier.CommitteeDigest(), ProposalID: "first-bindings",
		Bindings: []ownership.Binding{{Account: securityLocalNew, Shard: 0}, {Account: securityRemoteNew, Shard: 1}, {Account: securityOtherNew, Shard: 1}}}
	payload, err := ownership.SigningBytes(batch)
	if err != nil {
		t.Fatal(err)
	}
	cert := ownership.Certificate{Batch: batch}
	for i := 0; i < 3; i++ {
		cert.Signatures = append(cert.Signatures, ownership.Signature{MemberID: config.Members[i].ID, Value: ed25519.Sign(private[i], payload)})
	}
	configPath, configHash := securityJSON(t, "committee.json", config)
	bundlePath, bundleHash := securityJSON(t, "certificates.json", []ownership.Certificate{cert})
	t.Setenv("SAA_OWNERSHIP_SECURITY_V1", "true")
	t.Setenv("SAA_OWNERSHIP_CONFIG_PATH", configPath)
	t.Setenv("SAA_OWNERSHIP_CONFIG_SHA256", configHash)
	t.Setenv("SAA_OWNERSHIP_CERTIFICATES_PATH", bundlePath)
	t.Setenv("SAA_OWNERSHIP_CERTIFICATES_SHA256", bundleHash)
	return func(shard uint64) *BlockChain {
		params.DatabaseWrite_path = t.TempDir() + string(filepath.Separator)
		bc, err := NewBlockChain(&params.ChainConfig{ShardID: shard, ShardNums: 2, Nodes_perShard: 4, BlockSize: 4}, rawdb.NewMemoryDatabase())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(bc.CloseBlockChain)
		return bc
	}
}

func requireSecurityPanic(t *testing.T, action func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("unauthorized operation did not fail")
		}
	}()
	action()
}

func TestOwnershipSecurityNativeLocalAndRelayCreation(t *testing.T) {
	open := ownershipSecurityFixture(t)
	source, target := open(0), open(1)
	local := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 0)
	cross := nativeOwnerTx(securityOldAccount, securityRemoteNew, 0, 1, 1)
	originalHash := append([]byte(nil), cross.TxHash...)
	source.SendTx2Pool([]*core.Transaction{local, cross})
	block := source.GenerateBlock(0)
	if _, err := source.ExportAccountState(securityLocalNew); err == nil {
		t.Fatal("uncommitted proposal exposed a new account")
	}
	if err := source.AddBlock(block); err != nil {
		t.Fatal(err)
	}
	if state, err := source.ExportAccountState(securityOldAccount); err != nil || state.State.Balance.Int64() != 98 || state.State.Nonce != 2 {
		t.Fatal("authorized debit semantics changed", state, err)
	}
	if state, err := source.ExportAccountState(securityLocalNew); err != nil || state.State.Balance.Cmp(new(big.Int).Add(params.Init_Balance, big.NewInt(1))) != 0 {
		t.Fatal("authorized local recipient was not created", err)
	}
	if _, err := source.ExportAccountState(securityRemoteNew); err == nil {
		t.Fatal("cross-shard recipient created on source")
	}
	relay := core.DecodeTx(cross.Encode())
	relay.Relayed = true
	target.SendTx2Pool([]*core.Transaction{relay})
	if err := target.AddBlock(target.GenerateBlock(0)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(originalHash, relay.TxHash) {
		t.Fatal("security changed original transaction hash")
	}
	owner, err := target.NativeFirstOwner(securityRemoteNew)
	if err != nil || owner == nil || owner.Shard != 1 || owner.Role != "recipient" {
		t.Fatal("authorized relay creation lacks committed provenance", owner, err)
	}
	if _, err := target.ExportAccountState(securityOldAccount); err == nil {
		t.Fatal("relay sender was spuriously created")
	}
}

func TestOwnershipSecurityRejectsBeforeAdmissionOrState(t *testing.T) {
	open := ownershipSecurityFixture(t)
	bc := open(0)
	valid := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 0, 0)
	mutations := map[string]func(*core.Transaction){
		"wrong_owner":     func(tx *core.Transaction) { tx.RoutedRecipientShard = 1 },
		"unknown_owner":   func(tx *core.Transaction) { tx.Recipient = strings.Repeat("f", 40) },
		"legacy":          func(tx *core.Transaction) { tx.RoutingFrozen = false },
		"broker":          func(tx *core.Transaction) { tx.HasBroker = true },
		"address_alias":   func(tx *core.Transaction) { tx.Recipient = "0x" + tx.Recipient },
		"wrong_stage":     func(tx *core.Transaction) { tx.Relayed = true },
		"mapping_version": func(tx *core.Transaction) { tx.MappingVersion = 1 },
		"nil_value":       func(tx *core.Transaction) { tx.Value = nil },
		"negative_value":  func(tx *core.Transaction) { tx.Value = big.NewInt(-1) },
		"short_tx_hash":   func(tx *core.Transaction) { tx.TxHash = nil },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			bad := core.DecodeTx(valid.Encode())
			mutate(bad)
			root := append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
			requireSecurityPanic(t, func() { bc.SendTx2Pool([]*core.Transaction{valid, bad}) })
			if bc.Txpool.GetTxQueueLen() != 0 {
				t.Fatal("partial unauthorized batch entered queue")
			}
			requireSecurityPanic(t, func() { bc.GetUpdateStatusTrie([]*core.Transaction{valid, bad}) })
			if !bytes.Equal(root, bc.CurrentBlock.Header.StateRoot) {
				t.Fatal("rejection changed committed root")
			}
			if _, err := bc.ExportAccountState(securityLocalNew); err == nil {
				t.Fatal("preflight applied the earlier valid transfer")
			}
		})
	}
}

func TestOwnershipSecurityCachedRootAndQueueBypassRejected(t *testing.T) {
	open := ownershipSecurityFixture(t)
	bc := open(0)
	bad := nativeOwnerTx(securityOldAccount, securityLocalNew, 0, 1, 0)
	root := append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
	header := &core.BlockHeader{Number: 1, ParentBlockHash: bc.CurrentBlock.Hash, StateRoot: root, TxRoot: GetTxTreeRoot([]*core.Transaction{bad})}
	block := core.NewBlock(header, []*core.Transaction{bad})
	block.Hash = header.Hash()
	if bc.IsValidBlock(block) == nil || bc.AddBlock(block) == nil {
		t.Fatal("cached known root bypassed ownership validation")
	}
	if bc.CurrentBlock.Header.Number != 0 || !bytes.Equal(root, bc.CurrentBlock.Header.StateRoot) {
		t.Fatal("rejected cached-root block advanced ledger")
	}
	// Simulate legacy code placing a mutable pointer directly into TxQueue.
	// Even this bypass cannot consume the entry or write a proposal root.
	bc.Txpool.TxQueue = []*core.Transaction{bad}
	requireSecurityPanic(t, func() { bc.GenerateBlock(0) })
	if bc.Txpool.GetTxQueueLen() != 1 || bc.Txpool.TxQueue[0] != bad {
		t.Fatal("rejected proposal consumed pending work")
	}
	st, err := bc.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	before := st.Hash()
	if _, err := bc.applyIndexedRawRelay(st, bad, false); err == nil || st.Hash() != before {
		t.Fatal("direct indexed entry bypassed authorization", err)
	}
	requireSecurityPanic(t, func() { bc.AddAccounts([]string{securityLocalNew}, []*core.AccountState{{Balance: big.NewInt(1)}}, 0) })
	requireSecurityPanic(t, func() { bc.Update_PartitionMap(securityOldAccount, 1) })
	if bc.Get_PartitionMap(securityOldAccount) != 0 {
		t.Fatal("mapping bypass changed first owner")
	}
}

func TestOwnershipSecurityStartupFailuresBeforeGenesis(t *testing.T) {
	ownershipSecurityFixture(t)
	for _, name := range []string{"missing", "hash_mismatch", "unsupported_consensus", "reordered_selection"} {
		t.Run(name, func(t *testing.T) {
			switch name {
			case "missing":
				t.Setenv("SAA_OWNERSHIP_CONFIG_PATH", "")
			case "hash_mismatch":
				t.Setenv("SAA_OWNERSHIP_CERTIFICATES_SHA256", strings.Repeat("0", 64))
			case "unsupported_consensus":
				previous := params.ConsensusMethod
				params.ConsensusMethod = 1
				t.Cleanup(func() { params.ConsensusMethod = previous })
			case "reordered_selection":
				t.Setenv("BLOCK_EMULATOR_COMPLETION_SCHEDULER", "phase_priority")
			}
			params.DatabaseWrite_path = t.TempDir() + string(filepath.Separator)
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			if _, err := NewBlockChain(&params.ChainConfig{ShardNums: 2, BlockSize: 4}, db); err == nil {
				t.Fatal("unsafe security bootstrap accepted")
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
