package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/message"
	"blockEmulator/ownership"
	"blockEmulator/params"
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func ownershipIngressFixture(t *testing.T) (func(uint64) *RawRelayOutsideModule, string, string, string) {
	t.Helper()
	oldDB, oldConsensus := params.DatabaseWrite_path, params.ConsensusMethod
	t.Cleanup(func() { params.DatabaseWrite_path, params.ConsensusMethod = oldDB, oldConsensus })
	params.ConsensusMethod = 3
	t.Setenv("BLOCK_EMULATOR_COMPLETION_SCHEDULER", "fifo")
	t.Setenv("SAA_FIRST_PLACEMENT_FENCE", "")
	t.Setenv(chain.NativeEffectIndexEnv, "false")
	a, b, c := strings.Repeat("1", 40), strings.Repeat("2", 40), strings.Repeat("3", 40)
	write := func(name string, value interface{}) (string, string) {
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
	ledger := initialledger.Manifest{Schema: "synthetic_initial_ledger_v1", Shards: 2,
		Accounts: []initialledger.Account{{Address: a, Balance: "100", Shard: 0}, {Address: b, Balance: "100", Shard: 0}, {Address: c, Balance: "100", Shard: 1}}}
	ledgerPath, ledgerHash := write("ledger.json", ledger)
	routesHash, err := ownership.RoutesDigest(map[string]uint64{a: 0, b: 0, c: 1}, 2)
	if err != nil {
		t.Fatal(err)
	}
	// This fixture checks network admission against pinned genesis owners.
	// Quorum certificates and 3f+1 are covered by the separate ownership suite.
	config := ownership.Config{Context: ownership.Context{ChainID: "ingress-fixture", Shards: 2, GenesisDigest: ledgerHash,
		GenesisRoutesDigest: routesHash, PolicyDigest: strings.Repeat("1", 64), SnapshotDigest: strings.Repeat("2", 64), SourceRangeDigest: strings.Repeat("3", 64)},
		Faults: 1}
	for i := 0; i < 4; i++ {
		public := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(7 + i)}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
		config.Members = append(config.Members, ownership.Member{ID: string(rune('a' + i)), PublicKey: public})
	}
	configPath, configHash := write("committee.json", config)
	bundlePath, bundleHash := write("certificates.json", []ownership.Certificate{})
	for key, value := range map[string]string{"SAA_OWNERSHIP_SECURITY_V1": "true", "SAA_OWNERSHIP_CONFIG_PATH": configPath,
		"SAA_OWNERSHIP_CONFIG_SHA256": configHash, "SAA_OWNERSHIP_CERTIFICATES_PATH": bundlePath, "SAA_OWNERSHIP_CERTIFICATES_SHA256": bundleHash,
		"BLOCK_EMULATOR_INITIAL_LEDGER_PATH": ledgerPath, "BLOCK_EMULATOR_INITIAL_LEDGER_SHA256": ledgerHash} {
		t.Setenv(key, value)
	}
	open := func(shard uint64) *RawRelayOutsideModule {
		params.DatabaseWrite_path = t.TempDir() + string(filepath.Separator)
		bc, err := chain.NewBlockChain(&params.ChainConfig{ShardID: shard, ShardNums: 2, BlockSize: 4}, rawdb.NewMemoryDatabase())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(bc.CloseBlockChain)
		node := &PbftConsensusNode{ShardID: shard, CurChain: bc, seqIDMap: map[uint64]uint64{}, pl: &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}}
		return &RawRelayOutsideModule{pbftNode: node}
	}
	return open, a, b, c
}

func TestOwnershipIngressRejectsWithoutPanicOrProgress(t *testing.T) {
	open, a, b, _ := ownershipIngressFixture(t)
	for _, kind := range []message.MessageType{message.CInject, message.CRelay, message.CRelayWithProof} {
		t.Run(string(kind), func(t *testing.T) {
			module := open(0)
			node := module.pbftNode
			root := append([]byte(nil), node.CurChain.CurrentBlock.Header.StateRoot...)
			bad := core.NewTransaction(a, b, big.NewInt(1), 0, time.Unix(1, 0))
			bad.RoutingFrozen, bad.RoutedSenderShard, bad.RoutedRecipientShard = true, 0, 1
			bad.Relayed = kind != message.CInject
			var envelope interface{}
			switch kind {
			case message.CInject:
				envelope = message.InjectTxs{Txs: []*core.Transaction{bad}, ToShardID: 0}
			case message.CRelay:
				envelope = message.Relay{Txs: []*core.Transaction{bad}, SenderShardID: 1, SenderSeq: 99}
			default:
				envelope = message.RelayWithProof{Txs: []*core.Transaction{bad}, SenderShardID: 1, SenderSeq: 99}
			}
			raw, _ := json.Marshal(envelope)
			for _, payload := range [][]byte{[]byte("{"), raw} {
				// No recover is used: an untrusted transport panic fails the test.
				module.HandleMessageOutsidePBFT(kind, payload)
			}
			if node.CurChain.Txpool.GetTxQueueLen() != 0 || node.hasSeenWorkload.Load() || len(node.seqIDMap) != 0 || len(node.admissionLegSeen) != 0 {
				t.Fatal("rejected transport changed admission or relay progress")
			}
			if node.CurChain.CurrentBlock.Header.Number != 0 || !bytes.Equal(root, node.CurChain.CurrentBlock.Header.StateRoot) {
				t.Fatal("rejected transport changed committed state")
			}
		})
	}
}

func TestOwnershipIngressContinuesAfterRejectedTransport(t *testing.T) {
	open, a, b, c := ownershipIngressFixture(t)
	source, target := open(0), open(1)
	local := core.NewTransaction(a, b, big.NewInt(1), 0, time.Unix(1, 0))
	local.RoutingFrozen, local.RoutedSenderShard, local.RoutedRecipientShard = true, 0, 0
	bad := core.DecodeTx(local.Encode())
	bad.Recipient = strings.Repeat("f", 40)
	raw, _ := json.Marshal(message.InjectTxs{Txs: []*core.Transaction{local, bad}, ToShardID: 0})
	source.HandleMessageOutsidePBFT(message.CInject, raw)
	if source.pbftNode.CurChain.Txpool.GetTxQueueLen() != 0 {
		t.Fatal("mixed unauthorized batch partially admitted")
	}
	raw, _ = json.Marshal(message.InjectTxs{Txs: []*core.Transaction{local}, ToShardID: 0})
	source.HandleMessageOutsidePBFT(message.CInject, raw)
	if source.pbftNode.CurChain.Txpool.GetTxQueueLen() != 1 || !source.pbftNode.hasSeenWorkload.Load() {
		t.Fatal("valid input could not progress after rejection")
	}
	cross := core.NewTransaction(a, c, big.NewInt(1), 1, time.Unix(1, 0))
	cross.RoutingFrozen, cross.RoutedSenderShard, cross.RoutedRecipientShard, cross.Relayed = true, 0, 1, true
	raw, _ = json.Marshal(message.Relay{Txs: []*core.Transaction{cross}, SenderShardID: 0, SenderSeq: 7})
	target.HandleMessageOutsidePBFT(message.CRelay, raw)
	if target.pbftNode.CurChain.Txpool.GetTxQueueLen() != 1 || target.pbftNode.seqIDMap[0] != 7 || !target.pbftNode.hasSeenWorkload.Load() {
		t.Fatal("authorized relay input did not preserve legacy progress")
	}
}
