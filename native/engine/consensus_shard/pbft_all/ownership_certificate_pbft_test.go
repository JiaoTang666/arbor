package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/ownership"
	"blockEmulator/params"
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type issuedPacketManifest struct {
	Ledger string `json:"ledger_sha256"`
	Config string `json:"config_sha256"`
	Bundle string `json:"bundle_sha256"`
}

type issuedAccountObservation struct {
	Present bool                    `json:"present"`
	Balance string                  `json:"balance,omitempty"`
	Nonce   uint64                  `json:"nonce"`
	Owner   *chain.NativeFirstOwner `json:"native_first_owner,omitempty"`
}

type issuedNodeObservation struct {
	PID                int                                 `json:"pid"`
	Shard              uint64                              `json:"shard"`
	Node               uint64                              `json:"node"`
	Height             uint64                              `json:"height"`
	Root               string                              `json:"state_root"`
	Block              string                              `json:"block_hash"`
	TxHashes           []string                            `json:"tx_hashes"`
	Queue              int                                 `json:"queue"`
	Accounts           map[string]issuedAccountObservation `json:"accounts"`
	Ownership          ownership.BootstrapStatus           `json:"ownership"`
	EnforcementEnabled bool                                `json:"enforcement_enabled"`
	WorkloadSeen       bool                                `json:"workload_seen"`
	LastWorkloadAt     int64                               `json:"last_workload_at_ms"`
	RelaySequences     map[uint64]uint64                   `json:"relay_sequences"`
	AdmissionSeen      int                                 `json:"admission_seen"`
	SequenceID         uint64                              `json:"sequence_id"`
	PBFTStage          int32                               `json:"pbft_stage"`
}

func issuedAccount(id uint64) string { return fmt.Sprintf("%040x", id) }

func issuedPacketEnvironment(t *testing.T) issuedPacketManifest {
	t.Helper()
	dir := os.Getenv("SECURITY_ISSUED_FIXTURE_DIR")
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != os.Getenv("SECURITY_ISSUED_MANIFEST_SHA256") {
		t.Fatal("issued manifest hash mismatch")
	}
	var manifest issuedPacketManifest
	if err = json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"BLOCK_EMULATOR_INITIAL_LEDGER_PATH":   filepath.Join(dir, "ledger.json"),
		"BLOCK_EMULATOR_INITIAL_LEDGER_SHA256": manifest.Ledger,
		"SAA_OWNERSHIP_SECURITY_V1":            "true",
		"SAA_OWNERSHIP_CONFIG_PATH":            filepath.Join(dir, "committee.json"),
		"SAA_OWNERSHIP_CONFIG_SHA256":          manifest.Config,
		"SAA_OWNERSHIP_CERTIFICATES_PATH":      filepath.Join(dir, "certificates.json"),
		"SAA_OWNERSHIP_CERTIFICATES_SHA256":    manifest.Bundle,
		"BLOCK_EMULATOR_COMPLETION_SCHEDULER":  "fifo",
		"SAA_FIRST_PLACEMENT_FENCE":            "",
	} {
		t.Setenv(key, value)
	}
	ledger, err := initialledger.FromEnv(2)
	if err != nil || ledger == nil {
		t.Fatal("issued ledger unavailable", err)
	}
	view, err := ownership.FromEnv(ledger.Routes, ledger.FileHash, 2)
	if err != nil || view == nil || view.Version() != 2 || view.Check(issuedAccount(1), 1) != nil || view.Check(issuedAccount(2), 0) != nil {
		t.Fatal("issued certificate prefix does not authorize intended owners", err)
	}
	return manifest
}

// A complete file is atomically published; readers never treat partial JSON
// as completed evidence. No private key is loaded by this native harness.
func issuedEvidence(t *testing.T, path string, value interface{}) {
	t.Helper()
	file, err := os.OpenFile(path+".pending", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	encodeErr, syncErr, closeErr := json.NewEncoder(file).Encode(value), file.Sync(), file.Close()
	if encodeErr != nil || syncErr != nil || closeErr != nil {
		t.Fatal("evidence write failed", encodeErr, syncErr, closeErr)
	}
	if err = os.Rename(path+".pending", path); err != nil {
		t.Fatal(err)
	}
}

func issuedObserve(t *testing.T, node *PbftConsensusNode) issuedNodeObservation {
	t.Helper()
	bc := node.CurChain
	r := issuedNodeObservation{PID: os.Getpid(), Shard: node.ShardID, Node: node.NodeID,
		Height: bc.CurrentBlock.Header.Number, Root: hex.EncodeToString(bc.CurrentBlock.Header.StateRoot),
		Block: hex.EncodeToString(bc.CurrentBlock.Hash), Queue: bc.Txpool.GetTxQueueLen(),
		Accounts: map[string]issuedAccountObservation{}, Ownership: bc.OwnershipSecurityStatus(),
		EnforcementEnabled: bc.OwnershipSecurityEnabled(), WorkloadSeen: node.hasSeenWorkload.Load(),
		LastWorkloadAt: node.lastWorkloadAtMs.Load(), AdmissionSeen: len(node.admissionLegSeen),
		SequenceID: node.sequenceID, PBFTStage: node.pbftStage.Load(), RelaySequences: map[uint64]uint64{}}
	node.seqMapLock.Lock()
	for shard, sequence := range node.seqIDMap {
		r.RelaySequences[shard] = sequence
	}
	node.seqMapLock.Unlock()
	for _, tx := range bc.CurrentBlock.Body {
		r.TxHashes = append(r.TxHashes, hex.EncodeToString(tx.TxHash))
	}
	for _, id := range []uint64{1000, 1, 2, 3} {
		account := issuedAccount(id)
		state, err := bc.ExportAccountState(account)
		observation := issuedAccountObservation{}
		if err == nil {
			observation.Present, observation.Balance, observation.Nonce = true, state.State.Balance.String(), state.State.Nonce
		} else if !strings.Contains(err.Error(), "is not present") {
			t.Fatal("state observation failed", err)
		}
		observation.Owner, err = bc.NativeFirstOwner(account)
		if err != nil {
			t.Fatal(err)
		}
		r.Accounts[account] = observation
	}
	return r
}

// The child runs production TCP handling and the natural PBFT proposer. It
// only observes committed state under the commit lock; it never drives Commit.
func TestIssuedCertificatePBFTNode(t *testing.T) {
	rawID := os.Getenv("SECURITY_PBFT_NODE")
	if rawID == "" {
		t.Skip("explicit acceptance child only")
	}
	id, err := strconv.Atoi(rawID)
	if err != nil || id < 0 || id >= 8 {
		t.Fatal("invalid child identity")
	}
	root := os.Getenv("SECURITY_PBFT_RUN_DIR")
	issuedPacketEnvironment(t)
	acceptanceConfig(root)
	params.ConsensusMethod = 3
	if err = json.Unmarshal([]byte(os.Getenv("SECURITY_PBFT_TABLE")), &params.IPmap_nodeTable); err != nil {
		t.Fatal(err)
	}
	s, n := uint64(id/4), uint64(id%4)
	config := &params.ChainConfig{ChainID: s, ShardID: s, NodeID: n, ShardNums: 2, Nodes_perShard: 4, BlockSize: 1, BlockInterval: 100, InjectSpeed: 1}
	node := NewPbftNode(s, n, config, "RawRelay")
	if !node.CurChain.OwnershipSecurityEnabled() {
		t.Fatal("native ownership checks disabled")
	}
	if os.Getenv("SECURITY_PBFT_ATTACK_ISOLATION") == "true" {
		var valid core.Transaction
		if err := json.Unmarshal([]byte(os.Getenv("SECURITY_PBFT_VALID_TX")), &valid); err != nil {
			t.Fatal("invalid acceptance transaction", err)
		}
		installIssuedAttackObserver(t, node, root, id, &valid)
	}
	node.pbftLock.Lock()
	initial := issuedObserve(t, node)
	node.pbftLock.Unlock()
	issuedEvidence(t, filepath.Join(root, fmt.Sprintf("node_%d.initial.json", id)), initial)
	go node.TcpListen()
	go node.Propose()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		node.pbftLock.Lock()
		height := node.CurChain.CurrentBlock.Header.Number
		if height == 1 {
			committed := issuedObserve(t, node)
			node.pbftLock.Unlock()
			issuedEvidence(t, filepath.Join(root, fmt.Sprintf("node_%d.committed.json", id)), committed)
			select {} // Remain available until the parent kills this exact child.
		}
		node.pbftLock.Unlock()
		if height > 1 {
			t.Fatal("missed finite acceptance commit")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("natural PBFT did not commit the accepted transaction")
}

func TestIssuedCertificatePBFTAcceptance(t *testing.T) {
	fixture, root := os.Getenv("SECURITY_ISSUED_FIXTURE_DIR"), os.Getenv("SECURITY_PBFT_RUN_DIR")
	if fixture == "" || root == "" {
		t.Skip("explicit issued-certificate native acceptance only")
	}
	if !filepath.IsAbs(fixture) || !filepath.IsAbs(root) {
		t.Fatal("absolute isolated paths required")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("fresh empty run directory required", err)
	}
	manifest := issuedPacketEnvironment(t)
	acceptanceConfig(root)
	params.ConsensusMethod = 3
	table := map[uint64]map[uint64]string{}
	reservations := []net.Listener{}
	defer func() {
		for _, reservation := range reservations {
			_ = reservation.Close()
		}
	}()
	for s := uint64(0); s < 2; s++ {
		table[s] = map[uint64]string{}
		for n := uint64(0); n < 4; n++ {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			table[s][n] = listener.Addr().String()
			reservations = append(reservations, listener)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	table[params.SupervisorShard] = map[uint64]string{0: listener.Addr().String()}
	tableRaw, _ := json.Marshal(table)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	children, logs := []*exec.Cmd{}, []*os.File{}
	var reportMu sync.Mutex
	var collectorWait sync.WaitGroup
	var cleanupOnce sync.Once
	var collectorErr error
	reports := []*message.BlockInfoMsg{}
	recordCollectorError := func(err error) {
		reportMu.Lock()
		defer reportMu.Unlock()
		if collectorErr == nil {
			collectorErr = err
		}
	}
	cleanup := func() {
		cleanupOnce.Do(func() {
			for _, child := range children {
				_ = child.Process.Kill()
			}
			for _, child := range children {
				_ = child.Wait()
			}
			_ = listener.Close()
			collectorWait.Wait() // Drain complete buffered reports before publishing.
			for _, file := range logs {
				_ = file.Close()
			}
			networks.CloseAllConnInPool()
		})
	}
	defer cleanup()
	collectorWait.Add(1)
	go func() {
		defer collectorWait.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					recordCollectorError(err)
				}
				return
			}
			collectorWait.Add(1)
			go func() {
				defer collectorWait.Done()
				defer connection.Close()
				_ = connection.SetReadDeadline(time.Now().Add(50 * time.Second))
				reader := bufio.NewReader(connection)
				for {
					raw, err := reader.ReadBytes('\n')
					if err != nil {
						if !errors.Is(err, io.EOF) || len(raw) != 0 {
							recordCollectorError(fmt.Errorf("report read incomplete: %w", err))
						}
						return
					}
					if len(raw) < 30 {
						recordCollectorError(fmt.Errorf("short report envelope"))
						return
					}
					kind, body := message.SplitMessage(raw)
					if kind != message.CBlockInfo {
						continue
					}
					var report message.BlockInfoMsg
					if err = json.Unmarshal(body, &report); err != nil {
						recordCollectorError(err)
						return
					}
					reportMu.Lock()
					reports = append(reports, &report)
					reportMu.Unlock()
				}
			}()
		}
	}()
	tx := core.NewTransaction(issuedAccount(1000), issuedAccount(1), big.NewInt(1), 0, time.Now())
	tx.RoutingFrozen, tx.RoutedSenderShard, tx.RoutedRecipientShard, tx.MappingVersion, tx.IngressSequence = true, 0, 1, 0, 0
	tx.IngressSequenceAssigned, tx.AdmissionID = true, "security-network-isolation/original-0"
	validRaw, err := json.Marshal(tx)
	if err != nil {
		t.Fatal(err)
	}
	attackMode := os.Getenv("SECURITY_PBFT_ATTACK_ISOLATION") == "true"
	for id := 0; id < 8; id++ {
		file, err := os.OpenFile(filepath.Join(root, fmt.Sprintf("node_%d.stdout", id)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		logs = append(logs, file)
		child := exec.Command(executable, "-test.run=^TestIssuedCertificatePBFTNode$", "-test.timeout=65s", "-test.v")
		child.Env = append(os.Environ(), fmt.Sprintf("SECURITY_PBFT_NODE=%d", id), "SECURITY_PBFT_TABLE="+string(tableRaw), "SECURITY_PBFT_VALID_TX="+string(validRaw))
		child.Stdout, child.Stderr = file, file
		_ = reservations[id].Close()
		if err = child.Start(); err != nil {
			t.Fatal(err)
		}
		children = append(children, child)
	}
	deadline := time.Now().Add(45 * time.Second)
	wait := func(description string, ready func() bool) {
		for !ready() {
			if time.Now().After(deadline) {
				t.Fatal(description, "timed out; failure artifacts retained; no retry")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	readNodes := func(phase string) ([]issuedNodeObservation, bool) {
		result := []issuedNodeObservation{}
		for id := 0; id < 8; id++ {
			raw, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("node_%d.%s.json", id, phase)))
			if os.IsNotExist(err) {
				return nil, false
			}
			if err != nil {
				t.Fatal(err)
			}
			var node issuedNodeObservation
			if err = json.Unmarshal(raw, &node); err != nil {
				t.Fatal(err)
			}
			if node.PID != children[id].Process.Pid || node.Shard != uint64(id/4) || node.Node != uint64(id%4) {
				t.Fatal("child evidence identity mismatch")
			}
			result = append(result, node)
		}
		return result, true
	}
	var initial, committed []issuedNodeObservation
	wait("initial state", func() bool { var ready bool; initial, ready = readNodes("initial"); return ready })
	for id := 0; id < 8; id++ {
		wait("TCP readiness", func() bool {
			connection, err := net.DialTimeout("tcp", table[uint64(id/4)][uint64(id%4)], 100*time.Millisecond)
			if err != nil {
				return false
			}
			connection.Close()
			return true
		})
	}
	for _, node := range initial {
		if !node.EnforcementEnabled || !node.Ownership.Enabled || node.Ownership.Mode != "pinned_offline_bundle" ||
			node.Ownership.ConfigSHA256 != manifest.Config || node.Ownership.BundleSHA256 != manifest.Bundle ||
			node.Ownership.Context.GenesisDigest != manifest.Ledger || node.Ownership.RegistryVersion != 2 ||
			node.Ownership != initial[0].Ownership {
			t.Fatal("native process did not enforce the exact pinned public view", node)
		}
		if node.Height != 0 || node.Queue != 0 || node.Accounts[issuedAccount(1)].Present || node.Accounts[issuedAccount(2)].Present || node.Accounts[issuedAccount(1)].Owner != nil || node.Accounts[issuedAccount(2)].Owner != nil {
			t.Fatal("authorization created state before native execution")
		}
		old := node.Accounts[issuedAccount(1000)]
		if node.Root != initial[int(node.Shard)*4].Root || (node.Shard == 0 && (!old.Present || old.Balance != "100" || old.Nonce != 0)) || (node.Shard == 1 && old.Present) {
			t.Fatal("pinned genesis state mismatch", node)
		}
	}
	var rejected []issuedRejectedObservation
	if attackMode {
		for id := 0; id < 8; id++ {
			for _, attack := range issuedNetworkAttacks(t, uint64(id/4), tx) {
				connection, err := net.DialTimeout("tcp", table[uint64(id/4)][uint64(id%4)], time.Second)
				if err != nil {
					t.Fatal(err)
				}
				_ = connection.SetWriteDeadline(time.Now().Add(time.Second))
				wire := append(message.MergeMessage(attack.Kind, attack.Body), '\n')
				written, err := connection.Write(wire)
				connection.Close()
				if err != nil || written != len(wire) {
					t.Fatal("attack network write incomplete", id, attack.Name, written, err)
				}
			}
		}
		wait("production attack callbacks and unchanged states", func() bool {
			rejected = nil
			for id := 0; id < 8; id++ {
				raw, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("node_%d.rejected.json", id)))
				if os.IsNotExist(err) {
					return false
				}
				if err != nil {
					t.Fatal(err)
				}
				var event issuedRejectedObservation
				if err := json.Unmarshal(raw, &event); err != nil {
					t.Fatal(err)
				}
				observation, before := event.Observation, initial[id]
				expectedCases := issuedNetworkAttacks(t, uint64(id/4), tx)
				if observation.PID != children[id].Process.Pid || observation.Shard != before.Shard || observation.Node != before.Node ||
					observation.Height != before.Height || observation.Root != before.Root || observation.Block != before.Block || observation.Queue != 0 ||
					observation.WorkloadSeen || observation.LastWorkloadAt != before.LastWorkloadAt || len(observation.RelaySequences) != 0 || observation.AdmissionSeen != 0 ||
					observation.SequenceID != before.SequenceID || observation.PBFTStage != before.PBFTStage ||
					!observation.EnforcementEnabled || observation.Ownership != before.Ownership ||
					event.TransportCallbacksCompleted != len(expectedCases) || len(event.Cases) != len(expectedCases) {
					t.Fatal("rejected traffic changed state, progress, authorization or callback coverage", id, event)
				}
				beforeAccounts, _ := json.Marshal(before.Accounts)
				afterAccounts, _ := json.Marshal(observation.Accounts)
				if string(beforeAccounts) != string(afterAccounts) {
					t.Fatal("rejected traffic changed actual account state", id)
				}
				for index, attack := range expectedCases {
					digest := sha256.Sum256(attack.Body)
					completed := event.Cases[index]
					if completed.Name != attack.Name || completed.Kind != attack.Kind || completed.BodySHA256 != hex.EncodeToString(digest[:]) {
						t.Fatal("callback did not match the exact TCP attack payload", id, index)
					}
				}
				rejected = append(rejected, event)
			}
			return true
		})
	}
	input, _ := json.Marshal(message.InjectTxs{Txs: []*core.Transaction{tx}, ToShardID: 0})
	connection, err := net.DialTimeout("tcp", table[0][0], time.Second)
	if err != nil {
		t.Fatal(err)
	}
	connection.SetWriteDeadline(time.Now().Add(time.Second))
	payload := append(message.MergeMessage(message.CInject, input), '\n')
	written, err := connection.Write(payload)
	connection.Close()
	if err != nil || written != len(payload) {
		t.Fatal("injection write incomplete", written, err)
	}
	wait("committed states", func() bool { var ready bool; committed, ready = readNodes("committed"); return ready })
	wait("natural Relay reports", func() bool {
		reportMu.Lock()
		defer reportMu.Unlock()
		if collectorErr != nil {
			t.Fatal("natural report collector failed", collectorErr)
		}
		return len(reports) >= 2
	})
	cleanup()
	if collectorErr != nil {
		t.Fatal("natural report collection failed during shutdown", collectorErr)
	}
	txHash := hex.EncodeToString(tx.TxHash)
	for id, node := range committed {
		if node.Height != 1 || node.Queue != 0 || node.Root != committed[(id/4)*4].Root || node.Block != committed[(id/4)*4].Block || len(node.TxHashes) != 1 || node.TxHashes[0] != txHash || node.Accounts[issuedAccount(2)].Present || node.Accounts[issuedAccount(2)].Owner != nil || node.Accounts[issuedAccount(3)].Present || node.Accounts[issuedAccount(3)].Owner != nil {
			t.Fatal("replica commit or untouched authorization mismatch", node)
		}
		old, created := node.Accounts[issuedAccount(1000)], node.Accounts[issuedAccount(1)]
		if node.Shard == 0 {
			if !old.Present || old.Balance != "99" || old.Nonce != 1 || created.Present || created.Owner != nil {
				t.Fatal("source debit semantics mismatch", node)
			}
		} else {
			want := new(big.Int).Add(params.Init_Balance, big.NewInt(1)).String()
			if old.Present || !created.Present || created.Balance != want || created.Nonce != 0 || created.Owner == nil || created.Owner.Shard != 1 || created.Owner.TxHash != txHash || created.Owner.CreationHeight != 1 || created.Owner.Role != "recipient" || created.Owner.MappingVersion != 0 || created.Owner.IngressSequence != 0 {
				t.Fatal("target authorized creation mismatch", node)
			}
		}
	}
	first, second := 0, 0
	for _, report := range reports {
		if report.SenderShardID >= 2 || report.Epoch != 1 || report.CommittedStateRoot != committed[int(report.SenderShardID)*4].Root || report.CommittedBlockHash != committed[int(report.SenderShardID)*4].Block {
			t.Fatal("reported commit does not match actual replica state")
		}
		for _, item := range report.Relay1Txs {
			if report.SenderShardID == 0 && hex.EncodeToString(item.TxHash) == txHash {
				first++
			}
		}
		for _, item := range report.Relay2Txs {
			if report.SenderShardID == 1 && hex.EncodeToString(item.TxHash) == txHash {
				second++
			}
		}
	}
	if first != 1 || second != 1 {
		t.Fatal("natural source/target reports do not match one original transaction", first, second)
	}
	issuedEvidence(t, filepath.Join(root, "result.json"), map[string]interface{}{
		"status": "PASS", "scope": "issued fixed-committee certificate to natural native PBFT Relay creation",
		"topology": "2 shards x 4 real OS processes", "manifest": manifest, "registry_version": 2,
		"mapping_version": 0, "transaction": tx, "initial": initial, "committed": committed, "reports": reports,
		"block_size": 1, "block_interval_ms": 100, "original_transactions_completed": 1,
		"attack_isolation_enabled": attackMode, "rejected": rejected,
		"not_covered": []string{"online certificate issuance", "Byzantine PBFT message authentication", "Relay debit proof", "payment authorization", "performance", "migration"},
	})
}
