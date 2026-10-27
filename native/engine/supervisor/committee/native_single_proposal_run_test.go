package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"github.com/boltdb/bolt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Explicit opt-in orchestration of real node processes. Manual proposal, no
// model and no fabricated receipts. Not part of ordinary regression runs.
func TestNativeSingleProposalRun(t *testing.T) {
	root := os.Getenv("SAA_NATIVE_RUN_DIR")
	if root == "" {
		t.Skip("explicit native run only")
	}
	db, err := bolt.Open(filepath.Join(root, "exp/database/chainDB/S0_N0"), 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var head *core.Block
	err = db.View(func(tx *bolt.Tx) error {
		hash := tx.Bucket([]byte("newestBlockHash")).Get([]byte("OnlyNewestBlock"))
		head = core.DecodeB(tx.Bucket([]byte("block")).Get(hash))
		return nil
	})
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "state_observations"))
	if err != nil {
		t.Fatal(err)
	}
	var stateHash string
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(root, "state_observations", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var s struct {
			StateRoot string `json:"state_root"`
			StateHash string `json:"state_hash"`
		}
		if json.Unmarshal(raw, &s) == nil && s.StateRoot == hex.EncodeToString(head.Header.StateRoot) {
			stateHash = s.StateHash
		}
	}
	if stateHash == "" {
		var previous core.SourcePrepareCommand
		raw, readErr := os.ReadFile(filepath.Join(root, "control", "source_prepare.json"))
		if readErr != nil || json.Unmarshal(raw, &previous) != nil {
			t.Fatal("no observation or retained preparation identity")
		}
		stateHash = previous.ExpectedStateHash
	}
	for env, file := range map[string]string{"SAA_NATIVE_SOURCE_PREPARE_PATH": "source_prepare.json", "SAA_NATIVE_SOURCE_PREPARED_PATH": "source_prepared.json", "SAA_NATIVE_TARGET_INSTALL_PATH": "target_install.json", "SAA_NATIVE_INSTALLATION_RECEIPT_PATH": "install_receipts.jsonl", "SAA_NATIVE_TARGET_ACTIVATION_PATH": "target_activation.json", "SAA_NATIVE_TARGET_ACTIVATED_PATH": "target_activated.json", "SAA_NATIVE_SOURCE_RETIRED_PATH": "source_retired.json"} {
		t.Setenv(env, filepath.Join(root, "control", file))
	}
	t.Setenv("SAA_INTEGRATED_CONTROLLER", "legacy")
	account := "0000000000000000000000000000000000000010"
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, softDecisionMode: saaIntegratedDecisionMode, stableShardByAccount: map[string]uint64{"0x" + account: 0}}
	s := m.integratedStateLocked()
	s.authority.enableInstallationReceiptGate()
	s.nativeSourcePreparedPath = os.Getenv("SAA_NATIVE_SOURCE_PREPARED_PATH")
	s.nativeReceiptPath = os.Getenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH")
	if d := s.authority.submit(mappingAuthorityProposal{ProposalID: "native-one", Addresses: []string{"0x" + account}, SourceShard: 0, TargetShard: 1, EffectiveEpoch: int(head.Header.Number)}, int(head.Header.Number)-1); d.Status != mappingAccepted {
		t.Fatal(d)
	}
	c := core.SourcePrepareCommand{ProposalID: "native-one", Account: account, SourceShard: 0, TargetShard: 1, ExpectedBlockHash: hex.EncodeToString(head.Hash), ExpectedStateRoot: hex.EncodeToString(head.Header.StateRoot), ExpectedStateHash: stateHash}
	if saved, readErr := os.ReadFile(os.Getenv("SAA_NATIVE_SOURCE_PREPARE_PATH")); readErr == nil {
		if err = json.Unmarshal(saved, &c); err != nil {
			t.Fatal(err)
		}
	}
	if err = m.PublishNativeSourcePreparation(c, os.Getenv("SAA_NATIVE_SOURCE_PREPARE_PATH")); err != nil {
		t.Fatal(err)
	}
	// Receive actual block reports; this narrow harness is not the workload supervisor.
	listener, err := net.Listen("tcp", "127.0.0.1:49800")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	committed := make(chan message.BlockInfoMsg, 64)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				scanner := bufio.NewScanner(conn)
				scanner.Buffer(make([]byte, 4096), 4<<20)
				for scanner.Scan() {
					line := scanner.Bytes()
					prefix := message.MergeMessage(message.CBlockInfo, nil)
					if !bytes.HasPrefix(line, prefix) {
						continue
					}
					var report message.BlockInfoMsg
					if json.Unmarshal(line[len(prefix):], &report) == nil {
						select {
						case committed <- report:
						default:
						}
					}
				}
			}()
		}
	}()
	var expectedTx []byte
	// Registered before node cleanup: disk verification runs after both stop.
	t.Cleanup(func() {
		if len(expectedTx) != 0 {
			verifyNativePostRouteDisk(t, root, expectedTx)
		}
	})
	for _, shard := range []string{"0", "1"} {
		logFile, err := os.Create(filepath.Join(root, "logs", "migration_s"+shard+".log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("bash", "run_node.sh", shard)
		cmd.Dir = root
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			conn, err := net.DialTimeout("tcp", map[string]string{"0": "127.0.0.1:43117", "1": "127.0.0.1:43118"}[shard], time.Second)
			if err == nil {
				conn.SetWriteDeadline(time.Now().Add(time.Second))
				conn.Write(append(message.MergeMessage(message.CStop, []byte("native harness complete")), '\n'))
				conn.Close()
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				cmd.Process.Kill()
				<-done
			}
			logFile.Close()
		})
	}
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		m.lock.Lock()
		m.applySAAIntegratedDueLocked(int(head.Header.Number))
		m.lock.Unlock()
		if _, err := os.Stat(os.Getenv("SAA_NATIVE_SOURCE_RETIRED_PATH")); err == nil {
			m.lock.Lock()
			err = m.validateNativeRetirementLocked("native-one", "0x"+account)
			m.lock.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("NATIVE retirement witness accepted proposal=native-one route_version=%d state_hash=%s", s.authority.routeVersion, stateHash)
			// Last semantic check: one transaction on the authority's new route.
			var conn net.Conn
			for until := time.Now().Add(10 * time.Second); time.Now().Before(until); {
				conn, err = net.DialTimeout("tcp", "127.0.0.1:43118", time.Second)
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if err != nil {
				t.Fatal(err)
			}
			tx := core.NewTransaction(account, account, big.NewInt(1), 2, time.Now().UTC())
			expectedTx = append([]byte(nil), tx.TxHash...)
			tx.RoutingFrozen = true
			tx.RoutedSenderShard = s.authority.active["0x"+account]
			tx.RoutedRecipientShard = tx.RoutedSenderShard
			tx.MappingVersion = s.authority.routeVersion
			tx.IngressSequence = 2
			tx.SenderRequiresExistingState = true
			tx.RecipientRequiresExistingState = true
			payload, _ := json.Marshal(message.InjectTxs{Txs: []*core.Transaction{tx}})
			conn.SetWriteDeadline(time.Now().Add(time.Second))
			_, err = conn.Write(append(message.MergeMessage(message.CInject, payload), '\n'))
			conn.Close()
			if err != nil {
				t.Fatal(err)
			}
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()
			for {
				select {
				case report := <-committed:
					if report.SenderShardID != 1 {
						continue
					}
					for _, included := range report.InnerShardTxs {
						if bytes.Equal(included.TxHash, expectedTx) {
							t.Logf("commit report received hash=%x; PASS still requires disk verification", expectedTx)
							return
						}
					}
				case <-timer.C:
					t.Log("commit report timeout; stopped-node disk evidence is authoritative")
					return
				}
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("native migration timeout; inspect migration_s0.log and migration_s1.log; no automatic retry")
}
