package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// One opt-in continuous-input, fixed-action native run. Not a policy benchmark.
func TestLivePrepareNativeTiming(t *testing.T) {
	root := os.Getenv("LIVE_PREPARE_RUN_DIR")
	if root == "" {
		t.Skip("explicit native run only")
	}
	a := fmt.Sprintf("%040x", 16)
	b := fmt.Sprintf("%040x", 32)
	c := fmt.Sprintf("%040x", 33)
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, softDecisionMode: saaIntegratedDecisionMode, stableShardByAccount: map[string]uint64{"0x" + a: 0, "0x" + b: 0, "0x" + c: 1}}
	defer saaIntegratedRuntimeStates.Delete(m)
	s := m.integratedStateLocked()
	s.authority.enableInstallationReceiptGate()
	s.nativeSourcePreparedPath = os.Getenv("SAA_NATIVE_SOURCE_PREPARED_PATH")
	s.nativeReceiptPath = os.Getenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH")
	reports := make(chan message.BlockInfoMsg, 4096)
	listener, e := net.Listen("tcp", "127.0.0.1:49880")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	go func() {
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				scan := bufio.NewScanner(conn)
				scan.Buffer(make([]byte, 4096), 16<<20)
				for scan.Scan() {
					line := scan.Bytes()
					prefix := message.MergeMessage(message.CBlockInfo, nil)
					if bytes.HasPrefix(line, prefix) {
						var r message.BlockInfoMsg
						if json.Unmarshal(line[len(prefix):], &r) == nil {
							reports <- r
						}
					}
				}
			}(conn)
		}
	}()
	events, e := os.Create(filepath.Join(root, "timing.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	defer events.Close()
	enc := json.NewEncoder(events)
	start := time.Now()
	mark := func(kind string, v interface{}) {
		enc.Encode(map[string]interface{}{"event": kind, "at": time.Now().UTC(), "elapsed": time.Since(start).Seconds(), "details": v})
		events.Sync()
	}
	success := false
	defer func() {
		raw, _ := json.MarshalIndent(map[string]interface{}{"pass": success, "scope": "2 shards x 4 nodes, continuous synthetic input, fixed action; not 16-shard migration performance", "elapsed": time.Since(start).Seconds()}, "", "  ")
		os.WriteFile(filepath.Join(root, "result.json"), raw, 0600)
	}()
	for sh := 0; sh < 2; sh++ {
		for node := 0; node < 4; node++ {
			f, e := os.Create(filepath.Join(root, fmt.Sprintf("node-%d-%d.log", sh, node)))
			if e != nil {
				t.Fatal(e)
			}
			cmd := exec.Command(filepath.Join(filepath.Dir(root), "candidate"), "-S", "2", "-N", "4", "-s", fmt.Sprint(sh), "-n", fmt.Sprint(node))
			cmd.Dir = root
			cmd.Stdout = f
			cmd.Stderr = f
			if e = cmd.Start(); e != nil {
				t.Fatal(e)
			}
			defer func(cmd *exec.Cmd, f *os.File) {
				cmd.Process.Signal(os.Interrupt)
				done := make(chan error, 1)
				go func() { done <- cmd.Wait() }()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					cmd.Process.Kill()
					<-done
				}
				f.Close()
			}(cmd, f)
		}
	}
	for sh := 0; sh < 2; sh++ {
		for node := 0; node < 4; node++ {
			addr := fmt.Sprintf("127.0.0.1:%d", 44000+sh*10+node)
			ready := false
			for until := time.Now().Add(40 * time.Second); time.Now().Before(until); {
				conn, e := net.DialTimeout("tcp", addr, 200*time.Millisecond)
				if e == nil {
					conn.Close()
					ready = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !ready {
				t.Fatal("node startup failed", addr)
			}
		}
	}
	mark("all_nodes_listening", nil)
	start = time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	expected := map[string]bool{}
	seen := map[string]bool{}
	seenHandoffs := map[string]bool{}
	lastAckScan := time.Time{}
	var seq uint64
	issued := false
	active := false
	hotCount := 0
	firstSeen := map[string]bool{}
	send := func(sh int, txs []*core.Transaction) {
		raw, _ := json.Marshal(message.InjectTxs{Txs: txs})
		conn, e := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", 44000+sh*10), time.Second)
		if e != nil {
			t.Fatal(e)
		}
		defer conn.Close()
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, e = conn.Write(append(message.MergeMessage(message.CInject, raw), '\n')); e != nil {
			t.Fatal(e)
		}
	}
	for time.Since(start) < 150*time.Second {
		select {
		case <-tick.C:
			if time.Since(start) < 45*time.Second {
				batches := [2][]*core.Transaction{}
				for i := 0; i < 440; i++ {
					account := b
					sh := 0
					if i < 80 {
						account = a
						hotCount++
						if active {
							sh = 1
						}
					} else if i >= 400 {
						account = c
						sh = 1
					}
					tx := core.NewTransaction(account, account, big.NewInt(1), seq+1, time.Now().UTC())
					tx.RoutingFrozen = true
					tx.RoutedSenderShard = uint64(sh)
					tx.RoutedRecipientShard = uint64(sh)
					tx.IngressSequence = seq
					seq++
					if active {
						tx.MappingVersion = s.authority.routeVersion
					}
					tx.SenderRequiresExistingState = true
					tx.RecipientRequiresExistingState = true
					expected[hex.EncodeToString(tx.TxHash)] = true
					batches[sh] = append(batches[sh], tx)
				}
				send(0, batches[0])
				send(1, batches[1])
				mark("input_batch", map[string]interface{}{"total": len(expected), "target_active": active})
			}
		case r := <-reports:
			mark("block_report", map[string]interface{}{"shard": r.SenderShardID, "inner": len(r.InnerShardTxs), "relay1": len(r.Relay1Txs), "relay2": len(r.Relay2Txs)})
			for _, tx := range append(r.InnerShardTxs, r.Relay2Txs...) {
				h := hex.EncodeToString(tx.TxHash)
				if expected[h] {
					if seen[h] {
						t.Fatal("duplicate completion", h)
					}
					seen[h] = true
				}
			}
		case <-poll.C:
			if !issued && time.Since(start) >= 20*time.Second {
				d := s.authority.submit(mappingAuthorityProposal{ProposalID: "live-fixed", Addresses: []string{"0x" + a}, SourceShard: 0, TargetShard: 1, EffectiveEpoch: 1}, 0)
				if d.Status != mappingAccepted {
					t.Fatal(d)
				}
				cmd := core.SourcePrepareCommand{ProposalID: "live-fixed", Account: a, SourceShard: 0, TargetShard: 1, BindAtProposal: true}
				if e = m.PublishNativeSourcePreparation(cmd, os.Getenv("SAA_NATIVE_SOURCE_PREPARE_PATH")); e != nil {
					t.Fatal(e)
				}
				issued = true
				mark("intent_published", d)
			}
			if issued {
				m.lock.Lock()
				m.applySAAIntegratedDueLocked(1)
				m.lock.Unlock()
			}
			for key, path := range map[string]string{"prepared": os.Getenv("SAA_NATIVE_SOURCE_PREPARED_PATH"), "installed": os.Getenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH"), "activated": os.Getenv("SAA_NATIVE_TARGET_ACTIVATED_PATH"), "retired": os.Getenv("SAA_NATIVE_SOURCE_RETIRED_PATH")} {
				if !firstSeen[key] {
					if raw, e := os.ReadFile(path); e == nil && len(raw) > 0 {
						firstSeen[key] = true
						mark(key, string(raw))
						if key == "activated" {
							active = true
						}
					}
				}
			}
			if active && time.Since(lastAckScan) > time.Second {
				lastAckScan = time.Now()
				files, _ := filepath.Glob(filepath.Join(os.Getenv("SAA_NATIVE_HANDOFF_ACK_DIR"), "*.ack.json"))
				for _, file := range files {
					raw, e := os.ReadFile(file)
					if e != nil {
						t.Fatal(e)
					}
					var ack core.HandoffAck
					if e = json.Unmarshal(raw, &ack); e != nil {
						t.Fatal(e)
					}
					if seenHandoffs[ack.ID] || ack.RelayPending {
						continue
					}
					raw, e = os.ReadFile(filepath.Join(os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR"), ack.ID+".handoff.json"))
					if e != nil {
						t.Fatal(e)
					}
					var h core.MigrationHandoff
					if e = json.Unmarshal(raw, &h); e != nil {
						t.Fatal(e)
					}
					if e = core.ValidateHandoffAck(&h, ack); e != nil {
						t.Fatal(e)
					}
					tx := core.DecodeTx(h.Original)
					hash := hex.EncodeToString(tx.TxHash)
					if !expected[hash] || seen[hash] {
						t.Fatal("unknown or duplicate handoff completion", hash)
					}
					seen[hash] = true
					seenHandoffs[ack.ID] = true
					mark("handoff_completed", map[string]interface{}{"id": ack.ID, "hash": hash, "block": ack.Block})
				}
			}
			if firstSeen["retired"] && time.Since(start) > 45*time.Second && len(seen) == len(expected) {
				m.lock.Lock()
				e = m.validateNativeRetirementLocked("live-fixed", "0x"+a)
				m.lock.Unlock()
				if e != nil {
					t.Fatal(e)
				}
				mark("all_completed", map[string]interface{}{"input": len(expected), "completed": len(seen), "hot_account_input": hotCount, "route_version": s.authority.routeVersion})
				success = true
				return
			}
		}
	}
	mark("timeout", map[string]interface{}{"input": len(expected), "completed": len(seen), "stages": firstSeen})
	t.Fatal("bounded native run incomplete; do not retry")
}
