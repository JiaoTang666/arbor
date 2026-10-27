package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// One opt-in, quiescent-boundary semantic experiment. Real node workload and
// actual checkpoint. Synthetic workload, not prediction/performance evidence.
func TestNativeSAAOneDecision(t *testing.T) {
	root, project := os.Getenv("SAA_ONE_RUN_DIR"), os.Getenv("SAA_PROTOCOL_TEST_PROJECT")
	if root == "" || project == "" {
		t.Skip("explicit native SAA experiment only")
	}
	write := func(name string, v interface{}) {
		b, e := json.MarshalIndent(v, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(root, name), append(b, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
	checkpoint := filepath.Join(project, "evidence/v35_clockfix_20260906/relation_model_v2_seed43/relation_model.pt")
	raw, e := os.ReadFile(checkpoint)
	if e != nil {
		t.Fatal(e)
	}
	checkpointHash := sha256.Sum256(raw)
	for key, value := range map[string]string{
		integratedControllerEnv: forecastControllerMode, "SAA_FORECAST_STATE_COST_SCOPE": "committed_payload_v1",
		"SAA_REQUIRE_NATIVE_INSTALLATION_RECEIPT": "true", "SAA_FORECAST_STREAM_ID": "native-saa-one",
		"SAA_FORECAST_STREAM_PATH": filepath.Join(root, "routed.jsonl"), "SAA_FORECAST_ANCHOR_PATH": filepath.Join(root, "model/anchor_runtime.jsonl"),
		"SAA_FORECAST_CHECKPOINT_SHA256": hex.EncodeToString(checkpointHash[:]),
		"SAA_FORECAST_CONTEXT_PATH":      filepath.Join(root, "contexts.jsonl"), "SAA_FORECAST_DECISION_PATH": filepath.Join(root, "decision/authority_requests.jsonl"),
	} {
		t.Setenv(key, value)
	}
	for env, file := range map[string]string{"SAA_NATIVE_SOURCE_PREPARE_PATH": "source_prepare.json", "SAA_NATIVE_SOURCE_PREPARED_PATH": "source_prepared.json", "SAA_NATIVE_TARGET_INSTALL_PATH": "target_install.json", "SAA_NATIVE_INSTALLATION_RECEIPT_PATH": "install_receipts.jsonl", "SAA_NATIVE_TARGET_ACTIVATION_PATH": "target_activation.json", "SAA_NATIVE_TARGET_ACTIVATED_PATH": "target_activated.json", "SAA_NATIVE_SOURCE_RETIRED_PATH": "source_retired.json"} {
		t.Setenv(env, filepath.Join(root, "control", file))
	}
	a := fmt.Sprintf("%040x", 16)
	routes := map[string]uint64{"0x" + a: 0}
	assignments := "account,cluster_id,is_noise\n0x" + a + ",0,0\n"
	for i := 0; i < 256; i++ {
		b := fmt.Sprintf("%040x", 1001+2*i)
		routes["0x"+b] = 1
		assignments += "0x" + b + ",1,0\n"
	}
	if e = os.WriteFile(filepath.Join(root, "assignments.csv"), []byte(assignments), 0600); e != nil {
		t.Fatal(e)
	}
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2, stableShardByAccount: routes, stableShardByCluster: map[int]uint64{0: 0, 1: 1}}
	defer saaIntegratedRuntimeStates.Delete(m)
	s := m.integratedStateLocked()
	reports := make(chan message.BlockInfoMsg, 512)
	listener, e := net.Listen("tcp", "127.0.0.1:49800")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	reportFile, e := os.OpenFile(filepath.Join(root, "block_reports.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer reportFile.Close()
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				scan := bufio.NewScanner(c)
				scan.Buffer(make([]byte, 4096), 16<<20)
				for scan.Scan() {
					prefix := message.MergeMessage(message.CBlockInfo, nil)
					line := scan.Bytes()
					if !bytes.HasPrefix(line, prefix) {
						continue
					}
					var r message.BlockInfoMsg
					if json.Unmarshal(line[len(prefix):], &r) == nil {
						reports <- r
					}
				}
			}()
		}
	}()
	send := func(shard uint64, kind message.MessageType, payload []byte) error {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", 43117+shard), time.Second)
		if err != nil {
			return err
		}
		defer c.Close()
		c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		wire := append(message.MergeMessage(kind, payload), '\n')
		for len(wire) > 0 {
			n, err := c.Write(wire)
			if err != nil {
				return err
			}
			wire = wire[n:]
		}
		return nil
	}
	var commands []*exec.Cmd
	var logs []*os.File
	stop := func() {
		for shard := range commands {
			_ = send(uint64(shard), message.CStop, []byte("bounded SAA phase complete"))
		}
		for i, c := range commands {
			done := make(chan error, 1)
			go func() { done <- c.Wait() }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				c.Process.Kill()
				<-done
			}
			logs[i].Close()
		}
		commands = nil
		logs = nil
	}
	defer stop()
	start := func(phase string) {
		for shard := 0; shard < 2; shard++ {
			f, err := os.OpenFile(filepath.Join(root, "logs", fmt.Sprintf("%s_s%d.log", phase, shard)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			c := exec.Command("bash", "run_node.sh", fmt.Sprint(shard))
			c.Dir = root
			c.Stdout = f
			c.Stderr = f
			if err = c.Start(); err != nil {
				t.Fatal(err)
			}
			commands = append(commands, c)
			logs = append(logs, f)
		}
		for shard := 0; shard < 2; shard++ {
			ready := false
			for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
				c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", 43117+shard), time.Second)
				if err == nil {
					c.Close()
					ready = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !ready {
				t.Fatal("node startup timeout")
			}
		}
	}
	recordReport := func(r message.BlockInfoMsg) {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = reportFile.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
		m.observeForecastCommit(&r)
	}
	start("observation")
	expected := map[string]bool{}
	for window := 0; window < 4; window++ {
		batch := make([]*core.Transaction, 0, 2000)
		for j := 0; j < 2000; j++ {
			i := window*2000 + j
			b := fmt.Sprintf("%040x", 1001+2*(i%256))
			tx := core.NewTransaction(a, b, big.NewInt(1), uint64(i+1), time.Now().UTC())
			tx.RoutingFrozen = true
			tx.RoutedSenderShard = 0
			tx.RoutedRecipientShard = 1
			tx.IngressSequence = uint64(i)
			tx.RoutingEpoch = 1
			if err := m.recordForecastRoutedInput(tx); err != nil {
				t.Fatal(err)
			}
			expected[hex.EncodeToString(tx.TxHash)] = true
			batch = append(batch, tx)
		}
		b, err := json.Marshal(message.InjectTxs{Txs: batch})
		if err != nil {
			t.Fatal(err)
		}
		if err = send(0, message.CInject, b); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
	}
	seen := [2]map[string]bool{map[string]bool{}, map[string]bool{}}
	deadline := time.NewTimer(55 * time.Second)
	defer deadline.Stop()
	for len(seen[0]) < 8000 || len(seen[1]) < 8000 {
		select {
		case r := <-reports:
			recordReport(r)
			for _, tx := range append(append(r.InnerShardTxs, r.Relay1Txs...), r.Relay2Txs...) {
				h := hex.EncodeToString(tx.TxHash)
				if expected[h] && r.SenderShardID < 2 {
					if seen[r.SenderShardID][h] {
						t.Fatal("duplicate workload execution")
					}
					seen[r.SenderShardID][h] = true
				}
			}
		case <-deadline.C:
			t.Fatalf("native workload timeout source=%d target=%d", len(seen[0]), len(seen[1]))
		}
	}
	stop()
	t.Log("8000 source commits and 8000 target relay commits; nodes paused at quiescent boundary")
	if err := m.finishForecastInput(8000); err != nil {
		t.Fatal(err)
	}
	scripts := filepath.Join(project, "saa_research_data_contract_v1/scripts")
	runPython := func(script string, args ...string) {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		c := exec.CommandContext(ctx, "python3", append([]string{filepath.Join(scripts, script)}, args...)...)
		out, err := c.CombinedOutput()
		os.WriteFile(filepath.Join(root, script+".log"), out, 0600)
		if err != nil {
			t.Fatalf("%s: %v %s", script, err, out)
		}
	}
	runPython("run_live_anchor_controller.py", "--input", filepath.Join(root, "routed.jsonl"), "--stream-id", "native-saa-one", "--checkpoint", checkpoint, "--assignments", filepath.Join(root, "assignments.csv"), "--output", filepath.Join(root, "model"))
	if err := m.consumeForecastAnchorsLocked(); err != nil {
		t.Fatal(err)
	}
	round := 0
	for _, v := range s.forecastCommits {
		if v.Epoch >= round {
			round = v.Epoch + 1
		}
	}
	// Exact closed-workload conservation: every submitted transaction has both
	// native commit reports; no further producer exists while nodes are stopped.
	m.liveQueueByShard = map[uint64]float64{0: 0, 1: 0}
	m.pressureHintEpoch = round
	snapshot := m.forecastContextLocked(round)
	// Nodes have stopped above. Read the candidate accounts at committed heads;
	// this is a paused-boundary adapter, not an online cache freshness override.
	if err := supplementPausedPayloads(root, snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot["queue_evidence"] = map[string]interface{}{"kind": "closed_workload_commit_conservation_at_stopped_boundary", "input": 8000, "source_commits": len(seen[0]), "target_commits": len(seen[1]), "not_live_queue_telemetry": true}
	write("contexts.jsonl", snapshot)
	// Context stream is one JSON line, not the human-readable JSON above.
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "contexts.jsonl"), append(encoded, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	s.forecastContextVersion = s.forecastAnchorVersion
	write("weights.json", map[string]float64{"CrossWork": 1, "QueueArea": 1, "HandoffWork": 1.0 / 1024})
	runPython("build_runtime_decisions.py", "--predictions", filepath.Join(root, "model/relation_predictions.jsonl"), "--anchors", filepath.Join(root, "model/anchor_runtime.jsonl"), "--contexts", filepath.Join(root, "contexts.jsonl"), "--weights", filepath.Join(root, "weights.json"), "--output", filepath.Join(root, "decision"))
	data, err := os.ReadFile(s.forecastDecisionPath)
	if err != nil {
		t.Fatal(err)
	}
	var request forecastDecisionMessage
	if err = json.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	if err = m.consumeForecastDecisionsLocked(round); err != nil {
		t.Fatal(err)
	}
	decision := s.forecastBridge.results[request.Envelope.DecisionID]
	write("authority_receipt.json", decision)
	t.Logf("SAA selected=%s authority=%+v", request.Decision.Selected, decision)
	if request.Envelope.Keep {
		if decision.Status != mappingNoop || s.authority.routeVersion != 0 {
			t.Fatal("keep contract failed")
		}
		write("result.json", map[string]interface{}{"status": "KEEP", "migration_executed": false, "scope": "actual native fixture -> model -> authority at paused boundary", "proposal": request.Envelope.DecisionID})
		return
	}
	if decision.Status != mappingAccepted {
		t.Fatal("SAA migration rejected by authority")
	}
	env := request.Envelope
	account := strings.TrimPrefix(env.Account, "0x")
	head, before, encodedState, err := readSAAAccount(root, env.SourceShard, account)
	if err != nil {
		t.Fatal(err)
	}
	stateHash := sha256.Sum256(encodedState)
	command := core.SourcePrepareCommand{ProposalID: env.DecisionID, Account: account, SourceShard: env.SourceShard, TargetShard: env.TargetShard, MappingVersion: env.MappingVersion, AnchorVersion: env.AnchorVersion, ExpectedBlockHash: hex.EncodeToString(head.Hash), ExpectedStateRoot: hex.EncodeToString(head.Header.StateRoot), ExpectedStateHash: hex.EncodeToString(stateHash[:])}
	if err = m.PublishNativeSourcePreparation(command, os.Getenv("SAA_NATIVE_SOURCE_PREPARE_PATH")); err != nil {
		t.Fatal(err)
	}
	start("migration")
	retired := false
	for until := time.Now().Add(45 * time.Second); time.Now().Before(until); {
		m.lock.Lock()
		m.applySAAIntegratedDueLocked(round + 1)
		m.lock.Unlock()
		if _, err = os.Stat(os.Getenv("SAA_NATIVE_SOURCE_RETIRED_PATH")); err == nil {
			if err = m.validateNativeRetirementLocked(env.DecisionID, env.Account); err != nil {
				t.Fatal(err)
			}
			retired = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !retired {
		t.Fatal("SAA native migration timed out; no retry")
	}
	tx := core.NewTransaction(account, account, big.NewInt(1), before.Nonce+1, time.Now().UTC())
	tx.RoutingFrozen = true
	tx.RoutedSenderShard = s.authority.active[env.Account]
	tx.RoutedRecipientShard = tx.RoutedSenderShard
	tx.MappingVersion = s.authority.routeVersion
	tx.IngressSequence = 8000
	tx.SenderRequiresExistingState = true
	tx.RecipientRequiresExistingState = true
	wire, err := json.Marshal(message.InjectTxs{Txs: []*core.Transaction{tx}})
	if err != nil {
		t.Fatal(err)
	}
	if err = send(tx.RoutedSenderShard, message.CInject, wire); err != nil {
		t.Fatal(err)
	}
	committed := false
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
wait:
	for {
		select {
		case r := <-reports:
			recordReport(r)
			for _, v := range r.InnerShardTxs {
				if bytes.Equal(v.TxHash, tx.TxHash) && r.SenderShardID == env.TargetShard {
					committed = true
					break wait
				}
			}
		case <-timer.C:
			break wait
		}
	}
	stop()
	_, source, _, err := readSAAAccount(root, env.SourceShard, account)
	if err != nil {
		t.Fatal(err)
	}
	targetHead, target, _, err := readSAAAccount(root, env.TargetShard, account)
	if err != nil {
		t.Fatal(err)
	}
	if source.Nonce != before.Nonce || target.Nonce != before.Nonce+1 || source.Balance.Cmp(before.Balance) != 0 || target.Balance.Cmp(before.Balance) != 0 {
		t.Fatal("post-route state contract failed")
	}
	found := false
	for _, v := range targetHead.Body {
		if bytes.Equal(v.TxHash, tx.TxHash) {
			found = true
		}
	}
	if !found {
		t.Fatal("post-route transaction absent from target committed head")
	}
	write("result.json", map[string]interface{}{"status": "PASS", "proposal": env.DecisionID, "account": env.Account, "source": env.SourceShard, "target": env.TargetShard, "route_version": s.authority.routeVersion, "post_tx_hash": hex.EncodeToString(tx.TxHash), "target_block": targetHead.Header.Number, "source_nonce": source.Nonce, "target_nonce": target.Nonce, "commit_report_seen": committed, "migration_executed": true, "scope": "one actual SAA decision at quiescent boundary; synthetic native workload; not async/performance proof"})
	t.Logf("SAA NATIVE PASS proposal=%s route_version=%d source_nonce=%d target_nonce=%d", env.DecisionID, s.authority.routeVersion, source.Nonce, target.Nonce)
}

func readSAAAccount(root string, shard uint64, account string) (*core.Block, *core.AccountState, []byte, error) {
	db, err := bolt.Open(filepath.Join(root, fmt.Sprintf("exp/database/chainDB/S%d_N0", shard)), 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return nil, nil, nil, err
	}
	var head *core.Block
	err = db.View(func(tx *bolt.Tx) error {
		heads, blocks := tx.Bucket([]byte("newestBlockHash")), tx.Bucket([]byte("block"))
		if heads == nil || blocks == nil {
			return fmt.Errorf("chain absent")
		}
		data := blocks.Get(heads.Get([]byte("OnlyNewestBlock")))
		if len(data) == 0 {
			return fmt.Errorf("head absent")
		}
		head = core.DecodeB(data)
		return nil
	})
	db.Close()
	if err != nil {
		return nil, nil, nil, err
	}
	state, err := rawdb.NewLevelDBDatabase(filepath.Join(root, fmt.Sprintf("exp/database/mptDB/ldb/s%d/n0", shard)), 0, 1, "saa-readonly", true)
	if err != nil {
		return nil, nil, nil, err
	}
	defer state.Close()
	st, err := trie.New(trie.TrieID(common.BytesToHash(head.Header.StateRoot)), trie.NewDatabase(state))
	if err != nil {
		return nil, nil, nil, err
	}
	raw, err := st.Get([]byte(account))
	if err != nil {
		return nil, nil, nil, err
	}
	if len(raw) == 0 {
		return nil, nil, nil, fmt.Errorf("account absent")
	}
	return head, core.DecodeAS(raw), raw, nil
}
