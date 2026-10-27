package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/message"
	"blockEmulator/params"
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Explicit, finite semantic acceptance only. The supplied binary runs eight
// actual node processes; this test never calls any consensus commit handler.
// No policy, prediction, queue threshold, or performance comparison is involved.
func TestGroupNativeFiniteAcceptance(t *testing.T) {
	binary, root := os.Getenv("SAA_GROUP_NATIVE_ACCEPTANCE_BINARY"), os.Getenv("SAA_GROUP_NATIVE_ACCEPTANCE_DIR")
	if binary == "" && root == "" {
		t.Skip("explicit eight-process native acceptance only")
	}
	if !filepath.IsAbs(binary) || !filepath.IsAbs(root) {
		t.Fatal("absolute candidate binary and fresh evidence directory are required")
	}
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal("evidence directory must not already exist", e)
	}
	passed := false
	isolationChecked, replayChecked := false, false
	phase := "startup"
	defer func() {
		status := "FAIL"
		if passed {
			status = "PASS"
		}
		raw, err := json.MarshalIndent(map[string]interface{}{"status": status, "phase": phase, "scope": "2 shards x 4 real PBFT processes; fixed two-member group; eight finite admissions", "target_isolation": isolationChecked, "late_duplicate_relay": replayChecked, "limits": "sequential late retries only; trusted supervisor/receipt reporters; no concurrent retransmission, Byzantine certificate, restart, performance, or algorithm value evidence"}, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "result.json"), raw, 0600)
		}
		if err != nil {
			t.Errorf("saving native result: %v", err)
		}
	}()
	writeJSON := func(name string, value interface{}) string {
		t.Helper()
		raw, e := json.MarshalIndent(value, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		path := filepath.Join(root, name)
		if e = os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
		return path
	}
	accounts := []string{fmt.Sprintf("%040x", 16), fmt.Sprintf("%040x", 32), fmt.Sprintf("%040x", 48), fmt.Sprintf("%040x", 49)}
	ledger := initialledger.Manifest{Schema: "synthetic_initial_ledger_v1", Shards: 2}
	for i, a := range accounts {
		shard := uint64(0)
		if i == 3 {
			shard = 1
		}
		ledger.Accounts = append(ledger.Accounts, initialledger.Account{Address: a, Balance: "100", Shard: shard})
	}
	ledgerPath := writeJSON("ledger.json", ledger)
	ledgerRaw, e := os.ReadFile(ledgerPath)
	if e != nil {
		t.Fatal(e)
	}
	configPath := writeJSON("paramsConfig.json", map[string]interface{}{
		"ConsensusMethod": 3, "PbftViewChangeTimeOut": 60000,
		"ExpDataRootDir": filepath.Join(root, "exp"), "Block_Interval": 1000,
		"BlockSize": 8, "BlocksizeInBytes": 200000, "UseBlocksizeInBytes": 0,
		"InjectSpeed": 8, "TotalDataSize": 8, "TxBatchSize": 8,
		"BrokerNum": 0, "RelayWithMerkleProof": 0, "DatasetFile": "unused",
		"ReconfigTimeGap": 50, "Delay": 0, "JitterRange": 0, "Bandwidth": 10000000,
	})
	// Reserve all ports together. A startup collision fails this run; it does
	// not select a second topology or silently retry a candidate.
	reserved := []net.Listener{}
	table := map[uint64]map[uint64]string{}
	for s := uint64(0); s < 2; s++ {
		table[s] = map[uint64]string{}
		for n := uint64(0); n < 4; n++ {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			reserved = append(reserved, ln)
			table[s][n] = ln.Addr().String()
		}
	}
	defer func() {
		for _, ln := range reserved {
			ln.Close()
		}
	}()
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	table[params.SupervisorShard] = map[uint64]string{0: listener.Addr().String()}
	writeJSON("ipTable.json", table)
	transport := map[string]string{}
	for _, key := range []string{"SAA_GROUP_SOURCE_PREPARE_PATH", "SAA_GROUP_SOURCE_PREPARED_PATH", "SAA_GROUP_TARGET_INSTALL_PATH", "SAA_GROUP_TARGET_INSTALLED_PATH", "SAA_GROUP_TARGET_ACTIVATION_PATH", "SAA_GROUP_TARGET_ACTIVATED_PATH", "SAA_GROUP_SOURCE_RETIRED_PATH", "SAA_NATIVE_SOURCE_PREPARE_PATH", "SAA_NATIVE_SOURCE_PREPARED_PATH", "SAA_NATIVE_TARGET_INSTALL_PATH", "SAA_NATIVE_INSTALLATION_RECEIPT_PATH", "SAA_NATIVE_TARGET_ACTIVATION_PATH", "SAA_NATIVE_TARGET_ACTIVATED_PATH", "SAA_NATIVE_SOURCE_RETIRED_PATH"} {
		transport[key] = filepath.Join(root, key+".json")
	}
	for _, key := range []string{"SAA_NATIVE_SOURCE_QUEUE_ROOT", "SAA_NATIVE_HANDOFF_OUTBOX_DIR", "SAA_NATIVE_HANDOFF_ACK_DIR", "SAA_NATIVE_HANDOFF_RELAY_DIR"} {
		transport[key] = filepath.Join(root, key)
		if e = os.Mkdir(transport[key], 0700); e != nil {
			t.Fatal(e)
		}
	}
	// Ignore inherited experiment settings, then configure only this fixture.
	childEnv := []string{}
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "SAA_") && !strings.HasPrefix(entry, "BLOCK_EMULATOR_") {
			childEnv = append(childEnv, entry)
		}
	}
	for key, value := range transport {
		t.Setenv(key, value)
		childEnv = append(childEnv, key+"="+value)
	}
	t.Setenv("SAA_SERIAL_GROUP_CONTROL", "false")
	t.Setenv(chain.NativeEffectIndexEnv, "true")
	childEnv = append(childEnv, "BLOCK_EMULATOR_CONFIG_PATH="+configPath, "BLOCK_EMULATOR_INITIAL_LEDGER_PATH="+ledgerPath,
		fmt.Sprintf("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256=%x", sha256.Sum256(ledgerRaw)),
		"SAA_NATIVE_INITIAL_LEDGER_MIGRATION_V1=true", "SAA_REQUIRE_NATIVE_INSTALLATION_RECEIPT=true", "SAA_INTEGRATED_CONTROLLER=legacy", "SAA_NATIVE_HANDOFF_BATCH_V1=true", "SAA_NATIVE_EFFECT_INDEX=true")
	closed := make(chan struct{})
	defer close(closed)
	reports := make(chan message.BlockInfoMsg, 128)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				scan := bufio.NewScanner(conn)
				scan.Buffer(make([]byte, 4096), 4<<20)
				for scan.Scan() {
					prefix := message.MergeMessage(message.CBlockInfo, nil)
					if !bytes.HasPrefix(scan.Bytes(), prefix) {
						continue
					}
					var r message.BlockInfoMsg
					if json.Unmarshal(scan.Bytes()[len(prefix):], &r) != nil {
						continue
					}
					select {
					case reports <- r:
					case <-closed:
						return
					}
				}
			}(conn)
		}
	}()
	send := func(shard uint64, kind message.MessageType, value interface{}) {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := net.DialTimeout("tcp", table[shard][0], time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		wire := append(message.MergeMessage(kind, raw), '\n')
		if n, err := conn.Write(wire); err != nil || n != len(wire) {
			t.Fatal("incomplete native write", n, err)
		}
	}
	type process struct {
		cmd     *exec.Cmd
		log     *os.File
		address string
	}
	processes := []process{}
	stop := func() bool {
		for _, p := range processes {
			conn, err := net.DialTimeout("tcp", p.address, 250*time.Millisecond)
			if err == nil {
				conn.SetWriteDeadline(time.Now().Add(time.Second))
				conn.Write(append(message.MergeMessage(message.CStop, []byte("finite acceptance complete")), '\n'))
				conn.Close()
			}
		}
		graceful := true
		for _, p := range processes {
			done := make(chan error, 1)
			go func(cmd *exec.Cmd) { done <- cmd.Wait() }(p.cmd)
			select {
			case err := <-done:
				graceful = graceful && err == nil
			case <-time.After(5 * time.Second):
				graceful = false
				p.cmd.Process.Kill()
				<-done
			}
			p.log.Close()
		}
		processes = nil
		return graceful
	}
	defer func() { stop() }()
	for _, ln := range reserved {
		ln.Close()
	}
	for s := 0; s < 2; s++ {
		for n := 0; n < 4; n++ {
			log, err := os.Create(filepath.Join(root, fmt.Sprintf("node-%d-%d.log", s, n)))
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "-S", "2", "-N", "4", "-s", fmt.Sprint(s), "-n", fmt.Sprint(n))
			cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = root, childEnv, log, log
			if err = cmd.Start(); err != nil {
				log.Close()
				t.Fatal(err)
			}
			processes = append(processes, process{cmd, log, table[uint64(s)][uint64(n)]})
		}
	}
	deadline := time.Now().Add(150 * time.Second)
	for s := uint64(0); s < 2; s++ {
		for n := uint64(0); n < 4; n++ {
			for {
				conn, err := net.DialTimeout("tcp", table[s][n], 100*time.Millisecond)
				if err == nil {
					conn.Close()
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("native startup incomplete; no retry")
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
	tx := func(sequence uint64, sender, recipient string, from, to, version uint64, value int64) *core.Transaction {
		v := core.NewTransaction(sender, recipient, big.NewInt(value), sequence+1, time.Now().UTC())
		v.AdmissionID, v.IngressSequence = fmt.Sprintf("group-acceptance/%d", sequence+1), sequence+1
		v.RoutingFrozen, v.MappingVersion = true, version
		v.RoutedSenderShard, v.RoutedRecipientShard = from, to
		v.SenderRequiresExistingState, v.RecipientRequiresExistingState = true, true
		return v
	}
	transactions := []*core.Transaction{
		tx(0, accounts[2], accounts[2], 0, 0, 0, 1), tx(1, accounts[3], accounts[3], 1, 1, 0, 1),
		tx(2, accounts[0], accounts[1], 1, 1, 1, 3), tx(3, accounts[3], accounts[3], 1, 1, 0, 1),
		tx(4, accounts[3], accounts[0], 1, 0, 0, 5), tx(5, accounts[1], accounts[0], 1, 1, 1, 2),
		tx(6, accounts[2], accounts[2], 0, 0, 0, 1), tx(7, accounts[3], accounts[3], 1, 1, 0, 1),
	}
	writeJSON("transactions.json", transactions)
	completed := map[string]bool{}
	completionHeight := map[uint64]uint64{}
	var actualRelay *core.Transaction
	hash := func(v *core.Transaction) string { return hex.EncodeToString(v.TxHash) }
	consume := func(r message.BlockInfoMsg) {
		for _, v := range r.Relay1Txs {
			if hash(v) == hash(transactions[4]) {
				actualRelay = core.DecodeTx(v.Encode())
			}
		}
		for _, v := range append(r.InnerShardTxs, r.Relay2Txs...) {
			key := hash(v)
			if completed[key] {
				t.Fatal("duplicate native completion", key)
			}
			completed[key] = true
			if r.Epoch < 1 {
				t.Fatal("completion report lacks committed block position")
			}
			if uint64(r.Epoch) > completionHeight[r.SenderShardID] {
				completionHeight[r.SenderShardID] = uint64(r.Epoch)
			}
		}
	}
	wait := func(name string, condition func() bool) {
		t.Helper()
		phase = name
		for !condition() {
			select {
			case r := <-reports:
				consume(r)
			case <-time.After(50 * time.Millisecond):
			}
			if time.Now().After(deadline) {
				t.Fatal("finite native acceptance incomplete at", name, "no automatic retry")
			}
		}
	}
	send(0, message.CInject, message.InjectTxs{Txs: transactions[:1]})
	send(1, message.CInject, message.InjectTxs{Txs: transactions[1:2]})
	wait("warm-up commits", func() bool { return completed[hash(transactions[0])] && completed[hash(transactions[1])] })
	command := core.GroupPrepareCommand{}
	initial := map[string]uint64{}
	for _, a := range accounts[:2] {
		command.Members = append(command.Members, core.SourcePrepareCommand{ProposalID: "finite-group", Account: a, SourceShard: 0, TargetShard: 1, BindAtProposal: true})
		initial["0x"+a] = 0
	}
	g := &nativeGroupControl{spec: nativeGroupSpec{Arm: "move", AfterOccurrences: 2, Command: command}, authority: newMappingAuthority(2, initial)}
	g.authority.enableInstallationReceiptGate()
	atomic.StoreInt64(&g.inputCount, 2)
	// Stage a production-generated command outside the nodes' watched path.
	// This makes the controller's stage-2 pause deterministic without changing
	// its gate or calling a consensus handler. Publish the exact bytes below.
	installPath := transport["SAA_GROUP_TARGET_INSTALL_PATH"]
	stagedInstall := filepath.Join(root, "controller-install-staged.json")
	t.Setenv("SAA_GROUP_TARGET_INSTALL_PATH", stagedInstall)
	wait("committed prepare", func() bool {
		if err := g.poll(); err != nil {
			t.Fatal(err)
		}
		return g.stage == 2
	})
	installRaw, err := os.ReadFile(stagedInstall)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAA_GROUP_TARGET_INSTALL_PATH", installPath)
	if err = core.PublishSourcePrepareFile(installPath, installRaw); err != nil {
		t.Fatal(err)
	}
	// Stop controller polling only: nodes keep producing real PBFT blocks.
	wait("isolated target installation", func() bool {
		r, err := groupControlRead("SAA_GROUP_TARGET_INSTALLED_PATH")
		if err != nil {
			t.Fatal(err)
		}
		return r != nil
	})
	send(1, message.CInject, message.InjectTxs{Txs: transactions[2:5]})
	wait("unrelated target work during installation hold", func() bool { return completed[hash(transactions[3])] && actualRelay != nil })
	if completed[hash(transactions[2])] || completed[hash(transactions[4])] || g.active {
		t.Fatal("group transaction escaped before activation")
	}
	isolationChecked = true
	wait("committed activation", func() bool {
		if err := g.poll(); err != nil {
			t.Fatal(err)
		}
		return g.active
	})
	send(1, message.CInject, message.InjectTxs{Txs: transactions[5:6]})
	send(0, message.CInject, message.InjectTxs{Txs: transactions[6:7]})
	wait("source retirement", func() bool {
		if err := g.poll(); err != nil {
			t.Fatal(err)
		}
		return g.retired && completed[hash(transactions[2])] && completed[hash(transactions[5])] && completed[hash(transactions[6])]
	})
	if actualRelay == nil || !actualRelay.Relayed || actualRelay.AdmissionID != transactions[4].AdmissionID {
		t.Fatal("late relay must come from an actual committed first leg")
	}
	for _, effect := range []string{core.TransactionDebit, core.TransactionCredit} {
		original, e1 := core.BuildTransactionEffect(transactions[4], effect)
		relay, e2 := core.BuildTransactionEffect(actualRelay, effect)
		if e1 != nil || e2 != nil || original != relay {
			t.Fatal("committed Relay1 changed immutable admission payload", effect, e1, e2)
		}
	}
	// Replay the same committed relay after source retirement, without creating
	// a new admission occurrence or rewriting its frozen route identity. These
	// are sequential retries; concurrent duplicate queue admission is not covered.
	late := message.Relay{Txs: []*core.Transaction{actualRelay}, SenderShardID: 1}
	journal := filepath.Join(transport["SAA_NATIVE_SOURCE_QUEUE_ROOT"], "s0-n0")
	closeout := []groupAcceptanceQueueEvidence{}
	wait("original credit handoff closeout", func() bool {
		ready, err := groupAcceptanceHandoffClosed(transport, actualRelay, accounts[:2])
		if err != nil {
			t.Fatal(err)
		}
		return ready
	})
	for retry := 0; retry < 2; retry++ {
		before, err := groupAcceptanceQueue(journal, accounts[:2], actualRelay)
		if err != nil || before.Pending != 0 {
			t.Fatal("late retry requires drained source journal", before, err)
		}
		packet := late
		if retry == 0 {
			packet.Txs = []*core.Transaction{actualRelay, core.DecodeTx(actualRelay.Encode())}
		}
		send(0, message.CRelay, packet)
		wait(fmt.Sprintf("late retry %d durable closeout", retry+1), func() bool {
			after, err := groupAcceptanceQueue(journal, accounts[:2], actualRelay)
			if err != nil {
				t.Fatal(err)
			}
			if after.Additions > before.Additions+1 || after.Removals > before.Removals+1 {
				t.Fatal("identical packet entries produced more than one durable obligation", before, after)
			}
			if after.Generation <= before.Generation || after.Additions != before.Additions+1 || after.Removals != before.Removals+1 || after.Pending != 0 {
				return false
			}
			ready, err := groupAcceptanceHandoffClosed(transport, actualRelay, accounts[:2])
			if err != nil {
				t.Fatal(err)
			}
			if ready {
				closeout = append(closeout, after)
			}
			return ready
		})
	}
	writeJSON("late-replay-closeout.json", closeout)
	replayChecked = true
	send(1, message.CInject, message.InjectTxs{Txs: transactions[7:]})
	wait("independent post-retirement work", func() bool { return completed[hash(transactions[7])] })
	retired, err := groupControlRead("SAA_GROUP_SOURCE_RETIRED_PATH")
	if err != nil || retired == nil {
		t.Fatal("source retirement position unavailable", err)
	}
	if retired.Block >= completionHeight[0] {
		// A subsequent ordinary committed block proves each follower has also
		// applied the preceding source retirement control request.
		completionHeight[0] = retired.Block + 1
	}
	replicaHeights := map[string]uint64{}
	wait("all replica execution positions before stop", func() bool {
		for shard := uint64(0); shard < 2; shard++ {
			for node := 0; node < 4; node++ {
				height, err := groupAcceptanceCommittedHeight(filepath.Join(root, fmt.Sprintf("node-%d-%d.log", shard, node)))
				if err != nil {
					t.Fatal(err)
				}
				replicaHeights[fmt.Sprintf("%d/%d", shard, node)] = height
				if height < completionHeight[shard] {
					return false
				}
			}
		}
		return true
	})
	writeJSON("replica-commit-readiness.json", map[string]interface{}{"required": completionHeight, "observed": replicaHeights, "scope": "trusted local applied-block logs, not a Byzantine certificate"})
	if !stop() {
		t.Fatal("native node did not exit cleanly; retained evidence, no retry")
	}
	phase = "committed replica disk verification"
	groupAcceptanceDisk(t, root, accounts, transactions, g.prepared, map[string]string{accounts[0]: "104", accounts[1]: "101", accounts[2]: "100", accounts[3]: "95"}, map[string]uint64{accounts[0]: 1, accounts[1]: 1, accounts[2]: 2, accounts[3]: 4})
	passed = true
	phase = "complete"
}

type groupAcceptanceQueueEvidence struct {
	Generation                   uint64
	Additions, Removals, Pending int
}

func groupAcceptanceCommittedHeight(path string) (uint64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	const marker = "added the block "
	var maximum uint64
	for _, line := range strings.Split(string(raw), "\n") {
		if offset := strings.Index(line, marker); offset >= 0 {
			var height uint64
			if _, err := fmt.Sscanf(line[offset+len(marker):], "%d", &height); err == nil && height > maximum {
				maximum = height
			}
		}
	}
	return maximum, nil
}

// Restore only already-published generations into an isolated audit pool.
// Restore requires the first snapshot and cannot manufacture missing evidence.
func groupAcceptanceQueue(directory string, accounts []string, relay *core.Transaction) (groupAcceptanceQueueEvidence, error) {
	var result groupAcceptanceQueueEvidence
	pool := core.NewTxPool()
	if err := pool.RestoreGroupMigrationQueueJournal(directory, accounts); err != nil {
		return result, err
	}
	result.Pending = pool.GetTxQueueLen()
	entries, err := os.ReadDir(directory)
	if err != nil {
		return result, err
	}
	want, err := core.BuildTransactionEffect(relay, core.TransactionCredit)
	if err != nil {
		return result, err
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".queue.json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return result, err
		}
		var snapshot struct {
			Sequence     uint64
			Kind         string
			Transactions [][]byte
		}
		if err = json.Unmarshal(raw, &snapshot); err != nil {
			return result, err
		}
		if snapshot.Sequence > result.Generation {
			result.Generation = snapshot.Sequence
		}
		for _, encoded := range snapshot.Transactions {
			identity, err := core.BuildTransactionEffect(core.DecodeTx(encoded), core.TransactionCredit)
			if err != nil {
				return result, err
			}
			if identity == want {
				switch snapshot.Kind {
				case "append", "prepend":
					result.Additions++
				case "remove":
					result.Removals++
				}
			}
		}
	}
	return result, nil
}

func groupAcceptanceHandoffClosed(transport map[string]string, relay *core.Transaction, accounts []string) (bool, error) {
	records, err := core.LoadMigrationOutbox(transport["SAA_NATIVE_HANDOFF_OUTBOX_DIR"])
	if err != nil {
		return false, err
	}
	want, err := core.BuildTransactionEffect(relay, core.TransactionCredit)
	if err != nil {
		return false, err
	}
	var matching *core.MigrationHandoff
	for _, h := range records {
		identity, err := core.BuildTransactionEffect(core.DecodeTx(h.Original), core.TransactionCredit)
		if err != nil {
			return false, err
		}
		if identity == want {
			if matching != nil || h.ProposalID != "finite-group" || h.Leg != "credit" || h.SourceShard != 0 || h.TargetShard != 1 || len(h.GroupAccounts) != len(accounts) || h.GroupAccounts[0] != accounts[0] || h.GroupAccounts[1] != accounts[1] {
				return false, fmt.Errorf("ambiguous or mismatched credit handoff")
			}
			matching = h
		}
	}
	if matching == nil {
		return false, nil
	}
	raw, err := core.ReadMigrationArtifact(transport["SAA_NATIVE_HANDOFF_ACK_DIR"], matching.ID, ".ack.json")
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var ack core.HandoffAck
	if err = json.Unmarshal(raw, &ack); err != nil {
		return false, err
	}
	if err = core.ValidateHandoffAck(matching, ack); err != nil {
		return false, err
	}
	members := map[string]string{}
	for _, account := range accounts {
		members[account] = "finite-group"
	}
	audit, err := core.AuditMigrationHandoffsForMembers(transport["SAA_NATIVE_HANDOFF_OUTBOX_DIR"], members)
	if err != nil {
		return false, err
	}
	for _, member := range audit {
		if member.Persisted != member.Confirmed || len(member.Pending) != 0 {
			return false, nil
		}
	}
	return true, nil
}

// Recheck the exact durable closeout left by a finite native run. This does
// not drive consensus, modify evidence, or promote a failed run to PASS.
func TestGroupAcceptanceSavedCloseout(t *testing.T) {
	root := os.Getenv("SAA_GROUP_ACCEPTANCE_REPLAY_DIR")
	if root == "" {
		t.Skip("explicit saved native closeout evidence required")
	}
	raw, err := os.ReadFile(filepath.Join(root, "transactions.json"))
	if err != nil {
		t.Fatal(err)
	}
	var txs []*core.Transaction
	if err = json.Unmarshal(raw, &txs); err != nil || len(txs) != 8 {
		t.Fatal("invalid saved finite fixture", err)
	}
	relay := core.DecodeTx(txs[4].Encode())
	relay.Relayed = true
	transport := map[string]string{}
	for _, key := range []string{"SAA_NATIVE_HANDOFF_OUTBOX_DIR", "SAA_NATIVE_HANDOFF_ACK_DIR"} {
		transport[key] = filepath.Join(root, key)
	}
	accounts := []string{txs[2].Sender, txs[2].Recipient}
	closed, err := groupAcceptanceHandoffClosed(transport, relay, accounts)
	if err != nil || !closed {
		t.Fatal("saved committed bundle was not recognized", closed, err)
	}
	queue, err := groupAcceptanceQueue(filepath.Join(root, "SAA_NATIVE_SOURCE_QUEUE_ROOT", "s0-n0"), accounts, relay)
	if err != nil || queue.Pending != 0 {
		t.Fatal("saved source obligations did not close", queue, err)
	}
	t.Logf("saved_closeout=true queue=%+v (not full native acceptance)", queue)
}

func groupAcceptanceDisk(t *testing.T, root string, accounts []string, transactions []*core.Transaction, prepared *core.GroupPreparation, balances map[string]string, nonces map[string]uint64) {
	t.Helper()
	if prepared == nil || len(prepared.Members) != 2 {
		t.Fatal("missing committed two-member preparation")
	}
	commonRoots := map[string]string{}
	for shard := 0; shard < 2; shard++ {
		for node := 0; node < 4; node++ {
			if shard == 0 {
				journal := filepath.Join(root, "SAA_NATIVE_SOURCE_QUEUE_ROOT", fmt.Sprintf("s0-n%d", node))
				queue, err := groupAcceptanceQueue(journal, accounts[:2], transactions[4])
				if err != nil || queue.Pending != 0 {
					t.Fatal("stopped source replica retains migration obligations", node, queue, err)
				}
			}
			db, e := bolt.Open(filepath.Join(root, fmt.Sprintf("exp/database/chainDB/S%d_N%d", shard, node)), 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
			if e != nil {
				t.Fatal(e)
			}
			var head *core.Block
			e = db.View(func(tx *bolt.Tx) error {
				blocks, newest := tx.Bucket([]byte("block")), tx.Bucket([]byte("newestBlockHash"))
				if blocks == nil || newest == nil {
					return fmt.Errorf("replica %d/%d has no committed chain", shard, node)
				}
				raw := blocks.Get(newest.Get([]byte("OnlyNewestBlock")))
				if len(raw) == 0 {
					return io.ErrUnexpectedEOF
				}
				head = core.DecodeB(raw)
				for b := head; ; {
					key := fmt.Sprintf("%d/%d", shard, b.Header.Number)
					value := hex.EncodeToString(b.Header.StateRoot)
					if old, ok := commonRoots[key]; ok && old != value {
						return fmt.Errorf("same-height replica root mismatch %s", key)
					}
					commonRoots[key] = value
					if b.Header.Number == 0 {
						break
					}
					parent := blocks.Get(b.Header.ParentBlockHash)
					if len(parent) == 0 {
						return fmt.Errorf("replica %d/%d missing parent of block %d", shard, node, b.Header.Number)
					}
					b = core.DecodeB(parent)
				}
				return nil
			})
			db.Close()
			if e != nil {
				t.Fatal(e)
			}
			state, e := rawdb.NewLevelDBDatabase(filepath.Join(root, fmt.Sprintf("exp/database/mptDB/ldb/s%d/n%d", shard, node)), 0, 1, "audit", true)
			if e != nil {
				t.Fatal(e)
			}
			st, e := trie.New(trie.TrieID(common.BytesToHash(head.Header.StateRoot)), trie.NewDatabase(state))
			if e != nil {
				state.Close()
				t.Fatal(e)
			}
			for i, account := range accounts {
				raw, err := st.Get([]byte(account))
				if err != nil {
					state.Close()
					t.Fatal(err)
				}
				if shard == 0 && i < 2 {
					if !bytes.Equal(raw, prepared.Members[i].State) {
						state.Close()
						t.Fatal("retired source changed frozen member state", account)
					}
					continue
				}
				if (shard == 0 && i != 2) || (shard == 1 && i == 2) {
					continue
				}
				if len(raw) == 0 {
					state.Close()
					t.Fatal("final account missing", shard, node, account)
				}
				a := core.DecodeAS(raw)
				if a.Balance.String() != balances[account] || a.Nonce != nonces[account] {
					state.Close()
					t.Fatalf("replica %d/%d account %s balance=%s nonce=%d want=%s/%d", shard, node, account, a.Balance.String(), a.Nonce, balances[account], nonces[account])
				}
			}
			for _, tx := range transactions {
				for _, effect := range []string{core.TransactionDebit, core.TransactionCredit} {
					identity, err := core.BuildTransactionEffect(tx, effect)
					if err != nil {
						state.Close()
						t.Fatal(err)
					}
					// All member effects in this fixture happen after source
					// preparation. The outsider stays on source; both members
					// and the other outsider execute their effects on target.
					owner := 1
					if identity.Account == accounts[2] {
						owner = 0
					}
					if owner != shard {
						continue
					}
					status, err := chain.LookupTransactionEffect(st, tx, effect)
					if err != nil || status != chain.EffectSeen {
						state.Close()
						t.Fatalf("replica %d/%d admission %s %s status=%s err=%v", shard, node, tx.AdmissionID, effect, status, err)
					}
					receipt, err := chain.ReadTransactionEffectReceipt(st, tx, effect)
					if err != nil || receipt == nil || receipt.Effect != identity || receipt.Block == 0 || receipt.Block > head.Header.Number {
						state.Close()
						t.Fatalf("replica %d/%d effect commit receipt mismatch admission=%s effect=%s err=%v", shard, node, tx.AdmissionID, effect, err)
					}
				}
			}
			state.Close()
		}
	}
}
