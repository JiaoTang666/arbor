package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/initialledger"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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

func acceptanceConfig(root string) {
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + "/"
	params.DataWrite_path = filepath.Join(root, "metrics") + "/"
	params.LogWrite_path = filepath.Join(root, "logs")
	params.ShardNum = 2
	params.NodesInShard = 4
	params.Delay = 0
	params.JitterRange = 0
	params.Bandwidth = 1 << 24
	params.RelayWithMerkleProof = 0
	networks.InitNetworkTools()
}

// A separate OS process runs ordinary networking and Propose, not a test-driven
// HandleinCommit. Parent kills only the exact child processes it created.
func TestPBFTNodeProcess(t *testing.T) {
	raw := os.Getenv("SAA_ACCEPTANCE_NODE")
	if raw == "" {
		t.Skip("child process only")
	}
	id, e := strconv.Atoi(raw)
	if e != nil {
		t.Fatal(e)
	}
	acceptanceConfig(os.Getenv("SAA_ACCEPTANCE_ROOT"))
	if e = json.Unmarshal([]byte(os.Getenv("SAA_ACCEPTANCE_TABLE")), &params.IPmap_nodeTable); e != nil {
		t.Fatal(e)
	}
	s, n := uint64(id/4), uint64(id%4)
	config := &params.ChainConfig{ChainID: s, ShardID: s, NodeID: n, ShardNums: 2, Nodes_perShard: 4, BlockSize: 1, BlockInterval: 100, InjectSpeed: 8}
	node := NewPbftNode(s, n, config, "RawRelay")
	go node.TcpListen()
	node.Propose()
}

func TestPBFTMultiProcessAcceptance(t *testing.T) {
	clean := os.Getenv("SAA_CLEAN_COST") == "true"
	root := os.Getenv("SAA_ACCEPTANCE_ROOT")
	if root == "" {
		t.Skip("explicit multiprocess acceptance only")
	}
	acceptanceConfig(root)
	table := map[uint64]map[uint64]string{}
	for s := uint64(0); s < 2; s++ {
		table[s] = map[uint64]string{}
		for n := uint64(0); n < 4; n++ {
			ln, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			table[s][n] = ln.Addr().String()
			ln.Close()
		}
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	table[params.SupervisorShard] = map[uint64]string{0: listener.Addr().String()}
	tableRaw, _ := json.Marshal(table)
	executable, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	children := []*exec.Cmd{}
	logs := []*os.File{}
	defer func() {
		for _, c := range children {
			_ = c.Process.Kill()
		}
		for _, c := range children {
			_ = c.Wait()
		}
		for _, f := range logs {
			f.Close()
		}
		networks.CloseAllConnInPool()
		listener.Close()
	}()
	for i := 0; i < 8; i++ {
		f, e := os.OpenFile(filepath.Join(root, fmt.Sprintf("node_%d.stdout", i)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		logs = append(logs, f)
		cmd := exec.Command(executable, "-test.run=^TestPBFTNodeProcess$", "-test.timeout=70s", "-test.v")
		cmd.Env = append(os.Environ(), fmt.Sprintf("SAA_ACCEPTANCE_NODE=%d", i), "SAA_ACCEPTANCE_TABLE="+string(tableRaw))
		cmd.Stdout = f
		cmd.Stderr = f
		if e = cmd.Start(); e != nil {
			t.Fatal(e)
		}
		children = append(children, cmd)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	for s := uint64(0); s < 2; s++ {
		for n := uint64(0); n < 4; n++ {
			for {
				c, e := net.DialTimeout("tcp", table[s][n], 100*time.Millisecond)
				if e == nil {
					c.Close()
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("node startup timeout", s, n)
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
	}
	total := 8
	if clean {
		total = 5
	}
	m := committee.NewRelayCommitteeModule(table, nil, nil, "", total, 1)
	reports := make(chan *message.BlockInfoMsg, 64)
	delayedCreation := make(chan *message.BlockInfoMsg, 4)
	var reportMu sync.Mutex
	rf, e := os.OpenFile(filepath.Join(root, "block_reports.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer rf.Close()
	go func() {
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					raw, e := r.ReadBytes('\n')
					if e != nil {
						return
					}
					kind, body := message.SplitMessage(raw)
					if kind != message.CBlockInfo {
						continue
					}
					var b message.BlockInfoMsg
					if e = json.Unmarshal(body, &b); e != nil {
						panic(e)
					}
					reportMu.Lock()
					json.NewEncoder(rf).Encode(b)
					reportMu.Unlock()
					if !clean && len(b.FirstCreationReceipts) > 0 {
						delayedCreation <- &b
						continue
					}
					if e = m.ProcessFirstPlacementBlockReport(&b); e != nil {
						panic(e)
					}
					reports <- &b
				}
			}()
		}
	}()
	if e = m.StartFirstPlacementFence(ctx); e != nil {
		t.Fatal(e)
	}
	if clean {
		for _, id := range []int{0, 4} {
			for {
				raw, _ := os.ReadFile(filepath.Join(root, fmt.Sprintf("node_%d.stdout", id)))
				if strings.Contains(string(raw), "waiting_for_first_workload") {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("proposer readiness timeout")
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
	}
	measurementStarted := time.Now()
	account := func(n string) string { return strings.Repeat("0", 40-len(n)) + n }
	a, u, b, c, d, z := account("10"), account("20"), account("30"), account("40"), account("50"), account("60")
	reservation, e := m.VerifyAndReserveFirstPlacement(ctx, "multi", []firstplacement.Claim{{Account: u, TargetShard: 1}})
	if e != nil {
		t.Fatal(e)
	}
	session := reservation.Request.PresenceSession
	for s := uint64(0); s < 2; s++ {
		for n := uint64(1); n < 4; n++ {
			r, e := firstplacement.QueryPresence(ctx, table[s][n], firstplacement.PresenceQuery{Session: session, Shard: int(s), Hello: true})
			if e != nil || r.Status != "READY" {
				t.Fatal("follower hello", r, e)
			}
		}
	}
	initialStates := []firstplacement.PresenceResponse{}
	ledger, loadErr := initialledger.FromEnv(2)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if ledger != nil {
		for s := uint64(0); s < 2; s++ {
			for n := uint64(0); n < 4; n++ {
				rootAtZero := ""
				for _, entry := range ledger.Accounts {
					r, err := firstplacement.QueryPresence(ctx, table[s][n], firstplacement.PresenceQuery{Session: session, RequestID: "initial-" + entry.Address, Generation: 1, Account: entry.Address, Shard: int(s)})
					want := "ABSENT"
					if entry.Shard == s {
						want = "EXISTS"
					}
					if err != nil || r.Status != want || r.Height != 0 {
						t.Fatal("initial ledger presence", s, n, r, err)
					}
					if rootAtZero != "" && rootAtZero != r.StateRoot {
						t.Fatal("genesis changed during queries")
					}
					rootAtZero = r.StateRoot
					initialStates = append(initialStates, r)
				}
			}
		}
	}
	tx := func(s, r string, n uint64) *core.Transaction {
		x := core.NewTransaction(s, r, big.NewInt(1), n, time.Now())
		x.IngressSequence = n - 1
		return x
	}
	t1, t2, t3, t4, t5 := tx(a, u, 1), tx(b, u, 2), tx(u, c, 3), tx(d, z, 4), tx(u, a, 5)
	if e = m.SubmitFencedTransactions([]*core.Transaction{t1, t2, t3, t4, t5}); e != nil {
		t.Fatal(e)
	}
	if !clean {
		if _, e = m.ActivateFirstPlacementCreator(ctx, "multi", t2.AdmissionID); e == nil {
			t.Fatal("later creator accepted")
		}
	}
	dispatch, e := m.ActivateFirstPlacementCreator(ctx, "multi", t1.AdmissionID)
	if e != nil {
		t.Fatal(e)
	}
	terminal := map[string]bool{}
	trace := []*message.BlockInfoMsg{}
	lastHeight := map[uint64]uint64{}
	consume := func(r *message.BlockInfoMsg) {
		trace = append(trace, r)
		if uint64(r.Epoch) > lastHeight[r.SenderShardID] {
			lastHeight[r.SenderShardID] = uint64(r.Epoch)
		}
		for _, xs := range [][]*core.Transaction{r.InnerShardTxs, r.Relay2Txs} {
			for _, x := range xs {
				terminal[x.AdmissionID] = true
			}
		}
	}
	var pending *message.BlockInfoMsg
	invalidRejected, delayVerified := false, false
	for len(terminal) < 5 {
		select {
		case pending = <-delayedCreation:
		case r := <-reports:
			consume(r)
		case <-ctx.Done():
			t.Fatal("PBFT completion timeout; inspect node stdout and audit", terminal)
		}
		if pending != nil && terminal[t4.AdmissionID] {
			p, h, err := m.FirstPlacementProgress("multi")
			if err != nil || p.Status != firstplacement.RouteActive || h != 3 {
				t.Fatal("delayed receipt released hold or blocked independent work", p, h, err)
			}
			delayVerified = true
			raw, _ := json.Marshal(pending)
			var bad message.BlockInfoMsg
			json.Unmarshal(raw, &bad)
			bad.FirstCreationReceipts[0].Binding.Token = strings.Repeat("f", 64)
			func() {
				defer func() { invalidRejected = recover() != nil }()
				_ = m.ProcessFirstPlacementBlockReport(&bad)
			}()
			if !invalidRejected {
				t.Fatal("mismatched native receipt accepted")
			}
			if err = m.ProcessFirstPlacementBlockReport(pending); err != nil {
				t.Fatal(err)
			}
			consume(pending)
			if err = m.ProcessFirstPlacementBlockReport(pending); err != nil {
				t.Fatal("duplicate native receipt rejected", err)
			}
			pending = nil
		}
	}
	progress, held, e := m.FirstPlacementProgress("multi")
	if e != nil || held != 0 || progress.Status != firstplacement.CreationCommitted {
		t.Fatal(progress, held, e)
	}
	if clean {
		states := []firstplacement.PresenceResponse{}
		for s := uint64(0); s < 2; s++ {
			for n := uint64(0); n < 4; n++ {
				q := firstplacement.PresenceQuery{Session: session, RequestID: fmt.Sprintf("clean-final-%d", n), Generation: 1, Account: u, Shard: int(s), MinAppliedHeight: lastHeight[s]}
				for {
					r, err := firstplacement.QueryPresence(ctx, table[s][n], q)
					if err == nil && r.Status != "UNAVAILABLE" {
						states = append(states, r)
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("replica catch-up timeout", r, err)
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
		}
		for s := 0; s < 2; s++ {
			for n := 0; n < 4; n++ {
				x, y := states[s*4], states[s*4+n]
				want := "ABSENT"
				if s == 1 {
					want = "EXISTS"
				}
				if y.Status != want || x.Height != y.Height || x.StateRoot != y.StateRoot {
					t.Fatal("replica state mismatch", x, y)
				}
			}
		}
		file, err := os.OpenFile(filepath.Join(root, "result.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		err = json.NewEncoder(file).Encode(map[string]interface{}{"status": "PASS", "initial_ledger_states": initialStates, "mode": "startup-ready fixed-placement trace; no injected faults; not steady-state throughput", "measurement_started": measurementStarted, "measurement_finished": time.Now(), "planned_total": total, "topology": "2x4", "block_size": 1, "block_interval_ms": 100, "dispatch": dispatch, "reservation": reservation, "progress": progress, "reports": trace, "replica_states": states, "held": held})
		ce := file.Close()
		if err != nil || ce != nil {
			t.Fatal(err, ce)
		}
		return
	}
	// Submit old cross-shard work BEFORE acquiring its gate. Query must wait
	// for both native reports, then observe EXISTS at/above their read floors.
	w := account("71")
	old := tx(a, w, 6)
	if e = m.SubmitFencedTransactions([]*core.Transaction{old}); e != nil {
		t.Fatal(e)
	}
	_, oldErr := m.VerifyAndReserveFirstPlacement(ctx, "old-work", []firstplacement.Claim{{Account: w, TargetShard: 0}})
	if oldErr == nil || !strings.Contains(oldErr.Error(), "existing_state_reconciliation_required") {
		t.Fatal("old state missed", oldErr)
	}
	for !terminal[old.AdmissionID] {
		select {
		case r := <-reports:
			consume(r)
		case <-ctx.Done():
			t.Fatal("old-work terminal missing")
		}
	}
	// An actually applied replica must reject an unreachable read frontier.
	lagging, e := firstplacement.QueryPresence(ctx, table[1][1], firstplacement.PresenceQuery{Session: session, RequestID: "future-floor", Generation: 1, Account: u, Shard: 1, MinAppliedHeight: lastHeight[1] + 1000})
	if e != nil || lagging.Status != "UNAVAILABLE" || lagging.Failure != "applied_height_below_query_floor" {
		t.Fatal("unmet frontier accepted", lagging, e)
	}
	// Fault is injected at the supervisor query endpoint, not into PBFT votes.
	unavailableAccount := account("99")
	saved := m.IpNodeTable[1][0]
	m.IpNodeTable[1][0] = "127.0.0.1:0"
	_, unavailableErr := m.VerifyAndReserveFirstPlacement(ctx, "unavailable", []firstplacement.Claim{{Account: unavailableAccount, TargetShard: 1}})
	m.IpNodeTable[1][0] = saved
	if unavailableErr == nil {
		t.Fatal("unavailable query reserved")
	}
	blocked, independent := tx(a, unavailableAccount, 7), tx(d, z, 8)
	if e = m.SubmitFencedTransactions([]*core.Transaction{blocked, independent}); e != nil {
		t.Fatal(e)
	}
	for !terminal[independent.AdmissionID] {
		select {
		case r := <-reports:
			consume(r)
		case <-ctx.Done():
			t.Fatal("unavailable account blocked independent work")
		}
	}
	_, held, e = m.FirstPlacementProgress("multi")
	if e != nil || held != 1 || terminal[blocked.AdmissionID] {
		t.Fatal("unavailable gate not retained", held, e)
	}
	snapshots := []firstplacement.PresenceResponse{}
	for s := uint64(0); s < 2; s++ {
		for n := uint64(0); n < 4; n++ {
			q := firstplacement.PresenceQuery{Session: session, RequestID: fmt.Sprintf("final-%d", n), Generation: 1, Account: u, Shard: int(s), MinAppliedHeight: lastHeight[s]}
			var r firstplacement.PresenceResponse
			for {
				r, e = firstplacement.QueryPresence(ctx, table[s][n], q)
				if e == nil && r.Status != "UNAVAILABLE" {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("replica state unavailable", s, n, r, e)
				case <-time.After(20 * time.Millisecond):
				}
			}
			want := "ABSENT"
			if s == 1 {
				want = "EXISTS"
			}
			if r.Status != want {
				t.Fatal("wrong account placement", r)
			}
			snapshots = append(snapshots, r)
		}
	}
	for s := 0; s < 2; s++ {
		for n := 1; n < 4; n++ {
			x, y := snapshots[s*4], snapshots[s*4+n]
			if x.Height != y.Height || x.StateRoot != y.StateRoot {
				t.Fatal("same-shard state comparison not equal-height/equal-root", x, y)
			}
		}
	}
	result := map[string]interface{}{"status": "PASS", "topology": "2 shards x 4 OS processes", "block_size": 1, "block_interval_ms": 100, "dispatch": dispatch, "reservation": reservation, "progress": progress, "reports": trace, "replica_states": snapshots, "delayed_receipt_hold_verified": delayVerified, "invalid_receipt_rejected": invalidRejected, "old_work_rejected_as_existing": oldErr.Error(), "unavailable_query": unavailableErr.Error(), "lagging_response": lagging, "retained_hold_after_unavailable": held, "covered": "natural PBFT; creation and ordered followups; independent progress during withheld receipt; invalid and duplicate receipt; old cross-shard drain; read frontier refusal; unreachable query endpoint; equal-height replica state", "fault_scope": "supervisor delivery/query boundary controlled; PBFT votes unmodified", "not_covered": []string{"crashed/Byzantine replica recovery", "Byzantine receipt certification", "send-uncertain recovery", "performance"}}
	file, e := os.OpenFile(filepath.Join(root, "result.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	e = json.NewEncoder(file).Encode(result)
	ce := file.Close()
	if e != nil || ce != nil {
		t.Fatal(e, ce)
	}
}
