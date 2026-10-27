package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"context"
	"encoding/json"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Real TCP handler + real committed tries + actual committee aggregation.
// This does not run PBFT voting or claim creator release/placement performance.
func TestAdmissionPresenceNativeTCPAndReservation(t *testing.T) {
	t.Setenv("SAA_FIRST_PLACEMENT_FENCE", "true")
	for _, name := range []string{"SAA_NATIVE_TARGET_INSTALL_PATH", "SAA_NATIVE_TARGET_ACTIVATION_PATH", "SAA_NATIVE_SOURCE_PREPARE_PATH"} {
		t.Setenv(name, "")
	}
	t.Setenv("BLOCK_EMULATOR_RUNTIME_UNKNOWN_ADMISSION_ENABLED", "false")
	t.Setenv("BLOCK_EMULATOR_RUNTIME_MAPPING_BOUNDARY_ENABLED", "false")
	t.Setenv("BLOCK_EMULATOR_RUNTIME_EXTERNAL_ACTION_ENABLED", "false")
	t.Setenv("BLOCK_EMULATOR_SAA_DECISION_MODE", "legacy")
	root := t.TempDir()
	t.Setenv("SAA_FIRST_PLACEMENT_AUDIT", filepath.Join(root, "admission.jsonl"))
	oldRoot, oldDB, oldShards := params.ExpDataRootDir, params.DatabaseWrite_path, params.ShardNum
	defer func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.ShardNum = oldShards
	}()
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + "/"
	params.ShardNum = 2
	nodes := []*PbftConsensusNode{}
	table := map[uint64]map[uint64]string{}
	var wg sync.WaitGroup
	listeners := []net.Listener{}
	for s := 0; s < 2; s++ {
		bc, e := chain.NewBlockChain(&params.ChainConfig{ShardID: uint64(s), ShardNums: 2, Nodes_perShard: 1, BlockSize: 4}, rawdb.NewMemoryDatabase())
		if e != nil {
			t.Fatal(e)
		}
		n := &PbftConsensusNode{CurChain: bc, ShardID: uint64(s)}
		n.ohm = &RawRelayOutsideModule{pbftNode: n}
		nodes = append(nodes, n)
		ln, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		listeners = append(listeners, ln)
		table[uint64(s)] = map[uint64]string{0: ln.Addr().String()}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				conn, e := ln.Accept()
				if e != nil {
					return
				}
				wg.Add(1)
				go func() { defer wg.Done(); n.handleClientRequest(conn) }()
			}
		}()
	}
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
		wg.Wait()
		for _, n := range nodes {
			n.CurChain.CloseBlockChain()
		}
	}()
	m := committee.NewRelayCommitteeModule(table, nil, nil, "", 0, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e := m.StartFirstPlacementFence(ctx); e != nil {
		t.Fatal(e)
	}
	// After startup nodes are allowed to reach different heights.
	unrelated := strings.Repeat("c", 40)
	nodes[1].pbftLock.Lock()
	nodes[1].CurChain.Update_PartitionMap(unrelated, 1)
	nodes[1].CurChain.AddAccounts([]string{unrelated}, []*core.AccountState{{Balance: big.NewInt(2)}}, 0)
	nodes[1].pbftLock.Unlock()
	account := strings.Repeat("a", 40)
	r, e := m.VerifyAndReserveFirstPlacement(ctx, "native-query", []firstplacement.Claim{{Account: account, TargetShard: 1}})
	if e != nil || r.Status != firstplacement.Reserved {
		t.Fatal(r, e)
	}
	if len(r.Request.Proofs) != 2 || r.Request.Proofs[0].CommittedHeight == r.Request.Proofs[1].CommittedHeight {
		t.Fatal("different native heights not retained", r)
	}
	q := firstplacement.PresenceQuery{Session: r.Request.PresenceSession, RequestID: r.Request.ID, Generation: r.Request.PresenceGeneration, Account: account, Shard: 0}
	cached, e := firstplacement.QueryPresence(ctx, table[0][0], q)
	if e != nil {
		t.Fatal(e)
	}
	// Same generation is an immutable response even after an unrelated commit.
	nodes[0].pbftLock.Lock()
	nodes[0].CurChain.Update_PartitionMap(unrelated, 0)
	nodes[0].CurChain.AddAccounts([]string{unrelated}, []*core.AccountState{{Balance: big.NewInt(2)}}, 0)
	nodes[0].pbftLock.Unlock()
	again, e := firstplacement.QueryPresence(ctx, table[0][0], q)
	if e != nil || again.StateRoot != cached.StateRoot || !again.ObservedAt.Equal(cached.ObservedAt) {
		t.Fatal("retry response changed", again, e)
	}
	q.Generation++
	conflict, e := firstplacement.QueryPresence(ctx, table[0][0], q)
	if e != nil || conflict.Status != "UNAVAILABLE" {
		t.Fatal(conflict, e)
	}
	_, e = m.VerifyAndReserveFirstPlacement(ctx, "existing", []firstplacement.Claim{{Account: unrelated, TargetShard: 1}})
	if e == nil || !strings.Contains(e.Error(), "multiple_shard_state_invariant_violation") {
		t.Fatal("multiple existing states not detected", e)
	}
	hello := firstplacement.PresenceQuery{Session: "different-supervisor", Shard: 0, Hello: true}
	refused, e := firstplacement.QueryPresence(ctx, table[0][0], hello)
	if e != nil || refused.Status != "UNAVAILABLE" {
		t.Fatal("restart adopted old run", refused, e)
	}
	if path := os.Getenv("SAA_FENCE_TEST_EVIDENCE"); path != "" {
		data, _ := json.MarshalIndent(map[string]interface{}{"status": "PASS", "reservation": r, "coverage": "native TCP and committed tries; PBFT voting and creator release not exercised"}, "", "  ")
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		_, e = f.Write(data)
		f.Close()
		if e != nil {
			t.Fatal(e)
		}
		audit, e := os.ReadFile(filepath.Join(root, "admission.jsonl"))
		if e != nil {
			t.Fatal(e)
		}
		af, e := os.OpenFile(path+".audit.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		_, e = af.Write(audit)
		af.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
}

func TestAdmissionTransportDuplicateCannotExecuteAfterTerminal(t *testing.T) {
	t.Setenv("SAA_FIRST_PLACEMENT_FENCE", "true")
	n := &PbftConsensusNode{ShardID: 0, presenceSession: "run"}
	tx := core.NewTransaction(strings.Repeat("a", 40), strings.Repeat("b", 40), big.NewInt(1), 0, time.Now())
	tx.AdmissionID = "run/1"
	tx.RoutingFrozen = true
	tx.RoutedSenderShard = 0
	tx.RoutedRecipientShard = 1
	if len(n.filterAdmissionOccurrences([]*core.Transaction{tx}, false)) != 1 {
		t.Fatal("first missing")
	}
	if len(n.filterAdmissionOccurrences([]*core.Transaction{core.DecodeTx(tx.Encode())}, false)) != 0 {
		t.Fatal("duplicate admitted")
	}
	target := &PbftConsensusNode{ShardID: 1, presenceSession: "run"}
	tx.Relayed = true
	if len(target.filterAdmissionOccurrences([]*core.Transaction{tx}, true)) != 1 || len(target.filterAdmissionOccurrences([]*core.Transaction{tx}, true)) != 0 {
		t.Fatal("relay duplicate admitted")
	}
}
