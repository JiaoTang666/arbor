package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"bufio"
	"context"
	"encoding/json"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// One continuous request through actual TCP presence/injection/relay/reports,
// actual native queues and the production RawRelay commit handler. Commit is
// explicitly driven: this does NOT exercise PBFT proposal/prepare/commit votes.
func TestFirstPlacementNativeClosure(t *testing.T) {
	t.Setenv("SAA_FIRST_PLACEMENT_FENCE", "true")
	for _, k := range []string{"SAA_NATIVE_TARGET_INSTALL_PATH", "SAA_NATIVE_TARGET_ACTIVATION_PATH", "SAA_NATIVE_SOURCE_PREPARE_PATH"} {
		t.Setenv(k, "")
	}
	for _, k := range []string{"BLOCK_EMULATOR_RUNTIME_UNKNOWN_ADMISSION_ENABLED", "BLOCK_EMULATOR_RUNTIME_MAPPING_BOUNDARY_ENABLED", "BLOCK_EMULATOR_RUNTIME_EXTERNAL_ACTION_ENABLED"} {
		t.Setenv(k, "false")
	}
	t.Setenv("BLOCK_EMULATOR_SAA_DECISION_MODE", "legacy")
	root := t.TempDir()
	t.Setenv("SAA_FIRST_PLACEMENT_AUDIT", filepath.Join(root, "audit.jsonl"))
	oldRoot, oldDB, oldData, oldShards, oldProof := params.ExpDataRootDir, params.DatabaseWrite_path, params.DataWrite_path, params.ShardNum, params.RelayWithMerkleProof
	oldDelay, oldJitter, oldBW := params.Delay, params.JitterRange, params.Bandwidth
	params.ExpDataRootDir = root
	params.DatabaseWrite_path = filepath.Join(root, "database") + "/"
	params.DataWrite_path = filepath.Join(root, "metrics") + "/"
	params.ShardNum = 2
	params.RelayWithMerkleProof = 0
	params.Delay = 0
	params.JitterRange = 0
	params.Bandwidth = 1 << 24
	networks.InitNetworkTools()
	defer func() {
		params.ExpDataRootDir = oldRoot
		params.DatabaseWrite_path = oldDB
		params.DataWrite_path = oldData
		params.ShardNum = oldShards
		params.RelayWithMerkleProof = oldProof
		params.Delay = oldDelay
		params.JitterRange = oldJitter
		params.Bandwidth = oldBW
	}()
	table := map[uint64]map[uint64]string{}
	nodes := []*PbftConsensusNode{}
	listeners := []net.Listener{}
	var wg sync.WaitGroup
	for s := uint64(0); s < 2; s++ {
		config := &params.ChainConfig{ShardID: s, ShardNums: 2, Nodes_perShard: 1, BlockSize: 1}
		bc, e := chain.NewBlockChain(config, rawdb.NewMemoryDatabase())
		if e != nil {
			t.Fatal(e)
		}
		n := &PbftConsensusNode{CurChain: bc, ShardID: s, pbftChainConfig: config, ip_nodeTable: table, seqIDMap: map[uint64]uint64{}, requestPool: map[string]*message.Request{}, pl: &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}}
		n.ohm = &RawRelayOutsideModule{pbftNode: n}
		nodes = append(nodes, n)
		ln, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		listeners = append(listeners, ln)
		table[s] = map[uint64]string{0: ln.Addr().String()}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c, e := ln.Accept()
				if e != nil {
					return
				}
				wg.Add(1)
				go func() { defer wg.Done(); n.handleClientRequest(c) }()
			}
		}()
	}
	supervisor, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	listeners = append(listeners, supervisor)
	table[params.SupervisorShard] = map[uint64]string{0: supervisor.Addr().String()}
	m := committee.NewRelayCommitteeModule(table, nil, nil, "", 0, 1)
	reports := make(chan *message.BlockInfoMsg, 8)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, e := supervisor.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				reader := bufio.NewReader(c)
				for {
					raw, e := reader.ReadBytes('\n')
					if e != nil {
						return
					}
					kind, payload := message.SplitMessage(raw)
					if kind != message.CBlockInfo {
						continue
					}
					var b message.BlockInfoMsg
					if e := json.Unmarshal(payload, &b); e != nil {
						panic(e)
					}
					if e := m.ProcessFirstPlacementBlockReport(&b); e != nil {
						panic(e)
					}
					reports <- &b
				}
			}()
		}
	}()
	defer func() {
		networks.CloseAllConnInPool()
		for _, l := range listeners {
			l.Close()
		}
		wg.Wait()
		for _, n := range nodes {
			n.CurChain.CloseBlockChain()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e = m.StartFirstPlacementFence(ctx); e != nil {
		t.Fatal(e)
	}
	a, u := strings.Repeat("0", 38)+"10", strings.Repeat("0", 38)+"20"
	reservation, e := m.VerifyAndReserveFirstPlacement(ctx, "closure", []firstplacement.Claim{{Account: u, TargetShard: 1}})
	if e != nil {
		t.Fatal(e)
	}
	first := core.NewTransaction(a, u, big.NewInt(1), 0, time.Now())
	next := core.NewTransaction(u, a, big.NewInt(1), 1, time.Now())
	if e = m.SubmitFencedTransactions([]*core.Transaction{first, next}); e != nil {
		t.Fatal(e)
	}
	dispatch, e := m.ActivateFirstPlacementCreator(ctx, "closure", first.AdmissionID)
	if e != nil {
		t.Fatal(e)
	}
	waitPool := func(n *PbftConsensusNode) {
		t.Helper()
		for {
			n.CurChain.Txpool.GetLocked()
			count := len(n.CurChain.Txpool.TxQueue)
			n.CurChain.Txpool.GetUnlocked()
			if count > 0 {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("native queue timeout", n.ShardID)
			case <-time.After(time.Millisecond):
			}
		}
	}
	commit := func(n *PbftConsensusNode) *message.BlockInfoMsg {
		t.Helper()
		waitPool(n)
		n.pbftLock.Lock()
		block := n.CurChain.GenerateBlock(0)
		n.requestPool[string(block.Hash)] = &message.Request{Msg: message.RawMessage{Content: block.Encode()}, ReqTime: time.Now()}
		(&RawRelayPbftExtraHandleMod{pbftNode: n}).HandleinCommit(&message.Commit{Digest: block.Hash, SeqID: block.Header.Number})
		n.pbftLock.Unlock()
		select {
		case b := <-reports:
			return b
		case <-ctx.Done():
			t.Fatal("commit report timeout")
			return nil
		}
	}
	b1 := commit(nodes[0])
	progress, held, e := m.FirstPlacementProgress("closure")
	if e != nil || len(b1.FirstCreationReceipts) != 0 || progress.Status != firstplacement.RouteActive || held != 1 {
		t.Fatal("source leg released HOLD", progress, held, e)
	}
	b2 := commit(nodes[1])
	progress, held, e = m.FirstPlacementProgress("closure")
	if e != nil || len(b2.FirstCreationReceipts) != 1 || progress.Status != firstplacement.CreationCommitted || held != 0 {
		t.Fatal("creation did not release HOLD", progress, held, e)
	}
	if nodes[0].CurChain.ObserveAccountPresence(u, time.Now()).Status != chain.AccountAbsent || nodes[1].CurChain.ObserveAccountPresence(u, time.Now()).Status != chain.AccountPresent {
		t.Fatal("wrong state location")
	}
	b3 := commit(nodes[1])
	b4 := commit(nodes[0])
	if len(b3.Relay1Txs) != 1 || b3.Relay1Txs[0].AdmissionID != next.AdmissionID || !b3.Relay1Txs[0].SenderRequiresExistingState || len(b4.Relay2Txs) != 1 {
		t.Fatal("released occurrence did not complete using existing state")
	}
	state := nodes[1].CurChain.FetchAccounts([]string{u})[0]
	if state.Nonce != 1 || state.Balance.Cmp(params.Init_Balance) != 0 {
		t.Fatal("incorrect created account state", state)
	}
	// A duplicate genuine commit report must not send held work again.
	if e = m.ProcessFirstPlacementBlockReport(b2); e != nil {
		t.Fatal(e)
	}
	if path := os.Getenv("SAA_CLOSURE_EVIDENCE"); path != "" {
		data, e := json.MarshalIndent(map[string]interface{}{"status": "PASS", "scope": "one request; real native TCP/queues/RawRelay commit handler; PBFT voting NOT COVERED", "reservation": reservation, "dispatch": dispatch, "final": progress, "held": held, "reports": []*message.BlockInfoMsg{b1, b2, b3, b4}, "account_nonce": state.Nonce, "account_balance": state.Balance.String()}, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		file, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		_, e = file.Write(data)
		ce := file.Close()
		if e != nil || ce != nil {
			t.Fatal(e, ce)
		}
		audit, e := os.ReadFile(filepath.Join(root, "audit.jsonl"))
		if e != nil {
			t.Fatal(e)
		}
		af, e := os.OpenFile(path+".audit.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		_, e = af.Write(audit)
		ce = af.Close()
		if e != nil || ce != nil {
			t.Fatal(e, ce)
		}
	}
}
