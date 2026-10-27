package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"bufio"
	"context"
	"encoding/json"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Real supervisor txSending / report hooks and RPC client, with controlled
// shard responders. The separate PBFT test covers actual trie-backed responses.
func TestAdmissionFenceSendingDrainAndSixteenQueries(t *testing.T) {
	t.Run("legacy", func(t *testing.T) { testAdmissionFenceSendingDrain(t, 0) })
	t.Run("paced", func(t *testing.T) { testAdmissionFenceSendingDrain(t, 1000) })
}

func testAdmissionFenceSendingDrain(t *testing.T, offeredTPS float64) {
	oldDelay, oldJitter, oldBandwidth := params.Delay, params.JitterRange, params.Bandwidth
	params.Delay = 0
	params.JitterRange = 0
	params.Bandwidth = 1 << 24
	networks.InitNetworkTools()
	defer func() {
		networks.CloseAllConnInPool()
		params.Delay = oldDelay
		params.JitterRange = oldJitter
		params.Bandwidth = oldBandwidth
	}()
	oldShards, oldSpeed := params.ShardNum, params.InjectSpeed
	params.ShardNum = 16
	params.InjectSpeed = 8
	defer func() { params.ShardNum = oldShards; params.InjectSpeed = oldSpeed }()
	m := &RelayCommitteeModule{admission: &RuntimeUnknownAdmissionManager{shardNum: 16}, IpNodeTable: map[uint64]map[uint64]string{}}
	m.offeredLoadTPS = offeredTPS
	f := newFirstAdmissionRuntime(m)
	f.ledger = firstplacement.NewFence(f.session)
	f.started = true
	m.firstAdmission = f
	var queries, sent atomic.Int32
	var wg sync.WaitGroup
	listeners := []net.Listener{}
	for s := 0; s < 16; s++ {
		ln, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		listeners = append(listeners, ln)
		m.IpNodeTable[uint64(s)] = map[uint64]string{0: ln.Addr().String()}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c, e := ln.Accept()
				if e != nil {
					return
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer c.Close()
					c.SetReadDeadline(time.Now().Add(2 * time.Second))
					raw, e := bufio.NewReader(c).ReadBytes('\n')
					if e != nil || len(raw) < 30 {
						return
					}
					kind := strings.TrimRight(string(raw[:30]), "\x00")
					if kind == firstplacement.PresenceWireType {
						queries.Add(1)
						var q firstplacement.PresenceQuery
						if json.Unmarshal(raw[30:], &q) != nil {
							return
						}
						json.NewEncoder(c).Encode(firstplacement.PresenceResponse{Query: q, Status: "ABSENT", Height: uint64(10 + q.Shard), StateRoot: strings.Repeat("1", 64), ObservedAt: time.Now()})
					} else if kind == string(message.CInject) {
						var batch message.InjectTxs
						json.Unmarshal(raw[30:], &batch)
						for _, tx := range batch.Txs {
							if tx.AdmissionID == "" || !tx.RoutingFrozen {
								t.Error("unregistered send")
							}
							sent.Add(1)
						}
					}
				}()
			}
		}()
	}
	defer func() {
		for _, ln := range listeners {
			ln.Close()
		}
		wg.Wait()
	}()
	a, u := strings.Repeat("0", 39)+"0", strings.Repeat("0", 39)+"1"
	old := core.NewTransaction(a, u, big.NewInt(1), 1, time.Now())
	m.txSending([]*core.Transaction{old})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		r, e := m.VerifyAndReserveFirstPlacement(ctx, "drain", []firstplacement.Claim{{Account: u, TargetShard: 2}})
		if e == nil && r.Status != firstplacement.Reserved {
			e = context.Canceled
		}
		done <- e
	}()
	// Wait only for the gate linearization point, not an arbitrary sleep.
	deadline := time.Now().Add(time.Second)
	for {
		if _, _, e := f.ledger.Lease("drain"); e == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("gate not installed")
		}
		time.Sleep(time.Millisecond)
	}
	newcomer := core.NewTransaction(a, u, big.NewInt(1), 2, time.Now())
	newcomer.IngressSequence = 1
	m.txSending([]*core.Transaction{newcomer})
	f.report(&message.BlockInfoMsg{SenderShardID: 0, CommitTime: time.Now(), Relay1Txs: []*core.Transaction{old}})
	if queries.Load() != 0 {
		t.Fatal("query overtook old relay2")
	}
	l, n, e := f.ledger.Lease("drain")
	if e != nil || n != 1 || l.Querying {
		t.Fatal(l, n, e)
	}
	if f.ledger.HeldCount() != 1 {
		t.Fatal("new transaction escaped gate")
	}
	f.report(&message.BlockInfoMsg{SenderShardID: 1, CommitTime: time.Now(), Relay2Txs: []*core.Transaction{old}})
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if queries.Load() != 16 {
		t.Fatal("incomplete all-shard query", queries.Load())
	}
	if f.ledger.HeldCount() != 1 {
		t.Fatal("reservation released held creator")
	}
	deadline = time.Now().Add(time.Second)
	for sent.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("old send missing", sent.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPresenceRPCTimeoutCannotReturnProof(t *testing.T) {
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, e := ln.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(time.Second))
		bufio.NewReader(c).ReadBytes('\n')
		b := make([]byte, 1)
		c.Read(b)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, e := firstplacement.QueryPresence(ctx, ln.Addr().String(), firstplacement.PresenceQuery{Session: "s"}); e == nil {
		t.Fatal("missing response accepted")
	}
	<-done
}
