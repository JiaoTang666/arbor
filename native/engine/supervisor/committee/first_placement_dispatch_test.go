package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/message"
	"blockEmulator/params"
	"bufio"
	"context"
	"encoding/json"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Controlled presence responders and a real TCP injection write. Native trie
// and PBFT voting are not exercised by this supervisor regression.
func dispatchFixture(t *testing.T) (*RelayCommitteeModule, chan message.InjectTxs, context.Context) {
	t.Helper()
	// Existing full-lifecycle ordering tests explicitly exercise the reference mode.
	t.Setenv("SAA_LEGACY_SERIAL_ADMISSION", "true")
	old := params.ShardNum
	params.ShardNum = 2
	t.Cleanup(func() { params.ShardNum = old })
	m := &RelayCommitteeModule{admission: &RuntimeUnknownAdmissionManager{shardNum: 2}, IpNodeTable: map[uint64]map[uint64]string{}}
	f := newFirstAdmissionRuntime(m)
	f.started = true
	f.ledger = firstplacement.NewFence(f.session)
	m.firstAdmission = f
	injected := make(chan message.InjectTxs, 8)
	var wg sync.WaitGroup
	listeners := []net.Listener{}
	for s := uint64(0); s < 2; s++ {
		ln, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		listeners = append(listeners, ln)
		m.IpNodeTable[s] = map[uint64]string{0: ln.Addr().String()}
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
					c.SetReadDeadline(time.Now().Add(3 * time.Second))
					raw, e := bufio.NewReader(c).ReadBytes('\n')
					if e != nil || len(raw) < 30 {
						return
					}
					kind, payload := message.SplitMessage(raw)
					if string(kind) == firstplacement.PresenceWireType {
						var q firstplacement.PresenceQuery
						if json.Unmarshal(payload, &q) != nil {
							return
						}
						json.NewEncoder(c).Encode(firstplacement.PresenceResponse{Query: q, Status: "ABSENT", StateRoot: strings.Repeat("1", 64), ObservedAt: time.Now()})
					} else if kind == message.CInject {
						var batch message.InjectTxs
						if json.Unmarshal(payload, &batch) == nil {
							injected <- batch
						}
					}
				}()
			}
		}()
	}
	t.Cleanup(func() {
		for _, ln := range listeners {
			ln.Close()
		}
		wg.Wait()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return m, injected, ctx
}
func reserveAndHold(t *testing.T, m *RelayCommitteeModule, ctx context.Context) (*core.Transaction, *core.Transaction) {
	t.Helper()
	a, u := strings.Repeat("0", 39)+"0", strings.Repeat("0", 39)+"1"
	if _, e := m.VerifyAndReserveFirstPlacement(ctx, "request", []firstplacement.Claim{{Account: u, TargetShard: 0}}); e != nil {
		t.Fatal(e)
	}
	first := core.NewTransaction(a, u, big.NewInt(1), 0, time.Now())
	next := core.NewTransaction(u, a, big.NewInt(1), 1, time.Now())
	if m.firstAdmission.admit(first) || m.firstAdmission.admit(next) {
		t.Fatal("gate failed")
	}
	return first, next
}
func TestFirstPlacementDispatchOnlyDesignatedCreator(t *testing.T) {
	m, received, ctx := dispatchFixture(t)
	first, next := reserveAndHold(t, m, ctx)
	before := core.DecodeTx(first.Encode())
	m.firstAdmission.bindFixedRoute(before, 1)
	r, e := m.ActivateFirstPlacementCreator(ctx, "request", first.AdmissionID)
	if e != nil || !r.TransportWritten || r.MappingVersion != 1 {
		t.Fatal(r, e)
	}
	var batch message.InjectTxs
	select {
	case batch = <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if len(batch.Txs) != 1 {
		t.Fatal(batch)
	}
	tx := batch.Txs[0]
	if tx.AdmissionID != first.AdmissionID || tx.RoutedRecipientShard != 0 || tx.FirstPlacement == nil || tx.FirstPlacement.Validate(tx, m.firstAdmission.session) != nil {
		t.Fatal("wrong bound creator", tx)
	}
	if before.RoutedRecipientShard != 1 || before.MappingVersion != 0 {
		t.Fatal("old frozen snapshot changed")
	}
	if m.firstAdmission.ledger.HeldCount() != 1 {
		t.Fatal("other HOLD released")
	}
	again, e := m.ActivateFirstPlacementCreator(ctx, "request", first.AdmissionID)
	if e != nil || again.Token != r.Token {
		t.Fatal("retry not idempotent", again, e)
	}
	if _, e = m.ActivateFirstPlacementCreator(ctx, "request", next.AdmissionID); e == nil {
		t.Fatal("second creator allowed")
	}
	m.firstAdmission.report(&message.BlockInfoMsg{InnerShardTxs: []*core.Transaction{tx}, SenderShardID: 0, CommitTime: time.Now()})
	if m.firstAdmission.ledger.HeldCount() != 1 {
		t.Fatal("terminal report alone released HOLD")
	}
	later := core.NewTransaction(first.Sender, first.Recipient, big.NewInt(1), 2, time.Now())
	if m.firstAdmission.admit(later) {
		t.Fatal("gate removed at activation")
	}
	select {
	case extra := <-received:
		t.Fatal("duplicate send", extra)
	default:
	}
	if path := os.Getenv("SAA_DISPATCH_TEST_EVIDENCE"); path != "" {
		file, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		e = json.NewEncoder(file).Encode(map[string]interface{}{"status": "PASS", "dispatch": r, "received_transaction": tx, "remaining_hold": m.firstAdmission.ledger.HeldCount(), "coverage": "real TCP write with controlled shard responders; no native commit or PBFT voting"})
		ce := file.Close()
		if e != nil || ce != nil {
			t.Fatal(e, ce)
		}
	}
}
func TestFirstPlacementDispatchFailsBeforeMutation(t *testing.T) {
	for _, mode := range []string{"stale_version", "legacy_expired_epoch", "expired_proof", "wrong_creator", "connect_failure", "other_lease"} {
		t.Run(mode, func(t *testing.T) {
			m, _, ctx := dispatchFixture(t)
			first, _ := reserveAndHold(t, m, ctx)
			f := m.firstAdmission
			id := first.AdmissionID
			switch mode {
			case "stale_version":
				m.admission.routeVersion = 4
			case "legacy_expired_epoch":
				m.observedEpoch = 10
				r := f.reservations["request"]
				r.Request.PresenceGeneration = 0
				f.reservations["request"] = r
			case "expired_proof":
				r := f.reservations["request"]
				r.Request.Proofs[0].ReceivedAt = time.Now().Add(-time.Hour)
				f.reservations["request"] = r
			case "wrong_creator":
				id = "missing"
			case "connect_failure":
				m.IpNodeTable[0][0] = "127.0.0.1:0"
			case "other_lease":
				if _, e := f.ledger.Begin("other", []string{first.Sender}, time.Now()); e != nil {
					t.Fatal(e)
				}
			}
			version := m.admission.routeVersion
			if _, e := m.ActivateFirstPlacementCreator(ctx, "request", id); e == nil {
				t.Fatal("failure accepted", mode)
			}
			if m.admission.routeVersion != version || len(m.admission.runtimeShardByAccount) != 0 || f.ledger.HeldCount() != 2 || f.reservations["request"].Status != firstplacement.Reserved {
				t.Fatal("failure mutated route/hold")
			}
		})
	}
}

func TestNativeReservationSurvivesUnrelatedEpochProgress(t *testing.T) {
	m, received, ctx := dispatchFixture(t)
	first, _ := reserveAndHold(t, m, ctx)
	m.observedEpoch = 100
	result, err := m.ActivateFirstPlacementCreator(ctx, "request", first.AdmissionID)
	if err != nil || !result.TransportWritten {
		t.Fatal(result, err)
	}
	select {
	case packet := <-received:
		if len(packet.Txs) != 1 || packet.Txs[0].RoutingEpoch != 101 {
			t.Fatal("route did not retain actual epoch annotation")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if m.firstAdmission.ledger.HeldCount() != 1 {
		t.Fatal("creation completion protection released early")
	}
}
