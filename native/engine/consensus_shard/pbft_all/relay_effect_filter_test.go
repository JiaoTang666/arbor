package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/message"
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"testing"
	"time"
)

func TestRelayCreditFilterRequiresCommittedCredit(t *testing.T) {
	first := &core.Transaction{AdmissionID: "first", Relayed: true}
	done := &core.Transaction{AdmissionID: "done", Relayed: true}
	last := &core.Transaction{AdmissionID: "last", Relayed: true}
	statuses := map[*core.Transaction]chain.TransactionEffectStatus{first: chain.EffectUnseen, done: chain.EffectSeen, last: chain.EffectUnseen}
	got, err := filterRelayCredits([]*core.Transaction{first, done, last}, func(tx *core.Transaction) (chain.TransactionEffectStatus, error) {
		return statuses[tx], nil
	})
	if err != nil || len(got) != 2 || got[0] != first || got[1] != last {
		t.Fatal("committed credit filtering lost unseen work or reordered FIFO", err)
	}
}

func TestRelayCreditFilterUnknownNeverMeansUnseen(t *testing.T) {
	txs := []*core.Transaction{{Relayed: true}, {Relayed: true}}
	for _, readErr := range []error{nil, errors.New("unreadable committed root")} {
		calls := 0
		got, err := filterRelayCredits(txs, func(tx *core.Transaction) (chain.TransactionEffectStatus, error) {
			calls++
			if calls == 1 {
				return chain.EffectUnseen, nil
			}
			return chain.EffectUnknown, readErr
		})
		if err == nil || got != nil {
			t.Fatal("unknown credit admitted a partial batch")
		}
	}
}

func TestRelayCreditFilterDisabledPreservesLegacy(t *testing.T) {
	tx := &core.Transaction{Relayed: false}
	got, err := filterRelayCredits([]*core.Transaction{tx}, func(*core.Transaction) (chain.TransactionEffectStatus, error) {
		return chain.EffectIndexDisabled, nil
	})
	if err != nil || len(got) != 1 || got[0] != tx {
		t.Fatal("disabled effect index changed legacy admission", err)
	}
}

func TestRelayCreditFilterDisabledDoesNotWaitForConsensus(t *testing.T) {
	for _, mode := range []string{"false", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(chain.NativeEffectIndexEnv, mode)
			n := &PbftConsensusNode{}
			n.pbftLock.Lock()
			defer n.pbftLock.Unlock()
			tx := &core.Transaction{Relayed: true}
			done := make(chan error, 1)
			go func() {
				got, err := n.filterCommittedRelayCredits([]*core.Transaction{tx})
				if mode == "false" && (err != nil || len(got) != 1 || got[0] != tx) {
					err = errors.New("disabled index changed the admitted transactions")
				} else if mode == "invalid" && (err == nil || got != nil) {
					err = errors.New("invalid index configuration was not rejected")
				} else {
					err = nil
				}
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("a no-ledger-read admission waited on the consensus lock")
			}
		})
	}
}

func TestRawRelayProofCountMismatchDoesNotAdvanceBoundary(t *testing.T) {
	var output bytes.Buffer
	n := &PbftConsensusNode{pl: &pbft_log.PbftLog{Plog: log.New(&output, "", 0)}, seqIDMap: map[uint64]uint64{1: 3}}
	raw, err := json.Marshal(&message.RelayWithProof{SenderShardID: 1, SenderSeq: 4, Txs: []*core.Transaction{{Relayed: true}}})
	if err != nil {
		t.Fatal(err)
	}
	(&RawRelayOutsideModule{pbftNode: n}).handleRelayWithProof(raw)
	if n.seqIDMap[1] != 3 || !bytes.Contains(output.Bytes(), []byte("count mismatch")) {
		t.Fatal("malformed proven relay advanced the receipt boundary")
	}
}

func TestRawRelayInvalidProofDoesNotAdvanceBoundary(t *testing.T) {
	tx := &core.Transaction{Relayed: true, TxHash: []byte{1}}
	tests := []struct {
		name  string
		tx    *core.Transaction
		proof chain.TxProofResult
	}{
		{"nil_transaction", nil, chain.TxProofResult{}},
		{"hash_mismatch", tx, chain.TxProofResult{Found: true, TxHash: []byte{2}}},
		{"unpaired_proof_nodes", tx, chain.TxProofResult{Found: true, TxHash: []byte{1}, KeyList: [][]byte{{1}}}},
		{"not_found", tx, chain.TxProofResult{Found: false, TxHash: []byte{1}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			n := &PbftConsensusNode{pl: &pbft_log.PbftLog{Plog: log.New(&output, "", 0)}, seqIDMap: map[uint64]uint64{1: 3}}
			raw, err := json.Marshal(&message.RelayWithProof{SenderShardID: 1, SenderSeq: 4, Txs: []*core.Transaction{test.tx}, TxProofs: []chain.TxProofResult{test.proof}})
			if err != nil {
				t.Fatal(err)
			}
			(&RawRelayOutsideModule{pbftNode: n}).handleRelayWithProof(raw)
			if n.seqIDMap[1] != 3 || !bytes.Contains(output.Bytes(), []byte("wrong proof")) {
				t.Fatal("invalid proven relay advanced the receipt boundary")
			}
		})
	}
}
