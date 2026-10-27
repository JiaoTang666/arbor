package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/utils"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"testing"
	"time"
)

// This is a transaction-hash membership proof for exercising the existing
// transport path. It does not claim an authenticated remote debit or commit.
func ownershipIngressHashProof(t *testing.T, tx *core.Transaction) chain.TxProofResult {
	t.Helper()
	header := &core.BlockHeader{Number: 1, TxRoot: chain.GetTxTreeRoot([]*core.Transaction{tx})}
	header.Bloom.Set(utils.ModBytes(tx.TxHash, 2048))
	proof := chain.TxProofGenerateOnTheBlock(tx.TxHash, core.NewBlock(header, []*core.Transaction{tx}))
	if ok, err := chain.TxProofVerify(tx.TxHash, &proof); !ok || err != nil {
		t.Fatalf("invalid transport proof fixture: %v", err)
	}
	return proof
}

func ownershipIngressPayload(t *testing.T, kind message.MessageType, txs []*core.Transaction, proof chain.TxProofResult, seq uint64) []byte {
	t.Helper()
	var envelope interface{}
	switch kind {
	case message.CInject:
		envelope = message.InjectTxs{Txs: txs, ToShardID: 0}
	case message.CRelay:
		envelope = message.Relay{Txs: txs, SenderShardID: 0, SenderSeq: seq}
	case message.CRelayWithProof:
		proofs := make([]chain.TxProofResult, len(txs))
		for i := range proofs {
			proofs[i] = proof
		}
		envelope = message.RelayWithProof{Txs: txs, TxProofs: proofs, SenderShardID: 0, SenderSeq: seq}
	default:
		t.Fatalf("unsupported transport fixture: %s", kind)
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestOwnershipIngressRejectionsDoNotPoisonLaterOccurrence(t *testing.T) {
	open, a, _, c := ownershipIngressFixture(t)
	for _, kind := range []message.MessageType{message.CInject, message.CRelay, message.CRelayWithProof} {
		t.Run(string(kind), func(t *testing.T) {
			shard := uint64(0)
			if kind != message.CInject {
				shard = 1
			}
			module := open(shard)
			node := module.pbftNode
			node.seqIDMap[0] = 3
			node.admissionLegSeen = map[string]string{"unrelated/true": "retained"}
			root := append([]byte(nil), node.CurChain.CurrentBlock.Header.StateRoot...)
			valid := core.NewTransaction(a, c, big.NewInt(1), 4, time.Unix(1, 0))
			valid.RoutingFrozen, valid.RoutedSenderShard, valid.RoutedRecipientShard = true, 0, 1
			valid.Relayed = kind != message.CInject
			valid.AdmissionID, valid.IngressSequence, valid.IngressSequenceAssigned = "ingress/occurrence", 4, true
			proof := ownershipIngressHashProof(t, valid)
			assertUnchanged := func(t *testing.T) {
				t.Helper()
				if node.CurChain.Txpool.GetTxQueueLen() != 0 || node.hasSeenWorkload.Load() || node.lastWorkloadAtMs.Load() != 0 {
					t.Fatal("rejected transport changed queue or workload progress")
				}
				if len(node.seqIDMap) != 1 || node.seqIDMap[0] != 3 || len(node.admissionLegSeen) != 1 || node.admissionLegSeen["unrelated/true"] != "retained" {
					t.Fatal("rejected transport changed relay boundary or admission identities")
				}
				if node.CurChain.CurrentBlock.Header.Number != 0 || !bytes.Equal(root, node.CurChain.CurrentBlock.Header.StateRoot) {
					t.Fatal("rejected transport changed committed state")
				}
			}
			// Preserve the valid prefix's identities even in malformed and nil
			// batches. A rejected batch must not partially admit that prefix.
			for _, raw := range [][]byte{
				[]byte(" null\n"),
				append(ownershipIngressPayload(t, kind, []*core.Transaction{valid}, proof, 99), []byte(" trailing")...),
				ownershipIngressPayload(t, kind, []*core.Transaction{valid, nil}, proof, 99),
			} {
				module.HandleMessageOutsidePBFT(kind, raw)
				assertUnchanged(t)
			}
			// A valid transaction in an inconsistent envelope must not advance
			// another shard's sequence key. This is not sender authentication.
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal(ownershipIngressPayload(t, kind, []*core.Transaction{valid}, proof, 99), &envelope); err != nil {
				t.Fatal(err)
			}
			field := "SenderShardID"
			if kind == message.CInject {
				field = "ToShardID"
			}
			envelope[field] = json.RawMessage("1") // The frozen debit owner is 0.
			inconsistent, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			module.HandleMessageOutsidePBFT(kind, inconsistent)
			assertUnchanged(t)
			for _, rejection := range []struct {
				name   string
				mutate func(*core.Transaction)
			}{
				{"wrong_owner", func(tx *core.Transaction) { tx.RoutedRecipientShard = 0 }},
				{"account_alias", func(tx *core.Transaction) { tx.Recipient = "0x" + tx.Recipient }},
				{"stage_spoof", func(tx *core.Transaction) { tx.Relayed = !tx.Relayed }},
				{"unfrozen_route", func(tx *core.Transaction) { tx.RoutingFrozen = false }},
				{"nil_value", func(tx *core.Transaction) { tx.Value = nil }},
			} {
				t.Run(rejection.name, func(t *testing.T) {
					bad := core.DecodeTx(valid.Encode())
					rejection.mutate(bad)
					if !bytes.Equal(bad.TxHash, valid.TxHash) || bad.AdmissionID != valid.AdmissionID || bad.IngressSequence != valid.IngressSequence {
						t.Fatal("rejection fixture did not retain the later valid identity")
					}
					raw := ownershipIngressPayload(t, kind, []*core.Transaction{valid, bad}, proof, 99)
					// No recover: any panic from untrusted input fails this test.
					module.HandleMessageOutsidePBFT(kind, raw)
					module.HandleMessageOutsidePBFT(kind, raw)
					assertUnchanged(t)
				})
			}
			module.HandleMessageOutsidePBFT(kind, ownershipIngressPayload(t, kind, []*core.Transaction{valid}, proof, 7))
			if node.CurChain.Txpool.GetTxQueueLen() != 1 || !node.hasSeenWorkload.Load() {
				t.Fatal("valid occurrence did not progress after rejected versions with the same identities")
			}
			wantSeq := uint64(3)
			if kind != message.CInject {
				wantSeq = 7
			}
			if len(node.seqIDMap) != 1 || node.seqIDMap[0] != wantSeq {
				t.Fatal("valid transport did not preserve the expected relay boundary")
			}
			got := node.CurChain.Txpool.PackTxs(1)
			if len(got) != 1 || !bytes.Equal(got[0].Encode(), valid.Encode()) {
				t.Fatal("queue retained a rejected version of the valid occurrence")
			}
			workloadAt := node.lastWorkloadAtMs.Load()
			module.HandleMessageOutsidePBFT(kind, ownershipIngressPayload(t, kind, []*core.Transaction{}, proof, 8))
			if kind != message.CInject {
				wantSeq = 8
			}
			if node.CurChain.Txpool.GetTxQueueLen() != 0 || node.lastWorkloadAtMs.Load() != workloadAt || node.seqIDMap[0] != wantSeq {
				t.Fatal("structured empty transport lost the legacy relay watermark behavior")
			}
		})
	}
}

func TestOwnershipIngressDisabledKeepsLegacyAdmission(t *testing.T) {
	t.Setenv("SAA_FIRST_PLACEMENT_FENCE", "")
	t.Setenv(chain.NativeEffectIndexEnv, "false")
	for _, kind := range []message.MessageType{message.CInject, message.CRelay, message.CRelayWithProof} {
		t.Run(string(kind), func(t *testing.T) {
			node := &PbftConsensusNode{CurChain: &chain.BlockChain{Txpool: core.NewTxPool()}, seqIDMap: map[uint64]uint64{},
				pl: &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}}
			module := &RawRelayOutsideModule{pbftNode: node}
			valid := core.NewTransaction("legacy-sender", "legacy-recipient", big.NewInt(1), 4, time.Unix(1, 0))
			valid.Relayed = kind != message.CInject
			proof := ownershipIngressHashProof(t, valid)
			bad := core.DecodeTx(valid.Encode())
			bad.RoutedRecipientShard, bad.Recipient, bad.Relayed = 99, "0xlegacy-recipient", !valid.Relayed
			module.HandleMessageOutsidePBFT(kind, ownershipIngressPayload(t, kind, []*core.Transaction{valid, bad}, proof, 7))
			if module.ownershipIngressEnabled() || node.CurChain.Txpool.GetTxQueueLen() != 2 || !node.hasSeenWorkload.Load() || len(node.admissionLegSeen) != 0 {
				t.Fatal("disabled ownership enforcement changed legacy batch admission")
			}
			if kind != message.CInject && node.seqIDMap[0] != 7 {
				t.Fatal("disabled ownership enforcement changed legacy relay progress")
			}
			got := node.CurChain.Txpool.PackTxs(2)
			if len(got) != 2 || !bytes.Equal(got[0].Encode(), valid.Encode()) || !bytes.Equal(got[1].Encode(), bad.Encode()) {
				t.Fatal("disabled ownership enforcement changed legacy FIFO transport")
			}
			// Decoder errors retain their legacy panic contract when security is off.
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("disabled ownership enforcement changed malformed JSON behavior")
					}
				}()
				module.HandleMessageOutsidePBFT(kind, []byte("{"))
			}()
			// Security-off also retains its historical empty null-envelope path.
			module.HandleMessageOutsidePBFT(kind, []byte(" null\n"))
			if node.CurChain.Txpool.GetTxQueueLen() != 0 || (kind != message.CInject && node.seqIDMap[0] != 0) {
				t.Fatal("disabled ownership enforcement changed null-envelope behavior")
			}
		})
	}
}
