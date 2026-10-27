package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

const inboundDebitBatchRequest message.RequestType = "SEFInboundDebitBatchV1"
const handoffRelayBatchRequest message.RequestType = "SEFHandoffRelayBatchV1"

func auxiliaryBatch(r *message.Request) bool {
	return r != nil && (r.RequestType == inboundDebitBatchRequest || r.RequestType == handoffRelayBatchRequest)
}

func (n *PbftConsensusNode) validateAuxiliaryBatch(r *message.Request) error {
	if !auxiliaryBatch(r) || len(r.Msg.Content) > 8<<20 {
		return fmt.Errorf("invalid auxiliary batch envelope")
	}
	if r.RequestType == inboundDebitBatchRequest {
		var items []chain.InboundDebitItem
		if e := json.Unmarshal(r.Msg.Content, &items); e != nil {
			return e
		}
		return n.CurChain.ValidateInboundDebitBatch(items)
	}
	var items []chain.HandoffExecution
	if e := json.Unmarshal(r.Msg.Content, &items); e != nil {
		return e
	}
	return n.CurChain.ValidateHandoffRelayBatch(items)
}

func (n *PbftConsensusNode) commitAuxiliaryBatch(r *message.Request, digest []byte) error {
	started := time.Now()
	var stateDone time.Time
	if string(getDigest(r)) != string(digest) {
		return fmt.Errorf("auxiliary digest mismatch")
	}
	if e := n.validateAuxiliaryBatch(r); e != nil {
		return e
	}
	publications := &migrationPublications{}
	publish := func(kind message.RequestType, item interface{}) error {
		raw, e := json.Marshal(item)
		if e != nil {
			return e
		}
		one := &message.Request{RequestType: kind, ReqTime: r.ReqTime, Msg: message.RawMessage{Content: raw}}
		if kind == inboundDebitRequest {
			return n.commitInboundDebitPublishing(one, getDigest(one), publications.add)
		}
		return n.commitHandoffRelayPublishing(one, getDigest(one), publications.add, publications.mark)
	}
	if r.RequestType == inboundDebitBatchRequest {
		var items []chain.InboundDebitItem
		json.Unmarshal(r.Msg.Content, &items)
		if e := n.CurChain.CommitInboundDebitBatch(items); e != nil {
			return e
		}
		stateDone = time.Now()
		for _, item := range items {
			if e := publish(inboundDebitRequest, item); e != nil {
				return e
			}
		}
	} else {
		var items []chain.HandoffExecution
		json.Unmarshal(r.Msg.Content, &items)
		if e := n.CurChain.CommitHandoffRelayBatch(items); e != nil {
			return e
		}
		stateDone = time.Now()
		for _, item := range items {
			if e := publish(handoffRelayRequest, item); e != nil {
				return e
			}
		}
	}
	prepared := time.Now()
	if err := publications.flush(n); err != nil {
		return err
	}
	if n.pl != nil {
		n.pl.Plog.Printf("migration commit phases type=%s validateStateMs=%d receiptBuildMs=%d publishMs=%d totalMs=%d", r.RequestType, stateDone.Sub(started).Milliseconds(), prepared.Sub(stateDone).Milliseconds(), time.Since(prepared).Milliseconds(), time.Since(started).Milliseconds())
		n.pl.Plog.Printf("auxiliary batch committed type=%s block=%d", r.RequestType, n.CurChain.CurrentBlock.Header.Number)
	}
	return nil
}

func (n *PbftConsensusNode) relayBatchProposal() (*message.Request, error) {
	if r, e := n.redirectCreditProposal(); e != nil || r != nil {
		return r, e
	}
	dir := os.Getenv("SAA_NATIVE_HANDOFF_RELAY_DIR")
	if dir == "" {
		return nil, nil
	}
	entries, e := n.indexedArtifacts(dir, ".relay.json")
	if e != nil {
		return nil, e
	}
	items := []chain.HandoffExecution{}
	for _, id := range entries.IDs() {
		raw := entries[id]
		var item chain.HandoffExecution
		if e = json.Unmarshal(raw, &item); e != nil {
			return nil, e
		}
		if e = core.ValidateMigrationHandoff(&item.Handoff); e != nil {
			return nil, e
		}
		tx := core.DecodeTx(item.Handoff.Execution)
		if tx.RoutedRecipientShard != n.ShardID {
			continue
		}
		if done, ok := n.sourcePrepareSeen.Load("relay:" + item.Handoff.ID); ok && done == true {
			continue
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := core.DecodeTx(items[i].Handoff.Original), core.DecodeTx(items[j].Handoff.Original)
		if a.IngressSequence != b.IngressSequence {
			return a.IngressSequence < b.IngressSequence
		}
		return items[i].Handoff.ID < items[j].Handoff.ID
	})
	selected := []chain.HandoffExecution{}
	size := 2
	for _, item := range items {
		if e = n.CurChain.ValidateHandoffRelay(item); e != nil {
			if redirectErr := n.CurChain.ValidateRelayRedirect(item); redirectErr != nil {
				return nil, e
			}
			if len(selected) == 0 {
				return makeRedirectRequest(relayReserveRequest, item)
			}
			break
		}
		raw, e := json.Marshal(item)
		if e != nil {
			return nil, e
		}
		if size+len(raw)+1 > 8<<20 || uint64(len(selected)) >= n.CurChain.ChainConfig.BlockSize {
			break
		}
		selected = append(selected, item)
		size += len(raw) + 1
	}
	if len(selected) == 0 {
		return nil, nil
	}
	if e = n.CurChain.ValidateHandoffRelayBatch(selected); e != nil {
		return nil, e
	}
	raw, e := json.Marshal(selected)
	if e != nil {
		return nil, e
	}
	return &message.Request{RequestType: handoffRelayBatchRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: raw}}, nil
}

// Four work-conserving lanes. Stable consensus sequence, not wall time, rotates
// first service. Ordinary packing still enforces all source/target state fences.
func chooseFairHandoff(sequence uint64, ordinary bool, lanes [3]func() (*message.Request, error)) (*message.Request, error) {
	for offset := uint64(0); offset < 4; offset++ {
		lane := (sequence + offset) % 4
		if lane == 0 {
			if ordinary {
				return nil, nil
			}
			continue
		}
		r, e := lanes[lane-1]()
		if e != nil || r != nil {
			return r, e
		}
	}
	return nil, nil
}
func (n *PbftConsensusNode) fairHandoffProposal() (*message.Request, error) {
	return chooseFairHandoff(n.sequenceID, n.CurChain.Txpool.HasOrdinaryPending(), [3]func() (*message.Request, error){n.inboundProposal, n.relayBatchProposal, n.migrationExecutionProposal})
}
