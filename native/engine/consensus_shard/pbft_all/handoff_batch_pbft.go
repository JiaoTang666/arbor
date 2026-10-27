package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

const migrationHandoffBatchRequest message.RequestType = "SEFMigrationHandoffBatchV1"

func decodeMigrationHandoffBatch(r *message.Request) ([]core.MigrationHandoff, error) {
	var hs []core.MigrationHandoff
	if r == nil || r.RequestType != migrationHandoffBatchRequest {
		return nil, fmt.Errorf("wrong handoff batch type")
	}
	if len(r.Msg.Content) > 8<<20 {
		return nil, fmt.Errorf("handoff batch too large")
	}
	e := json.Unmarshal(r.Msg.Content, &hs)
	return hs, e
}

// Inputs have already passed LoadMigrationOutbox's validation. Do not sort or
// repeatedly decode unrelated/completed records. The seen cache is only the
// existing optimization: after restart it is empty, so durable replay remains.
func (n *PbftConsensusNode) pendingHandoffOrder(records []*core.MigrationHandoff) []*core.MigrationHandoff {
	type entry struct {
		h        *core.MigrationHandoff
		sequence uint64
	}
	entries := make([]entry, 0, len(records))
	for _, h := range records {
		if h.TargetShard != n.ShardID {
			continue
		}
		if v, seen := n.sourcePrepareSeen.Load("handoff:" + h.ID); seen && v == true {
			continue
		}
		entries = append(entries, entry{h, core.DecodeTx(h.Original).IngressSequence})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].sequence != entries[j].sequence {
			return entries[i].sequence < entries[j].sequence
		}
		return entries[i].h.ID < entries[j].h.ID
	})
	ordered := make([]*core.MigrationHandoff, len(entries))
	for i, e := range entries {
		ordered[i] = e.h
	}
	return ordered
}
func (n *PbftConsensusNode) handoffBatchProposal() (*message.Request, error) {
	records, e := n.indexedHandoffs(os.Getenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR"), func(h *core.MigrationHandoff) bool {
		if h.TargetShard != n.ShardID {
			return false
		}
		v, seen := n.sourcePrepareSeen.Load("handoff:" + h.ID)
		return !seen || v != true
	})
	if e != nil {
		return nil, e
	}
	records = n.pendingHandoffOrder(records)
	hs := []core.MigrationHandoff{}
	size := 2
	for _, h := range records {
		if h.TargetShard != n.ShardID {
			continue
		}
		if v, seen := n.sourcePrepareSeen.Load("handoff:" + h.ID); seen && v == true {
			continue
		}
		active, e := n.CurChain.TargetActivated(h.Account)
		if e != nil {
			return nil, e
		}
		if !active {
			continue
		}
		raw, _ := json.Marshal(h)
		if size+len(raw)+1 > 8<<20 {
			break
		}
		hs = append(hs, *h)
		size += len(raw) + 1
		if uint64(len(hs)) >= n.CurChain.ChainConfig.BlockSize {
			break
		}
	}
	if len(hs) == 0 {
		return nil, nil
	}
	if e = n.CurChain.ValidateMigrationHandoffBatch(hs); e != nil {
		return nil, e
	}
	raw, e := json.Marshal(hs)
	if e != nil {
		return nil, e
	}
	return &message.Request{RequestType: migrationHandoffBatchRequest, ReqTime: time.Unix(0, 0).UTC(), Msg: message.RawMessage{Content: raw}}, nil
}
func (n *PbftConsensusNode) commitMigrationHandoffBatch(r *message.Request, digest []byte) error {
	started := time.Now()
	if string(getDigest(r)) != string(digest) {
		return fmt.Errorf("handoff batch digest mismatch")
	}
	hs, e := decodeMigrationHandoffBatch(r)
	if e != nil {
		return e
	}
	if e = n.CurChain.CommitMigrationHandoffBatch(hs); e != nil {
		return e
	}
	publications := &migrationPublications{}
	stateDone := time.Now()
	for _, h := range hs {
		// The shared root and every dedup marker are already committed.
		// This existing path now only replays the marker and publishes receipts.
		raw, e := json.Marshal(h)
		if e != nil {
			return e
		}
		one := &message.Request{RequestType: migrationHandoffRequest, ReqTime: r.ReqTime, Msg: message.RawMessage{Content: raw}}
		if e = n.commitMigrationHandoffPublishing(one, getDigest(one), publications.add, publications.mark); e != nil {
			return e
		}
	}
	prepared := time.Now()
	if e := publications.flush(n); e != nil {
		return e
	}
	if n.pl != nil {
		n.pl.Plog.Printf("migration commit phases type=%s validateStateMs=%d receiptBuildMs=%d publishMs=%d totalMs=%d", r.RequestType, stateDone.Sub(started).Milliseconds(), prepared.Sub(stateDone).Milliseconds(), time.Since(prepared).Milliseconds(), time.Since(started).Milliseconds())
	}
	return nil
}
