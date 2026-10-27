package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/message"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Synthetic receipts test the consumer contract only. The native closure test
// separately generates receipts from the production committed-state reader.
func TestFirstPlacementReceiptGuardAndRelease(t *testing.T) {
	m, received, ctx := dispatchFixture(t)
	first, next := reserveAndHold(t, m, ctx)
	if _, e := m.ActivateFirstPlacementCreator(ctx, "request", first.AdmissionID); e != nil {
		t.Fatal(e)
	}
	var tx *core.Transaction
	select {
	case batch := <-received:
		tx = batch.Txs[0]
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	b := &message.BlockInfoMsg{InnerShardTxs: []*core.Transaction{tx}, SenderShardID: 0, Epoch: 1, CommitTime: time.Now(), CommittedBlockHash: strings.Repeat("1", 64), CommittedStateRoot: strings.Repeat("2", 64)}
	f := m.firstAdmission
	f.report(b)
	if f.ledger.HeldCount() != 1 {
		t.Fatal("terminal without receipt released HOLD")
	}
	r := core.FirstCreationReceipt{Binding: *tx.FirstPlacement, Account: tx.Recipient, Shard: 0, BlockHeight: 1, BlockHash: b.CommittedBlockHash, StateRoot: b.CommittedStateRoot, ParentStateRoot: strings.Repeat("3", 64), StateHash: strings.Repeat("4", 64), ObservedAt: time.Now()}
	b.FirstCreationReceipts = []core.FirstCreationReceipt{r}
	for _, mode := range []string{"token", "height", "root", "account", "state_hash", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			raw, _ := json.Marshal(b)
			var bad message.BlockInfoMsg
			json.Unmarshal(raw, &bad)
			switch mode {
			case "token":
				bad.FirstCreationReceipts[0].Binding.Token = strings.Repeat("9", 64)
			case "height":
				bad.FirstCreationReceipts[0].BlockHeight++
			case "root":
				bad.FirstCreationReceipts[0].StateRoot = strings.Repeat("9", 64)
			case "account":
				bad.FirstCreationReceipts[0].Account = tx.Sender
			case "state_hash":
				bad.FirstCreationReceipts[0].StateHash = ""
			case "transaction":
				bad.InnerShardTxs[0].MappingVersion++
			}
			if e := f.validateCreationReceipts(&bad); e == nil {
				t.Fatal("invalid receipt accepted")
			}
			if f.ledger.HeldCount() != 1 || f.reservations["request"].Status != firstplacement.RouteActive {
				t.Fatal("validation mutated lifecycle")
			}
		})
	}
	f.report(b)
	select {
	case batch := <-received:
		if len(batch.Txs) != 1 || batch.Txs[0].AdmissionID != next.AdmissionID || !batch.Txs[0].SenderRequiresExistingState {
			t.Fatal("incorrect release")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if f.ledger.HeldCount() != 0 || f.reservations["request"].Status != firstplacement.CreationCommitted {
		t.Fatal("not completed")
	}
	f.report(b)
	select {
	case extra := <-received:
		t.Fatal("duplicate release", extra)
	default:
	}
}
