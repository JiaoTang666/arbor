package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestCreatorBindingNativeIngress(t *testing.T) {
	t.Setenv("SAA_FIRST_PLACEMENT_FENCE", "true")
	tx := core.NewTransaction(strings.Repeat("a", 40), strings.Repeat("b", 40), big.NewInt(1), 0, time.Now())
	tx.AdmissionID = "session/1"
	tx.RoutingFrozen = true
	tx.MappingVersion = 1
	tx.RoutedSenderShard = 0
	tx.RoutedRecipientShard = 1
	tx.FirstPlacement = &core.FirstPlacementBinding{Session: "session", RequestID: "request", Token: strings.Repeat("1", 64), AdmissionID: tx.AdmissionID, TxHash: hex.EncodeToString(tx.TxHash), Generation: 1, MappingVersion: 1, SourceShard: 0, TargetShard: 1, Claims: map[string]uint64{tx.Recipient: 1}}
	node := func(shard uint64) *PbftConsensusNode {
		return &PbftConsensusNode{ShardID: shard, presenceSession: "session", presenceCache: map[string]presenceCached{"request/" + tx.Recipient: {Query: firstplacement.PresenceQuery{Session: "session", RequestID: "request", Account: tx.Recipient, Generation: 1, Shard: int(shard)}, Response: firstplacement.PresenceResponse{Status: "ABSENT"}}}}
	}
	source := node(0)
	if len(source.filterAdmissionOccurrences([]*core.Transaction{tx}, false)) != 1 {
		t.Fatal("creator rejected")
	}
	copy := core.DecodeTx(tx.Encode())
	if copy.FirstPlacement.Validate(copy, "session") != nil {
		t.Fatal("binding lost in gob")
	}
	if len(source.filterAdmissionOccurrences([]*core.Transaction{copy}, false)) != 0 {
		t.Fatal("retry re-admitted")
	}
	copy.Relayed = true
	target := node(1)
	if len(target.filterAdmissionOccurrences([]*core.Transaction{copy}, true)) != 1 {
		t.Fatal("relay binding rejected")
	}
	for _, mode := range []string{"missing_proof", "old_generation", "wrong_version", "wrong_token_replay"} {
		t.Run(mode, func(t *testing.T) {
			n := node(0)
			x := core.DecodeTx(tx.Encode())
			switch mode {
			case "missing_proof":
				n.presenceCache = nil
			case "old_generation":
				x.FirstPlacement.Generation++
			case "wrong_version":
				x.MappingVersion++
			case "wrong_token_replay":
				n.filterAdmissionOccurrences([]*core.Transaction{tx}, false)
				x.FirstPlacement.Token = strings.Repeat("2", 64)
			}
			defer func() {
				if recover() == nil {
					t.Fatal("invalid binding accepted", mode)
				}
			}()
			n.filterAdmissionOccurrences([]*core.Transaction{x}, false)
		})
	}
}
