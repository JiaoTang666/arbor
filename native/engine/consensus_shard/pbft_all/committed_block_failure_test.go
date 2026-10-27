package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/message"
	"bytes"
	"fmt"
	"log"
	"math/big"
	"strings"
	"testing"
	"time"
)

// The quorum-to-application boundary must fail visibly, not quietly return
// false with the consensus round held forever. No network or PBFT votes are
// simulated: this is a bounded production commit-handler failure regression.
func TestCommittedBlockWriteFailureStopsBeforeRelayAndReport(t *testing.T) {
	for _, mode := range []string{"raw_relay", "raw_broker", "clpa", "clpa_broker"} {
		t.Run(mode, func(t *testing.T) {
			open, sender, _, recipient := ownershipIngressFixture(t)
			node := open(0).pbftNode
			var output bytes.Buffer
			node.pl = &pbft_log.PbftLog{Plog: log.New(&output, "", 0)}
			node.sequenceID = 1
			tx := core.NewTransaction(sender, recipient, big.NewInt(1), 1, time.Unix(1, 0))
			tx.RoutingFrozen, tx.RoutedSenderShard, tx.RoutedRecipientShard = true, 0, 1
			node.CurChain.SendTx2Pool([]*core.Transaction{tx})
			proposal := node.CurChain.GenerateBlock(0)
			request := &message.Request{RequestType: message.BlockRequest, ReqTime: time.Unix(2, 0)}
			request.Msg.Content = proposal.Encode()
			digest := getDigest(request)
			node.requestPool = map[string]*message.Request{string(digest): request}
			head := node.CurChain.CurrentBlock
			if err := node.CurChain.Storage.DataBase.Close(); err != nil {
				t.Fatal(err)
			}
			var handler interface{ HandleinCommit(*message.Commit) bool }
			switch mode {
			case "raw_relay":
				handler = &RawRelayPbftExtraHandleMod{pbftNode: node}
			case "raw_broker":
				handler = &RawBrokerPbftExtraHandleMod{pbftNode: node}
			case "clpa":
				handler = &CLPAPbftInsideExtraHandleMod{pbftNode: node}
			case "clpa_broker":
				handler = &CLPAPbftInsideExtraHandleMod_forBroker{pbftNode: node}
			}
			var failure interface{}
			func() {
				defer func() { failure = recover() }()
				handler.HandleinCommit(&message.Commit{Digest: digest, SeqID: 1})
			}()
			if failure == nil || !strings.Contains(fmt.Sprint(failure), "native committed block application failed:") {
				t.Fatal("commit failure was silent or failed outside the application boundary", failure)
			}
			if node.CurChain.CurrentBlock != head || node.sequenceID != 1 || len(node.CurChain.Txpool.RelayPool) != 0 {
				t.Fatal("failed commit advanced the head/round or published Relay work")
			}
			state, err := node.CurChain.ExportAccountState(sender)
			if err != nil || state.State.Balance.Int64() != 100 || state.State.Nonce != 0 {
				t.Fatal("failed block storage exposed the proposed debit", state, err)
			}
			for _, marker := range []string{"added the block", "main node is trying", "sended excuted txs"} {
				if strings.Contains(output.String(), marker) {
					t.Fatal("failed commit reached a post-commit Relay/report branch", marker)
				}
			}
		})
	}
}
