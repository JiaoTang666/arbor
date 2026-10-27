package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"bytes"
	"log"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestGroupHandoffLogSeparatesContactRecoveryAndSubmittedConfirmation(t *testing.T) {
	n, _ := confirmationPreparedNode(t)
	var output bytes.Buffer
	n.pl = &pbft_log.PbftLog{Plog: log.New(&output, "", 0)}
	held := core.NewTransaction(confirmationMemberA, confirmationMemberB, big.NewInt(1), 1, time.Unix(1, 0))
	held.RoutingFrozen = true
	inbound := core.NewTransaction(confirmationOutsider, confirmationMemberA, big.NewInt(1), 2, time.Unix(1, 0))
	inbound.RoutingFrozen, inbound.RoutedRecipientShard = true, 1
	n.CurChain.Txpool.AddTxs2Pool([]*core.Transaction{held, inbound})
	if err := n.pollGroupSourceHandoff(); err != nil {
		t.Fatal(err)
	}
	awaitGroupSourcePublication(t, n)
	text := output.String()
	if !strings.Contains(text, "contact_pending=2 recovery_due=1 publication_submitted=1 confirmation_ready=0 confirmation_submitted=0") {
		t.Fatal("handoff progress populations or stages conflated", text)
	}
	if strings.Contains(text, " completed=") || strings.Contains(text, " confirmMs=") {
		t.Fatal("enqueue masqueraded as durable completion", text)
	}
}

func TestRetirementTraceMarksUnobservedQueue(t *testing.T) {
	n, _ := confirmationPreparedNode(t)
	var output bytes.Buffer
	n.pl = &pbft_log.PbftLog{Plog: log.New(&output, "", 0)}
	trace := newRetirementTrace()
	trace.status = "confirmation_running"
	trace.log(n)
	if text := output.String(); !strings.Contains(text, "contact_pending=-1 recovery_due=-1") || !strings.Contains(text, "drainStatus=not_observed") {
		t.Fatal("retirement diagnostic implied no debt before observing it", text)
	}
}
