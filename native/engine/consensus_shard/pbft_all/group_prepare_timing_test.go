package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/message"
	"bytes"
	"log"
	"testing"
)

func TestGroupPrepareTimingIncludesFailedStage(t *testing.T) {
	t.Setenv("SAA_NATIVE_SOURCE_QUEUE_ROOT", "")
	var output bytes.Buffer
	n := &PbftConsensusNode{pl: &pbft_log.PbftLog{Plog: log.New(&output, "", 0)}}
	c := core.GroupPrepareCommand{Members: []core.SourcePrepareCommand{{ProposalID: "failed-prepare"}}}
	if err := n.commitGroupSourcePrepareTimed(c, &message.Request{}, 7, nil); err == nil {
		t.Fatal("missing durable queue directory accepted")
	}
	for _, field := range []string{"proposal=failed-prepare", "seq=7", "status=error", "stage=prepareQueue", "prepareQueueUs=", "marshalUs=0", "chainCommitUs=0", "evidencePublishUs=0", "totalUs="} {
		if !bytes.Contains(output.Bytes(), []byte(field)) {
			t.Fatalf("failure timing missing %s", field)
		}
	}
}
