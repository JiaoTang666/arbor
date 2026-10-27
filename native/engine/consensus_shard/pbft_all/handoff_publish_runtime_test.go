package pbft_all

import (
	"blockEmulator/core"
	"math/big"
	"testing"
	"time"
)

func TestNodeHandoffPublisherReuseAndIsolation(t *testing.T) {
	dir := t.TempDir()
	a := "0000000000000000000000000000000000000010"
	tx := core.NewTransaction(a, a, big.NewInt(1), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, err := core.BuildMigrationHandoff(tx, "runtime-publisher", a, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	n := &PbftConsensusNode{}
	for i := 0; i < 5; i++ {
		if err = n.publishSourceHandoff(dir, h); err != nil {
			t.Fatal(err)
		}
	}
	v, ok := n.sourcePrepareSeen.Load("handoff-durable-publisher-v1:" + dir)
	if !ok {
		t.Fatal("publisher not retained")
	}
	d, c, e := v.(*core.HandoffPublisher).PublicationCounts()
	if d != 1 || c != 4 || e != 1 {
		t.Fatal(d, c, e)
	}
	other := &PbftConsensusNode{}
	if err = other.publishSourceHandoff(dir, h); err != nil {
		t.Fatal(err)
	}
	v, _ = other.sourcePrepareSeen.Load("handoff-durable-publisher-v1:" + dir)
	d, c, _ = v.(*core.HandoffPublisher).PublicationCounts()
	if d != 1 || c != 0 {
		t.Fatal("shared another node's durability cache", d, c)
	}
}
