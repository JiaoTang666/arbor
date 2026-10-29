package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"testing"
	"time"
)

func TestPlacementTelemetryV2InitialSampleBeforeTick(t *testing.T) {
	p:=&PbftConsensusNode{CurChain:&chain.BlockChain{Txpool:core.NewTxPool()}}
	p.placementService.clockID="local-test-clock";p.placementService.origin=time.Now()
	latest:=make(chan message.PlacementTelemetryV2,1);ticks:=make(chan time.Time);done:=make(chan struct{})
	go func(){p.producePlacementTelemetryV2(latest,ticks);close(done)}()
	select {
	case r:=<-latest:
		if r.Sequence!=1 || r.ClockID!="local-test-clock" || r.ReplacedReports!=0 || !r.ServiceMissing {t.Fatal("invalid initial sample",r)}
	case <-time.After(time.Second):t.Fatal("initial sample waited for ticker")
	}
	close(ticks)
	select {case <-done:case <-time.After(time.Second):t.Fatal("producer did not stop")}
	// A follower must wait for ticks/closure, rather than looping in its sample.
	p.NodeID=1;done=make(chan struct{});ticks=make(chan time.Time)
	go func(){p.producePlacementTelemetryV2(latest,ticks);close(done)}()
	close(ticks)
	select {case <-done:case <-time.After(time.Second):t.Fatal("follower busy-looped")}
	select {case <-latest:t.Fatal("follower published leader telemetry");default:}
}

func TestPlacementTelemetryV2LatestReplacementCounter(t *testing.T) {
	latest:=make(chan message.PlacementTelemetryV2,1);var replaced uint64
	for i:=uint64(1);i<=3;i++ {queueLatestPlacementV2(latest,message.PlacementTelemetryV2{Sequence:i},&replaced)}
	r:=<-latest
	if replaced!=2 || r.ReplacedReports!=2 || r.Sequence!=3 {t.Fatal("real replacements not counted",replaced,r)}
	queueLatestPlacementV2(latest,message.PlacementTelemetryV2{Sequence:4},&replaced)
	r=<-latest
	if replaced!=2 || r.ReplacedReports!=2 || r.Sequence!=4 {t.Fatal("free-slot enqueue counted as replacement")}
}

func TestPlacementServiceV2NonemptyLocalDuration(t *testing.T) {
	t.Setenv("SAA_PLACEMENT_TELEMETRY_V2","true")
	p:=&PbftConsensusNode{};at:=time.Now()
	p.notePlacementProposalV2(1,at)
	p.notePlacementCommitV2(1,"block1",600,at.Add(2*time.Second))
	if p.placementService.commitID!="block1" || p.placementService.fill!=.6 || len(p.placementService.durations)!=1 || p.placementService.durations[0]!=2e9 {t.Fatal("bad service duration")}
	p.notePlacementProposalV2(2,at)
	p.notePlacementCommitV2(2,"empty",0,at.Add(20*time.Second))
	if len(p.placementService.durations)!=1 || p.placementService.commitID!="block1" {t.Fatal("empty interval became service stall")}
	p.notePlacementCommitV2(3,"wrong-proposal",10,at.Add(time.Second))
	if len(p.placementService.durations)!=1 {t.Fatal("unmatched proposal used")}
	p.view.Store(1)
	p.notePlacementCommitV2(2,"changed-view",20,at.Add(time.Second))
	if len(p.placementService.durations)!=1 {t.Fatal("cross-view proposal used")}
}
