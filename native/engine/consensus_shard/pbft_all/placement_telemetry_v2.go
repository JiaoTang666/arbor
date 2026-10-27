package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"sync"
	"time"
)

type placementServiceV2 struct {
	mu sync.Mutex
	clockID string
	origin time.Time
	proposalAt time.Time
	proposalHeight uint64
	proposalView int32
	commitView int32
	commitID string
	fill float64
	durations []float64
}

func (p *PbftConsensusNode) notePlacementProposalV2(height uint64, at time.Time) {
	if os.Getenv("SAA_PLACEMENT_TELEMETRY_V2") != "true" { return }
	p.placementService.mu.Lock()
	p.placementService.proposalHeight, p.placementService.proposalAt = height, at
	p.placementService.proposalView=p.view.Load()
	p.placementService.mu.Unlock()
}

func (p *PbftConsensusNode) notePlacementCommitV2(height uint64, id string, count int, at time.Time) {
	if os.Getenv("SAA_PLACEMENT_TELEMETRY_V2") != "true" || count == 0 { return }
	s := &p.placementService
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proposalHeight != height || s.proposalView!=p.view.Load() || s.proposalAt.IsZero() || !at.After(s.proposalAt) { return }
	s.commitView=p.view.Load()
	s.commitID, s.fill = id, float64(count)/1000.
	s.durations = append(s.durations, float64(at.Sub(s.proposalAt).Nanoseconds()))
	if len(s.durations)>5 { s.durations = s.durations[len(s.durations)-5:] }
}

// The ticker samples only O(1) counters. A single sender consumes the latest
// observation, so a slow network cannot accumulate goroutines or block PBFT.
func (p *PbftConsensusNode) startPlacementTelemetryV2() {
	if os.Getenv("SAA_PLACEMENT_TELEMETRY_V2") != "true" { return }
	var id [16]byte
	if _, e := rand.Read(id[:]); e != nil { panic(e) }
	s := &p.placementService
	s.mu.Lock()
	s.clockID, s.origin = hex.EncodeToString(id[:]), time.Now()
	s.mu.Unlock()
	latest := make(chan message.PlacementTelemetryV2, 1)
	go func() {
		for report := range latest {
			raw, e := json.Marshal(report)
			if e != nil { panic(e) }
			networks.TcpDial(message.MergeMessage(message.CPlacementTelemetryV2, raw), p.ip_nodeTable[params.SupervisorShard][0])
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		defer close(latest)
		p.producePlacementTelemetryV2(latest,ticker.C)
	}()
}

// Exactly one producer owns replaced. Count only an observation that was
// actually removed from the one-slot queue, not a network error or a race in
// which the sender had already taken the previous observation.
func queueLatestPlacementV2(latest chan message.PlacementTelemetryV2, report message.PlacementTelemetryV2, replaced *uint64) {
	if cap(latest)!=1 { panic("placement telemetry requires one latest slot") }
	report.ReplacedReports=*replaced
	select { case latest<-report:return;default: }
	select { case <-latest:(*replaced)++;default: }
	report.ReplacedReports=*replaced
	latest<-report
}

func (p *PbftConsensusNode) producePlacementTelemetryV2(latest chan message.PlacementTelemetryV2,ticks <-chan time.Time) {
	var seq,replaced uint64
	sample:=func() {
		view:=p.view.Load()
		if p.stopSignal.Load() || view<0 || p.NodeID!=uint64(view) {return}
		q:=p.CurChain.Txpool.GetTxQueueLen();s:=&p.placementService
		s.mu.Lock();seq++
		r:=message.PlacementTelemetryV2{Schema:"placement_telemetry_v2",Shard:p.ShardID,Node:p.NodeID,View:uint64(view),ClockID:s.clockID,Sequence:seq,CapturedMonoNS:time.Since(s.origin).Nanoseconds(),Queue:q,LastNonemptyCommitID:s.commitID,LastNonemptyFill:s.fill,ServiceMissing:len(s.durations)==0 || s.commitView!=view}
		for _,d:=range s.durations {r.MeanNonemptyDurationNS+=d/float64(len(s.durations))}
		if r.ServiceMissing {r.LastNonemptyCommitID="";r.LastNonemptyFill=0;r.MeanNonemptyDurationNS=0}
		s.mu.Unlock()
		if p.view.Load()==view {queueLatestPlacementV2(latest,r,&replaced)}
	}
	sample() // Publish the initial observation before waiting for the first tick.
	for range ticks {
		if p.stopSignal.Load() {return}
		sample()
	}
}
