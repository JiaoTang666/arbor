package committee

import (
	"blockEmulator/message"
	"encoding/json"
	"fmt"
	"sort"
)

type lightTelemetryObservationV2 struct {
	Report message.PlacementTelemetryV2 `json:"report"`
	ReceivedMonoNS int64 `json:"received_mono_ns"`
	ServiceFirstReceivedMonoNS int64 `json:"service_first_received_mono_ns"`
	ClockID string `json:"supervisor_clock_id"`
	Valid bool `json:"valid"`
	IdentityInvalid bool `json:"identity_invalid"`
}

func (p *placementSignals) receiveLightV2(r message.PlacementTelemetryV2) (bool,placementClockReading,error) {
	p.mu.Lock();defer p.mu.Unlock()
	at:=p.clock.now()
	ok,e:=p.observeLightV2Locked(r,at)
	return ok,at,e
}

func (p *placementSignals) observeLightV2Locked(r message.PlacementTelemetryV2, at placementClockReading) (bool, error) {
	if r.Schema != "placement_telemetry_v2" || r.Shard>=uint64(len(p.rows)) || r.Node!=r.View || r.ClockID=="" || r.Sequence==0 || r.CapturedMonoNS<0 || r.Queue<0 || at.ClockID!=p.clock.id || at.MonoNS<0 || !finite(r.LastNonemptyFill) || r.LastNonemptyFill<0 || r.LastNonemptyFill>1 || !finite(r.MeanNonemptyDurationNS) || r.MeanNonemptyDurationNS<0 {
		return false, fmt.Errorf("invalid placement telemetry v2")
	}
	if !r.ServiceMissing && (r.LastNonemptyCommitID=="" || r.MeanNonemptyDurationNS<=0) { return false, fmt.Errorf("invalid nonempty service evidence") }
	if len(p.lightRows)==0 { p.lightRows=make([]lightTelemetryObservationV2,len(p.rows)) }
	x:=&p.lightRows[r.Shard]
	if x.IdentityInvalid { return false,nil }
	// This finite fresh-start experiment pins initial leader 0 and the first
	// process instance. A leader/restart change cannot be authenticated by a
	// telemetry packet: invalidate rather than let a late packet reclaim it.
	if r.Node!=0 || x.Report.ClockID!="" && (r.Node!=x.Report.Node || r.ClockID!=x.Report.ClockID) {
		x.Valid=false;x.IdentityInvalid=true;return false,nil
	}
	if x.Valid && (r.Sequence<=x.Report.Sequence || r.CapturedMonoNS<x.Report.CapturedMonoNS) { return false,nil }
	serviceAt:=x.ServiceFirstReceivedMonoNS
	if !x.Valid || r.ClockID!=x.Report.ClockID || r.LastNonemptyCommitID!=x.Report.LastNonemptyCommitID {
		serviceAt=at.MonoNS
	}
	if r.ServiceMissing { serviceAt=0 }
	x.Report,x.ReceivedMonoNS,x.ServiceFirstReceivedMonoNS,x.ClockID,x.Valid=r,at.MonoNS,serviceAt,at.ClockID,true
	return true,nil
}

func (m *RelayCommitteeModule) handleLightTelemetryV2(raw []byte) {
	if m.placementSignals==nil { return }
	var r message.PlacementTelemetryV2
	if e:=json.Unmarshal(raw,&r); e!=nil { panic(e) }
	accepted,at,e:=m.placementSignals.receiveLightV2(r)
	if e!=nil { panic(e) }
	if m.nativeAudit!=nil {
		m.nativeAudit.write(map[string]interface{}{"event":"light_telemetry_received","report":r,"accepted":accepted,"clock_id":at.ClockID,"received_mono_ns":at.MonoNS,"at":at.Wall.UTC(),"usage":"observation only; never work confirmation"})
	}
}

// Already validated immutable report values are copied with work/history under
// one lock. No node clock is subtracted from a supervisor clock.
func (p *placementSignals) lightSnapshotV2(origin int64) ([]placementSignal, []lightTelemetryObservationV2, placementClockReading, *sharePendingSample) {
	p.mu.Lock()
	defer p.mu.Unlock()
	at:=p.clock.now()
	rows:=append([]placementSignal(nil),p.rows...)
	observed:=make([]lightTelemetryObservationV2,len(rows))
	copy(observed,p.lightRows)
	var past *sharePendingSample
	if origin>=0 && at.MonoNS-origin>=5e9 {
		i:=p.historyHead+sort.Search(len(p.history)-p.historyHead,func(i int)bool{return p.history[p.historyHead+i].MonoNS>at.MonoNS-5e9})-1
		if i>=p.historyHead { x:=p.history[i];x.Pending=append([]int64(nil),x.Pending...);past=&x }
	}
	return rows,observed,at,past
}
