package committee

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"bufio"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"
)

func zeroLightModelV2() *shareModel {
	m:=zeroShareModel();m.Schema="entry_share_residual_v2";m.FeatureOrder=lightFeatureNamesV2
	m.ActorParameters=1138;m.DecisionGranularity="each_newnew_first_binding"
	m.Normalizer.Schema=m.Schema;m.Normalizer.Decisions="every newnew first binding"
	m.ArtifactMetadata=json.RawMessage(`{"calibration":{"source":"frozen development evidence"}}`)
	m.Normalizer.Mean=make([]float64,20);m.Normalizer.Scale=make([]float64,20)
	for i:=range m.Normalizer.Scale { m.Normalizer.Scale[i]=1 }
	m.Normalizer.Features=lightFeatureNamesV2;m.Normalizer.UnnormalizedColumns=[]int{3,5,9,13,18,19}
	for i:=range m.Encoder.Weight { m.Encoder.Weight[i]=make([]float64,20) }
	return m
}

func TestLightV2ZeroResidualAndContract(t *testing.T) {
	raw:=make([][]float64,16)
	for s:=range raw { raw[s]=make([]float64,20);raw[s][0]=float64(s);raw[s][3]=1;raw[s][1]=1000 }
	_,_,_,p,e:=shareProbabilities(raw,nil);if e!=nil {t.Fatal(e)}
	m:=zeroLightModelV2();data,_:=json.Marshal(m);parsed,e:=parseShareModel(data);if e!=nil {t.Fatal(e)}
	_,d,_,q,e:=shareProbabilities(raw,parsed);if e!=nil || !reflect.DeepEqual(p,q) {t.Fatal("zero model changed pressure",e)}
	for _,v:=range d {if v!=0 {t.Fatal("nonzero residual")}}
	raw[0][3]=0
	_,_,_,r,e:=shareProbabilities(raw,nil);if e!=nil || r[0]>=r[1] {t.Fatal("queue max ignored",e)}
	m.FeatureOrder=shareFeatureNames;data,_=json.Marshal(m)
	if _,e=parseShareModel(data);e==nil {t.Fatal("old feature meanings accepted")}
	m=zeroLightModelV2();m.ActorParameters=1266;data,_=json.Marshal(m)
	if _,e=parseShareModel(data);e==nil {t.Fatal("wrong declared architecture accepted")}
	m=zeroLightModelV2();m.DecisionGranularity="batch1000";data,_=json.Marshal(m)
	if _,e=parseShareModel(data);e==nil {t.Fatal("old decision frequency accepted")}
	m=zeroLightModelV2();m.Normalizer.Schema="entry_share_residual_v1";data,_=json.Marshal(m)
	if _,e=parseShareModel(data);e==nil {t.Fatal("old normalizer schema accepted")}
}

func TestLightTelemetryV2PinnedIdentityAndNoConfirmation(t *testing.T) {
	p:=newPlacementSignals(16);p.assign(0,1)
	r:=message.PlacementTelemetryV2{Schema:"placement_telemetry_v2",Shard:0,Node:0,View:0,ClockID:"node0",Sequence:1,CapturedMonoNS:0,Queue:12,LastNonemptyCommitID:"block1",LastNonemptyFill:.5,MeanNonemptyDurationNS:1e9}
	ok,first,e:=p.receiveLightV2(r);if e!=nil || !ok {t.Fatal(e)}
	if p.rows[0].Confirmed!=0 {t.Fatal("queue report confirmed work")}
	r.Sequence=2;r.CapturedMonoNS=100
	ok,_,e=p.receiveLightV2(r);if e!=nil || !ok || p.lightRows[0].ServiceFirstReceivedMonoNS!=first.MonoNS {t.Fatal("heartbeat refreshed commit evidence")}
	r.Sequence=1
	if ok,_,e=p.receiveLightV2(r);e!=nil || ok {t.Fatal("old report accepted")}
	r.Sequence=3;r.ClockID="restarted"
	if ok,_,e=p.receiveLightV2(r);e!=nil || ok || p.lightRows[0].Valid || !p.lightRows[0].IdentityInvalid {t.Fatal("restart did not invalidate")}
	r.ClockID="node0";r.Sequence=4
	if ok,_,_=p.receiveLightV2(r);ok {t.Fatal("old instance revived invalid cache")}
	p=newPlacementSignals(16);r.Node=1;r.View=1;r.ClockID="other-leader"
	if ok,_,e=p.receiveLightV2(r);e!=nil || ok || !p.lightRows[0].IdentityInvalid {t.Fatal("unpinned leader trusted")}
}

func TestLightV2EveryNewNewAndPrefix(t *testing.T) {
	old:=params.ShardNum;params.ShardNum=16;defer func(){params.ShardNum=old}()
	m,log:=forestHookFixtureV2(t,true);m.newNewPolicy.mode="pressure_share_v2"
	m.forestJoint.lightDayRates=make([]float64,16)
	for i,pair:=range [][2]int{{8,9},{10,10},{1,11},{8,12},{8,9}} {
		tx:=forestHookTransactionV2(t,m,uint64(i),pair[0],pair[1])
		if tx.RoutedSenderShard!=tx.RoutedRecipientShard {t.Fatal("follow/same entry contract")}
	}
	m.nativeAudit.flush();decisions:=0
	for _,line:=range bytes.Split(bytes.TrimSpace(log.Bytes()),[]byte("\n")) {
		var x map[string]json.RawMessage;if e:=json.Unmarshal(line,&x);e!=nil {t.Fatal(e)}
		var event string;json.Unmarshal(x["event"],&event)
		if event=="share_batch_decision" {t.Fatal("old batching used")}
		if event!="light_entry_decision" {continue};decisions++
		var fast ForestFactsSnapshotV2;var seq uint64;json.Unmarshal(x["fast"],&fast);json.Unmarshal(x["sequence"],&seq)
		if fast.BoundPrefix!=seq || fast.AssignedPrefix!=seq {t.Fatal("current occurrence leaked into features")}
		var raw [][]float64;json.Unmarshal(x["raw_features"],&raw);if len(raw)!=16 || len(raw[0])!=20 {t.Fatal("wrong features")}
	}
	if decisions!=2 || m.newNewPolicy.share.ordinal!=2 {t.Fatalf("decisions=%d",decisions)}
	if len(m.newNewPolicy.share.families)!=0 || len(m.newNewPolicy.share.exposure.young)!=0 {t.Fatal("old exposure duplicated v2 facts")}
}

func TestLightV2MissingFlagsAndCausalWatermarks(t *testing.T) {
	m,_:=forestHookFixtureV2(t,true)
	fast,e:=m.forestFacts.SnapshotNow(m.placementSignals.clock);if e!=nil {t.Fatal(e)}
	rows,observed,at,past:=m.placementSignals.lightSnapshotV2(atOriginForLightTest(m))
	graph:=m.forestJoint.lightGraphSnapshot()
	raw,e:=lightFeaturesV2(rows,observed,at,past,fast,graph,m.newNewPolicy.share.previous,0,2,1000);if e!=nil {t.Fatal(e)}
	for _,x:=range raw {
		for _,i:=range []int{3,5,9,13,18} {if x[i]!=1 {t.Fatalf("missing flag%d=%g",i,x[i])}}
		for _,i:=range []int{0,1,2,4,6,7,8,10,11,12,14,15,16,17} {if x[i]!=0 {t.Fatalf("missing value%d=%g",i,x[i])}}
	}
	fast.AssignedPrefix=1
	if _,e=lightFeaturesV2(rows,observed,at,past,fast,graph,m.newNewPolicy.share.previous,0,2,1000);e==nil {t.Fatal("future assigned prefix accepted")}
	fast.AssignedPrefix=0;graph.SourceTime=1001
	if _,e=lightFeaturesV2(rows,observed,at,past,fast,graph,m.newNewPolicy.share.previous,0,2,1000);e==nil {t.Fatal("future source graph accepted")}
}

func atOriginForLightTest(m *RelayCommitteeModule) int64 { return m.placementSignals.clock.now().MonoNS }

// Small local-cost measurement: no nodes, simulator or disk latency claim.
// Includes actual feature queries, inference, draw, binding, forest assignment,
// full audit serialization/enqueue and compact projection enqueue.
func TestLightV2HotPathCosts(t *testing.T) {
	fixture:=os.Getenv("SAA_SHARE_PARITY_FIXTURE")
	if fixture=="" {t.Skip("frozen trained model fixture required")}
	raw,e:=os.ReadFile(fixture);if e!=nil {t.Fatal(e)}
	var input struct{ Model json.RawMessage `json:"model"` }
	if e=json.Unmarshal(raw,&input);e!=nil {t.Fatal(e)}
	model,e:=parseShareModel(input.Model);if e!=nil || model.Schema!="entry_share_residual_v2" {t.Fatal("v2 fixture required",e)}
	old:=params.ShardNum;params.ShardNum=16;defer func(){params.ShardNum=old}()
	m,_:=forestHookFixtureV2(t,true);m.newNewPolicy.mode="residual_share_v2";m.newNewPolicy.share.model=model
	m.forestJoint.lightDayRates=make([]float64,16)
	a:=&batchAudit{w:bufio.NewWriterSize(io.Discard,1<<20),observer:&observerProjection{w:bufio.NewWriterSize(io.Discard,1<<20)}}
	a.startWriter();m.nativeAudit=a
	const count=2000
	durations:=make([]int64,count)
	var before,after runtime.MemStats;runtime.ReadMemStats(&before)
	begin:=time.Now()
	for i:=0;i<count;i++ {
		at:=time.Now();forestHookTransactionV2(t,m,uint64(i),100+i*2,101+i*2);durations[i]=time.Since(at).Nanoseconds()
	}
	enqueueElapsed:=time.Since(begin).Nanoseconds();a.flush();drainElapsed:=time.Since(begin).Nanoseconds()
	runtime.ReadMemStats(&after)
	sort.Slice(durations,func(i,j int)bool{return durations[i]<durations[j]})
	result:=map[string]interface{}{"status":"PASS","scope":"local query+forward+draw+binding+forest assignment+full/compact buffered audit; io.Discard sink; not native latency","samples":count,"mean_ns":float64(enqueueElapsed)/count,"p95_ns":durations[(count*95)/100],"p99_ns":durations[(count*99)/100],"max_ns":durations[count-1],"total_enqueue_ns":enqueueElapsed,"total_with_writer_drain_ns":drainElapsed,"allocs_per_entry":float64(after.Mallocs-before.Mallocs)/count,"allocated_bytes_per_entry":float64(after.TotalAlloc-before.TotalAlloc)/count,"writer":a.writerStats()}
	encoded,_:=json.Marshal(result);t.Log(string(encoded))
	if path:=os.Getenv("SAA_LIGHT_COST_OUTPUT");path!="" {if e=os.WriteFile(path,append(encoded,'\n'),0600);e!=nil {t.Fatal(e)}}
}
