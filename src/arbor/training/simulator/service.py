"""Timing/visibility adapter; transfer execution remains in the original engine."""
import bisect
import copy
from collections import deque
import heapq
import gymnasium as gym
import numpy as np

from .batch import BatchEnv, AreaCurve, candidates
from .environment import PlacementEnv
from .features import FEATURES
from .calendar import Calendar

EXTRA = ('visible_pending_delta_5s', 'visible_pending_delta_20s',
         'pending_history_5s_available', 'pending_history_20s_available')




class RobustEnv(BatchEnv):
    def __init__(self,*args,calendar=None,**kwargs):
        self.calendar=calendar or Calendar.nominal()
        super().__init__(*args,**kwargs)
        self.observation_space=gym.spaces.Dict(dict(
            shards=gym.spaces.Box(-np.inf,np.inf,(self.shards,23),np.float32),
            candidates=gym.spaces.MultiDiscrete([self.shards,self.shards]),
            previous=gym.spaces.Box(0.,1.,(2,),np.float32)))

    def reset(self,**kwargs):
        self.execution_signals=None
        self.first_proposal=np.zeros(self.shards,bool)
        self.report_height=np.zeros(self.shards,np.int64)
        self.block_height=np.zeros(self.shards,np.int64)
        self.unreported_completions=[deque() for _ in range(self.shards)]
        self.history=deque()
        self.received_at=np.full(len(self.source[0]),np.nan)
        self.report_events=[];self.service_events=[];self.stale_reports=0;self.report_capture_gaps=[]
        self.max_hidden_confirmations=0;self.reports_drained=False
        return super().reset(**kwargs)

    def _ensure_execution(self):
        if self.execution_signals is None:
            self.execution_signals=copy.deepcopy(self.signals)
            self._remember()

    def _remember(self):
        pending=np.array([r['Assigned']-r['Confirmed'] for r in self.signals],np.int64)
        if self.history and self.history[-1][0]==self.now:self.history[-1]=(self.now,pending)
        else:self.history.append((self.now,pending))
        while len(self.history)>2 and self.history[1][0]<self.now-20:
            self.history.popleft()

    def _event(self,at,kind,payload=None):
        if kind=='proposal':
            if not self.first_proposal[payload]:
                self.first_proposal[payload]=True
                at=float(self.calendar.phases[payload])
            at=self.calendar.available(payload,at)
        elif kind=='commit':
            at=self.calendar.finish(payload[0],self.now)
        elif kind=='publish':at+=self.calendar.publish_delay
        priority={'release':0,'commit':1,'report':2,'observe':3,'publish':3,'proposal':4}[kind]
        self.serial+=1
        heapq.heappush(self.events,(float(at),priority,self.serial,kind,payload))

    def _place(self,i,target=None):
        self._ensure_execution()
        super()._place(i,target)
        self._remember()

    def _deliver(self,at,s,height,captured):
        self.report_events.append(dict(received_at=float(at),shard=s,height=height,
            captured_at=captured['CommitAt'],confirmed=captured['Confirmed'],queue=captured['Queue'],
            accepted=bool(height>self.report_height[s])))
        if height<=self.report_height[s]:
            self.stale_reports+=1
            return
        if at<captured['CommitAt'] or captured['Confirmed']>self.work[s]:
            raise AssertionError('invalid report evidence')
        self.report_height[s]=height
        assigned=self.signals[s]['Assigned']
        self.signals[s]=dict(captured,Assigned=assigned,ReceivedAt=float(at))
        pending=self.unreported_completions[s]
        while pending and pending[0][0]<=height:
            _,ids=pending.popleft()
            self.received_at[ids]=at
        self.report_capture_gaps.append(at-captured['CommitAt'])
        if at==self.now:self._remember()

    def _handle(self,kind,payload):
        self._ensure_execution()
        if kind=='report':
            self._deliver(self.now,*payload)
            self.check_ledger()
            return
        if kind!='commit':
            super()._handle(kind,payload)
            if kind=='proposal' and self.queue_trace:
                self._correct_trace()
            return
        s,body,proposed=payload
        visible=self.signals[s]
        actual=self.execution_signals[s]
        actual['Assigned']=visible['Assigned']
        self.signals[s]=actual
        # Original loop alone confirms stages, generates relay legs and completes transactions.
        try:
            super()._handle(kind,payload)
            captured=copy.deepcopy(actual)
        finally:self.signals[s]=visible
        self.block_height[s]+=1
        height=int(self.block_height[s])
        self.service_events.append(dict(shard=s,height=height,proposed=proposed,committed=self.now,
            elapsed=self.now-proposed,body_size=len(body)))
        ids=[i for i,leg in body if leg==1 or self.route[i,0]==self.route[i,1]]
        self.unreported_completions[s].append((height,ids))
        if self.calendar.report_delay==0:
            self._deliver(self.now,s,height,captured)
        else:
            self._event(self.now+self.calendar.report_delay,'report',(s,height,captured))
        self.check_ledger()
        self._correct_trace()

    def _correct_trace(self):
        if self.queue_trace:
            self.queue_trace[-1].update(
                pending=(self.work-np.array([r['Confirmed'] for r in self.execution_signals])).tolist(),
                visible_pending=[r['Assigned']-r['Confirmed'] for r in self.signals])

    def check_ledger(self):
        self._ensure_execution()
        actual=np.array([r['Confirmed'] for r in self.execution_signals])
        visible=np.array([r['Confirmed'] for r in self.signals])
        np.testing.assert_array_equal(self.work-actual,
            np.array([len(q) for q in self.queues])+self.in_block+self.reserved)
        if np.any(visible>actual) or np.any(self.work<visible):
            raise AssertionError('visible confirmations exceed executed evidence')
        self.max_hidden_confirmations=max(self.max_hidden_confirmations,int((actual-visible).sum()))
        self.ledger_checks+=1

    def raw_features(self):
        self.check_ledger()
        base=PlacementEnv.raw_observation(self)[:,:19]
        times=[t for t,_ in self.history]
        current=np.array([r['Assigned']-r['Confirmed'] for r in self.signals])
        extra=np.zeros((self.shards,4))
        for j,window in enumerate((5.,20.)):
            index=bisect.bisect_right(times,self.now-window)-1
            if self.now>=window and index>=0:
                extra[:,j]=(current-self.history[index][1])/self.capacity
                extra[:,j+2]=1.
        return np.column_stack((base,extra))

    def step(self,action):
        obs,reward,done,truncated,info=super().step(action)
        if done:obs['shards']=np.zeros((self.shards,23),np.float32)
        return obs,reward,done,truncated,info

    def report(self):
        if not self.reports_drained:
            # No further decisions/execution; separately account for already scheduled receipts.
            for at,_,_,kind,payload in sorted(self.events):
                if kind=='report':self._deliver(at,*payload)
            self.reports_drained=True
        visible=self.signals
        for s,row in enumerate(self.execution_signals):row['Assigned']=int(self.work[s])
        self.signals=self.execution_signals
        try:report=super().report()
        finally:self.signals=visible
        curve=AreaCurve(self.release,self.done_at)
        boundaries=np.r_[np.arange(0.,curve.end,5.),curve.end]
        values=np.array([curve.at(t) for t in boundaries])
        waits=np.diff(values)
        np.testing.assert_allclose(waits.sum(),self.area,rtol=1e-10,atol=1e-5)
        ok=np.isfinite(self.received_at)
        if np.any(self.received_at[ok]<self.done_at[ok]):raise AssertionError('receipt precedes execution')
        report.update(completion_tps=float(report['completion_tps']),
            pressure_semantics='visible assigned minus report-confirmed; not hidden queue',
            ledger_true_pending_at_end=int(self.work.sum()-sum(r['Confirmed'] for r in self.execution_signals)),
            visible_pending_after_receipt_drain=sum(r['Assigned']-r['Confirmed'] for r in self.signals),
            max_hidden_confirmations=self.max_hidden_confirmations,stale_reports_ignored=self.stale_reports,
            receipt_completed=int(ok.sum()),receipt_missing=int((~ok).sum()),
            receipt_latency_mean=float((self.received_at[ok]-self.release[ok]).mean()) if ok.any() else None,
            receipt_latency_p99=float(np.quantile(self.received_at[ok]-self.release[ok],.99)) if ok.any() else None,
            receipt_completion_time=float(self.received_at[ok].max()) if ok.any() else None,
            execution_completion_time=float(self.done_at.max()),
            wait_5s_boundaries=boundaries.tolist(),wait_5s_integrals=waits.tolist(),
            report_age_max=max(self.report_capture_gaps,default=0.),
            service_seconds_mean=float(np.mean([r['elapsed'] for r in self.service_events])),
            service_seconds_p95=float(np.quantile([r['elapsed'] for r in self.service_events],.95)),
            block_fill_mean=float(np.mean([r['body_size']/self.capacity for r in self.service_events])),
            max_queued_stages=max((max(r['queue']) for r in self.queue_trace),default=0),
            scenario_family=self.calendar.row.get('family'),
            reward_uses_execution_completion=True,external_scenario_visible_to_actor=False)
        return report
