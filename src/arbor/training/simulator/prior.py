"""Existing causal graph and v2 source facts, joined only at identical prefixes.

Slow O(accounts + edges) publication runs in the observer event, not the decision
query. Fast runtime family accounting belongs to the light environment/native.
"""

import copy
import time
import numpy as np
from arbor.graph import prior as _graph
from arbor.forest import adapter as _slow




class JointFactsPrior:
    def __init__(self, frame, routes, checkpoint, shards=16, continuous_source=False):
        self.graph_prior = _graph.GraphPrior(frame, routes, shards)
        self.slow = _slow.SlowForestAdapter.from_checkpoint(checkpoint, routes, shards)
        if self.slow.forest.cutoff != self.cutoff or self.slow.forest.as_of != self.as_of:
            raise ValueError('v2 checkpoint and graph cutoff differ')
        if self.slow.seed_known != self.n:
            raise ValueError('v2 history account membership differs from layout')
        self.snapshot=None;self.publications=[]
        self.entry_profiles=None # Intentionally no obsolete endpoint-count profiles.
        self.continuous_source=bool(continuous_source)
        self._slow_observe_seconds=0.;self._slow_publish_seconds=0.
        self.publish(force=True)

    @property
    def cutoff(self):return self.graph_prior.cutoff
    @property
    def as_of(self):return self.graph_prior.as_of
    @property
    def n(self):return self.graph_prior.n
    @property
    def routes(self):return self.graph_prior.routes
    @property
    def graph(self):return self.graph_prior.graph
    @property
    def observe_seconds(self):return self.graph_prior.observe_seconds+self._slow_observe_seconds
    @property
    def publish_seconds(self):return self.graph_prior.publish_seconds+self._slow_publish_seconds

    def prepare_at(self, at):
        """A missing source interval censors pending samples; never fill with zeros."""
        at=int(at)
        if at<self.as_of:raise ValueError('input before seed')
        if at==self.as_of:return
        if self.continuous_source:
            self.slow.advance_watermark(at,self.cutoff)
        else:
            self.slow.mark_gap(at)
        g=self.graph
        g._publish(at);g.as_of=at;g.next_tick=g.origin+((at-g.origin)//3600+1)*3600
        self.graph_prior.publish(force=True)
        self.publish(force=True)

    def observe(self,u,v,t,assigned,first_sequence):
        # Both consume exactly the same accepted contiguous prefix. Neither can
        # publish to the controller until the joint watermark validates.
        self.graph_prior.observe(u,v,t,assigned,first_sequence)
        began=time.perf_counter()
        for i,(a,b,at,(sa,sb)) in enumerate(zip(u,v,t,assigned)):
            self.slow.observe(a,b,at,int(first_sequence)+i,sa,sb)
        self._slow_observe_seconds+=time.perf_counter()-began
        if self.slow.forest.cutoff!=self.cutoff or self.slow.forest.as_of!=self.as_of:
            raise ValueError('graph and forest source watermarks split')
        self.publish()

    def publish(self,force=False):
        row=self.graph_prior.snapshot
        if self.snapshot is not None and self.snapshot['version']==row['version']:
            return self.snapshot
        began=time.perf_counter()
        facts=self.slow.publish(row['version'],row['snapshot_sha256'],row['accepted_cutoff_exclusive'],row['source_time'])
        # Existing empty compatibility fields remain explicitly disabled; v2
        # facts have their own schema rather than pretending to be predictions.
        self.snapshot=copy.deepcopy(row)
        self.snapshot['forest_v2_facts']=facts
        self.snapshot['forest_v2_enabled']=True
        self._slow_publish_seconds+=time.perf_counter()-began
        self.publications.append(copy.deepcopy(self.snapshot))
        return self.snapshot

    def candidate_demand(self):return copy.deepcopy(self.snapshot)
