"""Graph-only maintenance, with no forest arrays, objects or predictor.

Reuses the existing PeriodicPrior, causal identity validation and empty-gap
handling. Graph projection is unchanged; outputs cannot select an account route.
"""
import hashlib,json,time
from pathlib import Path
import numpy as np
from .active import Frame, PeriodicPrior, Policy, PERIOD, HALF_LIVES, values
from arbor.io import snapshot_digest

class AcceptedPrefix:

    @property
    def cutoff(self):
        return self.graph.cutoff

    @property
    def as_of(self):
        return self.graph.as_of

    def _validate(self, u, v, t, assigned, first_sequence):
        if (type(first_sequence) is not int or first_sequence != self.cutoff
                or u.ndim != 1 or any(x.shape != u.shape for x in (v, t))
                or any(x.dtype.kind not in "iu" for x in (u, v, t, assigned))
                or assigned.shape != (len(u), 2)
                or np.any(u < 0) or np.any(v < 0) or np.any(u >= 2**32) or np.any(v >= 2**32)
                or np.any(t > np.iinfo(np.int64).max) or np.any(t[1:] < t[:-1])
                or (len(t) and t[0] < self.as_of)
                or np.any(assigned < 0) or np.any(assigned >= self.shards)):
            raise ValueError("invalid/noncontinuous accepted batch")
        new, next_id = {}, self.next_id
        for a, b, (sa, sb) in zip(map(int, u), map(int, v), assigned):
            old = [int(self.routes[x]) if x < self.next_id else new.get(x, -1) for x in (a, b)]
            if any(owner >= 0 and owner != s for owner, s in zip(old, (sa, sb))):
                raise ValueError("existing ownership changed")
            colocate = (max(old)<0 if self.strict_colocation=='newnew_only'
                        else self.strict_colocation and min(old)<0)
            if colocate and sa != sb:
                raise ValueError("first-placement colocation violated")
            if a == b and sa != sb:
                raise ValueError("self transaction has split route")
            for x, owner, s in zip((a, b), old, (sa, sb)):
                if owner < 0 and x not in new:
                    if x != next_id:
                        raise ValueError("novel identifiers must extend the causal catalogue")
                    new[x] = int(s)
                    next_id += 1
        return new, next_id

    def _fold_empty_gap(self, at):
        g = self.graph
        if g.pending or g.next_tick > at:
            return
        count = (at - g.next_tick) // PERIOD + 1
        if count <= 1:
            return
        # Decay is a semigroup; closing a single previous active bin is also
        # independent of the number of empty periods crossed. A single bounded
        # compaction can leave additional weak rows, never invent their mass.
        boundary = g.next_tick + (count - 1) * PERIOD
        g._publish(boundary)
        g.as_of = boundary
        g.next_tick = boundary + PERIOD
        self.collapsed_empty_periods += count - 1


class GraphPrior(AcceptedPrefix):
    def __init__(self,frame,routes,shards=16):
        self.n=len(routes);self.shards=shards
        self.routes=np.full(self.n+65536,-1,np.int32);self.routes[:self.n]=routes
        self.next_id=self.n;self.strict_colocation=False
        self.graph=PeriodicPrior(frame,self.n,Policy(),external_identity=True)
        self.snapshot=None;self.publications=[];self.poisoned=False
        self.collapsed_empty_periods=0;self.observe_seconds=self.publish_seconds=0.
        self.publish(force=True)

    @classmethod
    def from_paths(cls,index,routes,shards=16):
        index=Path(index);meta=json.loads((index/'manifest.json').read_text())
        if meta['status']!='PASS' or tuple(meta['half_lives_seconds'])!=HALF_LIVES or meta['bin_seconds']!=PERIOD:raise ValueError('graph seed schema')
        fields=('edge_sender','edge_recipient','edge_decayed_mass','edge_first_timestamp','edge_last_timestamp','window_offsets')
        a={}
        for f in fields:
            path=index/(f+'.npy');h=hashlib.sha256()
            with path.open('rb') as stream:
                for block in iter(lambda:stream.read(8<<20),b''):h.update(block)
            if h.hexdigest()!=meta['artifacts'][path.name]['sha256']:raise ValueError('graph seed hash '+f)
            a[f]=np.load(path,mmap_mode='r')
        keys=(a['edge_sender'].astype(np.uint64)<<np.uint64(32))|a['edge_recipient'].astype(np.uint64)
        at,origin=meta['as_of_source_timestamp'],meta['source_start_timestamp'];last=a['edge_last_timestamp']
        if np.any(keys[1:]<=keys[:-1]) or np.any(last>at):raise ValueError('unordered/future graph')
        first=(a['edge_first_timestamp']-origin)//PERIOD;last_bin=(last-origin)//PERIOD
        pending=np.where(last_bin==(at-origin)//PERIOD,last_bin,-1)
        closed=np.diff(a['window_offsets']).astype(np.int64)-(pending>=0)
        frame=Frame(keys,a['edge_decayed_mass'],first,closed,pending,last,origin=origin,as_of=at,cutoff=meta['accepted_cutoff_exclusive'],version=0)
        return cls(frame,np.load(routes,mmap_mode='r'),shards)

    def observe(self,u,v,t,assigned,first_sequence):
        if self.poisoned:raise ValueError('incomplete graph')
        u,v,t,assigned=map(np.asarray,(u,v,t,assigned))
        novel,next_id=self._validate(u,v,t,assigned,first_sequence)
        if not len(t):return
        began=time.perf_counter()
        try:
            if next_id>len(self.routes):self.routes=np.pad(self.routes,(0,next_id+65536-len(self.routes)),constant_values=-1)
            for a,s in novel.items():self.routes[a]=s
            self.next_id=next_id;self._fold_empty_gap(int(t[0]));self.graph.observe(u,v,t,first_sequence)
            self.observe_seconds+=time.perf_counter()-began;self.publish()
        except BaseException:self.poisoned=True;raise

    def publish(self,force=False):
        if self.poisoned:raise ValueError('incomplete graph')
        old=self.snapshot
        if not force and old and self.as_of-old['source_time']<PERIOD:return old
        began=time.perf_counter();self.graph.flush_observed();f=self.graph.published
        if f.cutoff!=self.cutoff or f.as_of!=self.as_of:raise ValueError('graph prefix mismatch')
        stages=np.zeros(self.shards);rates3=np.zeros((self.shards,3));persist=np.zeros(self.shards)
        for start in range(0,len(f.keys),200000):
            end=min(start+200000,len(f.keys));keys=f.keys[start:end]
            a=self.routes[(keys>>np.uint64(32)).astype(np.int64)];b=self.routes[(keys&np.uint64(0xffffffff)).astype(np.int64)]
            if np.any(a<0) or np.any(b<0):raise ValueError('unbound graph endpoint')
            rates,p,w,_,_=values(f.mass[start:end],f.first[start:end],f.closed[start:end],f.pending[start:end],f.as_of,f.base.origin)
            cross=a!=b
            stages+=np.bincount(a,weights=w,minlength=self.shards)+np.bincount(b[cross],weights=w[cross],minlength=self.shards)
            for k in range(3):rates3[:,k]+=np.bincount(a,weights=rates[:,k],minlength=self.shards)+np.bincount(b[cross],weights=rates[cross,k],minlength=self.shards)
            persist+=np.bincount(a,weights=rates[:,1]*p,minlength=self.shards)+np.bincount(b[cross],weights=rates[cross,1]*p[cross],minlength=self.shards)
        # The unchanged pressure-policy reader requires the legacy field widths.
        # These empty compatibility channels are never measured forest facts or
        # pressure costs. Neither arm constructs the old forest implementation.
        row=dict(schema='joint_live_prior_v1',version=1 if old is None else old['version']+1,
          accepted_cutoff_exclusive=f.cutoff,source_time=f.as_of,graph_version=f.version,
          graph_cutoff=f.cutoff,graph_source_time=f.as_of,forest_cutoff=f.cutoff,forest_source_time=f.as_of,
          forest_incidence_rate_proxy=[0.]*self.shards,direct_arrival_rate=[0.]*self.shards,
          forest_followup_incidence_rate_proxy=[0.]*self.shards,forest_mature_child_mass_by_shard=[0.]*self.shards,
          forest_channels_disabled=True,forest_channel_semantics='empty compatibility fields; not observed evidence',
          relation_stage_rate_proxy=stages.tolist(),
          relation_stage_rate_by_scale=rates3.tolist(),relation_day_rate_weighted_persistence=np.divide(persist,rates3[:,1],out=np.zeros(self.shards),where=rates3[:,1]>0).tolist(),
          graph_quality=dict(self.graph.publications[-1]),forest_enabled=False,native_ownership_changes=0,
          observation_kind='accepted_not_committed',affects_placement=False)
        row['snapshot_sha256']=snapshot_digest(row);row['publication_seconds']=time.perf_counter()-began
        self.publish_seconds+=row['publication_seconds'];self.snapshot=row;self.publications.append(row);return row
