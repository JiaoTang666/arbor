"""Periodic source-time graph maintenance, independent of placement/migration.

Active edges are compact arrays, NOT an overlay backed by archived cold edges.
Each publication is immutable. Pending observations never mutate a reader's
version. Only a continuous source prefix (or explicit watermark) advances time.
"""
from dataclasses import dataclass
from collections import deque
from types import SimpleNamespace
import math
import time
import numpy as np

HALF_LIVES=(21600.,86400.,604800.)
PERIOD=3600


@dataclass(frozen=True)
class Policy:
    # At most .01 relation-rate-equivalent occurrences in the next source hour
    # per removed edge. This is a compression parameter, NOT a migration gate.
    epsilon_per_period: float=.01
    quiet_seconds: int=86400
    max_omitted_fraction: float=.05

    def __post_init__(self):
        if (not math.isfinite(self.epsilon_per_period) or self.epsilon_per_period<0
                or type(self.quiet_seconds) is not int or self.quiet_seconds<PERIOD
                or not math.isfinite(self.max_omitted_fraction) or not 0<=self.max_omitted_fraction<=1):
            raise ValueError('invalid compression policy')


def pack(a,b):
    return (np.asarray(a,np.uint64)<<np.uint64(32))|np.asarray(b,np.uint64)


def values(mass,first,closed,pending,as_of,origin):
    current=(as_of-origin)//PERIOD
    active=closed+((pending>=0)&(pending<current))
    eligible=np.maximum(0,current-first)
    persistence=np.divide(active,eligible,out=np.zeros_like(active,dtype=float),where=eligible>0)
    rates=mass*np.log(2)/np.asarray(HALF_LIVES)
    weight=(rates[:,0]+persistence*(rates[:,1]+rates[:,2])/2)/(1+persistence)
    return rates,persistence,weight,active,eligible


class Frame:
    """Only retained edges and their sparse CSR adjacency; no base fallback."""
    def __init__(self,keys,mass,first,closed,pending,last,*,origin,as_of,cutoff,version):
        self.keys,self.mass,self.first,self.closed,self.pending,self.last=(keys,mass,first,closed,pending,last)
        self.base=SimpleNamespace(origin=origin,half_lives=HALF_LIVES)
        self.as_of,self.cutoff,self.version=as_of,cutoff,version
        a=(keys>>np.uint64(32)).astype(np.uint32);b=(keys&np.uint64(0xffffffff)).astype(np.uint32)
        ids=np.arange(len(keys),dtype=np.int64);nonself=a!=b
        accounts=np.concatenate((a,b[nonself]));edges=np.concatenate((ids,ids[nonself]))
        order=np.argsort(accounts,kind='stable');accounts=accounts[order]
        self.adj_ids=edges[order]
        self.adj_accounts,starts=np.unique(accounts,return_index=True)
        self.adj_offsets=np.r_[starts,len(accounts)].astype(np.int64)
        for x in self.arrays():x.flags.writeable=False

    def arrays(self):
        return (self.keys,self.mass,self.first,self.closed,self.pending,self.last,
                self.adj_ids,self.adj_accounts,self.adj_offsets)

    @property
    def bytes(self):return sum(x.nbytes for x in self.arrays())

    def incident(self,account):
        if type(account) is not int or not 0<=account<2**32:raise ValueError('invalid account')
        i=int(np.searchsorted(self.adj_accounts,np.uint32(account)))
        if i==len(self.adj_accounts) or self.adj_accounts[i]!=account:return ()
        keys=self.keys[self.adj_ids[self.adj_offsets[i]:self.adj_offsets[i+1]]]
        return ((int(k)>>32,int(k)&0xffffffff) for k in keys)

    def edge(self,key,as_of):
        if type(as_of) is not int or as_of<self.as_of:raise ValueError('query predates publication')
        if len(key)!=2 or any(type(a) is not int or not 0<=a<2**32 for a in key):raise ValueError('invalid pair')
        packed=pack(*key);i=int(np.searchsorted(self.keys,packed))
        if i==len(self.keys) or self.keys[i]!=packed:return None
        mass=self.mass[i:i+1]*np.exp2(-(as_of-self.as_of)/np.asarray(HALF_LIVES))
        r,p,w,c,e=values(mass,self.first[i:i+1],self.closed[i:i+1],self.pending[i:i+1],as_of,self.base.origin)
        return dict(rates=tuple(r[0]),persistence=float(p[0]),weight=float(w[0]),
                    active_closed=int(c[0]),eligible_closed=int(e[0]))


class PeriodicPrior:
    def __init__(self,frame,known_accounts,policy=Policy(),audit=None):
        self.policy=policy;self.known_accounts=int(known_accounts)
        self.parents={}  # Permanent identity metadata; NOT pruned with edges.
        self.cutoff,self.as_of=frame.cutoff,frame.as_of
        self.origin=frame.base.origin
        self.next_tick=self.origin+((self.as_of-self.origin)//PERIOD+1)*PERIOD
        self.pending=[];self.publications=deque(maxlen=256);self._discarded_rate=np.zeros(3);self.audit=audit
        self.published=self._compact(frame.keys,frame.mass,frame.first,frame.closed,frame.pending,frame.last,
                                     frame.as_of,frame.cutoff,frame.version,'bootstrap')
        if self.audit:self.audit(dict(self.publications[-1]))

    @classmethod
    def empty(cls,origin=0,policy=Policy()):
        z=np.empty(0,np.int64)
        frame=Frame(np.empty(0,np.uint64),np.empty((0,3),float),z.copy(),z.copy(),z.copy(),z.copy(),
                    origin=origin,as_of=origin,cutoff=0,version=0)
        return cls(frame,0,policy)

    def _compact(self,keys,mass,first,closed,pending,last,at,cutoff,version,reason):
        began=time.perf_counter()
        rates,_,weight,_,_=values(mass,first,closed,pending,at,self.origin)
        eligible=(np.max(rates,axis=1,initial=0)*PERIOD<self.policy.epsilon_per_period)&(at-last>=self.policy.quiet_seconds)
        if self.publications:
            self._discarded_rate*=np.exp2(-(at-self.publications[-1]['as_of'])/np.asarray(HALF_LIVES))
        # Many individually weak edges can carry most of the long-scale mass.
        # Budget against the exact aggregate unpruned exponential kernels,
        # including omissions from ALL preceding publications, not just this one.
        total_kernels=rates.sum(axis=0)+self._discarded_rate
        allowance=np.maximum(0.,self.policy.max_omitted_fraction*total_kernels-self._discarded_rate)
        ids=np.flatnonzero(eligible)
        burdens=np.max(np.divide(rates[ids],total_kernels,out=np.zeros((len(ids),3)),where=total_kernels>0),axis=1,initial=0)
        ids=ids[np.argsort(burdens,kind='stable')]
        cumulative=np.cumsum(rates[ids],axis=0)
        # Relative roundoff allowance only (not an extra compression budget).
        take=int(np.count_nonzero(np.all(cumulative<=allowance+1e-12*total_kernels,axis=1)))
        remove=np.zeros(len(keys),bool);remove[ids[:take]]=True
        discarded=rates[remove].sum(axis=0)
        self._discarded_rate+=discarded
        # Copy compaction frees removed rows; old publications remain valid only
        # while a reader intentionally holds them. No tombstone/base-edge map.
        keep=~remove
        frame=Frame(keys[keep],mass[keep],first[keep],closed[keep],pending[keep],last[keep],
                    origin=self.origin,as_of=at,cutoff=cutoff,version=version)
        total=float(weight.sum());lost=float(weight[remove].sum())
        self.publications.append(dict(version=version,as_of=at,cutoff=cutoff,reason=reason,
            before_edges=len(keys),active_edges=len(frame.keys),pruned_edges=int(remove.sum()),
            eligible_cold_edges=len(ids),retained_by_information_budget=len(ids)-take,
            active_bytes=frame.bytes,active_adjacency_entries=len(frame.adj_ids),
            discarded_relation_rate=lost,discarded_relation_rate_fraction=lost/total if total else 0.,
            discarded_kernel_rates=discarded.tolist(),
            omitted_kernel_rate_bound=self._discarded_rate.tolist(),
            omitted_kernel_rate_fraction=np.divide(self._discarded_rate,total_kernels,
                out=np.zeros(3),where=total_kernels>0).tolist(),
            # Persistence resets on reappearance; this kernel bound does NOT
            # bound the complete persistence-weighted nomination score error.
            persistence_score_error_bound=False,
            compaction_seconds=time.perf_counter()-began,
            native_route_write=False,ai_controls_maintenance=False))
        return frame

    def _publish(self,at):
        began=time.perf_counter();old=self.published
        old_mass=old.mass*np.exp2(-(at-old.as_of)/np.asarray(HALF_LIVES))
        current=(at-self.origin)//PERIOD
        closed=old.closed+((old.pending>=0)&(old.pending<current))
        pending=np.where(old.pending<current,-1,old.pending)
        if self.pending:
            u,v,t=(np.concatenate([row[i] for row in self.pending]) for i in range(3))
            observed,inverse=np.unique(pack(u,v),return_inverse=True)
            added=np.column_stack([np.bincount(inverse,weights=np.exp2(-(at-t)/h),minlength=len(observed)) for h in HALF_LIVES])
            last=np.full(len(observed),-1,np.int64);np.maximum.at(last,inverse,t)
            # Every pending segment belongs to this one source-hour bin.
            window=(int(t[0])-self.origin)//PERIOD
            if np.any((t-self.origin)//PERIOD!=window) or window!=current-1:
                raise AssertionError('maintenance mixed closed windows')
            keys=np.union1d(old.keys,observed);i=np.searchsorted(keys,old.keys);j=np.searchsorted(keys,observed)
            mass=np.zeros((len(keys),3));mass[i]=old_mass;mass[j]+=added
            first=np.full(len(keys),window,np.int64);first[i]=old.first
            counts=np.zeros(len(keys),np.int64);counts[i]=closed
            pbin=np.full(len(keys),-1,np.int64);pbin[i]=pending
            seen=np.full(len(keys),-1,np.int64);seen[i]=old.last;seen[j]=np.maximum(seen[j],last)
            # Existing bootstrap pending activity for this hour already closed
            # above; repeated observations must not count it a second time.
            prior_pending=np.full(len(keys),-1,np.int64);prior_pending[i]=old.pending
            counts[j]+=(prior_pending[j]!=window)
        else:
            keys,mass,first,counts,pbin,seen=old.keys,old_mass,old.first,closed,pending,old.last
        frame=self._compact(keys,mass,first,counts,pbin,seen,at,self.cutoff,old.version+1,'periodic')
        self.pending=[];self.published=frame
        self.publications[-1]['maintenance_seconds']=time.perf_counter()-began
        if self.audit:self.audit(dict(self.publications[-1]))

    def _identities(self,u,v):
        for a,b in zip(map(int,u),map(int,v)):
            old_a=a<self.known_accounts or a in self.parents
            old_b=b<self.known_accounts or b in self.parents
            # Evaluate both before inserting either; new-new cannot create a
            # fictitious parent. Pruned relationships never erase this identity.
            if not old_a:self.parents[a]=b if old_b else -1
            if not old_b and b!=a:self.parents[b]=a if old_a else -1

    def observe(self,u,v,t,first_sequence):
        u,v,t=map(np.asarray,(u,v,t))
        if (type(first_sequence) is not int or first_sequence!=self.cutoff or u.ndim!=1 or u.shape!=v.shape or u.shape!=t.shape
                or any(x.dtype.kind not in 'iu' for x in (u,v,t))
                or np.any(u<0) or np.any(v<0) or np.any(u>=2**32) or np.any(v>=2**32)
                or np.any(t>np.iinfo(np.int64).max) or np.any(t[1:]<t[:-1]) or (len(t) and t[0]<self.as_of)):
            raise ValueError('invalid/noncontinuous source observation')
        t=t.astype(np.int64,copy=False);start=0
        while start<len(t):
            while self.next_tick<=int(t[start]):
                self._publish(self.next_tick);self.next_tick+=PERIOD
            end=int(np.searchsorted(t,self.next_tick,side='left'))
            a,b,stamp=u[start:end],v[start:end],t[start:end]
            self._identities(a,b)
            self.pending.append((a.copy(),b.copy(),stamp.copy()))
            self.cutoff+=end-start;self.as_of=int(stamp[-1]);start=end

    def advance_watermark(self,at):
        """Caller certifies no future event has source_time < at; not wall time."""
        if type(at) is not int or at<self.as_of:raise ValueError('invalid source watermark')
        while self.next_tick<=at:
            self._publish(self.next_tick);self.next_tick+=PERIOD
        self.as_of=at

    @property
    def pending_occurrences(self):return sum(len(row[0]) for row in self.pending)
