"""Bounded active topology, exact omission ledger, lazy adjacency.

The sealed v1 remains a reference. Resource overflow reduces evidence quality,
never pauses native admission. This is not a calibrated demand predictor.
"""
from dataclasses import dataclass
from collections import deque
import threading
import time
import numpy as np

from . import periodic as reference
from .periodic import HALF_LIVES,PERIOD,pack,values


@dataclass(frozen=True)
class Policy(reference.Policy):
    max_edges: int=5_000_000
    max_pending: int=50_000

    def __post_init__(self):
        super().__post_init__()
        if any(type(x) is not int or x<1 for x in (self.max_edges,self.max_pending)):
            raise ValueError('positive integer resource capacities required')


class Frame(reference.Frame):
    def __init__(self,keys,mass,first,closed,pending,last,*,origin,as_of,cutoff,version):
        from types import SimpleNamespace
        self.keys,self.mass,self.first,self.closed,self.pending,self.last=keys,mass,first,closed,pending,last
        self.base=SimpleNamespace(origin=origin,half_lives=HALF_LIVES)
        self.as_of,self.cutoff,self.version=as_of,cutoff,version
        self._adjacency=None;self._lock=threading.Lock()
        for x in self.arrays():x.flags.writeable=False

    def arrays(self):
        arrays=(self.keys,self.mass,self.first,self.closed,self.pending,self.last)
        return arrays if self._adjacency is None else arrays+self._adjacency

    def incident(self,account):
        if type(account) is not int or not 0<=account<2**32:raise ValueError('invalid account')
        # Outgoing range is already sorted. Build incoming index only on demand.
        if self._adjacency is None:
            with self._lock:
                if self._adjacency is None:
                    targets=(self.keys&np.uint64(0xffffffff)).astype(np.uint32)
                    order=np.argsort(targets,kind='stable');targets=targets[order]
                    targets.flags.writeable=False;order.flags.writeable=False
                    self._adjacency=(targets,order)
        targets,order=self._adjacency
        lo=np.searchsorted(self.keys,np.uint64(account)<<np.uint64(32))
        hi=len(self.keys) if account==2**32-1 else np.searchsorted(self.keys,np.uint64(account+1)<<np.uint64(32))
        incoming=order[np.searchsorted(targets,account):np.searchsorted(targets,account,side='right')]
        ids=np.union1d(np.arange(lo,hi),incoming)
        return ((int(k)>>32,int(k)&0xffffffff) for k in self.keys[ids])


class PeriodicPrior(reference.PeriodicPrior):
    def __init__(self,frame,known_accounts,policy=Policy(),audit=None,external_identity=False):
        self.external_identity=external_identity
        super().__init__(frame,known_accounts,policy,audit)

    def _identities(self,u,v):
        if not self.external_identity:super()._identities(u,v)

    def _compact(self,keys,mass,first,closed,pending,last,at,cutoff,version,reason):
        began=time.perf_counter();rates=mass*(np.log(2)/np.asarray(HALF_LIVES))
        if self.publications:self._discarded_rate*=np.exp2(-(at-self.publications[-1]['as_of'])/np.asarray(HALF_LIVES))
        total=rates.sum(axis=0)+self._discarded_rate
        allowance=np.maximum(0,self.policy.max_omitted_fraction*total-self._discarded_rate)
        eligible=(rates.max(axis=1,initial=0)*PERIOD<self.policy.epsilon_per_period)&(at-last>=self.policy.quiet_seconds)
        ids=np.flatnonzero(eligible)
        remove=np.zeros(len(keys),bool);sort_edges=0;fast_path='empty_eligible'
        # Capacity selection still needs the original per-edge burden.
        burden=(np.max(np.divide(rates,total,out=np.zeros_like(rates),where=total>0),axis=1,initial=0)
                if len(ids) or len(keys)>self.policy.max_edges else None)
        if len(ids):
            candidate_burden=burden[ids];limit=allowance+1e-12*total
            first_id=ids[np.argmin(candidate_burden)]  # same first tie as stable argsort
            # Every nonnegative cumulative prefix contains this first row. If
            # it cannot fit, sorting cannot yield any deletable prefix. Preserve
            # the old path for nonfinite/negative data rather than a new gate.
            blocked=False
            if (np.any(rates[first_id]>limit) and np.all(np.isfinite(candidate_burden))
                    and np.all(np.isfinite(limit))):
                candidate_rates=rates[ids]
                blocked=np.all(np.isfinite(candidate_rates)) and np.all(candidate_rates>=0)
            fast_path='budget_prefix_blocked' if blocked else 'none'
            if not blocked:
                ranked=ids[np.argsort(candidate_burden,kind='stable')]
                cumulative=np.cumsum(rates[ranked],axis=0)
                count=int(np.count_nonzero(np.all(cumulative<=limit,axis=1)))
                remove[ranked[:count]]=True;sort_edges=len(ids)
        forced=max(0,int((~remove).sum())-self.policy.max_edges)
        if forced:
            left=np.flatnonzero(~remove)
            # Deterministic O(E) selection plus key-ordered tie resolution.
            threshold=np.partition(burden[left],forced-1)[forced-1]
            smaller=left[burden[left]<threshold];ties=left[burden[left]==threshold]
            remove[smaller]=True;remove[ties[:forced-len(smaller)]]=True
        discarded=rates[remove].sum(axis=0);self._discarded_rate+=discarded
        arrays=(keys,mass,first,closed,pending,last)
        if np.any(remove):
            keep=~remove;arrays=tuple(x[keep] for x in arrays)
        # An immutable publication can share unchanged arrays; no boolean-copy
        # is needed when compaction did not remove a row.
        f=Frame(*arrays,
                origin=self.origin,as_of=at,cutoff=cutoff,version=version)
        fractions=np.divide(self._discarded_rate,total,out=np.zeros(3),where=total>0)
        self.publications.append(dict(version=version,as_of=at,cutoff=cutoff,reason=reason,
            before_edges=len(keys),active_edges=len(f.keys),active_bytes=f.bytes,
            pruned_edges=int(remove.sum()),forced_capacity_evictions=forced,
            max_edges=self.policy.max_edges,omitted_kernel_rate_bound=self._discarded_rate.tolist(),
            omitted_kernel_rate_fraction=fractions.tolist(),
            information_budget_met=bool(np.all(fractions<=self.policy.max_omitted_fraction+1e-9)),
            pruning_sort_edges=sort_edges,pruning_fast_path=fast_path,
            omission_is_prediction_error_bound=False,adjacency='lazy incoming index; sorted outgoing',
            native_route_write=False,compaction_seconds=time.perf_counter()-began))
        return f

    def _publish(self,at):
        began=time.perf_counter();old=self.published;current=(at-self.origin)//PERIOD
        old_mass=old.mass*np.exp2(-(at-old.as_of)/np.asarray(HALF_LIVES))
        closed=old.closed+((old.pending>=0)&(old.pending<current))
        pending=np.where(old.pending<current,-1,old.pending)
        if self.pending:
            u,v,t=(np.concatenate([row[i] for row in self.pending]) for i in range(3))
            window=(int(t[0])-self.origin)//PERIOD
            if np.any((t-self.origin)//PERIOD!=window) or window>current:raise AssertionError('mixed/future bin')
            observed,inverse=np.unique(pack(u,v),return_inverse=True)
            added=np.column_stack([np.bincount(inverse,weights=np.exp2(-(at-t)/h),minlength=len(observed)) for h in HALF_LIVES])
            last=np.full(len(observed),-1,np.int64);np.maximum.at(last,inverse,t)
            keys=np.union1d(old.keys,observed);i=np.searchsorted(keys,old.keys);j=np.searchsorted(keys,observed)
            mass=np.zeros((len(keys),3));mass[i]=old_mass;mass[j]+=added
            first=np.full(len(keys),window,np.int64);first[i]=old.first
            counts=np.zeros(len(keys),np.int64);counts[i]=closed
            pbin=np.full(len(keys),-1,np.int64);pbin[i]=pending
            seen=np.full(len(keys),-1,np.int64);seen[i]=old.last;seen[j]=np.maximum(seen[j],last)
            prev=np.full(len(keys),-1,np.int64);prev[i]=old.pending
            if window<current:counts[j]+=(prev[j]!=window)
            else:pbin[j]=window
        else:keys,mass,first,counts,pbin,seen=old.keys,old_mass,old.first,closed,pending,old.last
        reason='periodic' if at==self.next_tick else 'partial_prefix'
        self.published=self._compact(keys,mass,first,counts,pbin,seen,at,self.cutoff,old.version+1,reason)
        self.pending=[];self.publications[-1]['maintenance_seconds']=time.perf_counter()-began
        if self.audit:self.audit(dict(self.publications[-1]))

    def observe(self,u,v,t,first_sequence):
        u,v,t=map(np.asarray,(u,v,t))
        # Validate entire batch before chunking; never accept half an invalid batch.
        if (first_sequence!=self.cutoff or type(first_sequence) is not int or u.ndim!=1 or u.shape!=v.shape or u.shape!=t.shape
            or any(x.dtype.kind not in 'iu' for x in (u,v,t)) or np.any(u<0) or np.any(v<0)
            or np.any(u>=2**32) or np.any(v>=2**32) or np.any(t>np.iinfo(np.int64).max)
            or np.any(t[1:]<t[:-1]) or (len(t) and t[0]<self.as_of)):raise ValueError('invalid batch')
        start=0
        while start<len(t):
            end=min(len(t),start+self.policy.max_pending-self.pending_occurrences)
            super().observe(u[start:end],v[start:end],t[start:end],self.cutoff);start=end
            if self.pending_occurrences==self.policy.max_pending:self._publish(self.as_of)

    def flush_observed(self):
        """Publish exact observed partial prefix; do NOT pretend the hour closed."""
        if self.pending:self._publish(self.as_of)
