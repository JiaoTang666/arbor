"""C ABI for the historical grouping and placement kernel."""
import ctypes
from pathlib import Path
import numpy as np

class Kernel:
    def __init__(self,path):
        self.lib=ctypes.CDLL(str(Path(path).resolve()))
        i,p=ctypes.c_int,ctypes.c_void_p;l=ctypes.c_int64;d=ctypes.c_double
        self.lib.match_round.argtypes=[l,l,i,p,p,p,p,p,i,p]
        self.lib.place.argtypes=[l,p,p,p,p,i,p,i,d,i,p,p,p]
        self.lib.ancestors.argtypes=[l,l,p,p]
        for f in (self.lib.match_round,self.lib.place,self.lib.ancestors):f.restype=i

    @staticmethod
    def ptr(a):
        assert a.flags.c_contiguous
        return a.ctypes.data_as(ctypes.c_void_p)

    def ancestors(self,parent,known):
        parent=np.ascontiguousarray(parent,dtype=np.int64);tag=np.empty_like(parent)
        if self.lib.ancestors(len(parent),known,self.ptr(parent),self.ptr(tag)):
            raise ValueError('noncausal forest parent')
        return tag

    def matching(self,a,b,size,demand,limit,max_size):
        a,b,size=[np.ascontiguousarray(x,dtype=np.int64) for x in (a,b,size)]
        demand,limit=[np.ascontiguousarray(x,dtype=np.float64) for x in (demand,limit)]
        partner=np.full(len(size),-1,np.int64)
        rc=self.lib.match_round(len(size),len(a),demand.shape[1],*[self.ptr(x) for x in
            (a,b,size,demand,limit)],max_size,self.ptr(partner))
        if rc:raise ValueError('matching bounds')
        return partner

    def placement(self,data,demand,shards,band=1.10,rounds=1):
        mean=demand.sum(axis=0)/shards
        active=mean>0
        norm=np.ascontiguousarray(demand[:,active]/mean[active])
        # Every input account contributes nonzero historical incidence.
        assert norm.shape[1]>0 and np.isfinite(norm).all()
        order=np.lexsort((np.arange(len(norm)), -norm.max(axis=1))).astype(np.int64)
        route=np.empty(len(norm),np.int32);trace=np.empty((rounds+1,3));moves=np.zeros(rounds,np.int64)
        off,peer,weight=[np.ascontiguousarray(data[x],dtype=d) for x,d in
                         [('offsets',np.int64),('peer',np.int32),('weight',np.float64)]]
        weight=weight/max(float(weight.sum()),1e-300)
        rc=self.lib.place(len(norm),self.ptr(off),self.ptr(peer),self.ptr(weight),self.ptr(norm),
             norm.shape[1],self.ptr(order),shards,band,rounds,self.ptr(route),self.ptr(trace),self.ptr(moves))
        if rc:raise ValueError('placement error %s'%rc)
        return route,dict(lexicographic_potential=trace.tolist(),moves=moves.tolist(),
                          active_coordinates=np.flatnonzero(active).tolist(),band=band)