"""Deterministic relation grouping and group adjacency."""
import numpy as np

def groups(kernel,u,v,weight,demand,shards,rounds=6,max_size=64):
    """Rebuild each arm from singleton leaves; real edges only, no forest edges."""
    n=len(demand);label=np.arange(n,dtype=np.int64);size=np.ones(n,np.int64)
    gd=demand.copy();limit=demand.sum(axis=0)/shards
    node=np.arange(n,dtype=np.int64);left=[];right=[];stats=[];next_id=n
    for level in range(rounds):
        gu,gv=label[u],label[v];g=len(size);mask=gu!=gv
        low,high=np.minimum(gu[mask],gv[mask]),np.maximum(gu[mask],gv[mask])
        key,inv=np.unique(low*g+high,return_inverse=True)
        if not len(key):break
        bridge=np.bincount(inv,weights=weight[mask],minlength=len(key))
        a,b=key//g,key%g
        incident=np.bincount(gu,weights=weight,minlength=g)+np.bincount(gv[mask],weights=weight[mask],minlength=g)
        score=2*bridge/np.maximum(incident[a]+incident[b],1e-300)
        order=np.lexsort((b,a,-score));partner=kernel.matching(a[order],b[order],size,gd,limit,max_size)
        first=np.flatnonzero((partner>=0)&(np.arange(g)<partner));second=partner[first]
        if not len(first):break
        representative=np.arange(g);representative[second]=first
        roots,new_label=np.unique(representative,return_inverse=True)
        new_nodes=node[roots].copy()
        slot=np.searchsorted(roots,first)
        new_nodes[slot]=np.arange(next_id,next_id+len(first))
        left.extend(node[first]);right.extend(node[second]);next_id+=len(first)
        label=new_label[label];size=np.bincount(new_label,weights=size).astype(np.int64)
        gd=np.column_stack([np.bincount(new_label,weights=gd[:,c]) for c in range(gd.shape[1])])
        node=new_nodes
        stats.append(dict(round=level,merges=len(first),groups=len(size)))
    if size.max()>max_size or size.sum()!=n:raise AssertionError('group coverage')
    return label,gd,dict(left=np.asarray(left,np.int64),right=np.asarray(right,np.int64),roots=node),stats

def aggregate_groups(group, nodes, sender, recipient, weight, order=None):
    """Match the original complete directed-edge aggregation exactly."""
    group = np.asarray(group)
    u0, v0, w = map(np.asarray, (sender, recipient, weight))
    if (u0.ndim != 1 or u0.shape != v0.shape or u0.shape != w.shape
            or u0.dtype.kind not in "iu" or v0.dtype.kind not in "iu"
            or np.any(u0 < 0) or np.any(v0 < 0)
            or np.any(u0 >= len(group)) or np.any(v0 >= len(group))
            or np.any(~np.isfinite(w)) or np.any(w < 0)):
        raise ValueError("invalid historical edge vectors")
    u, v, g = group[u0], group[v0], len(nodes)
    same = u == v
    internal = np.bincount(u[same], weights=w[same], minlength=g)
    low = np.minimum(u[~same], v[~same]).astype(np.int64)
    high = np.maximum(u[~same], v[~same])
    keys, inv = np.unique(low * g + high, return_inverse=True)
    bw = np.bincount(inv, weights=w[~same])
    a, b = (keys // g).astype(np.int32), (keys % g).astype(np.int32)
    owner, peer = np.concatenate((a, b)), np.concatenate((b, a))
    weights = np.concatenate((bw, bw))
    sort = np.argsort(owner, kind="stable")
    off = np.r_[0, np.cumsum(np.bincount(owner, minlength=g), dtype=np.int64)]
    incident = internal + np.bincount(owner, weights=weights, minlength=g)
    if order is None:
        order = np.lexsort((nodes, -incident)).astype(np.int64)
    order = np.asarray(order)
    if (order.shape != (g,) or order.dtype.kind not in "iu"
            or not np.array_equal(np.sort(order), np.arange(g))):
        raise ValueError("descent order must be a permutation of groups")
    return dict(offsets=off, peer=peer[sort], weight=weights[sort], internal=internal,
                order=np.ascontiguousarray(order, dtype=np.int64)), u, v