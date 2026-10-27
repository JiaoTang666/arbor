"""Historical temporal rates and source-attributed demand."""
import numpy as np

def cohort_credit(u,v,known,tag):
    """Partition each future-new transaction once; unresolved new-new gets -1.

    The two endpoint shares are attribution conventions, not execution stages.
    Self transfers have one distinct endpoint. Already-known pairs get no credit.
    """
    u,v=np.asarray(u),np.asarray(v)
    a=u>=known;b=(v>=known)&(v!=u)
    count=a.astype(np.int8)+b.astype(np.int8)
    rows=np.r_[np.flatnonzero(a),np.flatnonzero(b)]
    owners=np.r_[tag[u[a]],tag[v[b]]]
    shares=1.0/count[rows]
    total=np.bincount(rows,weights=shares,minlength=len(u))
    if not np.array_equal(total,(count>0).astype(float)):raise AssertionError('cohort conservation')
    return rows,owners,shares

def recurrence_class(edge_id,times,edges):
    order=np.argsort(edge_id,kind='stable');ids=edge_id[order];t=times[order]
    same=ids[1:]==ids[:-1];gap=np.diff(t)[same].astype(float);account=ids[1:][same]
    count=np.bincount(account,minlength=edges)
    total=np.bincount(account,weights=gap,minlength=edges)
    squares=np.bincount(account,weights=gap*gap,minlength=edges)
    mean=np.divide(total,count,out=np.zeros(edges),where=count>0)
    var=np.divide(squares,count,out=np.zeros(edges),where=count>0)-mean*mean
    cv=np.divide(np.sqrt(np.maximum(0,var)),mean,out=np.full(edges,np.inf),where=mean>0)
    # At least three gaps; small samples are explicitly kept in the unknown class.
    cls=np.zeros(edges,np.int8)
    cls[(count>=3)&(cv<=1)]=1;cls[(count>=3)&(cv>1)]=2
    return cls,dict(unknown=int((cls==0).sum()),distributed=int((cls==1).sum()),
                   bursty=int((cls==2).sum()),definition='>=3 gaps; CV<=1 versus >1; zero gaps bursty')

def snapshot(data,at,inclusive=False,half=(21600.,172800.,302400.)):
    HALF = np.asarray(half, dtype=float)
    u,v,t,edge=[data[k] for k in ['u','v','t','edge']]
    f=data['forest'];cut=int(np.searchsorted(t,at,side='right' if inclusive else 'left'));n=int(np.searchsorted(f['first_sequence'],cut,side='left'))
    counts=np.bincount(edge[:cut],minlength=len(data['graph']['sender']));known=np.flatnonzero(counts>0)
    em=np.empty((len(counts),3));am=np.empty((n,3));birth=np.empty((n,3))
    er=data['rows'];mask=(er<cut)&(data['owners']>=0);er=er[mask];owner=data['owners'][mask];credit=data['shares'][mask]
    normalizer=np.log(2)/HALF/(1-np.exp2(-(at-int(t[0]))/HALF))
    for k,h in enumerate(HALF):
        w=np.exp2(-(at-t[:cut])/h);nonself=u[:cut]!=v[:cut]
        em[:,k]=np.bincount(edge[:cut],weights=w,minlength=len(counts))*normalizer[k]
        am[:,k]=(np.bincount(u[:cut],weights=w,minlength=n)+np.bincount(v[:cut][nonself],weights=w[nonself],minlength=n))*normalizer[k]
        birth[:,k]=np.bincount(owner,weights=credit*np.exp2(-(at-t[er])/h),minlength=n)*normalizer[k]
    cls,summary=recurrence_class(edge[:cut],t[:cut],len(counts))
    for name,c in [('unknown',0),('distributed',1),('bursty',2)]:summary[name]=int(np.count_nonzero(cls[known]==c))
    mature={}
    for k,h in enumerate([3600,21600,86400]):
        eligible=(f['parent']>=0)&(f['first_timestamp']+h<at)
        values=data['profiles']['followup_incidence_observed'][eligible,k]
        mature[str(h)]=dict(count=len(values),mean_endpoint_incidence=float(values.mean()) if len(values) else 0,
                           zero_fraction=float(np.mean(values==0)) if len(values) else None,
                           censored_direct_children=int(np.count_nonzero((f['parent'][:n]>=0)&~eligible[:n])))
    return dict(at=int(at),cut=cut,n=n,known=known,erate=em[known],arate=am,entry=birth,
                cls=cls[known],classes=summary,mature=mature,
                u=data['graph']['sender'][known],v=data['graph']['recipient'][known])

def demand_and_relation(s,model,arm):
    if arm=='day_only':weight=s['erate'][:,1]
    elif arm=='fixed_mix':weight=s['erate'].mean(axis=1)
    else:weight=np.sum(s['erate']*np.asarray(model['conditioned_weights'])[s['cls']],axis=1)
    entry=s['entry']@np.asarray(model['entry_weights'])
    demand=s['arate'] if arm=='forest_off' else np.column_stack((s['arate'],entry))
    return weight,demand