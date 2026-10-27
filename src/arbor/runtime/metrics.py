"""One pass for completion metrics; no ownership, model or state replay."""
import csv,json
from pathlib import Path
import numpy as np


def stats(x):
    x=np.asarray(x,dtype=float)
    if not len(x):return None
    return dict(mean=float(x.mean()),p50=float(np.quantile(x,.5)),p95=float(np.quantile(x,.95)),p99=float(np.quantile(x,.99)),maximum=float(x.max()))

def collect(run,case,total):
    n=total;k=case['shards'];dynamic=case['method']=='CLPA'
    path=run/('clpa.jsonl' if dynamic else 'offered.csv.lifecycle.v1.jsonl')
    done=np.full(n,-1,np.int64);dispatch=np.full(n,-1,np.int64);mask=np.zeros(n,np.uint8)
    stages=np.zeros((k,3),np.int64);maintenance=[];decision_ns=[]
    planned=np.asarray((np.arange(n,dtype=np.int64)//case['batch_size'])*case['batch_size']/case['inject_tps']*1e9,np.int64)
    with path.open() as stream:
        for line in stream:
            x=json.loads(line);event=x.get('event')
            if event in ('failure','report_rejected'):raise ValueError('native execution reported an error')
            if dynamic and event=='dispatched':
                if not 0<=x['sequence']<n:raise ValueError('dispatch sequence out of bounds')
                dispatch[x['sequence']]=x['dispatch_elapsed_ns']
            elif not dynamic and event=='route_bound':
                if not 0<=x['ingress_sequence']<n:raise ValueError('dispatch sequence out of bounds')
                dispatch[x['ingress_sequence']]=x['native_elapsed_ns']
            if dynamic and event in ('terminal','stage_receipt'):
                seq,leg,shard,at=x['sequence'],x['leg'],x['shard'],x['registered_elapsed_ns']
            elif not dynamic and event=='leg_registered':
                d=x['details'];seq,leg,shard,at=x['ingress_sequence'],d['leg'],d['shard'],x['native_elapsed_ns']
            else:
                if event=='cost_entry_decision':decision_ns.append(x['inference_ns'])
                elif event=='epoch_complete':
                    maintenance.append({a:b for a,b in x.items() if isinstance(b,(int,float,str,bool)) and a not in ('clock_instance','recorded_at')})
                continue
            if not 0<=seq<n or not 0<=shard<k:raise ValueError('invalid metric occurrence')
            bit,col={'inner':(1,0),'relay1':(2,1),'relay2':(4,2)}[leg]
            if mask[seq]&bit:raise ValueError('duplicate stage receipt')
            mask[seq]|=bit;stages[shard,col]+=1;done[seq]=max(done[seq],at)
    if np.any(~np.isin(mask,[1,6])) or np.any(dispatch<planned) or np.any(done<dispatch):raise ValueError('transactions did not all complete')
    direct,r1,r2=map(int,stages.sum(0))
    if direct+r2!=n or r1!=r2 or int(stages.sum())!=n+r1:raise ValueError('completion/stage counts inconsistent')
    elapsed=float(done.max()/1e9)
    queues=[];fill=[]
    for shard in range(k):
        f=run/f'runtime/result/pbft_shardNum={k}/Shard{shard}{k}.csv'
        if not f.exists():continue
        with f.open() as stream:
            for row in csv.DictReader(stream):
                queues.append(int(row['TxPool Size']));fill.append(int(row['# of all Txs in this block'])/case['block_size'])
    return dict(status='PASS',**case,completed=n,unfinished=0,complete_tps=n/elapsed,
        completion_elapsed_seconds=elapsed,end_to_end_seconds=stats((done-planned)/1e9),
        predispatch_seconds=stats((dispatch-planned)/1e9),after_dispatch_seconds=stats((done-dispatch)/1e9),
        drain_after_last_scheduled_arrival_seconds=elapsed-float(planned.max()/1e9),
        total_stages=int(stages.sum()),cross_fraction=r1/n,shard_work=stages.sum(1).tolist(),
        stage_counts=stages.tolist(),max_shard_work=int(stages.sum(1).max()),
        native_queue=stats(queues),block_fill=stats(fill),maintenance_epochs=maintenance,
        decision_compute_ns=stats(decision_ns),
        clock='Scheduled batch release to latest required stage receipt on supervisor monotonic clock',
        post_run_processing='One metric pass only; no ledger scans, ownership or decision replay')
