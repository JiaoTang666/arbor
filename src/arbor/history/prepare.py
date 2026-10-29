"""Reconstruct historical representations from the distributed transaction input."""
from pathlib import Path
import numpy as np

from arbor.data import verify
from arbor.cache import cached_directory, producer
from arbor.forest.source import SourceForest
from arbor.io import load, save, sha
from .representation import aggregate_counts, aggregate_graph, temporal_profiles
from .relations import cohort_credit

ONLINE_HALF = (21600, 86400, 604800)


def prepare_counts(dataset, receipt=None):
    """Build the historical count graph used by CLPA."""
    dataset = Path(dataset)
    verify(dataset, receipt)
    identity = dict(dataset_sha256=sha(dataset/"manifest.json"),
                    producer=producer("src/arbor/history/prepare.py", "src/arbor/history/representation.py",
                                      "src/arbor/cache.py", "src/arbor/io.py"))
    def build(output):
        u, v = [np.load(dataset/"history"/(name+".npy"), mmap_mode="r")
                for name in ("sender", "recipient")]
        graph = aggregate_counts(u, v)
        np.savez(output/"graph.npz", **graph)
        return dict(accepted=len(u), edges=len(graph["count"]))
    return cached_directory(dataset/"counts", identity, build)


def prepare(dataset, receipt=None):
    dataset = Path(dataset)
    output = dataset/"prepared"
    manifest = verify(dataset, receipt)
    identity = dict(dataset_sha256=sha(dataset/"manifest.json"), half_seconds=ONLINE_HALF,
                    producer=producer("src/arbor/history/prepare.py", "src/arbor/history/representation.py",
                                      "src/arbor/history/relations.py", "src/arbor/forest/source.py",
                                      "src/arbor/cache.py", "src/arbor/io.py"))
    return cached_directory(output, identity, lambda staging: _prepare(dataset, staging, manifest))


def _prepare(dataset, output, manifest):
    u, v, t = [np.load(dataset/"history"/(x+".npy"), mmap_mode="r")
               for x in ("sender", "recipient", "timestamp")]
    n = manifest["history"]["accounts"]
    forest = dict(np.load(dataset/"entry_forest.npz"))
    graph, windows = aggregate_graph(u, v, t, int(t[0]), int(t[-1]), 3600, ONLINE_HALF)
    np.savez(output/"graph.npz", **graph)
    profiles, _ = temporal_profiles(dict(sender=u, recipient=v, timestamp=t), forest, n,
                                    int(t[0]), int(t[-1]), 3600, ONLINE_HALF, (3600,21600,86400))
    np.savez(output/"profiles.npz", **profiles)
    index = output/"graph_seed"
    index.mkdir()
    fields = {"edge_"+k: graph[k] for k in ("sender", "recipient", "decayed_mass", "first_timestamp", "last_timestamp")}
    fields["window_offsets"] = np.r_[0, np.cumsum(np.bincount(windows["edge"], minlength=len(graph["sender"])))]
    for name, values in fields.items():
        np.save(index/(name+".npy"), values)
    save(index/"manifest.json", dict(status="PASS", half_lives_seconds=ONLINE_HALF, bin_seconds=3600,
         as_of_source_timestamp=int(t[-1]), source_start_timestamp=int(t[0]), accepted_cutoff_exclusive=len(t),
         artifacts={p.name: dict(sha256=sha(p)) for p in index.glob("*.npy")}))
    source = SourceForest(capacity=n, namespace="history9m")
    for i, (a, b, at) in enumerate(zip(u, v, t)):
        source.observe(a, b, at, i)
    source.save(output/"forest_seed")
    return dict(accepted=len(t), accounts=n)


def load_history(dataset, kernel, prepared=None):
    dataset = Path(dataset)
    prepared = Path(prepared) if prepared is not None else prepare(dataset)
    u, v, t = [np.load(dataset/"history"/(x+".npy"), mmap_mode="r")
               for x in ("sender", "recipient", "timestamp")]
    forest = dict(np.load(dataset/"entry_forest.npz"))
    graph = dict(np.load(prepared/"graph.npz"))
    profiles = dict(np.load(prepared/"profiles.npz"))
    key = (graph["sender"].astype(np.uint64)<<np.uint64(32)) | graph["recipient"].astype(np.uint64)
    edge = np.searchsorted(key, (u.astype(np.uint64)<<np.uint64(32)) | v.astype(np.uint64))
    rows, owners, shares = [], [], []
    start, last = int(t[0]), int(t[-1])
    for day in range((last-start)//86400+1):
        lo, hi = np.searchsorted(t, [start+day*86400, start+(day+1)*86400])
        known = int(np.searchsorted(forest["first_sequence"], lo))
        tag = kernel.ancestors(forest["parent"], known)
        r, o, w = cohort_credit(u[lo:hi], v[lo:hi], known, tag)
        rows.append(r+lo); owners.append(o); shares.append(w)
    return dict(u=u, v=v, t=t, forest=forest, graph=graph, profiles=profiles, edge=edge,
                rows=np.concatenate(rows), owners=np.concatenate(owners), shares=np.concatenate(shares))
