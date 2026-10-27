"""Explicit historical-only sample generation, fitting and export."""
import copy
import gc
import json
import math
from pathlib import Path

import numpy as np

from arbor.admission.candidates import choose_candidate
from arbor.admission.model import fit
from arbor.build import ensure_build
from arbor.config import DEFAULTS
from arbor.data import ingest, make_demo
from arbor.forest.source import SourceForest
from arbor.graph.periodic import Frame
from arbor.history.kernel import Kernel
from arbor.history.layout import plan_snapshot
from arbor.history.prepare import load_history
from arbor.history.relations import cohort_credit, snapshot
from arbor.history.representation import aggregate_graph
from arbor.io import load, root, save, sha
from .fusion import fit_simplex

TRAIN_HALF = (21600., 86400., 604800.)
DAY = 86400


def fit_fusion(data, kernel, days=(12, 16)):
    samples, classes, entries, metadata = [], [[], [], []], [], []
    for day in days:
        at = int(data["t"][0])+day*DAY
        if at+DAY > int(data["t"][-1]):
            raise ValueError("history does not cover the requested training window")
        s = snapshot(data, at, half=TRAIN_HALF)
        cut, stop = s["cut"], int(np.searchsorted(data["t"], at+DAY))
        y = np.bincount(data["edge"][cut:stop], minlength=len(data["graph"]["sender"]))[s["known"]]
        tag = kernel.ancestors(data["forest"]["parent"], s["n"])
        _, owners, credits = cohort_credit(data["u"][cut:stop], data["v"][cut:stop], s["n"], tag)
        entry = np.bincount(owners[owners >= 0], weights=credits[owners >= 0], minlength=s["n"])
        x = s["erate"]*DAY
        samples.append((x, y)); entries.append((s["entry"]*DAY, entry))
        for cls in range(3):
            mask = s["cls"] == cls
            classes[cls].append((x[mask], y[mask]))
        metadata.append(dict(day=day, prefix=cut, label_stop=stop, label_max_timestamp=int(data["t"][stop-1])))
    global_alpha, global_fit = fit_simplex(samples)
    fitted = [fit_simplex(items, global_alpha) for items in classes]
    entry, entry_fit = fit_simplex(entries)
    return dict(global_weights=global_alpha.tolist(), conditioned_weights=[x.tolist() for x, _ in fitted],
                entry_weights=entry.tolist(), global_fit=global_fit, class_fit=[r for _, r in fitted],
                entry_fit=entry_fit, training_prefixes=metadata, regularization=.01, half_seconds=TRAIN_HALF)


def prepare_window(data, kernel, model, day, directory):
    at = int(data["t"][0])+day*DAY
    s = snapshot(data, at, half=TRAIN_HALF)
    stop = int(np.searchsorted(data["t"], at+DAY))
    if at+DAY > int(data["t"][-1]) or stop <= s["cut"]:
        raise ValueError("training requires a complete chronological window")
    route, _, _, _ = plan_snapshot(data, s, model, dict(DEFAULTS, shards=16), kernel)
    directory.mkdir(parents=True)
    np.save(directory/"routes.npy", route)
    u, v, t = (data[k][:s["cut"]] for k in ("u", "v", "t"))
    graph, windows = aggregate_graph(u, v, t, int(t[0]), at, 3600, TRAIN_HALF)
    keys = (graph["sender"].astype(np.uint64)<<np.uint64(32)) | graph["recipient"].astype(np.uint64)
    first = (graph["first_timestamp"]-int(t[0]))//3600
    current = (at-int(t[0]))//3600
    pending = np.where((graph["last_timestamp"]-int(t[0]))//3600 == current, current, -1)
    closed = np.bincount(windows["edge"], minlength=len(keys))-(pending >= 0)
    frame = dict(keys=keys, mass=graph["decayed_mass"], first=first, closed=closed, pending=pending,
                 last=graph["last_timestamp"], origin=int(t[0]), as_of=at, cutoff=s["cut"], version=0)
    forest = SourceForest(capacity=s["n"], namespace="history9m")
    for i, (a, b, stamp) in enumerate(zip(u, v, t)):
        forest.observe(a, b, stamp, i)
    forest.advance(at)
    forest.save(directory/"forest")
    np.savez(directory/"frame.npz", **frame)
    for key, name in (("u", "sender"), ("v", "recipient"), ("t", "timestamp")):
        np.save(directory/(name+".npy"), data[key][s["cut"]:stop])
    save(directory/"manifest.json", dict(day=day, as_of=at, cutoff=s["cut"], stop=stop,
         files={str(p.relative_to(directory)): sha(p) for p in directory.rglob("*") if p.is_file()}))
    return directory


def collect_costs(directory, day, family, *, small=False):
    from .simulator.calendar import Calendar
    from .simulator.cost import CostEnv
    from .simulator.fork import capture_pressure_state, rollout_candidate
    from .simulator.prior import JointFactsPrior
    source = tuple(np.load(directory/(k+".npy"), mmap_mode="r") for k in ("sender", "recipient", "timestamp"))
    def prior():
        fields = dict(np.load(directory/"frame.npz"))
        for key in ("origin", "as_of", "cutoff", "version"):
            fields[key] = int(fields[key])
        return JointFactsPrior(Frame(**fields), np.load(directory/"routes.npy"), directory/"forest", 16,
                               continuous_source=True)
    captured = []
    def capture(env, context):
        captured.append((capture_pressure_state(env), copy.deepcopy(context)))
    env = CostEnv(source=source, prior_factory=prior,
                  calibration=load(root()/"configs/training/calibration.json"),
                  calendar=Calendar(load(root()/f"configs/training/fit_{family}_0.json")),
                  placement_context=f"cost-ranker-v1/fit/day{day}/{family}", cost_policy="P", capture_callback=capture,
                  shards=16, capacity=100 if small else 1000, interval=5., tps=40 if small else 2000,
                  batch=20 if small else 1000, record=False,
                  sample_times=(0., 2., 4.) if small else (0.,5.,10.,20.,35.,50.,65.,80.,95.,110.,130.,150.))
    env.reset(seed=43)
    while True:
        _, _, done, truncated, info = env.step()
        if truncated:
            raise ValueError("training backbone truncated")
        if done:
            break
    report = info["episode_report"]
    if report["unfinished"] or report["receipt_missing"]:
        raise ValueError("training backbone did not drain")
    baseline = {k: np.array(v, copy=True) for k, v in dict(routes=env.route, completed=env.done_at,
                                                         received=env.received_at, work=env.work).items()}
    del env
    gc.collect()
    records = []
    for state, ctx in captured:
        costs, first = [], None
        for index, candidate in enumerate(ctx["candidates"]):
            branch = rollout_candidate(state, int(candidate))
            np.testing.assert_array_equal(branch["routes"][:state.sequence], baseline["routes"][:state.sequence])
            if index == 0:
                for key in baseline:
                    np.testing.assert_array_equal(branch[key], baseline[key])
                first = branch["completed"]
            difference = math.fsum(float(y-x) for x, y in zip(first, branch["completed"]))
            costs.append(difference/5000.)
        records.append(dict(day=day, family=family, sequence=ctx["sequence"], raw_features=ctx["raw_features"],
                            pressure=ctx["pressure"], candidates=ctx["candidates"], costs=costs,
                            r4_target=choose_candidate("R4", ctx["candidates"], ctx["pressure"]),
                            full_drain=True, baseline_exact=True))
    if not records:
        raise ValueError("training window contained no sampled joint-admission decisions")
    save(directory/(family+"_records.json"), records)
    return records


def train(kind, dataset, output, *, small=False):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    if small:
        raw = make_demo(output/"fixture")
        dataset = ingest(raw/"history.csv", raw/"test.csv", output/"dataset")
    dataset = Path(dataset)
    _, library = ensure_build()
    kernel = Kernel(library)
    data = load_history(dataset, kernel)
    if kind == "fusion":
        model = fit_fusion(data, kernel, (1, 3) if small else (12, 16))
    else:
        early = fit_fusion(data, kernel, (1, 3) if small else (8, 12))
        later = early if small else load(root()/"models/fusion.json")
        save(output/"early_fusion.json", early)
        records = []
        for day in ((4,) if small else range(13, 21)):
            window = prepare_window(data, kernel, early if day < 17 else later, day, output/f"day{day}")
            for family in (("N", "O") if small else ("N", "E", "L", "O", "C")):
                records.extend(collect_costs(window, day, family, small=small))
        save(output/"records.json", records)
        model = fit(records, source_hashes={"records.json": sha(output/"records.json")})
    save(output/"model.json", model)
    receipt = dict(status="PASS", kind=kind, small_fixture=small, model=str(output/"model.json"),
                   model_sha256=sha(output/"model.json"), dataset_sha256=sha(dataset/"manifest.json"),
                   native_experiments_started=0)
    save(output/"complete.json", receipt)
    return receipt
