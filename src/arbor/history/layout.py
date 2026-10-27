"""Content-addressed layouts shared across load levels; no experiment-directory imports."""
import json
from pathlib import Path
import struct
import subprocess

import numpy as np

from arbor.build import ensure_build
from arbor.io import fingerprint, load, root, save, sha
from .grouping import aggregate_groups, groups
from .kernel import Kernel
from .prepare import load_history, prepare
from .relations import demand_and_relation, snapshot

INITIAL_BALANCE = "100000000000000000000000000000000000000000000"


def plan_snapshot(data, snapshot_value, model, config, kernel):
    """Pure historical planning, shared by experiment and training-prefix preparation."""
    s, arm, shards = snapshot_value, config.get("ablation", "Full"), config["shards"]
    weights, demand = demand_and_relation(s, model, "forest_off" if arm == "NoForestLayout" else "conditioned")
    if arm == "NoTime":
        raw = data["graph"]["count"][s["known"]].astype(float)
        weights = raw*(weights.sum()/raw.sum())
    tree = {}
    if arm == "NoGraphLayout":
        labels, gd, stats = np.arange(len(demand)), demand, []
        u, v, weights = np.array([], dtype=np.int64), np.array([], dtype=np.int64), np.array([], dtype=float)
    else:
        u, v = s["u"], s["v"]
        labels, gd, tree, stats = groups(kernel, u, v, weights, demand, shards, rounds=6, max_size=config["group_size"])
    adjacency, _, _ = aggregate_groups(labels, np.arange(len(gd)), u, v, weights)
    routes, trace = kernel.placement(adjacency, gd, shards, config["tolerance"])
    return routes[labels], labels, tree, dict(grouping=stats, placement=trace)


def write_ledger(path, addresses, routes, shards):
    with Path(path).open("x", encoding="utf-8") as stream:
        stream.write('{"schema":"synthetic_initial_ledger_v1","shards":'+str(shards)+',"accounts":[')
        for i, (address, route) in enumerate(zip(addresses, routes)):
            if i:
                stream.write(",")
            json.dump(dict(account=address.decode(), balance=INITIAL_BALANCE, nonce=0,
                           storage_root="", code_hash="", shard=int(route)), stream, separators=(",", ":"))
        stream.write("]}\n")


def clpa_layout(data, addresses, output, shards):
    graph = data["graph"]
    mask = graph["sender"] != graph["recipient"]
    path = output/"clpa_input.bin"
    with path.open("xb") as stream:
        stream.write(struct.pack("<q", len(addresses)))
        stream.write(addresses.tobytes())
        stream.write(struct.pack("<q", int(mask.sum())))
        records = np.empty(int(mask.sum()), dtype=[("u", "<u4"), ("v", "<u4"), ("count", "<i8")])
        for field, name in (("u", "sender"), ("v", "recipient"), ("count", "count")):
            records[field] = graph[name][mask]
        stream.write(records.tobytes())
    with (output/"clpa_layout.log").open("xb") as log:
        subprocess.run([str(root()/".build/arbor-clpa-layout"), "--input", str(path),
                        "--output", str(output/"clpa_routes.bin"), "--shards", str(shards)],
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    route = np.fromfile(output/"clpa_routes.bin", dtype="<i4")
    path.unlink()
    return route


def build_layout(dataset, config):
    dataset = Path(dataset).resolve()
    _, library = ensure_build()
    prepare(dataset)
    method = config["method"]
    arm = config.get("ablation", "Full")
    if arm == "NoAdmission":
        arm = "Full"
    params = dict(method=method, ablation=arm, shards=config["shards"],
                  half_seconds=config["half_seconds"], group_size=config["group_size"],
                  tolerance=config["tolerance"], dataset_sha256=sha(dataset/"manifest.json"),
                  fusion_sha256=sha(root()/"models/fusion.json"), kernel_sha256=sha(library))
    output = dataset/"layouts"/fingerprint(params)[:20]
    if (output/"complete.json").exists():
        record = load(output/"complete.json")
        for name, expected in record["files"].items():
            if sha(output/name) != expected:
                raise ValueError("cached layout member changed: "+name)
        return output
    output.mkdir(parents=True, exist_ok=False)
    addresses = np.load(dataset/"addresses.npy", mmap_mode="r")
    shards = config["shards"]
    details = {}
    if method == "Monoxide":
        route = np.array([int(a[-8:], 16) % shards for a in addresses], np.int32)
    else:
        kernel = Kernel(library)
        data = load_history(dataset, kernel)
        if method == "CLPA":
            route = clpa_layout(data, addresses, output, shards)
        else:
            s = snapshot(data, int(data["t"][-1]), inclusive=True, half=config["half_seconds"])
            route, labels, tree, details = plan_snapshot(data, s, load(root()/"models/fusion.json"),
                                                         dict(config, ablation=arm), kernel)
            np.savez(output/"hierarchy.npz", **tree)
            np.save(output/"groups.npy", labels.astype(np.int32))
    if len(route) != len(addresses) or np.any((route < 0) | (route >= shards)):
        raise ValueError("layout coverage mismatch")
    np.save(output/"routes.npy", route.astype(np.int32))
    write_ledger(output/"initial_ledger.json", addresses, route, shards)
    save(output/"complete.json", dict(config=params, details=details,
         files={p.name: sha(p) for p in output.iterdir() if p.is_file()}))
    return output
