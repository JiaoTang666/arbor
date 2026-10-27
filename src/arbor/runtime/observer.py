"""Live graph and source-forest companions consuming native accepted events."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import resource
import time
import traceback

import numpy as np

from arbor.forest.adapter import SlowForestAdapter
from arbor.forest.publisher import JointPublisher
from arbor.graph.prior import GraphPrior
from arbor.io import load, save
from .compact import CompactReader, read_complete_line


def run(mode, directory):
    run = Path(directory)
    cfg = load(run/"observer.json")
    dataset, layout = Path(cfg["dataset"]), Path(cfg["layout"])
    arrays = [np.load(dataset/"test"/(x+".npy"), mmap_mode="r") for x in ("sender", "recipient", "timestamp")]
    u, v, at = arrays
    total, count = len(u), 0
    begun = time.monotonic()
    reader = CompactReader(cfg["input_sha256"], total)
    result = dict(status="FAIL", processed=0)
    result_path = run/("observer_result.json" if mode == "graph" else "forest/result.json")
    def next_line(stream):
        while True:
            row = read_complete_line(stream)
            if row is not None:
                return row
            if time.monotonic()-begun > cfg["timeout_seconds"]:
                raise TimeoutError("observer finite deadline")
            if (run/"native_failed.json").exists():
                raise RuntimeError("native process failed")
            time.sleep(.1)
    try:
        with ExitStack() as stack:
            accepted = None
            def accept():
                nonlocal accepted
                if accepted is None:
                    path = run/"offered.csv.lifecycle.v1.jsonl.observer.v1.jsonl"
                    while not path.exists():
                        if time.monotonic()-begun > cfg["timeout_seconds"]:
                            raise TimeoutError("missing accepted stream")
                        time.sleep(.1)
                    accepted = stack.enter_context(path.open())
                while True:
                    event = reader.feed(next_line(accepted))
                    if event is not None:
                        return event
            if mode == "graph":
                prior = GraphPrior.from_paths(dataset/"prepared/graph_seed", layout/"routes.npy", cfg["shards"])
                seed = prior.cutoff
                journal = stack.enter_context((run/"prior_publications.jsonl").open("x"))
                version = -1
                def publish():
                    nonlocal version
                    row = prior.snapshot
                    if row["version"] == version:
                        return
                    save(run/"prior_latest.json", row)
                    journal.write(json.dumps(dict(snapshot=row, published_at=time.time(), consumed=prior.cutoff-seed))+"\n")
                    journal.flush()
                    version = row["version"]
                publish()
                save(run/"observer_ready.json", dict(seed_cutoff=seed, source_time=prior.as_of))
                pending = []
                while count < total:
                    event = accept()
                    seq = event[1]
                    if seq != count+len(pending):
                        raise ValueError("accepted prefix gap")
                    pending.append(event)
                    hourly = int(at[seq])//3600 != int(at[count])//3600
                    if len(pending) < 1000 and seq+1 < total and not hourly:
                        continue
                    end = count+len(pending)
                    prior.observe(np.asarray(u[count:end]), np.asarray(v[count:end]), np.asarray(at[count:end]),
                                  np.asarray([x[4:6] for x in pending], np.int64), seed+count)
                    publish()
                    count, pending = end, []
                prior.publish(force=True)
                publish()
                result.update(publications=len(prior.publications), observe_seconds=prior.observe_seconds,
                              publish_seconds=prior.publish_seconds, final_cutoff=prior.cutoff)
            else:
                out = run/"forest"
                out.mkdir()
                adapter = SlowForestAdapter.from_checkpoint(dataset/"prepared/forest_seed", layout/"routes.npy", cfg["shards"])
                adapter.export_native_seed(out/"seed", np.load(dataset/"addresses.npy", mmap_mode="r"))
                bridge = JointPublisher(adapter)
                graphlog = stack.enter_context((run/"prior_publications.jsonl").open())
                journal = stack.enter_context((out/"publications.jsonl").open("x"))
                publications = 0
                def extend():
                    nonlocal count
                    row = accept()
                    if row[1] != count:
                        raise ValueError("forest accepted prefix gap")
                    bridge.accept(u[count], v[count], at[count], adapter.seed_cutoff+count, *row[4:6])
                    count += 1
                while True:
                    graph = next_line(graphlog)["snapshot"]
                    cutoff = int(graph["accepted_cutoff_exclusive"])
                    while adapter.seed_cutoff+count < cutoff:
                        extend()
                    if int(graph["source_time"]) > bridge.source_seen:
                        extend()
                    envelope = bridge.publish(graph, out/"joint_latest.json")
                    journal.write(json.dumps(dict(version=graph["version"], cutoff=cutoff,
                                                  payload_sha256=envelope["payload_sha256"]))+"\n")
                    journal.flush()
                    if publications == 0:
                        save(out/"ready.json", dict(seed_cutoff=adapter.seed_cutoff))
                    publications += 1
                    if cutoff == adapter.seed_cutoff+total:
                        break
                if bridge.pending or adapter.forest.cutoff != adapter.seed_cutoff+total:
                    raise ValueError("incomplete forest prefix")
                result.update(publications=publications, final=adapter.snapshot())
        result.update(status="PASS", processed=count)
    except BaseException as error:
        result.update(error=repr(error), traceback=traceback.format_exc(), processed=count)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    result.update(wall_seconds=time.monotonic()-begun, cpu_seconds=usage.ru_utime+usage.ru_stime,
                  peak_rss_kib=usage.ru_maxrss)
    save(result_path, result)
    if result["status"] != "PASS":
        raise RuntimeError(result["error"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("graph", "forest"))
    parser.add_argument("directory")
    args = parser.parse_args()
    run(args.mode, args.directory)
