"""One native execution path for Arbor, Monoxide and dynamic CLPA."""
from datetime import datetime, timedelta, timezone
import os
import json
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time

from arbor.build import ensure_build
from arbor.io import load, root, save, sha
from .metrics import collect
from .resources import birth, check_ports, check_resources, port_table

REMOVED_ENV = ("GOMAXPROCS", "GOMEMLIMIT", "BLOCK_EMULATOR_STATE_DB_TABLE_MB")


def clean_environment(settings=None):
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("SAA_", "BLOCK_EMULATOR_", "COOLCHAIN_", "SWIFT", "PAPER_SELF_"))
           and k not in REMOVED_ENV}
    env.update(OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1")
    env.update(settings or {})
    if any(k in env for k in REMOVED_ENV):
        raise ValueError("forbidden inherited runtime override")
    return env


def configure(target, case, dataset, layout):
    run = target/"native"
    run.mkdir()
    dataset, layout = Path(dataset), Path(layout)
    manifest = load(dataset/"manifest.json")
    total = manifest["test"]["accepted"]
    input_sha = manifest["test"]["input_sha256"]
    cfg = load(Path(__file__).with_name("engine_defaults.json"))
    cfg.update(ConsensusMethod=1 if case["method"] == "CLPA" else 3,
               ExpDataRootDir=str(run/"runtime"), DatasetFile=str(dataset/"test/input.csv"),
               TotalDataSize=total, InjectSpeed=case["inject_tps"], TxBatchSize=case["batch_size"],
               BlockSize=case["block_size"], Block_Interval=case["interval_ms"])
    table = port_table(case["shards"], case["nodes_per_shard"])
    env = dict(BLOCK_EMULATOR_CONFIG_PATH=str(run/"paramsConfig.json"),
               BLOCK_EMULATOR_STARTUP_GATE_DIR=str(run/"gate"),
               BLOCK_EMULATOR_INITIAL_LEDGER_PATH=str(layout/"initial_ledger.json"),
               BLOCK_EMULATOR_INITIAL_LEDGER_SHA256=sha(layout/"initial_ledger.json"),
               BLOCK_EMULATOR_BASELINE_CLOCK_V1="1", BLOCK_EMULATOR_STATE_DB_CACHE_MB="128",
               BLOCK_EMULATOR_STATE_DB_HANDLES="256", BLOCK_EMULATOR_STATE_TRIE_CACHE_MB="64")
    if case["method"] == "CLPA":
        env.update(BLOCK_EMULATOR_CLPA_EPOCH_V1="1", BLOCK_EMULATOR_CLPA_PLAN_PROVIDER="clpa",
                   BLOCK_EMULATOR_CLPA_EPOCH_ROUNDS="20", BLOCK_EMULATOR_CLPA_AUDIT_PATH=str(run/"clpa.jsonl"),
                   BLOCK_EMULATOR_CLPA_INPUT_SHA256=input_sha, BLOCK_EMULATOR_CLPA_BARRIER_TIMEOUT_SECONDS="1800",
                   BLOCK_EMULATOR_COMMON_BATCH_CLOCK_V1="1")
    else:
        env.update(BLOCK_EMULATOR_NATIVE_FIXED_ROUTES_V1="true" if case["method"] == "Monoxide" else "false",
                   BLOCK_EMULATOR_OFFERED_LOAD_TPS=str(case["inject_tps"]),
                   BLOCK_EMULATOR_OFFERED_LOAD_AUDIT_PATH=str(run/"offered.csv"),
                   SAA_NATIVE_BATCH_AUDIT_V1="true", SAA_NATIVE_INPUT_SHA256=input_sha)
    if case["method"] == "Arbor":
        clock = dataset/"test/source_time.bin"
        clock_sha = sha(clock)
        env.update(SAA_PLACEMENT_TELEMETRY_V1="true", SAA_PLACEMENT_TELEMETRY_V2="true",
                   SAA_ATTRIBUTION_INDEPENDENT_NEW_V1="false", SAA_NEWNEW_POLICY="cost_ranker_v1",
                   SAA_NEWNEW_COMPENSATION_V1="true", SAA_NEWNEW_CUMULATIVE_LAYOUT_V1="true",
                   SAA_NEWNEW_PRIOR_PATH=str(run/"prior_latest.json"), SAA_NEWNEW_SOURCE_TIME_PATH=str(clock),
                   SAA_NEWNEW_SOURCE_TIME_SHA256=clock_sha, SAA_NEWNEW_MODEL_PATH=str(root()/"models/admission.json"),
                   SAA_NEWNEW_MODEL_SHA256=sha(root()/"models/admission.json"),
                   SAA_SHARE_RANDOM_CONTEXT="native-contiguous10m-v1", SAA_SHARE_RANDOM_SEED="43",
                   SAA_OWNERSHIP_SECURITY_V1="false", SAA_LIVE_PLAN_URL="", SAA_FOREST_FACTS_V2="true",
                   SAA_FOREST_SEED_MANIFEST=str(run/"forest/seed/manifest.json"),
                   SAA_FOREST_JOINT_PATH=str(run/"forest/joint_latest.json"),
                   SAA_FOREST_SOURCE_TIME_PATH=str(clock), SAA_FOREST_SOURCE_TIME_SHA256=clock_sha)
        if case["ablation"] == "NoAdmission":
            env.update(BLOCK_EMULATOR_NATIVE_FIXED_ROUTES_V1="true", SAA_NEWNEW_POLICY="",
                       SAA_NEWNEW_COMPENSATION_V1="false", SAA_NEWNEW_CUMULATIVE_LAYOUT_V1="false")
        save(run/"observer.json", dict(dataset=str(dataset), layout=str(layout), input_sha256=input_sha,
                                      shards=case["shards"], timeout_seconds=case["timeout_seconds"]))
    for name, value in (("paramsConfig.json", cfg), ("environment.json", env), ("ipTable.json", table)):
        save(run/name, value)
    return run, env, total


def execute(target, case, dataset, layout, progress):
    binary, _ = ensure_build()
    run, settings, total = configure(target, case, dataset, layout)
    check_ports(load(run/"ipTable.json"))
    env = clean_environment(settings)
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, hard) if hard != -1 else 65536, hard))
    children, logs = [], []
    progress_stream, released = None, False
    accepted, completed = set(), set()
    begun, next_check = time.monotonic(), 0
    def spawn(command, name):
        log = (run/name).open("xb")
        logs.append(log)
        process = subprocess.Popen(command, cwd=run, env=env, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        children.append(process)
        save(target/"owned_processes.json", [dict(pid=p.pid, birth_ticks=birth(p.pid), command=p.args) for p in children])
        return process
    def check():
        nonlocal next_check, progress_stream
        now = time.monotonic()
        if now-begun > case["timeout_seconds"]:
            raise TimeoutError("native execution deadline")
        failed = [p for p in children if p.poll() not in (None, 0)]
        if failed:
            raise RuntimeError(f"child {failed[0].pid} exited {failed[0].returncode}; see native logs")
        if now >= next_check:
            state = check_resources(target, case["resources"], children)
            state.update(unix=time.time(), elapsed_seconds=now-begun)
            save(target/"resources.json", state)
            if released:
                path = run/("clpa.jsonl" if case["method"] == "CLPA" else "offered.csv.lifecycle.v1.jsonl.observer.v1.jsonl")
                if progress_stream is None and path.exists():
                    progress_stream = path.open()
                if progress_stream:
                    while True:
                        position = progress_stream.tell()
                        line = progress_stream.readline()
                        if not line.endswith("\n"):
                            progress_stream.seek(position)
                            break
                        event = json.loads(line)
                        if isinstance(event, list):
                            if event[0] == "r": accepted.add(event[1])
                            elif event[0] == "c": completed.add(event[1])
                        elif case["method"] == "CLPA":
                            if event.get("event") == "dispatched": accepted.add(event["sequence"])
                            elif event.get("event") == "terminal": completed.add(event["sequence"])
                progress("RUNNING", accepted_transactions=len(accepted), completed_transactions=len(completed),
                         elapsed_seconds=now-begun)
            next_check = now+5
    def wait_file(path):
        while not path.exists():
            check()
            time.sleep(.1)
    progress("STARTING", expected=total)
    try:
        if case["method"] == "Arbor":
            spawn([sys.executable, "-B", "-m", "arbor.runtime.observer", "graph", str(run)], "graph.log")
            wait_file(run/"observer_ready.json")
            spawn([sys.executable, "-B", "-m", "arbor.runtime.observer", "forest", str(run)], "forest.log")
            wait_file(run/"forest/ready.json")
        command = [str(binary), "-S", str(case["shards"]), "-N", str(case["nodes_per_shard"])]
        supervisor = spawn(command+["-c"], "supervisor.log")
        native = [supervisor]
        for shard in range(case["shards"]):
            for node in range(case["nodes_per_shard"]):
                native.append(spawn(command+["-s", str(shard), "-n", str(node)], f"node_{shard}_{node}.log"))
        wait_file(run/"gate/ready.json")
        ready = load(run/"gate/ready.json")
        if len(set(ready["endpoints"])) != case["shards"]*case["nodes_per_shard"]:
            raise ValueError("incomplete startup topology")
        actual = []
        for child in native:
            entries = Path(f"/proc/{child.pid}/environ").read_bytes().split(b"\0")
            actual_env = dict(e.split(b"=", 1) for e in entries if b"=" in e)
            if any(key.encode() in actual_env for key in REMOVED_ENV):
                raise ValueError("runtime process inherited a forbidden override")
            actual.append(dict(pid=child.pid, birth_ticks=birth(child.pid), overrides_absent=True))
        save(target/"launch_environment.json", dict(processes=actual, cpu_affinity=sorted(os.sched_getaffinity(0)),
             settings=settings, binary_sha256=sha(binary), input_sha256=sha(Path(dataset)/"test/input.csv")))
        at = datetime.now(timezone.utc)
        save(run/"gate/release.json", dict(binding=ready["binding"], issued_at=at.isoformat(),
                                           expires_at=(at+timedelta(seconds=30)).isoformat()))
        released = True
        progress("RUNNING", expected=total)
        while any(p.poll() is None for p in children):
            check()
            time.sleep(.3)
        check()
        save(target/"process_exit.json", dict(status="PASS", returncodes={str(p.pid): p.returncode for p in children}))
        progress("STATISTICS", expected=total)
        metrics = collect(run, case, total)
        if case["method"] == "Arbor":
            for key, path in (("graph_maintenance", "observer_result.json"), ("forest_maintenance", "forest/result.json")):
                row = load(run/path)
                if row["status"] != "PASS" or row["processed"] != total:
                    raise ValueError("required companion did not consume the entire input")
                metrics[key] = {k: v for k, v in row.items() if k != "final"}
        metrics.update(binary_sha256=sha(binary), input_sha256=sha(Path(dataset)/"test/input.csv"),
                       model_sha256=sha(root()/"models/admission.json") if case["method"] == "Arbor" else None)
        save(target/"performance.json", metrics)
        return metrics
    except BaseException:
        save(run/"native_failed.json", dict(unix=time.time()))
        raise
    finally:
        for process in children:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in children:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        for log in logs:
            log.close()
        if progress_stream:
            progress_stream.close()
