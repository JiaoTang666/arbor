"""Persistent serial queue: one owner, immutable attempts, explicit database retention."""
import argparse
import csv
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import traceback

from arbor.build import ensure_build
from arbor.data import ingest, make_demo, verify, verification_receipt
from arbor.cache import file_lock
from arbor.history.layout import build_layout
from arbor.io import load, resolve_path, root, save, sha
from .artifacts import build_snapshot, load_build, relative_path, resolve_run_path
from .resources import RunLock, alive, birth, check_resources, tree_rss
from .worker import clean_environment, execute


def summarize(directory):
    rows = []
    for case in load(directory/"plan.json")["cases"]:
        path = directory/"cases"/case["id"]/"complete.json"
        if path.exists():
            rows.append(load(path.parent/"performance.json"))
    save(directory/"summary.json", rows)
    with (directory/"summary.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["id", "method", "shards", "inject_tps", "completed", "complete_tps",
                                                    "mean_seconds", "p95_seconds", "p99_seconds", "cross_fraction", "rho_work"])
        writer.writeheader()
        for r in rows:
            writer.writerow({**{k: r[k] for k in ("id", "method", "shards", "inject_tps", "completed", "complete_tps", "cross_fraction")},
                **{k+"_seconds": r["end_to_end_seconds"][k] for k in ("mean", "p95", "p99")},
                "rho_work": max(r["shard_work"])/(sum(r["shard_work"])/len(r["shard_work"]))})
    return rows


def retain_and_cleanup(target, keep_db):
    if load(target/"process_exit.json")["status"] != "PASS" or load(target/"performance.json")["status"] != "PASS":
        raise ValueError("only successful exited runs may be retained/cleaned")
    for identity in load(target/"owned_processes.json"):
        if alive(identity):
            raise ValueError("owned process is still alive")
    bundle = target/"bundle"
    bundle.mkdir(exist_ok=True)
    names = ("configuration.json", "performance.json", "process_exit.json", "launch_environment.json")
    manifest = {name: sha(target/name) for name in names}
    def copy_verified(destination):
        destination.mkdir(exist_ok=True)
        for name, expected in manifest.items():
            output = destination/name
            if output.exists():
                if sha(output) != expected:
                    raise ValueError("retained package member hash mismatch: "+str(output))
            else:
                temporary = destination/("."+name+".part")
                shutil.copyfile(target/name, temporary)
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                if sha(temporary) != expected:
                    raise ValueError("result copy verification failed: "+name)
                temporary.replace(output)
        save(destination/"manifest.json", manifest)
    copy_verified(bundle)
    save(bundle/"manifest.json", manifest)
    retained = target.parents[1]/"retained"/target.name
    retained.parent.mkdir(exist_ok=True)
    copy_verified(retained)
    for path in (bundle, retained, retained.parent):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    released = 0
    native = target/"native"
    started = target/"cleanup_started.json"
    if not keep_db and native.exists():
        if native.is_symlink() or native.resolve().parent != target.resolve():
            raise ValueError("cleanup path escaped its owned case")
        for directory, dirs, files in os.walk(native, followlinks=False):
            for name in dirs+files:
                path = Path(directory)/name
                if path.is_symlink() or (path.is_file() and path.stat().st_nlink != 1):
                    raise ValueError("cleanup refuses symlink or shared hardlink: "+str(path))
                if path.is_file():
                    released += path.stat().st_size
        if not started.exists():
            save(started, dict(removed_file_bytes=released, path=str(native)))
        shutil.rmtree(native)
    if started.exists():
        released = load(started)["removed_file_bytes"]
    save(target/"cleanup.json", dict(status="PASS", keep_db=keep_db, removed_path=str(native) if not keep_db else None,
                                     removed_file_bytes=released, retained_manifest=manifest))


def model_snapshots(directory, config):
    record = directory/"models.json"
    if record.exists():
        models = load(record)
        for key, row in models.items():
            # v0.3.1 stored absolute paths. The canonical run-local copy survives a move.
            canonical = directory/"models"/(key.removesuffix("_model")+".json")
            path = canonical if canonical.is_file() else resolve_run_path(directory, row["path"])
            if sha(path) != row["sha256"]:
                raise ValueError("run model snapshot changed")
            if path != canonical:
                canonical.parent.mkdir(exist_ok=True)
                shutil.copyfile(path, canonical)
                if sha(canonical) != row["sha256"]:
                    raise ValueError("run model changed while restoring its snapshot")
                path = canonical
            row["path"] = relative_path(directory, path)
        save(record, models)
    else:
        models = {}
        for key, fallback in (("fusion_model", "fusion"), ("admission_model", "admission")):
            source = Path(config.get(key, root()/"models"/(fallback+".json")))
            destination = directory/"models"/(fallback+".json")
            destination.parent.mkdir(exist_ok=True)
            shutil.copyfile(source, destination)
            models[key] = dict(path=relative_path(directory, destination), sha256=sha(destination), source=str(source))
        save(record, models)
    return {key: str(resolve_run_path(directory, row["path"])) for key, row in models.items()}


def build_snapshots(directory):
    return build_snapshot(directory, ensure_build)


def dataset_location(directory, plan, dataset=None):
    """Persist the dataset relative to a run; explicit relocation preserves its identity."""
    candidate = (resolve_path(dataset) if dataset is not None
                 else resolve_run_path(directory, plan["config"]["dataset"]))
    marker = directory/"dataset_identity.json"
    identity = load(marker) if marker.exists() else {}
    expected = identity.get("manifest_sha256")
    if dataset is not None and not expected:
        original = resolve_run_path(directory, plan["config"]["dataset"])/"manifest.json"
        if not original.is_file():
            raise ValueError("original dataset identity is unavailable; restore it or start a new run directory")
        expected = sha(original)
    if expected:
        if not (candidate/"manifest.json").is_file():
            raise ValueError("run dataset is unavailable; use arbor resume RUN --dataset PATH to relocate it")
        if sha(candidate/"manifest.json") != expected:
            raise ValueError("relocated dataset does not match this run's original manifest")
    elif dataset is not None and not (candidate/"manifest.json").is_file():
        raise ValueError("relocated dataset has no manifest.json")
    value = relative_path(directory, candidate)
    plan["config"]["dataset"] = value
    for case in plan["cases"]:
        case["dataset"] = value
    save(directory/"plan.json", plan)
    return candidate


def finish(target, case):
    retain_and_cleanup(target, case["keep_db"])
    save(target/"complete.json", dict(status="PASS", finished_unix=time.time()))


def prepare_case(directory, case):
    command = [sys.executable, "-m", "arbor.runtime.queue", "prepare", str(directory), case["id"]]
    with (directory/"prepare.log").open("ab") as log:
        process = subprocess.Popen(command, env=clean_environment(), stdout=log, stderr=subprocess.STDOUT,
                                   stdin=subprocess.DEVNULL, start_new_session=True)
        save(directory/"preparation_process.json", dict(pid=process.pid, birth_ticks=birth(process.pid)))
        try:
            while process.poll() is None:
                check_resources(directory, case["resources"], dataset=case["dataset"])
                if tree_rss(process.pid) > case["resources"]["offline_rss_gib"]*2**30:
                    raise RuntimeError("offline preparation exceeded its RSS allowance")
                time.sleep(1)
            if process.returncode:
                raise RuntimeError("layout preparation failed; see prepare.log")
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
    return str(resolve_run_path(directory, load(directory/"prepared"/(case["id"]+".json"))["layout"]))


def run(directory):
    directory = Path(directory).resolve()
    plan = load(directory/"plan.json")
    with RunLock(directory):
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        controlled_stop = False
        def control_stop(*_):
            nonlocal controlled_stop
            controlled_stop = True
            raise KeyboardInterrupt()
        signal.signal(signal.SIGUSR1, control_stop)
        save(directory/"controller.json", dict(pid=os.getpid(), birth_ticks=birth(os.getpid()), started_unix=time.time()))
        state = dict(status="RUNNING", completed=len(summarize(directory)), total=len(plan["cases"]))
        def progress(stage, **fields):
            state.update(stage=stage, updated_unix=time.time(), **fields)
            save(directory/"status.json", state)
        try:
            initial_control = load(directory/"control.json") if (directory/"control.json").exists() else {}
            if initial_control.get("request") in ("pause", "stop"):
                label = "PAUSED" if initial_control["request"] == "pause" else "STOPPED"
                progress(label, status=label)
                return
            progress("BUILD")
            built = build_snapshots(directory)
            dataset = dataset_location(directory, plan)
            if plan["recipe"] == "demo" and not dataset.exists():
                raw = make_demo(root()/"data/demo_raw")
                ingest(raw/"history.csv", raw/"test.csv", dataset)
            manifest = verify(dataset)
            receipt = verification_receipt(dataset, manifest)
            receipt["directory"] = relative_path(directory, dataset)
            save(directory/"dataset_identity.json", receipt)
            models = model_snapshots(directory, plan["config"])
            for original in plan["cases"]:
                case = dict(original, dataset=str(dataset), **models)
                control = load(directory/"control.json") if (directory/"control.json").exists() else {}
                if control.get("request") in ("pause", "stop"):
                    progress("PAUSED" if control["request"] == "pause" else "STOPPED",
                             status="PAUSED" if control["request"] == "pause" else "STOPPED")
                    return
                target = directory/"cases"/case["id"]
                if (target/"complete.json").exists():
                    continue
                if target.exists():
                    if (target/"performance.json").exists() and (target/"process_exit.json").exists():
                        progress("FINALIZING", current=case["id"])
                        finish(target, case)
                        state["completed"] = len(summarize(directory))
                        continue
                    raise RuntimeError(f"case {case['id']} has an interrupted execution; inspect its logs before starting a new run")
                progress("ADMISSION", current=case["id"], method=case["method"], shards=case["shards"],
                         inject_tps=case["inject_tps"], accepted_transactions=0, completed_transactions=0)
                check_resources(directory, case["resources"], admission=True, dataset=dataset)
                progress("LAYOUT")
                layout = prepare_case(directory, case)
                target.mkdir(parents=True)
                save(target/"configuration.json", case)
                execute(target, case, dataset, layout, progress, build=built)
                progress("RETAINING", completed_transactions=load(target/"performance.json")["completed"])
                finish(target, case)
                state["completed"] = len(summarize(directory))
            progress("COMPLETE", status="COMPLETE", current=None)
            save(directory/"complete.json", dict(status="PASS", completed=state["completed"], finished_unix=time.time()))
        except BaseException as error:
            if controlled_stop:
                progress("STOPPED", status="STOPPED", error="Stopped by user; interrupted execution retained")
                return
            failure = dict(error=repr(error), traceback=traceback.format_exc(), current=state.get("current"), unix=time.time())
            save(directory/"failure.json", failure)
            progress("STOPPED", status="FAILED", error=repr(error))
            raise


def launch(directory, dataset=None):
    directory = Path(directory).resolve()
    # Never start a second queue while its previous controller/children are live.
    with RunLock(directory):
        for path in (directory/"cases").glob("*/owned_processes.json") if (directory/"cases").exists() else []:
            if (path.parent/"complete.json").exists():
                continue
            for item in load(path):
                if alive(item):
                    raise RuntimeError("an incomplete case still owns live processes")
        plan = load(directory/"plan.json")
        dataset_location(directory, plan, dataset)
        save(directory/"control.json", dict(request=None))
        save(directory/"status.json", dict(status="STARTING", stage="STARTING"))
    log = (directory/"controller.log").open("ab")
    try:
        process = subprocess.Popen([sys.executable, "-B", "-m", "arbor.runtime.queue", "run", str(directory)],
                                   env=clean_environment(), stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
    finally:
        log.close()
    return process.pid


def control(directory, request):
    directory = Path(directory).resolve()
    with file_lock(directory/"control.lock"):
        save(directory/"control.json", dict(request=request, unix=time.time()))
        identity_path = directory/"controller.json"
        active = False
        if identity_path.exists():
            identity = load(identity_path)
            active = alive(identity)
            if active and request == "stop":
                try:
                    os.kill(identity["pid"], signal.SIGUSR1)
                except ProcessLookupError:
                    active = False
        if request == "stop" and not active:
            records = []
            if (directory/"preparation_process.json").exists():
                records.append(load(directory/"preparation_process.json"))
            for path in (directory/"cases").glob("*/owned_processes.json"):
                records.extend(load(path))
            for row in records:
                if alive(row):
                    try:
                        if os.getpgid(row["pid"]) == row["pid"]:
                            os.killpg(row["pid"], signal.SIGTERM)
                    except ProcessLookupError:
                        pass
            if (directory/"status.json").exists():
                state = load(directory/"status.json")
                if state["status"] != "COMPLETE":
                    save(directory/"status.json", dict(state, status="STOPPED", stage="STOPPED"))
        return dict(request=request, active_controller=active,
                    message="Current case finishes before pause." if request == "pause" else "Stop requested; results and logs are retained.")


def status(directory):
    directory = Path(directory)
    state = load(directory/"status.json")
    identity_path = directory/"controller.json"
    if state["status"] == "RUNNING" and identity_path.exists():
        identity = load(identity_path)
        if not alive(identity):
            state = dict(state, status="INTERRUPTED", error="controller exited; inspect the last case before resume")
    return state


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=("run", "prepare")); p.add_argument("directory"); p.add_argument("case", nargs="?")
    a = p.parse_args()
    if a.action == "run":
        run(a.directory)
    else:
        directory = Path(a.directory)
        plan = load(directory/"plan.json")
        config = dict(next(c for c in plan["cases"] if c["id"] == a.case),
                      **{k: str(resolve_run_path(directory, v["path"])) for k, v in load(directory/"models.json").items()})
        dataset = resolve_run_path(directory, plan["config"]["dataset"])
        config["dataset"] = str(dataset)
        receipt = load(directory/"dataset_identity.json")
        receipt["directory"] = str(dataset)
        layout = build_layout(dataset, config, receipt=receipt, build=load_build(directory))
        save(directory/"prepared"/(a.case+".json"), dict(layout=relative_path(directory, layout)))
