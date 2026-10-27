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
from arbor.data import ingest, make_demo, verify
from arbor.history.layout import build_layout
from arbor.io import load, root, save, sha
from .resources import RunLock, birth, check_resources, tree_rss
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
        if birth(identity["pid"]) == identity["birth_ticks"]:
            raise ValueError("owned process is still alive")
    bundle = target/"bundle"
    bundle.mkdir(exist_ok=True)
    names = ("configuration.json", "performance.json", "process_exit.json", "launch_environment.json")
    for name in names:
        shutil.copyfile(target/name, bundle/name)
        with (bundle/name).open("rb") as stream:
            os.fsync(stream.fileno())
    manifest = {name: sha(bundle/name) for name in names}
    save(bundle/"manifest.json", manifest)
    retained = target.parents[1]/"retained"/target.name
    retained.parent.mkdir(exist_ok=True)
    if retained.exists():
        raise ValueError("retained copy already exists")
    shutil.copytree(bundle, retained)
    for name, expected in manifest.items():
        if sha(retained/name) != expected:
            raise ValueError("retained package member hash mismatch")
        with (retained/name).open("rb") as stream:
            os.fsync(stream.fileno())
    for path in (bundle, retained, retained.parent):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    released = 0
    native = target/"native"
    if not keep_db:
        if native.is_symlink() or native.resolve().parent != target.resolve():
            raise ValueError("cleanup path escaped its owned case")
        for directory, dirs, files in os.walk(native, followlinks=False):
            for name in dirs+files:
                path = Path(directory)/name
                if path.is_symlink() or (path.is_file() and path.stat().st_nlink != 1):
                    raise ValueError("cleanup refuses symlink or shared hardlink: "+str(path))
                if path.is_file():
                    released += path.stat().st_size
        shutil.rmtree(native)
    save(target/"cleanup.json", dict(status="PASS", keep_db=keep_db, removed_path=str(native) if not keep_db else None,
                                     removed_file_bytes=released, retained_manifest=manifest))


def run(directory):
    directory = Path(directory).resolve()
    plan = load(directory/"plan.json")
    with RunLock(directory):
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        save(directory/"controller.json", dict(pid=os.getpid(), birth_ticks=birth(os.getpid()), started_unix=time.time()))
        state = dict(status="RUNNING", completed=len(summarize(directory)), total=len(plan["cases"]))
        def progress(stage, **fields):
            state.update(stage=stage, updated_unix=time.time(), **fields)
            save(directory/"status.json", state)
        try:
            progress("BUILD")
            ensure_build()
            dataset = Path(plan["config"]["dataset"])
            if plan["recipe"] == "demo" and not dataset.exists():
                raw = make_demo(root()/"data/demo_raw")
                ingest(raw/"history.csv", raw/"test.csv", dataset)
            verify(dataset)
            for case in plan["cases"]:
                target = directory/"cases"/case["id"]
                if (target/"complete.json").exists():
                    continue
                if target.exists():
                    raise RuntimeError(f"case {case['id']} was already attempted; failure is preserved, not automatically retried")
                progress("ADMISSION", current=case["id"], method=case["method"], shards=case["shards"],
                         inject_tps=case["inject_tps"], accepted_transactions=0, completed_transactions=0)
                check_resources(directory, case["resources"], admission=True)
                progress("LAYOUT")
                # Offline preparation has an RSS guard in a separate process.
                command = [sys.executable, "-m", "arbor.runtime.queue", "prepare", str(directory), case["id"]]
                with (directory/"prepare.log").open("ab") as log:
                    process = subprocess.Popen(command, env=clean_environment(), stdout=log, stderr=subprocess.STDOUT,
                                               stdin=subprocess.DEVNULL, start_new_session=True)
                    try:
                        while process.poll() is None:
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
                                os.killpg(process.pid, signal.SIGKILL); process.wait()
                layout = load(directory/"prepared"/(case["id"]+".json"))["layout"]
                target.mkdir(parents=True)
                save(target/"configuration.json", case)
                execute(target, case, dataset, layout, progress)
                progress("RETAINING", completed_transactions=load(target/"performance.json")["completed"])
                retain_and_cleanup(target, case["keep_db"])
                save(target/"complete.json", dict(status="PASS", finished_unix=time.time()))
                state["completed"] = len(summarize(directory))
            progress("COMPLETE", status="COMPLETE", current=None)
            save(directory/"complete.json", dict(status="PASS", completed=state["completed"], finished_unix=time.time()))
        except BaseException as error:
            failure = dict(error=repr(error), traceback=traceback.format_exc(), current=state.get("current"), unix=time.time())
            save(directory/"failure.json", failure)
            progress("STOPPED", status="FAILED", error=repr(error))
            raise


def launch(directory):
    directory = Path(directory).resolve()
    # Never start a second queue while its previous controller/children are live.
    with RunLock(directory):
        for path in (directory/"cases").glob("*/owned_processes.json") if (directory/"cases").exists() else []:
            if (path.parent/"complete.json").exists():
                continue
            for item in load(path):
                if birth(item["pid"]) == item["birth_ticks"]:
                    raise RuntimeError("an incomplete case still owns live processes")
    log = (directory/"controller.log").open("ab")
    try:
        process = subprocess.Popen([sys.executable, "-B", "-m", "arbor.runtime.queue", "run", str(directory)],
                                   env=clean_environment(), stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
    finally:
        log.close()
    return process.pid


def status(directory):
    directory = Path(directory)
    state = load(directory/"status.json")
    identity_path = directory/"controller.json"
    if state["status"] == "RUNNING" and identity_path.exists():
        identity = load(identity_path)
        if birth(identity["pid"]) != identity["birth_ticks"]:
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
        config = next(c for c in plan["cases"] if c["id"] == a.case)
        layout = build_layout(config["dataset"], config)
        save(directory/"prepared"/(a.case+".json"), dict(layout=str(layout)))
