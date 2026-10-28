"""Small public CLI; scientific functions remain independent of terminal rendering."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import time
import uuid

from . import __version__
from .config import RECIPES, cases, resolve
from .io import load, root, save


def parser():
    p = argparse.ArgumentParser(prog="arbor", description="Arbor · reproducible account placement for sharded blockchains")
    p.add_argument("--version", action="version", version=__version__)
    p.add_argument("--json", action="store_true", help="machine-readable output")
    p.add_argument("--no-color", action="store_true", help="disable ANSI colors")
    sub = p.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="prepare, execute and summarize an experiment")
    run.add_argument("recipe", choices=RECIPES)
    run.add_argument("--config", type=Path)
    run.add_argument("--dataset", type=Path)
    run.add_argument("--output", type=Path)
    run.add_argument("--method", choices=("Arbor", "Monoxide", "CLPA"))
    run.add_argument("--shards", type=int)
    run.add_argument("--rate", type=int, dest="inject_tps")
    run.add_argument("--group-size", type=int)
    run.add_argument("--tolerance", type=float)
    run.add_argument("--fusion-model", type=Path)
    run.add_argument("--admission-model", type=Path)
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--detach", action="store_true", help="return after starting the independent queue")
    db = run.add_mutually_exclusive_group()
    db.add_argument("--keep-db", action="store_true", dest="keep_db", default=None)
    db.add_argument("--cleanup-db", action="store_false", dest="keep_db")
    for command in ("status", "resume", "results", "pause", "stop"):
        cmd = sub.add_parser(command)
        cmd.add_argument("directory", type=Path)
        if command == "results":
            cmd.add_argument("--plot", action="store_true")
        elif command == "resume":
            cmd.add_argument("--detach", action="store_true")
    sub.add_parser("doctor", help="check tools, builds and host resources")
    data = sub.add_parser("data", help="find official sources or prepare ordered transaction data")
    data.add_argument("source", choices=("demo", "paper", "prepare", "xblock"))
    data.add_argument("--source", nargs="+", type=Path, dest="archives", help="ordered XBlock CSV/ZIP inputs")
    data.add_argument("--history-count", type=int, default=8840117, help="accepted historical transactions")
    data.add_argument("--test-count", type=int, default=979572, help="accepted test transactions")
    data.add_argument("--history", type=Path)
    data.add_argument("--test", type=Path)
    data.add_argument("--output", type=Path)
    data.add_argument("--history-raw-limit", type=int)
    data.add_argument("--test-raw-limit", type=int)
    train = sub.add_parser("train", help="explicit offline fitting; experiments never train implicitly")
    train.add_argument("model", choices=("fusion", "admission"))
    train.add_argument("--dataset", type=Path, default=root()/"data/paper")
    train.add_argument("--output", type=Path)
    train.add_argument("--small", action="store_true", help="small software-validation fixture")
    # Common formatting options also work after the subcommand.
    for cmd in sub.choices.values():
        cmd.add_argument("--json", action="store_true", default=argparse.SUPPRESS)
        cmd.add_argument("--no-color", action="store_true", default=argparse.SUPPRESS)
    return p


def emit(value, args):
    if args.json:
        print(json.dumps(value, ensure_ascii=False, default=str), flush=True)
        return
    from rich.console import Console
    from rich.table import Table
    console = Console(no_color=args.no_color)
    if isinstance(value, list) and value and "complete_tps" in value[0]:
        table = Table(title="Arbor · completed results", header_style="bold cyan", box=None, pad_edge=False)
        for name in ("Method", "Shards", "Input tx/s", "Complete TPS", "Mean s", "P95 s", "P99 s", "Cross %"):
            table.add_column(name, justify="left" if name == "Method" else "right")
        for row in value:
            latency = row["end_to_end_seconds"]
            table.add_row(row["method"], str(row["shards"]), str(row["inject_tps"]), f"{row['complete_tps']:.2f}",
                          *(f"{latency[k]:.3f}" for k in ("mean", "p95", "p99")), f"{row['cross_fraction']*100:.2f}")
        console.print(table)
    else:
        console.print_json(json.dumps(value, ensure_ascii=False, default=str))


def watch(directory, args):
    from .runtime.queue import status
    from rich.console import Console
    from rich.live import Live
    from rich.panel import Panel
    console = Console(no_color=args.no_color)
    last, launched = None, time.monotonic()
    live = Live(console=console, refresh_per_second=2) if sys.stdout.isatty() and not args.json else None
    if live:
        live.start()
    try:
        while True:
            path = directory/"status.json"
            state = status(directory) if path.exists() else dict(stage="STARTING", status="STARTING")
            if live:
                text = (f"[bold cyan]Arbor[/]  {state.get('completed',0)}/{state.get('total','?')} completed\n"
                        f"{state.get('current') or ''}  ·  {state['stage']}\n"
                        f"Accepted {state.get('accepted_transactions',0)}  ·  Completed {state.get('completed_transactions',0)} / {state.get('expected','?')}\n"
                        f"Elapsed {time.monotonic()-launched:.0f}s  ·  {directory}")
                live.update(Panel(text, border_style="cyan"))
            key = tuple(state.get(k) for k in ("stage", "current", "completed", "accepted_transactions", "completed_transactions"))
            if key != last and not live:
                emit(dict(run=str(directory), **state), args)
                last = key
            if state["status"] in ("COMPLETE", "FAILED", "INTERRUPTED", "PAUSED", "STOPPED"):
                if state["status"] in ("PAUSED", "STOPPED"):
                    emit(dict(run=str(directory), **state), args)
                    return
                if state["status"] != "COMPLETE":
                    raise RuntimeError(state.get("error", "queue failed; see controller.log"))
                break
            if time.monotonic()-launched > 30 and not (directory/"controller.json").exists():
                raise RuntimeError("controller did not start; see controller.log")
            time.sleep(.5)
    except KeyboardInterrupt:
        emit(dict(run=str(directory), status="DETACHED", message="Queue continues; use arbor status to inspect it."), args)
        return
    finally:
        if live:
            live.stop()
    emit(load(directory/"summary.json"), args)
    if not args.json:
        console.print(f"Results: {directory}")


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == "doctor":
            from .runtime.resources import snapshot
            build = load(root()/".build/build.json") if (root()/".build/build.json").exists() else None
            emit(dict(version=__version__, python=sys.version.split()[0], platform=sys.platform,
                      gxx=shutil.which("g++"), go=shutil.which("go"),
                      build={k:build[k] for k in ("go","cxx","source_key","binary_sha256")} if build else None,
                      resources=snapshot(root())), args)
        elif args.command == "run":
            config = resolve(args.recipe, args.config, {k: getattr(args, k) for k in
                             ("dataset", "method", "shards", "inject_tps", "group_size", "tolerance", "keep_db",
                              "fusion_model", "admission_model")})
            plan = dict(schema="arbor_run_v1", recipe=args.recipe, config=config, cases=cases(config))
            if args.dry_run:
                emit(plan, args)
                return
            from .runtime.queue import launch
            label = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")+"_"+args.recipe+"_"+uuid.uuid4().hex[:6]
            directory = (args.output or root()/"runs"/label).resolve()
            directory.mkdir(parents=True, exist_ok=False)
            save(directory/"plan.json", plan)
            pid = launch(directory)
            if args.detach:
                emit(dict(run=str(directory), controller_pid=pid), args)
            else:
                watch(directory, args)
        elif args.command in ("status", "resume", "results", "pause", "stop"):
            directory = args.directory.resolve()
            if args.command in ("pause", "stop"):
                from .runtime.queue import control
                emit(control(directory, args.command), args)
            elif args.command == "status":
                from .runtime.queue import status
                emit(status(directory), args)
            elif args.command == "results":
                if args.plot:
                    from .results import plot
                    plot(directory)
                emit(load(directory/"summary.json"), args)
            else:
                from .runtime.queue import launch
                if (directory/"complete.json").exists():
                    emit(load(directory/"summary.json"), args)
                    return
                pid = launch(directory)
                if args.detach:
                    emit(dict(run=str(directory), controller_pid=pid), args)
                else:
                    time.sleep(1)
                    watch(directory, args)
        elif args.command == "data":
            from .data import ingest, make_demo
            destination = args.output or root()/"data"/args.source
            if args.source == "paper":
                info = load(root()/"configs/paper-source.json")
                command = ("./arbor data xblock --source data/raw/"+info["archive"]+
                           " --history-count 8840117 --test-count 979572 --output data/paper")
                if args.json:
                    emit(dict(catalog=info["catalog"], archive=info["archive"], url=info["archive_url"],
                              prepare_command=command, reproduction="method workflow on a locally prepared window",
                              reference_window=info), args)
                else:
                    from rich.console import Console
                    console = Console(no_color=args.no_color)
                    console.print("[bold cyan]XBlock · Block Transaction[/]")
                    console.print("Catalog: "+info["catalog"])
                    console.print("Archive: "+info["archive_url"])
                    console.print("\nDownload the archive, then prepare a local experiment window:")
                    console.print(command, markup=False)
                    console.print("\nCounts refer to accepted transactions. Paper reference identities are in configs/paper-source.json.")
                return
            elif args.source == "xblock":
                if not args.archives:
                    raise ValueError("data xblock requires --source with ordered CSV/ZIP paths")
                from .xblock import prepare
                destination = args.output or root()/"data/paper"
                prepare(args.archives, destination, args.history_count, args.test_count)
            elif args.source == "demo":
                raw = make_demo(root()/"data/demo_raw")
                ingest(raw/"history.csv", raw/"test.csv", destination)
            else:
                if not args.history or not args.test:
                    raise ValueError("data prepare requires --history and --test")
                ingest(args.history, args.test, destination, history_raw_limit=args.history_raw_limit,
                       test_raw_limit=args.test_raw_limit)
            emit(dict(dataset=str(destination)), args)
        elif args.command == "train":
            from .training.pipeline import train
            destination = args.output or root()/"runs"/("train_"+args.model+"_"+uuid.uuid4().hex[:8])
            emit(train(args.model, args.dataset, destination, small=args.small), args)
    except (ValueError, RuntimeError, OSError, KeyError) as error:
        emit(dict(status="ERROR", message=str(error)), args)
        raise SystemExit(1) from None
