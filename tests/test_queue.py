from pathlib import Path
import pytest
from arbor.io import load, save
from arbor.config import resolve, cases
from arbor.runtime import queue


def test_resume_skips_completed_and_refuses_failed_case(tmp_path,monkeypatch):
    config=resolve("demo")
    plan=dict(config=config,recipe="demo",cases=cases(config))
    save(tmp_path/"plan.json",plan)
    completed=tmp_path/"cases"/plan["cases"][0]["id"]
    save(completed/"complete.json",dict(status="PASS"))
    metric=dict(id=plan["cases"][0]["id"],method="Arbor",shards=4,inject_tps=40,completed=120,
                complete_tps=10.,cross_fraction=.25,shard_work=[30,40,40,40],
                end_to_end_seconds=dict(mean=1,p95=2,p99=3))
    save(completed/"performance.json",metric)
    failed=tmp_path/"cases"/plan["cases"][1]["id"]
    failed.mkdir()
    (failed/"evidence").write_text("retain")
    monkeypatch.setattr(queue,"ensure_build",lambda:None)
    monkeypatch.setattr(queue,"verify",lambda _:None)
    monkeypatch.setattr(queue,"verification_receipt",lambda *_:{})
    # Existing dataset prevents the demo generator from being invoked.
    plan["config"]["dataset"]=str(tmp_path)
    save(tmp_path/"plan.json",plan)
    with pytest.raises(RuntimeError,match="interrupted execution"):
        queue.run(tmp_path)
    assert load(tmp_path/"summary.json")==[metric]
    assert (failed/"evidence").read_text()=="retain"
    assert not (tmp_path/"cases"/plan["cases"][2]["id"]).exists()


def test_stale_controller_is_not_reported_running(tmp_path):
    save(tmp_path/"status.json",dict(status="RUNNING",stage="NATIVE"))
    save(tmp_path/"controller.json",dict(pid=999999999,birth_ticks=1))
    assert queue.status(tmp_path)["status"]=="INTERRUPTED"
    assert load(tmp_path/"status.json")["status"]=="RUNNING"


def setup_queue(tmp_path, monkeypatch):
    config = resolve("demo", overrides={"dataset": tmp_path, "keep_db": False})
    plan = dict(config=config, recipe="demo", cases=cases(config)[:2])
    save(tmp_path/"plan.json", plan)
    monkeypatch.setattr(queue, "ensure_build", lambda: None)
    monkeypatch.setattr(queue, "verify", lambda *_: {})
    monkeypatch.setattr(queue, "verification_receipt", lambda *_: {})
    monkeypatch.setattr(queue, "check_resources", lambda *_ , **kw: {})
    monkeypatch.setattr(queue, "prepare_case", lambda *_: "layout")
    return plan


def successful_case(target, case):
    save(target/"performance.json", dict(case, status="PASS", completed=120, complete_tps=10.,
         cross_fraction=.25, shard_work=[30,40,40,40], end_to_end_seconds=dict(mean=1,p95=2,p99=3)))
    save(target/"process_exit.json", dict(status="PASS"))
    save(target/"owned_processes.json", [])
    save(target/"launch_environment.json", {})
    save(target/"configuration.json", case)
    (target/"native").mkdir()
    (target/"native/db").write_bytes(b"database")


def test_resume_finalizes_success_without_executing_it_again(tmp_path, monkeypatch):
    plan = setup_queue(tmp_path, monkeypatch)
    first = tmp_path/"cases"/plan["cases"][0]["id"]
    successful_case(first, plan["cases"][0])
    queue.retain_and_cleanup(first, False)
    calls = []
    def execute(target, case, *_ , **kw):
        calls.append(case["id"])
        successful_case(target, case)
    monkeypatch.setattr(queue, "execute", execute)
    queue.run(tmp_path)
    assert calls == [plan["cases"][1]["id"]]
    assert len(load(tmp_path/"summary.json")) == 2
    assert load(first/"cleanup.json")["removed_file_bytes"] == 8


def test_pause_stops_admission_at_boundary_and_resumes(tmp_path, monkeypatch):
    plan = setup_queue(tmp_path, monkeypatch)
    calls = []
    def execute(target, case, *_ , **kw):
        calls.append(case["id"])
        successful_case(target, case)
        save(tmp_path/"control.json", dict(request="pause"))
    monkeypatch.setattr(queue, "execute", execute)
    queue.run(tmp_path)
    assert load(tmp_path/"status.json")["status"] == "PAUSED"
    assert len(calls) == 1
    save(tmp_path/"control.json", dict(request=None))
    queue.run(tmp_path)
    assert calls == [c["id"] for c in plan["cases"]]
    assert load(tmp_path/"status.json")["status"] == "COMPLETE"


def test_stop_does_not_signal_reused_controller_pid(tmp_path, monkeypatch):
    save(tmp_path/"controller.json", dict(pid=999, birth_ticks=10))
    monkeypatch.setattr(queue, "alive", lambda _: False)
    signals = []
    monkeypatch.setattr(queue.os, "kill", lambda *args: signals.append(args))
    assert queue.control(tmp_path, "stop")["active_controller"] is False
    assert signals == []
