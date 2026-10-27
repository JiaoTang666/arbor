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
    # Existing dataset prevents the demo generator from being invoked.
    plan["config"]["dataset"]=str(tmp_path)
    save(tmp_path/"plan.json",plan)
    with pytest.raises(RuntimeError,match="already attempted"):
        queue.run(tmp_path)
    assert load(tmp_path/"summary.json")==[metric]
    assert (failed/"evidence").read_text()=="retain"
    assert not (tmp_path/"cases"/plan["cases"][2]["id"]).exists()


def test_stale_controller_is_not_reported_running(tmp_path):
    save(tmp_path/"status.json",dict(status="RUNNING",stage="NATIVE"))
    save(tmp_path/"controller.json",dict(pid=999999999,birth_ticks=1))
    assert queue.status(tmp_path)["status"]=="INTERRUPTED"
    assert load(tmp_path/"status.json")["status"]=="RUNNING"
