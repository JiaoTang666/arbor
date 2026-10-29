from pathlib import Path
import shutil
import pytest
from arbor.io import load, save, sha
from arbor.config import resolve, cases
from arbor.runtime import queue
from arbor.runtime.artifacts import build_snapshot, load_build


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
    monkeypatch.setattr(queue,"build_snapshots",lambda _:None)
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
    monkeypatch.setattr(queue, "build_snapshots", lambda _: None)
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


def native_fixture(directory, engine=b"original-engine"):
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, data in (("arbor-engine", engine), ("layout.so", b"library"), ("arbor-clpa-layout", b"clpa")):
        path = directory/name
        path.write_bytes(data)
        path.chmod(0o755)
        paths.append(path)
    return tuple(paths)


def test_run_build_is_pinned_across_source_upgrade_and_directory_move(tmp_path):
    source = native_fixture(tmp_path/"checkout/.build")
    run = tmp_path/"run"; run.mkdir()
    pinned = build_snapshot(run, lambda: source)
    record = load(run/"build.json")
    assert all(not Path(record[key]).is_absolute() for key in ("binary", "library", "clpa"))
    assert pinned[0].read_bytes() == b"original-engine"
    source[0].write_bytes(b"upgraded-engine")
    source[1].write_bytes(b"upgraded-library")
    moved = tmp_path/"moved run"; run.rename(moved)
    def no_rebuild():
        raise AssertionError("resume must not rebuild the current checkout")
    again = build_snapshot(moved, no_rebuild)
    assert again == load_build(moved)
    assert again[0].read_bytes() == b"original-engine"
    assert again[1].read_bytes() == b"library"


def test_legacy_results_reject_a_different_current_engine(tmp_path):
    original = native_fixture(tmp_path/"original")
    run = tmp_path/"run"; run.mkdir()
    old = dict(binary=str(original[0]), library=str(original[1]),
               binary_sha256=sha(original[0]), library_sha256=sha(original[1]))
    save(run/"build.json", old)
    save(run/"cases/one/performance.json", dict(status="PASS", binary_sha256=sha(original[0])))
    shutil.rmtree(original[0].parent)
    current = native_fixture(tmp_path/"checkout/.build", b"new-engine")
    with pytest.raises(ValueError, match="current build differs"):
        build_snapshot(run, lambda: current)
    assert load(run/"build.json") == old


def test_legacy_build_is_migrated_only_when_original_identities_match(tmp_path):
    source = native_fixture(tmp_path/"checkout/.build")
    run = tmp_path/"run"; run.mkdir()
    save(run/"build.json", dict(binary=str(source[0]), library=str(source[1]),
         binary_sha256=sha(source[0]), library_sha256=sha(source[1])))
    save(run/"cases/one/performance.json", dict(status="PASS", binary_sha256=sha(source[0])))
    pinned = build_snapshot(run, lambda: pytest.fail("matching legacy build should not be rebuilt"))
    assert pinned[0] != source[0] and sha(pinned[0]) == sha(source[0])
    assert load(run/"build.json")["schema"] == "arbor_run_build_v1"


def legacy_clpa_fixture(tmp_path):
    source = native_fixture(tmp_path/"checkout/.build")
    run = tmp_path/"run"; run.mkdir()
    save(run/"build.json", dict(binary=str(source[0]), library=str(source[1]),
         binary_sha256=sha(source[0]), library_sha256=sha(source[1])))
    save(run/"plan.json", dict(config=dict(dataset=str(tmp_path/"data")),
         cases=[dict(id="one", method="CLPA"), dict(id="two", method="Arbor")]))
    return run, source


def test_legacy_unfinished_clpa_requires_recorded_helper_identity(tmp_path):
    run, source = legacy_clpa_fixture(tmp_path)
    original = load(run/"build.json")
    # Rebuilding only cmd/arbor-layout leaves the main engine/library unchanged.
    source[2].write_bytes(b"new-clpa-helper")
    save(source[2].parent/"build.json", dict(clpa_sha256=sha(source[2])))
    with pytest.raises(ValueError, match="original CLPA layout identity is unavailable"):
        build_snapshot(run, lambda: source)
    assert load(run/"build.json") == original
    assert not (run/"build").exists()
    # Losing the legacy build marker does not make already measured runs new runs.
    (run/"build.json").unlink()
    save(run/"cases/two/performance.json", dict(status="PASS", binary_sha256=sha(source[0])))
    with pytest.raises(ValueError, match="original CLPA layout identity is unavailable"):
        build_snapshot(run, lambda: source)


def test_legacy_clpa_uses_its_recorded_layout_identity_after_dataset_move(tmp_path):
    run, source = legacy_clpa_fixture(tmp_path)
    dataset = tmp_path/"data"; dataset.mkdir()
    save(dataset/"manifest.json", dict(input="unchanged"))
    manifest_sha = sha(dataset/"manifest.json")
    save(run/"dataset_identity.json", dict(manifest_sha256=manifest_sha))
    layout = dataset/"layouts/exact-recorded-layout"
    save(layout/"complete.json", dict(identity=dict(dataset_sha256=manifest_sha, clpa_sha256=sha(source[2]))))
    save(run/"prepared/one.json", dict(layout="/unavailable/old-data/layouts/exact-recorded-layout"))
    pinned = build_snapshot(run, lambda: pytest.fail("matching legacy artifacts need no rebuild"))
    assert sha(pinned[2]) == sha(source[2])
    assert load(run/"build.json")["clpa_sha256"] == sha(source[2])


def test_legacy_clpa_rejects_changed_helper_with_unchanged_engine_and_library(tmp_path):
    run, source = legacy_clpa_fixture(tmp_path)
    layout = tmp_path/"recorded-layout"
    save(layout/"complete.json", dict(identity=dict(clpa_sha256=sha(source[2]))))
    save(run/"prepared/one.json", dict(layout=str(layout)))
    original = load(run/"build.json")
    source[2].write_bytes(b"new-clpa-helper")
    with pytest.raises(ValueError, match="artifact is unavailable: clpa"):
        build_snapshot(run, lambda: source)
    assert load(run/"build.json") == original
    assert not (run/"build").exists()


@pytest.mark.parametrize("finalizing", [False, True])
def test_legacy_completed_clpa_does_not_require_unused_helper_identity(tmp_path, finalizing):
    run, source = legacy_clpa_fixture(tmp_path)
    if finalizing:
        save(run/"cases/one/performance.json", dict(status="PASS", binary_sha256=sha(source[0])))
        save(run/"cases/one/process_exit.json", dict(status="PASS"))
    else:
        save(run/"cases/one/complete.json", dict(status="PASS"))
    source[2].write_bytes(b"helper-not-used-by-remaining-Arbor")
    assert sha(build_snapshot(run, lambda: source)[0]) == sha(source[0])


def test_resume_uses_original_snapshot_for_remaining_cases(tmp_path, monkeypatch):
    plan = setup_queue(tmp_path, monkeypatch)
    source = native_fixture(tmp_path/"checkout/.build")
    pinned = build_snapshot(tmp_path, lambda: source)
    first = tmp_path/"cases"/plan["cases"][0]["id"]
    successful_case(first, plan["cases"][0])
    first_metric = load(first/"performance.json")
    first_metric["binary_sha256"] = sha(pinned[0])
    save(first/"performance.json", first_metric)
    save(first/"complete.json", dict(status="PASS"))
    source[0].write_bytes(b"new-checkout-engine")
    monkeypatch.setattr(queue, "build_snapshots", lambda directory: build_snapshot(
        directory, lambda: pytest.fail("a paused run must retain its original engine")))
    def execute(target, case, *_, **kwargs):
        assert kwargs["build"][0].read_bytes() == b"original-engine"
        successful_case(target, case)
        metric = load(target/"performance.json")
        metric["binary_sha256"] = sha(kwargs["build"][0])
        save(target/"performance.json", metric)
    monkeypatch.setattr(queue, "execute", execute)
    queue.run(tmp_path)
    assert len({row["binary_sha256"] for row in load(tmp_path/"summary.json")}) == 1


def test_legacy_model_paths_recover_from_run_local_copies_after_move(tmp_path):
    run = tmp_path/"original"; (run/"models").mkdir(parents=True)
    records = {}
    for key in ("fusion", "admission"):
        path = run/"models"/(key+".json"); path.write_text("{}")
        records[key+"_model"] = dict(path=str(path), sha256=sha(path), source="legacy")
    save(run/"models.json", records)
    moved = tmp_path/"moved run"; run.rename(moved)
    models = queue.model_snapshots(moved, {})
    assert all(Path(value).parent == moved/"models" for value in models.values())
    assert all(not Path(row["path"]).is_absolute() for row in load(moved/"models.json").values())
    (moved/"models/fusion.json").write_text('{"changed":true}')
    with pytest.raises(ValueError, match="snapshot changed"):
        queue.model_snapshots(moved, {})


def test_dataset_relocation_preserves_original_manifest_and_relative_paths(tmp_path):
    data = tmp_path/"old data"; data.mkdir(); save(data/"manifest.json", {"input":"same"})
    run = tmp_path/"runs/one"; run.mkdir(parents=True)
    plan = dict(config=dict(dataset=str(data)), cases=[dict(id="one", dataset=str(data))])
    save(run/"plan.json", plan)
    save(run/"dataset_identity.json", dict(directory=str(data), manifest_sha256=sha(data/"manifest.json")))
    moved = tmp_path/"new data"; data.rename(moved)
    assert queue.dataset_location(run, plan, moved) == moved
    stored = load(run/"plan.json")
    assert not Path(stored["config"]["dataset"]).is_absolute()
    assert queue.resolve_run_path(run, stored["cases"][0]["dataset"]) == moved
    bad = tmp_path/"different"; bad.mkdir(); save(bad/"manifest.json", {"input":"different"})
    with pytest.raises(ValueError, match="original manifest"):
        queue.dataset_location(run, stored, bad)
    assert load(run/"plan.json") == stored


def test_dataset_relocation_without_receipt_still_requires_the_original_manifest(tmp_path):
    original = tmp_path/"old"; original.mkdir(); save(original/"manifest.json", {"data":"original"})
    other = tmp_path/"new"; other.mkdir(); save(other/"manifest.json", {"data":"different"})
    run = tmp_path/"run"; run.mkdir()
    plan = dict(config=dict(dataset=str(original)), cases=[])
    save(run/"plan.json", plan)
    with pytest.raises(ValueError, match="original manifest"):
        queue.dataset_location(run, plan, other)
    original.rename(tmp_path/"unavailable")
    with pytest.raises(ValueError, match="original dataset identity is unavailable"):
        queue.dataset_location(run, plan, other)
