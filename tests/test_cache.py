import multiprocessing
from pathlib import Path
import time

from arbor.cache import cached_directory
from arbor.io import load


def _parallel_build(path):
    def build(staging):
        with (Path(path).parent/"builds").open("a") as stream:
            stream.write("build\n")
        time.sleep(.1)
        (staging/"value").write_text("complete")
    cached_directory(path, {"version": 1}, build)


def test_concurrent_cache_builds_publish_once(tmp_path):
    context = multiprocessing.get_context("fork")
    processes = [context.Process(target=_parallel_build, args=(tmp_path/"cache",)) for _ in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(10)
        assert process.exitcode == 0
    assert (tmp_path/"builds").read_text() == "build\n"


def test_incomplete_corrupt_and_changed_producer_rebuild(tmp_path):
    path = tmp_path/"cache"
    path.mkdir()
    calls = []
    def build(staging):
        calls.append(1)
        (staging/"value").write_text(str(len(calls)))
    cached_directory(path, {"producer": "one"}, build)
    cached_directory(path, {"producer": "one"}, build)
    assert len(calls) == 1
    (path/"value").unlink()
    cached_directory(path, {"producer": "one"}, build)
    assert len(calls) == 2
    cached_directory(path, {"producer": "two"}, build)
    assert len(calls) == 3
    assert load(path/"complete.json")["identity"]["producer"] == "two"


def test_failed_build_does_not_publish(tmp_path):
    import pytest
    def build(staging):
        (staging/"partial").write_text("partial")
        raise RuntimeError("interrupted")
    with pytest.raises(RuntimeError):
        cached_directory(tmp_path/"cache", {"version": 1}, build)
    assert not (tmp_path/"cache").exists()
