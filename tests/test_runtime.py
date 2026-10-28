import json
import os
from pathlib import Path
import socket

import numpy as np
import pytest

from arbor.config import cases, resolve
from arbor.io import load, save
from arbor.runtime.metrics import collect
from arbor.runtime.resources import RunLock, check_ports, port_table
from arbor.runtime.worker import REMOVED_ENV, clean_environment
from arbor.runtime.queue import retain_and_cleanup


def test_matrix_and_environment(monkeypatch):
    cells = cases(resolve("scale"))
    assert len(cells) == 36
    assert len({(c["method"], c["shards"], c["inject_tps"]) for c in cells}) == 36
    assert {c["inject_tps"] for c in cells if c["shards"] == 12} == {1200,1800,2400}
    for key in REMOVED_ENV:
        monkeypatch.setenv(key, "bad")
    monkeypatch.setenv("SAA_FOREST_FACTS_V2", "bad")
    env = clean_environment()
    assert not any(key in env for key in REMOVED_ENV)
    assert "SAA_FOREST_FACTS_V2" not in env
    assert resolve("full")["keep_db"] is True


def test_lock_and_live_listener_are_rejected(tmp_path):
    with RunLock(tmp_path):
        with pytest.raises(RuntimeError, match="active controller"):
            with RunLock(tmp_path):
                pass
    with socket.socket() as listener:
        listener.bind(("127.0.0.1",0)); listener.listen()
        with pytest.raises(OSError):
            check_ports({"0":{"0":f"127.0.0.1:{listener.getsockname()[1]}"}})


def test_listener_ports_do_not_compete_with_outbound_ephemeral_pool():
    low,high=map(int,Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split())
    table=port_table(16,4)
    ports=[int(a.rsplit(":",1)[1]) for row in table.values() for a in row.values()]
    assert len(ports)==65 and len(set(ports))==65
    assert all(not low <= port <= high for port in ports)


def test_full_latency_and_stage_conservation(tmp_path):
    case = dict(method="Arbor", shards=2, inject_tps=2, batch_size=1, block_size=100)
    rows = [dict(event="route_bound", ingress_sequence=0, native_elapsed_ns=0),
            dict(event="route_bound", ingress_sequence=1, native_elapsed_ns=500000000)]
    for i, leg, shard, at in [(0,"inner",0,1000000000),(1,"relay1",0,2000000000),(1,"relay2",1,3000000000)]:
        rows.append(dict(event="leg_registered", ingress_sequence=i, details=dict(leg=leg,shard=shard),native_elapsed_ns=at))
    path = tmp_path/"offered.csv.lifecycle.v1.jsonl"
    path.write_text("".join(json.dumps(r)+"\n" for r in rows))
    result = collect(tmp_path, case, 2)
    assert result["completed"] == 2 and result["total_stages"] == 3
    assert result["end_to_end_seconds"]["mean"] == 1.75
    assert result["complete_tps"] == pytest.approx(2/3)
    with path.open("a") as f:
        f.write(json.dumps(rows[-1])+"\n")
    with pytest.raises(ValueError, match="duplicate stage"):
        collect(tmp_path,case,2)


def target_fixture(tmp_path):
    target = tmp_path/"cases/one"
    (target/"native/runtime").mkdir(parents=True)
    (target/"native/runtime/db").write_bytes(b"database")
    save(target/"performance.json", dict(status="PASS"))
    save(target/"process_exit.json", dict(status="PASS"))
    save(target/"configuration.json", {})
    save(target/"launch_environment.json", {})
    save(target/"owned_processes.json", [])
    return target


def test_cleanup_is_explicit_and_scoped(tmp_path):
    target = target_fixture(tmp_path)
    retain_and_cleanup(target, True)
    retain_and_cleanup(target, True)
    assert (target/"native/runtime/db").exists()
    other = target_fixture(tmp_path/"second")
    retain_and_cleanup(other, False)
    retain_and_cleanup(other, False)
    assert not (other/"native").exists()
    assert load(other/"cleanup.json")["removed_file_bytes"] == 8
    assert (other.parents[1]/"retained/one/performance.json").exists()


def test_cleanup_refuses_link_to_external_data(tmp_path):
    target = target_fixture(tmp_path)
    outside = tmp_path/"original.db"; outside.write_text("keep")
    (target/"native/external").symlink_to(outside)
    with pytest.raises(ValueError, match="symlink"):
        retain_and_cleanup(target, False)
    assert outside.read_text() == "keep"
