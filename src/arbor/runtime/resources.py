"""Linux resource checks and process identities; only caller-owned children are managed."""
import fcntl
import json
import os
from pathlib import Path
import shutil
import socket


class RunLock:
    def __init__(self, directory):
        self.path = Path(directory)/"controller.lock"

    def __enter__(self):
        self.stream = self.path.open("a+")
        try:
            fcntl.flock(self.stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.stream.close()
            raise RuntimeError("this run already has an active controller") from None
        self.stream.seek(0); self.stream.truncate()
        json.dump(dict(pid=os.getpid(), birth_ticks=birth(os.getpid())), self.stream)
        self.stream.flush()
        return self

    def __exit__(self, *_):
        self.stream.close()


def birth(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return None if fields[0] == "Z" else int(fields[19])
    except (FileNotFoundError, ProcessLookupError):
        return None


def tree_rss(pid):
    """RSS sum of a preparation process and its descendants (not physical memory)."""
    seen, pending, total = set(), [pid], 0
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        total += process_sample(current)["rss_bytes"]
        try:
            pending.extend(int(x) for x in Path(f"/proc/{current}/task/{current}/children").read_text().split())
        except (FileNotFoundError, ProcessLookupError):
            pass
    return total


def process_sample(pid):
    try:
        status = Path(f"/proc/{pid}/status").read_text().splitlines()
        rss = next((int(s.split()[1])*1024 for s in status if s.startswith("VmRSS:")), 0)
        return dict(pid=pid, birth_ticks=birth(pid), rss_bytes=rss)
    except (FileNotFoundError, ProcessLookupError):
        return dict(pid=pid, birth_ticks=None, rss_bytes=0)


def snapshot(directory):
    mem = {line.split(":")[0]: int(line.split()[1])*1024 for line in Path("/proc/meminfo").read_text().splitlines()}
    return dict(available_gib=mem["MemAvailable"]/2**30,
                data_free_gib=shutil.disk_usage(directory).free/2**30,
                system_free_gib=shutil.disk_usage("/tmp").free/2**30)


def check_resources(directory, limits, children=(), admission=False):
    state = snapshot(directory)
    floor = limits["min_available_gib"] if admission else limits["runtime_available_floor_gib"]
    if state["available_gib"] < floor:
        raise RuntimeError(f"available memory {state['available_gib']:.1f} GiB is below {floor} GiB")
    if state["data_free_gib"] < limits["min_data_free_gib"] or state["system_free_gib"] < limits["min_system_free_gib"]:
        raise RuntimeError("disk free space is below the configured reserve")
    state["owned"] = [process_sample(p.pid) for p in children if p.poll() is None]
    state["owned_rss_gib"] = sum(p["rss_bytes"] for p in state["owned"])/2**30
    if state["owned_rss_gib"] > limits["max_owned_rss_gib"]:
        raise RuntimeError("owned-process RSS limit exceeded")
    return state


def port_table(shards, nodes):
    # Kernel-selected base plus a contiguous probe; Go listeners re-use closed TCP endpoints.
    for _ in range(100):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            base = probe.getsockname()[1]
        if base+shards*nodes >= 65535:
            continue
        table = {str(s): {str(n): f"127.0.0.1:{base+s*nodes+n}" for n in range(nodes)} for s in range(shards)}
        table["2147483647"] = {"0": f"127.0.0.1:{base+shards*nodes}"}
        try:
            check_ports(table)
            return table
        except OSError:
            continue
    raise RuntimeError("no available local port block")


def check_ports(table):
    held = []
    try:
        for shard in table.values():
            for address in shard.values():
                host, port = address.rsplit(":", 1)
                sock = socket.socket()
                held.append(sock)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((host, int(port)))
    finally:
        for sock in held:
            sock.close()
