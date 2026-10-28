"""Locked, versioned caches with atomic publication."""
from contextlib import contextmanager
import os
from pathlib import Path
import tempfile
import uuid

from .io import fingerprint, load, plain, root, save, sha


@contextmanager
def file_lock(path):
    import fcntl
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def signature(path):
    stat = Path(path).stat()
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def producer(*modules):
    return fingerprint({name: sha(root()/name) for name in sorted(modules)})


def cached_directory(output, identity, build):
    output = Path(output)
    identity = plain(identity)
    output.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(output.with_name("."+output.name+".lock")):
        marker = output/"complete.json"
        if marker.is_file():
            try:
                record = load(marker)
            except (ValueError, OSError):
                record = {}
            if record.get("schema") == "arbor_cache_v2" and record.get("identity") == identity:
                files = record.get("files", {})
                valid = bool(files)
                for name, expected in files.items():
                    path = output/name
                    if (not path.resolve().is_relative_to(output.resolve()) or path.is_symlink()
                            or not path.is_file()):
                        valid = False
                        break
                    if signature(path) != expected["stat"] and sha(path) != expected["sha256"]:
                        valid = False
                        break
                if valid:
                    return output
        with tempfile.TemporaryDirectory(prefix="."+output.name+"-", dir=output.parent) as temporary:
            staging = Path(temporary)/"content"
            staging.mkdir()
            details = build(staging) or {}
            files = {str(p.relative_to(staging)): dict(sha256=sha(p), stat=signature(p))
                     for p in sorted(staging.rglob("*")) if p.is_file()}
            save(staging/"complete.json", dict(schema="arbor_cache_v2", identity=identity,
                                              files=files, details=details))
            if output.exists():
                output.rename(output.with_name("."+output.name+".obsolete-"+uuid.uuid4().hex[:8]))
            staging.rename(output)
            fd = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    return output
