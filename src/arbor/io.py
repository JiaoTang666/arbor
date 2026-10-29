"""Small shared filesystem and identity operations."""
import hashlib
import json
import os
from pathlib import Path
import tempfile


def root():
    return Path(__file__).resolve().parents[2]


def resolve_path(value, base=None):
    """Expand a user path, resolving relative values against an explicit base."""
    path = Path(value).expanduser()
    if not path.is_absolute() and base is not None:
        path = Path(base)/path
    return path.resolve()


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def plain(value):
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "tolist"):
        return value.tolist()
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(v) for v in value]
    return value


def canonical(value):
    return json.dumps(plain(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def save(path, value):
    """Atomically replace a small document; callers own immutable-output policy."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(plain(value), stream, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def snapshot_digest(row):
    payload = {k: v for k, v in row.items() if k not in ("snapshot_sha256", "publication_seconds")}
    if "graph_quality" in payload:
        payload["graph_quality"] = {k: v for k, v in payload["graph_quality"].items()
                                    if k not in ("compaction_seconds", "maintenance_seconds")}
    return fingerprint(payload)
