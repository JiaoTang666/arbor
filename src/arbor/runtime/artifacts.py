"""Run-local build snapshots and relocatable artifact paths."""
import os
from pathlib import Path
import shutil
import tempfile

from arbor.io import load, resolve_path, save, sha


BUILD_FILES = dict(binary="arbor-engine", library="layout.so", clpa="arbor-clpa-layout")


def resolve_run_path(directory, value):
    return resolve_path(value, base=directory)


def relative_path(directory, value):
    return os.path.relpath(Path(value).resolve(), Path(directory).resolve())


def load_build(directory):
    """Resolve a pinned build without consulting the checkout's mutable build cache."""
    directory = Path(directory)
    record = load(directory/"build.json")
    paths = []
    for name in BUILD_FILES:
        path = resolve_run_path(directory, record[name])
        if not path.is_file() or sha(path) != record[name+"_sha256"]:
            raise ValueError("run build artifact changed or is missing: "+name)
        paths.append(path)
    return tuple(paths)


def _previous_engine(directory, record):
    identities = set()
    for path in (directory/"cases").glob("*/performance.json"):
        row = load(path)
        if row.get("status") == "PASS":
            identity = row.get("binary_sha256") or record.get("binary_sha256")
            if not identity:
                raise ValueError("existing successful results have no execution-build identity")
            identities.add(identity)
    if len(identities) > 1:
        raise ValueError("existing results contain different execution builds; use separate run directories")
    expected = next(iter(identities), record.get("binary_sha256"))
    if expected and record.get("binary_sha256", expected) != expected:
        raise ValueError("run build identity differs from its successful results")
    return expected


def build_snapshot(directory, factory):
    """Pin all three native artifacts once, including identity-checked legacy migration."""
    directory = Path(directory)
    marker = directory/"build.json"
    record = load(marker) if marker.exists() else {}
    expected = _previous_engine(directory, record)
    if record.get("schema") == "arbor_run_build_v1":
        return load_build(directory)

    sources = None
    if record.get("binary") and record.get("library"):
        binary = resolve_run_path(directory, record["binary"])
        library = resolve_run_path(directory, record["library"])
        clpa = resolve_run_path(directory, record["clpa"]) if record.get("clpa") else binary.parent/BUILD_FILES["clpa"]
        if all(p.is_file() for p in (binary, library, clpa)):
            if ((not expected or sha(binary) == expected)
                    and (not record.get("library_sha256") or sha(library) == record["library_sha256"])
                    and (not record.get("clpa_sha256") or sha(clpa) == record["clpa_sha256"])):
                sources = binary, library, clpa
    if sources is None:
        built = factory()
        sources = tuple(map(Path, built))
        if len(sources) == 2:
            sources += (sources[0].parent/BUILD_FILES["clpa"],)
    if len(sources) != 3 or not all(p.is_file() for p in sources):
        raise ValueError("native build requires an engine, layout library and CLPA layout executable")
    digests = {name: sha(path) for name, path in zip(BUILD_FILES, sources)}
    if expected and digests["binary"] != expected:
        raise ValueError("original execution build is unavailable; the current build differs from this run. "
                         "Restore the original build or start a new run directory.")
    for name in ("library", "clpa"):
        if record.get(name+"_sha256") and digests[name] != record[name+"_sha256"]:
            raise ValueError("original run build artifact is unavailable: "+name)

    destination = directory/"build"
    destination.mkdir(exist_ok=True)
    pinned = dict(schema="arbor_run_build_v1")
    for (name, filename), source in zip(BUILD_FILES.items(), sources):
        target = destination/filename
        if target.exists():
            if sha(target) != digests[name]:
                raise ValueError("run build artifact differs from its original snapshot: "+name)
        else:
            fd, temporary = tempfile.mkstemp(prefix="."+filename+"-", dir=destination)
            os.close(fd)
            try:
                shutil.copy2(source, temporary)
                with open(temporary, "rb") as stream:
                    os.fsync(stream.fileno())
                if sha(temporary) != digests[name]:
                    raise ValueError("native build changed while creating the run snapshot")
                os.replace(temporary, target)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        pinned[name] = relative_path(directory, target)
        pinned[name+"_sha256"] = digests[name]
    save(marker, pinned)
    return load_build(directory)
