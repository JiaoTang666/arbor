"""Verified dataset installation from HTTPS or authenticated GitHub release assets."""
from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request

from .data import verify
from .io import load, sha


def extract(archive, destination):
    destination = Path(destination).resolve()
    with tarfile.open(archive, "r:gz") as stream:
        for member in stream:
            path = (destination/member.name).resolve()
            if not path.is_relative_to(destination) or not (member.isfile() or member.isdir()):
                raise ValueError("dataset archive contains a link or unsafe path")
            stream.extract(member, destination, filter="data")


def fetch(manifest_path, destination):
    manifest = load(manifest_path)
    destination = Path(destination).resolve()
    if destination.exists():
        result = verify(destination)
        if sha(destination/"manifest.json") != manifest["dataset_manifest_sha256"]:
            raise ValueError("existing dataset differs from the requested release")
        return result
    destination.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(destination.parent).free < manifest["required_free_bytes"]:
        raise RuntimeError("insufficient free disk for downloading and unpacking the dataset")
    cache = destination.parent/".downloads"
    cache.mkdir(exist_ok=True)
    archive = cache/manifest["asset"]
    if not archive.exists() or sha(archive) != manifest["sha256"]:
        temporary = archive.with_suffix(".part")
        if manifest.get("private", False):
            if not shutil.which("gh"):
                raise RuntimeError("This private release requires GitHub CLI: install gh, then gh auth login")
            with tempfile.TemporaryDirectory(dir=cache) as stage:
                subprocess.run(["gh", "release", "download", manifest["tag"], "--repo", manifest["repository"],
                                "--pattern", manifest["asset"], "--dir", stage], check=True)
                shutil.move(str(Path(stage)/manifest["asset"]), temporary)
        else:
            url = manifest["url"]
            if not url.startswith("https://"):
                raise ValueError("dataset downloads require HTTPS")
            with urllib.request.urlopen(url, timeout=120) as source, temporary.open("wb") as target:
                shutil.copyfileobj(source, target, 8 << 20)
                target.flush(); os.fsync(target.fileno())
        if sha(temporary) != manifest["sha256"]:
            raise ValueError("dataset download hash mismatch; partial file retained")
        temporary.replace(archive)
    with tempfile.TemporaryDirectory(prefix=".dataset-", dir=destination.parent) as stage:
        extract(archive, stage)
        if sha(Path(stage)/"manifest.json") != manifest["dataset_manifest_sha256"]:
            raise ValueError("dataset manifest identity mismatch")
        result = verify(stage)
        Path(stage).rename(destination)
    return result
