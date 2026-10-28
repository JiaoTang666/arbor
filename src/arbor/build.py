"""Build the two native components, caching by source and compiler identity."""
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import urllib.request

from .io import fingerprint, load, root, save, sha
from .cache import file_lock


def go_tool():
    candidates = [os.environ.get("GO"), shutil.which("go"), str(root()/".tools/go/bin/go"),
                  "/usr/local/go/bin/go"]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    if os.uname().machine != "x86_64":
        raise RuntimeError("Install Go >=1.22 and set GO to its executable on this architecture")
    tools = root()/".tools"
    tools.mkdir(exist_ok=True)
    archive = tools/"go1.22.0.linux-amd64.tar.gz"
    url = "https://go.dev/dl/" + archive.name
    expected = urllib.request.urlopen(url + ".sha256", timeout=60).read().decode().strip()
    urllib.request.urlretrieve(url, archive)
    if sha(archive) != expected:
        raise ValueError("Go download checksum mismatch")
    with tarfile.open(archive) as tar:
        for member in tar.getmembers():
            if member.issym() or member.islnk() or not (tools/member.name).resolve().is_relative_to(tools.resolve()):
                raise ValueError("unsafe toolchain archive")
        tar.extractall(tools)
    archive.unlink()
    return str(tools/"go/bin/go")


def ensure_build():
    with file_lock(root()/".build/build.lock"):
        return _ensure_build()


def _ensure_build():
    project = root()
    output = project/".build"
    output.mkdir(exist_ok=True)
    go = go_tool()
    compiler = shutil.which("g++")
    if not compiler:
        raise RuntimeError("g++ is missing: sudo apt-get install g++")
    sources = list((project/"native/engine").rglob("*.go"))
    sources += [project/"native/engine/go.mod", project/"native/engine/go.sum", project/"native/layout/kernel.cpp"]
    identity = dict(files={str(p.relative_to(project)): sha(p) for p in sorted(sources)},
                    go=subprocess.check_output([go, "version"], text=True).strip(),
                    cxx=subprocess.check_output([compiler, "-dumpfullversion"], text=True).strip())
    key = fingerprint(identity)
    binary, library = output/"arbor-engine", output/"layout.so"
    clpa = output/"arbor-clpa-layout"
    receipt = output/"build.json"
    if receipt.exists() and binary.exists() and library.exists() and clpa.exists():
        old = load(receipt)
        if (old["source_key"] == key and old["binary_sha256"] == sha(binary)
                and old["library_sha256"] == sha(library) and old.get("clpa_sha256") == sha(clpa)):
            return binary, library
    env = dict(os.environ, GOWORK="off")
    subprocess.run([go, "build", "-p", "2", "-o", str(binary), "."],
                   cwd=project/"native/engine", env=env, check=True)
    subprocess.run([go, "build", "-p", "2", "-o", str(clpa), "./cmd/arbor-layout"],
                   cwd=project/"native/engine", env=env, check=True)
    subprocess.run([compiler, "-O3", "-std=c++17", "-shared", "-fPIC",
                    str(project/"native/layout/kernel.cpp"), "-o", str(library)], check=True)
    save(receipt, dict(source_key=key, **identity, binary_sha256=sha(binary),
                       library_sha256=sha(library), clpa_sha256=sha(clpa)))
    return binary, library


if __name__ == "__main__":
    print(*ensure_build(), sep="\n")
