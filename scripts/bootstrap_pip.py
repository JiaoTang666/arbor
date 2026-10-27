"""Install a pinned pip wheel into the active venv, without requiring system ensurepip."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.request

if sys.prefix == sys.base_prefix:
    raise RuntimeError("bootstrap must run inside the repository virtual environment")
with urllib.request.urlopen("https://pypi.org/pypi/pip/25.0.1/json",timeout=60) as response:
    release=json.load(response)
asset=next(x for x in release["urls"] if x["filename"].endswith(".whl"))
with tempfile.TemporaryDirectory() as folder:
    wheel=Path(folder)/asset["filename"]
    with urllib.request.urlopen(asset["url"],timeout=60) as response:
        payload=response.read()
    if hashlib.sha256(payload).hexdigest()!=asset["digests"]["sha256"]:
        raise ValueError("pip wheel checksum mismatch")
    wheel.write_bytes(payload)
    code="import sys,runpy;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('pip',run_name='__main__')"
    subprocess.run([sys.executable,"-c",code,str(wheel),"install","--no-deps",str(wheel)],check=True)
