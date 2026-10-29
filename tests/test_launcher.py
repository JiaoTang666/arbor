"""The repository launcher follows the checkout even with a stale editable install."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from arbor.io import root


@pytest.mark.parametrize("stale_path", [False, True])
def test_launcher_uses_moved_checkout_from_another_directory(tmp_path, stale_path):
    original = tmp_path/"checkout"
    package = original/"src/arbor"
    package.mkdir(parents=True)
    (package/"__init__.py").write_text("")
    (package/"__main__.py").write_text("import json,sys\nprint(json.dumps([__file__,sys.argv[1:]]))\n")
    launcher = original/"arbor"
    shutil.copyfile(root()/"arbor", launcher)
    python = original/".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.symlink_to(sys.executable)
    moved = tmp_path/"moved checkout with spaces"
    original.rename(moved)
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    if stale_path:
        old = tmp_path/"stale/arbor"
        old.mkdir(parents=True)
        (old/"__init__.py").write_text("")
        (old/"__main__.py").write_text("raise RuntimeError('stale checkout imported')\n")
        environment["PYTHONPATH"] = str(old.parent)
    result = subprocess.run(["bash", str(moved/"arbor"), "argument with spaces"], cwd=tmp_path,
                            env=environment, check=True, capture_output=True, text=True)
    source, arguments = json.loads(result.stdout)
    assert Path(source) == moved/"src/arbor/__main__.py"
    assert arguments == ["argument with spaces"]
