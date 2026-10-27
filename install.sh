#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"
for cmd in "$PYTHON" g++; do
  if ! command -v "$cmd" >/dev/null; then
    printf 'Missing %s. On Ubuntu: sudo apt-get install python3 python3-venv g++\n' "$cmd" >&2
    exit 2
  fi
done
"$PYTHON" -c 'import sys; assert sys.version_info[:2] == (3,12), "Python 3.12 required; set PYTHON=python3.12"'
"$PYTHON" -m venv --without-pip "$ROOT/.venv"
if ! "$ROOT/.venv/bin/python" -m pip --version >/dev/null 2>&1; then
  "$ROOT/.venv/bin/python" "$ROOT/scripts/bootstrap_pip.py"
fi
"$ROOT/.venv/bin/python" -m pip install --disable-pip-version-check -r "$ROOT/requirements.lock"
"$ROOT/.venv/bin/python" -m pip install --disable-pip-version-check --no-deps --no-build-isolation -e "$ROOT"
"$ROOT/.venv/bin/python" -m arbor.build
"$ROOT/.venv/bin/python" -m arbor doctor
printf '\nReady. Run: %s/arbor run demo\n' "$ROOT"
