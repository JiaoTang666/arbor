# Arbor

**Temporal and source-aware account placement for sharded blockchains.**

Arbor combines a temporal account graph, an entry forest and incremental admission to place interacting accounts while accounting for their associated demand. This repository includes the native execution engine, frozen models, data preparation, training, experiment recipes and result tools.

[中文说明](README.zh-CN.md) · [Architecture](docs/architecture.md) · [Experiments](docs/experiments.md) · [Data and models](docs/data-models.md)

## Quick start

Linux or WSL2, Ubuntu 24.04 recommended; Python **3.12**, a C++17 compiler, and Go **1.22 or later**. If Go is absent on Linux x86_64, the installer downloads the pinned Go 1.22.0 toolchain into `.tools/`. Nothing is installed globally.

During the private preview, obtain repository access and run `gh auth login` followed by `gh auth setup-git` before cloning. Install [GitHub CLI](https://cli.github.com/) if it is not already available.

```bash
sudo apt-get update
sudo apt-get install -y python3.12 python3.12-venv g++ git
git clone https://github.com/JiaoTang666/arbor.git
cd arbor
./install.sh
./arbor run demo
```

The demo generates a deterministic public trace, constructs the history graph, entry forest and layouts, then executes **Arbor, Monoxide and dynamic CLPA** on real local PBFT nodes. Each method completes 120 transactions. It exercises known-account, partner-following and joint-admission paths. This is a functional example, not a paper performance benchmark.

The terminal shows the active case, phase, accepted/completed counts and elapsed time, followed by throughput and latency. Detailed native logs stay in the run directory. Pressing Ctrl+C detaches the display; the server-side queue continues. No Windows client or browser connection is required.

## Reproduce the paper experiments

Download and verify the paper input once, then choose a recipe:

```bash
./arbor data paper
./arbor run full
./arbor run ablation --cleanup-db
./arbor run halflife --cleanup-db
./arbor run group-size --cleanup-db
./arbor run tolerance --cleanup-db
./arbor run scale --cleanup-db
```

The initial repository and data release are private. Collaborators must have repository access and authenticate using the standard GitHub CLI flow (`gh auth login`) before `data paper`; the downloader never reads browser credentials. The release manifest verifies both the archive and every extracted member. Large data, databases and logs are excluded from Git.

| Recipe | Measurements | Configuration |
|---|---:|---|
| `full` | 1 | Arbor, 16 shards, 2,000 input tx/s |
| `ablation` | 5 | Arbor and four component removals |
| `halflife` | 27 | 3×3×3 historical half-life grid, seed-43 order |
| `group-size` | 3 | Group limit 16 / 32 / 64 |
| `tolerance` | 3 | Burden tolerance 1.05 / 1.10 / 1.20 |
| `scale` | 36 | Three methods × 4/8/12/16 shards × 50/75/100% load |

Paper defaults are historical half-lives **6 h / 48 h / 3.5 d**, group limit **64**, and tolerance **1.10**. The online kernel remains **6 h / 24 h / 7 d**. Paper runs use four nodes per shard, block/batch capacity 1,000 and a 5 s interval. Nominal 100% offered load is `200 × shards` original tx/s. All methods use the same freshly built engine. See [exact recipe semantics and reference results](docs/experiments.md).

## Configure and inspect

```bash
./arbor run full --method Monoxide --shards 8 --rate 1200 --dry-run
./arbor run full --config my-run.yaml --detach
./arbor doctor
./arbor status runs/<run-id>
./arbor resume runs/<run-id>
./arbor results runs/<run-id> --plot
```

Precedence is **CLI → YAML → defaults**. `--dry-run` prints all cases and effective settings without preparing data or starting nodes. A matrix recipe defines its own method/topology grid; use `full` for an individual overridden configuration. `--json` and `--no-color` work before or after a subcommand. Redirected output is plain text.

**Databases are kept by default.** `--keep-db` and `--cleanup-db` are mutually exclusive. Explicit cleanup runs only after successful completion, normal process exits and two verified local result copies. It removes that case's database and large native logs, never source inputs, shared layouts or failed runs. The two copies reside on the same server.

Each run has one controller lock. Resume skips completed cases and starts unattempted cases; it refuses to repeat a failed or interrupted native case. Inspect its failure and create an explicitly new run when a retry is intended. An active controller or surviving owned process blocks a duplicate launch.

## Outputs

```text
runs/<run-id>/
  plan.json                 # resolved recipe and cases
  status.json               # live progress; controller.log has errors
  summary.json, summary.csv # completed measurements only
  cases/<case>/
    configuration.json, launch_environment.json
    performance.json, process_exit.json, cleanup.json
    bundle/                 # compact verified results
    native/                 # database and logs, unless cleanup requested
  retained/<case>/           # second verified result copy
  figures/                  # PDF, PNG and grayscale preview after --plot
```

Complete latency starts at the planned release time and ends when all required transaction stages have completed. Throughput includes injection and drain time. Each accepted measurement requires every input to complete, no unfinished transactions, and `stages = transactions + cross-shard transactions`. P95/P99, cross-shard fraction, drain time and per-shard work are preserved. Plots show measured points; one-run configurations have no invented error bars.

## Data preparation and training

Experiments use the supplied frozen fusion and admission models. Training is always explicit:

```bash
./arbor train fusion --small
./arbor train admission --small
./arbor train fusion --dataset data/paper
./arbor train admission --dataset data/paper
```

Small training fixtures validate chronological sample generation, fitting and export. Full training uses historical prefixes and held-out historical continuations, not the paper test input. Outputs go to new run directories and do not overwrite frozen models. [Data formats, raw XBlock processing and model provenance](docs/data-models.md) explain the complete path.

## Resources and troubleshooting

| Profile | Admission settings | Scope |
|---|---|---|
| Demo | 2 GiB available RAM; 2 GiB free on data/system filesystems | Small functional verification |
| Paper | 300 GiB available RAM; 70 GiB data / 100 GiB system reserve | Original large-run resource profile |
| Offline preparation | 8 GiB process RSS guard | Graph, forest and layout preparation |

These are configured admission limits, not memory measurements or promises of minimum hardware. Runtime checks run every five seconds and manage only Arbor's own children. Resource overrides belong in YAML under `resources`; smaller datasets can use smaller profiles. Running unrelated heavy jobs can change performance.

The launcher clears inherited `GOMAXPROCS`, `GOMEMLIMIT` and `BLOCK_EMULATOR_STATE_DB_TABLE_MB`. Database cache/handles/trie settings remain 128 MiB / 256 / 64 MiB. The effective launch environment and build hashes are recorded for each case.

- **Missing venv:** install `python3.12-venv`; set `PYTHON=python3.12` if needed.
- **Go dependency network timeout:** use your network's proxy, or explicitly run `GOPROXY=https://goproxy.cn,direct ./install.sh`. Module checksum validation remains enabled.
- **Private download denied:** check `gh auth status` and repository access.
- **Resource admission failed:** inspect `doctor` and your YAML. No cases are silently downscaled.
- **Port occupied:** stop only a process you own, or start a new run after inspecting the reported conflict.
- **Interrupted queue:** use `status`, inspect `controller.log`, then `resume`; completed cases stay intact.

## Development and licensing

```bash
.venv/bin/pytest -q
.venv/bin/python scripts/check_parity.py
cd native/engine && go test -p 2 ./supervisor/committee ./build ./core ./chain
```

Python modules live in `src/arbor`; Go/C++ kernels in `native`; experiment differences are YAML recipes. [Architecture and validation](docs/architecture.md) describe interfaces and equivalence checks.

Arbor code is MIT licensed. The BlockEmulator MIT notice and dependency notices remain in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Blockchain data retains its source terms and is not relicensed by the code license. Cite this software using [CITATION.cff](CITATION.cff).
