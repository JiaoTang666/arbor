# Run management

## Inspect, pause and stop

`arbor status <run>` displays progress. `arbor pause <run>` completes the active case, saves its results and stops admission at the next case boundary. `arbor resume <run>` continues the remaining plan. `arbor stop <run>` stops the controller and its owned processes, preserving the interrupted case. Stop is immediate; pause is appropriate when the current measurement should finish.

Ctrl+C closes the display while the queue continues. Use `--detach` to launch directly into the background. Each run directory permits one controller; process ownership includes PID and birth identity.

## Recovery and retention

Successful native execution records normal exits and complete-transaction metrics before finalization. If saving or cleanup is interrupted, `resume` verifies existing results and finishes that stage without executing transactions again. Already completed cases are skipped. A case interrupted during native execution is retained; start a new run to retry it.

The default `--keep-db` retains native databases and logs. `--cleanup-db` removes them after successful completion and verified compact result storage. Compact packages reside in `cases/<case>/bundle` and `retained/<case>`. Cleanup is restricted to the case's own directory and rejects links to shared files.

## Data and cache reuse

The queue hashes dataset members once at startup. Later cases check the same manifest and filesystem identities. Changing input files during a queue stops further processing. Prepared graphs and layouts use shared locks, temporary builds and atomic publication. Their cache keys include producing code, parameters, models and native kernels. Incomplete or incompatible cache directories are archived next to the rebuilt cache as `.obsolete-*` directories.

Selected models are copied into `runs/<run>/models/` and their hashes recorded in `models.json`, fixing the models used by later cases and resumed runs.

## Resources

```bash
./arbor doctor
./arbor run full --config configs/small.yaml --dataset data/demo
```

The small preset uses 4 shards, four nodes per shard, 120 synthetic transactions and 2 GiB available-memory/disk admission settings. The paper profile reserves 300 GiB available RAM, 70 GiB data disk and 100 GiB system disk. Configure limits for your workload under `resources`:

```yaml
resources:
  min_available_gib: 32
  min_data_free_gib: 30
  min_system_free_gib: 10
  max_owned_rss_gib: 48
  runtime_available_floor_gib: 2
```

Unspecified fields retain their recipe values. Runtime monitoring samples process RSS and available memory/disk every five seconds. Preparation uses a separate RSS allowance. `peak_sampled_owned_rss_gib` reports the maximum observed sum of the run's process RSS.

The five-arm, 120-transaction example was verified on Ubuntu 24.04 with Python 3.12 and Go 1.22.0. Its largest sampled owned-process RSS sum was **0.47 GiB**, using 16 native nodes plus the supervisor and observers. The corresponding admission preset requires 2 GiB available RAM. [Verification records](../validation/review_acceptance.json).

The launcher removes inherited Go resource and database table-size overrides. Cache/handles/trie settings are 128 MiB / 256 / 64 MiB. Effective configuration, environment and build/model/input hashes accompany each result.

## Outputs

```text
runs/<run-id>/
  plan.json, status.json         # plan and progress
  models.json, models/           # selected model snapshots
  dataset_identity.json         # startup verification
  summary.json, summary.csv      # completed cases
  cases/<case>/
    configuration.json, launch_environment.json
    performance.json, process_exit.json, cleanup.json
    bundle/                     # compact results
    native/                     # database and logs when retained
  retained/<case>/              # second compact copy
  figures/                     # results --plot
```

## Troubleshooting

- **Installation:** use Python 3.12 (`PYTHON=python3.12 ./install.sh`); install `g++` and `python3.12-venv` on Ubuntu.
- **Go downloads:** set your network's `GOPROXY`, for example `GOPROXY=https://goproxy.cn,direct ./install.sh`.
- **Data preparation:** use `arbor data paper` for official XBlock links, then `arbor data xblock --source ...` to prepare downloaded CSV/ZIP inputs.
- **Resource admission:** inspect `doctor`, dataset size and the YAML resource fields.
- **Failed execution:** inspect `controller.log` and the case's `native/` logs. Failed cases remain available for analysis.
