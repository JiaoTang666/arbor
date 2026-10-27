# Experiment recipes

All recipes use the common engine and the same ordered input. Arbor is the publication name of the method labeled **Full** in earlier records; **Monoxide** is the public label of the existing hash/relay execution path. Dynamic CLPA includes its actual epoch repartitioning and state transfer. These names do not change the stored reference values.

## Default configuration

| Setting | Value |
|---|---|
| Historical half-lives | 6 h / 48 h / 3.5 d |
| Online relation kernel | 6 h / 24 h / 7 d |
| Group limit / burden tolerance | 64 / 1.10 |
| Grouping / refinement rounds | 6 / 1 |
| Candidate limit / deterministic seed | 4 / 43 |
| Graph pruning | Existing 5% policy |
| Nodes per shard | 4 |
| Paper block and batch capacities | 1,000 / 1,000 |
| Paper block interval | 5 seconds |
| Database cache / handles / trie cache | 128 MiB / 256 / 64 MiB |

`full` runs one configured method. Each matrix cell is measured once. Repetition is explicit: use a new run directory for each repetition. A run never selects the most favorable duplicate.

## Ablations

| Internal arm | Removed component | Retained components |
|---|---|---|
| `Full` | None | Complete Arbor |
| `NoTime` | Historical temporal weighting, replaced by cumulative relations | Same marginal demand and online kernel |
| `NoForestLayout` | Forest-derived historical burden coordinate | Temporal graph and online forest/admission |
| `NoGraphLayout` | Historical graph edges and relation grouping | Marginal account activity, forest demand and online observations |
| `NoAdmission` | Partner-following, joint co-location and pressure/model-based first placement | Full historical layout and online observation; new accounts use independent hash |

`NoLearn` is not part of the published ablation recipe. NoGraph removes historical relation organization, not all online graph instrumentation.

## Sensitivity and scaling

The historical grid is `(3,6,12) h × (12,24,48) h × (3.5,7,14) d`, shuffled once with seed 43. Group-size and tolerance studies change only their named variable. Their reference control is shared where recorded; a new CLI invocation measures its three listed points explicitly.

The scaling matrix runs `K = 4,8,12,16`, then offered loads 50%, 75%, 100%, with the sealed cyclic method order. At 100%, the offered rate is `200K` original transactions per second:

| Shards | 50% | 75% | 100% |
|---:|---:|---:|---:|
| 4 | 400 | 600 | 800 |
| 8 | 800 | 1,200 | 1,600 |
| 12 | 1,200 | 1,800 | 2,400 |
| 16 | 1,600 | 2,400 | 3,200 |

Each scale has its own historical layout, reused across its three offered loads. CLPA retains the upstream Go implementation's map-iteration tie behavior; its initial layout can differ between independent processes. Reusing a cached layout within one recipe keeps load comparisons on that layout.

## Measurement and reference results

Full completion means all required stages are present exactly once and the logical stage count is `N + cross`. Planned release times do not slide with congestion. Complete TPS and mean/P95/P99 latency include queuing, execution, maintenance and drain. Replica work is not multiplied into logical stage counts.

`reference/` contains the published measurements, with original binary/input identities and scope. The 36-cell unified-runtime matrix, historical ablations, half-life study, sensitivity study and triplicate record are separate result sets. The half-life reference table uses the approved isolated reruns for its first two configurations. The separate environment diagnostic is not another candidate. Rebuilt measurements are always written under `runs/`.

Paper-resource defaults intentionally match the large native experiment profile. A custom YAML can run smaller data, for example:

```yaml
dataset: data/demo
shards: 4
inject_tps: 40
batch_size: 20
block_size: 100
interval_ms: 500
resources:
  min_available_gib: 2
  min_data_free_gib: 2
  min_system_free_gib: 2
  max_owned_rss_gib: 8
  runtime_available_floor_gib: 0.5
```

Use `./arbor run ablation --config small.yaml --cleanup-db` for a five-arm software smoke test. This configuration does not represent the paper's workload.
