# Architecture

Arbor exposes one configuration resolver, one native runner and one complete-transaction metric implementation. Scientific functions receive explicit inputs; they do not invoke the CLI or import archived experiments.

| Module | Inputs → outputs | Responsibility |
|---|---|---|
| `data`, `xblock`, `download` | Ordered transactions → verified dataset | Exact integers, filtering, chronological split, address IDs |
| `history/representation`, `graph` | Historical/accepted prefixes → temporal relations | Offline aggregation and online updates |
| `forest` | First contacts + completed observations → source state | Entry relationships, cohorts, recent workload |
| `history/grouping`, `layout` | Relations + demand + config → routes and initial ledger | Grouping, placement, reusable layouts |
| `admission` | Visible features → candidates and selected shard | Frozen normalization, counter RNG, linear cost ranker |
| `training` | Historical prefixes → labels → exported model | Fusion fitting and counterfactual admission fitting |
| `runtime` | Dataset + layout + recipe → native processes and measurements | Process lifecycle, environment, resources, retention |
| `results` | Completed result JSON → figures | Measured points, shared scales, reproducible export |

`native/layout/kernel.cpp` implements grouping and placement. `native/engine` retains the common Go execution path based on BlockEmulator, including PBFT, relay and dynamic CLPA. Existing Go protocol modules are retained together to preserve their tested call graph; optional legacy protocol paths are not exposed as Arbor recipes. The public Python layer has no date-version imports, source-fragment loading or runtime monkeypatching.

## Artifact boundaries

- **Dataset:** `manifest.json` plus ordered CSV, dense-ID arrays, source timestamps and entry relations. No layout chosen from test results.
- **Prepared state:** graph and forest reconstructed from the distributed history. Neither a private server checkpoint nor an old runtime directory is required.
- **Layout:** identity includes input, fusion model, kernel, shard count and planning settings. The same layout is reused across load levels.
- **Run:** immutable planned cases; each case stores effective configuration, build/input/model identities and normal-exit evidence.
- **Result:** complete-transaction metrics, per-shard logical work and compact provenance. Published reference measurements are separate from new runs.

The Go supervisor is the authoritative native admission path. Python uses the same normalization, candidate RNG and tie contract for fitting and parity checks. Normal native execution never loads a newly trained model implicitly.

## Lifecycle

The queue locks its run directory, validates data, prepares a cached layout, checks resources and ports, then starts its own process groups. It removes inherited runtime overrides before launching every child. Native execution must exit normally and complete every transaction with valid stage counts. Only then does retention copy and hash the compact results and perform explicitly requested cleanup.

The CLI is a viewer/launcher. Closing the terminal does not own or terminate the queue. Resume accepts completed and unstarted cases; an attempted case without completion remains available for diagnosis and blocks automatic repetition.

## Validation

- Python tests exercise exact amounts, duplicates, time boundaries, forest reconstruction, layout routines, model contracts, metrics, locks, port collisions and cleanup containment.
- `scripts/check_parity.py` compares the frozen Python and Go rankers, candidate generation and action selection on deterministic inputs.
- Native tests exercise the inherited protocol and execution checks.
- `run demo` covers the three methods using newly generated history, real nodes and complete metrics.
- `train ... --small` covers sample generation through exported model without running a paper experiment.
- `docs/paper_data_parity.json` and `docs/paper_layout_parity.json` record exact input-array and K4 layout/ledger comparisons against the pre-refactor implementation.

Performance depends on the runtime host. Rebuilt binaries have their own build hashes; reference measurements retain the original engine hashes instead of being relabeled as new results.
