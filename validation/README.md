# Acceptance evidence

`acceptance.json` records the clean Linux installation, native software smoke runs, training fixtures and cross-language checks. `demo_summary.csv` and `ablation_smoke.csv` contain these small runs; their timing is not a paper benchmark. The demo figure checks labels, export and layout.

Paper equivalence evidence is separate:

- `docs/paper_data_parity.json`: canonical inputs and six dense-array identities.
- `docs/paper_layout_parity.json`: rebuilt four-shard routing and initial-ledger identities.
- `docs/source_provenance.json`: source and model provenance.
- `reference/`: previously measured paper results, with their original scopes and build identities.

These checks exercise software and deterministic artifacts. They do not add formal paper repetitions or replace reference measurements. GitHub Actions runs the installation, core tests, three-method demo, five ablations and both small training pipelines on a fresh Ubuntu runner.
