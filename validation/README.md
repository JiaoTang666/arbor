# Acceptance evidence

`review_acceptance.json` covers v0.2.0 cache/recovery/configuration regression tests, boundary pause/resume, live stop, and native use of newly trained models. Missing RSS observations are stored as null.

`acceptance.json` records the clean Linux installation, native software smoke runs, training fixtures and cross-language checks. `demo_summary.csv` and `ablation_smoke.csv` contain these small runs; their timing is not a paper benchmark. The demo figure checks labels, export and layout.

`release_acceptance.json` records the final three-method and five-ablation native runs restricted to two CPUs, plus the successful GitHub Actions run. These include the shutdown fixes documented in the changelog. All cases completed 120 transactions, exited normally and passed cleanup.

Paper equivalence evidence is separate:

- `paper_download.json`: actual private-release download and extracted-member verification.

- `docs/paper_data_parity.json`: canonical inputs and six dense-array identities.
- `docs/paper_layout_parity.json`: rebuilt four-shard routing and initial-ledger identities.
- `docs/source_provenance.json`: source and model provenance.
- `reference/`: previously measured paper results, with their original scopes and build identities.

Software smoke tests use synthetic 120-transaction traces; paper measurements are in `reference/`. GitHub Actions runs installation, core tests, three-method execution, five ablations, both training pipelines and a native execution using the trained models.
