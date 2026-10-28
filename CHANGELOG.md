# Changelog

## 0.3.0

- Add one-command preparation of local XBlock archives, including source-row provenance.
- Make `data paper` display official source links and preparation commands.
- Remove the GitHub dataset mirror and its private-download implementation.
- Add Jiacheng Zhu and Jie Huang, Zhejiang University of Science and Technology, to author and citation metadata.
- Document baseline implementation scope and original references.

## 0.2.0

- Resume successful executions through interrupted result saving and cleanup.
- Lock shared caches, publish builds atomically and invalidate them when producing code changes.
- Verify datasets once per queue and check file identities during subsequent preparation.
- Merge nested resource overrides and add a ready-to-run small profile.
- Add model path options and snapshot selected models for the full run.
- Export the native candidate contract with newly trained admission models.
- Add boundary pause and owned-process stop commands.
- Preserve original archive/row provenance through raw-input preparation.
- Add automatic HTTPS/private-release download selection.
- Rework bilingual documentation around results, quick start and experiment use.

## 0.1.0

- Extracted temporal graphs, entry forests, planning, admission, training and native experiment management into explicit modules.
- Added one CLI and unified recipes for Arbor, Monoxide and dynamic CLPA.
- Added explicit database retention/cleanup, detached queues, completion checks and result export.
- Distributed frozen models, verified paper inputs, raw-input processing and reference tables separately from new runs.
- Added bilingual instructions, native smoke validation and cross-language model parity checks.
- Fixed shutdown accounting for out-of-order cross-shard completion reports: preserve their signed balance and require zero before drain completion. Placement, transaction execution and metric definitions are unchanged.
- Wait for stop-message writes before closing the connection pool, replacing a fixed 3 ms delay that could strand nodes on small-CPU hosts.
