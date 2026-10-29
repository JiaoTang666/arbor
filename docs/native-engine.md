# Native engine

Arbor uses one BlockEmulator-based Go engine for its three supported methods. The Python runner supplies effective configuration and model paths; the native supervisor dispatches transactions to PBFT shards and collects completion reports.

| Path | Responsibility |
|---|---|
| `native/engine/main.go`, `build/` | Process startup and node/supervisor construction |
| `supervisor/committee/committee_relay.go` | Common relay input and dispatch |
| `supervisor/committee/initial_placement_policy.go`, `placement_scoring.go`, `cost_ranker*.go` | Arbor first placement, visible features and frozen-model scoring |
| `supervisor/committee/forest_facts.go`, `forest_runtime.go` | Source relationships and execution-work observations |
| `supervisor/committee/clpa_epoch.go`, `partition/` | Dynamic CLPA planning, epoch control and repartitioning |
| `consensus_shard/pbft_all/`, `chain/`, `core/` | Shard consensus, execution, state and transaction pools |
| `message/`, `networks/` | Wire records and transport |

Monoxide uses fixed address placement and the common relay path. Arbor adds first placement and graph/forest observations; dynamic CLPA adds its epoch partitioning and state-transfer path. Recipe configuration selects these paths through the runner.

Inherited code also includes ownership certificates, forecast controllers, group migration and threshold/queue-adjusted admission policies. These remain in the engine's existing package graph; the Arbor recipes do not expose them as additional methods. Protocol schema names, wire fields, hash domains and deterministic random contexts retain their established identities independently of source filenames.

## Legacy integration tests

Three inherited tests require external fixtures and are excluded from the default build with the `legacy_integration` tag:

| Test | External inputs |
|---|---|
| `TestForecastCrossLanguageProtocol` | `SAA_PROTOCOL_TEST_PROJECT`, `SAA_PROTOCOL_TEST_PYTHON` |
| `TestNativeSAAOneDecision` | `SAA_PROTOCOL_TEST_PROJECT`, `SAA_ONE_RUN_DIR` |
| `TestNativeSAAReDecisionCurrentMapping` | `SAA_PROTOCOL_TEST_PROJECT`, `SAA_REDECISION_SOURCE`, `SAA_REDECISION_OUTPUT` |

The external project must provide `saa_research_data_contract_v1/scripts/` and, for the first two tests, `evidence/v35_clockfix_20260906/relation_model_v2_seed43/relation_model.pt`. The stopped-run reconstruction test also consumes its earlier run artifacts. Those fixtures and controller scripts are not distributed with Arbor; these tests are retained for maintainers who have that project, rather than as standalone reproduction commands. The one-decision test starts native nodes and writes its designated run directory.

Maintainers with those inputs can select an individual test from `native/engine`:

```bash
go test -tags legacy_integration ./supervisor/committee \
  -run '^TestForecastCrossLanguageProtocol$'
```

The default test suite, CLI examples and paper recipes use the inputs and models prepared by this repository.
