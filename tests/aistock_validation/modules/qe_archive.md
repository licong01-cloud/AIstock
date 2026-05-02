# QE Archive Validation Matrix

This matrix covers the QE realtime experiment warehouse. It borrows the Paper v2 result-oriented validation pattern, but keeps the current QE production runtime isolated by default.

## Business Goal

Every QE experiment or loop should be archived into a reproducible, queryable warehouse without changing the behavior of currently running QE production services. The warehouse must support later comparison, charts, optimization priority, factor participation analysis, and LLM-agent read-only analysis.

## Production Isolation Rules

- Do not restart production backend `8001` during QE archive development or validation unless the user explicitly requests it.
- New QE archive hooks must be disabled by default until a specific rollout step enables them.
- New archive workers must not mutate existing QE task/loop/experiment status.
- Archive failures must not convert a successful QE experiment into a failed QE experiment.
- Artifact access must go through node APIs, controlled downloads, or AIstock-owned `qe_archive/artifacts`; never directly read QE/RD-Agent worker workspace paths.
- Tests must prove feature flags are disabled by default before any runtime hook is wired.

## L0 Guardrails

Run on every QE archive change:

```powershell
python -m nox -s qe_archive_backend
python -m nox -s qe_archive_data_quality
```

Required checks:

- Schema DDL compiles and is idempotent.
- Every managed `qe_archive` table and column has PostgreSQL COMMENT metadata.
- New DB columns have explicit tests or documented assertions.
- No direct worker path tokens are introduced (`workspace_path`, `/mnt/f`, `\\wsl$`, `QE_WORKSPACE_WIN`, `RDAGENT_WORKSPACE_WIN`).
- No protected QE/RD-Agent artifacts, model weights, StrategyPackage manifests, or HMM snapshots are modified.

## Backend L1/L2

Current required coverage:

- `init_qe_archive_schema.py` declares all required tables, indexes, schema version rows, table comments, and column comments.
- Repository writes are explicit and do not create schema implicitly.
- Event capture is disabled by default and only writes outbox events when explicitly enabled.
- Worker processing is disabled by default and only claims outbox events when explicitly enabled.
- Worker claims only event types with registered handlers, so unsupported pending events are not consumed accidentally.
- Worker creates `archive_job`, completes outbox events on success, and marks job/outbox retry state on handler failure.
- Reproducibility config contract includes canonical config, raw config, config hash, factor list/hash, config provenance, missing config items, environment hashes, package versions, and reproducibility level.
- Payload extractor/service dry-run captures already-collected loop/experiment payloads without repository writes by default.
- Payload extractor maps ordered factors, data context, daily invalidity, account summary, scalar metrics, IC/return/training curves, factor rows, all/top/bottom symbol summaries, stock trade records, execution/parser events, and raw payload snapshots.
- Manual archive service writes run/source/config/repro/data_context/account/metric/curve/factor/symbol_summary/trade/execution_event/raw_payload rows only when `dry_run=false`.
- DB source assembler can build payloads from `qe_experiments` and `qe_evolution_loops` without reading worker artifact paths.
- Manual backfill CLI defaults to dry-run and requires `--write --confirm-write QE_ARCHIVE_WRITE` before inserting archive rows.
- Backend API `/api/v1/qe-archive/backfill` supports dry-run and confirmed-write historical backfill, requires `confirm_write=QE_ARCHIVE_WRITE` for writes, and can validate run-level row counts after writing.
- Backend API `/api/v1/qe-archive/backfill-candidates` returns selectable historical candidates from QE evolution tasks and single experiments, including type, description, loop counts, archived/pending counts, model/label/factor metadata, status, and execution timestamps.
- Backfill requests with `task_ids` expand each selected evolution task into all matching completed/terminal loops, so selecting one task in UI archives all runs under that experiment rather than one loop only.
- QE completion-time realtime ingestion hook is disabled by default through `QE_ARCHIVE_REALTIME_ENABLED`; when enabled, default mode enqueues durable outbox events and must not change QE loop/experiment status on archive failure.
- Direct realtime archive writes are only allowed through explicit `QE_ARCHIVE_REALTIME_MODE=direct` rollback/diagnostic mode and must remain covered by tests.
- API worker `/api/v1/qe-archive/worker/run-once` processes a bounded outbox batch only with `confirm_run=QE_ARCHIVE_WORKER_RUN`; it is not a scheduler and must not auto-start at FastAPI startup.
- API `/api/v1/qe-archive/outbox` and `/api/v1/qe-archive/jobs` expose recent queue/job state for UI monitoring.
- Confirmed backfill runs must pass run-level data-quality checks for run/config/source/context/account/metric/curve/factor/symbol/trade/event/raw-payload row counts.
- Data-quality smoke verifies DB schema version, table existence, table comments, column comments, and pending outbox count.

Future backend workflow coverage:

- QE single experiment completion -> gated realtime archive hook or outbox event -> archive job/write -> run/config/data/metric/raw payload rows.
- QE evolution loop completion -> gated realtime archive hook or outbox event -> archive job/write -> run/config/data/metric/raw payload rows.
- Failed/interrupted experiments are archived for audit without being ranked as valid research samples.
- Daily-frequency backtests without authoritative limit/suspend handling are archived with `research_valid=false`.
- Archive retry and failure states are visible in `archive_job` and do not affect QE source status.

## Data / Artifact L2

Future required coverage:

- Enhanced metrics extraction currently covers account summary, absolute returns, curves, all/top/bottom symbols, `stock_trades`, trade diagnostics, execution trace events, and raw payload snapshots; positions reconstructed from non-structured artifacts remain future parser work.
- Artifact manifests store hash, size, content type, storage tier, source node id, collection status, and parser status.
- Artifact download/parsing failures are explicit archive warnings or job failures, not fake empty success.
- Large artifacts remain outside PostgreSQL, under AIstock-owned artifact storage.

## API L2

Future required coverage when APIs are added:

- Backfill API can preview and write historical experiment/loop rows without shell-script execution.
- Backfill API rejects write requests without explicit confirmation text.
- Run quality API returns account/metric/curve/factor/symbol/trade/execution-event/raw-payload completeness for a selected run.
- List runs with default `research_valid=true` filtering.
- Run detail returns config, metrics, account summary, curves, factor list, artifact manifest, and reproducibility status.
- Archive job status endpoints show pending/running/failed/completed states with retry context.
- No API returns raw local absolute worker paths or sensitive environment variables.

## UI L3

Current required coverage:

- Dashboard shows archive health, ingestion lag, failed jobs, and invalid-run counts.
- Backfill panel lists selectable QE experiments/tasks not fully archived, supports "select all pending", dry-run first, and confirmed write with `QE_ARCHIVE_WRITE`.
- UI must explain that minimum metrics/curves/factors are fixed post-write quality gates, not filters that reduce the collected data scope.
- Worker panel supports one-shot confirmed outbox processing with `QE_ARCHIVE_WORKER_RUN`.
- Run quality lookup shows readable Chinese labels for config, metrics, reproducibility, and missing items.
- The first UI E2E uses mocked QE archive APIs to validate dashboard/backfill/worker/quality interactions without requiring production backend `8001`.
- UI fails tests on page errors, console errors, request failures, and unexpected HTTP 4xx/5xx.

Future UI coverage:

- Charts render return/drawdown/IC/RankIC/training curves from `qe_archive`.
- Factor participation pages show per-factor experiment history and metric distributions.
- Model trial pages show hyperparameters, objective values, score components, and training curves.

## Nox Entry Points

```powershell
python -m nox -s qe_archive_backend
python -m nox -s qe_archive_data_quality
$env:QE_ARCHIVE_UI_MOCK_API='1'
python -m nox -s qe_archive_ui
$env:QE_ARCHIVE_L3_SKIP_UI='1'
python -m nox -s qe_archive_l3
```

For full UI L3 without a live dev backend, set `QE_ARCHIVE_UI_MOCK_API=1`; for live API validation, start a dev backend on `8011`/`8012` and leave the mock flag unset.

## Evidence

Every significant run should create or update a Markdown record under `tests/aistock_validation/history/qe/` or `tests/aistock_validation/history/qe_archive/` with:

- Exact commands and environment flags.
- Production impact statement.
- DB/API/UI evidence.
- Artifact safety review.
- Failures, fixes, reruns, and residual risks.
