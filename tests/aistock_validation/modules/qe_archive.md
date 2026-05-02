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
- Payload extractor maps ordered factors, data context, daily invalidity, account summary, scalar metrics, IC/return/training curves, factor rows, and raw payload snapshots.
- Manual archive service writes run/source/config/repro/data_context/account/metric/curve/factor/raw_payload rows only when `dry_run=false`.
- DB source assembler can build payloads from `qe_experiments` and `qe_evolution_loops` without reading worker artifact paths.
- Manual backfill CLI defaults to dry-run and requires `--write --confirm-write QE_ARCHIVE_WRITE` before inserting archive rows.
- Confirmed backfill runs must pass run-level data-quality checks for run/config/source/context/account/metric/curve/factor/raw-payload row counts.
- Data-quality smoke verifies DB schema version, table existence, table comments, column comments, and pending outbox count.

Future backend workflow coverage:

- QE single experiment completion -> outbox event -> archive job -> run/config/data/metric/raw payload rows.
- QE evolution loop completion -> outbox event -> archive job -> run/config/data/metric/raw payload rows.
- Failed/interrupted experiments are archived for audit without being ranked as valid research samples.
- Daily-frequency backtests without authoritative limit/suspend handling are archived with `research_valid=false`.
- Archive retry and failure states are visible in `archive_job` and do not affect QE source status.

## Data / Artifact L2

Future required coverage:

- Enhanced metrics extraction covers account summary, absolute returns, curves, all/top/bottom symbols, trades, positions, and diagnostics.
- Artifact manifests store hash, size, content type, storage tier, source node id, collection status, and parser status.
- Artifact download/parsing failures are explicit archive warnings or job failures, not fake empty success.
- Large artifacts remain outside PostgreSQL, under AIstock-owned artifact storage.

## API L2

Future required coverage when APIs are added:

- List runs with default `research_valid=true` filtering.
- Run detail returns config, metrics, account summary, curves, factor list, artifact manifest, and reproducibility status.
- Archive job status endpoints show pending/running/failed/completed states with retry context.
- No API returns raw local absolute worker paths or sensitive environment variables.

## UI L3

Future required coverage when UI is added:

- Dashboard shows archive health, ingestion lag, failed jobs, and invalid-run counts.
- Run detail shows readable Chinese labels for config, metrics, reproducibility, and missing items.
- Charts render return/drawdown/IC/RankIC/training curves from `qe_archive`.
- Factor participation pages show per-factor experiment history and metric distributions.
- Model trial pages show hyperparameters, objective values, score components, and training curves.
- UI fails tests on page errors, console errors, request failures, and unexpected HTTP 4xx/5xx.

## Nox Entry Points

```powershell
python -m nox -s qe_archive_backend
python -m nox -s qe_archive_data_quality
$env:QE_ARCHIVE_L3_SKIP_UI='1'
python -m nox -s qe_archive_l3
```

When QE archive UI tests are implemented, remove `QE_ARCHIVE_L3_SKIP_UI=1` and require `qe_archive_ui` in the L3 suite.

## Evidence

Every significant run should create or update a Markdown record under `tests/aistock_validation/history/qe/` or `tests/aistock_validation/history/qe_archive/` with:

- Exact commands and environment flags.
- Production impact statement.
- DB/API/UI evidence.
- Artifact safety review.
- Failures, fixes, reruns, and residual risks.
