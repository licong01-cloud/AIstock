# AIstock Backend Async DB Architecture RCA

Date: 2026-05-12
Owner: Task 20 documentation worker
Scope: architecture RCA only; no code, service, server, DB, commit, or push changes
Output: docs/architecture/aistock_backend_async_db_architecture_rca_20260512.md

## Executive Summary

AIstock has an async FastAPI surface, but a large part of its backend work still
uses synchronous psycopg2, synchronous filesystem/artifact composition, and
synchronous data-preparation services. Task 18 correctly moved one QE compose
call off the event loop by wrapping `ConfigComposer.compose_experiment_in_memory`
with `loop.run_in_executor(...)`, but that is only a T3 stage-1 mitigation. It
reduces event-loop blocking for the submit path; it does not redesign backend DB
access, cache semantics, pool ownership, or observability.

The root cause is architectural coupling: hot request handlers, cold data/artifact
builders, cache creation, and DB-heavy readiness checks share the same process,
same generic sync pool, and often the same request latency budget. The fix should
be an incremental R7-R8 backend architecture program, not a cache workaround.

## Evidence And Findings

| Evidence | Finding | Architecture Impact |
|---|---|---|
| `backend/services/quantevolver/executors/backtest.py:80` gets the running event loop, and `backend/services/quantevolver/executors/backtest.py:95` awaits `loop.run_in_executor(None, compose_call)`. | Task 18 moved QE in-memory composition off the event loop. | This is useful T3 stage-1 sync isolation, but the default executor is unbounded by DB role and does not solve pool, cache, or hot/cold ownership. |
| `backend/services/quantevolver/executors/backtest.py:71` calls stock-pool payload preparation before the executor handoff. | Some synchronous pre-compose work remains in the async submit method. | T3 needs a complete inventory of sync calls around the submit path, not just the central composer call. |
| `backend/services/quantevolver/config_composer.py:1335` defines `compose_experiment_in_memory(...)`, and `backend/services/quantevolver/config_composer.py:1383`, `backend/services/quantevolver/config_composer.py:1384`, and `backend/services/quantevolver/config_composer.py:1385` synchronously load factors, model, and strategy. | Composition is not just CPU/string work; it contains DB reads and remote/source lookup paths. | Moving it to a thread protects the loop, but those threads still consume DB pool slots and backend CPU. |
| `backend/services/quantevolver/config_composer.py:887` calls `StockUniversePitService().ensure_st_pit_universe(...)` while building `qe_event_risk_policy.json`. | QE artifact generation can transitively trigger ST PIT readiness/rebuild logic from a submit/export path. | This is a hot/cold boundary violation that R7 must remove by readiness preflight plus background jobs. |
| `backend/services/quantevolver/config_composer.py:894`, `backend/services/quantevolver/config_composer.py:911`, and `backend/services/quantevolver/config_composer.py:935` synchronously read trading calendar, ST PIT spans, and PIT state. | Even after readiness, risk-policy artifact generation performs several blocking DB reads. | T3 and T4 must isolate these reads and bound their pool usage. |
| `backend/services/quantevolver/config_composer.py:3624`, `backend/services/quantevolver/config_composer.py:3773`, and `backend/services/quantevolver/config_composer.py:3825` synchronously read factor/model/strategy catalogs. | Composer reads multiple catalogs per submit and currently owns both orchestration and data retrieval. | T1/T2 should separate hot metadata resolution from cold artifact materialization and cache stable catalog snapshots. |
| `backend/services/quantevolver/config_composer.py:375` caches execution algorithm catalog entries in `_execution_algo_catalog_cache`. | Some read-mostly metadata is already cached in-process. | T2 should formalize cache scope, invalidation, and observability rather than adding ad hoc caches. |
| `backend/db/pg_pool.py:124` initializes a process-global `ThreadedConnectionPool`; `backend/main.py:240` starts it with `minconn=5, maxconn=40`. | One generic sync pool serves heterogeneous request, background, and artifact paths. | T4 should split or budget pools by workload class before more background work is added. |
| `backend/db/pg_pool.py:205` blocks on `_DB_POOL.getconn()`, while `backend/db/pg_pool.py:263` logs connections held longer than one second. | Pool checkout is synchronous and has partial slow-holder debugging. | T5 needs metrics/tracing/alerts; print-debug snapshots are not enough for production diagnosis. |
| `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md:260` states synchronous CPU/DB-heavy builder work must run through an executor, matching the QE submit lesson. | R7 already recognizes the event-loop risk for ST PIT. | The same principle should be generalized to all cold builders and sync DB work. |
| `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md:378` says ConfigComposer currently strict-ensures ST PIT spans before reading spans and should avoid making QE submit responsible for rebuilds. | ST PIT is the clearest current example of mixed hot/cold ownership. | R7 should remove rebuild work from QE submit/export and return structured stale/job hints. |
| `docs/operations/selection_center_health_preflight_analysis_20260512.md:146` records isolated tests with no services, ports, or DB. | Current cleanup validation is intentionally safe and limited. | This RCA should remain design-only; runtime validation belongs to later authorized dev-port phases. |

## Root Cause

The backend has an async API shell but not a fully async or workload-isolated
backend architecture. The root cause has five parts:

1. Request paths can call builders that perform many DB reads, local file reads,
   and sometimes readiness/rebuild work.
2. Cache behavior is embedded in individual services instead of being a typed
   layer with freshness contracts and invalidation signals.
3. Sync DB and sync builder work can be moved to threads, but those threads still
   compete for the same process CPU and the same generic psycopg2 pool.
4. The DB pool is process-global and role-agnostic; hot reads, cold builders,
   scanners, and background jobs do not have separate budgets or backpressure.
5. Observability is mostly debug prints and request-level failures, so the system
   can identify symptoms after the fact but cannot reliably attribute latency to
   event-loop blocking, pool starvation, cache miss cost, or cold job work.

## T1 Hot/Cold Separation

Hot paths are interactive API requests whose correct behavior is fast readiness,
submission, status, and health decisions. Cold paths are artifact builders,
rebuilds, historical scans, cache materialization, and bulk DB transforms.

Concise findings:

- QE submit currently invokes `BacktestExecutor.submit()` and builds the RDAgent
  payload from `ConfigComposer.compose_experiment_in_memory(...)`; Task 18 moved
  the central compose call to a worker thread, but the submit request still waits
  for cold artifact composition to finish before returning.
- ST PIT is the most explicit boundary violation: `ConfigComposer` can ensure ST
  PIT readiness and then read spans while composing `qe_event_risk_policy.json`.
  The R7 ST PIT design already states this should become readiness preflight plus
  async job status, not inline rebuild.
- Selection Center health preflight is a better hot-path model: fail fast on
  package/runtime readiness instead of generating or repairing authoritative
  artifacts inside the run request.

Target architecture:

- Hot API: validate request, read small readiness snapshots, enqueue/reuse jobs,
  return typed `ready`, `queued`, `stale`, `blocked`, or `failed` responses.
- Cold workers: compose large payloads, rebuild ST PIT, materialize factor/cache
  assets, generate live artifacts, and perform bulk scans under explicit worker
  concurrency and pool budgets.
- Contract boundary: hot paths may request or observe cold work; they should not
  own cold work unless an explicit admin/debug sync flag is used in a non-prod
  environment.

## T2 Cache Layer

This RCA is not recommending a cache workaround. The cache layer should be a
first-class architecture layer with typed sources, freshness, invalidation, and
observability.

Concise findings:

- `ConfigComposer._execution_algo_catalog_cache` shows the codebase already uses
  process-local read-mostly caching, but without a common cache contract.
- Composer repeatedly resolves factor, model, strategy, node paths, execution
  algorithm defaults, ST PIT state, and trading calendars. Some of these are
  stable per process or per source fingerprint; others must remain strict and
  current.
- Factor cache behavior exists in generated QE scripts and node paths, but the
  backend lacks a single place to answer: was the cache a valid hit, stale hit,
  forced miss, materialization failure, or unsupported cache mode?

Target architecture:

- Define cache classes by authority:
  - `metadata_snapshot`: model/strategy/execution-algo/node metadata with short
    TTL or explicit invalidation on catalog updates.
  - `readiness_snapshot`: ST PIT/source dataset health, never allowed to mask a
    stale authoritative state.
  - `artifact_cache`: generated configs, factor payloads, stock-pool payloads,
    and live-inference prerequisites keyed by content hash/source fingerprint.
- Every cache read should return structured metadata: `hit`, `miss`, `stale`,
  `source_version`, `fingerprint`, `age_ms`, and `reason`.
- Hot paths may use cache only when the cache contract says it is authoritative
  enough for that decision. If not, hot paths should enqueue cold refresh or fail
  fast with a typed reason.
- Cache invalidation should be event-driven where possible: catalog mutation,
  source sync success, node config change, and ST PIT source fingerprint change.

## T3 Sync Isolation

Task 18 delivered T3 stage-1 mitigation only: compose off event loop. It is a
necessary first step but not the final isolation architecture.

Concise findings:

- `BacktestExecutor.submit()` now awaits `run_in_executor(None, compose_call)` for
  `compose_experiment_in_memory(...)`. This protects the event loop from the main
  composer body.
- The executor uses `None`, so it relies on the default thread pool instead of an
  AIstock-defined executor with named workload, queue depth, timeout, and DB pool
  budget.
- Sync calls around compose, such as stock-pool preparation and later
  `create_and_run_loop(...)`, still need inventory and latency attribution.
- The R7 ST PIT design applies the same lesson to synchronous CPU/DB-heavy ST PIT
  builder work.

Target architecture:

- Create explicit sync-isolation executors by workload class, for example
  `qe_compose_executor`, `st_pit_worker_executor`, `live_artifact_executor`, and
  `maintenance_executor`.
- Each executor should have a bounded queue, max workers, timeout/cancellation
  policy, and metrics.
- Async routes should call only small async/readiness functions directly; any
  known sync DB-heavy or CPU-heavy function must cross an explicit isolation
  boundary.
- Use typed errors for queue saturation and timeouts so hot APIs can return 429,
  503, or 409 with operator hints rather than hanging.

## T4 Pool Redesign

The current pool is a generic process-global psycopg2 `ThreadedConnectionPool`.
That is better than per-query direct connections, but it does not protect hot
traffic from cold builders or background jobs.

Concise findings:

- `backend/main.py:240` initializes one pool with `minconn=5, maxconn=40`.
- `get_conn()` blocks synchronously on `_DB_POOL.getconn()` and all callers share
  the same pool unless the process has not initialized the pool and falls back to
  direct connections.
- The pool has debug hooks for slow checkout and long-held connections, which is
  a useful signal that pool starvation has already been a recognized risk.
- Adding R7 background jobs without pool budgeting could improve event-loop
  responsiveness while still starving interactive DB reads.

Target architecture:

- Split DB access by role, even if all roles still use psycopg2 initially:
  - `hot_read_pool`: request readiness, status, package health, small catalog
    reads, low timeout, high priority.
  - `cold_worker_pool`: ST PIT rebuilds, artifact generation, cache
    materialization, bulk scans, lower concurrency.
  - `maintenance_pool`: migrations/repair/admin jobs, disabled from normal API
    paths unless explicitly authorized.
- Add a pool broker or context wrapper requiring callers to declare workload,
  operation name, expected duration class, and read/write intent.
- Enforce statement timeout and pool checkout timeout per workload class.
- Prefer transaction-scoped functions that minimize connection hold time; do not
  keep a connection while doing local file IO, remote HTTP, or CPU transforms.
- Longer term, evaluate async DB drivers for hot read paths only after sync/cold
  isolation and role budgets are in place; an async driver alone will not fix
  cold work in request paths.

## T5 Observability

Observability must explain which layer is slow: event loop, executor queue, DB
checkout, SQL execution, cache miss, cold builder, remote node API, or local file
IO.

Concise findings:

- `pg_pool.py` can print slow checkout, pool state, and long-held connections,
  but this is debug output rather than structured telemetry.
- Task 18 and R7 both need event-loop responsiveness proof, not just unit tests.
- The Selection Center health preflight analysis shows the value of precise
  fail-fast reasons; the same style should be used for async DB and cache
  failures.

Target architecture:

- Add structured metrics:
  - event-loop lag histogram;
  - executor queue depth, wait time, run time, timeout count;
  - DB pool checkout latency, held duration, active/free count by pool role;
  - SQL duration by operation name, not raw SQL text;
  - cache hit/miss/stale counts by cache class;
  - cold job duration, phase, attempt count, and failure code.
- Add structured logs with correlation IDs across hot request, enqueue, cold job,
  DB operations, and remote node calls.
- Add readiness endpoints for internal diagnostics that expose redacted pool and
  executor health without requiring production log scraping.
- Add regression smoke tests for event-loop responsiveness while cold work is
  queued/running on a non-production dev port.

## Incremental R7-R8 Roadmap And Workload

### R7: Containment And Explicit Boundaries

Goal: stop new event-loop and request-path blocking while keeping current sync DB
implementation.

Workload:

1. Inventory hot routes that call sync DB-heavy or CPU-heavy services; start with
   QE submit/export, ST PIT endpoints, Local Data rebuild, Selection/Paper health,
   and live artifact generation.
2. Keep Task 18 as stage-1 mitigation and add tests proving `BacktestExecutor`
   invokes compose through an executor rather than inline on the event loop.
3. Implement the R7 ST PIT async job model from
   `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md`:
   enqueue/reuse job, readiness/status API, worker-thread execution, idempotency,
   and strict consumer fail-fast.
4. Replace QE composer ST PIT inline ensure/rebuild responsibility with readiness
   preflight plus typed stale/job hints.
5. Introduce named sync-isolation executors and migrate known cold builders from
   `run_in_executor(None, ...)` to bounded workload-specific executors.
6. Add minimum metrics/logging for executor queue time, run time, and DB checkout
   time by workload.

Acceptance:

- Interactive ST PIT rebuild returns quickly with a job id; it does not run the
  full builder inline.
- QE submit/export no longer rebuilds ST PIT in the request path.
- Event-loop responsiveness smoke passes while cold work is queued/running.
- Production `8001`, `3000`, production DB, dataset promotion, commits, and
  pushes are not used as validation unless separately approved.

### R8: Pool And Cache Architecture

Goal: make hot/cold separation durable under load and make cache behavior
explicit enough for operations.

Workload:

1. Introduce workload-declared DB access wrappers and split pool budgets for hot
   reads, cold workers, and maintenance.
2. Add per-role pool checkout timeout, statement timeout, and structured metrics.
3. Formalize cache contracts for metadata snapshots, readiness snapshots, and
   artifact caches; add invalidation hooks for catalog/source/node mutations.
4. Convert high-volume stable metadata reads in `ConfigComposer` to the cache
   layer where safe, while preserving fail-fast checks for authoritative runtime
   data.
5. Add trace/correlation IDs from request to job to DB to remote-node calls.
6. Decide whether hot read paths should move to an async DB driver after pool
   role separation proves stable.

Acceptance:

- Cold jobs cannot exhaust the hot request DB pool.
- Cache hits/misses/stale decisions are visible in metrics and logs.
- Pool starvation diagnostics identify workload owner and operation name without
  relying on ad hoc print output.
- Hot endpoints have documented latency budgets and fail typed when budgets are
  exceeded.

## Risks If This Is Treated As A Cache Workaround

- Caching ST PIT or catalog reads without source fingerprints can make stale data
  look healthy.
- Moving more work to default executor threads can hide event-loop blocking while
  creating DB pool starvation.
- Increasing `maxconn` can shift pressure to PostgreSQL instead of fixing caller
  ownership and connection hold time.
- Retrying failed hot requests can duplicate cold work unless idempotent job keys
  and queue semantics exist first.

## Validation Performed For This RCA

Static/documentation validation only:

```powershell
git status --short --branch
Get-Content backend/services/quantevolver/executors/backtest.py -TotalCount 260
Get-Content backend/services/quantevolver/config_composer.py -TotalCount 320
rg -n "get_conn|SELECT|INSERT|UPDATE|DELETE|httpx|cache|async|compose_experiment_in_memory|run_in_executor|pool|compute_nodes|factor" backend/services/quantevolver/config_composer.py backend/services/quantevolver/executors/backtest.py docs/architecture/r7_st_pit_universe_async_background_design_20260512.md docs/operations/selection_center_health_preflight_analysis_20260512.md
Get-Content docs/architecture/r7_st_pit_universe_async_background_design_20260512.md
Get-Content docs/operations/selection_center_health_preflight_analysis_20260512.md
Get-Content backend/db/pg_pool.py -TotalCount 260
rg -n "init_db_pool|close_db_pool|DB_POOL|AISTOCK_PG" backend -S --glob "*.py"
rg -n "Task 18|compose off event loop|run_in_executor|event loop|ConfigComposer|BacktestExecutor|async DB|pool starvation|R7|R8" docs backend/tests -S --glob "*.md" --glob "*.py"
```

Not run:

- No backend/frontend service start or restart.
- No production backend `8001` or frontend `3000` access.
- No DB reads or writes.
- No Paper daemon, broker, miniQMT, RDAgent node execution, nox, npm, or
  Playwright.
- No commits or pushes.
