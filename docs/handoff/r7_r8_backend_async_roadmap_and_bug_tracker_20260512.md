# R7/R8 Backend Async Roadmap And Bug Tracker - 2026-05-12

## Scope

- Worker: AIstock parallel worker C.
- Tasks: T22 and T24 documentation only.
- Worktree: `F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512`.
- Branch: `codex/qe-cleanup-and-pr005-prep-20260512`.
- Upstream: `origin/codex/qe-cleanup-and-pr005-prep-20260512`.
- Current `origin/main`: `da648066473be2546151bff58b8c2f3febcf2de9` (`docs(qe): add branch review decisions`).
- Merge base with `origin/main`: `48b6bef67ea175f539af7d85241fcfe59aada5be`.
- Current branch head while drafting: `102d52968cd11d0862f4461045f02907ebc1f796` (`fix(qe): keep backtest compose off event loop`).
- Ahead/behind versus `origin/main`: `origin/main...HEAD = 5 left / 4 right`.
- Safety: no DB writes, no service/server start, no production `8001`, no frontend `3000`, no Claude worktrees, no commit, no push.
- Files owned by this worker: this document and `docs/handoff/bug_entries_async_architecture_audit_20260512.md` only.

## Inputs Reviewed

- `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md`.
- Commit `102d52968cd11d0862f4461045f02907ebc1f796`.
- `backend/services/quantevolver/executors/backtest.py` diff in commit `102d529`.
- `backend/tests/unified_engine/test_backtest_executor.py` diff in commit `102d529`.
- Bug conventions in `tests/aistock_validation/bugs/README.md`, `docs/process/bug_registry_workflow_20260510.md`, and `backend/services/validation/finding_store.py`.

## Task 20 / Task 21 Themes

| Theme | Evidence | R7/R8 lesson |
|---|---|---|
| Event-loop isolation for heavy synchronous work | `102d529` moves `ConfigComposer.compose_experiment_in_memory(...)` behind `loop.run_in_executor(...)` and adds a ticker-style responsiveness regression test. | Heavy CPU/DB/filesystem work must not execute directly on FastAPI's event loop. |
| ST PIT rebuild leaves request paths | `r7_st_pit_universe_async_background_design_20260512.md` defines async enqueue/status APIs, a job table, worker execution, idempotency, retry, and consumer fail-fast behavior. | R7 should make Local Data/source sync/QE/Qlib calls cheap and predictable. |
| Strict consumers fail fast with job hints | R7 keeps Selection/Paper/QE/Qlib as consumers of ready state, not hidden rebuild owners. | Typed stale errors should include reason, latest or queued `job_id`, poll URL, and operator next step. |
| Durable operational state is missing | Existing readiness is mostly `stock_universe_pit_state` plus `last_error`; R7 proposes `market.stock_universe_pit_jobs`. | Job rows are required for progress, retry, dedupe, audit, and agent repair context. |
| Background work needs regression tests | The backtest hotfix proves event-loop responsiveness during compose. | R7 must test that enqueue returns quickly and worker execution does not starve lightweight endpoints. |

## R7 Roadmap

R7 should move ST PIT preparation out of interactive request paths while preserving current rule semantics and keeping the worker disabled until explicitly enabled in a dev environment.

### R7.1 Schema And Repository

- Add a migration for `market.stock_universe_pit_jobs` with `job_id`, universe/rule/window, source fingerprint, idempotency key, status, attempts, progress, result summary, error JSON, and timestamps.
- Add a partial unique index on active idempotency keys for `queued`, `running`, and `retry_wait`.
- Add status/priority indexes for worker claim and UI/operator status views.
- Add repository methods for enqueue/reuse, `FOR UPDATE SKIP LOCKED` claim, status update, terminal update, latest job lookup, and stale-running recovery.
- Keep `market.stock_universe_pit_state` as canonical readiness; jobs are operational metadata.

Acceptance evidence:

- Unit tests cover idempotency, active unique-index behavior, terminal-history reuse, claim ordering, and stale-running recovery.
- No production DB is touched; migration validation occurs only in an approved dev/test database or offline migration harness.

### R7.2 Service Boundary

- Introduce `StockUniversePitJobService` beside `StockUniversePitService`.
- Implement `ensure_or_enqueue(...)`, `enqueue_rebuild(...)`, `get_job(...)`, `list_jobs(...)`, `claim_next_job(...)`, and `run_job(...)`.
- Keep the existing synchronous rebuild path available only for CLI/admin/debug flows.
- Compute deterministic idempotency keys from universe/rule/window/source-fingerprint/reason-family.
- Preserve advisory locks as the second-line execution guard.

Acceptance evidence:

- Duplicate manual clicks return the same active job.
- Duplicate source-sync post-hooks coalesce when the source fingerprint is unchanged.
- A job with an old fingerprint cannot mark current state ready if source data changed mid-run.

### R7.3 Async API Contract

- Add `POST /api/v1/stock-universe/st-pit/ensure-async`.
- Change normal `POST /api/v1/stock-universe/st-pit/rebuild` to enqueue and return `202` by default.
- Keep sync rebuild only behind an explicit admin/debug option, not in normal UI.
- Extend `GET /api/v1/stock-universe/st-pit/status` with latest job hints.
- Add `GET /api/v1/stock-universe/st-pit/jobs` and `GET /api/v1/stock-universe/st-pit/jobs/{job_id}`.
- Return typed `409` stale responses when `rebuild_if_stale=false` or when a strict consumer cannot proceed.

Acceptance evidence:

- API tests prove `200 ready`, `202 queued/reused`, and `409 stale` paths.
- API response includes `job_id`, `status`, `poll_url`, stale reason, fingerprint hash, universe key, and rule version.

### R7.4 Worker Execution

- Add a single-concurrency worker behind config such as `ST_PIT_BACKGROUND_WORKER_ENABLED=false` by default.
- Run `StockUniversePitService.rebuild_st_pit_universe(...)` through `loop.run_in_executor(...)` or an equivalent worker thread/process.
- Store structured progress phases and terminal summaries.
- Classify validation failures as terminal and transient DB/connectivity failures as retryable.
- Add stale-running recovery with heartbeat/updated-at thresholds.

Acceptance evidence:

- A responsiveness smoke proves lightweight API calls remain responsive while a worker job is active.
- Worker config defaults are safe in tests/dev; enabling worker requires explicit config change.

### R7.5 Integration Points

- Source sync hook changes from `mark_dirty -> ensure_st_pit_universe(strict=False)` to `mark_dirty -> enqueue_rebuild(...)`.
- Local Data UI rebuild flow becomes enqueue + poll, not a blocking request.
- Qlib export and QE composer strict paths become readiness preflight + structured stale/job response.
- Selection Center and Paper v2 continue to read readiness and fail fast; they do not silently rebuild data.

Acceptance evidence:

- Source-sync tests show sync success is not failed by rebuild failure, but enqueue failure is surfaced and state remains dirty.
- Local Data/API tests show request returns immediately with a job id.
- QE/Qlib tests show stale state fails fast with no builder call.

## R8 Roadmap

R8 should harden operations after R7 is stable; it should not expand ST PIT rule semantics.

| R8 item | Goal | Exit criteria |
|---|---|---|
| External worker option | Move heavy rebuild execution out of FastAPI when in-process work competes with request traffic. | Same job table/API contract works with `python -m backend.services.stock_universe_pit_worker --poll`; FastAPI only enqueues/statuses. |
| Cancel/retry APIs | Add explicit operator control for safe retry and cancel-request states. | `cancel_requested`, `cancelled`, and manual retry states are tested and visible in UI/status. |
| Job heartbeat | Detect crashed workers without false recovery. | Stale `running` jobs are requeued only when heartbeat is expired and no active lock exists. |
| Better progress | Add row-count/progress details from builder phases. | Job progress includes loaded stocks/events/spans and validation summaries. |
| Fleet safety | Support multiple workers without duplicate rebuilds. | `SKIP LOCKED`, active uniqueness, and advisory-lock tests cover multi-worker claims. |
| Operator observability | Make failed jobs easy to triage. | Error JSON includes code, phase, retryable flag, traceback hash, source fingerprint, and next action. |
| Backpressure | Avoid flooding rebuild jobs during repeated source syncs. | Reason-family idempotency and priority policy avoid duplicate active work. |

## Backend Async Bug Tracker

These are proposed tracking items only. The companion doc gives registry-ready field suggestions; this task did not write bug JSON.

| Proposed ID | Severity | Module | Status | Summary | Suggested owner |
|---|---|---|---|---|---|
| `BUG-ASYNC-001` | P1 | `stock_universe` | proposed | ST PIT source-sync and Local Data rebuild can perform full rebuild inline in request/sync paths. | backend data owner |
| `BUG-ASYNC-002` | P1 | `qe_composer` | proposed | QE composer/Qlib export strict paths can rebuild ST PIT inline instead of fail-fast readiness preflight. | QE backend owner |
| `BUG-ASYNC-003` | P2 | `stock_universe` | proposed | ST PIT rebuild has no durable job registry for progress, retry, idempotency, and repair context. | backend data owner |
| `BUG-ASYNC-004` | P2 | `stock_universe` | proposed | Running rebuild may mark state ready after source fingerprint changes unless guarded by fingerprint check at completion. | backend data owner |
| `BUG-ASYNC-005` | P2 | `validation` | proposed | Async/background-work regressions lack a reusable event-loop responsiveness validation pattern outside the backtest executor hotfix. | validation owner |
| `BUG-ASYNC-006` | P3 | `local_data_ui` | proposed | Local Data ST PIT UI needs async job polling and structured failed-job display. | UI owner |

## Priority Order

1. `BUG-ASYNC-001` and `BUG-ASYNC-003`: create job table/service/API foundation.
2. `BUG-ASYNC-002`: convert QE/Qlib consumers after job hints are available.
3. `BUG-ASYNC-004`: implement source-fingerprint completion guard before enabling a real worker.
4. `BUG-ASYNC-005`: add reusable loop-responsiveness tests before full async rollout.
5. `BUG-ASYNC-006`: convert UI once backend API is stable.

## Validation Required Before Implementation Is Done

- Static/unit tests for idempotency, claim, retry classification, stale-ready matrix, and source-fingerprint completion guard.
- API tests for ready/queued/reused/stale responses and job inspection.
- Integration tests only on explicitly approved non-production dev ports and DBs.
- Event-loop responsiveness smoke while worker or simulated worker is active.
- Final report states whether production backend `8001`, frontend `3000`, production DB, dev DB, dataset promotion, commits, and pushes were touched.

## Non-Actions In This Worker Task

- No source code changed.
- No bug JSON changed.
- No migration added.
- No tests, nox sessions, servers, services, UI, DB, `8001`, or `3000` were run or touched.
- No commit or push was performed.
