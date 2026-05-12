# Bug Entries Async Architecture Audit - 2026-05-12

## Scope

- Worker: AIstock parallel worker C.
- Task: T24 audit existing bug entry conventions/registry and propose concrete async-architecture bug entries or backlog items.
- Worktree: `F:/Dev/AIstock_worktrees/qe-cleanup-and-pr005-prep-20260512`.
- Branch: `codex/qe-cleanup-and-pr005-prep-20260512`.
- Current `origin/main`: `da648066473be2546151bff58b8c2f3febcf2de9` (`docs(qe): add branch review decisions`).
- Merge base with `origin/main`: `48b6bef67ea175f539af7d85241fcfe59aada5be`.
- Current branch head while drafting: `102d52968cd11d0862f4461045f02907ebc1f796` (`fix(qe): keep backtest compose off event loop`).
- Safety: no DB writes, no service/server start, no production `8001`, no frontend `3000`, no Claude worktrees, no commit, no push.
- Write scope: this document and `docs/handoff/r7_r8_backend_async_roadmap_and_bug_tracker_20260512.md` only.

## Registry Locations Audited

| Surface | Finding |
|---|---|
| `tests/aistock_validation/bugs/` | Physical bug registry exists. It contains numeric `BUG-001` through at least `BUG-035`, plus `BUG-AUDIT-001` through `BUG-AUDIT-003`. This worker did not modify any JSON. |
| `tests/aistock_validation/bugs/README.md` | Documents file naming, schema `aistock_validation_bug_v1`, required lifecycle fields, status machine, and agent-context expectations. The file renders with encoding mojibake in this shell, but the structure is readable. |
| `docs/process/bug_registry_workflow_20260510.md` | Documents discover, register, assign, fix, independent verify, and close workflow. |
| `docs/process/dual_party_verify_20260510.md` | Documents that the fixer does not self-verify and the verifier reruns the stored reproduce command. |
| `backend/services/validation/finding_store.py` | `ValidationFindingStore._normalize_bug(...)` is the runtime source for normalized bug fields and `bug_agent_context`. |
| `backend/routers/validation.py` | Exposes bug agent-context route shape for Validation Center. |
| `docs/process/cross_tool_review_protocol_20260510.md` | Provides review bug-report conventions: severity, evidence, reproduce command, impact, and ownership. |

## Existing Bug Entry Convention

Physical registry naming:

```text
<YYYYMMDD>_BUG-<NNN>-<short-slug>.json
```

Examples:

```text
20260510_BUG-001-archive-handler-subclass-crash.json
20260510_BUG-006-cash-ledger-schema-divergence.json
20260511_BUG-035-paper-v2-daemon-emit-not-adding-routing-class.json
20260511_BUG-AUDIT-001-paper-ready-governance-limit-truncation.json
```

`BUG-AUDIT-*` is already used for audit-derived issues. There is no existing `BUG-ASYNC-*` JSON in the registry at audit time.

`finding_store.py::_normalize_bug` consumes:

- `schema_version`, `bug_id`, `title`, `description`, `module`, `severity`, `risk_area`, `status`.
- `trigger_condition`, `reproduce_command`, `failing_run_id`, `evidence_uris`, `fingerprint`.
- `github_issue_number`, `github_issue_url`, `assigned_agent`, `fix_branch`, `fix_commit`, `verification_run_id`.
- `created_at`, `first_seen_at`, `last_seen_at`, `fixed_at`, `submitted_at`, `closed_at`.
- `allowed_write_scope`, `suspected_modules`, `required_verification`, `closure_requirements`, `events`.

Status flow:

```text
open -> in_progress -> fixed -> verified
                         |
                         +-> wontfix
```

Important convention: `fixed` is not closed. Independent verification must produce `verification_run_id` before `verified`/close.

Agent-context entries should include `problem_statement`, `reproduce_command`, `evidence_uris`, `allowed_write_scope`, `suspected_modules`, `required_verification`, and `closure_requirements`.

## Recommendation

Use `BUG-ASYNC-*` as a planning namespace first. If the registry owner wants strict global numeric IDs, map these entries to the next available `BUG-<NNN>` numbers only after checking concurrent changes. At audit time the numeric series appears to reach `BUG-035`; this worker did not allocate new global numbers.

Suggested filenames if accepted:

```text
20260512_BUG-ASYNC-001-st-pit-inline-rebuild-request-path.json
20260512_BUG-ASYNC-002-qe-composer-inline-st-pit-rebuild.json
20260512_BUG-ASYNC-003-st-pit-no-durable-job-registry.json
20260512_BUG-ASYNC-004-st-pit-fingerprint-race-can-mark-stale-ready.json
20260512_BUG-ASYNC-005-async-event-loop-responsiveness-coverage-gap.json
20260512_BUG-ASYNC-006-local-data-st-pit-ui-blocking-rebuild.json
```

## Proposed BUG-ASYNC Entries

### BUG-ASYNC-001

- Title: ST PIT source-sync and Local Data rebuild can run full rebuild inline in request paths.
- Module: `stock_universe`.
- Severity: P1.
- Risk area: `performance`.
- Status if registered: `open`.
- Trigger condition: ST PIT universe is stale and source sync or Local Data rebuild is invoked.
- Expected behavior: quick enqueue or structured stale response.
- Current risk: synchronous rebuild may execute inline, keeping an HTTP request or sync task occupied while CPU/DB-heavy work runs.
- Reproduce command candidate: `pytest backend/tests/stock_universe -q -k "st_pit and (rebuild or source_sync)"`.
- Evidence: `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md`; commit `102d52968cd11d0862f4461045f02907ebc1f796`.
- Fingerprint: `stock_universe::st_pit::inline_rebuild_request_path`.
- Allowed write scope: `backend/services/stock_universe_pit*.py`, `backend/routers/stock_universe*.py`, stock-universe tests, Local Data UI files.
- Required verification: normal rebuild returns `202` job response without invoking full builder inline; source sync marks dirty and enqueues/reuses job; responsiveness smoke runs only on approved non-production dev port if service validation is authorized.

### BUG-ASYNC-002

- Title: QE composer and Qlib export can rebuild ST PIT inline instead of fail-fast readiness preflight.
- Module: `qe_composer`.
- Severity: P1.
- Risk area: `runtime_timeout`.
- Status if registered: `open`.
- Trigger condition: QE submit or Qlib export when ST PIT state is stale, missing, building, or failed.
- Expected behavior: structured `409` or typed stale error with latest job hint and no builder call.
- Current risk: strict ensure path can call rebuild inline.
- Reproduce command candidate: `pytest backend/tests -q -k "st_pit and (composer or qlib or export)"`.
- Evidence: `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md`.
- Fingerprint: `qe_composer::st_pit::strict_ensure_inline_rebuild`.
- Allowed write scope: `backend/services/quantevolver/**`, `backend/services/qlib/**`, composer/Qlib tests.
- Required verification: readiness matrix test; regression test proves builder is not called from submit/export path; API response includes stale reason and job hint.

### BUG-ASYNC-003

- Title: ST PIT rebuild lacks durable job registry for progress, retry, idempotency, and repair context.
- Module: `stock_universe`.
- Severity: P2.
- Risk area: `observability`.
- Status if registered: `open`.
- Trigger condition: ST PIT rebuild is started or fails.
- Expected behavior: durable job row exposes progress, result, error JSON, attempt count, requester, reason, and idempotency key.
- Current risk: operator sees readiness state/last_error without a job timeline.
- Reproduce command candidate: `pytest backend/tests/stock_universe -q -k "job or st_pit"`.
- Evidence: `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md`.
- Fingerprint: `stock_universe::st_pit::missing_durable_job_registry`.
- Allowed write scope: migrations, `backend/services/stock_universe_pit*.py`, stock-universe tests.
- Required verification: migration/schema test for active idempotency unique index; repository test for enqueue/reuse and `SKIP LOCKED` claim; agent context can include job id/status/progress/error JSON.

### BUG-ASYNC-004

- Title: Running ST PIT rebuild can mark stale fingerprint ready without completion guard.
- Module: `stock_universe`.
- Severity: P2.
- Risk area: `data_correctness`.
- Status if registered: `open`.
- Trigger condition: source fingerprint changes while an ST PIT job is running.
- Expected behavior: old job cannot clear dirty state for the new fingerprint.
- Current risk: no durable job-layer completion guard exists yet.
- Reproduce command candidate: `pytest backend/tests/stock_universe -q -k "fingerprint and superseded"`.
- Evidence: `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md`.
- Fingerprint: `stock_universe::st_pit::fingerprint_completion_guard_missing`.
- Allowed write scope: `backend/services/stock_universe_pit*.py`, stock-universe tests.
- Required verification: test source fingerprint changes during running job; old job is superseded or succeeded-stale and state remains dirty; newer job can later mark state ready.

### BUG-ASYNC-005

- Title: Async backend paths lack reusable event-loop responsiveness regression coverage.
- Module: `validation`.
- Severity: P2.
- Risk area: `test_coverage`.
- Status if registered: `open`.
- Trigger condition: new async backend path wraps synchronous heavy work.
- Expected behavior: responsiveness test proves event loop remains live.
- Current risk: coverage exists for BacktestExecutor only after commit `102d529`.
- Reproduce command candidate: `pytest backend/tests/unified_engine/test_backtest_executor.py::TestBacktestExecutorBasic::test_submit_keeps_event_loop_responsive_during_compose -q`.
- Evidence: commit `102d52968cd11d0862f4461045f02907ebc1f796`; `backend/tests/unified_engine/test_backtest_executor.py`.
- Fingerprint: `validation::async_backend::responsiveness_pattern_not_generalized`.
- Allowed write scope: backend tests, `tests/aistock_validation/**`, async architecture docs.
- Required verification: reusable responsiveness helper or documented pattern exists; R7 ST PIT worker/API tests include a responsiveness regression; BacktestExecutor test remains green.

### BUG-ASYNC-006

- Title: Local Data ST PIT UI needs async job polling and structured failed-job display.
- Module: `local_data_ui`.
- Severity: P3.
- Risk area: `ux_operability`.
- Status if registered: `open`.
- Trigger condition: operator clicks ST PIT rebuild in Local Data UI.
- Expected behavior: UI immediately shows job id/status and polls until terminal state.
- Current risk: UI path is documented as blocking today.
- Reproduce command candidate: `npm --prefix frontend test -- --runInBand -t "st pit"`.
- Evidence: `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md`.
- Fingerprint: `local_data_ui::st_pit::blocking_rebuild_no_polling`.
- Allowed write scope: Local Data UI files, frontend tests, stock-universe API client/router as needed.
- Required verification: UI test covers enqueue response and polling states; failed job displays structured error and retry guidance; no production frontend `3000` used unless explicitly approved.

## Backlog Items If Not Registered As Bugs

| Backlog key | Priority | Description |
|---|---|---|
| `ASYNC-BACKLOG-001` | High | Build ST PIT job table/repository/service/API foundation. |
| `ASYNC-BACKLOG-002` | High | Convert source-sync and Local Data rebuild to enqueue-only default behavior. |
| `ASYNC-BACKLOG-003` | High | Convert QE composer/Qlib export strict paths to readiness preflight + job hint. |
| `ASYNC-BACKLOG-004` | High | Add fingerprint completion guard and superseded-job handling. |
| `ASYNC-BACKLOG-005` | Medium | Generalize event-loop responsiveness regression tests for async backend paths. |
| `ASYNC-BACKLOG-006` | Medium | Convert Local Data UI to job polling and structured failure display. |
| `ASYNC-BACKLOG-007` | Medium | Add R8 external worker, cancel/retry APIs, heartbeat, and richer progress. |

## Registry Write Guidance For Future Dispatch

1. Re-run `Get-ChildItem tests/aistock_validation/bugs -Filter *.json | Sort-Object Name | Select-Object -Last 20` to avoid ID conflicts.
2. Decide whether `BUG-ASYNC-*` is accepted or whether entries must use the next global numeric `BUG-<NNN>` IDs.
3. Keep one bug per JSON file.
4. Keep `reproduce_command`, `allowed_write_scope`, and `required_verification` concrete enough for a repair agent.
5. Include evidence links to `docs/architecture/r7_st_pit_universe_async_background_design_20260512.md` and commit `102d529` where relevant.
6. Do not close or mark fixed until an independent verifier runs the required checks.

## Non-Actions In This Worker Task

- Did not write to `tests/aistock_validation/bugs/`.
- Did not allocate global numeric bug IDs.
- Did not modify `finding_store.py`, routers, migrations, tests, frontend, backend, or registry JSON.
- Did not start services, run DB checks, touch production `8001`/`3000`, commit, or push.
