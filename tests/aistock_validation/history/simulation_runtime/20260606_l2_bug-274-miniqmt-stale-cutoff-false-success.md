# BUG-274 MiniQMT L2 stale PIT cutoff false success validation

- generated_at: 2026-06-05T18:34:16Z
- module: simulation_runtime
- severity: P0
- github_issue: https://github.com/licong01-cloud/AIstock/issues/788
- github_pr: https://github.com/licong01-cloud/AIstock/pull/791
- worktree: F:/Dev/AIstock_worktrees/BUG-274-miniqmt-l2-daily-selection-evidence-reuses-stale-20260606
- branch: bug/BUG-274-miniqmt-l2-daily-selection-evidence-reuses-stale-20260606
- fix_commit: 555c045b959f632e4cac04b4adfbb90f24c2e978

## 1. Finding

The 2026-06-05 MiniQMT L2 run for `codex_final_ms_l2_20260603` was marked `SUCCEEDED/no_rebalance_required` because daily StrategyPackage selection reused stale point-in-time evidence instead of using the previous trading day's cutoff.

- evidence_id: `dse_2eeb7b60dda279b5`
- target_trade_date: `2026-06-05`
- actual cutoff_date: `2026-06-02`
- expected previous-trading-day cutoff: `2026-06-04`
- execution_plan_id: `plan_544c81e6c2e963b6`
- run_id: `simrun_eb5e3458eaff1870`
- plan intent_count: `0`

This is P0 because repeated reuse of the same stale cutoff can keep daily targets equal to the old holdings forever, suppress future rebalance intents, and still report success.

## 2. Why it was judged successful

Old behavior combined two independent rules:

1. The scheduler accepted/reused existing plans after checking target date/package/release identity, but it did not verify that `DailySelectionEvidence.cutoff_date` matched the previous trading day for the target date.
2. The lifecycle intentionally treats `plan.intents == []` as valid `SUCCEEDED/no_rebalance_required` because a genuinely unchanged target portfolio should not call the broker.
3. The stale cutoff produced old targets that matched current MiniQMT strategy-slot holdings, so zero intents were incorrectly interpreted as a valid no-rebalance day.

The fix makes daily PIT selection roll the cutoff each target date unless fixed/pinned historical replay is explicitly configured. Existing stale evidence is rejected before plan reuse or no-rebalance success.

## 3. Code changes and design compliance

| Requirement | Implementation | Evidence | Status |
|---|---|---|---|
| Daily PIT cutoff must not stay pinned by stale release config | `backend/services/simulation_runtime/selection.py` adds `selection_pit_mode` and `is_fixed_cutoff_replay_config`; non-fixed replay overrides stale cutoff and records `cutoff_override_reason` | `test_strategy_package_selection_service_rolls_stale_release_cutoff_for_daily_pit` | passed |
| Existing execution plan reuse must validate selection evidence freshness | `backend/services/simulation_runtime/scheduler.py` validates evidence before `NO_REBALANCE` or `REUSED_EXISTING_PLAN` | `test_scheduler_rejects_stale_pit_cutoff_selection_evidence_for_trade_date` | passed |
| Stale cutoff must fail fast with expected/actual context | `scheduler._validate_fresh_daily_selection_evidence` includes `cutoff_date` and `expected_cutoff_date` in error context | targeted pytest | passed |
| Successful retry must not keep old failure payload | `repository.update_simulation_daily_run` supports `payload_unset`; lifecycle/scheduler clear `submit_failure` on success | `test_lifecycle_successful_localsim_retry_clears_submit_failure` | passed |
| No schema or dependency drift | No migration/dependency files changed | production gates | passed |

## 4. Similar false-success scan

Read-only DB scan scope: `trade_date >= 2026-05-01`. No production DB writes were executed.

### 4.1 Stale PIT terminal success rows

The scan found 6 historical terminal or terminal-like runs bound to stale daily PIT evidence:

- `simrun_eb5e3458eaff1870`, MiniQMT L2, 2026-06-05, intent_count=0, cutoff `2026-06-02`, expected `2026-06-04`
- `simrun_742881a4be96bdc8`, MiniQMT L16, 2026-06-05, intent_count=1, cutoff `2026-06-02`, expected `2026-06-04`
- `simrun_2eaf9b90cdb847d9`, LocalSim, 2026-06-05, intent_count=8, cutoff `2026-06-02`, expected `2026-06-04`
- `simrun_eed93b457d168c00`, LocalSim, 2026-06-05, intent_count=39, cutoff `2026-06-02`, expected `2026-06-04`
- `simrun_e22704ae4492c70a`, MiniQMT L2, 2026-06-04, intent_count=8, cutoff `2026-06-02`, expected `2026-06-03`
- `simrun_eeeb4b11faa9689e`, MiniQMT L16, 2026-06-04, intent_count=50, cutoff `2026-06-02`, expected `2026-06-03`

Future scheduler runs will reject stale evidence instead of returning `NO_REBALANCE` or `REUSED_EXISTING_PLAN`. Historical rows were not mutated by this PR.

### 4.2 SUCCEEDED rows retaining submit_failure

The scan found 2 historical LocalSim rows with `status=SUCCEEDED` but stale `submit_failure` in payload:

- `simrun_eed93b457d168c00`, submitted_intents=39, last_stage=SUCCEEDED
- `simrun_2eaf9b90cdb847d9`, submitted_intents=8, last_stage=SUCCEEDED

The fix clears `submit_failure` on successful no-rebalance, LocalSim success, MiniQMT success, reconciliation success, and tail-handling success.

### 4.3 Other checked patterns

No rows were found for these additional false-success patterns:

- `SUCCEEDED` + `intent_count > 0` + `broker_called=false`: 0
- `SUCCEEDED` + `no_rebalance_required=true` + `execution_plan.intent_count > 0`: 0
- `SUCCEEDED` + `failed_intents > 0` or `qmt_batch_result.success=false`: 0 outside the stale `submit_failure` cases above

## 5. Validation commands

- `python -m pytest backend/tests/simulation_runtime/test_target_rebalance_shared.py::test_lifecycle_successful_localsim_retry_clears_submit_failure -q` -> 1 passed
- `python -m pytest backend/tests/simulation_runtime/test_strategy_package_selection_service.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py backend/tests/simulation_runtime/test_target_rebalance_shared.py -q` -> 56 passed
- `ruff check backend/services/simulation_runtime/selection.py backend/services/simulation_runtime/scheduler.py backend/services/simulation_runtime/lifecycle.py backend/services/simulation_runtime/repository.py backend/tests/simulation_runtime/test_strategy_package_selection_service.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py backend/tests/simulation_runtime/test_target_rebalance_shared.py` -> All checks passed
- `python -m compileall backend/services/simulation_runtime/selection.py backend/services/simulation_runtime/scheduler.py backend/services/simulation_runtime/lifecycle.py backend/services/simulation_runtime/repository.py` -> passed
- `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` -> files=8, findings=0, blocking=0 before commit
- post-rebase `python scripts/aistock_guardrail_scan.py <origin/main...HEAD changed files> --fail-on-severity P1` -> files=10, findings=4 P2-only, blocking=0
- `python -m nox -s validation_module_registry_l0` -> 8 passed; mapped=12 unmapped=0 ambiguous=0
- `python -m nox -s l0` -> successful; guardrail blocking=0
- `python -m nox -s validation_center_backend` -> 389 passed; coverage line=80.07 branch=62.3 status=passed
- `python scripts/aistock_issue_workflow.py finish --bug-id BUG-274 ...` -> workflow_gate=ready_for_pr; required verification passed

## 6. Production gates

- production_ddl_gate: noop. No migration, schema, index, or comment changes.
- production_frontend_dependency_gate: noop. No frontend or package dependency changes.
- production_backend_dependency_gate: noop. No Python/Conda dependency changes.
- production_runtime_touched: no. No backend/frontend/TDX restart, no process kill, no live scheduler tick, no MiniQMT order submit.
- production_db_write: no. DB checks were read-only; historical stale rows were not changed.

## 7. Handoff

PR #791 is open. The GitHub issue #788 is synced to `status:fixed-pending-review`. After merge, production runtime still needs a user-performed backend restart before the fix is active in the live scheduler. If historical 2026-06-04/2026-06-05 run rows need state correction, register a separate data-repair/audit issue rather than mixing DB mutation into this code fix PR.
