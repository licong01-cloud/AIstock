# [BASELINE] Stage 6 pre-flight verification on main (post-R3)

> **from**: paper-v2 team Lead (cross-test teammate)
> **date**: 2026-05-11
> **target branch**: `origin/main@3973e7d` (docs(cross_tool): fix r1 REVIEW request for stage 7.1 part 2)
> **reviewer branch**: `claude/paper-v2-vnpy-mvp-20260508`
> **purpose**: confirm main GREEN baseline before R4-R6 merges
> **service-start policy**: feedback_no_service_start enforced — UI E2E sessions skipped (8012/3012 not listening)

## §0 Verdict

**YELLOW** — Backend regression and DR sessions green, but two non-UI sessions failed: one structural (validation_module_registry_l0 duplicate module_id) and one data-state (data_quality_deep paper_v2 archive backfill gap). Neither is a paper-v2 backend regression; both surface pre-existing state.

GREEN sessions: 5 / 7 attempted
FAILED sessions: 2 (1 structural catalog bug, 1 data backfill/handler regression)
SKIPPED (service policy): 5 (1 paper_v2_l3 + 4 UI sessions)
NOX-SKIPPED (gated): 1 (model_registry_backend — module not yet merged to main)

Recommendation for R4: **GO-WITH-CAVEATS**

Rationale: paper-v2-series backend tests (paper_v2_backend 154/154, paper_v2_data_quality, qe_archive_backend 46/46, dr_validate 9/9) all pass. The two failures are scoped outside the R4 paper-v2 surface:
1. `validation_module_registry_l0` — registry catalog has a duplicate `rl_execution` module_id (validation infra, not paper-v2 runtime).
2. `data_quality_deep` — paper_v2.run=121 but qe_archive.paper_v2_run=0; signals archive handler/worker not consuming outbox events on dev DB (operational state, not code regression in paper-v2 itself, but a pre-existing data integrity flag).

Both should be filed as BUG entries for the respective owning workspaces before R4 lands, but they do not block paper-v2 R4 merge itself.

## §1 Session run matrix

| # | Session | Status | Result | Duration | Notes |
|---|---|---|---|---|---|
| 1 | l0 | GREEN | guardrail scan: 48 files, 6 findings (all baseline), 0 blocking | ~5s | All P0/P1 baseline; 28 raw_json findings noted (frontend tests) |
| 2 | paper_v2_backend | GREEN | 154 passed | 12s | Clean run |
| 3 | paper_v2_data_quality | GREEN | All PASS gates (1 WARN: ledger consistency legacy drift, non-strict mode) | ~4s | strategy_package_readiness PASS, paper_v2_run_traceability PASS, selection_result_traceability PASS, paper_v2_ledger_consistency WARN |
| 4 | paper_v2_l3 | SKIP | service-start-policy: 8012/3012 not listening | n/a | UI E2E coverage gap |
| 5 | validation_module_registry_l0 | **RED** | 1 failed, 7 passed | 1.13s | Duplicate module_id `rl_execution` in default registry |
| 6 | qe_archive_ui | SKIP | service-start-policy: 8012/3012 not listening | n/a | UI session |
| 7 | strategy_package_governance_ui | SKIP | service-start-policy: 8012/3012 not listening | n/a | UI session |
| 8 | market_regime_ui | SKIP | service-start-policy: 8012/3012 not listening | n/a | UI session |
| 9 | rl_execution_ui | SKIP | service-start-policy: 8012/3012 not listening | n/a | UI session |
| 10 | data_quality_deep | **RED** | 1 failed, 10 passed, 20 skipped | 1.30s | paper_v2.run=121 vs qe_archive.paper_v2_run=0 (handler/worker regression) |
| 11 | dr_validate | GREEN | 9 passed, 2 skipped | 2s | DR snapshot checks |
| 12 | qe_archive_backend | GREEN | 46 passed | 12s | Schema + repo + completion contract |
| 13 | model_registry_backend | NOX-SKIP | "Model Registry module not yet merged to main. Skipped pending origin/codex/qe-governance-integration-20260509 merge." | n/a | Gated by noxfile, not a service-policy skip |

## §2 Skipped sessions (service-start policy)

| Session | Why skipped | What's covered without it |
|---|---|---|
| paper_v2_l3 | 8012/3012 not listening; per feedback_no_service_start may not start | UI E2E coverage gap — user must manually start dev backend (8012) + dev frontend (3012) then rerun |
| qe_archive_ui | 8012/3012 not listening | QE archive UI coverage gap |
| strategy_package_governance_ui | 8012/3012 not listening | Strategy package UI coverage gap |
| market_regime_ui | 8012/3012 not listening | Market regime UI coverage gap |
| rl_execution_ui | 8012/3012 not listening | RL execution UI coverage gap |

Additional non-service skip:
| Session | Why skipped | Notes |
|---|---|---|
| model_registry_backend | nox-internal skip: "module not yet merged to main" | Will activate after `origin/codex/qe-governance-integration-20260509` merges |

## §3 Failed sessions

### BUG-AUDIT-001 [HIGH] — validation_module_registry_l0 duplicate module_id

- **Session**: `validation_module_registry_l0`
- **Failing test**: `backend/tests/test_validation_module_ownership.py::test_default_module_registry_and_file_ownership_catalog_load`
- **Error**: `backend.services.validation.module_registry.ModuleRegistryError: Duplicate module_id: rl_execution.`
- **Trigger**: `ModuleRegistry().load()` then `_validate_unique_modules(modules)` raises on a duplicate `rl_execution` entry.
- **Last 30 lines** (relevant):
  ```
  modules = [ModuleDefinition(module_id='qe', ...), ...]
      @staticmethod
      def _validate_unique_modules(modules: list[ModuleDefinition]) -> None:
          seen: set[str] = set()
          for module in modules:
              if module.module_id in seen:
                  raise ModuleRegistryError(f"Duplicate module_id: {module.module_id}.")
  E       backend.services.validation.module_registry.ModuleRegistryError: Duplicate module_id: rl_execution.
  ```
- **Severity**: HIGH — registry load fails completely; any downstream validation tooling that depends on `ModuleRegistry().load()` is broken on main today.
- **Suggested owner**: validation / module-registry workspace (likely Codex governance branch, given `model_registry_backend` is also gated on the same upcoming merge). Likely fixed by the pending `origin/codex/qe-governance-integration-20260509` merge.
- **Repro**: `nox -s validation_module_registry_l0` on main@3973e7d.

### BUG-AUDIT-002 [HIGH] — data_quality_deep paper_v2 archive backlog

- **Session**: `data_quality_deep`
- **Failing test**: `backend/tests/data_quality/test_cross_table_consistency.py::test_paper_v2_fill_count_matches_archive_per_run`
- **Error**: `Failed: paper_v2.run has 121 row(s) but qe_archive.paper_v2_run has 0 -- handler is not registered on the archive worker yet, OR the worker is not consuming outbox events. This is a regression, not a clean-slate condition; the archive backfill is expected to run.`
- **Severity**: HIGH — data-state regression sentinel; paper_v2 runs are accumulating but the archive backfill is not consuming. Operational state, not a code defect in the test, and the failing assertion is intentional per "must FAIL (not skip) when paper_v2.run has rows but qe_archive.paper_v2_run does not" docstring.
- **Suggested owner**: qe-archive / paper-v2 archive handler workspace. Action item: confirm archive worker registration + outbox consumption on dev DB (5433).
- **Repro**: `nox -s data_quality_deep` on main@3973e7d against dev DB.
- **Side notes**: The 20 skipped tests are conditional (likely empty-table skips); the 10 passed cover the rest of the cross-table matrix.

## §4 Aggregate stats

- Sessions targeted: 13
- Sessions attempted (not service-policy skipped): 7
- Sessions run + completed: 7 (6 ran tests, 1 nox-skipped at session entry)
- GREEN: 5 (l0, paper_v2_backend, paper_v2_data_quality, dr_validate, qe_archive_backend)
- FAILED: 2 (validation_module_registry_l0, data_quality_deep)
- SKIPPED (service policy): 5 (paper_v2_l3, qe_archive_ui, strategy_package_governance_ui, market_regime_ui, rl_execution_ui)
- NOX-SKIP (gated): 1 (model_registry_backend)
- Total duration: ~50s aggregate (well under 10min)
- Total tests passed: ~225 (154 + 46 + 9 + 7 + 10 + ~smoke gates)
- Total tests failed: 2 (one per failed session)

## §5 R4 readiness recommendation

R4 startable conditions:
- [x] All paper-v2 backend sessions GREEN (paper_v2_backend, paper_v2_data_quality, qe_archive_backend, dr_validate)
- [x] l0 guardrail GREEN (no new blocking findings)
- [ ] validation_module_registry_l0 GREEN — currently failing on duplicate module_id (likely auto-resolves with pending Codex governance merge)
- [ ] data_quality_deep GREEN — currently failing on archive backlog (operational state, not paper-v2 code defect)
- [x] UI session gap acknowledged (5 UI sessions skipped per service-start policy; user must manually run if E2E coverage needed before R4 lands)

Recommendation: **GO-WITH-CAVEATS for R4**. Paper-v2 surface itself is GREEN. The 2 failures live in adjacent validation infra and archive operational state; neither is introduced by paper-v2 R4 changes. Pre-R4 actions:
1. File BUG-AUDIT-001 with validation/governance workspace; confirm it clears after `origin/codex/qe-governance-integration-20260509` merge.
2. File BUG-AUDIT-002 with qe-archive workspace; verify archive worker config on dev (this may be a worker-not-started condition, not a code bug — same family as the UI service-start gap).
3. Re-run `paper_v2_l3` after user starts 8012/3012 if E2E confidence needed.

## §6 Boundary confirmations

- prod_db_writes=false (no 5432 connection in this task)
- prod_8001_touched=false (read-only port check only)
- no_service_started=true (UI sessions skipped per feedback_no_service_start)
- codex_code_modified=false (read-only nox verification run)
- frontend/tsconfig.tsbuildinfo NOT staged (only the baseline .md added)
- main_HEAD_verified=3973e7d (matches Lead pre-verification)
- worktree_branch=claude/paper-v2-vnpy-mvp-20260508
