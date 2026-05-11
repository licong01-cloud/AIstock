# [BRANCH-BASELINE] Stage 6 on codex/qe-governance @c2352a9 (R6 merge gate)

> **from**: paper-v2 team Lead (cross-test teammate)
> **date**: 2026-05-11
> **target**: codex/qe-governance-integration-20260509@c2352a9 "feat(qe): add paper v2 cold-start sanity gate"
> **main HEAD reference**: 01dfb40 (R5 merge tip + handoffs; codex branch is what gets merged to main as R6)
> **prior baseline on main**: e8ffbdd (Stage 6 baseline post-R5 v2, 16G/0F/14SKIP/1NOX-SKIP)
> **purpose**: R6 merge gate — verify codex branch ready to merge into main
> **branch (new)**: claude/paper-v2-branch-baseline-codex-qe-20260511
> **worktree (new)**: F:/Dev/AIstock-worktrees/branch-baseline-codex-qe/

## §0 Verdict

**YELLOW**

R6 merge readiness: **GO-WITH-CAVEATS**

Delta vs main e8ffbdd:
- Retained GREEN: 11 (l0, paper_v2_backend, local_data_management_audit, qe_read_backend,
  qe_archive_backend, validation_coverage_backend, validation_module_registry_l0,
  validation_center_backend, qe_data_contract_backend, qe_archive_data_quality,
  guardrail_changed_files)
- Newly GREEN (R6-specific): R6 spotlight test files now present (104 tests pass directly invoked)
- Regressions vs main e8ffbdd baseline: 1 (paper_v2_data_quality data freshness FAIL — operational, not code)
- Sessions matrix differs structurally from main e8ffbdd: codex branch noxfile.py defines a
  different session set (e.g. no `data_quality_deep`, `dr_validate`, `model_registry_backend`,
  `market_regime_label`, `rl_execution_smoke` sessions; these are main-only). Same-name
  backend sessions retain GREEN parity.

R6 in-branch spotlights:
- 6 migrations DDL: **verified ✓** (all 6 SQL files present in `backend/migrations/`,
  totals 741 lines DDL)
- 2 dev backfill scripts (b976c23/75470f5) + tests: **verified ✓** (17 tests pass — 8 + 9 split)
- 2 prod executors (2fb81b3 + 2866f66) + 57 tests: **verified ✓** (33 + 24 = 57 pass)
- Coldstart sanity (c2352a9) + 30 tests: **verified ✓** (30 pass, matches Codex claim)
- Runbook §1-§10: **verified ✓** (sections §0-§15 all present; §1 Roles, §2 Inputs,
  §3 Time Budget, §4 Preflight, §5 DR Snapshot, §6 Six Governance Migrations Apply,
  §7 Evidence Backfill Apply Order, §8 R6 Git Merge/Sync, §9 Backend 8001 Restart,
  §10 Cold-start Sanity Checks)

R6 spotlight aggregate: **104 tests total, 104 pass, 0 fail**.

## §1 Session run matrix

| # | Session | Status | Pytest summary | Duration | Notes |
|---|---|---|---|---|---|
| 1 | l0 | GREEN (retry) | guardrail scan: files=59, findings=6, blocking=0 | a few s | First attempt aborted on missing `tmp/validation/guardrails/baseline_20260504.json`; baseline copied from prod root (gitignored, env-artifact only) and retry succeeded. No code modified. |
| 2 | paper_v2_backend | GREEN | 244 passed | 20s | Includes paper_trading_v2 + selection_center + strategy_package. R5+R6 spotlight session. |
| 3 | paper_v2_data_quality | FAIL | data freshness gate exit 1 | < 1m | `stk_limit` dataset stale: latest_success=2026-05-08, min_required_date=2026-05-11. Operational/data freshness, not code. Also WARN on `paper_v2_ledger_consistency` (3 legacy order_fill_quantity_mismatches; pre-existing). |
| 4 | local_data_management_audit | GREEN | 3 passed (+schema smoke PASS) | 1s | |
| 5 | qe_read_backend | GREEN | 11 passed | 12s | |
| 6 | qe_archive_backend | GREEN | 46 passed | 12s | R4 T14a spotlight retained. |
| 7 | validation_coverage_backend | GREEN | 10 passed; coverage line=81.57 branch=68.55 | 1s | |
| 8 | validation_module_registry_l0 | GREEN | 8 passed; mapped=12 unmapped=0 | 2s | |
| 9 | validation_center_backend | GREEN | 52 passed; coverage line=82.21 branch=65.94 | 35s | |
| 10 | qe_data_contract_backend | GREEN | 17 passed | 2s | |
| 11 | qe_archive_data_quality | GREEN | smoke pass (550 pending outbox = informational WARN) | < 1m | |
| 12 | guardrail_changed_files | GREEN | files=0, findings=0, blocking=0 | < 1s | Empty staging area (clean tree). |

### R6 spotlight tests (direct invocation outside nox)

| # | Test file | Tests | Status |
|---|---|---|---|
| A | `backend/tests/scripts/test_paper_v2_coldstart_sanity.py` | 30 | PASS |
| B | `backend/tests/scripts/test_protected_asset_ledger_backfill_prod_executor.py` | 33 | PASS |
| C | `backend/tests/scripts/test_strategy_package_governance_evidence_backfill_prod_executor.py` | 24 | PASS |
| D | `backend/tests/scripts/test_protected_asset_ledger_backfill.py` (dev) | 8 | PASS |
| E | `backend/tests/scripts/test_strategy_package_evidence_backfill.py` (dev) | 9 | PASS |

Aggregate R6: 104 / 104 pass.

## §2 Skipped sessions (service-policy)

Per spec, all UI/service-starting/live sessions skipped. Codex branch session inventory:

| Session | Reason |
|---|---|
| `paper_v2_ui` | UI E2E (frontend 3012) |
| `paper_v2_l3` | UI stage (skip per spec row 4) |
| `qe_read_ui` | UI E2E |
| `qe_read_l3` | UI stage |
| `validation_center_ui` | UI E2E (mocked APIs but still starts dev frontend) |
| `validation_center_real_port_ui` | UI E2E against real dev backend+frontend |
| `validation_center_live_readonly` | Probes running dev backend |
| `validation_center_runner_smoke` | Starts allowlisted runner job on dev backend |
| `qe_archive_ui` | UI E2E |
| `qe_archive_l3` | UI stage |
| `paper_v2_live` | Catch-up-to-live validation; touches dev backend + TDX |

Total service-policy SKIP: **11**.

## §3 Failed sessions

### paper_v2_data_quality — FAIL (operational, not code)

Verbatim failure (excerpt):

```
FAIL dataset_refresh_audit: required datasets are not fresh enough for Paper v2/Selection smoke
  failures=[{
    "dataset": "stk_limit",
    "latest_success": "2026-05-08",
    "min_required_date": "2026-05-11",
    "refreshed_at": "2026-05-08T12:19:57.360044+08:00",
    "row_count_at_latest": 7580,
    ...
  }]
```

All other rows in the dataset_refresh_audit are fresh; only `stk_limit` is stale (1 trading day
behind). Other gates in the same smoke (`schema_required_tables`, `trading_calendar_latest`,
`strategy_package_readiness`, `selection_result_traceability`, `paper_v2_run_traceability`) PASS.

`paper_v2_ledger_consistency` is WARN (3 legacy order_fill_quantity_mismatches, pre-existing on
all prior baselines).

**Cause**: dev DB `stk_limit` refresh job has not advanced beyond 2026-05-08. Same
infrastructural cause regardless of which branch is under test; would be reproduced on main HEAD
01dfb40 at this moment. Not a regression introduced by R6 commits.

## §4 Aggregate stats

| Category | Count |
|---|---|
| GREEN | 11 |
| FAILED | 1 (paper_v2_data_quality — data freshness, not code) |
| SKIPPED (service-policy) | 11 |
| NOX-internal SKIP | 0 |
| MISSING (vs main e8ffbdd inventory) | 5 (data_quality_deep, dr_validate, model_registry_backend, market_regime_label, rl_execution_smoke — codex branch does not declare these) |

Direct R6 spotlight invocation (outside nox): **104 / 104 pass**.

## §5 Delta vs main e8ffbdd

| Session | main e8ffbdd | codex c2352a9 | Delta |
|---|---|---|---|
| l0 | GREEN | GREEN | parity |
| paper_v2_backend | GREEN | GREEN (244) | parity / R6 tests embedded |
| paper_v2_data_quality | GREEN | FAIL (stk_limit stale) | data-freshness regression (operational) |
| paper_v2_l3 | SKIP (UI) | SKIP (UI) | parity |
| validation_module_registry_l0 | GREEN | GREEN | parity |
| data_quality_deep | GREEN | N/A | MISSING — session not declared on codex branch noxfile.py |
| dr_validate | GREEN | N/A | MISSING |
| qe_archive_backend | GREEN | GREEN (46) | parity |
| model_registry_backend | NOX-SKIP (gated pending this very branch) | N/A | MISSING — session not declared on codex branch noxfile.py |
| qe_archive_data_quality | GREEN | GREEN | parity |
| guardrail_changed_files | GREEN | GREEN | parity |
| market_regime_label | GREEN | N/A | MISSING |
| rl_execution_smoke | GREEN | N/A | MISSING |
| qe_read_backend | (not in e8ffbdd matrix; new) | GREEN (11) | NEW GREEN |
| local_data_management_audit | (not in e8ffbdd matrix; new) | GREEN (3) | NEW GREEN |
| validation_coverage_backend | (not in e8ffbdd matrix; new) | GREEN | NEW GREEN |
| validation_center_backend | (not in e8ffbdd matrix; new) | GREEN (52) | NEW GREEN |
| qe_data_contract_backend | (not in e8ffbdd matrix; new) | GREEN (17) | NEW GREEN |

**Important caveat on model_registry_backend flip**: The spec anticipated this session would
flip NOX-SKIP→GREEN on the codex branch (since codex is the merge target). However the codex
branch noxfile.py does **not declare** a `model_registry_backend` session at all. The
relevant migration `model_registry_phase5_20260509.sql` (374 lines) is present in
`backend/migrations/`, but there is no nox session named `model_registry_backend` to flip on
this branch. Likely interpretation: model_registry tests are reached via a session that exists
on main but not yet on codex (because the noxfile changes that would add it post-merge happen
on main after R5). This will resolve naturally when R6 merges to main.

## §6 R6 merge readiness assessment

**Verdict: GO-WITH-CAVEATS**

Positives:
- All 6 governance migration DDL files present and reviewed-shape (52..374 lines each)
- 2 dev backfill scripts present with 17 unit tests passing (8 + 9)
- 2 prod executors present with 57 unit tests passing (33 + 24, matches spec exactly)
- Coldstart sanity script + 30 tests passing (matches Codex claim exactly)
- Runbook §0-§15 fully populated (more than required §1-§10), including:
  - DR Snapshot/Restore Point (§5)
  - Six-Migrations Apply (§6) — references each of the 6 SQL files by exact filename
  - Evidence Backfill Apply Order (§7)
  - 8001 daemon enable/restart (§9)
  - Cold-start sanity hooks (§10) — directly invokes `scripts/paper_v2_coldstart_sanity.py`
  - Rollback matrix (§13)
- All shared backend sessions retain GREEN parity with main e8ffbdd (11 GREEN, 0 regression
  in code paths)

Caveats:
1. `paper_v2_data_quality` FAIL is operational (stk_limit dev DB freshness), not introduced by
   R6 code. Should be re-checked after dev DB stk_limit refresh job catches up to 2026-05-11.
2. Main-only sessions (`data_quality_deep`, `dr_validate`, `model_registry_backend`,
   `market_regime_label`, `rl_execution_smoke`) are not declared on the codex branch
   noxfile.py and therefore could not be verified pre-merge from this worktree. They must be
   re-validated on a post-merge main commit (or by merging codex→main locally in a side
   worktree) before final R6 cutover.
3. `paper_v2_ledger_consistency` WARN (3 legacy order_fill_quantity_mismatches) is pre-existing
   on prior baselines; informational, does not block.
4. Production prod-executor guards are dev-locked by design (per runbook §0 caveat): production
   apply requires the strategy/user-approved production executor path, not a flag override on
   the dev backfill scripts.

The R6 in-branch deliverables required by spec (6 mig + 2 dev + 2 prod + coldstart + runbook)
are all present, complete, and passing tests. The merge gate is GO from the code/test side; the
operational stk_limit freshness must be cleared as a pre-cutover checklist item, not a code
fix.

## §7 Boundary confirmations

- target_HEAD: c2352a9 ✓ (verified via `git rev-parse HEAD`)
- prod_db_writes: false ✓
- prod_8001_touched: false ✓
- no_service_started: true ✓ (no UI/service sessions run; dev backend/frontend not started)
- code_modified: false ✓ (only `tmp/validation/guardrails/baseline_20260504.json` artifact
  copied from prod root; gitignored, not tracked, not a code change)
- .env loaded (gitignored): true ✓ (10 `^TDX_DB` keys present; `git status .env` reports clean)
- new worktree: F:/Dev/AIstock-worktrees/branch-baseline-codex-qe/ ✓
- new branch: claude/paper-v2-branch-baseline-codex-qe-20260511 ✓
- main_merged: false ✓
- existing worktrees untouched ✓
- frontend/tsconfig.tsbuildinfo NOT staged ✓ (working tree clean; only the new baseline doc to
  be staged in commit step)
