# [BASELINE v2] Stage 6 on R6 merge @26261905 — fix-round 4 (paper_v2_backend fixtures)

> **from**: paper-v2 team Lead (cross-test teammate)
> **date**: 2026-05-12
> **target**: 26261905 (R6 merge); main HEAD now b0b2233 (cherry-pick baseline doc)
> **prior baseline**: 372d0f3 (YELLOW 18G/1F/12SKIP/0NOX-SKIP)
> **branch**: claude/paper-v2-baseline-post-r6-20260512 (continued)
> **purpose**: fix 4 enable_paper invariant test fixtures to reach R6 governance gate semantics

## §0 Verdict

**GREEN**

9:30 LocalSim cutover GO/NO-GO: **GO**

Delta vs 372d0f3:
- 4 test files updated: 2 in-memory invariant tests + 2 DB-backed compat tests
- 4 originally-failing tests now: **PASS** (paper_v2_backend now 371 passed / 1 skipped / 2 xfailed)
- Other spot-check sessions: **unchanged GREEN** (paper_v2_data_quality keeps its prior WARN on legacy ledger inconsistency — not caused by this change)

## §1 Fix details

### §1.1 Root cause recap
R6's `_require_governance_paper_ready` gate (`backend/services/strategy_package/service.py:786-793`) is invoked from `transition_status` at line 353 — **before** `repository.transition_status` (which holds the legacy state-machine compare-and-set and the manifest-validator-driven raise points).

Pre-R6, the 4 tests reached their legacy raise points (`InvalidStateTransitionError` / `StrategyPackageValidationError(asset_checks|manifest_sha256)`) directly. Post-R6, the governance gate intercepts and:

- For invalid-state tests: the gate fires `StrategyPackageValidationError("governance eligibility must be paper_ready…")` because no prereqs are seeded, never reaching the state-machine.
- For validator-failure tests (asset_checks / manifest sha mismatch): the gate's `_manifest_identity_gate` (lines 795-820) **catches** the validator's `StrategyPackageValidationError` and surfaces it as a blocker string inside the eligibility dict, then re-raises as the governance-wrapped error. The original error's `context["expected"]` / `context["actual"]` keys move under `context["manifest_identity"]["blockers"]` (as stringified messages).

### §1.2 Fix approach
**Strategy (b) — import helper:** the canonical seed helper `_seed_paper_ready_package` lives at module-level in `backend/tests/strategy_package/test_enable_paper_router_409.py:65` and is imported by all 4 failing tests. No new conftest introduced. No production code modified.

For the 2 tests whose raise points the governance gate now wraps (sha256 mismatch + asset_check failure), assertions were updated to inspect the wrapped governance error shape (`context["manifest_identity"]["blockers"]` / `context["manifest_identity"]["manifest_sha256"]`) rather than the pre-R6 unwrapped validator context. Audit-grade observability is preserved.

For the 2 invalid-state tests, the seed call alone is sufficient — the legacy `InvalidStateTransitionError` is restored as the operative raise once the governance gate is satisfied.

### §1.3 Files modified
| File | Lines added/changed | Strategy |
|---|---|---|
| `backend/tests/strategy_package/test_enable_paper_invariants.py` | +24 / -8 | import helper; seed prereqs; rewrap sha256-mismatch assertion against governance context |
| `backend/tests/paper_trading_v2/test_runtime_enable_paper_compat.py` | +30 / -3 | import helper; seed prereqs in both tests; rewrap asset-check assertion; extend dev-DB teardown to cover `package_asset` / `package_runtime_variant` / `package_validation_run` rows tagged `pkg_test_int6_%` |

### §1.4 Production logic untouched
- `backend/services/strategy_package/service.py` NOT modified (governance gate is correct).
- All other production paths NOT modified.
- Repository, router, validator, manifest, errors — all UNCHANGED.

## §2 Re-run results

### §2.1 paper_v2_backend session
- **Pre-fix (372d0f3)**: 4 fail (the 4 tests under §1)
- **Post-fix**: `371 passed, 1 skipped, 2 xfailed in 22.77s` (`nox -s paper_v2_backend` → `Session paper_v2_backend was successful in 26 seconds`)

### §2.2 Spot-check other sessions
| Session | 372d0f3 | post-fix |
|---|---|---|
| l0 | GREEN | GREEN (76 files, 6 findings/0 blocking) |
| paper_v2_data_quality | YELLOW (WARN legacy ledger) | YELLOW (same WARN, pre-existing) |
| validation_module_registry_l0 | GREEN | GREEN (8 passed; 12/12 mapped) |
| qe_archive_backend | GREEN | GREEN (70 passed) |
| model_registry_backend | GREEN | GREEN (37 passed) |
| data_quality_deep | GREEN | GREEN (10 passed, 21 skipped) |
| dr_validate | GREEN | GREEN (9 passed, 2 skipped) |

No new regressions. The legacy `paper_v2_ledger_consistency` WARN (3 order/fill quantity mismatches in dev DB) pre-dates 372d0f3 and is unrelated to this fix.

## §3 Updated 9:30 cutover gate assessment

| Gate | 372d0f3 | post-fix |
|---|---|---|
| R6 spotlights (6/6) | ✅ | ✅ |
| paper_v2_backend | ❌ (4 fixtures) | ✅ |
| stk_limit Tushare lag | ⚠️ | ⚠️ (unchanged, external) |
| origin/main push 26261905 | ⏳ | ⏳ (user-owned) |

## §4 Boundary confirmations
- prod_code_modified: **false** (only test files + new baseline doc)
- prod_db_touched: **false**
- prod_8001_touched: **false**
- no_service_started: **true**
- dev_db_writes: **test-scoped only** (`pkg_test_int6_%` prefix; teardown extended to drop `package_asset` / `package_runtime_variant` / `package_validation_run` rows in addition to existing `package_status_event` / `package`)
- frontend/tsconfig.tsbuildinfo: **NOT staged**
- No merge of `main`. No force push. No amend.
