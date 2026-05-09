# QE Governance Phase 6.1 Integration Fixes Run Record

Date: 2026-05-09

Branch: `codex/qe-phase-6-1-integration-fixes-20260509`

Base: `origin/codex/qe-governance-integration-20260509` at `d9ce84ff1e819e7443e18100e1e6306ceb5d522f`

Worktree: `F:\Dev\AIstock_worktrees\qe-phase-6-1-integration-fixes-20260509`

Commit: final pushed commit is recorded in the handoff report for this branch.

## Documents Read

- `docs/codex_project_memory.md`
- `docs/standards/aistock_development_standard_v1.1_20260504.md`
- `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`
- `tests/aistock_validation/modules/qe_governance.md`
- `tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md`

## Fix Scope

1. Kept `strategy_pkg.promotion_review` as a standalone additive migration for existing dev/integration databases and clarified that the baseline must not be rerun just to create the table.
2. Hardened Phase 4 `package_seed_policy_check` and `package_master_seed_range_check` guards with `pg_catalog.pg_constraint` plus `to_regclass('strategy_pkg.package')`.
3. Extended the SOTA Hall leaderboard API and UI to show `qe_evolution_loops.is_sota=true` automatic candidates with promotion-review state, while keeping `qe_sota_registry` rows as the only `approved_sota=true` source.

## Changed Files

- `backend/migrations/strategy_pkg_promotion_review_20260509.sql`
- `backend/migrations/qe_phase4_master_seed_contract_20260509.sql`
- `backend/routers/quantevolver_evolution.py`
- `backend/tests/strategy_package/test_seed_contract.py`
- `backend/tests/strategy_package/test_sota_promotion_review.py`
- `frontend/src/app/quantevolver/evolution/sota/page.tsx`

## Key Files Checked But Not Modified

- `backend/services/strategy_package/promotion_review.py`
- `backend/migrations/trading_core_v2_schema.sql`
- `tests/aistock_validation/modules/qe_governance.md`
- `tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md`

## Validation

| Command | Result |
| --- | --- |
| `python -m py_compile backend/routers/quantevolver_evolution.py backend/services/strategy_package/promotion_review.py backend/services/strategy_package/seed_contract.py backend/tests/strategy_package/test_seed_contract.py backend/tests/strategy_package/test_sota_promotion_review.py` | Pass |
| `python -m pytest backend/tests/strategy_package/test_seed_contract.py backend/tests/strategy_package/test_sota_promotion_review.py -q -p no:cacheprovider` | Pass: 22 passed |
| `python -m pytest backend/tests/strategy_package -q -p no:cacheprovider` | Pass: 67 passed |
| `python -m pytest backend/tests -q -p no:cacheprovider -k "promotion_review or seed_contract or sota or governance"` | Pass: 25 passed, 947 deselected |
| `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` | Pass: 0 blocking findings; non-blocking P2 findings in changed files remain review notes |
| `git diff --check` | Pass |

## Business Validation Result

- Existing dev/integration DBs can apply the standalone `strategy_pkg.promotion_review` migration without rerunning the baseline.
- Re-running the Phase 4 seed migration does not attempt duplicate named CHECK constraints.
- Automatic SOTA candidates from `qe_evolution_loops.is_sota=true` appear in the SOTA Hall review list and can create `REVIEW_PENDING`; approved SOTA semantics remain tied to legacy `qe_sota_registry` rows.

## Isolation

- Production backend `8001`: not touched, not restarted.
- Protected assets: not touched.
- DB writes: none executed by this worker.
- `main`: not touched and not merged.

## Residual Risks

- Frontend build was not run because this worktree does not have `frontend/node_modules`; UI coverage is static/backend contract validation only.
- Guardrail scan reports non-blocking P2 findings in legacy changed files; no P0/P1 blocker was reported.
