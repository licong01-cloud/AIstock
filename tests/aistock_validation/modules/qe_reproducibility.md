# QE Reproducibility Validation Matrix

Date: 2026-05-09
Status: Phase 4 first-round unit gate implemented; L4 business gate not yet executed.
Related design: `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` sections 7, A.5.2, and B.5.1.

## 1. Scope

This module covers Phase 4 Master Seed Contract governance for future QE/StrategyPackage runs.

- Master seed must be explicit for non-legacy policies.
- Child seeds for Python, NumPy, Torch CPU/CUDA, LightGBM, XGBoost, CatBoost, and dataloader workers must be deterministically derived from the same `master_seed`.
- Missing or illegal seeds must fail fast; no silent fallback to process randomness is allowed.
- Historical artifacts without reliable seed evidence are represented only as `seed_policy=unset_legacy`, `master_seed=null`, and `reproducibility_level=audit_only`.
- New DB draft fields and tables must be additive-only, live outside `public`, and include PostgreSQL comments for each new table/column.

## 2. L0 Static And Schema Gates

| Case | Expected result |
| --- | --- |
| QE-REPRO-L0-001 | `git status --short --branch` shows a dedicated Phase 4 branch/worktree, not dirty `main`. |
| QE-REPRO-L0-002 | `git diff --check` has no whitespace errors. |
| QE-REPRO-L0-003 | Phase 4 migration draft contains no `DROP COLUMN`, no `ALTER COLUMN TYPE`, and no new `public` schema objects. |
| QE-REPRO-L0-004 | Every new table has `COMMENT ON TABLE`; every new or altered column has `COMMENT ON COLUMN`. |
| QE-REPRO-L0-005 | No production 8001 restart/kill/reload command is used. |
| QE-REPRO-L0-006 | No production DB write or migration execution is performed. |

## 3. L1 Unit Gates

| Case | Scenario | Expected result |
| --- | --- | --- |
| QE-REPRO-L1-001 | Build contract twice with same `master_seed` and `seed_policy=fixed`. | All derived child seeds and manifest payload fields are byte-for-byte identical. |
| QE-REPRO-L1-002 | Build contract with a different `master_seed`. | At least one derived child seed changes; the seed namespace is not constant. |
| QE-REPRO-L1-003 | Build `seed_policy=unset_legacy`, `master_seed=null`. | Contract records `audit_only`; runtime seed kwargs raise fail-fast error. |
| QE-REPRO-L1-004 | Missing, boolean, negative, out-of-range, or conflicting fixed seed input. | `SeedContractError` is raised before any runtime fallback. |
| QE-REPRO-L1-005 | Multi-seed sequence with worker seed derivation. | Sequence is preserved, worker seed is deterministic, invalid worker IDs fail fast. |
| QE-REPRO-L1-006 | Parse manifest-like payload without `seed_policy`. | Fails fast because seed semantics are not explicit. |
| QE-REPRO-L1-007 | Inspect Phase 4 DDL draft. | New strategy_pkg table/columns have full comments and no protected schema violations. |

## 4. L4 Core Gate - Not Yet Executed

Required Phase 4 acceptance gate from the architecture document:

- Same manifest + same `master_seed` must run QE train + backtest twice.
- Daily NAV absolute difference must be less than 0.01bp, including average and maximum checks.
- Holdings must be 100% identical for every rebalance day: symbols and weights/quantities match.
- Trades must be 100% identical: symbol, direction, quantity, and execution date match.

Current status: not executed in this first-round implementation. This branch only provides the code-level seed contract, additive DDL draft, unit tests, and validation record needed before wiring full QE L4 reproducibility runs.

## 5. Evidence Requirements

Each Phase 4 run record should include:

- Branch and commit.
- Changed files.
- Test commands and results.
- Whether production 8001 was touched.
- Whether protected assets were touched.
- Whether any DB write or migration execution happened.
- Seed contract payload summary, excluding secrets.
- NAV/holding/trade comparison evidence when L4 is eventually executed.
- Residual nondeterministic flags and follow-up owner.
