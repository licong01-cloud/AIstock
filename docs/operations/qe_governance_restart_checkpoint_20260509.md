# QE Governance Restart Checkpoint - 2026-05-09

This checkpoint preserves the current Codex-side AIstock QE governance progress for restart/recovery.

## Hard Constraints

- Do not merge program code to `main` without explicit user confirmation.
- Continue work in dedicated branches/worktrees, then merge only into `codex/qe-governance-integration-20260509`.
- Do not touch, restart, kill, or reload production backend `8001`.
- Do not write production DB without explicit user confirmation.
- Do not modify `AGENTS.md`.
- Do not touch protected assets: frozen StrategyPackage manifests, model weights, HMM snapshots, QE/RD-Agent artifacts, Paper ledgers, or validated policies.
- Paper v2 runtime / vn.py / trading_core execution-path work is owned by Claude Code, not this Codex governance plan.
- Stop only for real conflicts with Claude Code or unexpected external changes in files being edited.

## Active Branches And Worktrees

- Main repo: `F:\Dev\AIstock`
- Main branch status at checkpoint: `main...origin/main`; not touched by Codex governance work.
- Governance integration worktree: `F:\Dev\AIstock_worktrees\qe-governance-integration-20260509`
- Governance integration branch: `codex/qe-governance-integration-20260509`
- Latest integration code commit before this checkpoint-doc update: `c0ee2af merge(qe): integrate rl_execution visibility fix`

RD-Agent follow-up:

- Worktree: `F:\Dev\RD-Agent_worktrees\qe-rdagent-recorder-api-guard-fixups-20260509`
- Branch: `codex/qe-rdagent-recorder-api-guard-fixups-20260509`
- Commit: `810726a0 fix(qe): harden recorder guard tar extraction`
- Status: pushed; not merged to main.

## Completed AIstock Governance Work

All items below are merged only into `codex/qe-governance-integration-20260509`. Push state must be rechecked before handoff; this checkpoint includes local commits newer than `origin/codex/qe-governance-integration-20260509`.

1. Phase 5 Model Registry foundation
   - Feature commit: `0cdaa60 feat(qe): add model registry foundation`
   - Integration merge: `bfa75c6 merge(qe): phase 5 model registry foundation`

2. Phase 5.1 Model Registry migration smoke
   - Feature commit: `b47bcf0 feat(qe): add model registry migration smoke`
   - Integration merge: `8cbec83 merge(qe): phase 5.1 model registry migration smoke`

3. Phase 5.2 Model Registry bridge read API
   - Feature commit: `21ef17e feat(qe): add model registry bridge read api`
   - Integration merge: `8a7bc18 merge(qe): phase 5.2 model registry bridge read api`

4. Phase 6 Runtime Variants foundation
   - Feature commit: `63bcc31 feat(qe): add strategy package runtime variants`
   - Integration merge: `d9ce84f merge(qe): phase 6 runtime variants foundation`

5. Phase 6.1 Governance integration fixes
   - Feature commit: `e31732a fix(qe): address governance integration review gaps`
   - Integration merge: `a86fe21 merge(qe): phase 6.1 governance integration fixes`
   - Fixed standalone `promotion_review` migration, Phase 4 seed constraint idempotency, and SOTA Hall candidate/review visibility.

6. Phase 7 Validation Modes foundation
   - Feature commit: `f95d31c feat(qe): add strategy package validation runs`
   - Integration merge: `46bcdda merge(qe): phase 7 validation modes foundation`
   - Added `strategy_pkg.package_validation_run` and append-only APIs for original/latest/retrain/walk-forward/runtime-variant validation evidence.

7. Phase 7 Stability Scoring
   - Feature commit: `f593498 feat(qe): add validation stability scoring`
   - Integration merge: `1a17bca merge(qe): phase 7 stability scoring`
   - Added read-only seed/regime stability summaries from validation-run evidence.

8. Phase 3 Paper Retest Gate
   - Feature commit: `8a19b49 fix(qe): require original retest before paper enable`
   - Integration merge: `dcbe0d0 merge(qe): phase 3 paper retest gate`
   - `enable_paper()` now fail-fast requires current manifest to have a passed `original_fixed_weight` validation run.

9. Phase 2 Asset Ledger foundation
   - Feature commit: `a62fe15 feat(qe): add strategy package asset ledger`
   - Integration merge: `714ef8a merge(qe): phase 2 asset ledger foundation`
   - Added `strategy_pkg.package_asset` metadata ledger and APIs. It records metadata only; it does not copy/delete/overwrite protected asset files.

10. Governance smoke tooling
   - Migration smoke feature commit: `5d92a59 test(qe): add governance migration stack smoke`
   - Migration smoke integration merge: `24f1cb7 merge(qe): governance migration smoke`
   - StrategyPackage API smoke feature commit: `de59847 test(qe): add strategy package governance readonly smoke`
   - StrategyPackage API smoke integration merge: `056a18a merge(qe): strategy package governance readonly smoke`
   - DB error hardening feature commit: `069ae8b test(qe): harden governance migration DB smoke errors`
   - DB error hardening integration merge: `5a92ba7 merge(qe): harden governance smoke errors`
   - Production-readonly preflight feature commit: `83a569f test(qe): add production readonly governance preflight`
   - Production-readonly preflight integration merge: `64e90b9 merge(qe): production readonly governance preflight`
   - Added `scripts/governance_migration_smoke.py` for static dry-run validation of the full six-file governance migration stack, with guarded opt-in dev DB transaction mode.
   - Added `scripts/strategy_package_governance_readonly_smoke.py` for GET-only StrategyPackage governance API smoke checks that refuse production port `8001` by default.
   - Hardened DB smoke connection failures so missing local dev DB credentials return structured failure JSON instead of an uncaught traceback.
   - Added guarded `--production-readonly-preflight` mode to `scripts/governance_migration_smoke.py`; it requires `AISTOCK_QE_GOVERNANCE_PROD_READONLY_PREFLIGHT=true` plus confirmation token `QE_GOVERNANCE_PROD_READONLY_PREFLIGHT`, uses SELECT-only catalog introspection, and reports whether production catalog objects/columns/indexes/constraints are missing without DDL or writes.

11. Governance eligibility summary and Paper gate audit
   - Feature commit: `52bd086 feat(qe): add governance eligibility summary`
   - Integration merge: `0dede08 merge(qe): governance eligibility summary`
   - Added read-only `GET /strategy-packages/{package_id}/governance-eligibility`.
   - The endpoint aggregates `paper_ready`, `blockers`, `satisfied_gates`, `manifest_sha256`, current package status, original fixed-weight retest status, validation stability, protected-asset ledger status, and runtime-variant Paper candidate status.
   - Fixed the `enable_paper()` original fixed-weight retest gate to scan all matching validation runs, not only the latest 100, so an older passed original retest is not hidden by later noisy/requested runs.
   - Preserved the governance boundary: `latest_fixed_weight` cannot substitute for `original_fixed_weight`, failed gates do not mutate package status, and the endpoint is read-only.

12. Governance eligibility readonly smoke coverage
   - Feature commit: `6265607 test(qe): add governance eligibility readonly smoke`
   - Integration merge: `1b85473 merge(qe): governance eligibility readonly smoke`
   - Extended `scripts/strategy_package_governance_readonly_smoke.py` to call and validate `GET /strategy-packages/{package_id}/governance-eligibility`.
   - The smoke now checks package/detail manifest hash alignment plus `paper_ready`, `blockers`, `satisfied_gates`, and the nested gate objects for the new eligibility summary.
   - Added contract coverage for both the successful eligibility payload and a malformed summary that must fail fast.

13. Governance eligibility blocker regression coverage
   - Feature commit: `97ec0e9 test(qe): cover governance eligibility blockers`
   - Added explicit `paper_ready=false` regressions for missing protected asset ledger, missing runtime Paper candidate, fragile validation stability, and disallowed package status.
   - This is test-only coverage; no StrategyPackage service/router/runtime behavior changed.
   - Cross-tool D5/T8-A reply was sent to Claude Code via `drawer_cross-tool_codex-claude-coord_9cd6d6bb5c81161be688915e`.

14. `rl_execution` visibility fix integrated for dev-port startup
   - Upstream branch: `origin/fix/rl_execution_module_visibility-20260510`
   - Upstream commits: `da6673c fix(rl_execution): expose backend/services/rl_execution module by narrowing .gitignore + tracking source`, `6275e9d feat(main): graceful fallback for rl_execution router import (defense layer)`
   - Integration merge: `c0ee2af merge(qe): integrate rl_execution visibility fix`
   - Added tracked `backend/services/rl_execution` source files and the upstream defensive `backend.main` router import fallback.
   - This enabled an isolated `8012` backend startup/openapi smoke. It did not resolve missing local dev DB credentials for StrategyPackage live API data endpoints.

## Latest Verified Gates

The latest integration gates after governance eligibility blocker regression coverage:

- `python -m pytest backend\tests\strategy_package\test_governance_eligibility.py -q -p no:cacheprovider` - `6 passed`.
- `python -m pytest backend\tests\strategy_package\test_governance_readonly_smoke.py backend\tests\strategy_package\test_governance_eligibility.py backend\tests\strategy_package\test_repository_service.py -q -p no:cacheprovider` - `30 passed`.
- `python -m pytest backend\tests\strategy_package -q -p no:cacheprovider` - `103 passed`.
- `python -m pytest backend\tests\unified_engine\test_qrun_recorder_isolation.py -q -p no:cacheprovider` - `6 passed, 1 skipped`.
- `python scripts\governance_migration_smoke.py` - `status=passed mode=static_dry_run`.
- `python -m py_compile backend\main.py backend\services\rl_execution\__init__.py backend\services\rl_execution\deploy.py backend\services\rl_execution\model_registry.py backend\services\rl_execution\scheduler.py` - passed.
- `python -m py_compile backend\tests\strategy_package\test_governance_eligibility.py` - passed.
- `python scripts\aistock_guardrail_scan.py --fail-on-severity P1 backend\tests\strategy_package\test_governance_eligibility.py` - `findings=0 blocking=0`.
- `git diff --check` - passed.
- Isolated `8012` backend startup with `PYTHONUTF8=1` and `PYTHONIOENCODING=utf-8` - startup completed; `/openapi.json` returned HTTP 200 and includes `/api/v1/strategy-packages/{package_id}/governance-eligibility`.
- `python scripts\strategy_package_governance_readonly_smoke.py --api-base http://127.0.0.1:8012/api/v1 --timeout 5 --limit 5` - failed only on DB-backed endpoints with `psycopg2.OperationalError: fe_sendauth: no password supplied`; this confirms the remaining blocker is local dev DB credentials/schema, not `rl_execution` import visibility.

Previous integration gates after production-readonly preflight merge:

- `python scripts/governance_migration_smoke.py` - `status=passed mode=static_dry_run`.
- `python -m pytest backend/tests/model_registry/test_governance_migration_smoke.py backend/tests/strategy_package/test_governance_readonly_smoke.py -q -p no:cacheprovider` - `23 passed`.
- `python -m py_compile scripts/governance_migration_smoke.py backend/tests/model_registry/test_governance_migration_smoke.py` - passed.
- `python scripts/aistock_guardrail_scan.py --fail-on-severity P1 scripts/governance_migration_smoke.py backend/tests/model_registry/test_governance_migration_smoke.py` - `files=2, findings=0, blocking=0`.
- `git diff --check` - passed.

Previous broader integration gates before the production-readonly preflight merge:

- `python -m pytest backend/tests/strategy_package -q -p no:cacheprovider` - `93 passed`.
- `python -m pytest backend/tests/model_registry/test_model_registry_migration_smoke.py backend/tests/model_registry/test_governance_migration_smoke.py -q -p no:cacheprovider` - `20 passed`.

Previous integration gates after Phase 2 asset ledger:

- `python -m py_compile backend/services/strategy_package/package_asset.py backend/services/strategy_package/repository.py backend/services/strategy_package/service.py backend/routers/strategy_packages.py backend/tests/strategy_package/test_package_asset_ledger.py` - passed.
- `python -m pytest backend/tests/strategy_package/test_package_asset_ledger.py -q -p no:cacheprovider` - `5 passed`.
- `python -m pytest backend/tests/strategy_package -q -p no:cacheprovider` - `87 passed`.
- `python -m pytest backend/tests -q -p no:cacheprovider -k "package_asset or protected_asset or strategy_package"` - `94 passed, 898 deselected`.
- `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` - `blocking=0`.
- `git diff --check` - passed.

Previous integration gates also passed for Phase 6.1, Phase 7 validation runs, Phase 7 stability scoring, and Phase 3 Paper retest gate.

## Dev-Port And DB State

- No production DB writes were performed.
- No migrations were applied to any DB during this checkpointed governance work.
- Production-readonly preflight mode now exists for environments where a dev/test DB is unavailable; it is guarded by env + confirmation token and performs SELECT-only catalog inspection.
- The preflight reports base dependencies plus missing governance tables, views, columns, indexes, and named constraints; it does not apply migrations or run DDL/write SQL.
- A local guarded preflight sanity check with confirm/env but no DB password failed at connection to `postgres@127.0.0.1:5432/aistock` with `fe_sendauth: no password supplied`; no catalog SQL executed and no DB writes occurred.
- Full governance migration static smoke still exists and passes without opening a DB connection.
- Guarded full governance dev/test DB transaction smoke was attempted against `postgres@127.0.0.1:5432/aistock_dev` with `AISTOCK_QE_GOVERNANCE_MIGRATION_DEV_DB=true` and confirmation token.
  - Result: failed before applying migrations because local PostgreSQL credentials were unavailable: `fe_sendauth: no password supplied`.
  - After hardening, this failure is reported as structured JSON instead of an uncaught traceback.
  - No migrations were applied and no DB transaction reached migration execution.
- StrategyPackage governance GET-only smoke now exists and refuses production backend port `8001` by default before issuing any request.
- Readonly dev-port smoke was attempted against existing `http://127.0.0.1:8011`.
  - Output file: `C:\Users\lc999\Documents\Codex\2026-05-08\new-chat\validation_center_readonly_smoke_8011_phase7.json`
  - Earlier result: endpoints returned 404, so this is not a successful dev-port business validation.
  - Continued result on 2026-05-09: Validation Center smoke failed with 500 on `/validation/health`, `/validation/summary`, and `/validation/plans`; StrategyPackage governance smoke failed because the running 8011 backend does not expose the new governance endpoints.
  - `production_8001_touched=false`.
- Isolated `8012` backend startup was attempted from the integration worktree.
  - First failure: Windows console encoding needed `PYTHONIOENCODING=utf-8` / `PYTHONUTF8=1`.
  - Second failure: `backend.main` imports `rl_execution`, which requires missing `backend.services.rl_execution`; this blocks a clean full backend startup from the current integration branch.
  - A temporary branch proved that skipping the missing RL router lets Validation Center readonly smoke pass on `8012`, but that code change touched `backend/main.py` and was reverted from integration because guardrail scans flag existing historical P0 findings in that file.
  - StrategyPackage governance live smoke still fails without a reachable configured dev DB.

DB production risk summary:

- The governance migrations are mostly additive schema changes, but applying them directly to production is not zero-risk.
- Risk sources include `ALTER TABLE strategy_pkg.package`, new constraints/indexes, view dependencies on `public.aistock_model_catalog`, and possible short locks.
- Recommended next DB step is either a guarded SELECT-only production-readonly preflight when no dev/test DB is available, or a dev/test DB transaction smoke if a real dev/test DB can be provided. Neither path authorizes production DDL/write rollout.

## Important Behavioral Boundary

Codex governance work does not implement Paper v2 runtime functionality. Claude Code owns Paper v2 / vn.py / trading_core execution work.

Codex changes that can affect Paper-adjacent behavior:

- `StrategyPackageService.enable_paper()` requires a passed original fixed-weight validation run and scans all original fixed-weight runs for the current manifest.
- `GET /strategy-packages/{package_id}/governance-eligibility` exposes a read-only summary of Paper eligibility blockers and satisfied gates.
- Governance eligibility tests now explicitly cover `paper_ready=false` blockers for missing protected assets, missing runtime candidates, fragile stability, and disallowed package status.
- `scripts/strategy_package_governance_readonly_smoke.py` now validates the eligibility endpoint as part of the read-only governance smoke.
- `origin/fix/rl_execution_module_visibility-20260510` is merged into the governance line so `backend.main` can import/start with `backend.services.rl_execution` visible.
- Runtime variants can be marked Paper candidates only after validation evidence passes.
- Model registry bridge keeps `paper_selectable=false`; Paper should select StrategyPackages, not raw model catalog rows.

## Known Residual Risks

- Dev DB transaction migration smoke not yet successful for the full governance migration stack; local attempt stopped before migration execution because no dev DB password/config was available.
- Dev-port UI/API business smoke is partially blocked:
   - Existing `8011` is not a successful governance-enabled backend for these checks.
   - Current integration branch full backend startup on `8012` now succeeds after merging `origin/fix/rl_execution_module_visibility-20260510`.
   - StrategyPackage governance live API smoke still requires reachable dev DB credentials/schema; current local attempt failed with `fe_sendauth: no password supplied`.
- Guardrail scan on `backend/main.py` still reports pre-existing broad exception handler P0 findings; do not use that scan alone as merge proof until a baseline/new-only mode is provided or those historical findings are separately remediated.
- Integration branch contains governance APIs and migrations but is not production-deployed.
- `main` worktree may contain unrelated untracked files from other tools; Codex did not touch them.
- RD-Agent follow-up branch is pushed but not merged to RD-Agent main.

## Recommended Resume Steps

1. Start from `F:\Dev\AIstock_worktrees\qe-governance-integration-20260509`.
2. Run:
   - `git status --short --branch`
   - `git log --oneline -12`
3. Confirm integration branch is still clean and at or after `c0ee2af`.
4. Do not merge to `main`.
5. If continuing governance, first run static governance migration smoke:
   - `python scripts\governance_migration_smoke.py`
6. If no dev/test DB is available, run only the guarded production-readonly catalog preflight against the target DB; this is SELECT-only and must not be confused with migration apply:
   - `$env:AISTOCK_QE_GOVERNANCE_PROD_READONLY_PREFLIGHT='true'`
   - `python scripts\governance_migration_smoke.py --production-readonly-preflight --confirm-production-readonly-preflight QE_GOVERNANCE_PROD_READONLY_PREFLIGHT --db-host <host> --db-port <port> --db-name <db> --db-user <user> --json`
   - Inspect `checks.production_preflight.apply_needed`, `missing_base_dependencies`, and per-spec missing objects before any rollout decision.
7. If a real dev/test DB later becomes available, run guarded transaction migration smoke there instead of production:
   - `AISTOCK_QE_GOVERNANCE_MIGRATION_DEV_DB=true`
   - `python scripts\governance_migration_smoke.py --db-transaction-check --confirm-db-check QE_GOVERNANCE_FULL_STACK_DEV_ROLLBACK_CHECK --db-host 127.0.0.1 --db-port 5432 --db-name aistock_dev --db-user postgres --json`
   - The dev/test DB must already contain `strategy_pkg.package` and `public.aistock_model_catalog`.
8. Both DB preflight paths cover:
   - `model_registry_phase5_20260509.sql`
   - `strategy_pkg_promotion_review_20260509.sql`
   - `qe_phase4_master_seed_contract_20260509.sql`
   - `strategy_pkg_runtime_variant_20260509.sql`
   - `strategy_pkg_validation_run_20260509.sql`
   - `strategy_pkg_package_asset_20260509.sql`
9. For real live API smoke, provide/select a dev DB with `strategy_pkg` governance migrations applied and usable credentials; do not point the smoke at production.
10. Then run dev-port smoke on a real governance-enabled dev backend/frontend, not production `8001`:
   - Validation Center: `scripts\validation_center_readonly_smoke.py`
   - StrategyPackage governance: `scripts\strategy_package_governance_readonly_smoke.py`
11. Coordinate with Claude Code before touching Paper v2 / vn.py / trading_core files.
