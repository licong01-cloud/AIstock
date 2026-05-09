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
- Latest integration commit before this checkpoint update: `056a18a merge(qe): strategy package governance readonly smoke`

RD-Agent follow-up:

- Worktree: `F:\Dev\RD-Agent_worktrees\qe-rdagent-recorder-api-guard-fixups-20260509`
- Branch: `codex/qe-rdagent-recorder-api-guard-fixups-20260509`
- Commit: `810726a0 fix(qe): harden recorder guard tar extraction`
- Status: pushed; not merged to main.

## Completed AIstock Governance Work

All items below are merged only into `codex/qe-governance-integration-20260509` and pushed.

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
   - Added `scripts/governance_migration_smoke.py` for static dry-run validation of the full six-file governance migration stack, with guarded opt-in dev DB transaction mode.
   - Added `scripts/strategy_package_governance_readonly_smoke.py` for GET-only StrategyPackage governance API smoke checks that refuse production port `8001` by default.

## Latest Verified Gates

The latest integration gates after governance smoke tooling:

- `python scripts/governance_migration_smoke.py` - `status=passed mode=static_dry_run`.
- `python -m pytest backend/tests/model_registry/test_governance_migration_smoke.py backend/tests/strategy_package/test_governance_readonly_smoke.py -q -p no:cacheprovider` - `16 passed`.
- `python -m py_compile scripts/governance_migration_smoke.py scripts/strategy_package_governance_readonly_smoke.py backend/tests/model_registry/test_governance_migration_smoke.py backend/tests/strategy_package/test_governance_readonly_smoke.py` - passed.
- `python -m pytest backend/tests/strategy_package -q -p no:cacheprovider` - `93 passed`.
- `python -m pytest backend/tests/model_registry/test_model_registry_migration_smoke.py backend/tests/model_registry/test_governance_migration_smoke.py -q -p no:cacheprovider` - `20 passed`.
- `python scripts/aistock_guardrail_scan.py --fail-on-severity P1 scripts/governance_migration_smoke.py scripts/strategy_package_governance_readonly_smoke.py backend/tests/model_registry/test_governance_migration_smoke.py backend/tests/strategy_package/test_governance_readonly_smoke.py` - `files=4, findings=0, blocking=0`.
- `git diff --check` - passed.

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
- Full governance migration static smoke now exists and passes without opening a DB connection.
- Full governance dev/test DB transaction smoke has not yet been run against a real dev DB snapshot.
- StrategyPackage governance GET-only smoke now exists and refuses production backend port `8001` by default before issuing any request.
- A readonly dev-port smoke was attempted against `http://127.0.0.1:8011`.
  - Output file: `C:\Users\lc999\Documents\Codex\2026-05-08\new-chat\validation_center_readonly_smoke_8011_phase7.json`
  - Result: endpoints returned 404, so this is not a successful dev-port business validation.
  - `production_8001_touched=false`.

DB production risk summary:

- The governance migrations are mostly additive schema changes, but applying them directly to production is not zero-risk.
- Risk sources include `ALTER TABLE strategy_pkg.package`, new constraints/indexes, view dependencies on `public.aistock_model_catalog`, and possible short locks.
- Recommended next DB step is dev DB / test DB migration smoke before any production DB schema rollout.

## Important Behavioral Boundary

Codex governance work does not implement Paper v2 runtime functionality. Claude Code owns Paper v2 / vn.py / trading_core execution work.

Codex changes that can affect Paper-adjacent behavior:

- `StrategyPackageService.enable_paper()` requires a passed original fixed-weight validation run.
- Runtime variants can be marked Paper candidates only after validation evidence passes.
- Model registry bridge keeps `paper_selectable=false`; Paper should select StrategyPackages, not raw model catalog rows.

## Known Residual Risks

- Dev DB transaction migration smoke not yet run for the full governance migration stack; only the new static dry-run smoke has passed.
- Dev-port UI/API business smoke not yet successful because `8011` returned 404 for Validation Center endpoints.
- StrategyPackage governance live API smoke still requires a governance-enabled dev backend; the script is present but no successful live dev-port business validation has been recorded.
- Integration branch contains governance APIs and migrations but is not production-deployed.
- `main` worktree may contain an unrelated untracked doc from another tool: `F:\Dev\AIstock\docs\discussion\user_decisions_for_morning_review_20260510.md`; Codex did not touch it.
- RD-Agent follow-up branch is pushed but not merged to RD-Agent main.

## Recommended Resume Steps

1. Start from `F:\Dev\AIstock_worktrees\qe-governance-integration-20260509`.
2. Run:
   - `git status --short --branch`
   - `git log --oneline -12`
3. Confirm integration branch is still clean and at or after `056a18a`.
4. Do not merge to `main`.
5. If continuing governance, first run static governance migration smoke:
   - `python scripts\governance_migration_smoke.py`
6. Then run dev/test DB transaction migration smoke for:
   - `model_registry_phase5_20260509.sql`
   - `strategy_pkg_promotion_review_20260509.sql`
   - `qe_phase4_master_seed_contract_20260509.sql`
   - `strategy_pkg_runtime_variant_20260509.sql`
   - `strategy_pkg_validation_run_20260509.sql`
   - `strategy_pkg_package_asset_20260509.sql`
7. Then run dev-port smoke on a real governance-enabled dev backend/frontend, not production `8001`:
   - Validation Center: `scripts\validation_center_readonly_smoke.py`
   - StrategyPackage governance: `scripts\strategy_package_governance_readonly_smoke.py`
8. Coordinate with Claude Code before touching Paper v2 / vn.py / trading_core files.
