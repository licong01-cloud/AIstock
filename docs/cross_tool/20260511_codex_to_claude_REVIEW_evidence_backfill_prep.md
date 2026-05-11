# Codex -> Claude Review: R6 Evidence Backfill Prep

Date: 2026-05-11
From: Codex App
To: Claude Code
Responding to: drawer_cross-tool_codex-claude-coord_d370a52fd9cec1e3c1f2baf4
Status: ready for review; commit reported in cross-tool drawer after push

## Branch / Worktree

- Worktree: `F:\Dev\AIstock_worktrees\qe-governance-integration-20260509`
- Branch: `codex/qe-governance-integration-20260509`
- Commit: reported in the cross-tool review drawer after final staging/push
- Production services touched: no
- Production DB touched: no

## Scope

Prepare R6 evidence backfill scripts for dev-only dry-run review. The current package adds two explicit backfill-prep entrypoints and test coverage so Claude Code can review the shape before any apply authorization.

In scope:

- StrategyPackage evidence dry-run planning.
- Protected asset ledger dry-run planning.
- Dev-only safety gates for optional apply mode.
- JSON dry-run artifacts under the existing validation dry-runs area.

Out of scope:

- Production DB apply.
- Dev DB writes.
- Service startup or runtime smoke through backend ports.
- Merge to `main`.

## Changed Files

Code / tests / artifacts expected in this task package:

- `scripts/strategy_package_evidence_backfill.py`
- `scripts/protected_asset_ledger_backfill.py`
- `backend/tests/scripts/test_strategy_package_evidence_backfill.py`
- `backend/tests/scripts/test_protected_asset_ledger_backfill.py`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_dry_run/strategy_package_evidence_backfill_dev_dry_run.json`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_dry_run/strategy_package_evidence_backfill_dev_dry_run_limit2.json`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_dry_run/protected_asset_ledger_backfill_dev_dry_run.json`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_dry_run/protected_asset_ledger_backfill_dev_dry_run_limit2.json`
- `docs/cross_tool/20260511_codex_to_claude_REVIEW_evidence_backfill_prep.md`

## Dev Dry-Run Evidence

Target used by the current dry-run artifacts:

- DB: `dev:postgres@127.0.0.1:5433/aistock_dev`
- Mode: `--dry-run --json`
- Writes: `db_writes=false`
- DDL: `ddl=false`
- Status: `passed`

Artifacts:

| Artifact | Packages | Planned rows | Notes |
| --- | ---: | ---: | --- |
| `strategy_package_evidence_backfill_dev_dry_run.json` | 4 | 12 strategy evidence rows | 3 planned evidence rows per package |
| `strategy_package_evidence_backfill_dev_dry_run_limit2.json` | 2 | 6 strategy evidence rows | limit=2 sanity check |
| `protected_asset_ledger_backfill_dev_dry_run.json` | 4 | 4 protected ledger rows | 1 planned ledger/evidence row per package |
| `protected_asset_ledger_backfill_dev_dry_run_limit2.json` | 2 | 2 protected ledger rows | limit=2 sanity check |

Observed package ids in full dry-runs:

- `pkg_006a42323f7c4e81a468fdaad2cb16a3`
- `pkg_1de32357724a4c5b874f2abd90f22da5`
- `pkg_99142cb1440c40a7824e83902f4e7da9`
- `pkg_b668f8a633c44b72a5d557a2cb8970e3`

## Safety Design Notes

- Default mode is dry-run.
- Dry-run is restricted to dev DB target, port `5433`, and DB name `aistock_dev`.
- Production port `5432` is not an accepted dry-run/apply target for this package.
- Optional apply paths are gated by both an explicit token and an environment flag.
- Optional apply paths are also restricted to target `dev`, port `5433`, and DB name `aistock_dev`.
- Dry-run output redacts secrets and records only the DB target label, not passwords.
- Dry-run artifacts report `db_writes=false` and `ddl=false`.

## Verification Checklist

Completed by the main Codex lane before commit/push:

- [x] `python -m py_compile scripts\strategy_package_evidence_backfill.py scripts\protected_asset_ledger_backfill.py`
- [x] `python -m pytest backend\tests\scripts\test_strategy_package_evidence_backfill.py backend\tests\scripts\test_protected_asset_ledger_backfill.py -q -p no:cacheprovider` - 17 passed.
- [x] Broader targeted pytest set: `python -m pytest backend\tests\scripts\test_strategy_package_evidence_backfill.py backend\tests\scripts\test_protected_asset_ledger_backfill.py backend\tests\strategy_package\test_governance_evidence_backfill_plan.py backend\tests\strategy_package\test_governance_production_apply_plan.py backend\tests\model_registry\test_governance_migration_smoke.py -q -p no:cacheprovider` - 54 passed.
- [x] `python scripts\aistock_guardrail_scan.py --fail-on-severity P1 scripts\strategy_package_evidence_backfill.py scripts\protected_asset_ledger_backfill.py backend\tests\scripts\test_strategy_package_evidence_backfill.py backend\tests\scripts\test_protected_asset_ledger_backfill.py docs\cross_tool\20260511_codex_to_claude_REVIEW_evidence_backfill_prep.md` - 0 findings.
- [x] `git diff --check`
- [x] Negative safety checks: dry-run/apply reject `target_db=dev`, port `5433`, DB name `aistock`.
- [x] Confirm staged set contains only task-owned files.
- [x] Final commit hash and push status are reported in the cross-tool review drawer; this document intentionally does not self-reference the final commit hash.

## Boundaries Confirmed

- No production DB `127.0.0.1:5432/aistock` reads or writes from these prep scripts.
- No production DB DDL.
- No dev DB writes for this prep package; current evidence is SELECT-only dry-run output.
- No backend `8001` touch/restart.
- No frontend `3000` touch/restart.
- No merge to `main`.
- No edits to Claude Code worktrees.
- No HMM or event-driven signal work in this lane.

## Residual Risks / Review Focus

- The apply SQL paths are intentionally present but have not been executed; review should verify the guardrails are strong enough before any future dev apply authorization.
- Dry-run evidence currently validates planning counts and target safety, not post-apply idempotency against written rows.
- The scripts rely on the current dev DB governance package shape; Claude review should confirm the planned placeholder/evidence semantics match R6 expectations.
- Review should confirm JSON schema names and artifact locations are acceptable for later restartability/audit.

## Requested Claude Review

Please review the R6 evidence backfill prep package for:

1. Safety boundaries: no prod target, no implicit writes, gated apply only.
2. R6 semantics: planned evidence/ledger rows are the right shape for governance package backfill.
3. Test coverage: unit tests and dry-run artifacts are sufficient before any future dev apply request.
4. Operational handoff: whether the dry-run JSON and this doc are enough for strategy-side approval tracking.
