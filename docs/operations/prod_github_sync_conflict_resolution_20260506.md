# Production Directory and GitHub Sync Incident - 2026-05-06

## Purpose

This document records the 2026-05-06 reconciliation of the production working directory `F:/Dev/AIstock` with GitHub `origin/main`. The goal was to preserve all developed functionality, avoid losing local work, and make the production directory exactly match GitHub without restarting production port `8001`.

## Initial Problem

`F:/Dev/AIstock` was both the production runtime directory and a dirty development workspace. Before reconciliation it had:

- Branch state: `main...origin/main [ahead 5, behind 10]`
- Tracked modifications: `15`
- Untracked entries: `727`
- Local-only untracked entries not present on `origin/main`: `708`
- Local-only code/config/script candidates: `36`
- Local-only docs/records candidates: `51`
- Local-only artifact/temp candidates: `621`

This made both simple `git pull` and direct `git reset --hard origin/main` unsafe:

- `git pull` could produce conflicts in a production runtime directory.
- `git reset --hard` could remove local-only functionality from production if it had not first been committed to GitHub.
- Moving all untracked files out first could make production unable to access files it was still using.

## Safety Requirements

- Preserve all real developed functionality before cleaning the production directory.
- Do not use the backup directory as a runtime dependency.
- Do not delete local-only files before classifying them.
- Do not restart or replace production backend port `8001` during reconciliation.
- Use a clean worktree from `origin/main` for merge/reconcile work.
- Only synchronize `F:/Dev/AIstock` after GitHub has the complete preserved version.

## Backups Created

### Preflight Backup

- Backup directory: `F:/Dev/AIstock_backups/prod_reconcile_preflight_20260506_085409`
- Backup branch: `backup/prod-reconcile-preflight-20260506_085409`
- Included:
  - `all_refs.bundle`
  - `unstaged_binary.patch`
  - `staged_binary.patch`
  - `head_to_worktree_binary.patch`
  - `status_porcelain_uall.txt`
  - `untracked_manifest.json`
  - full copied untracked files under `untracked_files/`
  - generated audit: `prod_reconcile_audit_20260506.final.md`

### Final Pre-Reset Backup

- Backup directory: `F:/Dev/AIstock_backups/prod_final_sync_pre_reset_20260506_091002`
- Backup branch: `backup/prod-final-sync-pre-reset-20260506_091002`
- Included:
  - `unstaged_binary.patch`
  - `staged_binary.patch`
  - `head_to_worktree_binary.patch`
  - `status_porcelain_uall.txt`
  - `show_ref.txt`

## Reconcile Worktree

A clean worktree was created from GitHub `origin/main`:

- Worktree: `F:/Dev/AIstock_worktrees/prod-reconcile-20260506_085409`
- Branch: `codex/prod-reconcile-20260506_085409`
- Base: `origin/main` at `405adcb feat(validation): add real-port UI smoke`

Local-only files from `F:/Dev/AIstock` were copied into this clean worktree for review. Production files were not moved or removed during review.

## Preserved in GitHub

The following commit series was pushed to GitHub and then fast-forwarded into `main`:

- `98bacdb chore(prod-sync): preserve first local-only candidates`
- `c28c40a chore(prod-sync): preserve local diagnostics and records`
- `87c5593 docs(prod-sync): record remaining excluded artifacts`

Key preserved items include:

- `frontend/src/app/paper-trading/package-selection/page.tsx`
  - Restored the production-visible `/paper-trading/package-selection` route that was linked by `frontend/src/app/paper-trading/layout.tsx`.
- `backend/tests/test_dispatch_service_env.py`
  - Preserved RD-Agent dispatch environment-variable coverage for existing service functions.
- `configs/execution_algos/v25_two_stage.yaml`
  - Preserved the local V25 configuration artifact for traceability.
- V25/QE/HMM diagnostic scripts and validation records, including:
  - `scripts/v25_*`
  - `scripts/verify_v25_*`
  - `scripts/diagnostics/hmm_*`
  - `scripts/automation/hmm_*`
  - `tests/aistock_validation/history/...`
  - `docs/analysis/...`
- The detailed reconciliation audit:
  - `docs/operations/prod_reconcile_audit_20260506.md`

## Excluded from GitHub and Quarantined

After GitHub contained all preserved functionality, `F:/Dev/AIstock` was reset to `origin/main`. The remaining untracked entries were quarantined, not deleted.

Quarantine directory:

- `F:/Dev/AIstock_backups/prod_final_sync_pre_reset_20260506_091002/quarantined_untracked_after_reset`

Moved entries:

- `.codex_tmp/`
- `.coverage`
- `a_share_duplicate_1_zip_deleted_manifest.txt`
- `catboost_info/`
- `frontend/.codex_tmp/`
- `monitoring/process-exporter/process-exporter`
- `qlib_minute_validation/`
- `qmt_down_queue_delete_candidates.txt`
- `scripts/qrun_limit_minute.py.backup`
- `scripts/test_v25_simple.py`
- `scripts/v25_verify.py`
- `tests/aistock_validation/history/qlib_data/20260504_l3_pit-bin-lgb-smoke-pred.pkl`
- `tests/aistock_validation/history/qlib_data/20260504_l4_pit-full-bin-lgb-smoke-pred.pkl`

Reasons:

- `.codex_tmp/`, `.coverage`, `catboost_info/`, `qlib_minute_validation/`, and PKL files were temporary or generated artifacts.
- `scripts/test_v25_simple.py` and `scripts/v25_verify.py` had syntax errors and were not runnable production functions.
- `monitoring/process-exporter/process-exporter` was not referenced by the tracked service/docker configuration; tracked configuration uses `/usr/local/bin/process-exporter` or the Docker image.
- Root deletion-candidate manifests were operational scratch files, not runtime functionality.

## Validation Performed

Validation on the clean reconcile branch included:

- `git diff --check origin/main..HEAD` passed.
- `pytest backend/tests/test_dispatch_service_env.py -q -p no:cacheprovider` passed: `4 passed`.
- Preserved Python scripts were checked with `python -m py_compile`.
- `npm run build` in `frontend` passed and included `/paper-trading/package-selection` in the generated routes.
- `conda run -n AIstock python -m nox -s l0` passed after generating the local guardrail baseline; blocking count was `0`.

Final production directory verification:

- `F:/Dev/AIstock` status: `## main...origin/main`
- `HEAD`: `87c55932439d2959ba0b8f6919fe837650fac56f`
- `origin/main`: `87c55932439d2959ba0b8f6919fe837650fac56f`
- Untracked count: `0`

## Root Cause

The incident was caused by using the production root worktree `F:/Dev/AIstock` as both:

- a production runtime directory, and
- an active development workspace for multiple Codex sessions and manual experiments.

This allowed local commits, untracked functional files, temporary artifacts, and GitHub updates to diverge at the same time.

## Mandatory Future Rules

1. Do not develop directly in `F:/Dev/AIstock`.
   - Treat it as the production sync target and rescue baseline only.
2. Every Codex window must use a task-specific clean worktree from latest `origin/main`.
   - Preferred path: `F:/Dev/AIstock_worktrees/<task-name>`
   - Preferred branch: `codex/<module-task-date>`
3. At task start, run a Git preflight:
   - `git status --short --branch`
   - `git branch --show-current`
   - `git log --oneline -5`
4. Never leave real functionality as untracked files.
   - New pages, scripts, configs, docs, migrations, tests, and validation records must be committed or explicitly classified as artifacts before handoff.
5. Before syncing production to GitHub, classify local-only files first.
   - If a local-only file is functional, copy it into a clean reconcile branch and commit it before resetting production.
   - If it is a runtime-local asset, add it to an explicit keep-list.
   - If it is temporary, quarantine it under `F:/Dev/AIstock_backups`, not delete it.
6. Do not run `git pull`, `git merge`, `git reset --hard`, `git checkout -- .`, or `git clean -fd` in `F:/Dev/AIstock` unless:
   - a backup exists,
   - GitHub already contains all preserved functionality,
   - local-only files have been classified,
   - the action is explicitly part of a production sync procedure.
7. Production code sync and production process reload are separate actions.
   - Updating files does not guarantee the running `8001` process has loaded the code.
   - Do not restart production `8001` unless explicitly requested.
   - Use non-production ports for validation.
8. Handoffs must include:
   - branch name,
   - commit hash,
   - changed files,
   - validation commands and results,
   - untracked file status,
   - push status,
   - whether production port `8001` was touched.

## Current State After Resolution

As of this record:

- GitHub `origin/main` and `F:/Dev/AIstock` are synchronized at `87c5593`.
- `F:/Dev/AIstock` has no untracked or uncommitted files.
- The production runtime process on `8001` was not restarted during this operation.
- Any future production reload/restart should be done only in an explicit maintenance step.
