# Factor Cache WSL Path Policy Fix Validation

Date: 2026-05-06
Branch: codex/factor-cache-wsl-path-policy-20260506
Scope: QE factor backtest cache WSL launch path policy

## Business Goal

Fix `/api/v1/quantevolver/factor-cache/compute` WSL worker startup failure where `scripts/backfill_factor_cache.py` imports `ConfigComposer` under `/mnt/f/Dev/AIstock...` and the path policy incorrectly rejects AIstock-owned artifact roots as direct worker workspace access.

## Root Cause

- The factor-cache API starts a WSL subprocess and runs `scripts/backfill_factor_cache.py` from the AIstock repository path converted to `/mnt/f/...`.
- `ConfigComposer` resolves AIstock artifact roots from `__file__`, producing `/mnt/f/.../rdagent_assets/qe_programs` in WSL.
- `workspace_policy.is_forbidden_worker_workspace_path()` rejected every `/mnt/` path before checking whether the path was under an AIstock-owned artifact root.
- This blocked import before any factor code, ST PIT data, or parquet cache computation executed.

## Fix

- Keep Windows-side protection: `/mnt/...`, `//wsl$`, and `//wsl.localhost` are still rejected when evaluated from Windows-side code.
- In a WSL/Linux process, allow `/mnt/...` only when the path is under an AIstock-owned artifact root.
- Continue to reject `qe_workspace` and `rdagent_workspace` paths in both Windows and WSL contexts.

## Validation Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/strategy_package/workspace_policy.py backend/tests/test_workspace_policy_wsl_artifact_paths.py scripts/backfill_factor_cache.py backend/services/quantevolver/config_composer.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_workspace_policy_wsl_artifact_paths.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py backend/tests/test_factor_cache_wsl_env.py -q -p no:cacheprovider
wsl.exe --cd /mnt/f/Dev/AIstock_worktrees/factor-cache-wsl-path-policy-20260506 -- /home/lc999/miniconda3/envs/rdagent-gpu/bin/python -c "from backend.services.quantevolver.config_composer import QE_PROGRAMS_WIN, QE_EXPERIMENTS_ROOT, FACTOR_CACHE_ROOT_WIN; print('IMPORT_OK'); print('QE_PROGRAMS_WIN=', QE_PROGRAMS_WIN); print('QE_EXPERIMENTS_ROOT=', QE_EXPERIMENTS_ROOT); print('FACTOR_CACHE_ROOT_WIN=', FACTOR_CACHE_ROOT_WIN)"
wsl.exe --cd /mnt/f/Dev/AIstock_worktrees/factor-cache-wsl-path-policy-20260506 -- /home/lc999/miniconda3/envs/rdagent-gpu/bin/python scripts/backfill_factor_cache.py --help
wsl.exe --cd /mnt/f/Dev/AIstock_worktrees/factor-cache-wsl-path-policy-20260506 -- /home/lc999/miniconda3/envs/rdagent-gpu/bin/python -c "from types import SimpleNamespace; import scripts.backfill_factor_cache as b; args=SimpleNamespace(node_id=None, experiment_id=None, factor_data_dir='/tmp/factor_data', window_train_start='2026-04-30', window_backtest_end='2026-04-30', start=None, end=None, strict_backtest_data=True); ctx=b.resolve_execution_context(args); print('CTX_OK', ctx['factor_data_dir'], ctx['window_train_start'], ctx['window_backtest_end'], ctx['strict_backtest_data'])"
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_guardrail_scan.py --fail-on-severity P0 backend/services/strategy_package/workspace_policy.py backend/tests/test_workspace_policy_wsl_artifact_paths.py
```

## Results

- Py compile: PASS.
- Pytest targeted path policy suite: PASS, `36 passed in 14.93s`.
- WSL `ConfigComposer` import smoke: PASS, prints `IMPORT_OK` and resolves AIstock-owned `/mnt/f/.../rdagent_assets/...` roots.
- WSL `backfill_factor_cache.py --help`: PASS for import/startup; command prints CLI help. The PowerShell wrapper returned non-zero because the help output was piped through `Select-Object`, but the original import failure was gone and full help rendered.
- WSL `resolve_execution_context` smoke: PASS, prints `CTX_OK /tmp/factor_data 2026-04-30 2026-04-30 True`.
- Guardrail P0 scan on changed files: PASS, `findings=0, blocking=0`.

## Production Impact

- No production backend `8001` restart was performed during validation.
- No Qlib/H5/Bin/model/HMM/StrategyPackage assets were modified.
- A backend restart is required before production `8001` uses the fix.

## Residual Risks

- This validation confirms WSL startup/import/context resolution. It does not run a full multi-hour factor-cache recomputation.
- After backend restart, rerun `/api/v1/quantevolver/factor-cache/compute` on a small factor set first, then proceed to full cache rebuild.
