# QE custom evolution partial failure status and HMM retry fix

- Module: qe
- Level: L2
- Date: 2026-04-29T09:43:00
- Git commit: 59e1c80
- Operator: lc999

## Scope

- Changed files: `scripts/precompute_hmm_coefficients.py`, `backend/services/quantevolver/config_composer.py`, `backend/services/quantevolver/qe_evolution_service.py`, QE unified-engine tests.
- Impacted flows: QE custom evolution HMM coefficient precompute, retry of failed loops, custom-evo final task status derivation.
- Business goal: a 10-loop custom-evo task must not be marked successful when only 8 loops completed, and failed HMM loops must be retryable after workspace repair.
- Out of scope: frontend UI E2E and production backend restart.
- Protected assets reviewed: HMM coefficient artifacts were generated under ignored `backend/data/hmm_models/...`; no qlib bin, model snapshot, or historical QE result artifact was committed.

## Environment

- Backend port: production 8001 not restarted or modified.
- Frontend port: not used.
- TDX port: not used.
- Conda/env: Windows Python for pytest; WSL Ubuntu `rdagent-gpu` for HMM precompute smoke.
- Database: local PostgreSQL `aistock`; task `qe_20260429_015755_c4ba`.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | Changed Python files compile and HMM empty-output fallback is rejected | `py_compile` passed; pytest regression rejects empty `daily_coefficients`/`stock_sector_map` | Pass |
| Backend tests | QE unified-engine regression tests pass | `77 passed in 9.54s` | Pass |
| DB diagnosis | Task status and loop statuses explain partial success | task `max_loops=10`, `current_loop=8`, Loop1-8 completed, Loop9/10 failed before experiment_id | Pass |
| WSL HMM smoke | Both failed HMM snapshots produce non-empty coefficients | Loop9/10 each `sector_count=131`, `days=404`, `stock_map=5815`, `first_day_sectors=131` | Pass |
| Compose dry run | Retry workspace can be composed before submission | Loop9/10 each `files=62`, `cmd_has_qrun=True`, non-empty HMM artifact | Pass |
| Retry execution | Failed Loop9/10 can rerun to completion | Loop9 and Loop10 retried in `full_train` mode and completed; task now has 10 completed loops | Pass |
| Asset safety | No protected asset modified silently | Git staging excludes ignored HMM coefficients and unrelated dirty files | Pass |

## Commands

```bash
python -m py_compile scripts/precompute_hmm_coefficients.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/qe_evolution_service.py

set PYTHONIOENCODING=utf-8
set PYTHONDONTWRITEBYTECODE=1
pytest backend/tests/unified_engine/test_multi_alpha_command_generation.py backend/tests/unified_engine/test_qe_custom_evo_status.py backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider

# DB status check: task qe_20260429_015755_c4ba
# WSL HMM smoke: run scripts/precompute_hmm_coefficients.py for Loop9 and Loop10 model paths.
# Compose dry run: build_config_from_retry_loop + ConfigComposer.compose_experiment_in_memory(skip_db_save=True).
# Retry: AutoEvolutionScheduler().retry_loop("qe_20260429_015755_c4ba", 9/10), then poll DB/logs.
```

## Evidence

- API calls:
- DB checks before repair: task was `failed`, `current_loop=8`, `max_loops=10`; Loop1-8 `completed`; Loop9/10 `failed` with `WSL HMM 预计算失败`.
- DB checks after retry: task `completed`, `current_loop=10`, `max_loops=10`; loop status counts `completed=10`; Loop9 experiment `qe_20260429_015755_c4ba_L9`; Loop10 experiment `qe_20260429_015755_c4ba_L10`.
- HMM artifacts: Loop9 sha256 `b4698f0924e47f263793036c671507c3a0efec20305a45697dd958470eeade82`; Loop10 sha256 `6cbb78550f262fda398300a82f1291f402bae509568c9ac6d2ae6fe9271a664a`.
- Log files: not tailed; DB `agent_analysis` contains original failure traceback from config composition.
- Playwright report/trace:
- Screenshots:
- Business output summary: status derivation now requires all configured loops completed; HMM precompute supports horizon-v2 10-feature snapshots and legacy 7-feature snapshots without empty success; retry produced Loop9 annualized excess return `0.3741`, max drawdown `-0.1354`, Sharpe `1.8584`, and Loop10 annualized excess return `0.3632`, max drawdown `-0.1392`, Sharpe `1.8358`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Loop9/10 failed before backtest | HMM precompute could not build observations matching horizon-v2 10-feature schema and allowed empty output | Added schema-aware HMM observation building and fail-fast empty-output validation | WSL smoke: both Loop9/10 produced 131 sectors and 404 days |
| 8/10 loop task marked completed | custom-evo final fallback used `completed_count > 0` | Added strict final status helper requiring all configured loops completed and no failed/cancelled/active loops | pytest custom-evo status tests pass |
| Failed-before-workspace loop could not be retried | retry path required `params.pkl` and backtest-only mode | Retry now falls back to full train+backtest when params are absent | compose dry run and actual Loop9/10 retries completed |

## Result

- Final status: validation passed; Loop9/10 retry completed and task now has all 10 loops completed.
- Remaining risks: production 8001 must be restarted by the operator to pick up the patched strict status code for future API/UI-created runs; the successful retry was triggered from the current working tree service code.
- Need production backend restart: yes, for the running 8001 service to load the patched code.
- Need dev service restart: no dev service was started.
