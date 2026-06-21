# BUG-437 90-Minute Full Compute Validation

- Collected at: 2026-06-21T03:20:45+08:00
- Backend health was OK; no backend/frontend restart was performed by this validation step.
- Triggered endpoint: `POST /api/v1/quantevolver/factor-cache/compute`
- Task: `31adf993-4d8e-4ffd-acf6-6d3a2f840831`, remote=`268`, node=`wsl2-5080`
- Window: `2018-08-01~2026-04-30`, cache_source=`official_offline_backtest_factor_data`
- Request: workers=`4`, batch_size=`16`, force=`true`
- Result: status=`failed`, elapsed_minutes=`19.16`
- Count: success_count=`42`, fail_count=`533`
- Failure reason: `memory_gate_failed: available_memory_below_minimum`; this was fail-fast, not silent fallback.
- Observed effective_workers: `1, 1, 2`
- Conclusion: full 575-factor compute did not complete, so the <=90 minute target was not validated.

See `summary.json` for structured evidence.
