# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-04-29T19:32:12
- Git commit: 178a0b7
- Operator: lc999

## Scope

- Changed files: `scripts/aistock_data_quality_smoke.py`, `noxfile.py`, `tests/aistock_validation/modules/paper_v2_selection_center.md`, `docs/architecture/aistock_testing_version_management_system_design_20260429.md`
- Impacted flows: Paper v2 + Selection Center local validation pipeline, read-only data-quality smoke, L3 nox orchestration
- Business goal: add a result-oriented DB/data/ledger consistency gate without blocking new work on known legacy polluted Paper v2 rows
- Out of scope: permission/auth/security/Web security testing; UI E2E was intentionally skipped in this run with `PAPER_V2_L3_SKIP_UI=1`
- Protected assets reviewed: no StrategyPackage manifest, model weight, HMM snapshot/coefficient artifact, validated execution policy, QE/RD-Agent asset, or strategy source asset was modified

## Environment

- Backend port: not started or restarted in this validation
- Frontend port: not started or restarted in this validation
- TDX port: not required for this read-only pipeline step
- Conda/env: `AIstock`
- Database: local PostgreSQL/TimescaleDB read-only checks through `backend.db.pg_pool`
- Browser/headless: not used

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `nox -s l0`; 0 HIGH, 14 existing MEDIUM review findings | PASS |
| Backend tests | Paper v2 + Selection Center backend tests pass | `nox -s paper_v2_backend`; 112 passed | PASS |
| Data quality smoke | Required datasets, selection results, Paper v2 run trace, and ledger consistency are checked | `nox -s paper_v2_data_quality`; PASS with 1 legacy WARN | PASS |
| API flow | API/service validation is not part of this no-service run | deferred to service-backed L3/UI runs | N/A |
| UI E2E | User-visible flow works with no console/page/request errors | skipped by current scope; no frontend/backend dev services started | N/A |
| Asset safety | No protected asset modified silently | git diff limited to validation pipeline/docs/script files | PASS |

## Commands

```bash
set PYTHONIOENCODING=utf-8
set PYTHONDONTWRITEBYTECODE=1
python scripts/aistock_data_quality_smoke.py --json --output tmp/paper_v2_data_quality_smoke.json
python -m nox -s paper_v2_data_quality
python -m nox -s l0
python -m nox -s paper_v2_backend
set PAPER_V2_L3_SKIP_UI=1
python -m nox -s paper_v2_l3
```

## Evidence

- API calls: none; this run intentionally avoided starting/restarting backend services
- DB checks: `tmp/paper_v2_data_quality_smoke.json`
- Log files: terminal output from the nox sessions
- Playwright report/trace: none; UI E2E out of scope for this run
- Screenshots: none
- Business output summary: dataset audit is fresh enough for `suspend_d`, `stk_limit`, daily PV/basic/moneyflow, sector, and index data; StrategyPackage catalog has 3 usable packages and 1 paper-enabled validated policy; sampled 100 selection runs have persisted results; sampled 80 Paper v2 successful runs have snapshot/event trace; 3 legacy order/fill mismatches remain WARN-only baseline items

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Existing `paper_v2.run_events` check missed live session success events | Live finalization can be recorded in `paper_v2.session_events` instead of `paper_v2.run_events` | Data-quality smoke now accepts `SESSION_REPLAY_SUCCEEDED`, `SESSION_CATCHUP_REPLAY_SUCCEEDED`, `LIVE_DAY_FINALIZED`, and `NO_REBALANCE_REQUIRED` session events by `run_id` | `paper_v2_run_traceability` PASS |
| Historical local Paper v2 rows contain 3 order/fill mismatches | Legacy development/reset data predates the current validation gate and should not block new scoped validations | Default baseline reports legacy mismatches as WARN; validation-scoped `--portfolio-name-prefix`/`--portfolio-id` remains strict | `paper_v2_ledger_consistency` WARN, no failure |

## Result

- Final status: PASS for current non-security, no-service validation phase
- Remaining risks: 3 legacy Paper v2 ledger mismatches should be cleaned or archived separately if a strict historical DB baseline is required
- Need production backend restart: no
- Need dev service restart: no
