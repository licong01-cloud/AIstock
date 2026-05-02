# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-02T17:45:13
- Git commit: 3cca05c
- Operator: lc999

## Scope

- Changed files: `scripts/qe_archive_data_quality_smoke.py`, `backend/services/qe_archive/payload_extractor.py`, `backend/tests/test_qe_archive_repository_static.py`, `docs/codex_project_memory.md`, this validation record.
- Impacted flows: run-level QE archive data-quality verification, confirmed single-loop backfill write, and idempotent re-run validation.
- Business goal: prove one real completed QE loop can be safely inserted into `qe_archive` with reproducible config, metrics, curves, factors, raw payloads, and run-level validation.
- Out of scope: webhook/scheduler integration, API/UI consumers, artifact download/parser, broad historical backfill.
- Protected assets reviewed: no production backend restart, no worker workspace file access, no StrategyPackage/model/HMM/runtime asset modification.

## Environment

- Backend port: not started; production 8001 was not restarted.
- Frontend port: not started.
- TDX port: not used.
- Conda/env: base Python for direct compile/pytest/CLI; `C:/Users/lc999/miniconda3/envs/AIstock/python.exe` for nox.
- Database: local PostgreSQL `aistock`; one real archive row inserted for `qe_20260501_011054_c90a_Loop11`.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py ... --fail-on HIGH` returned 0 findings | PASS |
| Backend tests | QE archive schema/repository/extractor/source-assembler/service tests pass | `25 passed` | PASS |
| Confirmed write | One real loop is inserted into `qe_archive` only with explicit confirmation | `--write --confirm-write QE_ARCHIVE_WRITE`, `processed_count=1`, `written=true` | PASS |
| Run-level DB validation | Archived run has required config/source/context/account/metrics/curves/factors/raw payloads | metrics 81, curves 3489, factors 57, raw payloads 3, account summary 1 | PASS |
| Idempotency | Re-running the same confirmed backfill keeps row counts stable | second write still metrics 81, curves 3489, factors 57, raw payloads 3 | PASS |
| UI E2E | UI not required before archive pages exist | `QE_ARCHIVE_L3_SKIP_UI=1` | SKIP |
| Asset safety | No protected asset modified silently | no runtime hook, no worker path access, no production restart | PASS |

## Commands

```bash
python -m compileall scripts/qe_archive_data_quality_smoke.py backend/services/qe_archive/payload_extractor.py backend/tests/test_qe_archive_repository_static.py

python -m pytest backend/tests/test_qe_archive_schema.py backend/tests/test_qe_archive_repository_static.py -q

python scripts/qe_archive_backfill.py --loop-id qe_20260501_011054_c90a_Loop11 --write --confirm-write QE_ARCHIVE_WRITE --output tmp/qe_archive_backfill_write_loop11_after_backtest_infer.json

python scripts/qe_archive_data_quality_smoke.py --run-id qear_run_6aad101d9e6e31f629230a4c --min-metrics 80 --min-curves 3000 --min-factors 50 --require-account-summary --output tmp/qe_archive_data_quality_loop11_after_backtest_infer.json

python scripts/qe_archive_backfill.py --loop-id qe_20260501_011054_c90a_Loop11 --write --confirm-write QE_ARCHIVE_WRITE --output tmp/qe_archive_backfill_write_loop11_final_idempotent.json

python scripts/qe_archive_data_quality_smoke.py --run-id qear_run_6aad101d9e6e31f629230a4c --min-metrics 80 --min-curves 3000 --min-factors 50 --require-account-summary --output tmp/qe_archive_data_quality_loop11_final_idempotent.json

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality

$env:QE_ARCHIVE_L3_SKIP_UI='1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3
```

## Evidence

- API calls: none; no backend service was started.
- DB checks: `tmp/qe_archive_data_quality_loop11_final_idempotent.json` reports run exists with `config_capture_complete=true`, `reproducibility_level=full`, `metric_count=81`, `curve_count=3489`, `factor_count_rows=57`, `raw_payload_count=3`, `pending_outbox_count=0`, no failures/warnings.
- Log files: command output in Codex session; no backend service log.
- Playwright report/trace: none.
- Screenshots: none.
- Business output summary: `qe_20260501_011054_c90a_Loop11` is now archived as `qear_run_6aad101d9e6e31f629230a4c`, with inferred backtest window from return-curve dates and stable repeated-write counts.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| First confirmed write produced `missing_items=["backtest_window"]` and partial reproducibility | source loop config lacked explicit backtest start/end | enhanced `payload_extractor` to infer backtest window from return curve dates when config omits it | rerun write produced `missing_items=[]`, `config_capture_complete=true`, `reproducibility_level=full`, no warnings |

## Result

- Final status: PASS.
- Remaining risks: only one loop has been confirmed-written; broad backfill, artifact manifests, parser outputs, API/UI consumers, and worker/webhook integration remain future phases.
- Need production backend restart: no
- Need dev service restart: no
