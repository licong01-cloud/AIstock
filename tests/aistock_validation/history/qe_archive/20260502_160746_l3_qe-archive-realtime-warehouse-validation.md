# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-02T16:07:46
- Git commit: 3aa03e9
- Operator: lc999

## Scope

- Changed files: `noxfile.py`, `scripts/qe_archive_data_quality_smoke.py`, `tests/aistock_validation/modules/qe_archive.md`, QE archive tests/docs.
- Impacted flows: QE archive validation pipeline only; no runtime QE router/webhook/scheduler integration.
- Business goal: introduce Paper v2-style staged validation for QE realtime warehouse development while keeping production QE unchanged.
- Out of scope: realtime webhook hook, archive worker loop, artifact parser, QE archive UI implementation.
- Protected assets reviewed: QE/RD-Agent worker workspaces, model weights, StrategyPackage manifests, HMM snapshots, and runtime artifacts were not modified.

## Environment

- Backend port: not started; production `8001` not restarted.
- Frontend port: not started; UI tests skipped because QE archive UI is not implemented yet.
- TDX port: not used.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe` for nox; default python for targeted pytest.
- Database: local PostgreSQL/TimescaleDB via `.env`, read-only data-quality smoke.
- Browser/headless: not used in this phase.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py` over QE archive paths | PASS |
| Backend tests | QE archive schema/repository/event-capture tests pass | `qe_archive_backend` -> 15 passed | PASS |
| Data-quality smoke | DB schema/version/comment coverage is complete | `qe_archive_data_quality` -> 27 tables, 458 columns, no failures | PASS |
| UI E2E | UI is explicitly not part of this phase | `QE_ARCHIVE_L3_SKIP_UI=1`; UI session is skipped until tests exist | SKIPPED |
| Asset safety | No protected asset modified silently | No QE/RD-Agent artifacts or production services touched | PASS |

## Commands

```bash
python -m compileall noxfile.py scripts/qe_archive_data_quality_smoke.py backend/db/init_qe_archive_schema.py backend/services/qe_archive
python -m pytest backend/tests/test_qe_archive_schema.py backend/tests/test_qe_archive_repository_static.py -q
python scripts/qe_archive_data_quality_smoke.py --output tmp/qe_archive_data_quality_smoke.json
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality
$env:QE_ARCHIVE_L3_SKIP_UI='1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3
```

## Evidence

- API calls: none; no backend service started.
- DB checks: `tmp/qe_archive_data_quality_smoke.json`.
- Log files: command output in this run record.
- Playwright report/trace: not applicable; QE archive UI not implemented yet.
- Screenshots: not applicable.
- Business output summary: validation pipeline now has backend, DB smoke, guardrail, and future UI entry points.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| First `qe_archive_l3` guardrail failed on test literals for WSL UNC banned tokens | Test itself contained literal forbidden tokens | Built banned tokens from string fragments so guardrail can scan tests cleanly | Rerun `qe_archive_l3` passed with 0 guardrail findings |

## Result

- Final status: PASS
- Remaining risks: QE archive realtime hooks, archive worker, APIs, and UI still require implementation and must remain feature-flagged/default-off until rollout.
- Need production backend restart: no
- Need dev service restart: no
