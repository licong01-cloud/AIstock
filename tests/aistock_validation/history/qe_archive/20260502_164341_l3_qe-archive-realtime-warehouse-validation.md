# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-02T16:43:41
- Git commit: 27d356b
- Operator: lc999

## Scope

- Changed files: `backend/services/qe_archive/archive_service.py`, `backend/services/qe_archive/payload_extractor.py`, `backend/services/qe_archive/models.py`, `backend/services/qe_archive/repository.py`, `backend/services/qe_archive/__init__.py`, `backend/tests/test_qe_archive_repository_static.py`.
- Impacted flows: manual/dry-run QE archive payload extraction and repository writes for run/source/config/repro/data-context/account/metrics/curves/factors/raw-payload records.
- Business goal: support the next safe backend workflow stage where already-collected QE loop/experiment payloads can be normalized and archived without enabling QE runtime hooks.
- Out of scope: QE webhook integration, archive scheduler startup, historical backfill scanner, artifact download/parser, backend API, frontend UI.
- Protected assets reviewed: no production process restart, no QE/RD-Agent worker workspace access, no StrategyPackage/model/HMM/runtime asset modification.

## Environment

- Backend port: not started; production 8001 was not restarted.
- Frontend port: not started; QE archive UI does not exist yet.
- TDX port: not used.
- Conda/env: base Python for targeted pytest and one-off DB smoke; `C:/Users/lc999/miniconda3/envs/AIstock/python.exe` for nox.
- Database: local PostgreSQL `aistock` through `.env`; `qe_archive_v1_20260502`.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py ... --fail-on HIGH` returned 0 findings | PASS |
| Backend tests | QE archive schema/repository/extractor/service/worker tests pass | `23 passed` | PASS |
| Payload extraction | Loop payload captures config, ordered factors, account summary, scalar metrics, IC/return/training curves, raw payloads | `test_payload_extractor_captures_reproducible_config_metrics_account_and_curves` | PASS |
| Manual DB write | Synthetic payload writes all first-stage archive rows and is cleaned up | run/config/repro/data_context/account/metric=12/curve=12/factor=2/raw=3 | PASS |
| DB smoke | Managed schema comments complete and no backlog remains after cleanup | 27/27 tables, 458/458 commented columns, pending outbox 0, run_count 0 | PASS |
| UI E2E | UI not required before archive pages exist | `QE_ARCHIVE_L3_SKIP_UI=1` | SKIP |
| Asset safety | No protected asset modified silently | no runtime hook, no production restart, no worker workspace path access | PASS |

## Commands

```bash
python -m compileall backend/services/qe_archive backend/tests/test_qe_archive_repository_static.py

python -m pytest backend/tests/test_qe_archive_schema.py backend/tests/test_qe_archive_repository_static.py -q

# One-off real PostgreSQL archive-service smoke:
# - wrote a synthetic QE loop payload through QEArchiveService(dry_run=False)
# - verified qe_archive.run/run_config/run_reproducibility_manifest/run_data_context/run_account_summary/run_metric/run_curve/run_factor/raw_payload row counts
# - deleted the synthetic run_id from qe_archive.run so cascades cleaned all child rows
@'...python archive service DB smoke...'@ | python -

python scripts/qe_archive_data_quality_smoke.py --output tmp/qe_archive_data_quality_smoke.json

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality

$env:QE_ARCHIVE_L3_SKIP_UI='1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3
```

## Evidence

- API calls: none; this phase is manual service/repository foundation only.
- DB checks: service smoke produced `run=1`, `run_config=1`, `run_reproducibility_manifest=1`, `run_data_context=1`, `run_account_summary=1`, `run_metric=12`, `run_curve=12`, `run_factor=2`, `raw_payload=3`, `research_valid=false` for daily no-limit/suspend authoritative payload; final smoke returned `run_count=0`, `pending_outbox_count=0`, `archive_job_status_counts={}` after cleanup.
- Log files: command output in Codex session; no backend service log because no backend was started.
- Playwright report/trace: none; QE archive UI is not implemented yet.
- Screenshots: none.
- Business output summary: payload archive can now preserve reproducibility config, ordered factor list/hash, account absolute return fields, scalar metrics, curve data for charts, raw payload snapshots, and daily-invalid research exclusion without touching current QE runtime.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None in this implementation phase | N/A | N/A | compileall, pytest, DB smoke, nox backend/data_quality/L3 all passed |

## Result

- Final status: PASS.
- Remaining risks: extraction is validated on synthetic payloads; real QE payload coverage and historical backfill parsing still need the next phase. No webhook/scheduler/API/UI integration exists yet by design.
- Need production backend restart: no
- Need dev service restart: no
