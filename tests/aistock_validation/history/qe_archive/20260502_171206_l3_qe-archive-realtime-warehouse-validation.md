# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-02T17:12:06
- Git commit: 27d356b
- Operator: lc999

## Scope

- Changed files: `backend/services/qe_archive/source_assembler.py`, `scripts/qe_archive_backfill.py`, `backend/services/qe_archive/archive_service.py`, `backend/services/qe_archive/repository.py`, `backend/services/qe_archive/__init__.py`, `backend/tests/test_qe_archive_repository_static.py`, `tests/aistock_validation/modules/qe_archive.md`, `noxfile.py`.
- Impacted flows: read-only assembly of existing `qe_experiments` and `qe_evolution_loops` rows into archive payloads; manual dry-run/write-gated backfill CLI.
- Business goal: allow real QE DB rows to be previewed for archive ingestion without enabling QE runtime hooks or reading worker artifact paths.
- Out of scope: production webhook integration, automatic scheduler, artifact download/parser, frontend UI, non-dry-run historical backfill rollout.
- Protected assets reviewed: no production process restart, no QE/RD-Agent worker artifact access, no StrategyPackage/model/HMM/runtime asset modification.

## Environment

- Backend port: not started; production 8001 was not restarted.
- Frontend port: not started.
- TDX port: not used.
- Conda/env: base Python for targeted compile/pytest/CLI smokes; `C:/Users/lc999/miniconda3/envs/AIstock/python.exe` for nox.
- Database: local PostgreSQL `aistock` through `.env`; `qe_archive_v1_20260502`.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py ... --fail-on HIGH` returned 0 findings | PASS |
| Backend tests | QE archive schema/repository/extractor/source-assembler/service/worker tests pass | `25 passed` | PASS |
| CLI dry-run | Real DB rows can be assembled without writing archive rows | `processed_count=2`, `dry_run=true`, `written=false` | PASS |
| Write safety | Manual backfill cannot write without explicit confirmation | CLI requires `--write --confirm-write QE_ARCHIVE_WRITE`; default is dry-run | PASS |
| DB smoke | Dry-run leaves warehouse unchanged and metadata complete | `run_count=0`, `pending_outbox_count=0`, 27/27 tables, 458/458 commented columns | PASS |
| UI E2E | UI not required before archive pages exist | `QE_ARCHIVE_L3_SKIP_UI=1` | SKIP |
| Asset safety | No protected asset modified silently | source assembler omits worker artifact paths; no production restart | PASS |

## Commands

```bash
python -m compileall noxfile.py scripts/qe_archive_backfill.py scripts/qe_archive_data_quality_smoke.py backend/db/init_qe_archive_schema.py backend/services/qe_archive backend/tests/test_qe_archive_repository_static.py backend/tests/test_qe_archive_schema.py

python -m pytest backend/tests/test_qe_archive_schema.py backend/tests/test_qe_archive_repository_static.py -q

python scripts/qe_archive_backfill.py --source all --limit 1 --output tmp/qe_archive_backfill_dry_run.json

python scripts/qe_archive_data_quality_smoke.py --output tmp/qe_archive_data_quality_smoke.json

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality

$env:QE_ARCHIVE_L3_SKIP_UI='1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3
```

## Evidence

- API calls: none; this phase adds CLI/service foundation only.
- DB checks: `tmp/qe_archive_backfill_dry_run.json` previewed one completed experiment and one completed evolution loop with `written=false`; `tmp/qe_archive_data_quality_smoke.json` reports `run_count=0`, `pending_outbox_count=0`, `archive_job_status_counts={}` after dry-run.
- Log files: command output in Codex session; no backend service log because no backend was started.
- Playwright report/trace: none; QE archive UI not implemented yet.
- Screenshots: none.
- Business output summary: existing QE DB rows can now be converted into archive-service payloads for preview; write mode remains explicitly gated and is not enabled by default.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None in this implementation phase | N/A | N/A | compileall, pytest, CLI dry-run, DB smoke, nox backend/data_quality/L3 all passed |

## Result

- Final status: PASS.
- Remaining risks: real backfill has only been dry-run previewed; write-mode historical backfill should be run later in a small confirmed batch, then verified with row counts and archive UI/API consumers. Artifact parsing remains a later phase.
- Need production backend restart: no
- Need dev service restart: no
