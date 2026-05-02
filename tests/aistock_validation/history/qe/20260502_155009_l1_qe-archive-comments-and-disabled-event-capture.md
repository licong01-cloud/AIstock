# QE Archive Comments and Disabled Event Capture Validation

Date: 2026-05-02

## Scope

- Added PostgreSQL schema/table/column comments for the QE archive schema.
- Added static coverage tests so every managed `qe_archive` table and column must have a `COMMENT`.
- Added a disabled-by-default `QEArchiveEventCapture` helper as the next ingestion step foundation.
- Did not wire the helper into QE routers, webhooks, scanners, schedulers, or running production services.

## Production Safety

- Current QE production runtime is not changed by default.
- `QEArchiveEventCapture` only writes outbox events when explicitly enabled with `QE_ARCHIVE_EVENT_CAPTURE_ENABLED` or constructed with `enabled=True` in tests.
- Production backend `8001` was not restarted.
- No QE/RD-Agent worker workspace files or artifacts were read, modified, moved, or deleted.

## Commands

```powershell
python -m compileall backend/db/init_qe_archive_schema.py backend/services/qe_archive
python -m pytest backend/tests/test_qe_archive_schema.py backend/tests/test_qe_archive_repository_static.py -q
rg -n "workspace_path|/mnt/f|\\\\wsl\\$|\\\\wsl\\.localhost|QE_WORKSPACE_WIN|RDAGENT_WORKSPACE_WIN" backend/db/init_qe_archive_schema.py backend/services/qe_archive
python backend/db/init_qe_archive_schema.py
```

## Results

- Compile: passed.
- Targeted pytest: `15 passed in 0.48s`.
- Worker-path guardrail scan: no matches in new QE archive DB/service files.
- Explicit DB bootstrap: `QE archive schema initialized: qe_archive_v1_20260502`.
- DB comment verification:
  - `expected_tables=27`, `commented_tables=27`, `missing_tables=[]`
  - `expected_columns=458`, `commented_columns=458`, `missing_columns=[]`

## Business Oracles

- DB schema metadata is now machine-readable through PostgreSQL comments.
- New columns cannot be added to the managed QE archive schema without failing the comment coverage test.
- The next ingestion foundation exists, but it cannot affect existing QE production behavior unless explicitly enabled and wired in a future phase.

## Residual Risks / Next Phase

- Runtime QE webhook integration is still pending and must remain feature-flagged when implemented.
- Archive worker claim/retry loop is still pending.
- Artifact download and parser jobs are still pending and must use node APIs rather than worker filesystem paths.
