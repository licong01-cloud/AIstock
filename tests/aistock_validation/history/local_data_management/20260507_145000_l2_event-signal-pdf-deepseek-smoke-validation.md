# L2 Event Signal PDF DeepSeek Smoke Validation - 2026-05-07

## Scope

- Added a read-only announcement PDF smoke pipeline in `backend/services/event_signal/document_pdf_smoke.py`.
- Validates direct PDF URL resolution, Eastmoney PDF anti-bot cookie parsing, PDF text extraction, deterministic chunk preprocessing, and DeepSeek JSON analysis.
- Writes downloaded PDFs only to `F:\Dev\AIstock_artifacts\event_signal_pdf_smoke`; no PDF artifacts are stored in the repo.
- Does not write database rows and does not change QE, Selection Center, Paper Trading v2, QMT, or production port `8001`.

## Commands And Results

- `python -m pytest backend/tests/event_signal/test_document_pdf_smoke.py -q` -> `6 passed in 1.21s`.
- `python -m backend.services.event_signal.document_pdf_smoke --limit 1 --candidate-scan-limit 5 --no-deepseek --artifact-dir F:\Dev\AIstock_artifacts\event_signal_pdf_smoke --output-dir reports\event_signal\pdf_smoke --max-pages 4 --max-chunks 4` -> preprocessed 1 row, failed 0.
- `python -m backend.services.event_signal.document_pdf_smoke --limit 2 --candidate-scan-limit 8 --artifact-dir F:\Dev\AIstock_artifacts\event_signal_pdf_smoke --output-dir reports\event_signal\pdf_smoke --max-pages 4 --max-chunks 4 --max-tokens 1000` -> analyzed 2 rows with DeepSeek, failed 0.
- `python -m pytest backend/tests/announcements/test_title_classifier.py backend/tests/event_signal -q` -> `108 passed in 2.06s`.
- `python -m compileall -q backend/services/event_signal backend/services/announcements/title_classifier.py backend/tests/event_signal backend/tests/announcements/test_title_classifier.py` -> passed.
- `git diff --check` -> passed; only existing Windows LF-to-CRLF warnings were printed.
- Protected-module scan over `backend/services/selection_center`, `backend/services/paper_trading_v2`, `backend/services/quantevolver`, `backend/infra/qmt_client.py`, and `backend/routers/qmt.py` -> no matches for new event-signal/PDF identifiers.

## DeepSeek Smoke Evidence

- Report JSON: `reports/event_signal/pdf_smoke/document_pdf_smoke_20260507_144618.json` (ignored runtime report).
- Report Markdown: `reports/event_signal/pdf_smoke/document_pdf_smoke_20260507_144618.md` (ignored runtime report).
- Artifact directory: `F:\Dev\AIstock_artifacts\event_signal_pdf_smoke`.
- Sample rows analyzed:
  - `301139.SZ`, announcement `5740065`, event `regulatory_investigation_penalty`, PDF bytes `58106`, DeepSeek risk level `critical`, confidence `0.95`.
  - `300068.SZ`, announcement `5740063`, event `debt_default_overdue`, PDF bytes `64218`, DeepSeek risk level `high`, confidence `0.95`.

## Test Port Validation

- Attempted FastAPI test-port startup on `127.0.0.1:8012` with ingestion, strategy, paper, node-health, HMM, QE evolution, and QE experiment schedulers disabled.
- Startup failed before serving `/api/v1/health` due a pre-existing clean-worktree dependency issue: `backend/routers/rl_execution.py` imports `backend.services.rl_execution`, but `backend/services/rl_execution/` is ignored by `.gitignore` and is absent from this worktree.
- This failure is outside the event-signal/PDF module; production root currently has ignored local files under `backend/services/rl_execution/`, which explains why the issue can be hidden outside clean worktrees.
- Production port `8001` was not touched.

## Business Outcome

- Verified that recent announcement source URLs can be resolved to PDF bytes despite Eastmoney's JS cookie gate.
- Verified that only selected evidence chunks are sent to DeepSeek, not entire PDFs.
- Verified that DeepSeek returns structured JSON suitable for a later persisted LLM review table.
- Current implementation remains a smoke validator; it does not generate trading actions or modify signal tables.

## Residual Risks

- Main FastAPI startup on a clean worktree remains blocked by the pre-existing ignored RL service package until that module is committed, restored, or made optional.
- The PDF smoke parser is validated on Eastmoney samples; CNInfo download handling has unit coverage but still needs a live CNInfo sample before production use.
