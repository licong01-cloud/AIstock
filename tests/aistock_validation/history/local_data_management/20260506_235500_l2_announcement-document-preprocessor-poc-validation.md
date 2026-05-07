# Announcement Document Preprocessor PoC Validation

- Module: local_data_management / event_signal
- Level: L2
- Date: 2026-05-06T23:55:00+08:00
- Worktree: `F:\Dev\AIstock_worktrees\event-signal-st-llm-design-20260506`
- Branch: `codex/event-signal-st-llm-design-20260506`
- Production backend impact: none; port `8001` was not restarted or touched.

## Scope

- Added deterministic pre-LLM document preprocessing PoC: `backend/services/event_signal/document_preprocessor.py`.
- Added tests: `backend/tests/event_signal/test_document_preprocessor.py`.
- The PoC accepts already-extracted announcement text/pages and outputs small evidence chunks with page, section, hash, score, matched keywords, and token estimate.
- Out of scope: PDF download, PDF binary parsing, OCR, LLM calls, DB schema, signal generation, and any trading consumer integration.

## Business Goal

- Future LLM analysis should never receive whole PDF files by default.
- The system should first remove repeated headers, page footers, board guarantee boilerplate, and low-information text, then route only event-relevant evidence chunks to a future structured extractor.
- The preprocessing stage must be deterministic and auditable so backtest/paper/live can use consistent evidence selection once LLM analysis is introduced.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Page splitting | Text with manual page breaks becomes ordered pages | unit test | PASS |
| Boilerplate removal | Repeated security headers, page numbers, and board guarantee lines are dropped | unit test | PASS |
| Event routing | Regulatory, debt, capital occupation, litigation, inquiry, audit, and performance event keywords are separated | unit test | PASS |
| Evidence chunks | Chunks retain section title, page number, stable hash, matched keywords, and token estimate | unit test | PASS |
| No LLM/PDF side effects | Module has no network, PDF parser, LLM, DB write, or trading consumer dependency | code review + py_compile | PASS |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'
pytest backend/tests/event_signal/test_document_preprocessor.py -q
python -m py_compile backend\services\event_signal\document_preprocessor.py
pytest backend/tests/announcements/test_title_classifier.py backend/tests/event_signal -q
python -m py_compile backend\services\announcements\title_classifier.py backend\services\event_signal\st_announcement_adapter.py backend\services\event_signal\st_event_study.py backend\services\event_signal\financial_event_study.py backend\services\event_signal\document_preprocessor.py scripts\classify_announcement_titles_v0.py
git diff --check
```

## Automated Test Results

- Document preprocessor tests: `6 passed in 0.36s`.
- Full announcement/event-signal regression slice: `74 passed in 1.03s`.
- `py_compile`: passed for changed announcement/event-signal service files.
- `git diff --check`: passed; only CRLF normalization warnings were reported for existing working-copy line endings.

## Evidence Summary

- Repeated headers such as `证券代码/证券简称/公告编号` and page numbering are removed.
- Board guarantee boilerplate is removed and does not enter chunks.
- Event-specific routing distinguishes `regulatory_investigation_penalty` from `debt_default_overdue` keyword sets.
- Capital-occupation sample returns chunks containing `资金占用/占用资金` and trims the output below original document length.
- Each chunk has stable SHA256 hash and source block ids for future `event_llm_extract` or review queue traceability.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Initial shell-written file corrupted Chinese text into `?`, causing regex compile failure | PowerShell command text encoding did not preserve non-ASCII source content | Rewrote files through `apply_patch` so UTF-8 Chinese literals are preserved | `test_document_preprocessor.py` collection and tests passed |
| Initial block splitter lost blank-line paragraph separators | Empty lines were treated as dropped boilerplate before paragraph splitting | Preserve empty lines as separators before boilerplate filtering | `test_build_evidence_chunks_prefers_scored_risk_blocks_and_hashes_are_stable` passed |

## Result

- Final status for pre-LLM document preprocessing PoC: PASS.
- Need production backend restart: no.
- Need dev service restart: no.
- Remaining risks:
  - This is text-level preprocessing only; actual PDF/OCR parser quality still needs a later sample evaluation.
  - No DB tables were added yet; future chunk persistence must add full table/column comments.
  - Keyword routing is a deterministic baseline and should be expanded from real high-risk announcement samples before LLM rollout.
