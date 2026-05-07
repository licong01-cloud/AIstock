# L2 Event Signal Refined Document Queue Validation

Date: 2026-05-07 Asia/Shanghai

Scope:

- Continue event-signal R&D inside the local data / event_signal module only.
- Refine the future announcement PDF / LLM queue with title-level materiality and deterministic dedupe.
- Do not create DB tables, do not download PDFs, do not call LLMs, and do not connect signals to QE / Selection Center / Paper v2 / QMT / simulated or live trading.

Implemented scope:

- Added `backend/services/event_signal/document_queue_refiner.py`.
  - Extracts title-level amount and percentage materiality from Chinese announcement titles.
  - Uses configured thresholds: amount >= 50,000,000 yuan or ratio >= 5%.
  - Downgrades amount-sensitive categories without materiality evidence to `defer_until_materiality`.
  - Downgrades context-sensitive categories without linked financial / repeat context to `sample_only`.
  - Builds stable dedupe keys from `ts_code + event_type + time bucket + normalized title signature`.
- Added `backend/services/event_signal/document_review_refined_queue_report.py`.
  - Streams `market.ann_event_classification + market.anns` in batches.
  - Applies the refiner to full historical title classifications.
  - Produces read-only JSON/Markdown reports under `reports/event_signal/document_review_refined_queue/`.

Targeted validation:

```powershell
pytest backend/tests/event_signal/test_document_queue_refiner.py -q
# 7 passed in 0.82s

pytest backend/tests/event_signal/test_document_queue_refiner.py backend/tests/event_signal/test_document_review_refined_queue_report.py -q
# 8 passed in 1.04s
```

Full event-signal module regression:

```powershell
$env:PYTHONIOENCODING='utf-8'
pytest backend/tests/announcements/test_title_classifier.py backend/tests/event_signal -q
# 99 passed in 2.63s
```

Compile / diff validation:

```powershell
$env:PYTHONIOENCODING='utf-8'
python -m py_compile `
  backend\services\announcements\title_classifier.py `
  backend\services\event_signal\document_preprocessor.py `
  backend\services\event_signal\document_review_planner.py `
  backend\services\event_signal\document_review_queue_report.py `
  backend\services\event_signal\document_queue_refiner.py `
  backend\services\event_signal\document_review_refined_queue_report.py `
  backend\services\event_signal\financial_event_study.py `
  backend\services\event_signal\financial_signal_policy_diagnostics.py `
  backend\services\event_signal\st_announcement_adapter.py `
  backend\services\event_signal\st_event_study.py `
  backend\services\event_signal\st_signal_quality_report.py `
  scripts\classify_announcement_titles_v0.py
# passed

git diff --check
# passed; only LF->CRLF working-copy warnings for existing edited files
```

Isolation validation:

```powershell
rg -n "document_preprocessor|document_review_planner|document_review_queue_report|document_queue_refiner|document_review_refined_queue_report|financial_event_study|financial_signal_policy_diagnostics|st_event_study|st_signal_quality_report|STFirstAnnouncementEventSignalAdapter|unified_event_signal_rules_st_first_v1_20260506|financial_event_study_" `
  backend/services/selection_center `
  backend/services/paper_trading_v2 `
  backend/services/quantevolver `
  backend/infra/qmt_client.py `
  backend/routers/qmt.py -S
# no matches
```

Read-only DB report validation:

The feature worktree has no `.env`, so the validation explicitly loaded the root runtime `.env` read-only before opening DB connections. No schema/data write was performed.

Smoke run:

```powershell
$env:PYTHONIOENCODING='utf-8'
python -c "from pathlib import Path; import datetime as dt; from dotenv import load_dotenv; load_dotenv(r'F:\Dev\AIstock\.env', override=False); from backend.services.event_signal.document_review_refined_queue_report import run_refined_queue_report; payload=run_refined_queue_report(start_date=dt.date(2018,8,1), end_date=dt.date(2026,5,6), limit=1000, batch_size=500); print({'report_id': payload['report_id'], 'raw': {k: payload['summary']['raw'][k] for k in ['rows','document_rows','llm_rows','material_rows','by_action']}, 'deduped_document_rows': payload['summary']['deduped_document_queue']['document_rows']})"
```

Output:

- `rows=1000`
- `document_rows=12`
- `llm_rows=242`
- `material_rows=12`
- `deduped_document_rows=12`

Full historical run:

```powershell
$env:PYTHONIOENCODING='utf-8'
python -c "from pathlib import Path; import datetime as dt; from dotenv import load_dotenv; load_dotenv(r'F:\Dev\AIstock\.env', override=False); from backend.services.event_signal.document_review_refined_queue_report import run_refined_queue_report; payload=run_refined_queue_report(start_date=dt.date(2018,8,1), end_date=dt.date(2026,5,6), batch_size=50000); print({'report_id': payload['report_id'], 'outputs': payload['outputs'], 'raw_rows': payload['summary']['raw']['rows'], 'raw_document_rows': payload['summary']['raw']['document_rows'], 'raw_llm_rows': payload['summary']['raw']['llm_rows'], 'material_rows': payload['summary']['raw']['material_rows'], 'deduped_document_rows': payload['summary']['deduped_document_queue']['document_rows'], 'by_action': payload['summary']['raw']['by_action']})"
```

Output:

- Report: `reports/event_signal/document_review_refined_queue/document_review_refined_queue_20180801_20260506_20260507_133802.json`
- Markdown: `reports/event_signal/document_review_refined_queue/document_review_refined_queue_20180801_20260506_20260507_133802.md`
- `raw_rows=5132106`
- `raw_document_rows=136314`
- `deduped_document_rows=133347`
- `raw_llm_rows=1178756`
- `material_rows=62600`
- `by_action.defer_until_materiality=518459`
- `by_action.document_required=136314`
- `by_action.sample_only=1042442`
- `by_action.skip=3434891`

Interpretation:

- The previous coarse plan had `document_llm_candidate_rows=1293261`.
- The refined first-wave document-required set is reduced to `136314` raw rows and `133347` deduped rows.
- A large `518459` rows are now explicitly deferred until materiality evidence is available, avoiding premature PDF download.
- `sample_only=1042442` remains too large for LLM and should be sampled by event type / year rather than downloaded.

Business outcomes verified:

- Title-only ST / delisting / neutral / structured financial categories remain excluded from automatic PDF/LLM.
- Amount-sensitive event types no longer become document-download candidates without title/context materiality.
- Context-sensitive broad P2 categories are sample-only until linked to financial anomaly / repeat inquiry context.
- The logic is deterministic and can be reused later before persistent queue-table creation.

Residual risks / next work:

- `document_required=136314` is much smaller than the coarse queue but still high for immediate PDF downloading.
- Next step should create a deterministic first-wave sampler/cap plan, for example per event type/year caps, latest-period priority, and manual review sampling before persistent queue DDL.
- Actual PDF download success, document parser quality, OCR, and LLM JSON schema remain unimplemented in this phase.
- Production backend port `8001` was not restarted or touched.
