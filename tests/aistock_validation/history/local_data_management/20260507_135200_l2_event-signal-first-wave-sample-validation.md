# L2 Event Signal First-wave Document Sample Validation

Date: 2026-05-07 Asia/Shanghai

Scope:

- Continue event-signal R&D after refined document queue sizing.
- Add a deterministic first-wave sampler so parser / LLM validation can start from a capped sample rather than the full refined document queue.
- No DB schema changes, no PDF downloads, no LLM calls, no trading-consumer integration.

Implemented scope:

- Added `backend/services/event_signal/document_first_wave_sampler.py`.
  - Selects only refined `require_document=True` decisions.
  - Caps by total rows, event type, and event-type/year bucket.
  - Sorts deterministically by priority, materiality, amount, recency, symbol, and announcement id.
- Added `backend/services/event_signal/document_first_wave_report.py`.
  - Streams historical classifications in batches.
  - Reuses the refined queue decision logic.
  - Dedupes the refined document queue, then selects a capped first-wave sample.
  - Writes read-only JSON/Markdown reports under `reports/event_signal/document_first_wave/`.

Targeted validation:

```powershell
pytest backend/tests/event_signal/test_document_first_wave_sampler.py -q
# 2 passed in 0.49s

pytest backend/tests/event_signal/test_document_first_wave_sampler.py backend/tests/event_signal/test_document_first_wave_report.py -q
# 3 passed in 0.52s
```

Full event-signal module regression:

```powershell
$env:PYTHONIOENCODING='utf-8'
pytest backend/tests/announcements/test_title_classifier.py backend/tests/event_signal -q
# 102 passed in 1.52s
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
  backend\services\event_signal\document_first_wave_sampler.py `
  backend\services\event_signal\document_first_wave_report.py `
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
rg -n "document_preprocessor|document_review_planner|document_review_queue_report|document_queue_refiner|document_review_refined_queue_report|document_first_wave_sampler|document_first_wave_report|financial_event_study|financial_signal_policy_diagnostics|st_event_study|st_signal_quality_report|STFirstAnnouncementEventSignalAdapter|unified_event_signal_rules_st_first_v1_20260506|financial_event_study_" `
  backend/services/selection_center `
  backend/services/paper_trading_v2 `
  backend/services/quantevolver `
  backend/infra/qmt_client.py `
  backend/routers/qmt.py -S
# no matches
```

Read-only DB report validation:

The feature worktree has no `.env`, so validation explicitly loaded `F:\Dev\AIstock\.env` read-only before connecting to the local DB.

Smoke run:

```powershell
$env:PYTHONIOENCODING='utf-8'
python -c "from pathlib import Path; import datetime as dt; from dotenv import load_dotenv; load_dotenv(r'F:\Dev\AIstock\.env', override=False); from backend.services.event_signal.document_first_wave_report import run_first_wave_report; payload=run_first_wave_report(start_date=dt.date(2018,8,1), end_date=dt.date(2026,5,6), limit=1000, batch_size=500, total_cap=200); print({'report_id': payload['report_id'], 'eligible': payload['summary']['eligible_deduped_document_rows'], 'selected': payload['summary']['first_wave']['selected_rows'], 'by_event_type': payload['summary']['first_wave']['by_event_type']})"
```

Output:

- `eligible=12`
- `selected=12`
- `by_event_type={'litigation_arbitration_freeze': 1, 'pledge_shareholder_change_reduction': 8, 'regulatory_investigation_penalty': 3}`

Full historical run:

```powershell
$env:PYTHONIOENCODING='utf-8'
python -c "from pathlib import Path; import datetime as dt; from dotenv import load_dotenv; load_dotenv(r'F:\Dev\AIstock\.env', override=False); from backend.services.event_signal.document_first_wave_report import run_first_wave_report; payload=run_first_wave_report(start_date=dt.date(2018,8,1), end_date=dt.date(2026,5,6), batch_size=50000, total_cap=5000, per_event_year_cap=120); print({'report_id': payload['report_id'], 'outputs': payload['outputs'], 'source_rows': payload['source_rows_processed'], 'eligible': payload['summary']['eligible_deduped_document_rows'], 'selected': payload['summary']['first_wave']['selected_rows'], 'material': payload['summary']['first_wave']['material_rows'], 'by_event_type': payload['summary']['first_wave']['by_event_type']})"
```

Output:

- Report: `reports/event_signal/document_first_wave/document_first_wave_20180801_20260506_20260507_134744.json`
- Markdown: `reports/event_signal/document_first_wave/document_first_wave_20180801_20260506_20260507_134744.md`
- `source_rows=5132106`
- `eligible_deduped_document_rows=133347`
- `selected_rows=5000`
- `material_rows=1260`
- By event type:
  - `audit_opinion_internal_control_risk=990`
  - `capital_occupation_illegal_guarantee=1080`
  - `control_change_ma_restructuring=24`
  - `debt_default_overdue=716`
  - `guarantee_financial_assistance_related_party=481`
  - `litigation_arbitration_freeze=265`
  - `pledge_shareholder_change_reduction=364`
  - `regulatory_investigation_penalty=1080`

Business outcomes verified:

- The first PDF/LLM validation wave can be capped to 5,000 deterministic samples from 133,347 eligible deduped document candidates.
- The sample remains stratified across high-risk event types and historical years, instead of selecting only recent or only largest categories.
- Selection is still outside trading systems and cannot change alpha, backtest, paper, or live behavior.

Residual risks / next work:

- The first-wave sampler only selects candidate metadata; actual document fetch / parse / OCR / chunk-quality validation is still not implemented.
- Event-type caps are conservative defaults for validation, not production queue policy.
- Before persistent queue DDL, confirm whether the first-wave sample size should stay at 5,000 or be lowered for the first parser smoke.
- Production backend port `8001` was not restarted or touched.
