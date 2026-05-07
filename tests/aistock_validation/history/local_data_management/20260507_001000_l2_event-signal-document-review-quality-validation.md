# L2 Event Signal Document Review / Quality Validation

Date: 2026-05-07 Asia/Shanghai

Scope:

- Continue event-signal R&D inside the local data / event_signal module only.
- Add deterministic planning and diagnostics for future announcement PDF / LLM work.
- Keep QE, Selection Center, Paper v2, simulated trading, QMT, and production backend port 8001 untouched.

Implemented scope:

- Added `backend/services/event_signal/document_review_planner.py`.
  - Determines whether a classified announcement should be skipped, sampled, document-candidate, or first-batch document-required.
  - Keeps ST / delisting / bond / periodic-report / neutral titles out of PDF/LLM by default.
  - Treats audit opinion, regulatory penalty, capital occupation / illegal guarantee, and debt default as first-batch document-required categories.
  - Keeps structured financial events under structured data first; only samples financial text when linked anomaly context is provided.
- Added `backend/services/event_signal/document_review_queue_report.py`.
  - Read-only DB queue-size estimator grouped by title classification result.
  - Does not create queue tables, download PDFs, call LLMs, or write signals.
- Added `backend/services/event_signal/st_signal_quality_report.py`.
  - Read-only quality report for ST-first signal cross-check evidence, stock_st_events recall, and bond-like leakage.
- Added `backend/services/event_signal/financial_signal_policy_diagnostics.py`.
  - Research-only diagnostic over financial event-study aggregate metrics.
  - Recommends first-stage risk warning / record-only handling without enabling alpha, hard-block, or trading consumption.

Targeted validation:

```powershell
pytest backend/tests/event_signal/test_document_review_planner.py -q
# 6 passed in 0.33s

pytest backend/tests/event_signal/test_st_signal_quality_report.py -q
# 5 passed in 0.28s

pytest backend/tests/event_signal/test_financial_signal_policy_diagnostics.py -q
# 4 passed in 0.24s

pytest backend/tests/event_signal/test_document_review_queue_report.py -q
# 1 passed in 0.30s

pytest backend/tests/event_signal/test_st_signal_quality_report.py backend/tests/event_signal/test_document_review_planner.py backend/tests/event_signal/test_financial_signal_policy_diagnostics.py -q
# 15 passed in 0.32s
```

Module regression:

```powershell
$env:PYTHONIOENCODING='utf-8'
pytest backend/tests/announcements/test_title_classifier.py backend/tests/event_signal -q
# 91 passed in 0.86s
```

Compile / diff validation:

```powershell
$env:PYTHONIOENCODING='utf-8'
python -m py_compile `
  backend\services\announcements\title_classifier.py `
  backend\services\event_signal\announcement_adapter.py `
  backend\services\event_signal\document_preprocessor.py `
  backend\services\event_signal\document_review_planner.py `
  backend\services\event_signal\document_review_queue_report.py `
  backend\services\event_signal\financial_event_adapter.py `
  backend\services\event_signal\financial_event_study.py `
  backend\services\event_signal\financial_signal_policy_diagnostics.py `
  backend\services\event_signal\st_announcement_adapter.py `
  backend\services\event_signal\st_event_study.py `
  backend\services\event_signal\st_signal_quality_report.py `
  scripts\classify_announcement_titles_v0.py
# passed

git diff --check
# passed; only existing LF->CRLF warnings for title_classifier/test/design doc
```

Isolation validation:

```powershell
rg -n "document_preprocessor|document_review_planner|document_review_queue_report|financial_event_study|financial_signal_policy_diagnostics|st_event_study|st_signal_quality_report|STFirstAnnouncementEventSignalAdapter|unified_event_signal_rules_st_first_v1_20260506|financial_event_study_" `
  backend/services/selection_center `
  backend/services/paper_trading_v2 `
  backend/services/quantevolver `
  backend/infra/qmt_client.py `
  backend/routers/qmt.py -S
# no matches
```

Read-only DB report validation:

Initial direct run from the feature worktree failed because the worktree has no `.env` and no DB password in process env. Retried by explicitly loading the root runtime `.env` read-only; no code or DB schema was changed.

```powershell
$env:PYTHONIOENCODING='utf-8'
python -c "from pathlib import Path; import datetime as dt; from dotenv import load_dotenv; load_dotenv(r'F:\Dev\AIstock\.env', override=False); from backend.services.event_signal.st_signal_quality_report import run_st_signal_quality_report; payload=run_st_signal_quality_report(start_date=dt.date(2018,8,1), end_date=dt.date(2026,5,6)); print({'report_id': payload['report_id'], 'outputs': payload['outputs'], 'signal_rows': payload['signal_rows'], 'match_rate': payload['cross_check']['match_rate'], 'recall_rate': payload['stock_st_recall']['recall_rate'], 'bond_leakage': payload['bond_leakage']})"
```

Output:

- Report: `reports/event_signal/st_first_quality/st_first_signal_quality_20180801_20260506_20260507_000605.json`
- Markdown: `reports/event_signal/st_first_quality/st_first_signal_quality_20180801_20260506_20260507_000605.md`
- `signal_rows=12038`
- `stock_st_events recall_rate=0.7813698630136986`
- `embedded cross_check match_rate=0.17070942016946336`
- `bond_like_fact_rows=27154`
- `bond_like_active_signal_rows=0`
- `leakage_detected=false`

Interpretation:

- Bond-like convertible/corporate bond titles are not leaking into active ST stock risk signals.
- ST title signals cover most independent `stock_st_events` rows within the 5-day matching rule.
- Embedded cross-check match rate is low because many ST/delisting-risk warning titles are repeated or far from the canonical `stock_st_events` pub/imp date; this is an analysis signal, not a trading integration issue.

Document review queue estimate:

```powershell
$env:PYTHONIOENCODING='utf-8'
python -c "from pathlib import Path; import datetime as dt; from dotenv import load_dotenv; load_dotenv(r'F:\Dev\AIstock\.env', override=False); from backend.services.event_signal.document_review_queue_report import run_document_review_queue_report; payload=run_document_review_queue_report(start_date=dt.date(2018,8,1), end_date=dt.date(2026,5,6)); print({'report_id': payload['report_id'], 'outputs': payload['outputs'], 'source_rows': payload['summary']['source_rows'], 'by_action': payload['summary']['by_action'], 'by_llm_stage': payload['summary']['by_llm_stage']})"
```

Latest output:

- Report: `reports/event_signal/document_review_queue/document_review_queue_20180801_20260506_20260507_001154.json`
- Markdown: `reports/event_signal/document_review_queue/document_review_queue_20180801_20260506_20260507_001154.md`
- `source_rows=5132106`
- `by_action.skip=3434891`
- `by_action.document_required=89645`
- `by_action.document_candidate=1203616`
- `by_action.sample_only=403954`
- `by_llm_stage.first_batch=89645`
- `by_llm_stage.sampled=1607570`
- `document_llm_candidate_rows=1293261`

Interpretation:

- Even after title-only exclusions, raw document candidates are still too large for immediate full download / LLM.
- First real download phase should add context thresholds, dedupe, amount parsing, and financial anomaly linkage before creating persistent fetch queues.
- ST / delisting / periodic report / structured financial cases stay out of PDF/LLM by default.

Financial policy diagnostics:

```powershell
$env:PYTHONIOENCODING='utf-8'
python -m backend.services.event_signal.financial_signal_policy_diagnostics reports\event_signal\financial\financial_event_study_20240101_20260506_full_20260506_235123.json
```

Output:

- `reports/event_signal/financial/financial_event_study_20240101_20260506_full_20260506_235123_policy_diagnostics.json`
- Summary: `rows=10`, `warn_review=6`, `record_only=4`, `alpha_enabled=0`, `hard_block_candidates=0`

Business outcomes verified:

- Future PDF/LLM analysis now has a deterministic pre-queue decision layer.
- The queue-size report quantifies why all P2 announcements must not be downloaded or sent to LLM directly.
- ST hard-risk signal quality has a repeatable read-only report, and bond-like leakage is zero in the current DB snapshot.
- Financial structured signals remain independent risk warnings / record-only research signals; no alpha overlay, hard block, or trading consumer is enabled.

Residual risks / next work:

- `document_required=89645` is still too high for the first real PDF download wave; next refinement should add amount thresholds, per-symbol/event dedupe, and financial-anomaly context before persistent queue tables.
- `stock_st_events` recall is useful but not a perfect oracle because some title risk warnings are repeated, prospective, or not represented one-to-one in `stock_st_events`.
- The generated reports are under `reports/` and currently not tracked by git; this validation record keeps the reproducible paths and key metrics.
- Production backend port `8001` was not restarted or touched.
