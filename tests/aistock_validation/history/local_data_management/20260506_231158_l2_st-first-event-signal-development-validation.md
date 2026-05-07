# ST-first Event Signal Development Validation

- Module: local_data_management / event_signal
- Level: L2
- Date: 2026-05-06T23:11:58+08:00
- Worktree: `F:\Dev\AIstock_worktrees\event-signal-st-llm-design-20260506`
- Branch: `codex/event-signal-st-llm-design-20260506`
- Base commit at run start: `de9cb5a`
- Production backend impact: none; port `8001` was not restarted or touched.

## Scope

- Implemented ST-first announcement title rule version `aistock_announcement_title_rules_v1_20260506`.
- Added independent ST-first adapter from `market.ann_event_classification` to `market.event_fact` / `market.event_signal`.
- Added offline ST-first event-study report script under `backend/services/event_signal/st_event_study.py`.
- Added regression tests for ST title classification, ST adapter, and ST event study.
- Out of scope: QE, Selection Center, Paper v2, QMT, simulated/live trading consumers, frontend UI, LLM/PDF analysis, and schema changes.

## Business Goal

- Hard-risk ST / delisting titles should produce independent event signals that can later block buys or warn users, while bond redemption/delisting notices must not be misclassified as stock hard blocks.
- Backtest and future live/paper use the same versioned rules, with live visibility handled by `available_at` / first-seen semantics outside this ST validation slice.
- ST-first research must remain outside trading consumers until explicitly integrated in a later phase.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| ST title rules | ST imposed / delisting are P0, ST removal application is P2, confirmed removal is P3, bond delisting/repayment is P4 | `pytest backend/tests/announcements/test_title_classifier.py ...` | PASS |
| ST adapter | ST classifications create event facts; only stock risk/review types create active event signals; bond notices stay fact-only | `pytest backend/tests/event_signal/test_st_announcement_adapter.py` and DB counts | PASS |
| Event study | Reads independent `market.event_signal`, computes T-1/T0/T+1/T+2 and T0_T2 returns with suspend/down-limit/missing flags | `pytest backend/tests/event_signal/test_st_event_study.py` and full report | PASS |
| Data backfill | Full v1 title classification and ST-first event signals are present for 2018-08-01 through 2026-05-06 | DB backfill counts below | PASS |
| Consumer isolation | No ST-first adapter/rule/event-study references in QE, Selection Center, Paper v2, or QMT consumers | `rg` isolation scan returned no matches | PASS |
| Guardrails | Python compile, event-signal regression suite, diff whitespace check | commands below | PASS |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'
pytest backend/tests/announcements/test_title_classifier.py backend/tests/event_signal/test_announcement_adapter.py backend/tests/event_signal/test_st_announcement_adapter.py backend/tests/event_signal/test_st_event_study.py -q
pytest backend/tests/event_signal -q
python -m py_compile backend\services\announcements\title_classifier.py backend\services\event_signal\st_announcement_adapter.py backend\services\event_signal\st_event_study.py scripts\classify_announcement_titles_v0.py
git diff --check

# Full event-study rerun was executed with load_dotenv(F:/Dev/AIstock/.env)
python - <<'PY'
from pathlib import Path
from backend.services.event_signal.st_event_study import run_event_study
summary = run_event_study(output_dir=Path('reports/event_signal/st_first'))
print(summary)
PY

rg -n "STFirstAnnouncementEventSignalAdapter|unified_event_signal_rules_st_first_v1_20260506|st_event_study|stock_st_imposed|stock_delisting" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
```

## Automated Test Results

- Targeted ST/event tests: `24 passed in 0.52s`.
- Full `backend/tests/event_signal`: `52 passed in 1.04s`.
- `py_compile`: passed for changed service/script files.
- `git diff --check`: passed; only CRLF normalization warnings were reported for existing working-copy line endings.

## Data Backfill Evidence

- Title classification command completed earlier with `--start-date 2018-08-01 --end-date 2026-05-06 --persist --no-generate-signals --truncate-version`.
- Processed announcement rows: `5,132,106`.
- V1 title risk distribution:
  - `P4_NEUTRAL`: `3,326,804`
  - `P2_REVIEW`: `1,291,408`
  - `P3_POSITIVE_CANDIDATE`: `404,401`
  - `P1_HIGH`: `90,563`
  - `P0_BLOCK`: `18,930`
- ST-first event fact rows under `unified_event_signal_rules_st_first_v1_20260506`: `39,661`.
- ST-first active event signal rows: `12,048`.
- Bond-like delisting/repayment signal rows: `0` by design; these are archived as facts only.

## ST Event Counts

| Event type | Fact rows | Signal rows | Expected treatment |
|---|---:|---:|---|
| `stock_st_imposed` | 4,183 | 4,183 | P0 `block_buy` |
| `stock_delisting_risk_warning` | 3,993 | 3,993 | P0 `block_buy` |
| `stock_delisting_confirmed` | 2,196 | 2,196 | P0 `block_buy` |
| `stock_st_added_or_continued` | 918 | 918 | P1 `warn_high` |
| `stock_st_removal_applied` | 758 | 758 | P2 `warn_review` |
| `stock_st_removed_confirmed` | 447 | 0 | P3 fact-only |
| `convertible_bond_delisting_or_redemption` | 17,752 | 0 | P4 fact-only |
| `generic_bond_delisting_or_repayment` | 9,414 | 0 | P4 fact-only |

## Event Study Evidence

- Full report id: `st_first_event_study_all_all_20260506_233832`.
- Output JSON: `reports/event_signal/st_first/st_first_event_study_all_all_20260506_233832.json`.
- Output Markdown: `reports/event_signal/st_first/st_first_event_study_all_all_20260506_233832.md`.
- Output detail CSV: `reports/event_signal/st_first/st_first_event_study_all_all_20260506_233832_details.csv`.
- Total active signal rows in scope: `12,048`.
- Deduped event rows used for event study: `11,048`.
- Detail rows: `55,240`.
- Deduped event counts:
  - `stock_st_imposed`: `4,085`
  - `stock_delisting_risk_warning`: `3,732`
  - `stock_delisting_confirmed`: `1,648`
  - `stock_st_added_or_continued`: `902`
  - `stock_st_removal_applied`: `681`
- Key result sanity checks:
  - `stock_st_imposed` T0 mean raw return `-1.3339%`, T0_T2 mean raw return `-1.5621%`.
  - `stock_st_imposed` T0 down-limit rate `13.4394%`, T0_T2 down-limit-in-window rate `20.8813%`.
  - `stock_st_removal_applied` T0 mean raw return `+0.6733%`, T0_T2 mean raw return `+1.9643%`.
  - Confirmed delisting events have high missing/suspended rates, which is expected around delisting periods and must be handled separately before using returns for alpha scoring.

## Consumer Isolation Evidence

- Isolation scan command returned no matches in:
  - `backend/services/selection_center`
  - `backend/services/paper_trading_v2`
  - `backend/services/quantevolver`
  - `backend/infra/qmt_client.py`
  - `backend/routers/qmt.py`
- No production datasets, model assets, HMM assets, Qlib bin data, QMT code, QE runtime, Selection Center runtime, Paper v2 runtime, or frontend production service was modified.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Old broad title rules classified bond-like delisting/redemption/repayment notices as stock hard risk | `delisting_or_risk_warning` matched generic delisting terms before instrument context | Added ordered bond exclusions and separate stock ST/delisting event types | Title classifier tests and DB smoke count show bond-like rows no longer create P0 signals |
| Initial event-study summary mixed deduped event rows and total active signal rows | Study query dedupes by `(ts_code, effective_trade_date, event_type)` but summary label was ambiguous | Added `count_event_signals`; summary now reports both `signal_rows` and `deduped_events` | Full report `st_first_event_study_all_all_20260506_233832` shows `signal_rows=12048`, `deduped_events=11048` |
| Initial cumulative T0_T2 row did not carry suspend/down-limit/missing flags across the window | Only single-day detail rows had market-state flags | Carried T0/T+1/T+2 flags into cumulative rows | Event-study tests and full report rerun passed |

## Result

- Final status for ST-first independent signal slice: PASS.
- Need production backend restart: no.
- Need dev service restart: no.
- Remaining risks:
  - Event-study returns around delisting have many missing/suspended rows; use these as risk validation, not direct alpha scores.
  - ST-first signal consumption by backtest/paper/live is intentionally not integrated yet.
  - LLM/PDF preprocessing and financial structured signals remain future phases.
