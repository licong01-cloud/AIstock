# L2 Early Financial Distress Research Validation

Date: 2026-05-07 Asia/Shanghai
Scope: event_signal research only; validate whether structured `forecast` / `express` / `fina_indicator` risk events can provide early warning before ST / delisting-risk cycles.
Worktree: `F:\Dev\AIstock_worktrees\event-signal-policy-20260507`
Production backend impact: none; port `8001` was not restarted or touched.

## Implemented Scope

- Added research design document `docs/analysis/event_signal_early_financial_distress_research_design_20260507.md`.
- Added read-only research script `backend/services/event_signal/early_financial_distress_research.py`.
- Added unit tests `backend/tests/event_signal/test_early_financial_distress_research.py`.
- Generated full-window read-only report for 2018-08-01 through 2026-05-07.
- Out of scope: DB schema changes, QE/Selection/Paper/QMT/simulated/live trading consumers, PDF downloads, LLM calls, alpha overlay, hard block, forced exit.

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest backend/tests/event_signal/test_early_financial_distress_research.py -q
python -m py_compile backend/services/event_signal/early_financial_distress_research.py backend/tests/event_signal/test_early_financial_distress_research.py

$env:TDX_DB_HOST='127.0.0.1'
$env:TDX_DB_PORT='5432'
$env:TDX_DB_NAME='aistock'
$env:TDX_DB_USER='postgres'
$env:TDX_DB_PASSWORD='***'
python -m backend.services.event_signal.early_financial_distress_research `
  --start-date 2018-08-01 `
  --end-date 2026-05-07 `
  --lookback-days 365 `
  --cycle-gap-days 180 `
  --output-dir reports/event_signal/early_financial_distress
```

## Automated Test Results

- `backend/tests/event_signal/test_early_financial_distress_research.py`: `5 passed in 0.26s`.
- `py_compile`: passed for the research script and test.`r`n- Event-signal module regression: `119 passed in 2.65s` for `backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q`.

## Read-only DB Report

- Report id: `early_financial_distress_20180801_20260507_20260507_235739`.
- Output JSON: `reports/event_signal/early_financial_distress/early_financial_distress_20180801_20260507_20260507_235739.json`.
- Output Markdown: `reports/event_signal/early_financial_distress/early_financial_distress_20180801_20260507_20260507_235739.md`.
- Output CSV: none by default.

## Data Scope

```text
┌──────────────────────────────────────┬────────┐
│ Item                                 │ Value  │
├──────────────────────────────────────┼────────┤
│ Financial risk signals loaded        │ 70,755 │
│ Financial risk signals in window     │ 69,941 │
│ Financial signal symbols in window   │ 5,705  │
│ ST target events                     │ 11,290 │
│ ST target cycles                     │ 1,366  │
│ ST symbols                           │ 748    │
└──────────────────────────────────────┴────────┘
```

## ST-cycle Coverage By Prior Financial Risk Signals

Lookback: 365 natural days before each ST/delisting-risk cycle start.

```text
┌──────────────────────────────────────┬────────────┐
│ Metric                               │ Value      │
├──────────────────────────────────────┼────────────┤
│ ST cycles                            │ 1,366      │
│ Cycles with prior financial signal   │ 1,251      │
│ Coverage rate                        │ 91.58%     │
│ Median earliest lead                 │ 302.0 days │
│ Median latest lead                   │ 91.0 days  │
│ Mean latest lead                     │ 109.94 days│
└──────────────────────────────────────┴────────────┘
```

Coverage by first ST-cycle event type:

```text
┌──────────────────────────────┬────────┬─────────┬──────────┐
│ Primary ST event type         │ Cycles │ Matched │ Coverage │
├──────────────────────────────┼────────┼─────────┼──────────┤
│ stock_st_imposed              │ 850    │ 783     │ 92.12%   │
│ stock_delisting_risk_warning  │ 275    │ 257     │ 93.45%   │
│ stock_st_added_or_continued   │ 184    │ 173     │ 94.02%   │
│ stock_delisting_confirmed     │ 57     │ 38      │ 66.67%   │
└──────────────────────────────┴────────┴─────────┴──────────┘
```

Source contribution among matched cycles:

```text
┌──────────────────────────┬────────────────┐
│ Source type              │ Matched cycles │
├──────────────────────────┼────────────────┤
│ tushare_forecast         │ 1,085          │
│ tushare_fina_indicator   │ 942            │
│ tushare_express          │ 238            │
│ financial_relation       │ 235            │
└──────────────────────────┴────────────────┘
```

## Per-signal Future ST Precision

Horizon rates use censoring: a signal is eligible for a horizon only if that horizon is observable before 2026-05-07.

```text
┌──────────────────────────┬─────────┬───────────────┬──────────────┬────────────────┐
│ Source type              │ Signals │ Precision 90d │ Precision 180d│ Precision 365d │
├──────────────────────────┼─────────┼───────────────┼──────────────┼────────────────┤
│ tushare_express          │ 2,477   │ 5.42%         │ 6.41%        │ 9.81%          │
│ tushare_forecast         │ 24,250  │ 2.03%         │ 3.92%        │ 9.10%          │
│ tushare_fina_indicator   │ 36,601  │ 0.59%         │ 2.12%        │ 5.89%          │
│ financial_relation       │ 6,613   │ 1.00%         │ 1.83%        │ 4.77%          │
│ overall                  │ 69,941  │ 1.31%         │ 2.85%        │ 7.04%          │
└──────────────────────────┴─────────┴───────────────┴──────────────┴────────────────┘
```

Strongest event-type precision in this first pass:

```text
┌──────────────────────────────────────┬─────────┬───────────────┬──────────────┬────────────────┐
│ Event type                           │ Signals │ Precision 90d │ Precision 180d│ Precision 365d │
├──────────────────────────────────────┼─────────┼───────────────┼──────────────┼────────────────┤
│ financial_express_loss               │ 1,881   │ 6.64%         │ 7.89%        │ 11.64%         │
│ financial_forecast_loss              │ 14,810  │ 3.20%         │ 6.24%        │ 14.41%         │
│ financial_indicator_large_decline    │ 36,601  │ 0.59%         │ 2.12%        │ 5.89%          │
│ financial_positive_but_miss_expectation│ 6,613 │ 1.00%         │ 1.83%        │ 4.77%          │
└──────────────────────────────────────┴─────────┴───────────────┴──────────────┴────────────────┘
```

Multi-source trailing 120-day combination improves long-horizon precision, but still does not justify hard trading actions:

```text
┌──────────────────────────────┬─────────┬───────────────┬──────────────┬────────────────┐
│ Source count in trailing 120d │ Signals │ Precision 90d │ Precision 180d│ Precision 365d │
├──────────────────────────────┼─────────┼───────────────┼──────────────┼────────────────┤
│ 1 source                     │ 29,525  │ 0.95%         │ 2.14%        │ 4.73%          │
│ 2 sources                    │ 35,585  │ 1.46%         │ 3.34%        │ 8.15%          │
│ 3 sources                    │ 4,368   │ 2.50%         │ 3.77%        │ 12.59%         │
│ 4 sources                    │ 463     │ 2.23%         │ 2.90%        │ 12.53%         │
└──────────────────────────────┴─────────┴───────────────┴──────────────┴────────────────┘
```

## Interpretation

- Structured financial signals have high recall for future ST/delisting-risk cycles: 91.58% of cycles had a prior financial risk signal in the previous 365 days.
- Standalone precision is low at per-signal level: overall 90d/180d/365d precision is 1.31% / 2.85% / 7.04%.
- `financial_express_loss` and `financial_forecast_loss` are the strongest first-pass sources, but still should remain warning-only / research candidates.
- Multi-source confirmation improves 365d precision, especially 3+ sources, and should be the next threshold-research direction.
- Current evidence supports early-warning research and possible future score-down/block-add-candidate study, not hard block-buy or forced-sell rules.

## Isolation Result

- The script is read-only and writes reports under `reports/event_signal/early_financial_distress/` only.
- No DB schema was created or modified.
- No QE, Selection Center, Paper v2, QMT, simulated/live trading consumer code was modified.
- No PDF download or LLM call was performed.

