# L2 Early Financial Distress Return Study Validation

Date: 2026-05-08 Asia/Shanghai
Scope: event_signal research only; extend early financial distress research with post-signal returns, pre-ST return digestion, and research-only candidate rules.
Worktree: `F:\Dev\AIstock_worktrees\event-signal-policy-20260507`
Production backend impact: none; port `8001` was not restarted or touched.

## Implemented Scope

- Added execution plan doc: `docs/analysis/event_signal_early_financial_distress_execution_plan_20260508.md`.
- Enhanced read-only script: `backend/services/event_signal/early_financial_distress_research.py`.
  - Adds T0/T+1/T+5/T+10/T+20/T+60 return study from `market.kline_daily_raw`.
  - Adds matched-cycle signal-to-pre-ST return study.
  - Adds research-only `candidate_rules` with `hard_block_allowed=false`, `force_exit_allowed=false`, and `alpha_boost_allowed=false`.
- Expanded tests: `backend/tests/event_signal/test_early_financial_distress_research.py`.
- Added result doc: `docs/analysis/event_signal_early_financial_distress_research_result_20260508.md`.
- Generated full-window read-only report for 2018-08-01 through 2026-05-07.
- Out of scope: DB schema changes, QE/Selection/Paper/QMT/simulated/live trading consumers, PDF downloads, LLM calls, alpha overlay, hard block, forced exit.

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest backend/tests/event_signal/test_early_financial_distress_research.py -q
python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
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

rg -n "early_financial_distress|financial_distress" `
  backend/services/selection_center `
  backend/services/paper_trading_v2 `
  backend/services/quantevolver `
  backend/infra/qmt_client.py `
  backend/routers/qmt.py -S

git diff --check
```

## Automated Test Results

- Focused test: `9 passed in 0.38s`.
- Event-signal module regression: `123 passed in 3.31s`.
- `py_compile`: passed.
- Isolation grep: no matches in QE / Selection / Paper / QMT consumer paths.
- `git diff --check`: passed; only existing LF->CRLF warnings for `backend/db/init_unified_event_signal_schema.py` and `backend/tests/test_unified_event_signal_schema.py`.

## Read-only DB Report

- Report id: `early_financial_distress_20180801_20260507_20260508_003816`.
- Output JSON: `reports/event_signal/early_financial_distress/early_financial_distress_20180801_20260507_20260508_003816.json`.
- Output Markdown: `reports/event_signal/early_financial_distress/early_financial_distress_20180801_20260507_20260508_003816.md`.
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

## Main Findings

```text
┌────────────────────────────────────────┬─────────┬────────────┬─────────────┬─────────────┐
│ Signal                                 │ Signals │ 90d hit    │ 180d hit    │ 365d hit    │
├────────────────────────────────────────┼─────────┼────────────┼─────────────┼─────────────┤
│ financial_express_loss                 │ 1,881   │ 6.64%      │ 7.89%       │ 11.64%      │
│ financial_forecast_loss                │ 14,810  │ 3.20%      │ 6.24%       │ 14.41%      │
│ financial_indicator_large_decline      │ 36,601  │ 0.59%      │ 2.12%       │ 5.89%       │
│ overall                                │ 69,941  │ 1.31%      │ 2.85%       │ 7.04%       │
└────────────────────────────────────────┴─────────┴────────────┴─────────────┴─────────────┘
```

```text
┌──────────────────────┬─────────┬────────────┬─────────────┬─────────────┐
│ Source count 120d    │ Signals │ 90d hit    │ 180d hit    │ 365d hit    │
├──────────────────────┼─────────┼────────────┼─────────────┼─────────────┤
│ 1 source             │ 29,525  │ 0.95%      │ 2.14%       │ 4.73%       │
│ 2 sources            │ 35,585  │ 1.46%      │ 3.34%       │ 8.15%       │
│ 3 sources            │ 4,368   │ 2.50%      │ 3.77%       │ 12.59%      │
│ 4 sources            │ 463     │ 2.23%      │ 2.90%       │ 12.53%      │
└──────────────────────┴─────────┴────────────┴─────────────┴─────────────┘
```


Metric bucket findings:

```text
┌────────────────────────────────────────────────────────────┬───────┬────────────┬─────────────┬─────────────┐
│ Metric bucket                                              │ Signals│ 90d hit    │ 180d hit    │ 365d hit    │
├────────────────────────────────────────────────────────────┼───────┼────────────┼─────────────┼─────────────┤
│ forecast loss: 续亏 + loss >= 10bn yuan                    │ 700   │ 8.67%      │ 17.47%      │ 32.80%      │
│ forecast loss: 续亏 + unknown amount                       │ 474   │ 5.70%      │ 18.94%      │ 30.96%      │
│ express loss: loss >= 1bn yuan                             │ 237   │ 12.83%     │ 15.11%      │ 22.67%      │
│ forecast loss: 续亏 + loss 1bn-10bn yuan                   │ 3,118 │ 4.09%      │ 8.75%       │ 18.67%      │
└────────────────────────────────────────────────────────────┴───────┴────────────┴─────────────┴─────────────┘
```
Return-study conclusions:

- `financial_express_loss`: T0_T+20 median return `-3.39%`, negative-return rate `58.82%`; T0_T+60 median `-3.45%`, negative-return rate `57.09%`.
- `financial_forecast_loss`: T0/T+5 is weakly negative, but T0_T+20 mean/median turns positive, so it must be further thresholded before any overlay research.
- Signals that eventually hit ST within 365d have T0_T+60 median return `-3.86%`, worse than non-hit signals.
- Latest financial signal to pre-ST median return is `-2.84%` with `55.40%` negative-return rate, implying partial but not universal pre-ST digestion.


Specific source-combo findings:

```text
┌────────────────────────────────────────────────────────────┬─────────┬────────────┬─────────────┬─────────────┐
│ Source combo 120d                                          │ Signals │ 90d hit    │ 180d hit    │ 365d hit    │
├────────────────────────────────────────────────────────────┼─────────┼────────────┼─────────────┼─────────────┤
│ financial_relation + tushare_express + tushare_forecast    │ 894     │ 7.76%      │ 8.37%       │ 18.47%      │
│ tushare_express + tushare_forecast                         │ 1,807   │ 8.60%      │ 9.97%       │ 14.44%      │
│ tushare_express + tushare_fina_indicator + tushare_forecast│ 2,115   │ 1.38%      │ 2.66%       │ 11.09%      │
│ tushare_fina_indicator + tushare_forecast                  │ 29,584  │ 1.09%      │ 3.10%       │ 8.06%       │
└────────────────────────────────────────────────────────────┴─────────┴────────────┴─────────────┴─────────────┘
```
## Research-only Candidate Rules

- `financial_express_loss`: next-stage `research_score_down_candidate`.
- `financial_forecast_loss`: next-stage threshold refinement; cannot be used alone.
- `trailing_source_count >= 3`: next-stage multi-source combo research.
- `financial_indicator_large_decline`: `warning_only`.
- `financial_positive_but_miss_expectation`: current threshold too weak; refine before use.

All generated candidate rules have:

```text
hard_block_allowed = false
force_exit_allowed = false
alpha_boost_allowed = false
```

## Business Outcome

- Early structured financial signals are valid as a research-first warning source.
- They are not valid as direct hard prohibit-buy or forced-sell rules.
- The next implementation should stay research-only and focus on threshold/combo refinement before QE Loop1 offline overlay.
- Production backend port `8001` was not touched.







