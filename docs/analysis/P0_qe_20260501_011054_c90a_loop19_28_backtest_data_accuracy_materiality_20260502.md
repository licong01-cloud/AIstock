# QE Backtest Data Accuracy Materiality Audit: qe_20260501_011054_c90a

Scope: existing P0/P1 JSON artifacts, persisted reports, and run logs only. No QE rerun, no strategy behavior change, and no new strategy logging.

## Direct Answer

- No NAV/account/position/IC/RankIC/V25 aggregate calculation error has been found in the audited Loop19-28 artifacts.
- One real data-coverage issue has been found: some stock-date pairs have DB minute data and Qlib day close, but Qlib 1min `$close` is all null. This can skip a small number of planned buys, so it is a data coverage warning, not a proven return-calculation error.
- Current return credibility is high enough for data-accuracy purposes, but not absolute: exact V25 child-order branch replay is still aggregate-only, and all Loop1-18 rerun conclusions should wait until those reruns finish.

## Accuracy Gates

```text
Gate                     Status  Evidence
-----------------------  ------  ---------------------------------------------------------------------------------------------
SignalMetricRecompute    PASS    IC max diff=0.000e+00, RankIC max diff=0.000e+00
ReportReturnAccount      PASS    return/account max diff=1.110e-16
PositionReportReconcile  PASS    account=0.000e+00, cash=0.000e+00, stock=1.192e-07
V25DayMinuteAggregate    PASS    value=6.706e-08, deal_amount=4.657e-08, bad_dates=0
ReportNaNInf             PASS    nan=0, inf=0
QlibMinuteCoverage       WARN    DB-present/not-suspended warnings=1123, Qlib 1min all-null=1123, invalid DB-present skips=107
```

## Numerical Integrity Summary

```text
Metric                Value      Meaning
--------------------  ---------  ----------------------------------
ICMaxDiff             0.000e+00  recomputed IC vs Qlib artifact
RankICMaxDiff         0.000e+00  recomputed RankIC vs Qlib artifact
ReturnAccountMaxDiff  1.110e-16  daily return vs account pct-change
PositionAccountDiff   0.000e+00  positions vs report account
PositionCashDiff      0.000e+00  positions vs report cash
PositionStockDiff     1.192e-07  positions vs report stock value
V25ValueDiff          6.706e-08  1day value vs 1min value aggregate
V25DealDiff           4.657e-08  1day deal_amount vs 1min aggregate
ReportNaN             0          numeric report NaN count
ReportInf             0          numeric report inf count
```

## Close-None Materiality Summary

```text
Metric                  Value  Meaning
----------------------  -----  ------------------------------------------
InvalidPriceSkips       326    all ScoreWeighted invalid-price skip lines
InvalidDBPresentSkips   107    DB minute exists, Qlib 1min close all-null
DerivedBuyTrades        21842  stock_trades derived buy rows
InvalidSkipVsBuys       1.49%  all invalid skips / derived buy rows
InvalidDBPresentVsBuys  0.49%  coverage-gap skips / derived buy rows
```

## Loop-Level Close-None Materiality

```text
Loop  DBWarn  SkipAll  SkipDB  BuyRows  Skip/Buy  SkipDB/Buy  SkipDates  SkipDateRet  OtherDateRet
----  ------  -------  ------  -------  --------  ----------  ---------  -----------  ------------
19    114     29       11      2191     1.32%     0.50%       22         -0.01%       0.25%
20    110     23       12      2199     1.05%     0.55%       16         0.35%        0.22%
21    103     73       8       2203     3.31%     0.36%       57         0.28%        0.21%
22    116     39       14      2181     1.79%     0.64%       29         0.43%        0.23%
23    132     49       7       2203     2.22%     0.32%       46         0.13%        0.24%
24    102     16       10      2189     0.73%     0.46%       12         0.62%        0.25%
25    127     16       9       2124     0.75%     0.42%       11         0.45%        0.24%
26    110     13       12      2184     0.60%     0.55%       6          0.56%        0.28%
27    110     45       13      2139     2.10%     0.61%       22         0.17%        0.24%
28    99      23       11      2229     1.03%     0.49%       18         -0.36%       0.24%
```

## Loop-Level Numerical Checks

```text
Loop  RetAcct   PosAcct   PosCash   PosStock  V25Value  V25Deal   BadMin  NaN  Inf
----  --------  --------  --------  --------  --------  --------  ------  ---  ---
19    1.11e-16  0.00e+00  0.00e+00  1.19e-07  4.47e-08  3.26e-08  0       0    0
20    1.11e-16  0.00e+00  0.00e+00  1.19e-07  5.96e-08  2.05e-08  0       0    0
21    1.11e-16  0.00e+00  0.00e+00  1.19e-07  5.22e-08  2.98e-08  0       0    0
22    1.10e-16  0.00e+00  0.00e+00  1.19e-07  4.47e-08  3.26e-08  0       0    0
23    1.11e-16  0.00e+00  0.00e+00  1.19e-07  6.71e-08  2.24e-08  0       0    0
24    1.11e-16  0.00e+00  0.00e+00  1.19e-07  6.71e-08  4.66e-08  0       0    0
25    1.11e-16  0.00e+00  0.00e+00  1.19e-07  5.96e-08  2.33e-08  0       0    0
26    1.11e-16  0.00e+00  0.00e+00  1.19e-07  4.47e-08  3.73e-08  0       0    0
27    1.11e-16  0.00e+00  0.00e+00  1.19e-07  4.84e-08  2.33e-08  0       0    0
28    1.11e-16  0.00e+00  0.00e+00  1.19e-07  4.47e-08  3.26e-08  0       0    0
```

## Signal-To-Portfolio Sanity Check

```text
Loop  Top50-Bottom50  D1-D10Win  HoldOverlapLT50
----  --------------  ---------  ---------------
19    5.05%           69.67%     1
20    3.26%           71.53%     21
21    3.22%           72.22%     5
22    4.95%           68.25%     0
23    2.18%           70.48%     129
24    3.83%           75.00%     56
25    4.51%           76.62%     29
26    3.73%           72.22%     5
27    4.21%           78.01%     141
28    2.19%           67.73%     27
```

Interpretation: this confirms that the high IC/RankIC signal generally converts into a positive top-bucket spread; it is not a model-optimization conclusion.

## Current Scope Boundary

- Continue data-accuracy validation only until Loop1-18 full_train reruns complete.
- Do not start new QE experiments in this stage.
- Do not change strategy logging or execution behavior in this stage.
- Do not begin model/factor optimization synthesis until the rerun set is complete.
