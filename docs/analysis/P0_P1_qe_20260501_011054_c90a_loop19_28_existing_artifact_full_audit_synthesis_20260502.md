# P0/P1 QE Existing-Artifact Full Audit Synthesis: qe_20260501_011054_c90a Loop19-28

Scope: completed Loop19-28 artifacts only. No QE task was rerun, no QE workspace result was modified, and no new backtest runtime logging was added.

## 1. Validation Matrix

```text
Priority  Check                          Status             Evidence
--------  -----------------------------  -----------------  -----------------------------------------------------------------------------
P0        IC/RankIC recompute            PASS               IC max diff=0.00e+00, RankIC max diff=0.00e+00
P0        Label horizon alignment        PASS               10/10 loops have report-signal date gap equal to H
P0        Top bucket conversion          PASS               min Top50-Bottom50=2.18%, min LSWin=67.73%
P0        Static leakage scan            PASS               high-risk hits=0
P0        V25 1min/1day aggregate        PASS               max value diff=6.71e-08, max amount diff=4.66e-08, bad dates=0
P0        Price/tradability warnings     PASS_WITH_FINDING  minute audited=1642/1642; DB-present-not-suspended=1123
P0        Actual-trade price precision   PASS               samples=300, max diff <= 3.75e-06
P0        Exact V25 child-branch replay  NOT_VERIFIABLE     exact branch trace loops=0/10; only aggregate replay level is available
P1        Dynamic truncation expansion   PASS               checks=18, compared rows=250827, mismatches=0
P1        Year segment stability         PASS_WITH_RISK     2024/2025 strong; 2026 IC and return are weaker and need continued monitoring
```

## 2. Loop Signal/Return Snapshot

```text
Loop  H   Feat  IC      RankIC  CAGR    MDD      T50-B50  LSWin
----  --  ----  ------  ------  ------  -------  -------  ------
19    20  77    0.0679  0.1311  73.78%  -17.60%  5.05%    69.67%
20    10  77    0.0570  0.1066  68.72%  -18.16%  3.26%    71.53%
21    10  77    0.0575  0.1027  66.84%  -18.99%  3.22%    72.22%
22    20  77    0.0739  0.1367  75.61%  -18.57%  4.95%    68.25%
23    5   77    0.0506  0.0905  71.96%  -17.55%  2.18%    70.48%
24    10  77    0.0659  0.1002  81.14%  -19.37%  3.83%    75.00%
25    10  77    0.0822  0.1201  75.80%  -20.09%  4.51%    76.62%
26    10  77    0.0638  0.1002  93.41%  -17.72%  3.73%    72.22%
27    10  77    0.0800  0.1124  73.69%  -21.32%  4.21%    78.01%
28    5   77    0.0487  0.0864  65.51%  -17.12%  2.19%    67.73%
```

## 3. V25 Minute Execution Existing-Artifact Audit

```text
Loop  MinRows  RowsByDate                BadDates  MaxValueDiff  MaxAmountDiff  TailValue%  MaxDayTail%  ReplayLevel
----  -------  ------------------------  --------  ------------  -------------  ----------  -----------  -------------------------
19    106128   {"240": 153, "241": 288}  0         4.47e-08      3.26e-08       2.40%       23.45%       AGGREGATE_VERIFIABLE_ONLY
20    106128   {"240": 153, "241": 288}  0         5.96e-08      2.05e-08       2.30%       23.74%       AGGREGATE_VERIFIABLE_ONLY
21    106128   {"240": 153, "241": 288}  0         5.22e-08      2.98e-08       3.20%       22.55%       AGGREGATE_VERIFIABLE_ONLY
22    106128   {"240": 153, "241": 288}  0         4.47e-08      3.26e-08       2.36%       21.79%       AGGREGATE_VERIFIABLE_ONLY
23    106128   {"240": 153, "241": 288}  0         6.71e-08      2.24e-08       3.03%       22.01%       AGGREGATE_VERIFIABLE_ONLY
24    106128   {"240": 153, "241": 288}  0         6.71e-08      4.66e-08       4.12%       32.96%       AGGREGATE_VERIFIABLE_ONLY
25    106128   {"240": 153, "241": 288}  0         5.96e-08      2.33e-08       5.26%       48.10%       AGGREGATE_VERIFIABLE_ONLY
26    106128   {"240": 153, "241": 288}  0         4.47e-08      3.73e-08       2.48%       21.68%       AGGREGATE_VERIFIABLE_ONLY
27    106128   {"240": 153, "241": 288}  0         4.84e-08      2.33e-08       7.75%       51.53%       AGGREGATE_VERIFIABLE_ONLY
28    106128   {"240": 153, "241": 288}  0         4.47e-08      3.26e-08       2.17%       22.08%       AGGREGATE_VERIFIABLE_ONLY
```

Note: `value` is the monetary turnover field. Qlib `deal_amount` is quantity-style and must not be interpreted as monetary turnover. Current 1min and 1day artifacts aggregate with near-zero error for both fields. Exact plan/no-fill/tail-substitute branches are not proven because original event rows are absent.

## 4. Price And Tradability Full-Minute Warning Audit

```text
DBState                                         Rows
----------------------------------------------  ----
DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED     1123
SUSPEND_D_PRESENT_DAILY_PRESENT_MINUTE_MISSING  35
SUSPEND_D_PRESENT_NO_DB_PRICE                   484
```

```text
Basis             DaySamples  MinuteSamples
----------------  ----------  -------------
close_div_factor  126         123
close_raw         174         177
```

Full warning minute audit covered 1642/1642 rows. 1,123 rows have DB daily+minute+limit records and are not suspend_d rows, so `$close=None` is not explained by complete DB absence. 484 rows are suspend_d/no DB price, and 35 rows have suspend_d plus daily rows but no minute bars. In 300 actual-trade samples, max DB-vs-Qlib close/limit diff is <= 3.75e-06.

## 5. P1 Factor Priority And Dynamic Truncation

```text
Factor                              LoopCnt  AvailCnt  MeanGainPct  MaxGainPct
----------------------------------  -------  --------  -----------  ----------
m_free_turnover_ind_neutral         10       10        3.17         3.38
neg_composite_score                 10       10        3.11         3.25
SmallOrderIntensityBreakoutFactor   10       10        3.09         3.34
Price_Deviation_Historical_High     10       10        2.77         3.46
m_intraday_range_ratio_5d           10       10        2.68         3.16
neg_gross_margin_times_turnover     10       10        2.67         2.95
Price_ChipNormalized_Position       10       10        2.61         2.98
small_order_flow_intensity          10       10        2.60         3.16
Fundamental_Liquidity_Cross_Factor  10       10        2.60         3.15
m_idio_vol_60d                      10       10        2.59         3.08
m_turnover_mf_divergence            10       10        2.44         2.96
m_mom_weighted_strength_20d         10       10        2.41         2.60
```

```text
Loop  Factor                             Compared  Mismatch  MaxAbsDiff  Status
----  ---------------------------------  --------  --------  ----------  ------
19    m_free_turnover_ind_neutral        13971     0         0.00e+00    PASS
19    neg_composite_score                13985     0         0.00e+00    PASS
19    SmallOrderIntensityBreakoutFactor  13971     0         0.00e+00    PASS
19    Price_Deviation_Historical_High    13985     0         0.00e+00    PASS
19    m_intraday_range_ratio_5d          13978     0         0.00e+00    PASS
19    neg_gross_margin_times_turnover    13719     0         0.00e+00    PASS
22    m_free_turnover_ind_neutral        13971     0         0.00e+00    PASS
22    neg_composite_score                13985     0         0.00e+00    PASS
22    SmallOrderIntensityBreakoutFactor  13971     0         0.00e+00    PASS
22    Price_Deviation_Historical_High    13985     0         0.00e+00    PASS
22    m_intraday_range_ratio_5d          13978     0         0.00e+00    PASS
22    neg_gross_margin_times_turnover    13719     0         0.00e+00    PASS
26    m_free_turnover_ind_neutral        13971     0         0.00e+00    PASS
26    neg_composite_score                13985     0         0.00e+00    PASS
26    SmallOrderIntensityBreakoutFactor  13971     0         0.00e+00    PASS
26    Price_Deviation_Historical_High    13985     0         0.00e+00    PASS
26    m_intraday_range_ratio_5d          13978     0         0.00e+00    PASS
26    neg_gross_margin_times_turnover    13719     0         0.00e+00    PASS
```

This round expanded dynamic PIT truncation to 3 representative loops (Loop19/22/26) and 6 top-priority factors: 18 factor-loop checks, 250,827 compared rows, and 0 mismatches. Together with the previous 2-factor Loop19 sample, high-priority custom-factor PIT evidence is stronger, but this is still not a full proof for all 57 custom factors.

## 6. Segment Stability Risk

```text
Year  ICMin   ICMax   RankICMin  RankICMax  RetMin  RetMax  SharpeMin  SharpeMax
----  ------  ------  ---------  ---------  ------  ------  ---------  ---------
2024  0.0726  0.1126  0.0986     0.1395     39.12%  53.71%  1.7627     2.4922
2025  0.0463  0.0804  0.0898     0.1385     50.31%  87.28%  1.9151     2.8739
2026  0.0114  0.0344  0.0471     0.1224     4.98%   16.52%  0.7764     2.6662
```

Conclusion: 2024 and 2025 are strong across IC and returns. 2026 IC/RankIC are weaker, but all loops remain positive. Future loops should continue comparing 2026-only samples, drawdown windows, Top50 conversion, and tail execution ratios.

## 7. Next Highest-Priority Actions

```text
Priority  Action                              Implementation
--------  ----------------------------------  -----------------------------------------------------------------------------------------------------------------
P0        Keep existing-artifact audit chain  For every new loop, run loop_p0 + execution_truth + v25_existing_artifact + price warning incremental audit
P0        Explain DB-present close-none rows  Trace Qlib instrument/calendar/exchange lookup path for the 1,123 DB-present-not-suspended warning rows
P1        Expand dynamic truncation           Complete the remaining top12 factors and cover Loop24/25/27 high-IC or high-tail-ratio loops
P1        V25 exact branch validation         Do not add logs now; if aggregate replay cannot explain an anomaly, design opt-in batched plan/no-fill/tail trace
P1        2026 segment monitoring             For new loops, compare 2026-only IC, drawdown windows, Top50 conversion, and tail value ratio
```


## 8. Follow-up Update 2026-05-02

Follow-up document: `docs/analysis/P0_P1_qe_20260501_011054_c90a_loop19_28_existing_artifact_followup_20260502.md`.

```text
Audit          Evidence                              Result
-------------  ------------------------------------  --------------------------------------------------------------------------
CloseNoneRoot  1642 warning rows                     1123 Qlib 1min close all-null; 519 suspend-confirmed states
Suspend35      35 daily-present/minute-missing rows  all suspend_type=S, daily volume=0, minute count=0
TailWindow     Loop24/25/27 minute indicators        tail activity high on some days but weak return correlation
PITTop12       12 factors x 3 loops                  36/36 PASS, 501495 compared rows, mismatch=0
V25Aggregate   10 loops                              day/minute indicator aggregate reconciles; exact child branch trace absent
```

Updated interpretation:
- The 35 daily-present/minute-missing rows are confirmed suspension/no-trade rows, not ordinary minute-data loss.
- The 1123 DB-present/not-suspended `$close=None` warnings are explained by Qlib 1min feature coverage: 240 minute index rows exist but `$close` is null for every minute, while DB minute data and Qlib day close exist.
- Tail-window activity in Loop24/25/27 is not proven to be the return driver; same-day tail-ratio/return Spearman is weak.
- Dynamic PIT truncation now covers the top-12 importance factors on Loop19/22/26 with 0 mismatches; remaining lower-priority factors are still not fully recomputed.
