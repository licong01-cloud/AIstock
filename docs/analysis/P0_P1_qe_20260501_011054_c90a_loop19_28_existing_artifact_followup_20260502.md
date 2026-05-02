# qe_20260501_011054_c90a Loop19-28 Existing-Artifact Follow-up Audit

Date: 2026-05-02

Scope: only existing QE artifacts, DB rows, and Qlib bin features were audited. No new QE experiment was started, no strategy behavior was changed, and no extra strategy logging was added.

## New Evidence Summary

```text
Audit          Evidence                              Result
-------------  ------------------------------------  --------------------------------------------------------------------------
CloseNoneRoot  1642 warning rows                     1123 Qlib 1min close all-null; 519 suspend-confirmed states
Suspend35      35 daily-present/minute-missing rows  all suspend_type=S, daily volume=0, minute count=0
TailWindow     Loop24/25/27 minute indicators        tail activity high on some days but weak return correlation
PITTop12       12 factors x 3 loops                  36/36 PASS, mismatch=0
V25Aggregate   10 loops                              day/minute indicator aggregate reconciles; exact child branch trace absent
```

## Close-None Warning Classification

```text
DBState                                         Rows  VerifiedMeaning
----------------------------------------------  ----  -------------------------------------------------------------------------
DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED     1123  DB daily/minute/limit present; no suspend_d row; Qlib 1min close all-null
SUSPEND_D_PRESENT_DAILY_PRESENT_MINUTE_MISSING  35    suspend_d=S; daily zero-volume row exists; no DB minute bars
SUSPEND_D_PRESENT_NO_DB_PRICE                   484   suspend_d present and DB price absent in audited daily/minute lookup
```

## The 35 Daily-Present Minute-Missing Rows

```text
Metric            Value  Evidence
----------------  -----  --------------------------------------------------
Rows              35     from full price/tradability warning audit
UniqueStocks      10     stock-date warning rows include repeated loop hits
UniqueStartDates  9      start date from parsed Qlib warning
SuspendTypeS      35     all rows have market.suspend_d suspend_type=S
DailyVolumeZero   35     all daily rows volume_hand=0
DBMinuteBars      0      all 35 rows have minute.count=0
```

Conclusion: these 35 rows are confirmed suspension/no-trade days, not ordinary minute-data loss. The daily bar exists as a zero-volume suspension marker, while minute bars are correctly absent under the local DB representation.

## DB-Present Not-Suspended Root Cause

```text
Metric                   Value  Interpretation
-----------------------  -----  --------------------------------------------------------
TargetRows               1123   DB daily/minute/limit present and no suspend_d row
UniquePairs              486    unique stock-date pairs checked through Qlib feature API
QlibInstrumentInAll      1123   instrument membership exists for all target pairs
QlibDayCloseNonNull      1123   Qlib day close exists for all target pairs
QlibMinuteRows240        1123   Qlib 1min calendar rows exist
QlibMinuteCloseNonNull0  1123   Qlib 1min $close is all null
```

Conclusion: the 1123 non-suspended warnings are not caused by DB daily/minute absence, suspension, limit-price precision, or missing Qlib daily close. They are caused by Qlib minute feature coverage for those stock-date pairs: 240 minute index rows exist, but `$close` is null for every minute.

## Tail-Window Risk: Loop24/25/27

```text
Loop  Days  TailMean  TailP95  TailMax  HighDays  HighRet  LowRet  TailRetR
----  ----  --------  -------  -------  --------  -------  ------  --------
24    441   3.85%     12.56%   32.96%   45        0.48%    0.23%   0.0171
25    441   5.02%     22.10%   48.10%   45        0.74%    0.19%   0.0712
27    441   6.85%     28.86%   51.53%   45        0.82%    0.18%   0.0456
```

Conclusion: Loop25/27 have higher tail activity than Loop24, but the same-day tail-ratio/return Spearman correlation is weak (0.0171 to 0.0712). Existing artifacts do not prove tail-substitute branches per order, but aggregate data does not show tail activity as a mechanical or dominant source of returns.

## Top-12 Factor Dynamic PIT Truncation

```text
Scope            Loops     Factors  Checks  ComparedRows  Mismatch  MaxAbsDiff  Status
---------------  --------  -------  ------  ------------  --------  ----------  ------
Top12Importance  19,22,26  12       36      501495        0         0.000e+00   PASS
```

Audited top-12 factors:

```text
#   Factor
--  ----------------------------------
1   Fundamental_Liquidity_Cross_Factor
2   Price_ChipNormalized_Position
3   Price_Deviation_Historical_High
4   SmallOrderIntensityBreakoutFactor
5   m_free_turnover_ind_neutral
6   m_idio_vol_60d
7   m_intraday_range_ratio_5d
8   m_mom_weighted_strength_20d
9   m_turnover_mf_divergence
10  neg_composite_score
11  neg_gross_margin_times_turnover
12  small_order_flow_intensity
```

Conclusion: the top-12 importance factors audited on Loop19/22/26 at cutoff 2025-12-31 showed no PIT truncation mismatch on the selected dates. This materially lowers leakage risk for the highest-priority factors, but it is still a targeted audit, not a full 57-factor proof.

## V25 Existing-Artifact Reliability Snapshot

```text
Loop  Days  RowsByDate                BadDates  ValueDiff  DealDiff  ReplayLevel
----  ----  ------------------------  --------  ---------  --------  -------------------------
19    441   {'240': 153, '241': 288}  0         4.47e-08   3.26e-08  AGGREGATE_VERIFIABLE_ONLY
20    441   {'240': 153, '241': 288}  0         5.96e-08   2.05e-08  AGGREGATE_VERIFIABLE_ONLY
21    441   {'240': 153, '241': 288}  0         5.22e-08   2.98e-08  AGGREGATE_VERIFIABLE_ONLY
22    441   {'240': 153, '241': 288}  0         4.47e-08   3.26e-08  AGGREGATE_VERIFIABLE_ONLY
23    441   {'240': 153, '241': 288}  0         6.71e-08   2.24e-08  AGGREGATE_VERIFIABLE_ONLY
24    441   {'240': 153, '241': 288}  0         6.71e-08   4.66e-08  AGGREGATE_VERIFIABLE_ONLY
25    441   {'240': 153, '241': 288}  0         5.96e-08   2.33e-08  AGGREGATE_VERIFIABLE_ONLY
26    441   {'240': 153, '241': 288}  0         4.47e-08   3.73e-08  AGGREGATE_VERIFIABLE_ONLY
27    441   {'240': 153, '241': 288}  0         4.84e-08   2.33e-08  AGGREGATE_VERIFIABLE_ONLY
28    441   {'240': 153, '241': 288}  0         4.47e-08   3.26e-08  AGGREGATE_VERIFIABLE_ONLY
```

Conclusion: V25 aggregate minute/day indicators are internally consistent for all Loop19-28, with no bad minute dates and near-zero day/minute aggregate differences. Exact child-order plan/no-fill/tail-substitute branch replay remains `AGGREGATE_VERIFIABLE_ONLY` because the old artifacts do not persist branch-level rows.

## Credibility Assessment After Follow-up

```text
Area              Credibility  Reason
----------------  -----------  ----------------------------------------------------------------------------------
IC/RankIC         High         recomputed from pred.pkl and label.pkl with max diff=0 in prior P0 audit
LabelHorizon      High         Loop19-28 date-gap audit matched configured horizon
TopBucket         High         Top50-Bottom50 positive in prior P0 audit
PositionsNAVCash  High         position/account truth reconciled from persisted Qlib artifacts
V25Aggregate      High         minute/day value and deal_amount aggregates reconcile
CloseNone         MediumHigh   root cause classified; Qlib minute all-null remains a data-provider coverage issue
ExactV25Branch    Medium       aggregate is verified; exact branch events are absent
AllFactorLeakage  MediumHigh   top-12 dynamic PIT PASS; remaining lower-priority factors not fully recomputed
SeedStability     NotVerified  requires new repeated-seed QE runs, outside current scope
```

## Next Audits Without New QE Experiments Or Logging Changes

```text
Priority  Audit                       Why                                                           Input
--------  --------------------------  ------------------------------------------------------------  -----------------------------------------
P0        QlibMinuteCoverageMap       quantify all-null 1min close coverage by date/stock/pool      Qlib 1min bin + DB minute
P0        RemainingFactorPIT          upgrade targeted top-12 PASS toward all-factor leakage proof  existing factor scripts + temp truncation
P0        HoldingsTopBucketState      verify top50 conversion by market state and drawdown windows  pred/label/positions/report
P1        FeatureImportanceStability  avoid dropping factors that only look weak in one period      feature_importance + yearly IC
P1        TrainingCurveExtraction     compare model overfit/underfit using existing logs/artifacts  mlruns logs and model artifacts
P1        CostMetricSemanticCheck     document cost-in-cash vs zero report cost edge case per loop  Qlib source + report/cash
```

## Files Generated In This Follow-up

```text
File                                                                                                  Purpose
----------------------------------------------------------------------------------------------------  -------------------------------------------------------------
scripts/qe_close_none_root_cause_audit.py                                                             Qlib close=None root-cause audit from existing price JSON
scripts/qe_tail_window_risk_audit.py                                                                  tail-window activity/return risk audit from minute indicators
docs/analysis/P0_qe_20260501_011054_c90a_loop19_28_close_none_root_cause_20260502.md                  close-none root cause report
docs/analysis/P1_qe_20260501_011054_c90a_loop24_25_27_tail_window_risk_20260502.md                    tail-window risk report
docs/analysis/P1_qe_20260501_011054_c90a_loop19_22_26_dynamic_truncation_top12_remaining_20260502.md  remaining top-12 PIT report
```
