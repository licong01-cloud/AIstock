# P0-P1 QE Backtest Truth Synthesis: qe_20260501_011054_c90a Loop19-28

Scope: Loop19-28 completed artifacts only. No QE task was rerun. Capital-size, capacity, market impact, and dedicated capital-size assumptions are out of scope for this round.

## Executive Validation Matrix

```text
Priority  Check                           Status                Evidence                                                                                              
--------  ------------------------------  --------------------  ------------------------------------------------------------------------------------------------------
P0        IC/RankIC recompute             PASS                  pred.pkl + label.pkl recompute max diff = 0 for all Loop19-28                                         
P0        Label horizon alignment         PASS                  Signal/report date gap equals configured 5D/10D/20D for all Loop19-28                                 
P0        Static leakage scan             PASS                  0 high-risk pattern hits across 58-59 code files per loop                                             
P0        Dynamic truncation sample       PASS                  Loop19 selected 2 factors, 27,876 rows compared, 0 mismatches                                         
P0        Position/report truth           PASS                  positions_normal_1day account/cash/value diff vs report = 0.000 max                                   
P0        Daily strategy code             PASS                  ScoreWeightedTopkStrategyV2 uses shift=1 previous-step signal and TopK ranking                        
P0        V25 aggregate minute execution  PASS                  day/minute deal_amount diff = 0; minute rows are 240/241, bad dates = 0                               
P0        Price basis precision           PASS                  20 trade samples max DB-vs-Qlib close/limit diff <= 0.000002                                          
P0        Cost NAV path                   PASS_WITH_METRIC_GAP  Qlib source subtracts cost from Position.cash; report cost columns remain 0 due inner metrics disabled
P0        V25 order-level trace           NOT_VERIFIABLE        Plan/no-fill/tail substitute branch events are not persisted in current artifacts                     
P1        Market-state segmentation       PASS_WITH_RISK        Bull/sideways positive; bear segment negative for all loops                                           
P1        Seed stability                  NOT_VERIFIABLE        Artifacts contain no seed evidence; repeated-seed experiments were not run                            
```

## Loop-Level Truth Snapshot

```text
Loop  H   Feat  IC       RankIC   CAGR      MDD       MaxPos  AvgPos   Top50Ov   LeakHits  CostMetric
----  --  ----  -------  -------  --------  --------  ------  -------  --------  --------  ----------
19    20  77     0.0679   0.1311    73.78%   -17.60%  68         57.9    74.41%  0         MISSING   
20    10  77     0.0570   0.1066    68.72%   -18.16%  61         54.8    62.36%  0         MISSING   
21    10  77     0.0575   0.1027    66.84%   -18.99%  59         50.7    67.52%  0         MISSING   
22    20  77     0.0739   0.1367    75.61%   -18.57%  68         58.3    74.23%  0         MISSING   
23    5   77     0.0506   0.0905    71.96%   -17.55%  62         52.3    54.52%  0         MISSING   
24    10  77     0.0659   0.1002    81.14%   -19.37%  69         53.2    62.76%  0         MISSING   
25    10  77     0.0822   0.1201    75.80%   -20.09%  61         52.1    66.31%  0         MISSING   
26    10  77     0.0638   0.1002    93.41%   -17.72%  70         57.5    70.27%  0         MISSING   
27    10  77     0.0800   0.1124    73.69%   -21.32%  59         51.0    53.97%  0         MISSING   
28    5   77     0.0487   0.0864    65.51%   -17.12%  55         50.8    62.65%  0         MISSING   
```

## Price / Tradability Audit

Parsed Qlib `$close=None` warnings: rows=1642, unique=1642; daily/limit/suspend audited for all unique rows, DB minute audited for 206 rows.

```text
DBState                                                  Rows
-------------------------------------------------------  ----
DB_DAILY_LIMIT_PRESENT_NOT_SUSPENDED_MINUTE_NOT_AUDITED  1023
DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED              100 
DB_DAILY_PRESENT_MINUTE_NOT_AUDITED                      35  
SUSPEND_D_PRESENT_NO_DAILY_PRICE_MINUTE_NOT_AUDITED      378 
SUSPEND_D_PRESENT_NO_DB_PRICE                            106 
```

### Minute-Audited Warning Rows

```text
DBState                                      Rows
-------------------------------------------  ----
DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED  100 
SUSPEND_D_PRESENT_NO_DB_PRICE                106 
```

### Price Basis Samples

```text
Basis             DaySamples  MinuteSamples
----------------  ----------  -------------
close_div_factor  12          11           
close_raw         8           9            
```

Max day close diff=0.000002; max minute close diff=0.000002. `close_raw` rows are ties where Qlib factor is effectively 1; `close_div_factor` rows verify adjusted-to-raw conversion.

## Dynamic Future-Leakage Truncation Evidence

```text
Loop  Factor                      Cutoff      Compared  Mismatch  MaxAbsDiff  Status
----  --------------------------  ----------  --------  --------  ----------  ------
19    m_conditional_momentum_20d  2025-12-31  13946     0         0.000e+00   PASS  
19    m_turnover_zscore_60d       2025-12-31  13930     0         0.000e+00   PASS  
```

## Strategy Accuracy And Risk Conclusions

```text
Area                        Conclusion             EvidenceOrRisk                                                                                                                                     
--------------------------  ---------------------  ---------------------------------------------------------------------------------------------------------------------------------------------------
Daily ScoreWeightedTopkV2   Verified               shift=1 signal; TopK ranking; ghost holdings forced sell; actual holdings avg 50.7-58.3, max 55-70                                                 
V25 minute execution        Verified aggregate     V25 model files/config present; raw price conversion and limit comparison code present; minute indicator aggregate exact                           
Tail substitute / tail buy  Partially verifiable   TAIL_SUBSTITUTE config/code present and topk cap code present; actual branch execution not reconstructable from logs                               
Cost accounting             NAV valid, metric gap  Cost path subtracts from Position.cash; report cost/total_cost = 0, so cost columns and with/without-cost split are inaccurate                     
Qlib close None warnings    Mixed factual causes   106 minute-audited suspend/no-price; 100 minute-audited DB-present-not-suspended; remaining all daily/limit/suspend classified but minute unaudited
```

## Highest Priority Next Actions

```text
Priority  Action                         Implementation                                                                                                                                                                 
--------  -----------------------------  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
P0        Persist V25 order-level trace  Record per order: generated plan hash/early-late weights, market block reason, limit/pre_close/close/factor, substitute selected sid, boost fallback, final child order amounts
P0        Fix cost metric recording      Keep NAV behavior unchanged; make inner-executor cost/turnover accumulate into daily report or separately persist cost audit so with/without-cost columns are truthful         
P0        Full warning minute audit      If required, build a DB minute coverage cache table keyed by ts_code/trade_date; current direct minute table audit is exact but too slow for all 1642 warning rows             
P1        Repeat seed stability          Run 3-5 seeds for LSTM/GRU/TCN top candidates; current artifacts cannot prove seed robustness                                                                                  
P1        Expand dynamic truncation      Run all selected custom factors or at least factor-family representatives after Loop1-18 rerun completes                                                                       
```

## Source Documents Generated

- `docs/analysis/P0_qe_20260501_011054_c90a_loop19_28_backtest_accuracy_leakage_audit_20260502.md`
- `docs/analysis/P0_P1_qe_20260501_011054_c90a_loop19_28_execution_truth_audit_20260502.md`
- `docs/analysis/P0_qe_20260501_011054_c90a_loop19_28_price_tradability_audit_20260502.md`
- `docs/analysis/P0_qe_20260501_011054_c90a_loop19_28_strategy_code_evidence_20260502.md`
- `docs/analysis/P0_qe_20260501_011054_c90a_loop19_factor_dynamic_truncation_20260502.md`

