# Event Signal Financial Distress Research Findings

## Persistent Findings

```text
+------+----------------------------------------------+--------------------------------------------------------------+
| id   | finding                                      | implication                                                  |
+------+----------------------------------------------+--------------------------------------------------------------+
| F001 | Small-cap relative-loss signal is strongest  | Keep loss/mv >=50% + mv <10bn as benchmark.                 |
| F002 | Fixed rank20 score-down is stable benchmark  | Use before trying stronger demotion or hard filters.         |
| F003 | Severity balanced adds explainability        | Useful for future configurable score-down profiles.          |
| F004 | Loss-history-only is too broad               | Use as a feature, not a standalone runtime rule.             |
| F005 | Industry exposure is not single-industry only | Avoid hard industry filters until larger evidence exists.    |
| F006 | Current research is biased toward small caps  | Next phase must report every signal by market-cap bucket.    |
| F007 | Every rule now has market-cap bucket coverage | Use bucket rows to prevent small-cap-only overgeneralization. |
| F008 | Direct event returns are not pure downside events | Do not convert financial distress rules to hard bans without context. |
| F009 | Medium-cap indicator decline is context-sensitive | Keep as QE rank/severity score-down candidate, not standalone risk filter. |
| F010 | Context overlay improves policy shape more than raw return | Prefer it for runtime-shaped design, not because it beats every benchmark. |
| F011 | Sector relief is not yet a decision driver | Keep compatibility for plate rotation, but do not select rules by sector relief. |
| F012 | Phase-13 config can stay non-hard and schema-neutral | Store as draft policy/rule config later; do not change raw source tables. |
| F013 | Additional loops weaken 20td default and favor more research | Keep 60td as primary test; run parameter sweep before runtime promotion. |
| F014 | 22-loop parameter sweep favors 60td context-balanced score-down | Use as primary research candidate; keep fixed 10% only as baseline. |
| F015 | Robustness gate rejects runtime and DB promotion for now | Median is zero and effect is replacement-sparse; design true QE rerun next. |
| F016 | qrun --pred-backtest enables copied-loop true rerun | Use materialized pred.pkl for research-only PortAnaRecord validation. |
| F017 | Cheap overlay can overstate true QE materiality | Require one-loop WSL true smoke before runtime or multi-loop true promotion. |
+------+----------------------------------------------+--------------------------------------------------------------+
```

## Current Best Rule

```text
loss_to_market_cap_ge_50pct_mv_lt_10bn
```

Observed benchmark from the 10-loop overlay research:

```text
+-----------+-----------+---------+-----------+---------+------+-----------+
| active_td | pos/loops | blocked | eval_topk | dropped | repl | avg_ret_d |
+-----------+-----------+---------+-----------+---------+------+-----------+
| 60        | 6/10      | 77      | 77        | 6       | 3    | 0.20%     |
| 120       | 6/10      | 137     | 136       | 9       | 4    | 0.18%     |
| 242       | 6/10      | 247     | 246       | 11      | 5    | 0.16%     |
+-----------+-----------+---------+-----------+---------+------+-----------+
```

## Loss-History Finding

```text
+-------------------------------------------+------------------+-------------------------------------------------------------+
| rule                                      | decision         | reason                                                      |
+-------------------------------------------+------------------+-------------------------------------------------------------+
| loss_reports_ge_4                         | REJECT_RUNTIME   | Too broad; 60/120td return contribution is not stable.      |
| loss_reports_ge_4_mv_lt_10bn              | RESEARCH_FEATURE | Useful small-cap feature, but still too broad standalone.   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | RESEARCH_FEATURE | Best 242td row, but 60/120td remain weak.                   |
| forecast_loss_reports_ge_4_mv_lt_10bn     | RESEARCH_FEATURE | Cross-source explanation feature, not standalone runtime.   |
+-------------------------------------------+------------------+-------------------------------------------------------------+
```

## Important Caution

The current empirical win is concentrated in small-cap financial distress. The framework must still generate and evaluate signals for all market-cap buckets. Do not hard-code a small-cap-only architecture.

## Market-Cap Bucket Finding

The phase-8 report generated 504 bucket rows across current rule families, active lifetimes, modes, and canonical market-cap buckets.

```text
+----------+--------------------------------------------------------------+
| bucket   | current interpretation                                       |
+----------+--------------------------------------------------------------+
| <5bn     | dominant Top50 interaction for financial distress candidates |
| 5-10bn   | secondary interaction; still part of current best benchmark  |
| 10-30bn  | sparse but non-zero; needs dedicated medium-cap research     |
| 30-100bn | very sparse for current loss-based rules                     |
| >=100bn  | almost no current Top50 interaction                          |
| unknown  | data-quality bucket; do not use for runtime action           |
+----------+--------------------------------------------------------------+
```

## Phase 9 Initial Data Availability

Read-only DB count on `market.event_signal` joined to PIT `market.daily_basic` for 2023-01-01..2026-04-27 shows medium/large samples are present for structured financial events.

```text
+--------------------------------------+----------+---------+---------+---------+
| event family                         | 10-30bn  | 30-100bn| >=100bn | note    |
+--------------------------------------+----------+---------+---------+---------+
| financial_positive_but_miss_expect.  | 407      | 91      | 20      | usable  |
| financial_forecast_large_decline     | 608      | 159     | 35      | usable  |
| financial_indicator_large_decline    | 2153     | 473     | 77      | broad   |
| financial_express_large_decline      | 42       | 11      | 2       | sparse  |
+--------------------------------------+----------+---------+---------+---------+
```

Phase 9 first implementation should therefore test structured medium/large-cap rules before title/PDF/LLM rules.

## Phase 9 Medium/Large-Cap Structured Event Finding

```text
+--------------------------------------+-----------------------+------------------------------------------------------------+
| candidate                            | decision              | evidence                                                   |
+--------------------------------------+-----------------------+------------------------------------------------------------+
| indicator_large_decline_mv_ge_10bn   | KEEP_RESEARCH_FEATURE | 242td avg_ret_d 0.28% fixed / 0.24% severity               |
| structured_financial_risk_mv_ge_10bn | COVERAGE_BENCHMARK    | broad positive average but too blunt as standalone         |
| forecast_express_large_decline_mv_ge | REJECT_STANDALONE     | 120/242td fixed mode negative                              |
| expectation_miss_mv_ge_10bn          | WATCHLIST_RESEARCH    | weak Top50 interaction; useful concept but not enough      |
| expectation_miss_mv_ge_30bn          | REJECT_RUNTIME        | 30bn+ sample had no dropped Top50 events                   |
+--------------------------------------+-----------------------+------------------------------------------------------------+
```

Medium/large-cap structured financial risk evidence is concentrated in the 10-30bn bucket. Industry concentration should be treated as sector-rotation context, not as a neutralization or rejection gate.

## Phase 10 Constraint Update - Fund Size and Sector Exposure

User confirmed on 2026-05-09 that expected capital is about 10 million CNY, market impact is not a near-term constraint, and industry neutrality is not required. Industry concentration should not be used as a rejection reason by itself; sector/industry exposure remains an explanatory field for possible sector-rotation opportunities.

```text
+----------------------+--------------------------------------------------------------+
| constraint           | phase-10 implication                                         |
+----------------------+--------------------------------------------------------------+
| capital ~10m CNY     | no market-impact filter in current research                  |
| no industry neutral  | do not reject signals solely because they cluster by sector  |
| sector rotation goal | keep industry/sector stats as explanation, not neutralizer   |
| risk-first framework | still evaluate return/drawdown impact before runtime design  |
+----------------------+--------------------------------------------------------------+
```


## Phase 10 Refinement Finding

```text
+--------------------------------------------+-----------------------+------------------------------------------------------------+
| candidate                                  | decision              | evidence                                                   |
+--------------------------------------------+-----------------------+------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn         | KEEP_PRIMARY_CANDIDATE| 60td severity avg_ret_d 0.20%, pos 6/10, min approx 0      |
| indicator_large_decline_mv_30_100bn        | WATCHLIST_ONLY        | weak but non-zero; 242td avg_ret_d about 0.03%             |
| indicator_large_decline_mv_ge_100bn        | REJECT_RUNTIME        | no dropped Top50 events                                    |
| prior-loss refinements                     | REJECT_REFINEMENT     | lower coverage and no improvement over size-only rule      |
| structured_financial_risk_mv_10_30bn       | COVERAGE_BENCHMARK    | positive but broader/worse tail than indicator split       |
+--------------------------------------------+-----------------------+------------------------------------------------------------+
```

Preferred current candidate for future non-hard overlay research: `indicator_large_decline_mv_10_30bn` with 20-60 trading-day severity-style score-down. It should not be a hard ban or forced sell at this stage.


## Phase 11 Direct Event Return Finding

```text
+----------------------------+----------------------------------------------------------------------------------------------+------------------------------------------------------------------------+
| rule                       | direct finding                                                                               | implementation implication                                             |
+----------------------------+----------------------------------------------------------------------------------------------+------------------------------------------------------------------------+
| smallcap_loss_mv50         | T+5/T+20 abnormal returns are positive; T+60 median slightly negative with high missing rate | benchmark remains useful for QE overlay comparison, not hard-ban proof |
| indicator_decline_10_30bn  | T+20/T+60 abnormal medians are negative while means are positive                             | score-down only when model ranks the stock highly                      |
| indicator_decline_30_100bn | abnormal medians negative from T+5 onward but QE overlay weak                                | watchlist-only research feature                                        |
| structured_risk_10_30bn    | positive abnormal mean but negative abnormal median                                          | coverage benchmark, not standalone runtime signal                      |
+----------------------------+----------------------------------------------------------------------------------------------+------------------------------------------------------------------------+
```


## Phase 12 Context Overlay Finding

```text
+-----------------------------------------------------------------------------------------+----------------------------------------------+
| finding                                                                                 | implication                                  |
+-----------------------------------------------------------------------------------------+----------------------------------------------+
| balanced context profile is best runtime-shaped candidate for 10-30bn indicator decline | use score-down only, 20-60td, no hard ban    |
| old aggressive rank20 can have higher average but worse tail                            | do not promote aggressive profile first      |
| smallcap loss/mv benchmark remains useful                                               | continue comparing every new rule against it |
| sector relief had little measurable effect in this run                                  | keep as configurable future hook only        |
+-----------------------------------------------------------------------------------------+----------------------------------------------+
```


## Phase 13 Policy Config Proposal Finding

```text
+----------------------------------------+---------------------------------------------------------------+
| finding                                | implication                                                   |
+----------------------------------------+---------------------------------------------------------------+
| first non-hard config is draft-ready   | document profile/rule params before any runtime integration    |
| score-down depends on candidate rank   | treat as post-alpha rerank overlay, not a daily factor          |
| existing policy schema is enough now   | no schema change or raw table modification in Phase 13          |
| true consumer audit remains future work| final rerank trace should be persisted when runtime is added    |
+----------------------------------------+---------------------------------------------------------------+
```


## Phase 14 Additional QE Validation Finding

```text
+----------------------------------------+---------------------------------------------------------------+
| finding                                | implication                                                   |
+----------------------------------------+---------------------------------------------------------------+
| extra loops do not confirm 20td default| keep 20td as a comparison row, not the preferred default       |
| 60td is better but still weak          | continue research; do not persist or integrate runtime yet     |
| effect size is limited by few drops    | test stronger but still non-hard rank demotion profiles        |
| smallcap benchmark has too few hits    | this extra loop set is not a strong benchmark sample           |
+----------------------------------------+---------------------------------------------------------------+
```


## Phase 15 Parameter Sweep Finding

```text
+----------------------------------------+---------------------------------------------------------------+
| finding                                | implication                                                   |
+----------------------------------------+---------------------------------------------------------------+
| 60td context-balanced is best overall  | current primary non-hard candidate for further validation      |
| fixed 10% is simpler but weaker        | keep as explanation baseline, not preferred policy shape       |
| 20td variants are close but not best   | keep as comparison; do not restore as default                  |
| 120td severity has non-zero value      | keep as secondary diagnostic branch                            |
+----------------------------------------+---------------------------------------------------------------+
```


## Phase 16 Robustness Gate Finding

```text
+----------------------------------------+---------------------------------------------------------------+
| finding                                | implication                                                   |
+----------------------------------------+---------------------------------------------------------------+
| selected profile passes weak research gates | keep as primary research candidate only                    |
| median effect is zero                  | do not persist or integrate until true QE rerun improves proof |
| outlier dependence remains             | require broader experiment support before promotion            |
| traceable rerun is justified           | next phase should design rerun trace and hook points           |
+----------------------------------------+---------------------------------------------------------------+
```


## Phase 17 True QE Rerun Design Finding

```text
+---------------------------------------------+---------------------------------------------------------------+
| finding                                     | implication                                                   |
+---------------------------------------------+---------------------------------------------------------------+
| --pred-backtest can rerun real PortAnaRecord| copied-loop true QE validation is feasible without runtime edit |
| prediction date must be shifted             | trade-date risk for T should rewrite T-1 pred rows by default  |
| materializer creates pkl/csv/json/md trace  | next empirical phase can compare baseline vs adjusted rerun     |
| score-weighted sizing remains approximate   | require copied-loop baseline parity before interpreting alpha   |
+---------------------------------------------+---------------------------------------------------------------+
```

## Phase 18 Copied-Loop Smoke Finding

```text
+---------------------------------------------+---------------------------------------------------------------+
| finding                                     | implication                                                   |
+---------------------------------------------+---------------------------------------------------------------+
| copied-loop pred-backtest completes         | the true QE rerun harness is operational as a technical path   |
| full-universe copied rerun hits MemoryError | current Windows copied-loop path cannot validate full PnL      |
| narrowed-universe metrics are non-comparable| do not infer alpha/risk value from completed smoke metrics     |
| signal promotion remains blocked            | require memory-safe or parity-controlled rerun before runtime  |
+---------------------------------------------+---------------------------------------------------------------+
```

## Phase 19 WSL Full-Universe Finding

```text
+---------------------------------------------+---------------------------------------------------------------+
| finding                                     | implication                                                   |
+---------------------------------------------+---------------------------------------------------------------+
| WSL full-universe rerun completes           | future true QE research should run in WSL, not Windows UNC    |
| baseline and adjusted are now comparable    | full-universe same-conf parity is available for shortlisted rules |
| current candidate is only weak positive     | keep it as research-only and do not write runtime policy      |
| full rerun cost is high                     | use cheap overlay gates before WSL true rerun expansion       |
+---------------------------------------------+---------------------------------------------------------------+
```

## Phase 20 Selective True QE Shortlist Finding

```text
+---------------------------------------------+---------------------------------------------------------------+
| finding                                     | implication                                                   |
+---------------------------------------------+---------------------------------------------------------------+
| no candidate passes direct WSL rerun gate   | do not start broad WSL full-universe rerun batch yet          |
| 60td ctx-balanced remains weak baseline     | keep as calibration after one-loop true rerun, not deployment |
| best new rows are still only 10-loop cheap  | expand them to the 22-loop cheap overlay set first            |
| benchmark loss/mv rule is not hard risk     | direct T+5/T+20 abnormal medians are positive                 |
| next phase is cheap expansion               | test top shortlist rules before spending WSL true-rerun time  |
+---------------------------------------------+---------------------------------------------------------------+
```

## Phase 21 Cheap 22-Loop Overlay Expansion Finding

```text
+---------------------------------------------+---------------------------------------------------------------+
| finding                                     | implication                                                   |
+---------------------------------------------+---------------------------------------------------------------+
| loss-history small-cap row has best average | promising but tail loss is too large for risk-first use       |
| structured ge10 did not scale               | 10-loop signal strength was not stable enough                 |
| clean-tail benchmark is sparse              | useful comparison, not a risk-policy candidate                |
| no immediate WSL rerun candidate             | true-rerun budget should wait for tail-controlled profile     |
| next phase is tail control                  | test softer/rank-aware loss-history variants before WSL       |
+---------------------------------------------+---------------------------------------------------------------+
```

## Phase 22 Loss-History Tail-Control Finding

```text
+---------------------------------------------+---------------------------------------------------------------+
| finding                                     | implication                                                   |
+---------------------------------------------+---------------------------------------------------------------+
| softer loss-history modes reduce tail       | worst improves from -1.966% to -0.935%                        |
| reduced tail sacrifices average             | fixed_5 avg falls to +0.120%; ctx_light avg +0.111%           |
| clean benchmark remains better tail shape   | loss/mv<10bn fixed_20 worst -0.174%, avg +0.122%              |
| no loss-history row passes WSL gate          | do not true-rerun or integrate loss-history yet               |
| next research should pivot                  | benchmark smoke or cleaner signal families before LLM/PDF     |
+---------------------------------------------+---------------------------------------------------------------+
```


## Phase 23 Benchmark True QE Smoke Finding

```text
+---------------------------------------------+---------------------------------------------------------------+
| finding                                     | implication                                                   |
+---------------------------------------------+---------------------------------------------------------------+
| clean benchmark true smoke is non-negative  | keep as calibration benchmark, not a risk policy              |
| true QE impact is far smaller than cheap row| cheap overlay is only a shortlist screen                      |
| drawdown relief is effectively zero         | do not classify as risk-control proof                         |
| next phase needs stronger signal family     | search beyond this small-cap loss/mv fixed_20 profile         |
+---------------------------------------------+---------------------------------------------------------------+
```

## Phase 24 Structured Signal Family Finding

```text
+-------------------------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                                             | decision                   | evidence                                                               |
+-------------------------------------------------------+----------------------------+------------------------------------------------------------------------+
| indicator_decline_ocf_negative_or_leverage_mv_ge_10bn | WATCHLIST_PRIMARY          | 22-loop cheap avg +0.131%, ex-best +0.047%, worst -0.183%, direct down |
| expectation_miss_gap_ge_50/100                         | DIRECT_DOWNSIDE_ONLY       | T+20 abnormal median strongly negative, but QE top50 interaction weak  |
| current_ratio_lt_1 / debt_assets_ge_70                 | WATCHLIST_FEATURE          | direct downside exists, overlay average positive but sparse            |
| immediate WSL true rerun                               | REJECT_NOW                 | no rule passed strict cheap gate; best score 55.6 below 60             |
| next empirical step                                    | REFINE_OR_TRUE_QE_SMOKE    | either refine top rule thresholds or run one-loop WSL smoke if desired |
+-------------------------------------------------------+----------------------------+------------------------------------------------------------------------+
```

## Phase 25 Threshold Refinement Finding

```text
+---------------------------+----------------------+---------------------------------------------------------------------------------+
| candidate                 | decision             | evidence                                                                        |
+---------------------------+----------------------+---------------------------------------------------------------------------------+
| ocf/leverage 10-30bn      | WATCHLIST_PRIMARY    | 15/22 positive, avg +0.124%, ex-best +0.040%, worst -0.100%, T+20 median -0.71% |
| q_ocf_to_sales < 0 >=10bn | WATCHLIST_PRIMARY    | 14/22 positive, avg +0.117%, ex-best +0.068%, worst -0.092%, T+20 median -0.93% |
| current_ratio <0.8 >=10bn | WATCHLIST_SECONDARY  | avg +0.114% but 120td/fixed20 and worst -0.273%; direct downside stronger       |
| 30-100bn / debt>=90       | DIRECT_DOWNSIDE_ONLY | direct abnormal median strongly negative, but cheap overlay sparse/weak         |
| WSL true QE               | DEFER                | no row passed score>=60; run parameter-shape sweep first                        |
+---------------------------+----------------------+---------------------------------------------------------------------------------+
```

## Phase 26 Parameter Shape Sweep Finding

```text
+---------------------------+----------------------+---------------------------------------------------------------------+
| candidate                 | decision             | evidence                                                            |
+---------------------------+----------------------+---------------------------------------------------------------------+
| q_ocf_to_sales < 0 >=10bn | WATCHLIST_PRIMARY    | best sweep row score 56.1 at 60td and 15% rank penalty              |
| ocf/leverage 10-30bn      | WATCHLIST_PRIMARY    | still strong at 55.5, but not improved enough                       |
| current_ratio <0.8 >=10bn | WATCHLIST_SECONDARY  | top score 47.1, useful but clearly behind the first two             |
| debt>=90 / 30-100bn       | DIRECT_DOWNSIDE_ONLY | direct downside persists but overlay interaction remains weak       |
| WSL true QE               | DEFER                | best score still below 60, so keep searching before expensive rerun |
+---------------------------+----------------------+---------------------------------------------------------------------+
```

## Phase 27 q_ocf Fine Sweep Finding

```text
+--------------------------------------+-------------------+--------------------------------------------------------------------------------+
| candidate                            | decision          | evidence                                                                       |
+--------------------------------------+-------------------+--------------------------------------------------------------------------------+
| q_ocf_to_sales < 0 >=10bn 90td 15%   | TRUE_QE_CANDIDATE | score 68.4, avg +0.181%, ex-best +0.100%, worst -0.183%, drop 16               |
| q_ocf_to_sales < 0 >=10bn 90td 17.5% | TRUE_QE_CANDIDATE | same cheap score/effect as 15%, can be a sensitivity check                     |
| q_ocf_to_sales < 0 >=10bn 90td 20%   | TRUE_QE_CANDIDATE | score 67.6, slightly worse tail -0.252%                                        |
| WSL true QE                          | RUN_NEXT          | candidate now passes strict cheap gate; use one-loop full-universe smoke first |
+--------------------------------------+-------------------+--------------------------------------------------------------------------------+
```

## Phase 28 q_ocf WSL True QE Smoke Finding

```text
+--------------------------------------+-------------------+--------------------------------------------------------------------------------+
| candidate                            | decision          | evidence                                                                       |
+--------------------------------------+-------------------+--------------------------------------------------------------------------------+
| q_ocf_to_sales < 0 >=10bn 90td 15%   | KEEP_RESEARCH     | true ann excess +0.0907pp, IR +0.00549, MDD relief near zero                  |
| cheap-to-true translation            | NEED_DIAGNOSTIC   | strict cheap gate passed but one-loop true effect is modest                    |
| runtime promotion                    | REJECT            | no buy ban, forced sell, score boost, DB policy, or Paper/QE integration       |
| next empirical step                  | HOLDING_HIT_STUDY | compare rank-date penalties, actual holdings, top-k drops, and replacement PnL |
+--------------------------------------+-------------------+--------------------------------------------------------------------------------+
```
