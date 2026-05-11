# Event Signal Financial Distress Research Task Plan

## Purpose

Maintain a restart-safe research trail for AIstock event-signal financial-distress studies. This is a research-only branch/worktree plan and must not be treated as runtime approval.

## Workspace Boundary

```text
Dedicated worktree : F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508
Branch             : codex/financial-distress-rerank-20260508
Remote branch      : origin/codex/financial-distress-rerank-20260508
Do not merge main  : true, unless the user explicitly asks
Production 8001    : do not restart or touch
Runtime code scope : do not modify QE/Paper/Selection/QMT runtime without explicit user confirmation
Research scope     : backend/services/event_signal, backend/tests/event_signal, docs/analysis, validation records
Generated reports  : reports/ is ignored; commit only curated docs and validation records
```

## Completed Phases

```text
+-------+------------------------------------------+----------+-------------------------------------------------------------+
| phase | topic                                    | status   | commit/docs                                                 |
+-------+------------------------------------------+----------+-------------------------------------------------------------+
| 1     | single-loop financial distress overlay   | complete | event_signal_financial_distress_qe_overlay_result_20260508  |
| 2     | multi-loop overlay validation            | complete | event_signal_financial_distress_multiloop_qe_overlay_result |
| 3     | market-cap size bucket split             | complete | event_signal_financial_distress_size_bucket_qe_overlay      |
| 4     | fixed score-down rerank overlay          | complete | d87e308 / score_down_qe_overlay_result                      |
| 5     | severity score-down overlay              | complete | 46fe636 / severity_qe_overlay_result                        |
| 6     | market-cap and industry exposure audit   | complete | 9c61508 / exposure_qe_overlay_result                        |
| 7     | rolling loss-history rules               | complete | bf67daa / loss_history_qe_overlay_result                    |
| 8     | market-cap bucket report for every signal| complete | market_cap_bucket_qe_overlay_result                         |
| 9     | medium/large-cap event families          | complete | mid_large_qe_overlay_result_20260509                        |
| 10    | size/loss-history/decay refinements      | complete | refinement_qe_overlay_result_20260509                       |
| 18    | copied-loop true QE smoke rerun          | complete | true_qe_rerun_smoke_result_20260509                         |
| 19    | WSL full-universe true QE parity rerun   | complete | true_qe_wsl_full_universe_result_20260510                   |
| 20    | selective true QE rerun shortlist        | complete | selective_true_qe_shortlist_20260510                        |
| 21    | shortlist cheap 22-loop overlay expansion| complete | phase21_22_loop_overlay_result_20260510                     |
| 22    | loss-history tail-control sweep          | complete | phase22_tail_control_result_20260510                        |
| 23    | benchmark true QE smoke                  | complete | benchmark_true_qe_smoke_result_20260510                     |
| 24    | structured signal-family screen          | complete | phase24_signal_family_screen_result_20260510                |
+-------+------------------------------------------+----------+-------------------------------------------------------------+
```

## Current Research Baseline

```text
+----------------------------------------+----------------------+------------------------------------------------+
| baseline                               | current conclusion   | use                                            |
+----------------------------------------+----------------------+------------------------------------------------+
| loss_to_market_cap_ge_50pct_mv_lt_10bn | strongest so far     | benchmark for every new financial-risk signal |
| fixed rank20 score-down                | simple stable mode   | baseline score-down simulation                |
| severity balanced score-down           | explainable variant  | compare with fixed rank20                     |
| loss-history-only                      | too broad standalone | feature candidate, not runtime rule           |
+----------------------------------------+----------------------+------------------------------------------------+
```

## Next Phases

```text
+-------+----------------------------------------------+------------+--------------------------------------------------------------+
| phase | next research                                 | status     | validation requirement                                       |
+-------+----------------------------------------------+------------+--------------------------------------------------------------+
| 9     | medium/large-cap event families               | complete   | structured miss/decline tested on 10 QE loops                |
| 10    | loss history + size + decay refinements       | complete   | industry explanatory only; no neutralization                 |
| 11    | sector-regime attribution + direct event study | complete   | direct raw/abnormal returns + sector attribution validated   |
| 12    | context-aware overlay rule research             | complete   | rank/severity/decay context profiles tested on 10 QE loops   |
| 13    | proposed non-hard signal-policy config          | complete   | config proposal documented; no runtime integration           |
| 14    | additional QE experiment validation             | complete   | 12 extra loops tested; 60td weakly better than 20td          |
| 15    | non-hard parameter sweep across loop sets       | complete   | 22-loop sweep shows 60td context-balanced as best shape      |
| 16    | robustness gate / persistence decision          | complete   | keep research-only; reject runtime and DB promotion for now  |
| 17    | true QE rerun design / dry-run harness          | complete   | pred-backtest materializer path designed and unit-tested      |
| 18    | copied-loop true QE smoke rerun                 | complete   | technical path passes; full-universe PnL blocked by memory   |
| 19    | memory-safe true QE rerun feasibility           | complete   | WSL full-universe baseline/adjusted parity rerun completed   |
| 20    | selective true QE rerun shortlist               | complete   | no direct true-rerun candidate; cheap expansion first         |
| 21    | shortlist cheap 22-loop overlay expansion       | complete   | loss-history improved but tail too large; no runtime promote |
| 22    | loss-history tail-control parameter sweep       | complete   | tail improved but still not enough for WSL true-rerun        |
| 23    | benchmark true-rerun smoke                      | complete   | clean small-cap benchmark true-QE effect is too weak         |
| 24    | structured signal-family screen                 | complete   | best new family is watchlist, not true-QE candidate          |
| 25    | top-family threshold refinement or WSL smoke     | pending    | refine OCF/leverage rule or run one-loop WSL true-QE smoke   |
| 26    | LLM/PDF preprocessing design                    | deferred   | start only after structured signals stop improving           |
+-------+----------------------------------------------+------------+--------------------------------------------------------------+
```

## Operating Rules

- Commit after each meaningful research phase and push to `origin/codex/financial-distress-rerank-20260508`.
- Do not merge to `main` during research unless explicitly requested.
- Every committed phase needs both a curated analysis doc and a validation record.
- Every new signal must be validated on the same 10 QE loops before being considered for a signal table or runtime hook.
- Every report must compare against `loss_to_market_cap_ge_50pct_mv_lt_10bn` and show market-cap exposure. Industry/sector concentration is explanatory only and must not be used as an automatic rejection gate under the 10m CNY/no-neutralization assumption.
- Financial distress signals remain research-only: no hard buy ban, no forced sell, no live/paper runtime connection.

## Resume Checklist

```text
1. cd F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508
2. git status --short --branch
3. git log --oneline -8
4. read docs/analysis/event_signal_financial_distress_research/task_plan.md
5. read docs/analysis/event_signal_financial_distress_research/findings.md
6. read docs/analysis/event_signal_financial_distress_research/progress.md
7. continue from the first pending phase
```


## Phase 11 Direct Event Study Finding

```text
+-------------------------------------+-----------------------+-----------------------------------------------------------------------------+
| candidate                           | decision              | evidence                                                                    |
+-------------------------------------+-----------------------+-----------------------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn  | CONTEXTUAL_SCORE_DOWN | QE overlay positive, but direct abnormal medians turn negative at T+20/T+60 |
| indicator_large_decline_mv_30_100bn | WATCHLIST_ONLY        | direct abnormal median negative and QE effect weak                          |
| smallcap_loss_mv50                  | KEEP_BENCHMARK        | strong QE benchmark but direct event returns are not a hard-ban proof       |
| structured_risk_10_30bn             | COVERAGE_BENCHMARK    | broad coverage, positive mean but negative abnormal median                  |
+-------------------------------------+-----------------------+-----------------------------------------------------------------------------+
```


## Phase 12 Context Overlay Finding

```text
+----------------------------------------+-----------------------------+----------------------------------------------------------------------------+
| candidate                              | decision                    | evidence                                                                   |
+----------------------------------------+-----------------------------+----------------------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn     | PREFERRED_CONTEXT_CANDIDATE | balanced 20/60td: 6/10 positive, avg about 0.20%, min about 0              |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | KEEP_BENCHMARK              | old rank20 remains stronger avg; context useful as safer design comparison |
| structured_financial_risk_mv_10_30bn   | RESEARCH_ONLY               | context reduces tail but best row only 4/10 positive                       |
| indicator_large_decline_mv_30_100bn    | WATCHLIST_ONLY              | effect is positive but too small                                           |
+----------------------------------------+-----------------------------+----------------------------------------------------------------------------+
```


## Phase 13 Policy Config Proposal Finding

```text
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                              | decision                   | evidence                                                               |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn     | CONFIG_DRAFT_READY         | rank-aware 20/60td score-down can be represented as policy config      |
| financial distress hard action         | REJECT_CURRENT_STAGE       | direct event study is mixed; no hard buy ban or forced sell            |
| event_signal_policy_* existing schema  | SUFFICIENT_FOR_DRAFT       | profile/rule/state/overlay tables can carry config without raw changes |
| final candidate rerank audit           | FUTURE_TRACE_REQUIREMENT   | rank penalty is candidate-list dependent, so audit belongs at consumer  |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
```


## Phase 14 Additional QE Validation Finding

```text
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                              | decision                   | evidence                                                               |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn 20td| DOWNGRADE_TO_TEST          | extra 12 loops average -0.01%, even though positive count is 8/12       |
| indicator_large_decline_mv_10_30bn 60td| KEEP_AS_PRIMARY_TEST       | extra 12 loops average +0.04%, positive count 8/12, tail -0.32%         |
| immediate runtime promotion            | DEFER                      | combined evidence is not stable enough and still artifact-level         |
| next research                          | PARAMETER_SWEEP            | test active windows and moderate penalties on both loop sets            |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
```


## Phase 15 Parameter Sweep Finding

```text
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                              | decision                   | evidence                                                               |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn     | KEEP_RESEARCH_PRIMARY      | 60td context-balanced best overall: avg +0.11%, 14/22 positive         |
| fixed 10% baseline                     | KEEP_BASELINE              | simpler but weaker tail than the best context profile                  |
| 20td variants                          | KEEP_COMPARISON            | useful reference, but not preferred over the 60td context profile      |
| 120td severity profile                 | KEEP_SECONDARY             | still viable, but not better than the 60td balanced candidate          |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
```


## Phase 16 Robustness Gate Finding

```text
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                              | decision                   | evidence                                                               |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| 60td context-balanced score-down       | KEEP_RESEARCH_PRIMARY      | avg +0.1134%, 14/22 positive, tail -0.3206%, but median is 0            |
| runtime integration                    | REJECT_NOW                 | benefit is modest and partly driven by a few replacement events         |
| DB policy draft persistence            | DEFER                      | wait for true QE rerun or stronger breadth before writing policy rows   |
| true QE rerun                          | DESIGN_NEXT                | next phase should define a traceable, research-only rerun path          |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
```


## Phase 17 True QE Rerun Design Finding

```text
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                              | decision                   | evidence                                                               |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| pred-backtest rerun path               | IMPLEMENT_RESEARCH_HARNESS | qrun_limit_minute.py supports external pred.pkl + PortAnaRecord rerun  |
| prediction-date mapping                | REQUIRED                   | Qlib trades date T from prediction date T-1 in current generated config |
| runtime integration                    | STILL_REJECT               | harness is for copied-loop research only; no production QE code change  |
| next empirical step                    | RUN_ONE_LOOP_SMOKE         | compare baseline rerun vs adjusted-pred rerun before 22-loop batch      |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
```

## Phase 18 Copied-Loop Smoke Finding

```text
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                              | decision                   | evidence                                                               |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| copied-loop pred-backtest path          | VALIDATED                  | adjusted pred.pkl completed SigAnaRecord + PortAnaRecord               |
| full-universe true rerun                | FAIL_CURRENT_MACHINE       | copied full-universe attempts reproduced MemoryError                   |
| narrowed-universe PnL comparison        | REJECT_AS_EVIDENCE         | quote universe mismatch makes return/drawdown non-comparable           |
| indicator_large_decline_mv_10_30bn      | KEEP_RESEARCH_ONLY         | materializer works, but no valid PnL proof yet                         |
| immediate runtime promotion             | REJECT                     | Phase 18 is a technical smoke, not a deployment gate                   |
| next empirical step                     | FEASIBILITY_REDESIGN       | solve memory-safe or parity-controlled true rerun before batch         |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
```

## Phase 19 WSL Full-Universe Finding

```text
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                              | decision                   | evidence                                                               |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| WSL full-universe rerun path            | VALIDATED                  | baseline and adjusted full-universe PortAnaRecord completed            |
| Windows full-universe rerun path        | AVOID_FOR_RESEARCH         | Phase 18 MemoryError remains a Windows copied-run blocker              |
| indicator_large_decline_mv_10_30bn      | KEEP_RESEARCH_ONLY         | one-loop full true rerun is weak positive but immaterial               |
| immediate runtime promotion             | REJECT                     | +0.147pp annualized return is insufficient for deployment              |
| next empirical step                     | SELECTIVE_TRUE_RERUN       | screen stronger candidates before expensive WSL multi-loop reruns      |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
```

## Phase 20 Selective True QE Shortlist Finding

```text
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                              | decision                   | evidence                                                               |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
| WSL true-rerun batch                    | REJECT_NOW                 | no candidate passed strict cheap gate                                  |
| indicator_large_decline_mv_10_30bn      | KEEP_WEAK_BASELINE         | 22-loop cheap score ok, but one-loop WSL true rerun was immaterial     |
| structured_financial_risk_mv_ge_10bn    | EXPAND_22_LOOP_FIRST       | best 10-loop cheap row; needs same 22-loop cheap validation before WSL |
| loss_to_market_cap_ge_50pct_mv_lt_10bn  | BENCHMARK_ONLY             | direct event T+5/T+20 medians positive; not a hard-risk proof          |
| next empirical step                     | CHEAP_OVERLAY_EXPANSION    | run 22-loop overlay expansion for top 10-loop candidates               |
+----------------------------------------+----------------------------+------------------------------------------------------------------------+
```

## Phase 21 Cheap 22-Loop Overlay Expansion Finding

```text
+---------------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                                   | decision                   | evidence                                                               |
+---------------------------------------------+----------------------------+------------------------------------------------------------------------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss   | TAIL_CONTROL_NEXT          | avg +0.178%, median +0.021%, but worst loop -1.966%                    |
| structured_financial_risk_mv_ge_10bn         | REJECT_TRUE_RERUN_NOW      | 22-loop avg fell to +0.110% and worst loop reached -0.879%             |
| loss_to_market_cap_ge_50pct                  | BENCHMARK_ALPHA_WATCH      | clean tail, avg +0.122%, but sparse drops and not a risk proof         |
| immediate runtime promotion                  | REJECT                     | no candidate is both strong and tail-safe enough                       |
| next empirical step                          | TAIL_CONTROL_SWEEP         | test softer/rank-aware loss-history profiles before WSL true rerun     |
+---------------------------------------------+----------------------------+------------------------------------------------------------------------+
```

## Phase 22 Loss-History Tail-Control Finding

```text
+---------------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                                   | decision                   | evidence                                                               |
+---------------------------------------------+----------------------------+------------------------------------------------------------------------+
| loss-history severity_balanced               | REJECT_TRUE_RERUN_NOW      | avg +0.178%, but worst loop -1.966%                                    |
| loss-history fixed_5 / ctx_light             | WATCHLIST_ONLY             | worst improved to -0.935%, but still not tail-safe                     |
| loss_to_market_cap_ge_50pct_mv_lt_10bn       | CLEAN_BENCHMARK            | avg +0.122%, ex-best +0.065%, worst -0.174%                            |
| immediate WSL true rerun                     | REJECT_NOW                 | no loss-history row combines high average with acceptable tail         |
| next empirical step                          | BENCHMARK_OR_NEW_FAMILY    | either true-rerun clean benchmark smoke or screen cleaner signal family |
+---------------------------------------------+----------------------------+------------------------------------------------------------------------+
```


## Phase 23 Benchmark True QE Smoke Finding

```text
+---------------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                                   | decision                   | evidence                                                               |
+---------------------------------------------+----------------------------+------------------------------------------------------------------------+
| loss_to_market_cap_ge_50pct_mv_lt_10bn       | BENCHMARK_ONLY             | WSL true ann excess +0.036pp, IR +0.00175, MDD relief near zero        |
| cheap overlay as final evidence              | REJECT_AS_FINAL_GATE       | one-loop cheap +0.80% final delta did not translate materially         |
| immediate runtime promotion                  | REJECT                     | true effect too small and sparse for policy/runtime integration        |
| next empirical step                          | NEW_SIGNAL_FAMILY_SCREEN   | find stronger direct-event or broader financial-risk candidates        |
+---------------------------------------------+----------------------------+------------------------------------------------------------------------+
```

## Phase 24 Structured Signal Family Finding

```text
+-------------------------------------------------------+----------------------------+------------------------------------------------------------------------+
| candidate                                             | decision                   | evidence                                                               |
+-------------------------------------------------------+----------------------------+------------------------------------------------------------------------+
| indicator_decline_ocf_negative_or_leverage_mv_ge_10bn | WATCHLIST_PRIMARY          | best 22-loop cheap row, but does not pass TRUE_QE gate                  |
| expectation miss gap rules                             | DIRECT_DOWNSIDE_ONLY       | strong direct downside; weak top50 overlay interaction                  |
| runtime integration                                    | REJECT                     | financial signals remain non-hard research-only                         |
| next phase                                             | pending                    | refine thresholds or run one-loop WSL smoke for watchlist candidate     |
+-------------------------------------------------------+----------------------------+------------------------------------------------------------------------+
```

## Phase 25 Threshold Refinement Finding

```text
+-------------------------------------------------------+-----------------------+-----------------------------------------------------------------------+
| candidate                                             | decision              | evidence                                                              |
+-------------------------------------------------------+-----------------------+-----------------------------------------------------------------------+
| indicator_decline_ocf_negative_or_leverage_mv_10_30bn | WATCHLIST_PRIMARY     | best Phase25 cheap score 55.5; below TRUE_QE gate                     |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn      | WATCHLIST_PRIMARY     | score 55.3; better ex-best but not enough                             |
| immediate WSL true rerun                              | DEFER                 | no rule crossed score>=60; cheap overlay remains shortlist-only       |
| next phase                                            | PARAMETER_SHAPE_SWEEP | test top rules across shorter/softer fixed penalties before WSL spend |
+-------------------------------------------------------+-----------------------+-----------------------------------------------------------------------+
```

## Phase 26 Parameter Shape Sweep Finding

```text
+-------------------------------------------------------+-------------------+--------------------------------------------------------+
| candidate                                             | decision          | evidence                                               |
+-------------------------------------------------------+-------------------+--------------------------------------------------------+
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn      | WATCHLIST_PRIMARY | best sweep row 56.1 with 60td / 15% penalty            |
| indicator_decline_ocf_negative_or_leverage_mv_10_30bn | WATCHLIST_PRIMARY | 55.5 remains strong but lower than q_ocf_to_sales      |
| immediate WSL true rerun                              | DEFER             | no rule reached 60 yet                                 |
| next phase                                            | TIGHTER_SWEEP     | focus on the best rule and penalty/lifetime refinement |
+-------------------------------------------------------+-------------------+--------------------------------------------------------+
```

## Phase 27 q_ocf Fine Sweep Finding

```text
+--------------------------------------------------+-------------------+---------------------------------------------------------------+
| candidate                                        | decision          | evidence                                                      |
+--------------------------------------------------+-------------------+---------------------------------------------------------------+
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn | TRUE_QE_CANDIDATE | 90td / 15% rank penalty, score 68.4                           |
| runtime integration                              | STILL_REJECT      | cheap overlay is not final PnL evidence                       |
| next phase                                       | WSL_TRUE_QE_SMOKE | materialize pred and run one-loop full-universe pred-backtest |
+--------------------------------------------------+-------------------+---------------------------------------------------------------+
```
