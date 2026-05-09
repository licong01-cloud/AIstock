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
| 17    | true QE rerun design / dry-run harness          | pending    | define traceable rerun path without production QE coupling   |
| 18    | LLM/PDF preprocessing design                    | deferred   | start only after structured signals show value               |
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
