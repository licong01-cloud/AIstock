# Phase 12 Context-Aware Financial Distress Overlay Research - 2026-05-09

Research-only offline QE overlay study. This phase tests whether a rank-aware, severity-aware, decaying score-down is a better design direction than plain fixed-rank demotion. It still does not modify QE, Paper Trading, Selection Center, QMT, or any runtime trading consumer.

## Scope

```text
+--------------------+---------------------------------------------------------------------------------------------------------------------------------+
| item               | value                                                                                                                           |
+--------------------+---------------------------------------------------------------------------------------------------------------------------------+
| branch             | codex/financial-distress-rerank-20260508                                                                                        |
| runtime boundary   | no QE/Paper/Selection/QMT runtime integration                                                                                   |
| date range         | 2024-07-01 -> 2026-04-27                                                                                                        |
| loops              | 10                                                                                                                              |
| light/severity run | reports/event_signal/financial_distress_context_qe_overlay/financial_distress_qe_multiloop_20240701_20260509_124606.md          |
| balanced run       | reports/event_signal/financial_distress_context_balanced_qe_overlay/financial_distress_qe_multiloop_20240701_20260509_130054.md |
| validations        | 480 + 320 = 800                                                                                                                 |
+--------------------+---------------------------------------------------------------------------------------------------------------------------------+
```

## Context Profiles Tested

```text
+-----------------------------------+-----------------------------------------------+-------------------------------------------------+
| profile                           | design                                        | intended use                                    |
+-----------------------------------+-----------------------------------------------+-------------------------------------------------+
| rank_decay_light                  | rank + trading-day decay only                 | minimal score-down probe                        |
| rank_decay_severity               | rank + decay + stronger severity add-ons      | risk-weighted context probe                     |
| rank_decay_sector_relief          | severity profile with industry-cluster relief | plate-rotation compatibility probe              |
| rank_decay_balanced               | higher base + moderate severity + decay       | main Phase 12 runtime-shape candidate           |
| rank_decay_balanced_sector_relief | balanced profile + industry-cluster relief    | same candidate with sector-relief compatibility |
+-----------------------------------+-----------------------------------------------+-------------------------------------------------+
```

## Best Context Rows By Rule

```text
+----------------------------+-----------+---------------------------------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+-----------+
| rule                       | active_td | mode                                  | pos/loops | blocked | eval_topk | dropped | repl | avg_pen | avg_ret_d | med_ret_d | min_ret_d | max_ret_d |
+----------------------------+-----------+---------------------------------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+-----------+
| indicator_decline_10_30bn  | 20        | ctx_rank_decay_balanced_sector_relief | 6/10      | 102     | 101       | 5       | 2    | 12.03%  | 0.20%     | 0.00%     | -0.00%    | 1.88%     |
| indicator_decline_30_100bn | 242       | ctx_rank_decay_balanced_sector_relief | 5/10      | 181     | 181       | 4       | 2    | 14.90%  | 0.04%     | 0.00%     | -0.00%    | 0.24%     |
| smallcap_loss_mv50         | 242       | ctx_rank_decay_balanced_sector_relief | 6/10      | 247     | 246       | 8       | 4    | 16.13%  | 0.18%     | 0.00%     | -0.00%    | 0.80%     |
| structured_risk_10_30bn    | 242       | ctx_rank_decay_balanced_sector_relief | 4/10      | 1246    | 1238      | 24      | 12   | 13.18%  | 0.17%     | -0.00%    | -0.37%    | 1.93%     |
+----------------------------+-----------+---------------------------------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+-----------+
```

## Comparison With Previous Benchmarks

```text
+---------------------------+-----------------------+-----------+---------------------------------------+-----------+---------+---------+-----------+-----------+-----------+
| rule                      | scenario              | active_td | mode                                  | pos/loops | dropped | avg_pen | avg_ret_d | min_ret_d | max_ret_d |
+---------------------------+-----------------------+-----------+---------------------------------------+-----------+---------+---------+-----------+-----------+-----------+
| smallcap_loss_mv50        | best old benchmark    | 60        | rank20                                | 6/10      | 6       | 20.00%  | 0.20%     | -0.00%    | 1.02%     |
| smallcap_loss_mv50        | best context          | 242       | ctx_rank_decay_balanced_sector_relief | 6/10      | 8       | 16.13%  | 0.18%     | -0.00%    | 0.80%     |
| indicator_decline_10_30bn | old rank20 aggressive | 242       | rank20                                | 4/10      | 36      | 20.00%  | 0.29%     | -1.68%    | 4.34%     |
| indicator_decline_10_30bn | old severity 60td     | 60        | sev_bal                               | 6/10      | 6       | 10.00%  | 0.20%     | -0.00%    | 1.88%     |
| indicator_decline_10_30bn | best context          | 20        | ctx_rank_decay_balanced_sector_relief | 6/10      | 5       | 12.03%  | 0.20%     | -0.00%    | 1.88%     |
| structured_risk_10_30bn   | old severity 60td     | 60        | sev_bal                               | 6/10      | 11      | 10.49%  | 0.21%     | -0.00%    | 1.88%     |
| structured_risk_10_30bn   | best context          | 242       | ctx_rank_decay_balanced_sector_relief | 4/10      | 24      | 13.18%  | 0.17%     | -0.37%    | 1.93%     |
+---------------------------+-----------------------+-----------+---------------------------------------+-----------+---------+---------+-----------+-----------+-----------+
```

## Decisions

```text
+----------------------------+------------------------+----------------------------------------------------------------------------------------------------------+
| candidate                  | phase-12 decision      | reason                                                                                                   |
+----------------------------+------------------------+----------------------------------------------------------------------------------------------------------+
| indicator_decline_10_30bn  | KEEP_CONTEXT_CANDIDATE | 20/60td balanced context pos 6/10 avg 0.20%, min ~0; close to old severity but more policy-shaped.       |
| smallcap_loss_mv50         | KEEP_BENCHMARK         | Context is stable but lower than old rank20 benchmark; use to compare risk-aware designs.                |
| structured_risk_10_30bn    | RESEARCH_ONLY          | Context reduces worst tail vs old rank20 but still only 4/10 positive for best avg row.                  |
| indicator_decline_30_100bn | WATCHLIST_ONLY         | Positive but tiny avg effect, too few drops; not enough for runtime action.                              |
| sector relief              | NOT_DECISION_DRIVER    | Balanced sector-relief and non-relief results are mostly identical in this run; keep compatibility only. |
+----------------------------+------------------------+----------------------------------------------------------------------------------------------------------+
```

## Interpretation

- Context-aware overlay did not universally beat the old fixed `rank20` or plain `severity_balanced` benchmarks on average return.
- For `indicator_large_decline_mv_10_30bn`, balanced context at 20/60 trading days matched the desired safety shape: `6/10` positive loops, average return delta about `0.20%`, and no materially negative worst loop in this approximation.
- The aggressive old `rank20` 242-day row still has higher average return for `indicator_large_decline_mv_10_30bn`, but it also has a much worse tail (`min_ret_d -1.68%`). It is therefore not the preferred first runtime-shaped design.
- Sector-relief did not materially change top results in this validation; keep the profile capability for future plate-rotation compatibility, but do not make sector relief a selection criterion now.
- Phase 12 supports a future non-hard overlay design: score-down only, rank-aware, effective for roughly 20-60 trading days, no hard buy ban, no forced sell.

## Next Research Direction

- Convert the Phase 12 preferred design into a proposed signal-policy configuration document, still outside runtime.
- Before runtime integration, validate on additional QE experiments or rerun a true QE overlay/backtest path if available, because current overlay is an approximation over persisted QE artifacts.
- Continue treating LLM/PDF as deferred; structured financial data is already enough to define a first score-down candidate.
