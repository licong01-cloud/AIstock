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

Medium/large-cap structured financial risk evidence is concentrated in the 10-30bn bucket. The next research step should split `indicator_large_decline_mv_ge_10bn` by industry, size sub-buckets, and loss history before any runtime design.
