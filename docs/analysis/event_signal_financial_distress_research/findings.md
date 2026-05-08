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
