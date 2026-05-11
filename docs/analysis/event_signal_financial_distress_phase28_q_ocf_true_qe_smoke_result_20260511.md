# Phase 28 q_ocf WSL True QE Smoke Result - 2026-05-11

Research-only WSL full-universe true QE smoke for `indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn / fixed_15 / 90td`. This validates the Phase-27 cheap-overlay candidate through the real `qrun_limit_minute.py --pred-backtest` portfolio path.

No QE runtime, Selection Center, Paper Trading, QMT, live trading, database schema, database data, or production backend `8001` path was modified.

## Scope

```text
+-------------------+-------------------------------------------------------------+
| item              | value                                                       |
+-------------------+-------------------------------------------------------------+
| branch            | codex/financial-distress-rerank-20260508                    |
| phase             | 28                                                          |
| source loop       | qe_20260507_132049_d4e7 / Loop2                             |
| candidate         | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn            |
| profile           | fixed_15, 90td, previous prediction date, TopK=50           |
| baseline recorder | 7b57828280ad40b988e6574c9a083da6                            |
| adjusted recorder | 8afe567e2bec4dc88a1f3fe15768567b                            |
| runtime impact    | none                                                        |
| DB impact         | none                                                        |
+-------------------+-------------------------------------------------------------+
```

## Materialization Summary

```text
+------------------------+-----------+
| metric                 | value     |
+------------------------+-----------+
| overlay rows           | 41,673    |
| overlay symbols        | 393       |
| penalized symbols      | 386       |
| rank-date penalty rows | 41,537    |
| rank dates touched     | 441       |
| prediction rows        | 2,256,997 |
| changed symbols        | 5,117     |
| top-k drop count       | 25        |
+------------------------+-----------+
```

## Commands

```bash
cd /mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2
python qrun_limit_minute.py conf.yaml --pred-backtest /mnt/f/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260511_q_ocf_qe20260507_loop2/materialized_fixed15/indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn_fixed15_adjusted_pred.pkl
```

Command log and metrics are stored outside the repo:

```text
+------------------+------------------------------------------------------------------------------------------------------------------+
| item             | path                                                                                                             |
+------------------+------------------------------------------------------------------------------------------------------------------+
| adjusted log      | <artifact_root>/wsl_full_universe_q_ocf_fixed15_adjusted.log                                                     |
| materialization   | <artifact_root>/materialized_fixed15/                                                                            |
| metrics snapshot  | <artifact_root>/metrics_snapshot/                                                                                |
+------------------+------------------------------------------------------------------------------------------------------------------+
```

## Full-Universe Metrics

```text
+-------------------------------+---------------+---------------+---------------+--------------------+
| metric                        | baseline      | adjusted      | delta         | interpretation     |
+-------------------------------+---------------+---------------+---------------+--------------------+
| IC                            | 0.0613135033  | 0.0613024523  | -0.0000110511 | weak negative      |
| ICIR                          | 0.6196672847  | 0.6195228810  | -0.0001444037 | weak negative      |
| Rank IC                       | 0.0999630204  | 0.0999648778  | +0.0000018574 | immaterial         |
| Rank ICIR                     | 0.8744420390  | 0.8744033161  | -0.0000387229 | immaterial         |
| annualized excess return cost | 0.4763182376  | 0.4772251230  | +0.0009068854 | weak positive      |
| information ratio cost        | 2.2159536500  | 2.2214440864  | +0.0054904364 | weak positive      |
| max drawdown cost             | -0.1754268423 | -0.1754153342 | +0.0000115081 | effectively zero   |
| daily excess mean cost        | 0.0020013371  | 0.0020051476  | +0.0000038104 | weak positive      |
+-------------------------------+---------------+---------------+---------------+--------------------+
```

## Comparison With Earlier True Smokes

```text
+---------------------------------------------------------+------------------+---------------+---------------+----------------------------------------------+
| candidate                                               | ann excess delta | IR delta      | MDD delta     | decision                                     |
+---------------------------------------------------------+------------------+---------------+---------------+----------------------------------------------+
| indicator_large_decline_mv_10_30bn / ctx-balanced 60td  | +0.0014692370    | +0.0063244896 | +0.0002757115 | still best one-loop smoke                    |
| q_ocf_to_sales < 0 >=10bn / fixed_15 90td               | +0.0009068854    | +0.0054904364 | +0.0000115081 | positive, but weak drawdown relief           |
| loss_to_market_cap_ge_50pct_mv_lt_10bn / fixed_20 242td | +0.0003580542    | +0.0017516633 | +0.0000225773 | clean benchmark only                         |
+---------------------------------------------------------+------------------+---------------+---------------+----------------------------------------------+
```

## Interpretation

```text
+-------------------------------------------+------------------------------------------------------------+------------------------------+
| finding                                   | evidence                                                   | decision                     |
+-------------------------------------------+------------------------------------------------------------+------------------------------+
| WSL true smoke completed                  | 442/442 backtest steps and PortAnaRecord completed         | path usable                  |
| Phase-27 candidate is directionally valid | ann excess +0.000907 and IR +0.00549 vs baseline           | keep as research candidate   |
| drawdown relief is not meaningful         | max-drawdown delta only +0.0000115                         | not a risk-control proof     |
| IC-level impact is mixed                  | IC/ICIR slightly lower, Rank IC slightly higher            | do not treat as alpha factor |
| cheap overlay overstated materiality      | strict cheap gate passed, but true smoke is modest         | cheap gate remains shortlist |
| runtime integration not justified         | one loop, weak effect, no multi-loop true evidence          | no DB policy/runtime hook    |
+-------------------------------------------+------------------------------------------------------------+------------------------------+
```

## Decision

- `indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn / fixed_15 / 90td` passes the technical WSL true-QE smoke and is directionally positive.
- The result is not strong enough for runtime, DB policy, buy-ban, force-sell, Paper/Selection/QE integration, or multi-loop WSL batch promotion yet.
- Compared with prior true smokes, it is stronger than the clean small-cap benchmark on return/IR but weaker than the earlier `indicator_large_decline_mv_10_30bn / ctx-balanced 60td` smoke, especially on drawdown relief.
- Keep it as a secondary research candidate and use it to diagnose how to improve the cheap-screen-to-true-QE translation.

## Next Step

```text
+------+-----------------------------------------------------------------------------------------+--------------------------------------+
| step | action                                                                                  | gate                                 |
+------+-----------------------------------------------------------------------------------------+--------------------------------------+
| 1    | Do not promote q_ocf fixed_15 to runtime                                                | completed                            |
| 2    | Analyze why top-k drops are broad but true PnL is modest                                | compare rank-date hits and holdings  |
| 3    | Test combined or context-aware variants cheaply before more WSL reruns                  | must beat Phase19 true-smoke target  |
| 4    | Reserve WSL true reruns for candidates likely to exceed +0.15pp ann excess or MDD relief | avoid expensive weak validations     |
+------+-----------------------------------------------------------------------------------------+--------------------------------------+
```

## Residual Risks

```text
+--------------------------------------------+-----------------------------------------------------------------------+
| risk                                       | mitigation                                                            |
+--------------------------------------------+-----------------------------------------------------------------------+
| one-loop dependence                        | do not promote; require multi-loop true evidence before runtime work  |
| cheap-overlay mismatch                     | treat cheap overlay as a shortlist gate only                          |
| materialization is rank-order approximation| use only for research pred-backtest, not production strategy logic    |
| broad active overlay                       | inspect actual holdings/top-k overlap before increasing penalty       |
+--------------------------------------------+-----------------------------------------------------------------------+
```
