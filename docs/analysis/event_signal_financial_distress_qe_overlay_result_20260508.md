# AIstock Financial Distress QE Overlay Result

Date: 2026-05-08
Scope: read-only research script; no QE / Selection / Paper / QMT / simulated / live trading runtime changes.
Experiment: `qe_20260507_132049_d4e7` / `Loop1`
Generated report: `reports/event_signal/financial_distress_qe_overlay/financial_distress_qe_overlay_qe_20260507_132049_d4e7_Loop1_20240701_20260508_094938.json`

## 1. Decision Summary

```text
+------------------------------------+--------------------------------------------------------------+
| Question                           | Conclusion                                                   |
+------------------------------------+--------------------------------------------------------------+
| LLM/PDF needed first               | No. Structured loss/mv and loss-history signals are enough    |
|                                    | for the first offline overlay validation.                    |
| Hard block or forced sell allowed  | No. Financial signals stay research-only buy-filter inputs.   |
| Most promising Loop1 rule          | loss_to_market_cap_ge_50pct with 60 trading-day activity.     |
| Rules to defer                     | Broad rolling-loss>=4 rules; they hurt Loop1 return heavily.  |
| Next step                          | Re-test across more QE loops, years, size buckets, industries.|
+------------------------------------+--------------------------------------------------------------+
```

## 2. Baseline

```text
+---------------+----------------------------+
| Metric        | Loop1 baseline             |
+---------------+----------------------------+
| Date range    | 2024-07-01 -> 2026-04-27   |
| Final account | 290,649,636.16             |
| Total return  | 190.65%                    |
| CAGR          | 79.59%                     |
| Max drawdown  | -17.42%                    |
+---------------+----------------------------+
```

## 3. Overlay Results

```text
+-----------+--------------------------------------+----------------+--------------+--------------+------------+-----------+----------------+
| active_td | rule_key                             | mode           | blocked_buys | return_delta | cagr_delta | mdd_delta | final_delta    |
+-----------+--------------------------------------+----------------+--------------+--------------+------------+-----------+----------------+
| 60        | loss_to_market_cap_ge_50pct          | cash           | 6            | 1.26%        | 0.43%      | 0.00%     | 1,262,212.98   |
| 60        | loss_to_market_cap_ge_50pct          | next_candidate | 6            | 3.34%        | 1.13%      | 0.00%     | 3,339,742.70   |
| 60        | forecast_loss_to_market_cap_ge_50pct | cash           | 6            | 1.26%        | 0.43%      | 0.00%     | 1,262,212.98   |
| 60        | forecast_loss_to_market_cap_ge_50pct | next_candidate | 6            | 3.34%        | 1.13%      | 0.00%     | 3,339,742.70   |
| 60        | loss_20_50pct_and_loss_reports_ge_4  | cash           | 10           | 0.48%        | 0.16%      | 0.00%     | 484,908.91     |
| 60        | loss_20_50pct_and_loss_reports_ge_4  | next_candidate | 10           | 0.07%        | 0.03%      | 0.00%     | 74,965.12      |
| 60        | loss_to_market_cap_20_50pct          | cash           | 31           | 1.44%        | 0.49%      | 0.00%     | 1,439,297.13   |
| 60        | loss_to_market_cap_20_50pct          | next_candidate | 31           | -0.64%       | -0.22%     | 0.00%     | -636,246.16    |
| 120       | loss_to_market_cap_ge_50pct          | cash           | 13           | -0.29%       | -0.10%     | 0.00%     | -286,581.25    |
| 120       | loss_to_market_cap_ge_50pct          | next_candidate | 13           | -0.18%       | -0.06%     | 0.00%     | -181,149.05    |
| 120       | forecast_loss_and_loss_reports_ge_4  | cash           | 134          | -14.40%      | -4.94%     | 1.74%     | -14,395,331.46 |
| 120       | forecast_loss_and_loss_reports_ge_4  | next_candidate | 134          | -24.87%      | -8.60%     | -0.12%    | -24,872,481.61 |
| 242       | loss_to_market_cap_ge_50pct          | cash           | 23           | -0.93%       | -0.32%     | 0.00%     | -932,964.15    |
| 242       | loss_to_market_cap_ge_50pct          | next_candidate | 23           | 0.14%        | 0.05%      | 0.00%     | 144,899.69     |
| 242       | forecast_loss_and_loss_reports_ge_4  | cash           | 163          | -15.89%      | -5.46%     | 1.99%     | -15,893,198.33 |
| 242       | forecast_loss_and_loss_reports_ge_4  | next_candidate | 163          | -25.67%      | -8.89%     | -0.09%    | -25,674,906.79 |
+-----------+--------------------------------------+----------------+--------------+--------------+------------+-----------+----------------+
```

Notes:

- `cash`: the filtered buy is not replaced; capital remains as cash.
- `next_candidate`: the filtered buy is replaced by the next available QE prediction candidate.
- `mdd_delta` is overlay max drawdown minus baseline max drawdown; positive means shallower drawdown.
- The table lists key rows only. The generated JSON/MD report contains all 30 validations.

## 4. Initial Rule Interpretation

```text
+--------------------------------------+--------------------------------------------------------------+
| Rule                                 | Interpretation                                               |
+--------------------------------------+--------------------------------------------------------------+
| loss_to_market_cap_ge_50pct          | Keep for more research. 60td is positive; longer windows are |
|                                      | unstable in Loop1.                                           |
| forecast_loss_to_market_cap_ge_50pct | Almost identical to the >=50% rule in Loop1; useful as a     |
|                                      | narrower candidate.                                          |
| loss_20_50pct_and_loss_reports_ge_4  | 60td is slightly positive; longer windows hurt return.       |
| forecast_loss_and_loss_reports_ge_4  | Too broad; filters many profitable baseline positions.       |
| loss_to_market_cap_20_50pct          | 60td cash is positive but replacement is negative; candidate |
|                                      | replacement logic must be validated separately.              |
+--------------------------------------+--------------------------------------------------------------+
```

## 5. Engineering Impact

1. Do not promote rolling-loss>=4 as a formal overlay rule yet; combine it with size, industry, and loss/mv strength first.
2. `loss_to_market_cap_ge_50pct` can enter the next multi-loop offline validation round, but it is still not a live hard rule.
3. Active lifetime must be configurable. Loop1 favors 60 trading days over 120 / 242 trading days.
4. `next_candidate` is not always better than holding cash; replacement selection needs separate validation.
5. Next script evolution should add multi-loop, size-bucket, industry, and yearly stability summaries plus accept / reject / refine decisions.
