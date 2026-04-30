# HMM DB版本 vs 动态候选 1年脚本对比验证

- 生成时间: 2026-04-29T02:32:45.369750+00:00
- 窗口: 2025-03-11 ~ 2026-03-03
- 方法: qlib daily Top50 等权, 5D rebalance, trailing 5D/10D/20D raw score
- 范围: 不写数据库、不启动QE实验、不修改AIstock后端/前端业务代码

## 结论

- 正式口径只看 PIT-compatible，当前最优是 `HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag::preset_A`，总收益 -0.81%，Sharpe 0.142，相对 No-HMM 20.19%。
- DB中覆盖完整1年窗口的既有系数多为 diagnostic-only，因为训练/验证截止晚于 2025-03-11，不能作为正式最优结论。
- 未覆盖完整1年窗口的DB系数没有参与排名，避免把6个月或单日结果混入1年对比。

## 排名

| Rank | Version | Source | PIT | Total | Ann. | Sharpe | MaxDD | Monthly Win | Avg Replaced | 5D Spread | 10D Spread | 20D Spread |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | `HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag::preset_A` | db | Y | -0.81% | -0.90% | 0.142 | -30.91% | 54.55% | 14.69 | 1.74% | 1.59% | 1.39% |
| 2 | `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075` | offline_dynamic | Y | -0.81% | -0.90% | 0.142 | -30.91% | 54.55% | 14.69 | 1.74% | 1.59% | 1.39% |
| 3 | `HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag::preset_A` | db | Y | -0.95% | -1.06% | 0.138 | -30.91% | 54.55% | 14.69 | 1.73% | 1.62% | 1.44% |
| 4 | `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p10` | offline_dynamic | Y | -0.95% | -1.06% | 0.138 | -30.91% | 54.55% | 14.69 | 1.73% | 1.62% | 1.44% |
| 5 | `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_B` | db | N | -12.89% | -14.32% | -0.440 | -29.44% | 45.45% | 34.80 | 0.30% | -0.21% | 1.11% |
| 6 | `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_A` | db | N | -18.51% | -20.49% | -0.682 | -32.58% | 45.45% | 30.58 | 0.08% | -0.09% | 1.30% |
| 7 | `NO_HMM_BASELINE` | baseline | Y | -21.00% | -23.20% | -0.628 | -37.34% | 36.36% | 0.00 |  |  |  |

## 入库判断

- 暂不建议入库：正式PIT口径没有证明新动态候选显著优于可比基线。

## 未纳入完整1年排名的DB系数

| Version | Preset | Coverage | Reason |
|---|---|---|---|

## 产物

- JSON: `/mnt/f/Dev/AIstock/.codex_tmp/hmm_db_after_registration_1y_20260429/run_summary.json`
- Summary CSV: `/mnt/f/Dev/AIstock/.codex_tmp/hmm_db_after_registration_1y_20260429/summary.csv`
- Monthly CSV: `/mnt/f/Dev/AIstock/.codex_tmp/hmm_db_after_registration_1y_20260429/monthly.csv`
- Contributions CSV: `/mnt/f/Dev/AIstock/.codex_tmp/hmm_db_after_registration_1y_20260429/contributions.csv`
