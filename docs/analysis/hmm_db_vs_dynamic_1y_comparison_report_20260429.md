# HMM DB版本 vs 动态候选 1年脚本对比验证

- 生成时间: 2026-04-29T02:03:34.338943+00:00
- 窗口: 2025-03-11 ~ 2026-03-03
- 方法: qlib daily Top50 等权, 5D rebalance, trailing 5D/10D/20D raw score
- 范围: 不写数据库、不启动QE实验、不修改AIstock后端/前端业务代码

## 结论

- 正式口径只看 PIT-compatible，当前最优是 `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075`，总收益 -0.81%，Sharpe 0.142，相对 No-HMM 20.19%。
- DB中覆盖完整1年窗口的既有系数多为 diagnostic-only，因为训练/验证截止晚于 2025-03-11，不能作为正式最优结论。
- 未覆盖完整1年窗口的DB系数没有参与排名，避免把6个月或单日结果混入1年对比。

## 排名

| Rank | Version | Source | PIT | Total | Ann. | Sharpe | MaxDD | Monthly Win | Avg Replaced | 5D Spread | 10D Spread | 20D Spread |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075` | offline_dynamic | Y | -0.81% | -0.90% | 0.142 | -30.91% | 54.55% | 14.69 | 1.74% | 1.59% | 1.39% |
| 2 | `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p10` | offline_dynamic | Y | -0.95% | -1.06% | 0.138 | -30.91% | 54.55% | 14.69 | 1.73% | 1.62% | 1.44% |
| 3 | `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore::preset_A` | db | N | -8.74% | -9.74% | -0.182 | -27.27% | 36.36% | 29.69 | 0.42% | 0.43% | 1.02% |
| 4 | `HMM_HORIZON_V2_w5w10w20_oos6m__n3_diag_ms75_no_limitup::preset_A` | db | N | -11.20% | -12.45% | -0.239 | -32.26% | 36.36% | 27.82 | 0.24% | -0.25% | -0.51% |
| 5 | `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_B` | db | N | -12.89% | -14.32% | -0.440 | -29.44% | 45.45% | 34.80 | 0.30% | -0.21% | 1.11% |
| 6 | `HMM_BASELINE_ORIGINAL_w3_raw_unfixed__n3_diag_rw3_nozscore::preset_A` | db | N | -18.51% | -20.49% | -0.682 | -32.58% | 45.45% | 30.58 | 0.08% | -0.09% | 1.30% |
| 7 | `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_A` | db | N | -18.51% | -20.49% | -0.682 | -32.58% | 45.45% | 30.58 | 0.08% | -0.09% | 1.30% |
| 8 | `NO_HMM_BASELINE` | baseline | Y | -21.00% | -23.20% | -0.628 | -37.34% | 36.36% | 0.00 |  |  |  |

## 入库判断

- 建议进入“待入库候选”但暂不写DB：`p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075` 明显优于 No-HMM，也优于本次可正式比较的PIT版本。
- 入库前建议再由你确认：是否只入库主候选，还是同时保留 conf=0.10 作为稳健备选。

## 未纳入完整1年排名的DB系数

| Version | Preset | Coverage | Reason |
|---|---|---|---|
| `HMM_HORIZON_V2_w5w10w20_oos6m__n3_diag_ms75_no_limitup` | `preset_horizon_v2` | 2025-09-01 ~ 2026-03-03 | coefficient artifact does not cover full 1Y window |
| `HMM_HORIZON_V2_w5w10w20_oos6m__n3_diag_ms75_no_limitup` | `preset_horizon_v2_conservative` | 2025-09-01 ~ 2026-03-03 | coefficient artifact does not cover full 1Y window |
| `HMM_HORIZON_V2_w5w10w20_oos6m__n3_diag_ms75_no_limitup` | `preset_horizon_v2_risk_only` | 2025-09-01 ~ 2026-03-03 | coefficient artifact does not cover full 1Y window |
| `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore` | `preset_A` | 2025-09-01 ~ 2026-03-03 | coefficient artifact does not cover full 1Y window |
| `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore` | `preset_A` | 2026-03-04 ~ 2026-03-04 | coefficient artifact does not cover full 1Y window |
| `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore` | `preset_A` | 2026-04-28 ~ 2026-04-28 | coefficient artifact does not cover full 1Y window |
| `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore` | `preset_B` | 2025-09-01 ~ 2026-03-03 | coefficient artifact does not cover full 1Y window |

## 产物

- JSON: `/mnt/f/Dev/AIstock/.codex_tmp/hmm_db_vs_dynamic_1y_20260429/run_summary.json`
- Summary CSV: `/mnt/f/Dev/AIstock/.codex_tmp/hmm_db_vs_dynamic_1y_20260429/summary.csv`
- Monthly CSV: `/mnt/f/Dev/AIstock/.codex_tmp/hmm_db_vs_dynamic_1y_20260429/monthly.csv`
- Contributions CSV: `/mnt/f/Dev/AIstock/.codex_tmp/hmm_db_vs_dynamic_1y_20260429/contributions.csv`
