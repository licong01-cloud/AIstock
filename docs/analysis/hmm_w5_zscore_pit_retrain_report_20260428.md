# HMM w5 zscore PIT 重训验证报告

日期：2026-04-28
目的：验证此前 6 个月脚本收益最好的 `HMM_COVFIX_w5_zscore_candidate::preset_A` 是否只是因为 Train/Validation 与回测窗口重叠导致的 diagnostic-only 偏差。

## 1. 结论

可以重新选择训练集和验证集，并且本次已经完成了不重叠 PIT 重训。结论是：

- 旧 `w5 zscore preset_A` 的 +6.46% 脚本收益 **没有在 PIT 重训后复现**。
- 新 PIT w5 zscore preset_A：总收益 -16.38%，Sharpe -1.134。
- 新 PIT w5 zscore preset_B：总收益 -14.98%，Sharpe -1.025。
- 因此，旧 diagnostic-only w5 zscore 的 +6.46% 很可能含有训练/验证窗口重叠带来的未来信息污染或窗口选择偏差，不能作为正式最优版本。
- 目前正式 PIT-compatible 口径下，仍然是 `HMM_COVFIX_w3_raw_same_params::preset_B` 最优。

## 2. 新 PIT 训练版本

- Display name：`HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore`
- Config ID：`c095ab83-48f4-453d-9eb9-c1987b6bd7fe`
- Snapshot ID：`b6e18fc0-2b58-4f8b-a27b-353bdf203c6f`
- Job ID：`a40d9db5-36d6-4384-bbea-2f20d4748931`
- Model：`backend/data/hmm_models/c095ab83-48f4-453d-9eb9-c1987b6bd7fe/2026-04-28/models.json`
- Preset A coefficients：`backend/data/hmm_models/c095ab83-48f4-453d-9eb9-c1987b6bd7fe/2026-04-28/coefficients_preset_A_2025-09-01_2026-03-03.json`
- Preset B coefficients：`backend/data/hmm_models/c095ab83-48f4-453d-9eb9-c1987b6bd7fe/2026-04-28/coefficients_preset_B_2025-09-01_2026-03-03.json`
- Training result：`backend/data/hmm_models/c095ab83-48f4-453d-9eb9-c1987b6bd7fe/2026-04-28/training_result.json`

## 3. 时间切分

| 区间 | 日期 | 是否与 6 个月回测重叠 |
|---|---|---|
| Train | 2022-09-01 ~ 2025-05-30 | 不重叠 |
| Validation | 2025-06-02 ~ 2025-08-29 | 不重叠 |
| Script backtest | 2025-09-01 ~ 2026-03-03 | 验证窗口 |

旧 diagnostic w5 zscore 的 split 是：

- Train：2023-01-30 ~ 2026-01-23
- Validation：2026-01-26 ~ 2026-04-24

该旧 split 与 2025-09-01 ~ 2026-03-03 回测窗口重叠，所以之前只能标记为 diagnostic-only。

## 4. 训练指标

- 训练行业：131/131
- rolling_window：5
- zscore：true
- covariance_type：diag
- covariance fixed sectors：121
- covariance anomaly count：248

Validation forward return by label：

| Label spread | 1D | 2D | 3D | 5D | 10D | 20D |
|---|---:|---:|---:|---:|---:|---:|
| trending - fading | +0.0044% | -0.0355% | -0.0461% | -0.1329% | -0.0056% | +0.4194% |

解释：

- 新 PIT w5 zscore 在 5D 和 10D 上没有稳定验证优势。
- 20D spread 为正，但本次 5D 调仓脚本验证没有转化为组合收益优势。
- 这说明 `preset_A` / `preset_B` 固定奖励 trending 的逻辑仍然不稳。

## 5. 六个月脚本回测结果

输出：

- JSON：`.codex_tmp/hmm_w5_zscore_pit_backtest_20260428.json`
- Summary CSV：`.codex_tmp/hmm_w5_zscore_pit_backtest_20260428_summary.csv`
- Monthly CSV：`.codex_tmp/hmm_w5_zscore_pit_backtest_20260428_monthly.csv`
- Markdown：`.codex_tmp/hmm_w5_zscore_pit_backtest_20260428.md`

| Version | PIT status | Total | AnnRet | Sharpe | MaxDD | Avg HMM-only 5D | Avg raw-only 5D |
|---|---|---:|---:|---:|---:|---:|---:|
| Old w5 zscore preset_A | diagnostic-only | +6.46% | +14.04% | +0.511 | -12.28% | +0.25% | -1.24% |
| New PIT w5 zscore preset_A | PIT-compatible | -16.38% | -31.31% | -1.134 | -23.67% | -0.77% | -0.32% |
| New PIT w5 zscore preset_B | PIT-compatible | -14.98% | -28.88% | -1.025 | -22.40% | -0.69% | -0.33% |
| w3 raw same-params preset_B | PIT-compatible | -9.48% | -18.87% | -0.703 | -12.99% | +0.37% | +0.02% |
| Raw/no-HMM | raw | -13.98% | -27.11% | -0.848 | -19.03% | N/A | N/A |

## 6. 月度差异

| Month | Raw | w3 preset_B | Old w5 diagnostic | New PIT w5 A | New PIT w5 B |
|---|---:|---:|---:|---:|---:|
| 2025-09 | -8.08% | -0.13% | +1.19% | -11.90% | -11.37% |
| 2025-10 | -1.78% | -1.35% | -0.25% | +0.38% | +0.38% |
| 2025-11 | -3.76% | -7.49% | +5.19% | -8.62% | -8.62% |
| 2025-12 | +7.42% | +5.88% | +8.23% | +9.17% | +10.72% |
| 2026-01 | -13.14% | -9.81% | -8.34% | -13.48% | -13.78% |
| 2026-02 | +6.09% | +4.01% | +1.07% | +9.56% | +9.56% |

关键变化：

- 旧 diagnostic w5 的强势月份是 2025-09 和 2025-11；新 PIT w5 在这两个月表现很差。
- 新 PIT w5 在 2025-12 和 2026-02 有优势，但无法抵消 2025-09、2025-11、2026-01 的回撤。

## 7. 股票贡献摘要

| Version | Top contributors | Bottom contributors |
|---|---|---|
| New PIT w5 preset_A | `603778.SH` +2.27%, `001331.SZ` +2.19%, `002713.SZ` +1.97% | `601116.SH` -1.19%, `688205.SH` -1.19%, `300620.SZ` -1.01% |
| New PIT w5 preset_B | `603778.SH` +2.27%, `001331.SZ` +2.19%, `002713.SZ` +1.97% | `601116.SH` -1.19%, `688205.SH` -1.19%, `300620.SZ` -1.01% |

## 8. 最终排序

按正式 PIT-compatible 口径：

1. `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_B`：-9.48%，当前最优正式候选。
2. `HMM_BASELINE_ORIGINAL_w3_raw_unfixed::preset_A` / `HMM_COVFIX_w3_raw_same_params::preset_A`：-11.03%。
3. Raw/no-HMM：-13.98%。
4. `HMM_COVFIX_w5_zscore_PIT_6m::preset_B`：-14.98%。
5. `HMM_COVFIX_w5_zscore_PIT_6m::preset_A`：-16.38%。
6. Horizon v2 risk_only / conservative / main：均更弱。

如果不看 PIT，只看绝对收益，旧 w5 diagnostic 仍是 +6.46%，但这次 PIT 重训已经说明它不能作为正式版本依据。

## 9. 建议

- 不要把旧 `w5 zscore preset_A` 当作最优正式版本；它的收益在无重叠重训后消失。
- 当前正式 HMM 候选仍保留 `w3 raw same-params preset_B`。
- 若继续研究 w5/zscore，应改为 rolling 多快照验证，而不是单一窗口重训。
- 下一轮重点应转向 replacement utility：限制 HMM 替换 Top50 的数量，并直接优化 HMM-only vs raw-only 的 5D/10D/20D forward return。
