# HMM Horizon-Aware v2 训练与六个月脚本回测报告

日期：2026-04-28
范围：仅 HMM 训练与脚本级对比验证；未启动 QE 实验，未覆盖旧版本模型、旧系数文件或旧训练脚本。

## 1. 结论摘要

1. **新版 HMM Horizon v2 已成功训练并注册为独立版本**，训练 131/131 个行业，0 个行业跳过，模型与系数均保存到新的 config 目录。
2. **Horizon v2 的训练设计方向是正确的**：不再固定 `trending=1.05`，改为按 5D/10D/20D validation utility 校准；排除了 `limit_up_ratio`，并用更长持仓周期的状态效用标注。
3. **但本次 v2 初版不能晋升为默认 HMM**：6 个月脚本回测中，v2 main / conservative / risk_only 均弱于 Raw/no-HMM 与旧 w3 preset_B。
4. **当前脚本验证下，最好的 PIT-compatible 版本是 `HMM_COVFIX_w3_raw_same_params::preset_B`**，总收益 -9.48%，优于 Raw/no-HMM 的 -13.98%。
5. **`HMM_COVFIX_w5_zscore::preset_A` 表现最好（+6.46%）但不可作为正式结论**，因为其 train/validation 与本次回测窗口重叠，被标记为 diagnostic-only。
6. 新版 v2 的失败不是因为没有按长周期校准，而是说明：**仅用 sector-level HMM 乘法系数调 raw score，仍可能在 5D/10D/20D 股票选择层面放大错误替换**。

## 2. 参考文档与权重

- `docs/HMM_Optimization_Analysis_Report.md`：作为优化假设来源使用，但该文档没有测试数据支撑，权重低。
- `docs/analysis/qe_c5b2_v25_hmm_optimization_reference_20260428.md`：作为主要依据使用；该文档包含 QE c5b2 对比结果、持仓周期、HMM-only 替换与收益差异分析，权重最高。
- `docs/analysis/hmm_horizon_optimization_training_plan_20260428.md`：本次落地训练方案。

核心证据沿用第二份权威文档：

- 新 HMM 的 `trending` 在 3D 以后验证收益为负。
- 策略真实持仓中位数约 4 天，P90 超过 40 天。
- `preset_A` 固定奖励 `trending=1.05` 与真实持仓周期不匹配。
- HMM 验收必须同时看替换股票、HMM-only label、月度收益、股票贡献、资金使用率、买入未成交率、最终持仓数。
- 回测必须尽量使用 PIT/rolling 快照，避免训练截止日期晚于回测早期日期。

## 3. 本次新版 HMM 训练方案

### 3.1 版本隔离

新增文件与资产均为 additive，不覆盖旧脚本或旧模型：

- 训练脚本：`scripts/hmm_horizon_v2_train.py`
- 对比脚本：`scripts/hmm_horizon_v2_compare.py`
- 训练计划：`docs/analysis/hmm_horizon_optimization_training_plan_20260428.md`
- 新模型目录：`backend/data/hmm_models/f1da5529-0109-495f-a2b8-a2033cc31ee8/2026-04-28/`

### 3.2 时间切分

- Train：2022-09-01 ~ 2025-05-30
- Validation / calibration：2025-06-02 ~ 2025-08-29
- Script backtest：2025-09-01 ~ 2026-03-03

该切分保证新 v2 的训练和校准都早于 6 个月验证窗口，属于 PIT-compatible 初验。

### 3.3 特征与训练

v2 使用 10 个 PIT 可用的行业观测特征：

1. `daily_return`
2. `excess_return_5d_mean`
3. `excess_return_10d_mean`
4. `excess_return_20d_mean`
5. `volatility_5d`
6. `volatility_10d`
7. `volatility_20d`
8. `volume_share_5d_mean`
9. `net_mf_ratio_5d_mean`
10. `elg_net_mf_ratio_5d_mean`

关键变化：

- 移除 `limit_up_ratio`，避免复现之前 covariance anomaly 的主导来源。
- 使用 winsorize + train-only z-score，避免 validation/backtest 泄漏。
- HMM 参数：`n_states=3`、`covariance_type=diag`、`n_iter=300`、`min_self_trans=0.75`。
- 状态命名不再按 1D，而按 `0.35 * 5D + 0.35 * 10D + 0.30 * 20D` 的 train-window forward excess utility 排序。
- 系数不固定奖励 `trending`，而在 validation 上按标签 utility 校准，并限制在 0.97 ~ 1.03。

## 4. 新版训练结果

- Display name：`HMM_HORIZON_V2_w5w10w20_oos6m__n3_diag_ms75_no_limitup`
- Config ID：`f1da5529-0109-495f-a2b8-a2033cc31ee8`
- Snapshot ID：`77113d1b-1225-4cb2-9d1c-9d0c24f1d130`
- Job ID：`c0fe6c9c-014b-459a-97b1-11a4bbae6f7f`
- Model：`backend/data/hmm_models/f1da5529-0109-495f-a2b8-a2033cc31ee8/2026-04-28/models.json`
- Main coefficients：`backend/data/hmm_models/f1da5529-0109-495f-a2b8-a2033cc31ee8/2026-04-28/coefficients_preset_horizon_v2_2025-09-01_2026-03-03.json`
- Conservative coefficients：`backend/data/hmm_models/f1da5529-0109-495f-a2b8-a2033cc31ee8/2026-04-28/coefficients_preset_horizon_v2_conservative_2025-09-01_2026-03-03.json`
- Risk-only coefficients：`backend/data/hmm_models/f1da5529-0109-495f-a2b8-a2033cc31ee8/2026-04-28/coefficients_preset_horizon_v2_risk_only_2025-09-01_2026-03-03.json`
- Training result：`backend/data/hmm_models/f1da5529-0109-495f-a2b8-a2033cc31ee8/2026-04-28/training_result.json`

### 4.1 Validation 校准

| Label | Weighted utility | Calibrated coeff | 1D | 3D | 5D | 10D | 20D |
|---|---:|---:|---:|---:|---:|---:|---:|
| fading | +0.034876% | 1.020983 | +0.015825% | -0.010816% | -0.012550% | +0.007830% | +0.121760% |
| neutral | -0.375377% | 0.992210 | -0.031093% | -0.121988% | -0.192542% | -0.335763% | -0.634901% |
| trending | -0.452413% | 0.986807 | -0.025019% | -0.116626% | -0.220057% | -0.390346% | -0.795908% |

解释：

- 训练命名中的 `trending` 在 validation 上仍然是负的，因此 v2 正确地把 `trending` 下调到 0.986807。
- validation 最优标签反而是 `fading`，这再次证明 **state name 不能被固定解释**，必须按每个 snapshot 的 forward return 校准。
- 但 validation 校准正确不等于 portfolio-level 替换正确，后续 6 个月脚本回测显示 v2 仍然会造成不利替换。

## 5. 六个月脚本回测方法

输出文件：

- JSON：`.codex_tmp/hmm_horizon_v2_backtest_20260428.json`
- Summary CSV：`.codex_tmp/hmm_horizon_v2_backtest_20260428_summary.csv`
- Monthly CSV：`.codex_tmp/hmm_horizon_v2_backtest_20260428_monthly.csv`
- Markdown：`.codex_tmp/hmm_horizon_v2_backtest_20260428.md`

方法：

- 回测窗口：2025-09-01 ~ 2026-03-03
- 股票打分：只使用 trailing 5D/10D/20D raw rank，权重 0.35 / 0.35 / 0.30
- HMM overlay：`adjusted_score = raw_score * sector_coefficient`
- 组合：Top50 equal-weight
- 调仓：每 5 个交易日调仓一次，共 24 个 period
- 资金使用率：脚本 close-to-close proxy，Top50 满仓构造，因此 proxy 为 100%
- 买入未成交率：脚本未模拟 minute execution / limit / suspend order fill，因此只输出 close-to-close proxy 0%，不能当真实成交率
- 注意：这不是 QE 实验，不含 V25 minute execution，也不含真实未成交订单模拟。

## 6. 六个月对比结果

| Version | PIT status | Total | AnnRet | Sharpe | MaxDD | Win periods | Avg raw overlap | Avg HMM-only count | Avg HMM-only 5D | Avg raw-only 5D | Final holdings |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| RAW_NO_HMM | raw | -13.98% | -27.11% | -0.848 | -19.03% | 54.17% | 50.00 | 0.00 | N/A | N/A | 50 |
| Baseline original w3 raw preset_A | PIT-compatible | -11.03% | -21.76% | -0.807 | -13.80% | 50.00% | 17.12 | 32.88 | +0.43% | +0.16% | 50 |
| Covfix w3 raw preset_A | PIT-compatible | -11.03% | -21.76% | -0.807 | -13.80% | 50.00% | 17.12 | 32.88 | +0.43% | +0.16% | 50 |
| Covfix w3 raw preset_B | PIT-compatible | -9.48% | -18.87% | -0.703 | -12.99% | 50.00% | 15.00 | 35.00 | +0.37% | +0.02% | 50 |
| Covfix w5 zscore preset_A | diagnostic-only | +6.46% | +14.04% | +0.511 | -12.28% | 54.17% | 22.12 | 27.88 | +0.25% | -1.24% | 50 |
| Horizon v2 main | PIT-compatible | -23.87% | -43.60% | -1.564 | -30.17% | 33.33% | 27.71 | 22.29 | -0.72% | +0.64% | 50 |
| Horizon v2 conservative | PIT-compatible | -22.28% | -41.10% | -1.295 | -26.78% | 50.00% | 43.58 | 6.42 | -2.12% | +1.04% | 50 |
| Horizon v2 risk_only | PIT-compatible | -15.58% | -29.92% | -0.945 | -21.92% | 45.83% | 46.67 | 3.33 | -0.72% | -0.71% | 50 |

关键观察：

- v2 main 替换强度中等，平均与 Raw Top50 重合 27.71 只，HMM-only 平均 22.29 只，但 HMM-only 5D 为 -0.72%，明显低于 raw-only +0.64%。
- v2 conservative 替换很少，平均 HMM-only 仅 6.42 只，但这些替换的 5D 表现更差（-2.12%），说明问题不是单纯“替换太多”，而是替换方向本身错误。
- v2 risk_only 与 raw 最接近，平均只替换 3.33 只，但仍弱于 Raw/no-HMM，说明当前 sector-level 风险剔除信号没有稳定 alpha。
- w3 preset_B 是当前最好的 PIT-compatible 脚本候选；w5 zscore 虽然看起来最好，但因 train/val overlap 只能作为下一轮 rolling/PIT 方向线索，不能直接接受。

## 7. 月度收益差异

| Month | Raw | w3 preset_A | w3 preset_B | w5 zscore diagnostic | v2 main | v2 conservative | v2 risk_only |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2025-09 | -8.08% | -1.14% | -0.13% | +1.19% | -15.46% | -8.49% | -8.48% |
| 2025-10 | -1.78% | -2.74% | -1.35% | -0.25% | -6.06% | -4.27% | -1.86% |
| 2025-11 | -3.76% | -8.11% | -7.49% | +5.19% | -7.22% | -5.40% | -4.57% |
| 2025-12 | +7.42% | +8.18% | +5.88% | +8.23% | +5.15% | +5.52% | +6.29% |
| 2026-01 | -13.14% | -9.81% | -9.81% | -8.34% | -9.88% | -16.25% | -14.30% |
| 2026-02 | +6.09% | +3.21% | +4.01% | +1.07% | +9.03% | +6.11% | +8.13% |

解释：

- v2 main 的主要亏损来自 2025-09，单月 -15.46%，比 Raw 多亏约 7.38pct。
- v2 conservative 虽然在 2025-09 好于 v2 main，但 2026-01 出现 -16.25%，显著弱于 Raw 与 w3。
- v2 risk_only 在 2026-02 反弹较强，但 2025-09 和 2026-01 未能控制回撤。
- w5 zscore diagnostic 在 2025-09、2025-11、2025-12 更强，是后续 rolling/PIT 复验重点。

## 8. 股票贡献诊断

脚本已在 `.codex_tmp/hmm_horizon_v2_backtest_20260428.json` 的 `contributions` 字段中保存每个版本 top/bottom 贡献摘要。贡献按每期 Top50 等权 `weight * fwd_5d_return` 汇总。

| Version | Top contributors | Bottom contributors |
|---|---|---|
| Raw | `002969.SZ` +3.09%, `002931.SZ` +2.49%, `001331.SZ` +2.19% | `001209.SZ` -1.05%, `688205.SH` -1.05%, `002682.SZ` -0.80% |
| w3 preset_B | `920576.BJ` +2.53%, `000609.SZ` +1.89%, `603301.SH` +1.77% | `601116.SH` -1.19%, `688205.SH` -1.19%, `001209.SZ` -1.05% |
| w5 zscore diagnostic | `002969.SZ` +4.31%, `002931.SZ` +2.39%, `603778.SH` +2.27% | `601116.SH` -1.19%, `002682.SZ` -0.90%, `920223.BJ` -0.81% |
| v2 main | `002931.SZ` +2.39%, `001331.SZ` +2.19%, `002149.SZ` +1.61% | `601116.SH` -1.19%, `688205.SH` -1.19%, `603083.SH` -0.84% |
| v2 conservative | `002969.SZ` +3.09%, `002931.SZ` +2.49%, `001331.SZ` +2.19% | `601116.SH` -1.19%, `688205.SH` -1.19%, `002682.SZ` -0.80% |
| v2 risk_only | `002969.SZ` +3.09%, `002931.SZ` +2.49%, `001331.SZ` +2.19% | `001209.SZ` -1.05%, `688205.SH` -1.05%, `000632.SZ` -1.01% |

解释：

- v2 并没有稳定保留 Raw 中最强贡献股票，尤其 v2 main 的 top contributor 缺少 Raw 中贡献最高的 `002969.SZ`。
- v2 conservative / risk_only 更接近 Raw，因此 top contributors 与 Raw 更相似，但仍未改善整体收益，说明少量替换也没有正收益。
- w5 zscore diagnostic 的收益主要来自继续抓住 `002969.SZ`、`002931.SZ`、`603778.SH` 等正贡献股票，但该结论需要 PIT rolling 重跑。

## 9. 资金使用率、买入未成交率、最终持仓数

| Version group | Capital utilization proxy | Buy-unfilled proxy | Final holdings count | 说明 |
|---|---:|---:|---:|---|
| All script versions | 100% | 0% | 50 | Top50 close-to-close 等权脚本假设满仓，未模拟真实订单成交 |

重要限制：

- 这次按用户要求没有启动 QE 实验，也没有调用 V25 minute execution。
- 因此资金使用率和买入未成交率只是脚本 proxy，不是实盘/分钟级撮合意义上的指标。
- 真正验收 HMM 能否上线，仍必须在后续 QE/Paper 级别加入 suspend/limit/minute fill 诊断；但本任务明确要求不使用 QE 实验，所以这里只报告脚本可观测的 proxy。

## 10. 为什么 Horizon v2 没有变好

1. **sector-level 状态与 stock-level 选股收益之间仍有断层**：v2 在行业状态上按 5D/10D/20D 校准正确，但最终组合收益由 Top50 股票替换决定；行业系数不能保证替换股票优于 raw-only 股票。
2. **乘法 overlay 对 rank 边界股票很敏感**：`raw_score * sector_coeff` 即使只有 1%~3% 系数差，也可能把边界股票大批换入/换出。
3. **状态标签仍然不稳定**：validation 最优标签是 `fading`，说明 HMM 隐状态的语义不自然稳定；即使按 snapshot 校准，也可能跨窗口漂移。
4. **长周期收益目标需要与 QE 因子权重共同训练**：本次 raw score 是 trailing return proxy，不是未来 5D/10D/20D rankIC 高权重的正式 QE 因子组合。
5. **校准目标是 label utility，不是组合替换 utility**：下一版应直接优化 HMM-only 替换的 forward return，而不是只优化状态平均收益。

## 11. 后续优化方向

### 11.1 不建议上线 v2 main/conservative/risk_only

- 新 v2 保留为对比资产，未来 QE/Paper 可继续作为候选版本。
- 不应替换当前默认 HMM，也不应把 v2 main 作为 preset_A 的继任。

### 11.2 下一版 HMM 训练建议

1. **以 w3 raw same-params 为下一轮基础，而不是直接沿用 w5/zscore**
   w3 preset_B 当前是最好的 PIT-compatible 脚本候选；w5 zscore 要先做 rolling/PIT 复验。

2. **把 HMM 从乘法 score 改为 rank overlay / risk filter**
   建议测试三类用法：
   - risk filter：只剔除最差状态行业，不奖励好状态；
   - additive rank：`adjusted_rank = raw_rank + alpha * hmm_rank_delta`；
   - replacement cap：限制 HMM 每期最多替换 5~10 只 Top50 股票。

3. **校准目标从 state utility 改为 replacement utility**
   每个 snapshot 必须输出：HMM-only vs raw-only 的 5D/10D/20D 差值；只有 HMM-only 显著优于 raw-only 时才允许提高替换强度。

4. **按 5D/10D/20D rolling PIT 快照训练**
   至少每月一个 snapshot，训练截止日必须早于验证起点；不能再接受 train/validation 覆盖回测窗口的候选作为正式结论。

5. **与未来 QE 因子权重联动**
   当 QE 模型训练重心转向 5D/10D/20D rankIC 因子后，HMM 训练也应读取相同 holding-horizon 权重，并把 HMM 作为组合风险/状态层，而不是独立 alpha 层。

6. **验收表必须固定输出**
   后续每次 HMM 训练/回测报告必须同时包含：
   - Raw vs HMM Top50 替换数和替换股票；
   - HMM-only / raw-only 的 5D/10D/20D forward return；
   - 月度收益差异；
   - 股票贡献 top/bottom；
   - 资金使用率、买入未成交率、最终持仓数；
   - PIT/diagnostic-only 标记。

## 12. 本次执行记录

- 已完成优化方案文档：`docs/analysis/hmm_horizon_optimization_training_plan_20260428.md`
- 已完成新训练脚本：`scripts/hmm_horizon_v2_train.py`
- 已完成新对比脚本：`scripts/hmm_horizon_v2_compare.py`
- 已在 WSL `rdagent-gpu` 训练新 HMM v2。
- 已在 WSL `rdagent-gpu` 执行 6 个月脚本回测。
- 已生成新模型、三套系数、JSON/CSV/MD 回测结果。
- 未覆盖旧模型、旧系数或旧训练脚本。
- 未启动 QE 实验。

## 13. 最终建议

当前可用排序：

1. **正式 PIT 候选优先级最高**：`HMM_COVFIX_w3_raw_same_params::preset_B`
2. **保留观察但不能正式接受**：`HMM_COVFIX_w5_zscore::preset_A`，必须 rolling/PIT 重训后再判断
3. **仅作为失败样本与后续研究资产保留**：`HMM_HORIZON_V2_*` 三个 preset
4. **Raw/no-HMM 仍是必要基准**：任何 HMM 版本必须稳定超过 Raw，且 HMM-only 替换必须优于 raw-only，才进入 QE/Paper 验证

本次最重要的结论是：**HMM 的优化方向不能只停留在“按 5D/10D/20D 校准 state coefficient”，而要升级为“按真实 Top50 替换收益校准 HMM 对组合的干预强度”。**
