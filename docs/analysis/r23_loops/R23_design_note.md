# R23 实验设计记录 — 基石整合 + 可部署级锁定（收敛轮）

> 日期 2026-06-17 · 上游 R22(GPU 8301 + CPU 12cb 全完成) + 全历史复盘
> GPU task `qe_20260617_110835_5b1b` (wsl2-5080, parallel_2, 12 loop)
> CPU task `qe_20260617_111027_73ac` (rdagent-node1, parallel_4, 12 loop)

## 1. R22 结果（驱动 R23）

| 腿×模型 | CAGR | CV | Sharpe | MDD | Turn | 判定 |
|---|---|---|---|---|---|---|
| α5 MARG10×LSTM | 0.792 | 0.056 | 2.18 | -0.160 | 13.9 | 锁定确认 |
| α4 VOL12×**TCN** | 0.798 | **0.040** | 2.20 | **-0.148** | **13.0** | ✅ TCN救活α4(最浅回撤/最低换手) |
| α6 FundVal×TCN | 0.851 | 0.101 | 2.17 | -0.170 | 15.1 | ⭐ICIR0.88最高 |
| α7 Flow×TCN | 0.900 | 0.100 | 2.27 | **-0.189** | 14.3 | 高收益但回撤最深 |
| α5 MARG10×LGBM_C | 0.703 | 0.058 | 1.96 | -0.159 | 13.1 | 双模型确认 |
| α4 VOL12×CatBoost | **0.361** | 0.088 | 1.28 | -0.183 | **17.5** | ❌树侧失败 |
| α6 FundVal×golden | 0.749 | 0.147 | 2.06 | -0.160 | 15.3 | ✅树侧可用 |
| α3 FM12+×golden | 0.690 | **0.023** | 2.03 | -0.152 | 13.3 | ✅极稳 |

**核心结论**: ① 波动率(α4)是**序列模型专属信号**——TCN 0.80 / LSTM 0.77 强, CatBoost 0.36 / conservative 0.53 崩;
② α5 双模型×各6种子已彻底锁定; ③ α7 三模型(LSTM/conservative/TCN)MDD 全 -0.18~-0.19, 深回撤是**信号本性**, 加模型无解。

## 2. 全历史腿×模型覆盖图（R14→R22）

| Alpha | 信号域 | 已验证模型(CAGR) | 状态 |
|---|---|---|---|
| **α1 PLUS** | 量价/换手综合(23/26f) | LSTM@tk25(23f≈1.08 / 26f≈1.12, leaderboard首位) | ⚠️仅LSTM, **从未上树** |
| **α3 FM12+** | 基本面动量(24f) | LSTM(0.86变动) + golden(0.69稳) | 2模型 |
| **α4 VOL12** | 波动率(12f) | LSTM(0.77) + TCN(0.80, n=3) | 序列专属, 待锁n=6 |
| **α5 MARG10** | 融资融券情绪(10f) | LSTM×6seed(0.77) + LGBM_C×6seed(0.73) | ✅✅锁定→出策略包 |
| **α6 FundVal** | 估值(12f) | conservative(0.71)+TCN(0.85)+golden(0.75) | 3模型, 待锁n=6 |
| **α7 Flow** | 微观资金流(12f) | LSTM(0.76)+conservative(0.83)+TCN(0.90) | 3模型, MDD深(信号本性) |

**最高价值缺口 = α1 基石**: leaderboard 最强腿, 但只 LSTM、从未上树, 无法做模型集成(其余每腿都有树伴侣)。

## 3. R23 布局（收敛轮, 因子集数仓复核零漂移）

### GPU 线 (cuda, parallel_2) — task 5b1b
| Loop | group | 因子集 | 模型 | seeds | 目的 |
|---|---|---|---|---|---|
| 1-3 | a1_plus26_lstm | PLUS26(26f) | LSTM | 333,555,777 | α1锚扩种子(R14已888/2026, 验1.12稳健性) |
| 4-6 | a4_vol_tcn | VOL12(12f) | TCN | 888,7,88 | α4防御腿锁定 n=3→6 |
| 7-9 | a6_fundval_tcn | FundVal12(12f) | TCN | 888,7,88 | α6最优腿锁定 n=3→6 |
| 10-12 | a3_fm12_tcn | FM12+(24f) | TCN | 42,2024,2026 | α3找"稳且高"模型(LSTM变动/golden低) |

### CPU 线 (cpu, parallel_4) — task 73ac
| Loop | group | 因子集 | 模型 | seeds | 目的 |
|---|---|---|---|---|---|
| 1-3 | a1_plus3_lgbmc | PLUS3(23f) | LGBM_C | 999,111,333 | **α1首次上树**(测组合集成可行性) |
| 4-6 | a1_plus3_golden | PLUS3(23f) | golden | 42,2024,2026 | α1树变体2 |
| 7-9 | a3_fm12_conservative | FM12+(24f) | conservative | 999,111,333 | α3第二树(golden已) |
| 10-12 | a6_fundval_golden | FundVal12(12f) | golden | 888,7,88 | α6树侧锁定 n=3→6 |

## 4. 锁定配置（全 24 loop 一致）
`topk=25 / n_drop=2 / label_horizon=20 / weight=softmax / V25_1_SMALL_CAP /
filtered_pool_20260428 / risk_policy=st_pit(block_buy+force_exit) / no-HMM / 10M / commission=0.00025`
注: α1 历史在 topk20 跑过, R23 统一到 topk25 以与各腿可比(既复核又口径对齐)。

## 5. 种子去碰撞
- a1_plus26_lstm {333,555,777}: R14 1f70 已用 888/2026 → 新种子不碰。
- a1_plus3×LGBM_C/golden: α1 从未上树 → 任意种子。
- a4_vol_tcn / a6_fundval_tcn {888,7,88}: R22 用 42/2024/2026 → 不碰, 合并 n=6。
- a6_fundval_golden {888,7,88}: R22 golden 用 42/2024/2026 → 不碰, 合并 n=6。
- a3_fm12_tcn(全新) / a3_fm12_conservative(R22 golden, conservative全新): 不碰。

## 6. R23 验收口径
1. **α1 整合**: PLUS26×LSTM 3新种子 CAGR 均值仍 ≥1.0 → 锚稳健; ×LGBM_C/golden 任一 CAGR ≥0.7 且与 LSTM 预测值相关<0.9 → α1 可模型集成。
2. **α4 锁定**: VOL12×TCN 6种子 CAGR 均值 ≥0.75 且 CV<0.10 → 钉为防御腿(低回撤低换手), 出策略包。
3. **α6 锁定**: TCN 6种子 + golden 6种子均稳 → α6 双模型集成定稿。
4. **α3 模型搜索**: FM12+×TCN 若 CAGR≥0.85 且 CV<0.10 → 取代 LSTM/golden 作 α3 主模型。
5. 跑完拉两个 loop_comparison, 更新全腿×模型矩阵。

## 7. R23 后的下一步（非实验）
单腿信号发现已基本完成(6腿×信号域全覆盖, 各≥2模型)。**瓶颈转入组合层**:
- **正交性矩阵**: 6腿预测值相关 + 持仓 Jaccard（需 P2 预测存储落地, Codex 进行中）。
- **多Alpha离线组合**(P3): 等正交矩阵 → 加权/风险平价组合, 目标 Sharpe>2.8。
- **α7 降险**: 不靠加模型, 靠组合层低相关腿对冲 / 风控 overlay。
- α5 已锁定 → 先出策略包(advisory-first)。

## 8. 复现
`python gen_r23_loops.py` → `r23_gpu_loops.json` + `r23_cpu_loops.json`(各12), 经 MCP
`create_pending`(node/parallelism 见上) + `run_confirmed(QE_CUSTOM_EVO_RUN)`。
