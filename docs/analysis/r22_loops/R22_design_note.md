# R22 实验设计记录 — α5锁定 / α4去留决策 / 第二集成模型

> 日期 2026-06-17 · 上游 R21(b6af GPU + f858 CPU, 各12 loop 全完成) · 节点/并行度沿用
> GPU task `qe_20260617_031007_8301` (wsl2-5080, parallel_2, 12 loop)
> CPU task `qe_20260617_031151_12cb` (rdagent-node1, parallel_4, 12 loop)

## 1. R21 结论(驱动 R22)

| Alpha | 因子集 | 模型 | R21 表现 | 判定 |
|---|---|---|---|---|
| **α5 MARG10** (融资融券情绪) | 10f | LSTM | CAGR≈0.75, **CV<1%**(种子极稳) | ⭐ 新星 → 锁定+扩种子 |
| α5 MARG10 | 10f | LGBM_C | CAGR≈0.79, IC≈0.08 | ⭐ 树侧同样强 → 锁定+扩种子 |
| **α4 VOL12** (波动率域) | 12f | LSTM | CAGR≈0.77 | 边际 → 换模型抢救 |
| α4 VOL12 | 12f | LGBM | CAGR≈0.53(弱) | 树侧弱 → 换 CatBoost 再判 |
| α6 FundVal12 | 12f | LSTM | 前轮 ICIR0.81/CV2.7% 鲁棒冠军 | 补第二模型(TCN/golden) |
| α7 Flow12 (微观资金流) | 12f | LSTM | 独立信号源(补为α6/α7) | 补第二模型(TCN) |
| α3 FM12+ | 24f | LSTM | FM12+×LSTM=86.5%(#3历史) | 补树侧 golden 第二模型 |

**核心动机**: 多Alpha组合需要每条腿(a)信号域正交 (b)有≥2个模型背书(集成降方差)。R21
确立 α5 是新的高夏普低换手腿;R22 要 (1)把 α5 钉死(双模型×各6种子) (2)给 α4 最后一次
机会换模型 (3)给 α6/α7/α3 补上第二个模型族,为后续跨Alpha集成与组合回测铺路。

## 2. R22 布局(因子集与 R21 零漂移)

### GPU 线 (cuda, 序列内 parallel_2) — task 8301
| Loop | group | 因子集 | 模型 | seeds | 目的 |
|---|---|---|---|---|---|
| 1-3 | a5_marg_lstm | MARG10(10f) | `__seed_LSTM_10D_hs64_d02__` | 888,7,88 | α5 LSTM 扩种子(并R21的42/2024/2026→共6) |
| 4-6 | a4_vol_tcn | VOL12(12f) | `__seed_TCN_10D_d02__` | 42,2024,2026 | α4 换 TCN 抢救(序列模型第二族) |
| 7-9 | a6_fundval_tcn | FundVal12(12f) | `__seed_TCN_10D_d02__` | 42,2024,2026 | α6 第二模型(TCN vs LSTM) |
| 10-12 | a7_flow_tcn | Flow12(12f) | `__seed_TCN_10D_d02__` | 42,2024,2026 | α7 第二模型(TCN) |

### CPU 线 (cpu, parallel_4) — task 12cb
| Loop | group | 因子集 | 模型 | seeds | 目的 |
|---|---|---|---|---|---|
| 1-3 | a5_marg_lgbmc | MARG10(10f) | `__seed_LGBModel_conservative_v1__` | 999,111,333 | α5 LGBM_C 扩种子(并R21的42/2024/2026/12345/888/7/88) |
| 4-6 | a4_vol_catboost | VOL12(12f) | `__seed_CatBoost_10D__` | 42,2024,2026 | α4 换 CatBoost 抢救(树侧第二族) |
| 7-9 | a6_fundval_golden | FundVal12(12f) | `__seed_LGBModel_golden_v1__` | 42,2024,2026 | α6 树侧第二模型 |
| 10-12 | a3_fm12_golden | FM12+(24f) | `__seed_LGBModel_golden_v1__` | 42,2024,2026 | α3 树侧第二模型 |

## 3. 锁定配置(全 24 loop 一致)
`topk=25 / n_drop=2 / label_horizon=20 / weight=softmax / V25_1_SMALL_CAP /
filtered_pool_20260428 / risk_policy=st_pit(block_buy+force_exit, score_overlay off) /
no-HMM / initial_cash=10M / commission=0.00025 / max_weight=0.05 / TAIL_SUBSTITUTE(depth15)`

## 4. 种子去碰撞核查
- MARG10×LSTM: R21 用 {42,2024,2026};R22 新增 {888,7,88} — 无碰撞,合并后 6 种子。
- MARG10×LGBM_C: R21 用 {42,2024,2026,12345,888,7,88};R22 新增 {999,111,333} — 无碰撞。
- α4 TCN/CatBoost、α6 TCN/golden、α7 TCN、α3 golden: 该(因子集×模型)从未跑过 → 全新,种子任意。

## 5. R22 验收口径
1. **α5 锁定确认**: 双模型各 6 种子 CAGR 均值 ≥0.70 且 CV<0.10 → 钉为生产腿,出策略包候选。
2. **α4 去留**: TCN 或 CatBoost 任一 CAGR 均值 ≥0.65 且与 α5/α6 持仓 Jaccard 低 → 保留为波动率腿;
   否则两模型族均弱 → α4 退役,波动率域改由 FM12+ 内含的波动率子因子覆盖。
3. **第二模型集成价值**: α6/α7/α3 的第二模型与首模型预测值相关性 <0.9 且各自 CAGR 不塌 →
   该腿可做种子+模型双重集成降方差。
4. 跑完拉两个 `loop_comparison` + 各腿 `loop_metrics`,更新 Phase0 正交矩阵(纳入 α4/α5)。

## 6. 复现
`python gen_r22_loops.py` → `r22_gpu_loops.json` + `r22_cpu_loops.json`(各12),再经 MCP
`create_pending`(node/parallelism 见上) + `run_confirmed(QE_CUSTOM_EVO_RUN)`。
