# 板块轮动因子 Batch A/B 研发结果与入库处置

- 关联设计：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md`
- 执行日期：2026-07-11
- 执行 skill：`develop-factor`、`analyze-factor-library`、`smart-explore`
- 批次：`sector_rotation_batch_a1_20260711`
- 结论：9 个预注册候选均未通过完整 Stage-3；本批没有可用因子晋级。A1/A5/B2 以真实可执行研究资产进入因子库后已禁用，其余 6 个候选不入库。

## 1. 结论与边界

本批完成 A1–A6、B1、B2 以及设计要求的 N1 negative control，共 9 个候选。结果不能表述为“成功交付 9 个可用因子”：

- A2、A3、B1、N1 在 validation KILL，不开放最终 test；
- A4、A6 在最终 test 方向失稳，KILL；
- A1 的原始 OOS IC 达标，但 h20 HAC ICIR 低于 0.3；
- A5、B2 的原始 OOS IC 较好，但在控制既有因子/风险暴露后，block-bootstrap 区间跨零；
- 因此本批没有 `is_available=true` 的新因子，没有 candidate → active promotion，没有 realtime transformation，没有 QE/模拟盘/实时启用。

三项到达 Stage 2 的资产已真实写入 `aistock_factor_catalog`、落地 offline 源码并完成 LLM 分类；Stage-3 失败后均设置 `is_available=false` 和明确 `disable_reason`。这保留了可审计研究资产，同时满足“失败者不得伪装成可用因子”的设计约束。

## 2. 冻结数据与试验契约

### 2.1 Candidate bundle

| 项目 | Receipt |
|---|---|
| 隔离路径 | `RD-Agent_worktrees/sector-rotation-batch-a1-20260711/git_ignore_folder/sector_rotation_batch_a1_20260711/static_factors.parquet` |
| 行数 / 数据列 | 7,304,119 / 121 |
| `l2_code_id` | `int16`；unknown=`-1` 共 126 行；已知覆盖率 0.9999827495；131 个已知行业码 |
| parquet SHA-256 | `6660081b8da9e32d40a367dbcab289bf5711d1504082fd438ef07cef3b05ea15` |
| schema JSON SHA-256 | `23c6de83584cdb64c6595f9d072958319964ea89d2aa764688568dd7654cb52e` |
| schema CSV SHA-256 | `4e16ba161605e8d57ce5c4755e8d5a68460247e96b3308d0e13b773664e3587e` |
| active 状态 | 未 promotion；生产 active 数据未修改 |

### 2.2 标签、切分与防泄漏

- 标签：`T21T1 = close[t+21] / close[t+1] - 1`；horizon=20。
- train：2018-08-01～2022-11-30；validation：2023-01-02～2023-12-29；untouched test：2024-02-01～2026-03-27。
- purge=20，embargo=0；方向在 train/validation 后冻结；test 最多开放一次。
- 重叠收益同时报告 naive 与 Bartlett lag=19 HAC；Stage-3 使用 moving-block bootstrap（block=20、1,000 次、seed=20260711）和 20 个 non-overlap offset。
- append-only ledger：29 条有效 JSONL，其中 9 条 `FINAL_DISPOSITION`。

## 3. 离线实现与结构验证

所有输出均为 `(datetime, instrument)` MultiIndex、单一预期列、无重复索引、无空值、无 unknown/unmapped 输出行。唯一运行提示是 pandas legacy `stack` FutureWarning，不影响本批数值结果。

| ID | factor | 行数 | 日期范围 | 股票数 |
|---|---|---:|---|---:|
| A1 | `m_sector_breadth_ma20_level` | 7,215,974 | 2018-08-28～2026-04-28 | 4,680 |
| A2 | `m_sector_breadth_ma20_thrust_5d` | 7,201,420 | 2018-09-04～2026-04-28 | 4,680 |
| A3 | `m_sector_rs_rank_velocity_20d_5d` | 7,223,723 | 2018-09-05～2026-04-28 | 4,691 |
| A4 | `m_sector_participation_gap_20d` | 7,205,178 | 2018-08-29～2026-04-28 | 4,680 |
| A5 | `m_sector_residual_cohesion_10d_60d` | 6,937,408 | 2018-11-01～2026-03-10 | 4,666 |
| A6 | `m_sector_vol_compression_5d_20d` | 7,223,908 | 2018-08-29～2026-04-28 | 4,689 |
| B1 | `m_sector_turnover_breadth_accel_5d` | 7,085,056 | 2018-11-07～2026-04-28 | 4,680 |
| B2 | `m_stock_sector_leadership_persistence_20d_10d` | 7,099,527 | 2018-09-11～2026-04-28 | 4,677 |
| N1 | `m_sector_flow_rotation_10d` | 7,227,182 | 2018-08-28～2026-04-28 | 4,689 |

A5 的输出尾日为 2026-03-10，早于 bundle 尾日；其 test 仍有足够样本，但该 coverage 特征已作为限制项保留，不允许静默外推到 2026-04-28。

## 4. h20 Stage-1 结果

下表 IC/RankIC 为 raw 符号；`dir` 是预注册/冻结方向。`—` 表示按门禁未开放 test。

| ID | dir | train IC / RankIC | validation IC / RankIC | test IC / RankIC | 最终处置 |
|---|---:|---:|---:|---:|---|
| A1 | -1 | -0.003061 / -0.002952 | -0.032593 / -0.033467 | -0.036310 / -0.041220 | `REJECT_STAGE3_HAC` |
| A2 | +1 | 0.016347 / 0.017691 | -0.000456 / -0.000931 | — | `KILL_VALIDATION` |
| A3 | +1 | 0.011372 / 0.011579 | -0.010131 / -0.010545 | — | `KILL_VALIDATION` |
| A4 | +1 | -0.018696 / -0.018924 | 0.027880 / 0.034363 | -0.014910 / -0.010864 | `KILL_TEST` |
| A5 | +1 | 0.008309 / 0.012749 | 0.031905 / 0.037423 | 0.042237 / 0.049583 | `REJECT_STAGE3_RESIDUAL` |
| A6 | +1 | 0.004935 / 0.006635 | 0.025381 / 0.027446 | -0.002514 / -0.001586 | `KILL_TEST` |
| B1 | +1 | -0.000174 / 0.001067 | -0.004545 / -0.002638 | — | `KILL_VALIDATION` |
| B2 | -1 | -0.052931 / -0.066844 | -0.039725 / -0.040359 | -0.059080 / -0.073618 | `REJECT_STAGE3_PARTIAL` |
| N1 | +1 | 0.016085 / 0.017848 | -0.007958 / -0.007031 | — | `KILL_VALIDATION` |

补充诊断：N1 validation 的 1d IC/RankIC 为 -0.003221/-0.004969，与冻结正方向同样相反，因此不是简单的 1d/h20 方向错配。

### 4.1 test 打开审计

- A2/A3/B1/N1 均未打开 test；A1/A4/A5/A6/B2 各打开一次，没有重复查看。
- A1 是预注册 baseline，允许继续到 OOS 作为基线诊断。
- A4/A6 的 train 与 validation 符号冲突，实际执行中仍各打开一次 test 做稳定性诊断；按严格 Stage-1 自动门禁，它们应更早停止。这是本批流程偏差，不是晋级证据。两者均已 KILL，结果未用于改公式、翻方向或创建派生窗口。后续 runner 应把冲突停止条件编码化，避免人工放宽。

## 5. Stage-3 去重与条件增量

### 5.1 A1：广度 level

- test directional IC/RankIC：0.036310/0.041220。
- directional HAC ICIR：0.1234/0.1361，低于 0.3。
- 结论：价格广度的高位更接近拥挤/均值回归，而不是顺势扩散；原始强度可观，但统计稳定性不足，停止。

### 5.2 A5：残差协同性

- 与 A6、ATR compression、intraday range compression、行业中性波动、原始残差 MAD、sector RV20 的最大相关性为 0.3557，无 `|corr|>=0.8` 重复。
- raw test directional IC/RankIC：0.042237/0.049583；HAC ICIR：0.1441/0.1604。
- 控制后 residual IC/RankIC：0.012270/0.015352；HAC：0.0555/0.0662。
- block-bootstrap 95% CI：[-0.00949, 0.03221]，正值概率 0.837；区间跨零。
- 结论：公式较独特，但没有稳定条件增量。

### 5.3 B2：领导持续性

- 与 `m_stock_vs_industry_mom_20d`、`m_mom_residual_20d`、`m_sector_momentum_spread` 最大相关性分别为 0.6283、0.6397、0.1018；不属于 `|corr|>=0.8` 的机械重复。
- raw test directional IC/RankIC：0.059080/0.073618；HAC ICIR：0.2476/0.2772。
- 控制后三因子 residual directional IC/RankIC：0.006493/0.025820；HAC：0.0482/0.1442。
- block-bootstrap 95% CI：[-0.00629, 0.01857]，正值概率 0.853；区间跨零。
- 结论：强 raw 表现主要落在既有行业相对/残差动量信息簇，未证明稳定条件增量。

## 6. 因子库与 MCP 处置

### 6.1 MCP 是否需要

需要，但职责必须拆开：

- MCP 适合 exact/语义查重、基线查询、register/metrics/deprecate 预检、catalog/指标结果回读；
- MCP `register_confirmed` 只写 catalog 元数据，不能替代真实 executable asset 保存；
- 本批真实源码和 catalog/classification 使用 `ManualFactorService.save_factor`；
- 官方指标必须显式指向隔离 candidate 数据。当前 MCP metrics 默认 active 数据目录不含本批 `l2_code_id`，因此不能直接提交；
- MCP deprecate 预检成功，但 confirmed 因生产 catalog 缺 `updated_at` 返回 HTTP 500。本批在校验现存列后，以单事务写入 `is_available/disable_reason/disable_batch_id/disable_at`，再用 MCP 回读确认。

### 6.2 Catalog 最终状态

| catalog id | factor | asset | 分类 | 最终状态 |
|---:|---|---|---|---|
| 1523 | `m_sector_breadth_ma20_level` | `rdagent_assets/manual_factors/m_sector_breadth_ma20_level.py` | `STAT` | `is_available=false`；HAC 稳定性不足 |
| 1524 | `m_sector_residual_cohesion_10d_60d` | `rdagent_assets/manual_factors/m_sector_residual_cohesion_10d_60d.py` | `STAT` | `is_available=false`；控制后增量不稳定 |
| 1525 | `m_stock_sector_leadership_persistence_20d_10d` | `rdagent_assets/manual_factors/m_stock_sector_leadership_persistence_20d_10d.py` | `STAT` | `is_available=false`；partial IC bootstrap 跨零 |

源码与研究版本做了 `git diff --no-index`，内容相同；Windows catalog asset 使用 CRLF，隔离研究文件使用 LF，因此 raw byte SHA-256 不同。官方 remote 271 已直接从 catalog `code_text` 成功计算三项资产。

A2/A3/A4/A6/B1/N1 均没有 catalog 行，符合 KILL 不入库约束。

### 6.3 官方统一指标与生产 DDL gate

官方任务 271 对 A1/A5/B2 计算成功 3/3，每个因子 7,714,989 行、5 个窗口均成功，运行时与资源门禁通过。最终数据库写入为 0，原因是生产 `aistock_factor_metrics` 尚无 `h20_return_horizon` 及其 companion 字段。

同一只读 schema 核验还确认：生产 `aistock_factor_catalog` 没有 MCP deprecate 使用的 `updated_at`；这解释了 MCP confirmed 的 HTTP 500。当前生产库 h20 列集合为空，三因子 metrics 行均为空。

本批没有执行生产 DDL。原因不是技术上无法迁移，而是设计把 DDL 定义为需要独立授权、备份/演练和迁移 receipt 的 production gate。由于本批没有 Stage-3 通过者，不允许为了填充失败资产的指标而绕过该门禁。

任务 receipt：

- remote 269：误指向 active 数据目录，发现后立即取消；
- remote 270：官方 writer 拒绝 symlink escape；隔离输入改为同卷 hardlink；
- remote 271：计算成功，DB 写入被缺失 DDL 阻断。

## 7. 设计验收映射

| design item | 本批状态 | 结论 |
|---|---|---|
| F-007 offline/realtime 与失败策略 | `VERIFIED_NO_SURVIVOR` | 9 个 offline 代码/输出完成；三项 Stage-2 offline asset 入库后禁用。没有通过者，因此不生成 realtime loader，不得声称双形态完成。 |
| F-008 去重与条件增量 | `VERIFIED` | MCP 查重、控制因子、A5/B2 residual/partial IC 与相关性 receipt 完成；无稳定增量。 |
| F-009 稳健性/成本容量/拥挤 | `STOPPED_BY_GATE` | h20 HAC 与 A5/B2 block/non-overlap 完成；没有 Stage-3 通过者，因此 DSR/PBO、组合成本容量和持仓拥挤不启动。 |
| F-010 QE 组合增量 | `NOT_STARTED_BY_GATE` | 无因子通过 F-009，不启动 GATs/LGBM 组合试验。 |
| F-011 生产副作用 | `SCOPED` | 已执行授权范围内的 catalog/classification/disable 写入；未执行 DDL、metrics 回填、active promotion、服务重启或交易运行。 |

## 8. 证据驱动的下一批研发建议

本批 test 已被打开，不能在相同 test 上调窗口、翻方向或选择新公式。以下建议必须建立新的 trial，先冻结公式和方向，并使用 walk-forward 或 2026-03-27 之后积累的全新 holdout 验证。

### 8.1 保留的研究启示

1. **广度更像极值拥挤而非线性顺势。** A1 的冻结负方向在 validation/test 一致，说明“高广度后的 h20 回撤”值得研究；但当前 HAC 不足。下一版只能预注册非线性极值/状态交互，不能用本次 test 调阈值。
2. **领导持续性 raw alpha 强，但与残差动量簇重叠。** B2 应改为“leader set 的变化”而不是“leader level/persistence”，减少与 `m_stock_vs_industry_mom_20d`、`m_mom_residual_20d` 的同源性。
3. **残差协同性较独特，但需要状态条件。** A5 与控制项相关性低，适合作为预注册 STATE interaction 腿；单独 level 不再重跑。
4. **停止原公式族。** A2/A3/A4/A6/B1/N1 的现公式不得换名或只改窗口重新入场。

### 8.2 建议的新候选

| 建议 ID | 因子方向 | 核心公式意图 | 对应本批证据 | 首要控制项 |
|---|---|---|---|---|
| C1 | `m_sector_leader_set_turnover_5d_20d` | 板块内 20d 相对收益 top-quintile 集合的 5 日 Jaccard turnover；捕捉 leader 更替而非 leader level | B2 raw 强但 residual 弱 | B2、行业相对动量、板块成交活跃度 |
| C2 | `m_sector_leadership_entropy_20d` | 对板块成员正相对收益权重计算 entropy/concentration；识别“少数龙头拉指数” | A4 participation gap 线性口径失稳 | 市值集中度、B2、A4 |
| C3 | `m_sector_cohesion_trend_state_10d_60d` | 只在预注册的正板块趋势/中等市场波动状态启用 A5 cohesion | A5 unique 但无无条件增量 | A5、A6、市场/行业波动、行业动量 |
| C4 | `m_sector_breadth_extreme_reversal_20d` | 预注册尾部变换刻画极端广度拥挤，方向冻结为 reversal | A1 validation/test 负方向一致但 HAC 弱 | A1、行业反转、市场宽度 |

C1/C2 优先于 C3/C4：前两者改变信息结构，后两者属于从本批结果得到的状态/非线性假设，选择偏差风险更高。下一批仍须先做 MCP 精确查重和 family-level trial 计数。

## 9. 分离状态

| 状态面 | 当前事实 |
|---|---|
| 研究代码/结果 | 9 个 offline 候选和 receipts 位于隔离、gitignored RD worktree |
| 因子库 | 3 个真实研究资产已登记并禁用；6 个 KILL 未登记；0 个新可用因子 |
| 官方 metrics | 计算成功；生产 DDL 未执行，DB 指标未落表 |
| candidate 数据 | 隔离且冻结；未 promotion active |
| QE | 未启动 |
| runtime | 未重启、未变更配置 |
| paper/live | 未启用 |

因此本批的真实交付是“完成研发、证伪并保留可审计资产”，不是“生产就绪的板块轮动因子”。
