# 板块轮动因子研发与演进蓝图：候选池、h20 基线、组合及长期趋势

- 文档类型：F2 因子研发规格 / Gate-0 开发指引（`develop-factor`）
- 主线：板块轮动（sector rotation）——让模型显式理解板块归属、轮动速度、成员参与度与板块内结构
- 初版日期：2026-07-10
- 当前版本：v4.6（F-014 决策门、可成交性桥接、两层 oracle 与 R8M 路线收敛，2026-07-15）
- 面向：Codex 因子研发 → Tier2/IC 审核 → QE 对照实验
- 关联：`develop-factor`、`analyze-factor-library`、#1939/#1940/#1941/#1943（`l2_code_id` 链路）、原 F1–F4 规格

---

## 1. 背景与已确认事实

策略目标是捕捉板块轮动 alpha：不仅识别“哪个板块在领涨”，还要识别轮动是否扩散到多数成员、成员是否协同、板块是否正在进入或退出领涨区，以及板块内哪些股票具备稳定的相对强度。

当前基础能力如下：

- GATs 关系模型已接入真实申万 L2 行业信息；模型侧可以显式利用板块归属。
- 导出侧已在 `sector_data.h5` 的 22 个 `sw2_*` 数值字段之外增加稳定的 `l2_code_id`。编码来自权威 `sw_index_classify` 映射，未知值为 `-1`，PIT 归属来自 `market.sw_index_member`。
- `sw2_*` 是“个股当日所属板块的指数聚合值”，按 PIT 归属展开到个股；`l2_code_id` 是离散分组键，不是连续特征。
- 方向 A 的签名 fallback 邻接偏置在实验 `qe_20260710_005329_4b05` 的指定配置中未观察到可辨识增量（off≈industry_bias，0.0930 vs 0.0927）。该结果不能外推否定所有邻接设计，但足以说明后续主线不再依赖字段签名猜测同业关系。
- 真实申万 L2 二值同业邻接已经由 `SwIndexMemberIndustryIdProvider` 基于 `market.sw_index_member` 的 PIT 归属完成 R4 对照，并非“尚未验证”。在 `qe_20260713_195926_11e3` 中，seed 7 的 off/industry-bias RankIC 为 `0.105816/0.095048`，seed 17 为 `0.103728/0.100237`；两颗种子均未改善 RankIC，组合收益表现混合。因此后续不重复同一种二值同业边实验，图模型增量研究转向动态权重、多关系或层次结构，同时保留 `l2_code_id` embedding 与显式板块因子主线。

本次修订同时纳入因子库 MCP 的去重与统一指标证据。关键结论是：原 F1–F4 不能作为四个全新的同优先级因子直接开发。

| 原编号 | 原设计 | 统一状态 | 当前证据与处置 |
|---|---|---|---|
| F1 | `m_sector_rs_rank_20d` 板块相对强度排名 | `BASELINE` | 与既有行业动量/行业反转族同源。对收益做 percentile rank 是单调变换，本身不产生正交性。保留为研究基线，不作为首批新增因子；新增研发改为“板块排名速度”。 |
| F2 | `m_sector_breadth_ma20` 板块内成员站上均线比例 | `BASELINE` | raw level 作为基线；当前英文泛化 MCP 搜索不足以证明无同族资产，Stage 0 仍须用精确名、中文描述、公式和相关簇查重。A2 breadth thrust 作为待证伪的 `NEW` 主候选，而非已证明独有。 |
| F3 | `m_sector_flow_rotation_10d` 板块资金流加速 | `NEGATIVE_CONTROL` | 与现有 `m_sw2_net_vol_momentum` 等高度相邻；既有 out-sample 1d 证据弱。快筛不过不入库。 |
| F4 | `m_stock_sector_leadership_20d` 个股 20 日动量减板块动量 | `REUSE` | 经济公式意图已由 `m_stock_vs_industry_mom_20d` 覆盖，并与 `m_mom_residual_20d` 进入同一高相关簇；但 catalog 资产存在 PIT 口径缺陷，只有完成 F-006 repair source 同步与重算后才能正式复用。禁止换名重复入库；B2 只允许结构不同的 leadership persistence。 |

关键策略约束保持不变：

- 标签不做板块中性化；主标签保持与目标 QE 实验一致的裸 h20 前向收益。
- 因子内部可以使用行业相对值、残差、板块内排名等结构，但不能把“因子使用相对值”和“标签板块中性化”混为一谈。
- 正交性和模型增量价值优先于单因子绝对 IC；不得为了扩充数量重复注册同公式、反向或单调变换因子。

## 2. Scope / 范围

### 2.1 目标

1. 建立一个可扩展的板块轮动候选池，不把交付数量固定为四个。
2. 首批开发 5–10 个口径明确、数据依赖可控的候选，通过 h20 快筛、统一指标、双层相关性和模型消融逐级淘汰。
3. 同时覆盖四类互补信息：
   - 板块间状态：强度、排名速度、波动压缩；
   - 板块参与度：价格广度、换手广度；
   - 板块内部结构：等权参与、残差协同性、领导持续性；
   - 负对照：已知低成功率的板块资金流加速。
4. 通过 G12、显式板块因子和 `l2` embedding 的受控消融，确认增量来自哪里。
5. 通过的因子与 RDAgent/QE/Qlib、因子库和未来实时加载链保持同公式、同 PIT、同编码语义。
6. 在完成当前因子/embedding 归因后，按“低成本组合验证 → 长周期标签/模型 → 两层板块选股 → 最多三腿组合 → PIT 关系模型 → 概念多关系图”的顺序研究长期上涨趋势 alpha；每一级都必须有独立基线、冻结实验卡和停止条件。

候选在研发前和研发后使用统一状态，不用“计划开发”“已完成”“可用”混写：

| 状态 | 含义 |
|---|---|
| `NEW` | 公式已冻结，准备新开发。 |
| `BASELINE` | 只作比较基线，不默认新增可用因子。 |
| `REUSE` | 复用已有资产，只补缺失的 h20/相关性/模型证据。 |
| `NEGATIVE_CONTROL` | 负对照；未过快筛立即停止。 |
| `CONDITIONAL` | 只有上游数据或前一批证据通过后才开发。 |
| `PASS/MARGINAL/KILL/DUPLICATE` | 研发后的最终处置。 |

### 2.2 Non-goals / 非目标

- 不以“开发数量”替代质量门禁。
- 不重复创建现有行业动量、行业残差或其反向副本。
- 不把 `l2_code_id` 当连续数值直接输入因子公式。
- 不用最终 out-sample 结果选择符号、窗口或公式；这些选择必须在 train/validation 阶段冻结。
- 不在本规格中授权 candidate 数据向 active/production 的自动 promotion。
- 不因把方向写入蓝图而自动授权模型接入、概念数据采集、生产 DDL、QE 任务创建或运行时启用；这些仍需对应 feature/数据/实验流程和独立验收。

### 2.3 2026-07-11 Gate-0 本批执行边界

用户于 2026-07-11 明确批准按本方案启动“前置批次”。本批交付范围是：研究门禁与 F2 设计、candidate bundle 闭环、通用 h20 快筛、RD-Agent/AIstock h20 companion 指标契约，以及 F4/R2 tracked repair source 的 PIT 修复。候选 A1–A6/B1/B2/N1 的实际开发、offline/realtime 双资产生成、成本容量/拥挤回测、QE 消融、生产 DDL/回填、candidate → active promotion 与运行时启用均明确后置；这些后置项不是本批允许以简化版替代的缺口，而是下一阶段必须按 F-007–F-010 重新验收的独立工作。

### 2.4 v4 路线补全边界

第 2.3、11.1、Phase G0-A–G0-C、15–17 节保留 Gate-0 当时的交付与门禁证据，不把历史 receipt 改写成当前运行状态。v4 新增的第 4.10、9.4–9.9、11.6 与 Phase G0-E–G0-G 是 post-R6 研究方向。v4.2 进一步用 `docs/architecture/qe_long_trend_evaluation_f2_design_20260714.md` 冻结 F-014 的计算、制品、数仓、API/MCP/UI 和历史结果评价契约；v4.3 把该能力收紧为 QE-only：只由 QE task/Loop 显式触发，只读 QE dataset，只写 QE 专属 CAS namespace 与三张 additive evaluation 表，只在 `/quantevolver`、`/qe-archive` 和 QE MCP 展示。通用 Prediction Store、既有 Archive 通用表以及 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 的服务、表、缓存、调度和 UI 均不得修改。v4.4 在不改变上述隔离边界的前提下，将已完成的 R6/R7A 正式结果、R7B 与 R8 后续实验卡、当前因子库/数据状态写回原有路线章节，使本文同时承担设计蓝图与研究执行总账。v4.5 进一步写回 R7B 正式结果和 R8A/R8B 已启动的任务 receipt，并将 R8B 从“等待 R8A 结果后条件进入”校准为预注册 h40/h60 的并行 LSTM canary；该调整不允许依据中途结果改变期限、输入窗口、超参或种子。后续每个实验必须引用 F-013–F-017，并在创建任务前把数据快照、预测资产、种子、切分、资源类和并行策略写入实验卡。

v4.6 将模型讨论收敛为可执行门禁：R8A/R8B 原卡完成并完整归档，hN IC/RankIC 继续作为对应标签的有效预测尺和 fail-fast 证据，但不能替代长期趋势业务捕获尺；F-014 完整 QE-only 链路成为期限选择、R8B2、R8M 最终裁决和 R8C 的第一阻断门。F-014 内部允许分里程碑并行交付，但只有计算、可成交性桥接、CAS/状态/三表、API/MCP/UI、历史补算和真实 E2E 全部通过后才是 `F014_RESEARCH_DECISION_READY`。两层模型先做预注册四格 oracle 与 soft-gating 上界；多期限共享表示作为独立 R8M 假设，不预设正迁移；不同模型名或不同 horizon 不自动构成独立 Alpha 腿。

### 2.5 当前执行总账（截至 2026-07-15）

本表是阅读本文时判断“已完成/待执行/仅设计”的首要入口。历史 Gate-0 receipt 不因后续进度而删除，但当前状态以本表、对应实验 task/run 和第 15 节验收矩阵为准。

| 工作流 | 当前状态 | 权威证据 | 当前结论 / 下一门禁 |
|---|---|---|---|
| QE 数据快照与申万 L2 键 | `COMPLETED_FOR_RESEARCH` | `dataset_as_of=2026-06-30`；R6 共 30 个 Loop 成功完成；`l2_code_id` 被 GAT embedding 和板块因子实际消费 | 足以继续 QE 研究；不代表非 QE 交易运行时或未来概念 PIT 数据已经就绪。 |
| 三个板块候选入因子库 | `RESEARCH_AVAILABLE` | catalog `1525/1528/1532`；统一指标批次 `049b25d8-1893-4369-a820-925f0e6b78d8`；每因子 583 个相关性配对 | 可用于 QE；catalog 的 `asset_status/transformation_status` 仍为 `pending/PENDING`，`realtime_code_text` 为空，不得宣称荐股、模拟盘或生产实时可用。 |
| R6 LGBM 因子析因 | `COMPLETED` | `qe_20260714_104829_a9ca`，5 个因子集 × 3 seeds，15/15 Loop 完成 | `G14-FP` 是风险收益与信号强度较均衡的 h20 锚点；`G15-FPL` RankIC 更高但收益转换未同步提高。 |
| R6 GAT 因子析因 | `COMPLETED` | `qe_20260714_104830_0230`，5 个因子集 × 3 seeds，15/15 Loop 完成 | `G12 + l2 embedding` 保留关系模型对照价值；新增 F/P/L 在 GAT 上未形成稳定的普遍增量。 |
| R7A 两腿 `equal + rank` | `COMPLETED_NO_PROMOTION` | `macb_365aed6303e71d6e_20240701_20260629_20260714T174425343045Z` | 组合 Sharpe/Calmar 均低于 LGBM 基线；证明“低重合/正交”不等于成本后组合增益。 |
| R7B 两腿 `equal + zscore` | `COMPLETED_NO_PROMOTION` | `macb_365aed6303e71d6e_20240701_20260629_20260714T190901628242Z` | CAGR 67.95%、Sharpe 1.8313、Calmar 3.5429；较 R7A 略改善，但 Sharpe/Calmar 仍分别落后 LGBM 基线 0.1902/0.1200，停止当前 prediction-fusion 权重扩展。 |
| 30/40/60/120/180D 标签基础架构 | `IMPLEMENTED` | `ALLOWED_LABEL_HORIZONS`、`LongHorizonLabelMaturityPurge` 及对应测试 | 可训练长周期标签；标签期限不等于 LSTM 输入窗口或策略持仓期。 |
| F-014 长期趋势评价层 | `DESIGN_READY_AMENDED_CODE_PENDING` | `docs/architecture/qe_long_trend_evaluation_f2_design_20260714.md` | QE-only 设计已补齐可成交性桥接和完整决策门；evaluator、QE CAS/三表、API/MCP/UI、DDL 和实际 R6/R8 评价仍待独立实现与验收。 |
| F-014 研究决策门 | `BLOCKING_GATE_NOT_READY` | 第 9.6 节；F-018/F-020 | 纯计算核或本地 receipt 只能形成开发诊断；完整链路达到 `F014_RESEARCH_DECISION_READY` 前，不得据此选择 horizon、启动 R8B2、裁决 R8M 或组建 R8C。 |
| R8A 长周期 LGBM 对照 | `RUNNING_PRELIMINARY` | `qe_20260715_101942_d873`；12 个 CPU Loop；`rdagent-node1` 并行 4 | h30/h40/h60/h120 × 3 seeds 已启动；复用 h20 基线，任务完成与 F-014 评价层落地前不得选定长期期限。 |
| R8B LSTM 长周期对照 | `RUNNING_PRELIMINARY` | `qe_20260715_104922_001d`；6 个 GPU Loop；`wsl2-5080` 并行 2 | 预注册 h40/h60 × 3 seeds、`step_len=20`；与 R8A 同数据/因子/切分，仅改变模型族，禁止中途改设计。 |
| 两层板块 oracle 上界 | `DESIGN_READY_RUN_PENDING` | 第 9.5 节；F-018 | 先做 reality/oracle 四格与 soft gating，预注册阈值和置信区间；它是不可部署的未来信息上界，不是 Alpha 证据。 |
| 两层板块→个股模型 | `CONDITIONAL_NOT_STARTED` | 第 9.5 节 | 只有 oracle 证明存在可提取空间后才投入完整工程；板块层和个股层分别归因，通过后才竞争第三条组合腿。 |
| R8M 多期限共享表示 | `DESIGN_PLANNED_NOT_STARTED` | 第 9.6.3 节；F-019 | 独立实验比较独立训练、共享头、冻结迁移和全量微调；必须做 transfer matrix/LOO/梯度冲突，且最终仍由 F-014 裁决。 |
| HIST/动态多关系/概念超图 | `DEFERRED` / `BLOCKED_BY_PIT_DATASET` | 第 9.7–9.8 节 | HIST-industry 后置到低成本标签/组合验证之后；概念方向继续等待独立 PIT 数据集。 |

## 3. 证据口径与基线因子

因子库搜索摘要可能展示最新 `recent_1m` 记录，不能直接当作 out-sample 证据。本规格中的历史对比必须使用 `factor_library_get_metric_summary` 或明确指定 `eval_window=out_sample` 的统一指标，并同时记录 `snapshot_date`、`universe`、`return_horizon` 和 `calc_batch_id`。

首批研发前需要固定以下基线组：

| 作用 | 基线因子 | 用法 |
|---|---|---|
| 行业动量/反转基线 | `Industry_Momentum`、`SW2_MOM5`、`m_industry_reversal_20d` | 判断新板块级信号是否只是窗口或单调变换重复。 |
| 行业相对估值基线 | `m_ind_pb_rel_mom` | 检查相对价格/估值混叠及相关性红海。 |
| 个股相对行业基线 | `m_stock_vs_industry_mom_20d`、`m_mom_residual_20d`、`m_sector_momentum_spread` | 复用现有 F4 同族因子，不再换名复制。 |
| 板块资金流基线 | `m_sw2_net_vol_momentum`、`m_ind_flow_deviate`、`m_sector_mf_divergence_lg` | 作为 F3 低成功率方向的历史证据。 |

历史 1d 指标只用于定位重复、方向风险和 negative control，不得替代 h20 验收。QE archive 中“包含某因子的运行表现”也只能说明组合使用背景，不能当作该因子的因果贡献；最终贡献必须由受控消融证明。

2026-07-11 Gate-0 因子库 MCP 只读复核进一步确认：

| factor | eval_window | snapshot_date | universe | return_horizon | IC / RankIC | calc_batch_id | calculated_at |
|---|---|---|---|---|---|---|---|
| `m_stock_vs_industry_mom_20d` | out_sample | 2026-04-30 | `shsz_st_pit_active_v1` | 1d | -0.03802977 / -0.03668953 | `cf25429d-928c-4938-88ee-96514e65d214` | 2026-06-20T05:00:57.811464+08:00 |
| `m_mom_residual_20d` | out_sample | 2026-04-30 | `shsz_st_pit_active_v1` | 1d | -0.03886011 / -0.03851000 | `cf25429d-928c-4938-88ee-96514e65d214` | 2026-06-20T04:52:25.170443+08:00 |

查询 receipt：2026-07-11 调用 `factor_library_get`、`factor_library_get_metric_summary` 与 `factor_corr_get_clusters(min_abs_corr=0.8)`；相关性快照在 catalog 中记录为 2026-06-20。上表只用于查重和发现旧口径问题，不是 h20 验收。

- `m_stock_vs_industry_mom_20d`（manual，id=1247）仍为 `is_available=true` 但 `asset_status=pending`；其 catalog `realtime_code_text` 沿 instrument 对 `sw2_close` 做 20 日 `pct_change`，与第 4.1 节 PIT 契约冲突，因此 transformation `SUCCESS` 不能视为口径正确。
- 该因子与 `m_mom_residual_20d` 的官方指标仍只有 `return_horizon=1d`。out-sample 1d IC/RankIC 分别约为 `-0.03803/-0.03669` 与 `-0.03886/-0.03851`，形态和方向高度接近；修复 PIT 口径并重算 h20 前，不得引用这些历史值为 PASS 证据。
- `min_abs_corr=0.8` 的相关簇把 `m_stock_vs_industry_mom_20d`、`m_mom_residual_20d`、`m_ind_pb_rel_mom` 归入同一簇；`Industry_Momentum` 与 `SW2_MOM5` 也在同一高相关簇。该证据支持 F4 `REUSE`、B2 条件增量以及 A3 相对 R1 去重，不支持换名新增。
- 英文泛化搜索 `sector breadth`/`industry momentum` 返回 0 条不能解释为“因子库不存在同族因子”；Stage 0 必须继续用精确名称、中文描述、公式线索和相关簇联合检索。

### 3.1 2026-06-30 快照的正式因子库证据

下表来自同一官方指标批次 `049b25d8-1893-4369-a820-925f0e6b78d8`，股票池为 `shsz_st_pit_active_v1`，`snapshot_date=2026-06-30`，h20 契约为 `T21T1`。数值是 catalog 当前可执行因子值的独立指标，不是 R6 模型归因；正负方向不得在看到 test 后临时翻转。

| 因子 | catalog id | out-sample h20 IC / RankIC | recent 6m h20 IC / RankIC | HAC ICIR（out-sample） | 相关性状态 |
|---|---:|---:|---:|---:|---|
| `m_sector_flow_price_divergence_10d_20d`（F） | 1532 | +0.01311 / +0.01217 | -0.00774 / -0.01294 | +0.05469 | 583 pairs，2026-07-12 完成 |
| `m_sector_breadth_persistence_10d_20d`（P） | 1528 | -0.00784 / -0.00976 | +0.07353 / +0.06024 | -0.02201 | 583 pairs，2026-07-12 完成 |
| `m_stock_sector_leadership_persistence_20d_10d`（L） | 1525 | -0.04285 / -0.06153 | +0.01420 / -0.01239 | -0.17459 | 583 pairs，2026-07-12 完成 |

三项均为 `is_available=true`，其含义是“研究可选择”，不是“生产实时资产完整”。截至本次复核，三项的 `asset_status=pending`、`transformation_status=PENDING` 且 `realtime_code_text` 为空；R6 成功证明 QE 离线资产可运行，不证明荐股/模拟盘/选股加载链可用。P 因子存在全 OOS 与 recent 6m 方向差异，L 因子独立 h20 为显著负向；它们只能通过预注册方向、非线性交互和组合消融解释，禁止用单次近期窗口覆写全期处置。

## 4. Architecture / 架构与统一设计原则

### 4.1 先构造板块面板，再做时序运算

F1/F3/F4 原口径中“先沿股票计算 `sw2_*` rolling/pct_change，再去重到板块”的顺序必须废止。股票发生行业变更时，该写法会把两个行业指数接在同一股票窗口中，产生跨行业伪收益。

所有板块级 `sw2_*` 计算统一采用：

1. 从当日股票记录中取 `l2_code_id` 与目标字段；过滤 `l2_code_id == -1`。
2. 按 `(datetime, l2_code_id)` 构造每日一个板块值；先验证每个目标 `sw2_*` 字段的 `nunique(dropna=True) <= 1`，冲突时 loud fail，通过后才允许取 `first`。
3. 在 `(datetime, l2_code_id)` 板块面板上按 `l2_code_id` 做 `shift`、`rolling`、`Slope` 等时序计算。
4. 当日跨板块排名时，每个板块只占一个样本。
5. 按 `(datetime, l2_code_id)` 映射回个股 MultiIndex。

所有收益计算必须显式使用 `pct_change(fill_method=None)`；板块缺日、股票停牌或断点不得通过默认前向填充伪造收益。每天还必须记录有效板块数、unknown 数量、成员覆盖率和小样本板块占比。

成员聚合类因子则先在个股时序上计算成员状态，再按当日 PIT `l2_code_id` 聚合；不得用当前成分回填历史。

### 4.2 `l2_code_id` 语义与失败策略

- `l2_code` 是权威申万 L2 行业代码；`l2_code_id` 是稳定映射后的整数类别键。两者不能交替当作同一种物理字段使用。
- `l2_code_id` 只作为离散分组键。
- `-1` 必须在分组、排名和映射前排除；不得成为“未知板块”样本。
- parquet 路径若返回 float dtype，必须验证所有有限值均为整数语义后再显式转换；不得静默截断小数。
- 发现列缺失、非整数编码、板块字段同日不一致或覆盖率不足时，必须带 `reason_code` loud fail，不得空列、全 NaN 或 try-except 兜底。

### 4.3 PIT、标签与信息泄露

- 因子名中的 `5d/20d/60d` 表示特征回看或变化窗口；`h20` 表示预测标签的持有期限，两者不得混称。
- `full/out_sample/recent_6m/recent_3m/recent_1m` 是评估窗口；`1d/5d/10d/20d` 是收益期限画像，两套维度必须分别记录。
- 所有 rolling 只使用当日及历史数据；特征严禁 `shift(-N)`。
- h20 标签统一为 T+1 到 T+21 的裸前向收益：`close[t+21] / close[t+1] - 1`。标签构造可使用未来价格，但只能存在于评估器，不得进入因子代码。
- 因子开发、快筛、统一指标和 QE 对照实验必须使用相同股票池、交易时点、复权口径和冻结数据快照。
- 20 日标签高度重叠，ICIR 显著性必须使用 block bootstrap、Newey-West/HAC 或非重叠抽样复核。

### 4.4 双层评估与正交性

板块级因子映射回股票后，同一板块成员共享因子值，普通股票级 IC 会让成员数更多的板块获得更高权重。因此每个板块级候选必须同时报告：

1. 股票映射层：与模型实际输入一致的股票级 IC/RankIC 和相关性；
2. 板块原生层：按 `(datetime, l2_code_id)` 去重后的等权板块 IC/RankIC 和相关性；
3. 显著性：按时间 block 或板块 cluster 稳健的置信区间。

相关性 `< 0.8` 的门禁要在股票映射层和板块原生层同时满足。只在一个层面低相关不能宣称正交。

### 4.5 预注册与多重检验

- 每个候选在最终 out-sample 前冻结：公式、窗口、预期方向、缺失值规则和最小成员数。
- 不得在最终 out-sample 看到负 IC 后直接取负；若 train/validation 证明反向语义成立，应创建有清晰金融解释的版本，再进入 untouched test。
- 同族窗口变体必须作为一个 family 报告，保留 family-level 淘汰记录，避免从大量参数中择优造成数据挖掘偏差。

### 4.6 G0-01：试验台账、依赖检验与选择偏差

机构和论文证据只提供研究先验，不直接证明 A 股 alpha。Harvey、Liu、Zhu 指出因子海量检验下传统 `t > 2` 不足；2026 年更新进一步强调测试依赖、原假设分布和样本选择，并建议 local FDR。Bailey 与 López de Prado 的 Deflated Sharpe Ratio（DSR）则校正多次尝试、非正态和选择偏差。对应 AIstock 规则为：

- 每次公式、窗口、符号、阈值、种子、切分或数据快照组合都分配唯一 `trial_id`；validation 后的任何修改都算新试验。
- 台账最小字段冻结为：`trial_id`、`parent_trial_id`、`created_at_utc`、`candidate_id`、`family_id`、`formula_hash`、`code_hash`、`data_snapshot_sha256`、`label_contract`、train/validation/test 边界、`purge_days`、`embargo_days`、`expected_direction`、阈值、随机种子、状态与 disposition。实际运行台账随实验 artifact 保存为 JSONL append log，或 immutable partitioned Parquet dataset + manifest；不写入源码目录，也不得删除或覆写 KILL/ERROR 行。
- 相关候选按 family 计数：`{A1,A2,A4}`、`{A3,B2,R1,R2}`、`{A5,A6}`、`{B1,N1}`。N1 即使 KILL 也保留在试验台账。
- 至少报告候选总数、family 数、有效独立试验数估计和 HAC t 值；生成组合收益后再报告 DSR/PBO 或等价选择偏差诊断。
- `t >= 3` 与 local FDR 是统计治理参考，不能机械替换本规格的 h20 IC/RankIC 门槛。

### 4.7 G0-02/G0-03：purge、embargo 与重叠 h20 推断

- 固定 chronological train/validation/test；最终 test 只允许开启一次，禁止随机切分。
- 按标签区间精确 purge。对 `close[t+21] / close[t+1] - 1`，训练/验证边界至少移除会与后段标签重叠的 20 个信号日；若采用双向 CV/CPCV，再使用预注册 embargo。
- rolling 标准化、阈值和方向 `d` 只能由 train/validation 冻结。
- 普通 IC/RankIC 之外，必须报告 Newey-West long-run variance 调整的 ICIR，默认 `lag = h - 1 = 19`；同时用更长 lag、stationary/block bootstrap 或非重叠抽样做敏感性检查。

### 4.8 G0-04/G0-06：条件增量、信息扩散与 STATE 通道

行业动量可以解释相当部分个股动量；行业内 lead-lag 也可能来自共同信息的缓慢扩散。因此 rank、相对行业收益或 leadership 不能天然视为新 alpha：

- A3 必须控制 R1、`Industry_Momentum`、`SW2_MOM5` 和原始板块 20 日收益；B2 必须控制 R2、`m_stock_vs_industry_mom_20d` 和 `m_mom_residual_20d`。
- A2/A4 必须控制 A1 和原始板块动量。除相关系数外，报告 partial IC、残差 IC 或条件回归增量。
- A5/A6 是 `STATE`，不强迫具有固定单调方向。允许各自增加一个预注册的 `state × momentum_or_breadth` 模型交互腿，但交互不生成新的 catalog 原子因子，也不能在 test 后挑选。
- A5 必须区分 residual cohesion、原始成员离散度和普通低波，并检查高协同性是否表现为拥挤后的反转。

### 4.9 G0-05/G0-07/G0-08/G0-09：breadth、成本容量、拥挤与组合增量

- 外部 breadth 研究只能支持“成员参与值得检验”的先验，不能证明 A1/A2 在 A 股有效。A1 保持 level baseline，A2 保持唯一 thrust 主公式；advance/decline、自由流通加权等仅作为预注册 sensitivity。
- 所有候选都报告换手、实际费用、停牌/涨跌停可成交性、成交参与率和多资金规模 capacity curve。A2/A3/B1/N1 是高换手重点，A1/A5/A6 也不豁免。
- 去重不止检查平均因子值相关性，还检查 long-leg/目标持仓重合、同向换手和冲击重合、压力期相关性、尾部亏损与成本跳升。平均相关性低但尾部持仓高度重合时，标记为“不同公式、相同拥挤风险”。
- 最终采用标准是 GATs/LGBM 的 out-sample 组合增量，包括 `ΔIC`、净 Sharpe、回撤、换手、容量和多种子稳定性；单因子 IC 不能代替组合验证。

上述门禁分别参考多重检验、PBO/DSR、行业/因子动量、信息扩散、离散度、真实交易成本和机构拥挤模型的一手研究。完整引用见第 18 节；所有外部结论都必须在冻结的 A 股 candidate 数据上重新证伪。

### 4.10 post-R6 模型与组合研究层级

后续研究按信息增量和工程成本分层，不把“换模型”当成默认答案：

1. **组合层**：先复用已归档预测做 GATs + LGBM 的 prediction fusion 与 portfolio fusion，验证关系模型是否以正交性而非单腿 RankIC 创造价值。
2. **决策层**：再建立“板块评分 → 板块内选股”的两层基线，直接检验板块轮动与板块内 leadership 是否优于一次性全市场排序。
3. **关系层**：在真实 PIT 申万 L2 归属上研究 HIST-industry、动态加权图和多关系注意力。R4 已证伪的二值同业邻接不得换名重复。
4. **概念层**：只有概念成员 PIT 数据集通过独立数据门禁后，才研究 HIST-concept、HATS/多关系图或概念超图；同一股票同日属于多个概念是基础语义，不得强制压成单一类别。
5. **状态层**：MASTER、IGMTF、TRA 只作为关系/市场状态机制得到增量后的条件探针。TRA 若用于 Type B，只允许在长期趋势内部路由状态，不得把 Type A 超跌反弹和 Type B 长期趋势混入同一标签头。

所有关系输入统一服从以下契约：

- 行业或概念成员关系必须是 decision-as-of 可知的 PIT 关系；不得使用“当前成分静态快照回填历史”的简化版。`docs/analysis/p2_relational_model_hist_master_feasibility_20260708.md` 中允许首版静态 `stock2concept` 探路的旧建议由本条取代。
- 动态矩阵/稀疏边必须同时记录 `as_of_date`、relation type、source version/hash、有效起止区间和 instrument mapping hash；`stock_index`、Qlib instruments 与关系矩阵行序不一致时 loud fail。
- 动态权重只能使用当日 cutoff 前可知的滚动收益、残差相关、资金流、leadership 或板块状态；训练/验证/测试边界分别构图，不得用全样本相似度。
- 多关系图至少分离 `industry_membership`、`sector_state/leadership` 和未来 `concept_membership`，不得把不同经济含义的边先求和再声称可解释。
- 关系模型必须与相同因子、标签、切分、种子和训练预算的 LGBM/GATs 基线比较；新增架构的首个 loop 只作 composer、fit/predict、归档和资源 canary，不承担 alpha 晋级结论。

## 5. 代码与运行时契约

当前因子研发链存在两种代码形态，本规格明确要求双产物而不是混用：

### 5.1 离线研发 `code_text`

- 用于 WSL 执行、`result.h5` 生成、h20 quick screen 和统一指标。
- 只能读取明确注入到任务 workspace 的 candidate h5/parquet 数据，不得读取 active/production 的隐式默认路径。
- 输出必须是单列 DataFrame，索引为 `MultiIndex(datetime, instrument)`，列名等于因子名，末尾 `dropna()`。
- 代码只依赖 pandas/numpy/scipy；不得 import qlib、硬编码股票或日期、写入项目目录。

### 5.2 实时/QE `realtime_code_text`

- 函数签名固定：`def calculate_{factor_name}(instruments: list, start_date: str, end_date: str) -> pd.DataFrame:`。
- 行情只通过 `_REALTIME_LOADER`，静态字段只通过 `_STATIC_FACTORS_LOADER` 显式取列。
- 禁止文件 I/O、try-except 兜底、空值伪造、空 DataFrame 静默返回和 `$` 前缀列名。
- 输出索引名称继承 loader，禁止手写索引名称掩盖输入错误。

### 5.3 离线/实时一致性

同一因子的两种代码形态必须在冻结小窗口上完成 parity：

- 公共索引覆盖率一致；
- 非空值位置一致；
- 数值在声明容差内一致；
- `l2_code_id` 的 unknown、PIT 归属与板块映射一致。

因子 MCP 当前用于查库、指标、覆盖率、使用情况和相关性门禁；可执行源码保存仍使用 manual factor API/脚本。不得把只登记 catalog 元数据的 MCP register 当成可执行入库完成。

## 6. 候选因子池与研发批次

候选池允许扩展，但每批保持 5–10 个因子。新增方向必须先通过名称、公式和相关性去重；同族变体只有在前一版本给出明确信号后才进入下一批。

新增因子统一使用 `m_` 前缀并满足 `^[a-z][a-z0-9_]{2,80}$`；名称中的窗口后缀必须与唯一主公式一致，禁止同一名称承载可切换公式。

优先级 `A/B/C` 分别表示首批主要假设、次要/状态假设、基线或高重复风险假设；它不是 AIstock 的 P0/P1 风险等级，也不代表验收已通过。

### 6.1 Batch A：首批核心候选

| 编号 | 因子名 | 状态 | 类型 | 主数据源 | 最小历史 | 优先级 |
|---|---|---|---|---|---:|---|
| A1 | `m_sector_breadth_ma20_level` | `BASELINE` | 板块价格广度 level | close + `l2_code_id` | 20d | C |
| A2 | `m_sector_breadth_ma20_thrust_5d` | `NEW` | 板块价格广度扩散速度 | close + `l2_code_id` | 25d | A |
| A3 | `m_sector_rs_rank_velocity_20d_5d` | `NEW` | 板块排名进入速度 | `sw2_close` + `l2_code_id` | 25d | A |
| A4 | `m_sector_participation_gap_20d` | `NEW` | 典型成员与指数参与差 | close + `sw2_close` + `l2_code_id`；控制项 `db_circ_mv` | 20d | A |
| A5 | `m_sector_residual_cohesion_10d_60d` | `NEW` | 板块成员残差协同性 | close + `sw2_close` + `l2_code_id` | 60d | B |
| A6 | `m_sector_vol_compression_5d_20d` | `NEW` | 板块波动压缩状态 | `sw2_close` + `l2_code_id` | 20d | B |

#### A1 `m_sector_breadth_ma20_level`——价格广度 level 基线

- 个股时序：`ma20 = MA20(close)`；只在 `ma20.notna()` 时计算 `above_ma20[i,t] = 1(close[i,t] > ma20[i,t])`。不得先比较再直接 `.astype(float)`，否则无效 MA 会被误记为 0。
- 板块聚合：对当日有效成员取均值。
- 最小样本：有效成员数 `< 5` 或有效覆盖率 `< 0.8` 时该板块当日为 NaN。
- 输出：将板块 breadth 映射回当日成员。
- 方向：不预先锁死。高 breadth 可能表示趋势健康，也可能表示拥挤；作为 level 基线与 A2 比较。

#### A2 `m_sector_breadth_ma20_thrust_5d`——价格广度扩散速度

- 先计算 A1 的 `breadth20[s,t]`。
- 主公式：`thrust[s,t] = breadth20[s,t] - breadth20[s,t-5]`。
- 每日对有效板块做 percentile rank 后映射回成员。
- 预期方向：正；成员参与度正在扩散，比绝对 level 更贴近轮动形成，但仍可能在行情末端形成追涨信号。
- 变体门禁：只有主公式 MARGINAL/PASS 后，才允许另立 `m_sector_breadth_ma20_abnormal_60d = breadth20 - MA60(breadth20)`；不得在一个因子名下保留二选一公式。
- 研究门禁：advance/decline、自由流通市值加权 breadth 等只能作为预注册 sensitivity；A2 是本批唯一主 thrust 公式，sensitivity 不形成新的 catalog 候选，也不得在看到 test 后择优报告。

#### A3 `m_sector_rs_rank_velocity_20d_5d`——板块排名速度

研究附加门禁：除原始相关性外，必须相对 R1、`Industry_Momentum`、`SW2_MOM5` 和原始板块 20 日收益报告 partial/residual IC；控制后没有稳定 h20 增量则 `DUPLICATE/REUSE/KILL`。

- 在板块面板计算 `ret20[s,t] = sw2_close[s,t] / sw2_close[s,t-20] - 1`。
- 每日等权跨板块排名：`rank20[s,t] = CsRank(ret20[:,t])`。
- 主公式：`velocity[s,t] = rank20[s,t] - rank20[s,t-5]`。该值已经由两个截面分位之差归一化，主版本不再二次 rank。
- 预期方向：正；正在进入领涨区比“已经处于高位”更接近轮动速度。
- 相关性重点：与 `Industry_Momentum`、`SW2_MOM5`、`m_industry_reversal_20d` 同时检查，rank 变换不能被当作天然正交证明。

#### A4 `m_sector_participation_gap_20d`——成员参与差

研究附加门禁：必须控制 A1、原始板块动量、板块权重集中度、SIZE 与有效成员数；若 gap 仅重述少数权重股效应，不得 promotion。

`db_circ_mv` 只用于 SIZE/集中度诊断和条件回归，不进入 A4 主公式；若后续改用权威指数成分权重，必须作为新 trial 冻结数据源与时点，不得用事后当前权重回填历史。

- 个股 20 日收益：`stock_ret20[i,t]`。
- 当日按 PIT 成员聚合：`member_median20[s,t] = median_i(stock_ret20[i,t])`。
- 板块指数 20 日收益在板块面板上计算：`sector_ret20[s,t]`。
- 主公式：`gap[s,t] = member_median20[s,t] - sector_ret20[s,t]`，跨板块 rank 后映射回成员。
- 预期方向：正；中位成员也参与上涨，说明轮动不是少数权重股拉动。
- 风险：可能混入小盘风格，必须额外报告与 SIZE/市值因子的相关性。

#### A5 `m_sector_residual_cohesion_10d_60d`——成员残差协同性

研究附加门禁：同时与原始成员离散度、市场/板块波动和既有 VOL/low-vol 因子做条件比较；只允许一个预注册的 `state × momentum/breadth` 交互进入组合增量实验，该交互不作为 catalog 原子因子。

- 个股日收益 `stock_ret1[i,t]` 必须在单一 instrument 的连续价格序列上由 close 执行 `pct_change(fill_method=None)`；板块日收益 `sector_ret1[s,t]` 必须在 4.1 的板块面板上由 `sw2_close` 执行同一计算。两者都不使用预填充收益列。
- 日残差：`resid[i,t] = stock_ret1[i,t] - sector_ret1[s,t]`。
- 当日板块离散度：`mad[s,t] = median_i(abs(resid[i,t] - median_i(resid[i,t])))`。
- 主公式：`cohesion[s,t] = -log(MA10(mad[s,t]) / MA60(mad[s,t]))`；分母为 0 或样本不足时置 NaN，不使用任意 epsilon 掩盖异常。
- 每日跨板块 rank 后映射回成员。
- 经济含义：高值表示近期成员残差相对长期收敛。它是状态特征，本身不预设涨跌方向；方向由 train/validation 冻结。
- 风险：可能退化为板块低波风格，必须检查与波动率因子及 A6 的相关性。

#### A6 `m_sector_vol_compression_5d_20d`——板块波动压缩

研究附加门禁：必须与 A5、既有 VOL/low-vol 因子去重，并只使用预注册交互检验条件增量；不得因测试期某个交互较优而临时改变方向或公式。

- 在板块面板以 `pct_change(fill_method=None)` 计算行业日收益 `sector_ret1`。
- 冻结定义：`RVw[s,t] = rolling_std(sector_ret1[s], window=w, min_periods=w, ddof=1)`，其中 `w ∈ {5, 20}`；不得在实现时替换为 RMS、平方和或年化波动。
- 主公式：`compression[s,t] = -log(RV5[s,t] / RV20[s,t])`；任一窗口样本不足、`RV5 <= 0` 或 `RV20 <= 0` 时置 NaN。
- 每日跨板块 rank 后映射回成员。
- 方向：作为原子状态信号，不在因子内部预先乘动量；h20 方向由 train/validation 冻结。
- 研究假设：检验短长波动比在板块层是否提供区别于简单行业动量的 STATE 信息；该迁移尚未获得 A 股 h20 证据，必须允许无效或条件性结论。

### 6.2 Batch B：条件扩展候选

Batch B 只在 Batch A 完成快筛、相关性和失败归因后启动。

| 编号 | 因子名 | 状态 | 类型 | 最小历史 | 优先级 |
|---|---|---|---|---:|---|
| B1 | `m_sector_turnover_breadth_accel_5d` | `CONDITIONAL` | 自由流通换手异常广度 | 65d | B |
| B2 | `m_stock_sector_leadership_persistence_20d_10d` | `CONDITIONAL` | 板块内领导持续性 | 30d | C |

#### B1 `m_sector_turnover_breadth_accel_5d`——自由流通换手异常广度

研究附加门禁：属于高换手重点审计候选，必须在 A 股 T+1、停牌、涨跌停和实际费用约束下报告多资金规模/参与率的净结果与 capacity curve。

- 数据：`db_turnover_rate_f` + `l2_code_id`。
- 个股异常：`x = log1p(db_turnover_rate_f)`，`z60 = (x - MA60(x)) / STD60(x)`；只在 60 日均值/标准差有效且标准差大于 0 时计算 `hot[i,t] = 1(z60[i,t] > 1)`，否则保持 NaN。
- 板块参与率：`turn_breadth[s,t] = mean_i(hot[i,t])`。
- 主公式：`turn_breadth[s,t] - turn_breadth[s,t-5]`，跨板块 rank 后映射。
- 预期方向：正；关注度从少数个股向更多成员扩散。
- 风险：极端换手可能是出货；必须检查非线性和与换手率 Top 因子的相关性。

#### B2 `m_stock_sector_leadership_persistence_20d_10d`——板块内领导持续性

研究附加门禁：必须控制 R2、`m_stock_vs_industry_mom_20d` 和 `m_mom_residual_20d` 后报告 partial/residual IC；行业切换必须重置 persistence spell。

- 先按 membership-safe 板块面板得到 20 日板块收益，计算 `lead20 = stock_ret20 - sector_ret20`。
- 每日做板块内 percentile rank：`q20[i,t] = rank_within_sector(lead20[i,t])`。
- 主公式：`MA10(1(q20 >= 0.8))`，表示最近 10 个有效交易日持续位于板块前 20% 的比例。
- 10 日 rolling 必须按 instrument 的连续行业 spell 计算；`l2_code_id` 变化时重置，禁止把上一行业的领导状态带入新行业。
- 目的：识别持续龙头，而不是复制单一 20 日端点残差。
- 方向：不得沿用原 F4 的正向假设；在 train/validation 冻结后再进入 h20 test。
- 去重：与 `m_stock_vs_industry_mom_20d`、`m_mom_residual_20d`、`m_sector_momentum_spread` 任一层相关性 `>= 0.8` 即淘汰。

### 6.3 复用基线与 negative control

#### R1 原 F1 行业强度基线

不新增 `m_sector_rs_rank_20d`。优先复用现有 `Industry_Momentum`、`SW2_MOM5`、`m_industry_reversal_20d`，把 A3 的 rank velocity 与它们比较。只有确认现有因子缺少所需 20 日口径且 A3 无法替代时，才允许设计新的行业强度原子因子。

#### R2 原 F4 个股相对行业基线

不新增 `m_stock_sector_leadership_20d`。复用前必须读取现有可执行源码，审计其中所有 `sw2_*` 收益和 rolling 是否按 4.1 先构造板块面板；若不合规，既有相关指标视为失效，应修复原资产并重算，不得另起近义因子规避修复。审计通过后，再对 `m_stock_vs_industry_mom_20d`、`m_mom_residual_20d` 和可用反向版本做 h20 重评估；B2 是唯一允许继续研发的结构差异版本。

Gate-0 已修复 tracked regeneration source `scripts/p1_new_factors.py` 中的 F4/R2 offline 公式：先构造唯一 `(datetime,l2_code_id)` 面板、沿板块自身时序计算 20 日收益，再按当日 membership 映射回股票；unknown 不回退，板块日值冲突 fail-fast。因本批不写生产 DB，catalog 中既有 offline/realtime 资产与历史指标仍未替换；后续同步必须同时生成双代码形态、做 parity、重新计算 h20，并把旧 1d 指标标记为旧口径证据。

#### N1 `m_sector_flow_rotation_10d` negative control

- 板块面板上计算 `flow = sw2_mf_net_amt / sw2_amount`；`sw2_amount == 0` 时置 NaN。
- 加速：`MA10(flow) - MA10(flow).shift(10)`；每日跨板块 rank。
- 仅执行离线代码验证和 h20/1d quick screen。
- h20 未达到 PASS 时立即 KILL，不入 catalog、不跑全量、不派生窗口变体。
- 即使 PASS，也必须与现有板块资金流因子完成双层相关性后才能进入 Stage 2。

## 7. 数据前置与 train/serve parity

### 7.1 Gate-0 历史快照与当前 QE 数据状态

下表前三行保留 2026-07-11 Gate-0 的历史 receipt；最后一行是 2026-07-15 的当前 QE 研究状态。不得再把旧 2026-04-28 candidate 误读为 R6/R7 实际使用的数据。

| 数据位置 | `sector_data.h5` | `static_factors.parquet` | 结论 |
|---|---|---|---|
| active `factor_implementation_source_data` | 22 个 `sw2_*` 字段，无 `l2_code_id` | 122 列，无 `l2_code_id` | 不能用于 A1–A6/B1/B2/N1 的正式离线验证。 |
| candidate `factor_implementation_source_data_20260428_candidate` | 23 列，含 `l2_code_id` | 120 个数据列，无 `l2_code_id` | `sector_data.h5` 已满足；旧 bundle 缺离散行业键。 |
| Gate-0 隔离产物 `gate0_sector_factor_candidate_20260711` | 复用上述 23 列 candidate | 121 个数据列；旧 candidate 的 120 个数据列全部保留并新增 `l2_code_id=int16` | 已完成物理/schema/指纹验证；仍为 gitignored candidate，未 promotion。 |
| 当前 QE `factor_data`（2018-08-01 至 2026-06-30） | 23 列，含 `l2_code_id` | 123 个数据列，含 `l2_code_id=int16` | WSL 副本已被 R6/R7 与 R8B 实际读取，同身份远端副本正被 R8A 使用；满足当前 QE 研究，不外推为非 QE 运行时 readiness。 |

2026-07-11 Gate-0 实测审计：candidate `sector_data.h5` 共 7,334,829 行、1,876 个交易日、4,691 只股票，日期为 2018-08-01 至 2026-04-28，131 个已知板块，源表 `l2_code_id` 覆盖率 100%。旧生成器会把所有列统一转为 `float32` 且遗漏 `margin_detail.h5`，因此旧 candidate bundle 不具备离散类别键语义。修复后隔离生成产物为 7,304,119 行、4,691 只股票、1,876 日：7,303,993 行为已知板块，126 行显式为 unknown `-1`，known coverage 为 99.99827494595858%，取值范围 `[-1,133]`，共 131 个已知板块；旧 120 个数据列全部保留，共同字段 dtype 无变化，只新增 `l2_code_id=int16`。`static_factors.parquet` SHA-256 为 `FE91FA9C519F4FD501D5E979F03B604C66F3904387B48C0E982D8366747D60A6`；schema JSON/CSV SHA-256 分别为 `04252DD8E8941CDD8018885B1BBBE95F4C606FBAEE49C61BAB6E1986DFFF5DFE`、`D193BDBF4B003291B5FD708A1D420FF14E6526C3473F5E786F869889B81B6FD6`。产物仍在任务 worktree 的 gitignored 目录，未修改旧 candidate、active 或数据库。

输出以唯一的 `daily_basic` 索引为左连接基表：sector 有 7,334,829 个唯一键，daily-basic 有 7,304,119 个唯一键，交集 7,303,993；因此丢弃 30,836 个 sector-only keys，并将 126 个 daily-basic-only keys 的 `l2_code_id` 写为 `-1`，净行数差为 `30,836 - 126 = 30,710`。这不是随机丢行，必须随 snapshot receipt 保留。

上述 Gate-0 candidate 是截至 2026-04-28 的冻结历史快照，只用于解释当时的前置门禁。当前官方指标和 R6/R7 使用 2026-06-30 快照；任何 hN 评价仍必须按交易日历反推 `last_evaluable_signal_date`，未成熟尾部只能用于 inference/backtest 特征，不能进入训练标签、IC 或期限择优。长周期 h30–h180 由 `LongHorizonLabelMaturityPurge` 对每个 learning segment 屏蔽最后 `horizon + 1` 个交易日标签，但 inference frame 保留；F-014 evaluator 还必须使用右删失而不是把未成熟样本伪造为失败。

### 7.2 数据 gate 与当前状态

1. `[COMPLETED]` `generate_static_factors_bundle.py` 已保持连续因子 `float32`，并对 `l2_code_id` 校验整数/范围、缺失 `-1` 和有符号 `int16/int32`。
2. `[COMPLETED]` Gate-0 隔离 candidate 生成、schema/指纹/覆盖/unknown receipt 已完成；历史产物保持不可变。
3. `[COMPLETED_FOR_QE]` 2018-08-01 至 2026-06-30 的 WSL QE bundle 已部署并由 R6/R7 实验验证；sector、price、basic 和 static 使用同一快照。
4. `[COMPLETED_FOR_OFFICIAL_METRICS]` 三个 R6 板块因子的 2026-06-30 官方独立指标和基本相关性已持久化。
5. `[PENDING_FOR_NON_QE]` 自动 transformation/review、`realtime_code_text`、离线/实时 parity 和统一 runtime `industry_code_map` 尚未闭环；QE loader/离线资产可用不能外推为荐股或模拟盘可用。
6. `[NOT_AUTHORIZED]` candidate/factor → production/paper/live promotion 必须由用户单独确认；本文和 QE 实验都不隐式执行。

GAT embedding 在研究期使用实验冻结 mapping；进入任何非 QE 运行时前，必须统一 embedding、导出与实时侧 `industry_code_map`，并验证 unknown、新增行业和重启后的映射稳定性。

## 8. 研发流程

### Stage 0：预检与去重

1. 数据 gate 全部通过。
2. 在任何公式运行前建立 append-only `trial_id` 台账；公式、窗口、方向、阈值、种子、切分及失败版本均计入，按 `{A1,A2,A4}`、`{A3,B2,R1,R2}`、`{A5,A6}`、`{B1,N1}` 管理相关候选族，N1 即使 KILL 也不得删除记录。
3. 用因子 MCP 对名称、描述、公式和同族因子定向搜索；搜索摘要必须下钻到明确窗口指标。
4. 对复用基线读取代码与 out-sample 指标，禁止换名重复开发；A2/A3/A4/B2 同时冻结其 partial/residual IC 控制集。
5. 为每个新候选写入预注册卡：公式、字段、窗口、方向假设、最小成员数、缺失值规则、主要相关性对照、成本/容量重点和 STATE 交互（如适用）。

研究治理采用“保留完整审计、缩短显然无效路径”的快车道。`NEGATIVE_CONTROL`、`DUPLICATE`、`REUSE` 和 `BASELINE` 仍写入 append-only ledger、计入 family trial count，并保留 KILL/ERROR receipt；确定性公式重复、单调变换或方向副本在 Stage 0 直接停止，同族显然候选只做低成本 novelty screen。只有 `NEW` 主候选和通过 novelty screen 的条件候选进入完整预注册、purge/HAC、多重检验和组合门禁。快车道减少计算与文书等待，不删除失败版本、不重置 family multiplicity，也不把快速 KILL 改写为“未测试”。

### Stage 1：离线执行与双周期快筛

1. 在任务隔离 workspace 生成离线 `code_text` 和 `result.h5`。
2. 检查索引、列名、日期、股票数、板块覆盖、unknown 处理和非空率。
3. 主快筛使用与目标实验一致的 h20 裸标签；1d 只作短周期诊断。
4. 正式 h20 快筛使用 `quick_ic_screen.py --horizon 20 --split-manifest split.json <workspace>`。manifest 必须冻结 `trial_id/split_id/split_role/signal_start/signal_end/label_horizon_days/purge_days/embargo_days/expected_direction/data_snapshot_sha256`，并由预切分/purge 编排器生成；脚本校验 horizon、方向、日期、SHA-256 与 `purge_days >= 20`，输出 manifest SHA-256、`label_source_end` 和 `last_evaluable_signal_date` receipt。`quick_ic_screen.py` 只是指标核，不是 split authority，也不能单独保证 final test 只开启一次；该约束由 append-only trial ledger 审计。
   - 省略 `--split-manifest` 时，即使传入 `--direction` 也只是 diagnostic，不具备 Stage 1 PASS 资格；未传方向时保留旧 absolute verdict 仅为 1d 向后兼容。不得用 1d、unsigned 或无 split receipt 的 PASS 替代正式 h20 PASS。
5. 固定 chronological train/validation/test；按标签信息区间精确 purge。裸 h20 边界至少移除前一分段末尾 20 个信号日；若采用双向 CV/CPCV，再使用预注册 embargo。滚动标准化、阈值与方向只能在 train/validation 冻结，最终 test 只开启一次。
6. h20 的重叠日收益必须同时报告普通 ICIR 与 Bartlett lag=19 的 Newey-West HAC ICIR；再以 stationary/block bootstrap 或预注册非重叠抽样做区间与符号敏感性。`HAC ICIR = mean / sqrt(long-run variance)`，不是 t-stat；退化或样本不足必须显式为空。
7. 查看 validation 后改变任何公式、窗口、方向、阈值或样本切分，必须新建 `trial_id`，不得覆写旧结果。

以下 h20 初筛门槛为暂定门槛，必须先在 train/validation 上校准并冻结；在完成校准前只用于研发排序，不能据此宣称最终 out-sample PASS：

train/validation 同时冻结预期方向 `d ∈ {-1, +1}`。下表使用方向调整后的 `d * IC_h20` 与 `d * RankIC_h20`，因此绝对值达标但符号与冻结方向相反的结果不得 PASS。

| 条件 | 判定 | 行动 |
|---|---|---|
| `d * IC_h20 >= 0.015` 且 `d * RankIC_h20 >= 0.015` | PASS | 进入 Stage 2。 |
| 未满足 PASS，`d * IC_h20 >= 0` 且 `d * RankIC_h20 >= 0`，并且（`d * IC_h20 >= 0.005` 或 `d * RankIC_h20 >= 0.010`） | MARGINAL | 保留失败归因；只允许一个预注册修订版。 |
| 其余情况（含结果与冻结方向相反） | KILL | 不入库，不派生窗口。 |

N1 必须 PASS 才能继续；1d 与 h20 方向不一致时不得自动翻转，先做持有期与金融语义诊断。

### Stage 2：可执行入库与统一指标

1. 通过 manual factor API/脚本保存离线源码和 `asset_path`。
2. 生成 loader-only `realtime_code_text`，完成离线/实时 parity。
3. 计算统一指标，至少覆盖 `full`、`out_sample`、`recent_6m`、`recent_3m`、`recent_1m`。
4. RD-Agent 指标结果保持既有 1d 行与 legacy `rank_ic_20d` 兼容，并在同一结果增加 exact nullable contract：`h20_return_horizon=T21T1`、`h20_ic_mean`、`h20_ic_std`、`h20_rank_ic_mean`、`h20_rank_ic_std`、`h20_icir`、`h20_rank_icir`、`h20_icir_hac`、`h20_rank_icir_hac`、`h20_ic_positive_ratio`、`h20_n_obs`、`h20_hac_lag=19`；其中 positive ratio 与 n_obs 均按 raw Pearson IC 日序列统计，主筛选不得只读 `return_horizon=1d`。
   - legacy 行键 `return_horizon=1d` 表示持久化主记录兼容；RD 内部计算 key `20d` 表示持有期；区间 label `T21T1` 表示 T+1 入场到 T+21 出场。三者语义不同，不得互相覆写或据字符串推断唯一键。
   - RD 官方 naive std/ICIR 使用 NumPy population std（`ddof=0`）。quick screen 为保持旧 1d 输出继续保留 legacy `icir/rank_icir`（`ddof=1`），同时显式输出与 RD 对齐的 `ic_std_ddof0`、`icir_ddof0`、`rank_ic_std_ddof0`、`rank_icir_ddof0`；正式重叠 h20 推断优先读取 HAC 字段。不得把两种 naive ICIR 混为同一数值口径。
5. 执行 LLM 分类和增量相关性；记录 catalog、metrics、classification、correlation 的完整性 receipt。
6. AIstock 只提交 additive schema/upsert/router/MCP 字段支持；本 Gate-0 不应用生产 DDL、不写生产指标行。生产迁移必须作为独立 gate 执行和留证。
7. writer authority 保持不变：official evaluation writer 是唯一允许落 `aistock_factor_metrics` 的路径；`rdagent_factor_metrics_sync` 仅保留并测试兼容 SQL/旧 payload normalization，task/loop 非官方落表继续明确禁用，不得因 h20 字段就绕过。
   - 旧 payload 完全不含 h20 keys 时，presence flag 为 false，冲突更新必须保留已有 h20 值；新 contract 即使显式携带 `None`，presence flag 仍为 true，可正确清除本次已退化/不足的旧值。不得用简单 `COALESCE` 混淆“字段缺席”和“显式空值”。

### Stage 3：双层相关性与筛选

- 股票映射层和板块原生层均要求与基线/Top 因子 `|corr| < 0.8`。
- 同族候选高相关时只保留 h20 更稳定、覆盖更高、模型增量更好的一个。
- 除原始相关性外，执行第 4.8 节冻结的 partial/residual IC；控制后无稳定增量的候选即使 `|corr| < 0.8` 也不得被视为新发现。
- 沿用 Stage 1 冻结方向 `d`，定义 `IC_d = d * IC_h20`、`RankIC_d = d * RankIC_h20`、`ICIR_d = d * ICIR_h20`；不得在 Stage 2/3 重新选择符号或覆写 `d`。
- out-sample h20 目标：`IC_d >= 0.02`、`RankIC_d >= 0.02`，且 block/HAC `ICIR_d > 0.3`；`IC_d` 或 `RankIC_d >= 0.03` 可标记为优秀，但不得忽略显著性与模型增量。
- full 与 out-sample 的 `IC_d`、`RankIC_d` 均应为正且方向一致；近期窗口漂移必须解释。
- 任何因子都不能仅因 QE archive 共现表现良好而跳过独立门禁。
- 按候选族报告有效独立试验数、HAC t/ICIR、local FDR 或等价多重检验结果；组合/策略结果另外报告 DSR 与 PBO，禁止以单次最佳 Sharpe 代替。
- 除因子值相关性外，报告目标持仓/long-leg 重合、同向换手与冲击重合、压力期相关性、尾部亏损及成本跳升；平均相关性低但尾部重合高时标记“不同公式、相同拥挤风险”。
- 在多资金规模和成交参与率下报告换手、冲击、净 Sharpe、净回撤与 capacity curve；目标规模下净增量消失即不得 promotion。

### Stage 4：失败归因与下一批

对 KILL/MARGINAL 因子记录失败类型：数据覆盖、PIT 对齐、同族重复、方向漂移、短长周期冲突、板块规模偏置、波动/市值暴露或纯噪声。只有存在可证伪的新假设时才进入 Batch B。

## 9. QE 对照实验设计

最终目标不是“单因子 IC 排名”，而是确认显式板块因子和关系 embedding 对 G12 的独立增量。

### 9.1 GATs 2×2 消融

保持裸 h20 标签、数据切分、随机种子、训练预算和评价指标完全一致：

1. G12，关闭 `l2` embedding；
2. G12 + 通过因子，关闭 `l2` embedding；
3. G12，开启 `l2` embedding；
4. G12 + 通过因子，开启 `l2` embedding。

GATs 继续使用 1-parallel，防止并行资源争用污染比较。不得只比较第 1 组和第 4 组，否则无法区分因子贡献、embedding 贡献和交互贡献。

### 9.2 LGBM 对照

LGBM 至少比较 G12 与 G12 + 通过因子。若另行把 `l2_code_id` 作为 categorical feature，必须作为单独实验腿，不能称为 GATs embedding 的等价实现。

A5/A6 作为 STATE 信号时，可各自增加且仅增加一个在 test 前冻结的 `state × momentum/breadth` 交互实验腿，用于判断条件组合增量。交互只属于模型消融，不新增 catalog 原子因子，也不得在测试后从多个交互中择优。

### 9.3 结果门禁

报告 h20 IC/RankIC、naive/HAC ICIR、bootstrap 区间、CAGR、DSR、PBO、Sharpe、最大回撤、换手、成本和容量曲线，并分训练/验证/测试及主要市场 regime。GATs 2×2 与 LGBM 都必须比较 OOS ΔIC、净 Sharpe、回撤、换手、容量和多种子稳定性；只有跨合理种子/切分稳定的增量才进入 Tier2/IC 审核。

### 9.4 GATs + LGBM 组合验证（最高优先级）

该方向不新增训练架构，先回答“GATs 单腿即使不超过 LGBM，是否能改善组合”。历史归档 `qe_20260709_055708_fe49_L2`（GATs）与 `qe_20260708_030408_80cd_L1`（LGBM）可作管线 canary；既有分析给出的日截面 rank 相关约 `0.595`、Top25 重合约 `6.9%` 只作为待复核先验，不是最终验收证据。正式结论必须使用 R6 或后续同数据快照、同因子集、同 seed、同 split 的配对预测。

至少比较：

1. 两个单腿；
2. validation 冻结权重的 rank/prediction fusion；
3. 独立下单后在组合层合并的 portfolio fusion；
4. 在相同总持仓数、总风险预算和成本模型下的 sector-exposure constrained sensitivity。

融合权重和归一化方法只能在 validation 冻结，最终 test 不得重新选权。报告预测 rank 相关、Top-K/行业暴露/换手重合、边际贡献、净 CAGR/Sharpe/Calmar/最大回撤、容量和 leave-one-leg-out。若组合不改善扣费后风险收益或改善只来自扩大风险预算，则停止继续扩展 GATs 单腿。

#### 9.4.1 2026-07-14 历史 prediction-fusion canary receipt

本 canary 只验证历史预测资产的可对齐性、信号正交性和 prediction fusion 管线，不创建 QE 实验、不重新训练模型、不执行组合回测，也不构成 F-013 的正式晋级证据。输入腿固定为 `qe_20260709_055708_fe49_L2`（GATs）与 `qe_20260708_030408_80cd_L1`（LGBM）：两份 `pred.pkl` 各有 `2,260,161` 行、`443` 个共同预测日、`5,120` 个 instrument，预测窗口为 `2024-07-01` 至 `2026-04-28`。

正交性复核得到日截面预测 rank 相关 `0.594975`，Top25 Jaccard `0.036607`；后者等价于每天平均约重合 `1.77/25` 只股票，或以单腿 Top25 为分母约 `7.1%`。因此两腿存在显著选股差异，但低重合本身不证明组合收益提高。

h20 标签只纳入已经成熟的信号日。两腿共有 `2,154,168` 个预测/标签对、`424` 个成熟交易日和 `5,116` 个 instrument，评价窗口截止 `2026-03-31`；两份 label artifact 在全部共同样本上 `max_abs_diff=0`。预测窗口尾部尚未成熟的 19 个信号日没有进入 IC、RankIC 或 Top25 标签统计。

在读取结果前冻结两种等权方案：主方案为 `equal + rank`，敏感性方案为 `equal + zscore`，两腿权重均为 `0.5/0.5`。两腿场景中的 `orthogonality_aware` 会退化为相同的 `0.5/0.5`，因此不重复；`ic_weighted` 与 `risk_parity` 必须留到正式 R6 validation 窗口估权，禁止用本 canary 全段 OOS 选择权重。

| 方案 | h20 RankIC | h20 IC | RankIC 正向率 | Top25 h20 标签均值 |
|---|---:|---:|---:|---:|
| GATs 单腿 | 0.102045 | 0.055060 | 77.59% | 0.053662 |
| LGBM 单腿 | 0.113758 | 0.077744 | 88.21% | 0.072899 |
| `equal + rank` | 0.119018 | 0.065213 | 84.20% | 0.058810 |
| `equal + zscore` | 0.121489 | 0.074162 | 84.20% | 0.069291 |

相对 LGBM，`equal + rank` 与 `equal + zscore` 的平均 RankIC 分别增加约 `0.005260`（`+4.6%`）和 `0.007731`（`+6.8%`），但两者的 IC、RankIC 正向率和 Top25 h20 标签均值均未全面超过 LGBM；其中 `equal + zscore` 的 Top25 标签均值仍比 LGBM 低约 `5.0%`。当前结论因此冻结为：**prediction fusion 显示排序增量与正交性，但尚未证明头部收益转换或成本后组合增量**。后续正式判断仍需 R6 同因子、同 seed、同 split、同数据快照的配对预测，并完成固定总风险预算的 portfolio fusion、成本、回撤、容量和 leave-one-leg-out 回测。

执行时 R6 CPU/GPU 节点均有在途任务，节点容量门禁禁止组合回测抢占现有 QE 资源，因此本 canary 没有提交 combine-backtest。该状态是有意的资源隔离，不得记录为组合回测失败。

#### 9.4.2 R6 正式同口径因子析因结果（2026-07-14）

R6 已在 `dataset_as_of=2026-06-30`、`filtered_pool_20260630`、h20 裸标签、Alpha158 关闭、相同切分、V25_1_SMALL_CAP 和 seeds `123/314/2718` 下完成。CPU 任务 `qe_20260714_104829_a9ca` 与 GPU 任务 `qe_20260714_104830_0230` 均为 15/15 Loop completed。下表使用三种子均值；Sharpe、最大回撤和 CAGR 使用 absolute portfolio 口径，不能与单 Loop 的 excess-return `information_ratio/max_drawdown` 混用。

LGBM：

| 因子集 | Loops | RankIC 均值 ± σ | IC 均值 | CAGR | Sharpe | 最大回撤 | Top20 h20 | 年化换手 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| G12 | 1/6/11 | 0.08799 ± 0.00033 | 0.05236 | 0.6107 | 1.7406 | -19.78% | 0.05174 | 18.92 |
| G13-F | 2/7/12 | 0.08848 ± 0.00017 | 0.05402 | 0.7030 | 1.9184 | -19.45% | 0.05507 | 19.78 |
| G14-FP | 3/8/13 | 0.09185 ± 0.00023 | 0.05630 | 0.6900 | 1.9214 | -21.12% | 0.05981 | 19.37 |
| G14-FL | 4/9/14 | 0.09101 ± 0.00050 | 0.05379 | 0.6586 | 1.8382 | -19.65% | 0.05542 | 19.32 |
| G15-FPL | 5/10/15 | 0.09405 ± 0.00068 | 0.05591 | 0.6761 | 1.8625 | -20.39% | 0.06004 | 19.12 |

EfficientGATs（`l2_code_id` embedding on_dim8，binary adjacency off）：

| 因子集 | Loops | RankIC 均值 ± σ | IC 均值 | CAGR | Sharpe | 最大回撤 | Top20 h20 | 年化换手 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| G12 | 1/6/11 | 0.09450 ± 0.00402 | 0.04984 | 0.5455 | 1.5812 | -16.96% | 0.05169 | 16.88 |
| G13-F | 2/7/12 | 0.09245 ± 0.00398 | 0.04486 | 0.4626 | 1.4619 | -18.45% | 0.03816 | 15.55 |
| G14-FP | 3/8/13 | 0.08590 ± 0.00905 | 0.04670 | 0.6012 | 1.7143 | -16.65% | 0.05420 | 16.37 |
| G14-FL | 4/9/14 | 0.09207 ± 0.00947 | 0.04561 | 0.5848 | 1.6819 | -16.70% | 0.05180 | 15.92 |
| G15-FPL | 5/10/15 | 0.08404 ± 0.00551 | 0.04030 | 0.5278 | 1.5643 | -16.34% | 0.04341 | 14.39 |

R6 结论冻结如下：

1. F/P/L 不是“加入越多越好”。LGBM 的 G15-FPL 取得最高 RankIC，但 CAGR/Sharpe 低于 G14-FP；L 因子改善排序不等于改善组合转换。
2. G14-FP 在 LGBM 上兼顾较高 IC/RankIC、Top20 和 Sharpe，且种子 RankIC 标准差仅 0.00023，因此作为 R7 h20 锚点；这不是宣称其所有指标均为五组最优。
3. GAT 的 G12 保留关系模型对照价值，但不同因子集的 RankIC 标准差明显高于 LGBM；新增 F/P/L 未形成跨种子、跨指标一致增量，不得据单个高值 Loop 扩容图模型。
4. R6 仍只评价 h20。它证明板块因子可被模型使用，但不能证明已捕获 60–180 日主升浪或可以进入生产。

#### 9.4.3 R7A 正式两腿组合结果（2026-07-15）

正式 run `macb_365aed6303e71d6e_20240701_20260629_20260714T174425343045Z` 使用 LGBM G14-FP h20（R6 Loops 3/8/13）和 GAT G12 embedding h20（R6 Loops 1/6/11）的三种子预测，OOS 为 2024-07-01 至 2026-06-29，Top50、`equal 0.5/0.5 + rank`、固定同一回测模板；状态为 `succeeded`。

| 方案 | CAGR | Sharpe | Calmar | 最大回撤 | Top20 h20 | Top20 命中率 | 年化换手 | 相对 LGBM |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| LGBM seed-ensemble baseline | — | 2.0215 | 3.6629 | — | — | — | — | 基线；Sharpe/Calmar 由 run 的正式 delta 反推 |
| R7A `equal + rank` | 0.664839 | 1.8052 | 3.3445 | -19.88% | 0.060834 | 59.05% | 17.82 | Sharpe -0.2163；Calmar -0.3184 |

R7A 的停止结论是 `COMPLETED_NO_PROMOTION`：组合管线已经通过真实回测，但等权 rank 未改善 LGBM 的风险调整后收益。历史 canary 的 RankIC 增量、较低预测相关和较低 TopK 重合不能推翻正式组合结果。当前 GAT 腿不得直接进入默认 Type B 多 Alpha 包，也不允许通过增加总风险、总持仓或事后改权制造“增量”。

#### 9.4.4 R7B 正式结果与停止结论（2026-07-15）

R7B 严格执行 R7A 的单变量敏感性：seed ensemble、OOS、Top50、固定等权和 baseline 全部不变，仅将 `normalize_method: rank -> zscore`。正式 run 为 `macb_365aed6303e71d6e_20240701_20260629_20260714T190901628242Z`，状态 `succeeded`；OOS 为 2024-07-01 至 2026-06-29，LGBM G14-FP h20 与 GAT G12 embedding h20 各使用三种子 ensemble，权重固定 `0.5/0.5`。

| 方案 | CAGR | Sharpe | Calmar | 最大回撤 | TopK h20 收益 | TopK 命中率 | 换手 | 相对 LGBM Sharpe / Calmar |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| R7A `equal + rank` | 66.48% | 1.8052 | 3.3445 | -19.88% | 6.0834% | 59.05% | 17.82 | -0.2163 / -0.3184 |
| R7B `equal + zscore` | 67.95% | 1.8313 | 3.5429 | -19.18% | 6.3768% | 58.63% | 23.54 | -0.1902 / -0.1200 |

R7B 相对 R7A 的 CAGR、Sharpe、Calmar 和最大回撤分别改善约 1.47 个百分点、0.0261、0.1984 和 0.70 个百分点，但 TopK 命中率下降约 0.42 个百分点，换手增加约 5.72。更重要的是，R7B 的 Sharpe/Calmar 仍低于同口径 LGBM baseline，因此状态为 `COMPLETED_NO_PROMOTION`。预注册停止条件已经触发：不继续试 `ic_weighted/risk_parity` 等 prediction-fusion 权重变体；后续资源转向长周期标签腿、两层板块模型以及通过独立门禁的 portfolio fusion/LOO。

### 9.5 两层板块轮动模型

正式投入完整 top-down 工程前，先运行不可部署的四格 oracle 上界，回答“板块选择”和“板块内选股”各自还有多少可提取空间：

| 板块选择 | 板块内选股 | 用途 |
|---|---|---|
| reality | reality | 当前可实现的一层/两层基线 |
| oracle | reality | 隔离板块预测层上界 |
| reality | oracle | 隔离板块内排序层上界 |
| oracle | oracle | 整条层次结构的理论上界 |

oracle 只能使用未来收益构造研究上界，结果必须标记 `NON_DEPLOYABLE_FUTURE_INFORMATION_CEILING`，不得进入训练、预测、组合或 Alpha 晋级证据。运行前冻结 Top-M、评价 horizon、Top-K、PIT 成员、可交易约束、信号到成交规则、成本和每格相对 reality/reality 的最小经济增量；板块层与个股层阈值分别预注册。主判定使用置信区间：增量下界超过阈值为 `GO`，上界低于阈值为 `STOP`，其余为 `INCONCLUSIVE`。同时运行 soft-gating 上界，避免 hard Top-M 因一次性截断而丢失跨板块的边际赢家。F-014 尚未 `F014_RESEARCH_DECISION_READY` 时可完成设计、数据对齐和 dry-run，但不得形成正式 go/stop。

只有 oracle 显示可提取空间后，才建立可解释的真实 top-down 对照：

1. 板块层使用等权板块面板及 A1–A6 中通过门禁的因子，对申万 L2 板块做 20/40/60 日趋势、广度、资金与状态评分，输出 hard Top-M 与连续 soft gate；
2. 个股层在板块条件下使用长期趋势因子、板块内 leadership 和流动性/可交易性选择股票；soft gate 对全市场保留非零候选权重，不把层次模型简化为硬过滤；
3. 行业不做标签中性化，但组合层记录单板块上限、板块集中度、轮动成本和涨跌停/停牌造成的捕获损失；
4. 与相同候选池、Top-K、风险和成本预算的一层 LGBM、GATs、简单“板块动量 + 板块内动量”以及 oracle 四格比较。

板块层和个股层分别归档分数、入选原因和淘汰原因。若 oracle-sector/reality-stock 已高但现实板块层无增量，瓶颈在板块预测；若 reality-sector/oracle-stock 已高但现实个股层无增量，瓶颈在板块内排序；若 oracle/oracle 上界也低于预注册阈值，则停止完整两层工程，而不是继续增加架构容量。

### 9.6 长期上涨趋势专用评价

h20 继续作为当前模型对照的统一信号标签，但它不能单独代表“连续上涨数月、捕获右尾大行情”的策略目标。post-R6 结果必须与 `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` 的 Phase 8 口径对齐，增加：

- 20/40/60/120/180 个交易日收益画像和按日期聚类的置信区间；
- `+30%/+50%/+70%` 有序目标的 Recall@20/50、到达概率与 time-to-hit calibration；只有独立深池契约获批后才评估 Recall@100；
- trend-stage survival、右删失、趋势失效、峰前回撤、MFE/MAE、trend capture ratio 与 false early-exit rate；
- 最近 1 年、最近 6 个月及科技抱团/板块集中等预注册 regime 切片，但不得用切片结果反向选择全期公式；
- 持仓超过 30 日不设硬约束，由信号存活、趋势失效和成本后收益决定。

Type A 超跌反弹与 Type B 长期趋势保持独立因子选择、标签头、调仓和退出逻辑；旧多 Alpha 腿只能作为组合相关性/风险基线，不能作为 Type B 演进母体。

F-014 的可实施详细设计已固化在 `docs/architecture/qe_long_trend_evaluation_f2_design_20260714.md`。该设计只读复用现有 Recorder/prediction pointer 和 `qe_archive.run` 父身份，不扩展通用 Prediction Store 或既有 Archive 通用 writer/schema；使用 feature/outcome 双快照身份、extension-only 历史价格校验和右删失；逐信号/episode 明细进入 QE-only CAS Parquet，PostgreSQL 只通过三张 additive `run_evaluation*` 表保存评价身份、状态、标量和制品指针。能力只在 QE task/Loop、QE Archive、QE MCP 与对应 UI 内可见，不接入任何交易或选股运行时。当前状态仅为 `DESIGN_READY_AMENDED`，不得写成 evaluator 已实现或 R6 已完成长期评价。

为避免 F-014 成为串行大工程，其实施按三个并行工作流推进：`计算/统计/可成交性`、`CAS/状态/三表/幂等恢复`、`API/MCP/UI/历史补算`。里程碑语义固定为：

| 状态 | 最小证据 | 允许用途 |
|---|---|---|
| `CORE_COMPUTE_VERIFIED` | 公式、删失、barrier、episode、oracle fixture 通过 | 仅开发诊断，不作期限或模型裁决 |
| `PRELIMINARY_CAPTURE_AVAILABLE` | 不可变 profile/数据身份、QE-only CAS 制品和可复算 receipt 通过 | 可观察和发现数据问题，不得 promotion |
| `F014_RESEARCH_DECISION_READY` | 三表/状态/API/MCP/UI、历史 `long_trend_only`、真实 E2E、重启幂等和 QE-only 零影响全部通过 | 才可正式选择期限、裁决 R8M、进入 R8B2/R8C |

上述是同一 F2 交付内的工程里程碑，不是“先交脚本、以后再补平台”的简化验收。任何脚本-only、JSON-only、后端-only、无历史补算或无 UI 的实现都不能把 F-014 标记完成。

F-014 同时增加信号→成交桥接：理论机会层继续以 `T+1 qfq close` 衡量可预测机会，实际捕获层以经 reconciliation 的 order/trade artifact 为最高权威，区分 `filled_t1`、`delayed_fill`、`blocked_limit_up`、`blocked_suspension`、`never_filled` 及 `entry_delay_days`，并报告入场阻断损失的 MFE/barrier winner；退出侧对称区分跌停、停牌和延迟退出。日线触及涨跌停不等于订单必然未成交；没有订单/队列证据时必须保守标记 `NOT_VERIFIABLE`，不得从派生交易汇总伪造精确 child-order 行为。组合成本后收益仍以 portfolio report 为权威。

#### 9.6.1 长周期实验的三条独立轴

长期趋势实验必须把以下三条轴分开，禁止一次实验同时改变后再把差异归因给某一项：

1. `label_horizon`：未来 30/40/60/120/180 个交易日收益目标；决定模型学习什么。
2. `step_len/lookback`：LSTM/TCN 向后观察多少个交易日；决定模型看到什么历史。
3. 持仓/退出周期：由信号存活、趋势失效、成本后收益和组合约束决定；不得等同于标签期限或设置“必须持有 N 日”的硬规则。

当前 QE 已支持 `1/3/5/10/20/30/40/60/120/180` 标签，并对 `horizon >= 30` 的 learning frame 应用 `LongHorizonLabelMaturityPurge`。R8A 先固定模型和因子，只比较标签期限：

| 项目 | 冻结值 |
|---|---|
| 基线 | 复用 R6 LGBM G14-FP h20 三种子结果，不重复训练 |
| 新标签 | h30、h40、h60、h120；h180 暂不进入首批 |
| 模型/因子 | `LGBModel_conservative_v1` / G14-FP |
| seeds | 123、314、2718 |
| 规模 | 12 个 CPU Loop；远端 CPU 按已验证资源上限执行 |
| 固定项 | 2026-06-30 数据、股票池、split、Alpha158 off、Top50、V25、费用与 ST PIT 策略 |
| 主评价 | 各自 hN IC/RankIC + HAC/block/non-overlap、TopK hN、absolute CAGR/Sharpe/回撤/换手 |
| 长期评价 | F-014 的 +30/+50/+70 Recall、time-to-hit、MFE/MAE、survival、capture/false-exit 和 regime 切片 |

h30 是 h20→h40 的插值控制，h40/h60 是第一优先的两月至一季度趋势目标，h120 是半年趋势探针。h120 会从每个 learning segment 屏蔽最后 121 个交易日标签，有效样本和近期成熟标签显著减少；必须同时报告统一共同成熟截止日和各期限最大可用截止日，不得因长周期样本更少而直接用原始 IC 大小择优。R8A 可以在现有指标链上先完成初筛，但 F-014 evaluator 未实现前只能标记 `PRELIMINARY_HORIZON_SCREEN`，不能宣布长期趋势目标已经通过。

R8A 已按上述冻结卡创建并启动，task 为 `qe_20260715_101942_d873`，名称为 `R8A-CPU Type-B G14-FP 长周期标签 h30-h120 4horizon x 3seed LGBM`。12 个 Loop 固定分配到 `rdagent-node1`，并行度 4；本次更新查询时任务为 `running`，首批 h30 三种子与 h40/seed123 已进入运行。任务使用 `filtered_pool_20260630`、G14-FP 14 因子、Alpha158 off、`score_weighted_topk_v2`、Top50、`V25_1_SMALL_CAP`、同一数据 split、费用和 QE-only ST PIT。任何已完成子 Loop 的中途指标都不得触发缩减期限、增加模型或变更种子。

#### 9.6.2 R8B LSTM 预注册 canary 与其他模型进入顺序

R8B 不读取 R8A 中途结果，而是提前预注册 h40/h60 两个业务优先期限作为并行 canary；每个期限使用 seeds `123/314/2718`，首轮固定 `step_len=20`，共 6 个 `gpu_parallel_standard` Loop。这样可在 R8A 完成前利用独立 GPU 节点缩短等待时间，同时仍能在事后按同期限、同种子比较 LGBM 与 LSTM 的模型归纳偏置。若 LSTM 在长期捕获、种子稳定性或与 LGBM 的组合边际上通过，再另立 R8B2 对胜出标签测试 `step_len=20/40/60`，不得回写或自适应修改 R8B。

R8B 已创建并启动，task 为 `qe_20260715_104922_001d`，名称为 `R8B-GPU Type-B G14-FP LSTM h40-h60 2horizon x 3seed canary`。模型固定 `__seed_LSTM_10D_hs64_d02__`，使用 `TimeSeries`/`TSDatasetH`、`step_len=20`；G14-FP 14 因子、数据快照、股票池、split、Alpha158、策略、Top50、执行算法、费用和 QE-only ST PIT 与 R8A 完全一致，仅模型族和预注册期限矩阵不同。6 个 Loop 分配到 `wsl2-5080`，并行度 2；本次更新查询时任务为 `running`，h40/seed123 与 h40/seed314 已有真实 runner 进程。启动前节点 Results API 曾离线，按正式服务入口恢复并 probe 后原配置启动成功，未降级 CPU、未改变实验卡；该事件只作为 F-017 运行证据，不作为研究收益证据。

模型优先级冻结为：

1. LSTM：第一挑战者；历史 h20 有正向证据，但必须在 2026-06-30 同口径数据上重跑。
2. TCN：只有 LSTM 显示时序结构增量后，才对一个胜出期限做 canary；既有 h20 结果较弱，不进入首轮全矩阵。
3. GAT：R7B 已完成但未晋级，因此当前 GAT prediction-fusion 不扩展至 h40/h60；关系研究继续按 HIST/动态边路线独立推进。
4. 时间维 Transformer：既有 RankIC 近零，状态保持 `KILL_EQUIVALENT_ARCHITECTURE`。
5. GRU/CatBoost/XGBoost/TabPFN：不是当前 Type B 的优先机制探针；只有 LGBM/LSTM 与两层板块模型均不能解释剩余误差时再立独立实验卡。

#### 9.6.3 R8M 多期限共享表示与迁移假设

R8M 是 R8 之外的独立研究卡，不追加或重写 R8A/R8B，也不因“共享表示”自动成为新 Alpha 腿。它检验长 horizon 样本减少时共享/迁移是否有净增量，不预设正迁移：

1. 独立训练 h20/h40/h60/h120；
2. 共享 encoder + 各期限独立 head；
3. h20 预训练 + 冻结 encoder，只训练长期 head；
4. h20 预训练 + 全量 fine-tune。

每个 head 使用自身 maturity mask、purge 和 loss denominator，禁止用未成熟标签补 0。实验必须输出 transfer matrix、leave-one-head-out、per-head 样本量与梯度余弦/冲突诊断；只有观察到稳定梯度冲突后，才允许另立 PCGrad 或动态 loss weighting 对照，不得在首卡中捆绑。设计和 wiring canary 可与 F-014 并行；完整多种子矩阵应在独立 R8 基线归档后执行。R8M 的 IC 迁移只回答预测能力，最终胜负仍需 `F014_RESEARCH_DECISION_READY` 后比较长期捕获、可成交性损失和固定风险预算下的组合增量。

#### 9.6.4 多腿组合的准入与上限

首轮 Type B 多 Alpha 最多三条腿，每条腿必须代表不同经济任务，而不是同一因子集或相邻标签的重复模型：

| 角色 | 首选候选 | 准入条件 |
|---|---|---|
| 中短期趋势锚点 | LGBM G14-FP h20 | R6 已完成；R7 baseline |
| 长周期趋势腿 | R8A/R8B 或后续 R8M 胜出的 h40/h60 LGBM/LSTM | F-014 长期指标通过，且相对 h20 具备任务级增量 |
| 板块决策/关系腿 | 两层板块→个股模型；后续 HIST-industry | 板块 Recall 与板块内排序分别通过；同风险预算下增加净收益或降低回撤 |

h30、h40、h60、h120 不是天然的四条腿，模型名不同也不自动构成经济上独立的任务。任一候选加入组合前必须预注册并报告：每日 prediction rank 相关、TopK 与实际持仓重合、入场时点重合、板块暴露/集中度重合、P&L 相关、`+30%/+50%/+70%` 右尾事件重合、换手/成本重合，以及固定总风险预算下的 leave-one-leg-out。不得用任意“相关性必须低于 X”单独否决；最终准入看该腿在固定风险预算下是否增加可成交的长期捕获和成本后收益，或降低回撤/尾部风险。删除某腿后组合更好、增量只来自扩大风险预算，或相邻期限无 LOO 边际时，该腿不得进入组合。当前 GAT G12 不占用默认第三腿名额；R7B 只证伪当前 0.5/0.5、rank/zscore、Top50 和现有成本下的 prediction-fusion，不否定 portfolio fusion、跨标签组合或关系模型的板块选择价值。

### 9.7 PIT 关系模型路线

1. **HIST-industry canary**：基于 `market.sw_index_member` 生成逐日 PIT `stock2concept`/稀疏成员矩阵，补齐 composer 分支、`stock_index` 对齐、每日截面 batch、fit/predict、归档与回测契约。第一轮只验证 wiring、资源和同口径基线；禁止静态行业快照。
2. **动态加权/多关系图**：在 HIST-industry 或当前 GATs 显示结构增量后，分别测试只用历史窗口构造的 residual co-movement、leadership、flow/state 权重；边类型分头处理并做逐关系消融。R4 的真码二值 `industry_bias` 是已完成负/混合证据，不再重复。
3. **HATS/层次关系**：只有两种及以上 PIT 关系分别显示增量后才进入，避免仅用单一行业边却包装成多关系模型。
4. **MASTER/IGMTF/TRA**：MASTER 自研和市场状态特征管线成本高，后置到关系增量成立之后；IGMTF/TRA 可作较低成本 canary，但必须明确它们不是 MASTER 的等价实现。既有时间维 Transformer RankIC 近零，不再重复等价架构。
5. **DoubleAdapt/在线适应**：只在 F-014 的预注册 regime 切片证明静态 Type B 模型存在显著时间衰减后立项；在此之前先定义“持仓跨模型版本”的预测归属、重训频率、冻结期和归因语义。频繁更新若快于 30–180 日持仓生命周期，会破坏可复算性，因此其优先级低于长期标签、两层模型和 Type B 内部 TRA。

### 9.8 概念板块与超图路线

概念关系频繁新增、同一股票同时属于多个概念，必须先完成独立的数据设计与 PIT 验收：关系记录至少包含 `concept_id`、`instrument`、`in_date`、`out_date`、source/version、采集/公告可用时点和变更原因；板块标识不得依赖易变名称，开放区间和同日多成员关系必须可复算。数据完整性、历史覆盖、变更捕获和退市/更名语义通过后，再按以下顺序研究：

1. HIST-concept，对比 HIST-industry，验证细粒度题材关系是否增加信号；
2. industry + concept 多关系 HATS/GAT，做逐关系和交互消融；
3. 概念超图或多头关系注意力，直接表示一股多概念，不复制样本、不把多个概念压成单码；
4. 概念层板块评分 → 概念内龙头选择的两层模型。

概念数据集未入库前，上述项目状态统一为 `BLOCKED_BY_PIT_DATASET`，不得用 `sina_board_daily` 的板块聚合或当前成分列表伪造历史成员。

### 9.9 执行顺序与资源门禁

截至 2026-07-15，R6 已收口，R7A/R7B 均完成但未晋级，R8A/R8B 已按预注册卡并行运行。研究任务和工程关键路径分开排序，默认顺序更新为：

1. **R8A/R8B 原卡完成并完整归档**（运行中，不阻塞文档/工程并行）：不追加 Loop，不改变 horizon、step length、超参或 seeds；保存 prediction、position、trade、数据身份和 receipt，确保 F-014 可历史补算。hN IC/RankIC 可用于完整性诊断，不作期限晋级。
2. **P0：F-014 完整 QE-only 实现**：三个工作流并行推进，先达到 `CORE_COMPUTE_VERIFIED` 和 `PRELIMINARY_CAPTURE_AVAILABLE`，但只有计算、可成交性桥接、CAS/状态/三表、API/MCP/UI、历史补算、真实 E2E、重启幂等与非 QE 零影响全部验收后，才标记 `F014_RESEARCH_DECISION_READY`。
3. **P1：四格 oracle + soft-gating 上界**：设计、数据对齐和阈值预注册可与 F-014 并行；正式 go/stop 必须等待 F-014 决策门，且 oracle 永远不是可部署 Alpha 证据。
4. **P1：R6/R7/R8 历史统一重评**：F-014 就绪后用同一 profile 和 outcome vintage 执行 `long_trend_only`；先比较 R8 同期限、同种子的 LGBM/LSTM，再与 h20 锚点比较任务级增量，不按单个最好 Loop 或原始 RankIC 选胜者。
5. **P2：R8M 设计与 wiring canary**：可提前完成多头 maturity/purge、transfer matrix 和 LOO 接线；完整多种子矩阵及最终裁决在独立 R8 归档和 F-014 就绪后执行。观察到梯度冲突后才增加 PCGrad 等修正。
6. **P2：完整两层模型**：只有四格 oracle/soft gate 达到预注册 `GO` 才开发 hard/soft 两种现实模型；否则停止该工程方向。
7. **P2/P3：R8B2 与 R8C**：R8B2 仅对 F-014 胜出且 LSTM 有稳定增量的期限比较 `step_len=20/40/60`；R8C 最多三腿，候选先通过任务级重合、右尾重合和固定风险预算 LOO。
8. **P3：条件模型路线**：HIST-industry → 动态多关系图 → 概念 PIT/超图 → MASTER/IGMTF/TRA。TRA 只在 Type B 内部路由；DoubleAdapt 仅在 F-014 regime 切片证明静态模型显著衰减、且持仓跨模型版本语义已设计后进入。

低成本、复用预测的实验先于新架构训练；同一研究阶段不得为了等待一个节点而擅自提升图模型并行度。

- GATs/HIST/大截面关系模型归为 `gpu_serial_graph`，默认 1-parallel；只有资源 canary 证明 host↔GPU 交换、显存和系统响应均稳定后才能调整。
- LSTM/TCN 等已验证可并行的模型归为 `gpu_parallel_standard`，并行上限由调度器按模型类判断；回测可与下一 loop 训练重叠，但必须隔离 recorder、工作目录和 GPU/CPU/内存配额。
- 远端 CPU 可并行执行 LGBM/融合回测，但在共享 factor cache 原子写、每因子锁、损坏检测/重建和 MLflow recorder 隔离通过定向验证前，不得把“可配置 4 并行”当作已放行能力。
- 后端重启不应终止已启动的外部 QE worker；新增调度逻辑必须继续满足任务状态可恢复、运行进程不被重复启动、结果只归档一次的契约。

## 10. 风险与控制

1. **重复因子风险**：F1/F4 已有大量同族或精确重复。通过公式级去重和双层相关性阻止换名复制。
2. **1d/h20 错配**：现有快筛和 MCP 主摘要偏 1d。h20 能力未补齐前，任何 PASS 只能标记为 preliminary。
3. **PIT 行业切换污染**：必须先构造板块面板，再做板块时序运算。
4. **大板块权重偏置**：同时报告等权板块层结果，不能只用股票映射层 IC。
5. **F3 低成功率**：N1 只作 negative control，禁止因“资金流叙事合理”跳过快筛。
6. **多重检验**：窗口、符号和公式在最终 test 前冻结；同族变体按 family 管理。
7. **成员样本不足**：成员数 `< 5` 或覆盖率 `< 0.8` 的板块日不参与聚合/排名。
8. **低波/规模暴露**：A4/A5/A6 必须检查 SIZE、VOL 与行业成员数暴露。
9. **离线/实时漂移**：双代码形态必须做数值 parity；loader 支持不等于转换提示、MCP 或 active 数据已经闭环。
10. **运行状态混淆**：代码合并、candidate 数据准备、active promotion、QE 实验和实时启用是五个独立状态，必须分别报告。
11. **重叠标签虚高**：h20 日度标签机械重叠；普通标准误仅作描述，决策必须包含 lag=19 HAC 和 block/non-overlap sensitivity。
12. **回测选择偏差**：trial 台账不完整、验证后覆写结果或只报告最佳种子都视为 gate 失败；候选族必须做多重检验，策略层必须报告 DSR/PBO。
13. **成本、容量与拥挤**：毛收益通过但目标资金规模净增量消失，或压力期持仓/冲击高度重合，均不得 promotion。
14. **生产副作用**：Gate-0 只允许代码、隔离 candidate 和测试证据；生产 DDL、生产 DB 写入、active promotion、服务重启和实时启用均保持 pending。
15. **关系身份混淆**：embedding、二值邻接、HIST 概念聚合和动态权重图不是同一能力；必须逐层消融，不能把任一结果外推到其他关系机制。
16. **静态关系未来函数**：当前行业/概念成员快照回填历史会系统性泄漏；所有关系模型只接受逐日 PIT 关系和可复核 mapping hash。
17. **h20 目标错配**：只优化 h20 RankIC 可能继续偏向短周期反转或较早止盈；长期趋势晋级还必须通过第 9.6 节的 60–180 日、右尾、存活和捕获率指标。
18. **融合伪增量**：改变总持仓、风险预算或成本假设会制造组合提升；融合实验必须固定总风险并报告 leave-one-leg-out、暴露和换手重合。
19. **概念多重成员膨胀**：复制一股多概念样本会改变权重和统计量；未来概念模型使用稀疏多热关系/超边，并在聚合后还原到唯一股票决策行。
20. **并行制品竞争**：共享 factor cache 或 recorder 的非原子写可能产生损坏或错读；并行度提升前必须验证锁、临时文件原子替换、制品 hash 和失败后的定向重建。
21. **F-014 简化交付造成真值分裂**：纯计算核、临时 JSON 或后端-only 页面若被当作正式决策证据，会让历史补算、UI 与数仓口径分叉；用三级里程碑和唯一 `F014_RESEARCH_DECISION_READY` 门禁阻止提前晋级。
22. **日线触板等同不可成交**：仅凭 high/low 触及涨跌停推断订单结果会夸大或低估捕获；trade artifact 为最高权威，无订单/队列证据时保守标记 `NOT_VERIFIABLE`，入场和退出阻断对称报告。
23. **oracle 事后阈值**：看到上界后再定义“值得投入”会产生方向性事后解释；四格、soft gate、成本/可交易约束、最小增量和置信区间判定在运行前冻结。
24. **多期限负迁移**：共享表示不保证优于独立训练；per-head maturity/purge、transfer matrix、LOO 和梯度冲突诊断为强制证据，未观察冲突不得预先堆叠修正算法。
25. **期限即独立腿的错误外推**：h20/h60 或 LGBM/LSTM 可能仍消费相同经济信息；用入场、持仓、板块、P&L、右尾事件、成本和固定风险预算 LOO 判定增量，不按模型名或 horizon 自动授予腿身份。

## 11. 验收与交付物

### 11.1 本批 Gate-0 交付物

- 融合一手机构/论文实施推论、F-001–F-012、L0–L5 验证与 production gates 的 F2 规格；
- 隔离 candidate bundle、schema、指纹、行列/覆盖/unknown/freshness receipt，旧 candidate/active 不变；
- `quick_ic_screen.py` 的 horizon、冻结方向、split manifest、HAC 和 exact label 契约及单测；
- RD-Agent → AIstock 的 exact h20 companion contract、nullable additive migration/official writer/router/MCP 代码与定向测试；RD task/loop 非官方 writer 仍禁用；
- F4/R2 tracked repair source 的 PIT 板块面板修复、冲突 fail-fast 与单测；catalog 双代码同步和指标重算后置；
- 两仓独立 PR/验证证据，以及 merge、DDL、DB、promotion、QE、runtime 状态的分离报告。

### 11.2 后续 G0-D：数据与接口

- candidate `sector_data.h5` / `static_factors.parquet` 的 schema、指纹与 `l2_code_id` receipt；
- 生成器对 `l2_code_id` 的整数 dtype、`-1` unknown、source/semantic schema 和覆盖率定向测试；
- transformation/review 对 `l2_code_id` 的兼容性 receipt；
- 离线/实时代码 parity 结果；
- unknown、PIT 行业切换、最小成员数和板块字段一致性测试。

### 11.3 后续 G0-D：因子研发

- Batch A 的 6 个候选代码；Batch B 仅在 gate 通过后交付；
- R1/R2 复用基线的 h20 重评估，不新增重复 catalog 项；
- N1 negative control 的快筛与最终 disposition receipt：KILL 时记录淘汰依据，PASS 时记录后续门禁；
- 每个候选的预注册卡、h20/1d 快筛、统一指标、双层相关性、分类与最终 disposition。
- append-only trial ledger、候选族有效试验数、purge/embargo 记录、HAC/block 推断和 partial/residual IC receipt；
- 多资金规模/参与率成本容量曲线，以及持仓、换手、冲击与尾部拥挤审计。

### 11.4 后续 G0-D：因子库完整性

仅对通过者要求：

- `aistock_factor_catalog`：`is_available=true`，`asset_path` 指向实际可执行源码；
- `aistock_factor_metrics`：官方窗口齐全，并有明确 h20 companion fields；生产 DDL 与生产回填未执行前必须标记 pending；
- `qe_factor_classification`：至少一条有效分类；
- `qe_factor_correlations`：股票映射层和板块原生层的增量相关性 receipt；
- 失败者不得以空代码、占位实现或仅元数据记录伪装成已交付因子。

### 11.5 后续 G0-D：模型验证与状态报告

- GATs 2×2 消融和 LGBM 对照结果；
- Tier2/IC 审核结论与未满足项；
- 分别报告：文档/代码合并状态、candidate 数据状态、active promotion 状态、QE 实验状态、模拟盘/实时状态；
- 未完成 h20 指标、数据 promotion 或 train/serve mapping 统一时，不得宣称板块轮动能力已生产就绪。

### 11.6 post-R6 研究交付物

- 历史 GATs/LGBM prediction-fusion canary receipt：预测/标签逐行对齐、正交性、Top25 重合、预冻结等权 rank/zscore 与信号级 h20 结果已完成；
- R6 正式同数据/同 split/同 seeds 的 LGBM/GAT 5 因子集 × 3 seeds 共 30 个 Loop 已完成；任务、因子集聚合和失败归因见第 9.4.2 节；
- R7A `equal + rank` 与 R7B `equal + zscore` 正式组合回测均已成功执行但未超过 LGBM Sharpe/Calmar，状态均为 `COMPLETED_NO_PROMOTION`；R7B 相对 R7A 略改善但换手更高，当前 prediction-fusion 权重扩展停止；portfolio fusion、完整 LOO、容量与风险预算敏感性继续 pending；
- 两层模型前置的 reality/oracle 四格与 soft-gating 上界：阈值、PIT、成本、可交易约束和置信区间判定预注册；通过后再交付真实板块评分、板块 Recall、板块内排序、集中度/轮动成本及一层模型对照；
- R8A/R8B/R8C 实验卡：R8A LGBM 长周期标签扫描 `qe_20260715_101942_d873` 与 R8B LSTM 预注册 canary `qe_20260715_104922_001d` 已创建并运行；R8C 仍等待长期趋势胜者、两层/关系胜者和 F-014 评价证据；
- 与 Advisory Phase 8 对齐的 20–180 日、MFE/MAE、有序目标、time-to-hit、生存、右删失、捕获率、假退出，以及信号→成交/退出阻断分层报告仍 pending；
- F-014 详细设计：`docs/architecture/qe_long_trend_evaluation_f2_design_20260714.md`，覆盖同一 evaluator 的正常 Loop/`long_trend_only`、双快照、理论机会与可成交性桥接、QE-only CAS/三表数仓、QE API/MCP/UI、失败语义、重启恢复和非 QE 模块零影响门禁；当前为 `DESIGN_READY_AMENDED`，代码/DDL/实际评价 pending，完整链路是唯一研究决策门；
- R8M 独立设计卡：独立训练、共享多头、冻结迁移、全量微调四臂；per-head maturity/purge、transfer matrix、LOO、梯度冲突和 F-014 最终裁决；
- HIST-industry 的逐日 PIT relation artifact、mapping hash、`stock_index` 对齐测试、composer/fit/predict canary 与资源 receipt；
- 动态/多关系图的逐关系消融；概念方向则先交付独立 PIT 数据设计与数据门禁，未通过前不交付模型“成功”结论；
- 每个方向独立的实验卡、停止条件、失败归因、资源类、并行策略和归档状态；不得用单次最好 loop 代替方向结论。

## 12. Design Acceptance Index / 设计验收索引

下列条目是 F2 的稳定验收 ID。实现、测试、PR 与后续生产 gate 必须引用这些 ID；“代码存在”不等于“生产启用”。

| ID | 设计要求 | 验收口径 |
|---|---|---|
| F-001 | 研究治理与试验台账 | 研究来源可追溯；每次公式/窗口/方向/阈值/种子/切分及失败版本有唯一 `trial_id`；候选族多重检验、purge/embargo 与最终 test 一次性开启规则明确。 |
| F-002 | candidate bundle 离散行业键 | `l2_code_id` 连接缺失为 `-1`，保留有符号整数 dtype，schema 为 `sector_data_raw/categorical_id`，生成覆盖率 receipt，且不覆盖 active/旧 candidate。 |
| F-003 | 通用 horizon 快筛 | `quick_ic_screen.py --horizon N` 的标签严格为 `close[t+N+1]/close[t+1]-1`；默认 1d 向后兼容；h20 提供 lag=19 HAC companion 指标，正式判定必须使用冻结方向和通过校验的 split manifest/receipt。 |
| F-004 | RD-Agent h20 统一指标 | 保留既有 1d 与 legacy `rank_ic_20d`；同一指标记录增加 `h20_return_horizon=T21T1`、IC/RankIC mean/std、naive/HAC ICIR、raw Pearson positive ratio/n_obs 与 lag=19 共 12 个 nullable 字段，API 可序列化。 |
| F-005 | AIstock h20 持久化与查询契约 | additive schema/upsert/router/MCP 暴露 F-004 字段；旧记录/旧客户端兼容；生产 DDL 和回填是独立 pending gate。 |
| F-006 | F4/R2 PIT 安全 repair source | tracked regeneration source 中的 `sw2_close` 先按 `(datetime,l2_code_id)` 构造唯一板块面板，再按板块时序计算；冲突 fail-fast，旧指标明确失效且后续需双代码同步/h20 重算，不新建近义因子。 |
| F-007 | 因子代码双形态与失败策略 | offline `code_text` 与 loader-only `realtime_code_text` 数值 parity；缺字段、重复板块值、unknown 或行业切换不静默回退。 |
| F-008 | 去重与条件增量 | MCP 定向搜索、股票映射层/板块原生层相关性、partial/residual IC 和既有 R1/R2 代码审计均留证；无增量则 `DUPLICATE/REUSE/KILL`。 |
| F-009 | 稳健性、成本容量与拥挤 | h20 HAC/block 推断、多重检验、DSR/PBO、真实 A 股约束下成本/容量曲线及持仓/尾部拥挤审计齐全。 |
| F-010 | QE 组合增量 | GATs 2×2、LGBM 对照、equal-sector/stock-mapped、多种子 OOS 增量；A5/A6 仅允许预注册 STATE 交互腿。 |
| F-011 | 零隐式生产副作用 | 本批不写生产 DB、不应用生产 DDL、不 promotion active、不重启服务、不启动 QE/模拟盘/实时交易。 |
| F-012 | 验证与状态分离 | 定向单测、F2 设计校验、diff 检查和 receipt 通过；合并、candidate、DDL、promotion、实验、运行时状态分别报告。 |
| F-013 | 组合与两层决策增量 | 同口径 GATs+LGBM prediction/portfolio fusion 和板块→个股两层基线；冻结权重/风险预算，报告正交性、成本、容量、暴露与 leave-one-leg-out；R7 结论只覆盖已运行的简单 prediction-fusion，不外推否定其他组合层。 |
| F-014 | 长期趋势目标一致性 | h20 对照之外，按 Advisory Phase 8 报告 20–180 日、有序右尾目标、MFE/MAE、time-to-hit、生存、右删失、捕获率和假退出；理论机会与实际成交/退出阻断分层；Type A/B 标签与生命周期隔离；计算、CAS、表、API/MCP/UI 只属于 QE，非 QE 模块零变化。只有完整链路可达到 `F014_RESEARCH_DECISION_READY`。 |
| F-015 | PIT 关系模型 | HIST-industry、动态加权图和多关系图只消费逐日 PIT 关系；mapping/stock_index fail-fast；R4 二值真码邻接不重复；新架构先通过 composer/资源 canary。 |
| F-016 | 概念多关系前置门禁 | 概念成员先完成多成员、有效区间、可用时点与 source version 的 PIT 数据设计/验收；通过后才允许 HIST-concept、HATS/多关系图或超图实验。 |
| F-017 | 研究调度与制品隔离 | 模型类决定串并行；共享 cache 原子写/锁/损坏重建和 recorder 隔离先验收；后端重启不终止或重复启动已运行 worker。 |
| F-018 | 两层 oracle 上界 | reality/oracle 四格与 soft gate 使用冻结 Top-M/horizon/Top-K/PIT/可交易/成本口径；GO/STOP/INCONCLUSIVE 由预注册阈值和置信区间判定；结果永久标记不可部署。 |
| F-019 | 多期限迁移完整性 | R8M 独立训练、共享多头、冻结迁移、全量微调四臂；per-head maturity/purge、transfer matrix、LOO 和梯度冲突齐全；不预设正迁移，最终过 F-014。 |
| F-020 | 任务级 Alpha 准入 | 不同 horizon/模型名不自动成为独立腿；预测/持仓/入场/板块/P&L/右尾事件/成本重合和固定风险预算 LOO 齐全，以可成交捕获及组合净增量裁决。 |

## 13. Implementation Plan / 实施计划

### Phase G0-A：研究与设计冻结

1. 把第 4.6–4.9 节研究门禁、候选族和研究来源写入本规格。
2. 运行 `aistock_feature_workflow.py validate --tier F2`，在代码交付前关闭所有设计结构缺口。

### Phase G0-B：数据与评估器前置能力

1. RD-Agent：修复 candidate bundle 的 `l2_code_id` dtype/unknown/schema/receipt，并在隔离目录生成新 bundle。
2. AIstock：给 `quick_ic_screen.py` 增加通用 horizon、精确 T+1→T+N+1 标签、split manifest/receipt、冻结方向及 HAC 指标。
3. RD-Agent：给统一指标引擎与 SOTA API 增加 F-004 companion fields。
4. AIstock：增加 F-005 additive DB/ingest/router/MCP 契约，但不执行生产迁移。

### Phase G0-C：PIT 基线修复与证据

1. 修复 F4/R2 可执行资产源的板块时序语义并增加切换/唯一性测试。
2. 对两个仓库分别运行最小定向测试、lint/compile/diff check；生成 candidate receipt。
3. 更新本矩阵为真实状态，列明所有外部门禁；各仓库独立提交 PR，禁止把跨仓库状态混写为一个“已完成”。

### Phase G0-D：后续因子研发（不属于本次前置实现）

F-001–F-006 通过且隔离 candidate 达到 `research-ready` 后，即可用 `develop-factor` 与因子库 MCP 按 A1→A6 顺序开展纯离线研发；无需等待 production DDL 或 active promotion。Batch B 只在 Batch A 失败归因完成后启动。每个候选独立执行预注册、快筛、统一指标、去重、分类和 disposition；生产持久化、promotion 与运行时仍受第 17 节独立门禁约束。

### Phase G0-E：post-R6 归因与长期趋势闭环

1. `[COMPLETED]` R6 已冻结并归档：同数据、同 split、同三 seeds 的 LGBM/GAT 五因子集共 30 个 Loop 全部完成；失败/不完整 Loop 不参与本次聚合。
2. `[COMPLETED_NO_PROMOTION]` R7A `equal + rank` 与 R7B `equal + zscore` 均已完成；R7B 仍不超过 LGBM，当前 prediction-fusion 权重扩展按预注册规则停止，portfolio fusion 与完整 LOO 保持 pending。
3. `[RUNNING_PRELIMINARY]` 按第 9.6.1–9.6.2 节运行 R8A 标签期限扫描与预注册 R8B LSTM canary；原卡完整完成并归档，不能把 label horizon、step length 与持仓期限混成一个变更，也不能按中途或最终 IC 改卡/晋级。
4. `[DESIGN_READY_AMENDED / BLOCKING]` 按 `docs/architecture/qe_long_trend_evaluation_f2_design_20260714.md` 三工作流实现 QE-only evaluator。内部里程碑不得冒充完整交付；只有 `F014_RESEARCH_DECISION_READY` 才允许期限选择、R8B2、R8M 最终裁决和 R8C。
5. `[DESIGN_READY_RUN_PENDING]` 预注册并执行第 9.5 节四格 oracle 与 soft-gating 上界；设计/数据准备可并行，正式 go/stop 等 F-014，oracle 不进入可部署链。
6. `[PLANNED]` 按第 9.6.3 节建立独立 R8M，先 wiring/transfer 诊断，后多种子；不预设共享表示优于独立训练。
7. `[CONDITIONAL]` oracle 通过后执行真实两层 hard/soft 基线；长期趋势胜者与两层/关系胜者均通过任务级增量门禁后，才按第 9.6.4 节组建最多三腿 R8C。不能转化为可成交的 60–180 日右尾捕获时，应回到因子/标签，不继续堆模型容量或腿数。

### Phase G0-F：PIT 关系模型 canary

1. 为 HIST-industry 形成独立 F1/F2 接入设计，包含逐日 relation artifact、composer、stock index、截面 batch、资源、归档与回滚契约。
2. 在同因子/标签/切分下比较 LGBM、当前 GATs embedding 和 HIST-industry；先 wiring canary，后多 seed alpha 实验。
3. 只有显式关系显示稳定增量，才进入动态权重与多关系逐项消融；MASTER/IGMTF/TRA 保持条件触发。

### Phase G0-G：概念 PIT 数据与多关系扩展

1. 先建立概念成员 PIT 数据集专项设计、采集/变更/质量/版本/回放门禁；数据未通过时状态为 `BLOCKED_BY_PIT_DATASET`。
2. 依次验证 HIST-concept、industry+concept 多关系、超图和概念层两层选股，不并行开启全部架构。
3. 若概念关系不能在同风险预算下改善长期趋势捕获或只提高拥挤，则停止模型扩展并保留数据集供解释/风险用途。

## 14. Verification Plan / 验证计划

### 14.1 Business oracle / 业务判定真值

1. 标签真值：horizon=N 必须逐点等于 `close[t+N+1]/close[t+1]-1`；h20 的最后可评估信号日由 close 交易日历反推，不允许未成熟尾部进入 IC。
2. PIT 真值：个股切换行业后，F4/R2 使用“当前行业自身的历史面板”，不能把个股切换前后的两个行业价格串接；同一板块日出现冲突值必须报错。
3. 数据真值：static 输出以 daily-basic 键为基表，`l2_code_id` 为 signed integer，daily-basic-only 键为 `-1`，旧 120 个数据列和 dtype 不回归。
4. 指标真值：RD 计算 key `20d`、区间 label `T21T1` 与 legacy DB row `return_horizon=1d` 三者并存；12 个 h20 字段从 RD engine 经 official writer、router 到 MCP 不丢失，旧 payload 全部补空而不报错。
5. 权威与副作用真值：只有 official evaluation writer 可落表；RD task/loop writer 继续禁用。本批任何测试都不得连接/写生产 DB、应用 DDL、promotion 或重启 runtime。
6. 长期评价真值：hN IC/RankIC 与 hN 标签严格对齐但不替代业务捕获；只有完整 F-014 链路可发布研究决策状态，内部计算/receipt 里程碑不得提前选期限或模型。
7. 可成交性真值：理论 `T+1 close_qfq` 机会与实际成交分层并存；reconciled trade artifact 优先于日线涨跌停推断，无订单/队列证据时为 `NOT_VERIFIABLE`，买入和退出阻断对称。
8. oracle 真值：未来信息只构造不可部署上界；四格、soft gate、成本/可交易口径和阈值在运行前冻结，GO/STOP/INCONCLUSIVE 由置信区间机械判定。

### 14.2 L0–L5 验证映射

| level | 本批/后续验证 | 状态口径 |
|---|---|---|
| L0 | 文档章节、exact field list、SQL named-parameter 与 schema contract、F2 validator、diff check | 本批必须 PASS。 |
| L1 | quick horizon/direction/manifest/HAC、bundle dtype/unknown/schema、F4 PIT/conflict、RD h20 engine/API 单测 | 本批必须 PASS。 |
| L2 | 隔离的 nullable schema/upsert 参数、official summary positional mapping、router/MCP emit/旧 payload 回归 | 本批必须 PASS；不执行生产 DDL。 |
| L3 | 真实 2026-06-30 数据、官方指标/相关性与 QE 因子加载 | 三个因子独立指标/基本相关性和 R6 离线加载已完成；realtime parity 仍 pending。 |
| L4 | GATs/LGBM、融合/两层 oracle 与真实模型、长期趋势、R8M、PIT 关系模型、成本容量、拥挤/尾部、DSR/PBO 与多种子业务流 | R4/R6/R7A/R7B 已完成部分证据，R8A/R8B 运行中；F-014、oracle、R8M、两层、capacity/DSR/PBO 与关系模型继续按 F-010、F-013–F-020 验收。 |
| L5 | 生产 DDL/回填、freshness、candidate → active、服务与 paper/live 运行时验收 | `APPROVED_BY_USER: DEFERRED_TO_PRODUCTION_GATE`。 |

新增/修改业务逻辑的覆盖率目标为 line ≥ 80%、branch ≥ 70%；优先由定向 pytest coverage/CI 记录。若因嵌入式因子代码或外部引擎边界无法可靠计量，必须用上述 business oracle 分支测试补证并在矩阵记录明确例外，不得以全仓平均覆盖率掩盖关键路径。

### 14.3 具体命令与证据

| 层级 | 验证 | 预期证据 |
|---|---|---|
| Design | `python scripts/aistock_feature_workflow.py validate --design ... --tier F2` | F2 PASS，design item 与 matrix 行数一致。 |
| Candidate unit | 生成器 dtype/unknown/schema 测试 | `l2_code_id` 为 int16/int32；NaN→`-1`；非整数/越界 fail-fast；receipt 字段齐全。 |
| Candidate artifact | 在新隔离目录生成 bundle | 行数、日期、股票、板块、known coverage、`-1`、schema 与文件指纹 receipt；active 未改变。 |
| Quick screen unit | horizon=1/20 标签和 HAC 边界测试 | 默认 1d 不变；h20 精确 T+1→T+21；lag=19；不足/退化返回空而非伪值。 |
| RD metrics unit | 引擎/API 序列化测试 | legacy 字段不变，h20 companion fields 数值定义和 nullable 行为正确。 |
| AIstock contract | schema/upsert/router/MCP 定向测试 | 新字段往返，旧 payload 兼容；不连接/修改生产库。 |
| F4 PIT unit | 多行业、多日期、行业切换与重复值测试 | 板块收益仅按板块时序计算；切换不串组；板块日值冲突 fail-fast。 |
| Fusion/two-layer | 同快照预测对齐、四格 oracle/soft gate、风险预算、组合与分层归因 | oracle 永久标记不可部署；阈值预注册；单腿/融合/两层基线可复算；任务级重合与 leave-one-leg-out、暴露和成本齐全。 |
| Long-trend | 20–180 日、有序 barrier、MFE/MAE、生存/右删失、信号→成交/退出阻断测试 | 与 Advisory Phase 8 日期/标签契约一致；Type A/B 不共用标签头；末端未成熟样本不伪装失败；日线触板不伪装订单真值。 |
| Multi-horizon transfer | 独立/共享/冻结迁移/全量微调、per-head mask/purge、transfer matrix/LOO/gradient conflict | 不补零未成熟标签；共享负迁移可见；PCGrad 只在观察到冲突后另立对照；最终裁决引用同一 F-014 vintage。 |
| Relational canary | PIT relation/mapping/stock_index/composer/fit-predict/resource 测试 | 静态快照被拒绝；错位 loud fail；首 loop 完整归档；R4 二值邻接不重复冒充新实验。 |
| Parallel artifact | cache 原子写/锁/损坏重建、recorder 隔离与 restart recovery | 并行任务不互相读到半文件、不覆盖归档；后端重启不终止或重复启动 worker。 |
| Targeted coverage | quick screen + shared h20 contract 的 line/branch coverage | 29 tests；combined coverage 92%，shared contract 100%；F4 嵌入代码以 oracle 两分支补证。 |
| Repository | compile/lint/diff/targeted pytest | 两仓各自通过；已知基线告警与本次新增问题分离。 |
| Baseline authority audit | `test_factor_metrics_authority_static.py` | 14 passed/2 failed；失败均在未修改的 origin/main 文件：4 个既有硬编码本地路径，以及测试引用已不存在的 `MultiAlphaGroupEditor.tsx`。不作为本批成功证据，也不归因于本改动。 |
| External gate | 真实数据 E2E、生产 DDL、promotion、QE | QE 数据、R6/R7 正式结果和 R8A/R8B 运行 receipt 已更新；F-014 DDL、realtime parity、promotion 与 paper/live 继续按 L3/L4/L5 单独授权。 |

## 15. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文 2.3、4.6–4.9、8、18 | ledger 字段/存储契约、候选族与研究来源已冻结；本批未运行候选公式 | VERIFIED | 无 |
| F-002 | RD-Agent `tools/generate_static_factors_bundle.py` | 9 项 unit；7,304,119 行 candidate receipt；parquet/schema SHA-256 | VERIFIED | 无 |
| F-003 | AIstock `scripts/quick_ic_screen.py` | horizon/label/direction/manifest/HAC/CLI 单测 20 passed；与 shared contract 合计 coverage 92% | VERIFIED | 无 |
| F-004 | RD-Agent metrics engine 与 SOTA API | h20 engine/API 2 passed；与 bundle 合计 11 passed | VERIFIED | 无 |
| F-005 | AIstock migration、`factor_metrics_contract.py`、official writer、routers/MCP | contract 9 passed；contract+MCP/emit 51 passed；official batch 26 passed/1 skipped | VERIFIED | 无 |
| F-006 | AIstock `scripts/p1_new_factors.py` F4/R2 tracked repair source | PIT 当前行业历史与冲突 fail-fast 2 passed；旧 catalog 口径失效已记录 | VERIFIED | 无 |
| F-007 | 后续候选 asset/realtime loader | 三个候选已登记并由 R6 QE 离线执行；catalog asset/transformation 状态复核 | APPROVED_BY_USER: PARTIAL_QE_ASSET_READY | catalog 1525/1528/1532 `is_available=true` 且 QE 可运行；`asset_status/transformation_status=pending/PENDING`、`realtime_code_text` 为空，离线/实时 parity 与荐股/模拟盘加载仍未验收。 |
| F-008 | 因子库 MCP + Stage 0/3 | 2026-06-30 官方 h20 批次；每因子 583 个基本相关性 pair；第 3.1 节 | APPROVED_BY_USER: PARTIAL_OFFICIAL_METRICS_CORR_COMPLETE | 独立指标和基本相关性已完成；板块原生层、partial/residual IC、完整分类/去重处置仍按候选门禁后置。 |
| F-009 | Stage 1/3 + portfolio evaluator | R6/R7A/R7B 已有成本后回测；HAC/bootstrap/DSR/PBO/cost/capacity/crowding 设计与 L4 oracle | APPROVED_BY_USER: PARTIAL_QE_BACKTEST_COMPLETE | 已有统一回测但未完成多规模 capacity、冲击/拥挤、DSR/PBO 和完整 portfolio fusion；不得 promotion。 |
| F-010 | QE GATs/LGBM experiment specs | R4 二值邻接、R6 30 Loop、multi-seed OOS 与第 9.4.2 节 | APPROVED_BY_USER: PARTIAL_R6_COMPLETE | 2×2/真码邻接和 R6 LGBM/GAT 多种子已完成；长期标签、两层模型和完整 L4 成本容量验收仍 pending。 |
| F-011 | 两仓隔离 worktree 与第 17 节 | active/旧 candidate/production DB/DDL/runtime 均未修改 | VERIFIED | 无 |
| F-012 | 两仓定向测试、lint/compile/diff 与 F2 validation | AIstock 99 passed/1 skipped；RD-Agent 11 passed；F2 PASS；authority 14 passed/2 个 origin/main 既有失败已分离；PR/merge 分离 | VERIFIED | 无 |
| F-013 | 本文 4.10、9.4–9.5、11.6、Phase G0-E | R6 同口径 prediction receipt；R7A/R7B 正式回测；固定风险预算、长期成本后组合与 leave-one-leg-out | APPROVED_BY_USER: PARTIAL_FORMAL_BACKTEST_COMPLETE | R7A/R7B 均成功但 Sharpe/Calmar 低于 LGBM，只证伪当前 0.5/0.5 rank/zscore prediction-fusion；portfolio fusion、跨标签组合、完整任务级 LOO、容量和两层模型仍 pending。 |
| F-014 | 本文 9.6、11.6、Phase G0-E；`qe_long_trend_evaluation_f2_design_20260714.md`；Advisory Phase 8 | 20–180 日标签基础架构；R8A/R8B receipt；有序 barrier、MFE/MAE、time-to-hit、生存/删失、capture/false-exit；信号→成交/退出阻断；双快照、QE-only CAS/三表/API/MCP/UI | APPROVED_BY_USER: DESIGN_AMENDED_BLOCKING_GATE | 30–180D 标签与 maturity purge 已实现；R8A/R8B 已运行。F2 evaluator 完整链仍 pending；`CORE_COMPUTE_VERIFIED`/`PRELIMINARY_CAPTURE_AVAILABLE` 不能替代 `F014_RESEARCH_DECISION_READY`，当前结果只能 preliminary。 |
| F-015 | 本文 4.10、9.7、Phase G0-F | R4 真码邻接 receipt；未来 HIST PIT relation、mapping 对齐、composer/resource canary 和逐关系消融 | APPROVED_BY_USER: DEFERRED_TO_POST_R6 | 二值同业邻接已测试且 RankIC 无增益；动态/层次关系尚未实现。 |
| F-016 | 本文 4.10、9.8、Phase G0-G | 概念 PIT 数据设计、成员变更/多成员/版本/回放验收，随后才有 HIST-concept/HATS/超图证据 | APPROVED_BY_USER: DEFERRED_TO_CONCEPT_DATASET | 当前概念成员 PIT 数据集未入库，不允许静态快照或聚合板块数据替代。 |
| F-017 | 本文 9.9、Phase G0-F、14.3 | 模型资源分类、cache/recorder 隔离、并行制品、combine-backtest 与 restart recovery | APPROVED_BY_USER: PARTIAL_RUNTIME_VERIFIED | R6、R7A/R7B 证明组合路径可运行；R8A 已按远端 CPU 4 并行、R8B 已按标准 GPU 模型 2 并行启动。R8B 节点入口离线后经正式 Results API 恢复与 probe 成功且无降级；全部 Loop 归档、失败恢复和 F-014 重启恢复仍需继续留证。 |
| F-018 | 本文 9.5、9.9、14 | 四格 oracle、soft gate、预注册阈值/置信区间、PIT/成本/可交易同口径、不可部署标记 | APPROVED_BY_USER: DESIGN_READY_RUN_PENDING | 尚未运行；设计/数据准备可并行，正式 go/stop 等 F-014；不得作为可部署 Alpha 证据。 |
| F-019 | 本文 9.6.3、9.9、14 | R8M 四臂、per-head maturity/purge、transfer matrix、LOO、梯度冲突和 F-014 评价 | APPROVED_BY_USER: DESIGN_PLANNED | 尚未创建实验；先 wiring canary，不预设正迁移，完整多种子和裁决后置。 |
| F-020 | 本文 9.6.4、9.9、14 | 预测/持仓/入场/板块/P&L/右尾事件/成本重合和固定风险预算 LOO | APPROVED_BY_USER: DESIGN_READY_GATE_PENDING | 长周期腿尚未选出；不同 horizon 或模型名不自动形成独立腿，R8C 在 F-014 和任务级增量门禁前保持 blocked。 |

## 16. Rollout / Rollback / 发布回滚

- Gate-0/运行时状态：基础代码、2026-06-30 QE 数据、长标签支持和 R7 combine-backtest 修复已经分别完成；合并、数据部署、实验成功和生产启用仍是不同事实。
- v4.6 文档 rollout：在 v4.5 receipt 基础上收敛 F-014 决策门、可成交性桥接、四格 oracle、R8M 和任务级 Alpha 准入；本次文档变更自身不创建/停止任务，不修改代码、DB、数据或生产运行时，无需服务重启。R8A/R8B 继续按原卡运行，任何后续实验仍需独立授权。
- Schema rollout：现有 factor h20 指标已可用；未来 F-014 三表必须通过版本化 migration 和独立 DDL 授权，依赖既有每日备份，不在 DDL 前额外导出数据库。
- Data rollout：R8 默认继续冻结 2026-06-30 QE 快照；任何新快照另立 dataset identity 并保留上一版本回滚，不影响非 QE PIT/模拟盘数据。
- Rollback：文档按 PR revert；未来 evaluator/schema 可停止新写入并保留历史 receipt，数据回切上一版本；任何回滚不得删除试验台账、预测或评价制品。
- Runtime rollback：本文不触发运行时动作；未来 F-014/R8 实现必须另写启动前检查、QE-only zero-impact 与恢复步骤。

## 17. Production Gates / 生产门禁

| gate | 本批状态 | 放行条件 |
|---|---|---|
| source merge | GATE0_AND_RUNTIME_FIXES_MERGED | Gate-0、长标签基础架构和 R7 combine-backtest 修复已进入 main；本 v4.6 蓝图收敛更新仍需单独提交/合入。 |
| QE dataset | VERIFIED_20260630 | 当前 QE 快照已支持 R6/R7，并被 R8A/R8B 继续冻结复用；未来数据切换继续要求版本化快照和回滚保留。 |
| factor asset | RESEARCH_AVAILABLE_ONLY | catalog 1525/1528/1532 可供 QE；realtime transformation/parity 完成前不得进入荐股、模拟盘或生产交易。 |
| production_ddl_gate | PARTIAL_EXISTING_SCHEMA_READY | 现有 factor h20 指标可读写；F-014 三张 additive evaluation 表的 migration 尚未实现/应用，必须走独立 feature/DDL 门禁。 |
| production_db_write_gate | OFFICIAL_RESEARCH_WRITES_COMPLETE | 官方因子指标/相关性和 QE archive 已持久化；本 v4.6 文档任务不写 DB，RD task/loop 非官方 factor writer 继续禁用。 |
| active_promotion_gate | NOT_AUTHORIZED | 研究可用不等于生产 promotion；需 realtime asset/parity、长期评价和策略包门禁。 |
| production_frontend_dependency_gate | noop | 本批无前端/lockfile 变化。 |
| production_backend_dependency_gate | noop | 本批无依赖/lockfile 变化。 |
| candidate_freshness_gate | PASSED_FOR_QE_20260630 | R6/R7 已锁定 2026-06-30；R8 继续复用该快照以保证对比，除非另立数据版本实验。 |
| QE experiment | R6_R7_COMPLETED_R8_RUNNING | R7A/R7B 均完成且未晋级；R8A/R8B 运行中；两层模型与长期 evaluator 结果按第 9.9 节推进。 |
| service/runtime restart | NOT_REQUIRED_FOR_DOCS | 本文更新不修改运行时代码或配置；未来 F-014 实现另行决定重启窗口。 |
| paper/live trading | NOT_ENABLED | 不属于本规格自动动作。 |

## 18. Research Sources / 一手研究来源

以下来源只用于形成 Gate-0 研究先验与统计治理，不能替代 A 股、PIT、成本后样本外证据：

- [Harvey, Liu & Zhu, “…and the Cross-Section of Expected Returns”](https://www.nber.org/papers/w20592)：因子动物园、多重检验与更高发现阈值。
- [Harvey, Sancetta & Zhao, “What Threshold Should be Applied to Tests of Factor Models?” (2026)](https://www.nber.org/papers/w34898)：依赖检验、原假设分布、样本选择与 local FDR；`t≈3` 仅作治理参考。
- [Bailey et al., Probability of Backtest Overfitting](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253) 与 [Bailey & López de Prado, Deflated Sharpe Ratio](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551)：PBO/DSR 与选择偏差治理。
- [Research Affiliates, A Backtesting Protocol in the Era of Machine Learning](https://www.researchaffiliates.com/insights/journal-papers/702-a-backtesting-protocol-in-the-era-of-machine-learning)：其受保护测试集、经济逻辑与可复制协议形成本文预注册/untouched test 的实施推论，并非论文直接规定 AIstock 字段。
- [López de Prado, K-Fold CV with Purging & Embargo / CPCV 方法索引](https://www.quantresearch.org/Innovations.htm)：形成 h20 split manifest、purge 与 embargo 契约的实施依据。
- [Newey & West](https://www.nber.org/papers/t0055) 与 [Politis & Romano stationary bootstrap](https://doi.org/10.1080/01621459.1994.10476870)：重叠 h20 的自相关稳健推断和时间序列重采样。
- [Moskowitz & Grinblatt, Do Industries Explain Momentum?](https://onlinelibrary.wiley.com/doi/10.1111/0022-1082.00146)、[Ehsani & Linnainmaa, Factor Momentum](https://www.nber.org/papers/w25551) 与 [Hou, Industry Information Diffusion](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=463005)：行业动量、因子持续性和信息扩散的控制基线。
- [Campbell & Lettau, Dispersion and Volatility](https://www.nber.org/papers/w7144) 与 [Barberis, Shleifer & Wurgler, Comovement](https://www.nber.org/papers/w8895)：区分行业/个股离散度、波动与非基本面共振。
- [Frazzini, Israel & Moskowitz, Trading Costs](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3229719)：机构级交易成本、冲击和规模依赖。
- [Zaremba et al., Herding for profits: Market breadth and the cross-section of global equity returns](https://www.sciencedirect.com/science/article/pii/S0264999319312982)：论文使用上涨股减下跌股类 breadth，只支持“成员参与值得检验”的先验，不直接验证 MA20 breadth。
- [MSCI Integrated Factor Crowding Model](https://www.msci.com/research-and-insights/paper/msci-integrated-factor-crowding-model) 与 [Lazo-Paz, Moneta & Chincarini](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4618248)：其多维拥挤框架形成本文持仓、资金流、成本与尾部风险联合审计的实施推论，不声称复刻 MSCI 模型。
- [Novy-Marx, Backtesting Strategies Based on Multiple Signals](https://www.nber.org/papers/w21329) 与 [Gu, Kelly & Xiu, Empirical Asset Pricing via Machine Learning](https://www.nber.org/papers/w25398)：组合信号的选择偏差、非线性交互与模型增量。
- [Shin, 2026 preprint](https://arxiv.org/abs/2606.19550)：测试资产构造可能改变模型排名；仅作为前沿敏感性提示，不作为已确立结论。

内部设计锚点（用于约束 AIstock 实现，不替代一手论文证据）：

- `docs/analysis/p2_relational_model_hist_master_feasibility_20260708.md`：HIST/MASTER/IGMTF/TRA 的早期接入评估；其中静态关系快照建议已由本规格第 4.10 节取代。
- `docs/architecture/qe_efficient_gats_l2_industry_embedding_f1_design_20260710.md`：真实 PIT 申万 L2 provider、embedding 与 industry bias 的同源契约。
- `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` Phase 8：Type B 长期趋势的多期限、有序目标、生存、MFE/MAE 与捕获率口径。
- `docs/analysis/sector_rotation_factors_batch_e_plan_20260711.md`：当前板块因子批次的后续候选与执行衔接。
