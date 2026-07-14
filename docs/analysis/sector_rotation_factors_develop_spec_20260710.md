# 板块轮动因子研发规格：候选池、去重与 h20 验收

- 文档类型：F2 因子研发规格 / Gate-0 开发指引（`develop-factor`）
- 主线：板块轮动（sector rotation）——让模型显式理解板块归属、轮动速度、成员参与度与板块内结构
- 初版日期：2026-07-10
- 当前版本：v4.1（post-R6 研究路线与历史融合 canary receipt，2026-07-14）
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
6. 在完成当前因子/embedding 归因后，按“低成本组合验证 → 两层板块选股 → PIT 关系模型 → 概念多关系图”的顺序研究长期上涨趋势 alpha；每一级都必须有独立基线、冻结实验卡和停止条件。

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

第 2.3、11.1、Phase G0-A–G0-C、15–17 节保留 Gate-0 当时的交付与门禁证据，不把历史 receipt 改写成当前运行状态。v4 新增的第 4.10、9.4–9.9、11.6 与 Phase G0-E–G0-G 是 post-R6 研究方向：当前 QE 运行、数据集、因子库、生产 DB 和运行时不因本次文档更新发生变化。后续每个实验必须引用 F-013–F-017，并在创建任务前把数据快照、预测资产、种子、切分、资源类和并行策略写入实验卡。

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

### 7.1 当前数据状态

截至本规格修订时的只读核验：

| 数据位置 | `sector_data.h5` | `static_factors.parquet` | 结论 |
|---|---|---|---|
| active `factor_implementation_source_data` | 22 个 `sw2_*` 字段，无 `l2_code_id` | 122 列，无 `l2_code_id` | 不能用于 A1–A6/B1/B2/N1 的正式离线验证。 |
| candidate `factor_implementation_source_data_20260428_candidate` | 23 列，含 `l2_code_id` | 120 个数据列，无 `l2_code_id` | `sector_data.h5` 已满足；旧 bundle 缺离散行业键。 |
| Gate-0 隔离产物 `gate0_sector_factor_candidate_20260711` | 复用上述 23 列 candidate | 121 个数据列；旧 candidate 的 120 个数据列全部保留并新增 `l2_code_id=int16` | 已完成物理/schema/指纹验证；仍为 gitignored candidate，未 promotion。 |

2026-07-11 Gate-0 实测审计：candidate `sector_data.h5` 共 7,334,829 行、1,876 个交易日、4,691 只股票，日期为 2018-08-01 至 2026-04-28，131 个已知板块，源表 `l2_code_id` 覆盖率 100%。旧生成器会把所有列统一转为 `float32` 且遗漏 `margin_detail.h5`，因此旧 candidate bundle 不具备离散类别键语义。修复后隔离生成产物为 7,304,119 行、4,691 只股票、1,876 日：7,303,993 行为已知板块，126 行显式为 unknown `-1`，known coverage 为 99.99827494595858%，取值范围 `[-1,133]`，共 131 个已知板块；旧 120 个数据列全部保留，共同字段 dtype 无变化，只新增 `l2_code_id=int16`。`static_factors.parquet` SHA-256 为 `FE91FA9C519F4FD501D5E979F03B604C66F3904387B48C0E982D8366747D60A6`；schema JSON/CSV SHA-256 分别为 `04252DD8E8941CDD8018885B1BBBE95F4C606FBAEE49C61BAB6E1986DFFF5DFE`、`D193BDBF4B003291B5FD708A1D420FF14E6526C3473F5E786F869889B81B6FD6`。产物仍在任务 worktree 的 gitignored 目录，未修改旧 candidate、active 或数据库。

输出以唯一的 `daily_basic` 索引为左连接基表：sector 有 7,334,829 个唯一键，daily-basic 有 7,304,119 个唯一键，交集 7,303,993；因此丢弃 30,836 个 sector-only keys，并将 126 个 daily-basic-only keys 的 `l2_code_id` 写为 `-1`，净行数差为 `30,836 - 126 = 30,710`。这不是随机丢行，必须随 snapshot receipt 保留。

该 candidate 是截至 2026-04-28 的冻结研究快照，不代表 2026-07-11 的当前生产新鲜度。`daily_pv.close` 同样覆盖 2018-08-01 至 2026-04-28；按 T+1→T+21，最后可评估 signal date 为 2026-03-27（T+1=2026-03-30，T+21=2026-04-28）。因此 h20 的 `recent_1m/3m/6m` 均相对 `last_evaluable_signal_date=2026-03-27` 定义，不相对 wall-clock，也不相对未成熟的 2026-04-28 特征尾部；2026-03-30 至 2026-04-28 只能用于特征/data freshness。QE 或 promotion 前必须通过独立 freshness gate 刷新并重算该日期。

### 7.2 必须完成的数据 gate

1. 修复 `generate_static_factors_bundle.py`：连续因子可下转 `float32`，`l2_code_id` 必须跳过浮点转换、校验整数/范围、连接缺失填 `-1`，并使用有符号 `int16/int32`。
2. 使用 candidate `sector_data.h5` 在任务 worktree 运行修复后的生成器，输出到新的、gitignored 的隔离 candidate 目录；不得覆盖 active 或旧 candidate。
3. 验证 `static_factors.parquet` 包含 `l2_code_id`、整数 dtype、取值范围、未知语义；schema 必须标记 `source=sector_data_raw`、`semantic_type=categorical_id`，receipt 必须报告 `known_coverage`、known sector 数和 `-1` 数量。
4. 记录数据快照指纹、日期范围、行数、股票数、板块数、逐日覆盖率和最小/中位成员数。
5. 在同一 candidate 快照上生成离线因子结果；禁止 sector、price、basic 等数据混用不同截点。
6. candidate → active/production promotion 必须由用户单独确认；本因子研发流程不隐式执行。

QE DB loader 已能返回 `l2_code_id`，但离线因子源未闭环前不能宣称完整研发链可用。自动 transformation/review 提示也必须显式列出 `l2_code_id`，避免 loader 实际支持而转换器错误拒绝或遗漏。

GATs embedding 在研究期可以使用同一实验内稳定映射；进入模拟盘/实时前，必须统一 embedding 侧和导出侧 `industry_code_map`，并验证未知值、增量新行业和重启后的映射稳定性。

## 8. 研发流程

### Stage 0：预检与去重

1. 数据 gate 全部通过。
2. 在任何公式运行前建立 append-only `trial_id` 台账；公式、窗口、方向、阈值、种子、切分及失败版本均计入，按 `{A1,A2,A4}`、`{A3,B2,R1,R2}`、`{A5,A6}`、`{B1,N1}` 管理相关候选族，N1 即使 KILL 也不得删除记录。
3. 用因子 MCP 对名称、描述、公式和同族因子定向搜索；搜索摘要必须下钻到明确窗口指标。
4. 对复用基线读取代码与 out-sample 指标，禁止换名重复开发；A2/A3/A4/B2 同时冻结其 partial/residual IC 控制集。
5. 为每个新候选写入预注册卡：公式、字段、窗口、方向假设、最小成员数、缺失值规则、主要相关性对照、成本/容量重点和 STATE 交互（如适用）。

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

### 9.5 两层板块轮动模型

建立一个可解释的 top-down 对照，不直接从全市场一次性挑 Top-K：

1. 板块层使用等权板块面板及 A1–A6 中通过门禁的因子，对申万 L2 板块做 20/40/60 日趋势、广度、资金与状态评分，先选 Top-M 板块；
2. 个股层只在入选板块内，使用长期趋势因子、板块内 leadership 和流动性/可交易性选择股票；
3. 行业不做标签中性化，但组合层必须记录单板块上限、板块集中度和轮动成本；
4. 与相同候选池/Top-K/成本预算的一层 LGBM、GATs 和简单“板块动量 + 板块内动量”规则基线比较。

板块层和个股层分别归档分数、入选原因和淘汰原因。若板块 Recall 提升但个股收益不提升，应归因为板块内排序问题；若板块层本身无增量，则不进入更复杂的层次图模型。

### 9.6 长期上涨趋势专用评价

h20 继续作为当前模型对照的统一信号标签，但它不能单独代表“连续上涨数月、捕获右尾大行情”的策略目标。post-R6 结果必须与 `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` 的 Phase 8 口径对齐，增加：

- 20/40/60/120/180 个交易日收益画像和按日期聚类的置信区间；
- `+30%/+50%/+70%` 有序目标的 Recall@20/50、到达概率与 time-to-hit calibration；只有独立深池契约获批后才评估 Recall@100；
- trend-stage survival、右删失、趋势失效、峰前回撤、MFE/MAE、trend capture ratio 与 false early-exit rate；
- 最近 1 年、最近 6 个月及科技抱团/板块集中等预注册 regime 切片，但不得用切片结果反向选择全期公式；
- 持仓超过 30 日不设硬约束，由信号存活、趋势失效和成本后收益决定。

Type A 超跌反弹与 Type B 长期趋势保持独立因子选择、标签头、调仓和退出逻辑；旧多 Alpha 腿只能作为组合相关性/风险基线，不能作为 Type B 演进母体。

### 9.7 PIT 关系模型路线

1. **HIST-industry canary**：基于 `market.sw_index_member` 生成逐日 PIT `stock2concept`/稀疏成员矩阵，补齐 composer 分支、`stock_index` 对齐、每日截面 batch、fit/predict、归档与回测契约。第一轮只验证 wiring、资源和同口径基线；禁止静态行业快照。
2. **动态加权/多关系图**：在 HIST-industry 或当前 GATs 显示结构增量后，分别测试只用历史窗口构造的 residual co-movement、leadership、flow/state 权重；边类型分头处理并做逐关系消融。R4 的真码二值 `industry_bias` 是已完成负/混合证据，不再重复。
3. **HATS/层次关系**：只有两种及以上 PIT 关系分别显示增量后才进入，避免仅用单一行业边却包装成多关系模型。
4. **MASTER/IGMTF/TRA**：MASTER 自研和市场状态特征管线成本高，后置到关系增量成立之后；IGMTF/TRA 可作较低成本 canary，但必须明确它们不是 MASTER 的等价实现。既有时间维 Transformer RankIC 近零，不再重复等价架构。

### 9.8 概念板块与超图路线

概念关系频繁新增、同一股票同时属于多个概念，必须先完成独立的数据设计与 PIT 验收：关系记录至少包含 `concept_id`、`instrument`、`in_date`、`out_date`、source/version、采集/公告可用时点和变更原因；板块标识不得依赖易变名称，开放区间和同日多成员关系必须可复算。数据完整性、历史覆盖、变更捕获和退市/更名语义通过后，再按以下顺序研究：

1. HIST-concept，对比 HIST-industry，验证细粒度题材关系是否增加信号；
2. industry + concept 多关系 HATS/GAT，做逐关系和交互消融；
3. 概念超图或多头关系注意力，直接表示一股多概念，不复制样本、不把多个概念压成单码；
4. 概念层板块评分 → 概念内龙头选择的两层模型。

概念数据集未入库前，上述项目状态统一为 `BLOCKED_BY_PIT_DATASET`，不得用 `sina_board_daily` 的板块聚合或当前成分列表伪造历史成员。

### 9.9 执行顺序与资源门禁

post-R6 的默认顺序为：R6 同口径收口 → GATs+LGBM 融合 → 两层板块模型 → 长期趋势画像 → HIST-industry canary → 动态多关系图 → 概念 PIT 数据与概念模型 → MASTER/IGMTF/TRA 条件探针。低成本、复用预测的实验先于新架构训练。

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

- 历史 GATs/LGBM prediction-fusion canary receipt：预测/标签逐行对齐、正交性、Top25 重合、预冻结等权 rank/zscore 与信号级 h20 结果已完成；正式 R6 同口径组合回测仍 pending；
- GATs/LGBM 同 seed、同 split 预测对齐 receipt，以及 prediction/portfolio fusion 的权重冻结、正交性、成本后组合和 leave-one-leg-out 报告；
- 两层板块→个股模型的板块评分、板块 Recall、板块内排序、集中度/轮动成本及一层模型对照；
- 与 Advisory Phase 8 对齐的 20–180 日、MFE/MAE、有序目标、time-to-hit、生存、右删失、捕获率和假退出报告；
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
| F-013 | 组合与两层决策增量 | 同口径 GATs+LGBM prediction/portfolio fusion 和板块→个股两层基线；冻结权重/风险预算，报告正交性、成本、容量、暴露与 leave-one-leg-out。 |
| F-014 | 长期趋势目标一致性 | h20 对照之外，按 Advisory Phase 8 报告 20–180 日、有序右尾目标、MFE/MAE、time-to-hit、生存、右删失、捕获率和假退出；Type A/B 标签与生命周期隔离。 |
| F-015 | PIT 关系模型 | HIST-industry、动态加权图和多关系图只消费逐日 PIT 关系；mapping/stock_index fail-fast；R4 二值真码邻接不重复；新架构先通过 composer/资源 canary。 |
| F-016 | 概念多关系前置门禁 | 概念成员先完成多成员、有效区间、可用时点与 source version 的 PIT 数据设计/验收；通过后才允许 HIST-concept、HATS/多关系图或超图实验。 |
| F-017 | 研究调度与制品隔离 | 模型类决定串并行；共享 cache 原子写/锁/损坏重建和 recorder 隔离先验收；后端重启不终止或重复启动已运行 worker。 |

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

1. 冻结 R6 完整归档，核对同 seed/同 split/同数据快照的 GATs、LGBM 和关系开关配对；失败/不完整 loop 不进入择优。
2. 先用已归档预测执行第 9.4 节融合，再执行第 9.5 节两层板块→个股基线；两项均不以扩大风险预算换取收益。第 9.4.1 节历史信号 canary 已完成，只证明管线与排序增量；正式 R6 portfolio fusion 仍待同口径预测完整归档和节点容量释放。
3. 对单腿与组合统一补第 9.6 节长期趋势指标，确认 h20 强度是否能转化为 60–180 日右尾捕获；不能转化则回到因子/标签而不是继续堆模型容量。

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

### 14.2 L0–L5 验证映射

| level | 本批/后续验证 | 状态口径 |
|---|---|---|
| L0 | 文档章节、exact field list、SQL named-parameter 与 schema contract、F2 validator、diff check | 本批必须 PASS。 |
| L1 | quick horizon/direction/manifest/HAC、bundle dtype/unknown/schema、F4 PIT/conflict、RD h20 engine/API 单测 | 本批必须 PASS。 |
| L2 | 隔离的 nullable schema/upsert 参数、official summary positional mapping、router/MCP emit/旧 payload 回归 | 本批必须 PASS；不执行生产 DDL。 |
| L3 | 用真实隔离 candidate 完成 RD engine → AIstock 非生产库/接口端到端并核对 12 字段 | `APPROVED_BY_USER: DEFERRED_TO_G0_D`，开始实际候选研发时执行。 |
| L4 | GATs/LGBM、融合/两层模型、长期趋势、PIT 关系模型、成本容量、拥挤/尾部、DSR/PBO 与多种子业务流 | `APPROVED_BY_USER: DEFERRED_TO_G0_D_OR_POST_R6`；按 F-010、F-013–F-017 独立验收。 |
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
| Fusion/two-layer | 同快照预测对齐、风险预算、组合与分层归因 | 权重只在 validation 冻结；单腿/融合/两层基线可复算；leave-one-leg-out、暴露和成本齐全。 |
| Long-trend | 20–180 日、有序 barrier、MFE/MAE、生存/右删失测试 | 与 Advisory Phase 8 日期/标签契约一致；Type A/B 不共用标签头；末端未成熟样本不伪装为失败。 |
| Relational canary | PIT relation/mapping/stock_index/composer/fit-predict/resource 测试 | 静态快照被拒绝；错位 loud fail；首 loop 完整归档；R4 二值邻接不重复冒充新实验。 |
| Parallel artifact | cache 原子写/锁/损坏重建、recorder 隔离与 restart recovery | 并行任务不互相读到半文件、不覆盖归档；后端重启不终止或重复启动 worker。 |
| Targeted coverage | quick screen + shared h20 contract 的 line/branch coverage | 29 tests；combined coverage 92%，shared contract 100%；F4 嵌入代码以 oracle 两分支补证。 |
| Repository | compile/lint/diff/targeted pytest | 两仓各自通过；已知基线告警与本次新增问题分离。 |
| Baseline authority audit | `test_factor_metrics_authority_static.py` | 14 passed/2 failed；失败均在未修改的 origin/main 文件：4 个既有硬编码本地路径，以及测试引用已不存在的 `MultiAlphaGroupEditor.tsx`。不作为本批成功证据，也不归因于本改动。 |
| External gate | 真实数据 E2E、生产 DDL、promotion、QE | `APPROVED_BY_USER: DEFERRED`；按 L3/L4/L5 分层，只有单独授权和 receipt 后更新。 |

## 15. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文 2.3、4.6–4.9、8、18 | ledger 字段/存储契约、候选族与研究来源已冻结；本批未运行候选公式 | VERIFIED | 无 |
| F-002 | RD-Agent `tools/generate_static_factors_bundle.py` | 9 项 unit；7,304,119 行 candidate receipt；parquet/schema SHA-256 | VERIFIED | 无 |
| F-003 | AIstock `scripts/quick_ic_screen.py` | horizon/label/direction/manifest/HAC/CLI 单测 20 passed；与 shared contract 合计 coverage 92% | VERIFIED | 无 |
| F-004 | RD-Agent metrics engine 与 SOTA API | h20 engine/API 2 passed；与 bundle 合计 11 passed | VERIFIED | 无 |
| F-005 | AIstock migration、`factor_metrics_contract.py`、official writer、routers/MCP | contract 9 passed；contract+MCP/emit 51 passed；official batch 26 passed/1 skipped | VERIFIED | 无 |
| F-006 | AIstock `scripts/p1_new_factors.py` F4/R2 tracked repair source | PIT 当前行业历史与冲突 fail-fast 2 passed；旧 catalog 口径失效已记录 | VERIFIED | 无 |
| F-007 | 后续候选 asset/realtime loader | 2.3、5、13 G0-D 与 L3 parity/fail-fast 验证契约 | APPROVED_BY_USER: DEFERRED_TO_G0_D | 用户明确批准 2026-07-11 本批仅执行 Gate-0 前置批次；实际候选双资产在 G0-D 验收。 |
| F-008 | 因子库 MCP + Stage 0/3 | 本批 R1/R2 MCP receipt；后续双层相关性/partial IC 契约 | APPROVED_BY_USER: DEFERRED_TO_G0_D | 用户明确批准 2026-07-11 本批不运行候选公式；完整查重随各 trial 执行。 |
| F-009 | Stage 1/3 + portfolio evaluator | HAC/bootstrap/DSR/PBO/cost/capacity/crowding 设计与 L4 oracle | APPROVED_BY_USER: DEFERRED_TO_G0_D | 用户明确批准 2026-07-11 将真实候选与组合持仓验证后置 G0-D。 |
| F-010 | QE GATs/LGBM experiment specs | 2×2、STATE、multi-seed OOS 与 L4 契约 | APPROVED_BY_USER: DEFERRED_TO_G0_D | 用户明确批准 2026-07-11 本批不启动 QE。 |
| F-011 | 两仓隔离 worktree 与第 17 节 | active/旧 candidate/production DB/DDL/runtime 均未修改 | VERIFIED | 无 |
| F-012 | 两仓定向测试、lint/compile/diff 与 F2 validation | AIstock 99 passed/1 skipped；RD-Agent 11 passed；F2 PASS；authority 14 passed/2 个 origin/main 既有失败已分离；PR/merge 分离 | VERIFIED | 无 |
| F-013 | 本文 4.10、9.4–9.5、11.6、Phase G0-E | 融合/两层实验卡、同口径 prediction receipt、固定风险预算、长期成本后组合与 leave-one-leg-out | APPROVED_BY_USER: PARTIAL_CANARY_COMPLETE | 用户于 2026-07-14 明确批准执行并写入历史融合 canary；两腿 prediction fusion 已完成全量对齐、正交性、等权 rank/zscore 和信号级 h20 receipt。未创建 QE 实验，未执行 portfolio fusion；正式 R6 同口径配对、成本后组合、容量、风险预算和 leave-one-leg-out 仍 pending。 |
| F-014 | 本文 9.6、11.6、Phase G0-E；Advisory Phase 8 | 20–180 日、有序 barrier、MFE/MAE、time-to-hit、生存/删失、capture/false-exit 报告 | APPROVED_BY_USER: DEFERRED_TO_POST_R6 | 当前 h20 实验不能替代长期趋势验收；需独立标签成熟度和 OOS 证据。 |
| F-015 | 本文 4.10、9.7、Phase G0-F | R4 真码邻接 receipt；未来 HIST PIT relation、mapping 对齐、composer/resource canary 和逐关系消融 | APPROVED_BY_USER: DEFERRED_TO_POST_R6 | 二值同业邻接已测试且 RankIC 无增益；动态/层次关系尚未实现。 |
| F-016 | 本文 4.10、9.8、Phase G0-G | 概念 PIT 数据设计、成员变更/多成员/版本/回放验收，随后才有 HIST-concept/HATS/超图证据 | APPROVED_BY_USER: DEFERRED_TO_CONCEPT_DATASET | 当前概念成员 PIT 数据集未入库，不允许静态快照或聚合板块数据替代。 |
| F-017 | 本文 9.9、Phase G0-F、14.3 | 模型资源分类、cache/recorder 隔离、并行制品和 restart recovery 定向验证 | APPROVED_BY_USER: DEFERRED_TO_POST_R6 | 本次只冻结调度门禁，不修改正在运行的 QE、worker 或并行配置。 |

## 16. Rollout / Rollback / 发布回滚

- Rollout：本批准备并提交向后兼容的代码能力与设计文档 PR；candidate artifact 留在隔离、gitignored 目录。AIstock 与 RD-Agent 分仓 PR、分仓验证，merge 仍需明确授权，不能把“PR 已提交”写成“已合入”。
- Schema rollout：新增列全部 nullable；生产 DDL 由独立变更窗口执行，先备份/演练，再迁移、回填与查询验收。本批仅交付迁移能力，不执行。
- Data rollout：只有 candidate receipt 通过且用户单独批准，才允许 candidate → active；必须原子切换路径/配置并保留上一 active 快照。
- Rollback：代码按各仓 PR revert；schema 新列可先停止写读而不立即 drop；active 数据回切上一快照；任何回滚不得删除试验台账或验证 receipt。
- Runtime rollback：本批没有重启或启用动作，因此不存在需要执行的运行时回滚；若后续启用，必须另写启动前检查与恢复步骤。

## 17. Production Gates / 生产门禁

| gate | 本批状态 | 放行条件 |
|---|---|---|
| source merge | PENDING | 两仓 PR 审查、定向验证与设计矩阵完成；合并需按用户授权。 |
| candidate artifact | VERIFIED_ISOLATED | F-002 receipt、隔离路径与 SHA-256 已复核；尚未 promotion。 |
| production_ddl_gate | pending | 已交付 nullable additive migration，但未应用；需独立批准、备份/演练、迁移与回填计划。 |
| production_db_write_gate | not_performed | 仅后续因子 promotion/回填任务可单独授权；RD task/loop 非官方 writer 继续禁用。 |
| active_promotion_gate | not_performed | candidate 完整性、新鲜度、离线结果和用户单独确认。 |
| production_frontend_dependency_gate | noop | 本批无前端/lockfile 变化。 |
| production_backend_dependency_gate | noop | 本批无依赖/lockfile 变化。 |
| candidate_freshness_gate | pending | QE/promotion 前刷新到批准的 as-of date，并记录 close label end 与 `last_evaluable_signal_date`。 |
| QE experiment | NOT_STARTED | F-001–F-009 证据与独立实验卡批准。 |
| service/runtime restart | NOT_PERFORMED | 后续部署窗口与健康检查批准。 |
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
