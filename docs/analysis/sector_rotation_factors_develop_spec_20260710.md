# 板块轮动因子研发规格：候选池、去重与 h20 验收

- 文档类型：因子研发规格 / 开发指引（`develop-factor`）
- 主线：板块轮动（sector rotation）——让模型显式理解板块归属、轮动速度、成员参与度与板块内结构
- 初版日期：2026-07-10
- 当前版本：v2（证据整合修订版，2026-07-10）
- 面向：Codex 因子研发 → Tier2/IC 审核 → QE 对照实验
- 关联：`develop-factor`、`analyze-factor-library`、#1939/#1940/#1941/#1943（`l2_code_id` 链路）、原 F1–F4 规格

---

## 1. 背景与已确认事实

策略目标是捕捉板块轮动 alpha：不仅识别“哪个板块在领涨”，还要识别轮动是否扩散到多数成员、成员是否协同、板块是否正在进入或退出领涨区，以及板块内哪些股票具备稳定的相对强度。

当前基础能力如下：

- GATs 关系模型已接入真实申万 L2 行业信息；模型侧可以显式利用板块归属。
- 导出侧已在 `sector_data.h5` 的 22 个 `sw2_*` 数值字段之外增加稳定的 `l2_code_id`。编码来自权威 `sw_index_classify` 映射，未知值为 `-1`，PIT 归属来自 `market.sw_index_member`。
- `sw2_*` 是“个股当日所属板块的指数聚合值”，按 PIT 归属展开到个股；`l2_code_id` 是离散分组键，不是连续特征。
- 方向 A 的签名 fallback 邻接偏置在实验 `qe_20260710_005329_4b05` 的指定配置中未观察到可辨识增量（off≈industry_bias，0.0930 vs 0.0927）。该结果不能外推否定所有邻接设计，但足以说明后续主线应使用真实离散板块码和显式板块因子，不再依赖字段签名猜测同业关系。

本次修订同时纳入因子库 MCP 的去重与统一指标证据。关键结论是：原 F1–F4 不能作为四个全新的同优先级因子直接开发。

| 原编号 | 原设计 | 统一状态 | 当前证据与处置 |
|---|---|---|---|
| F1 | `m_sector_rs_rank_20d` 板块相对强度排名 | `BASELINE` | 与既有行业动量/行业反转族同源。对收益做 percentile rank 是单调变换，本身不产生正交性。保留为研究基线，不作为首批新增因子；新增研发改为“板块排名速度”。 |
| F2 | `m_sector_breadth_ma20` 板块内成员站上均线比例 | `BASELINE` | raw level 作为基线；因子库中未发现申万 L2 成员均线广度的直接同义因子，因此将结构不同的 breadth thrust（A2）作为 `NEW` 主候选。 |
| F3 | `m_sector_flow_rotation_10d` 板块资金流加速 | `NEGATIVE_CONTROL` | 与现有 `m_sw2_net_vol_momentum` 等高度相邻；既有 out-sample 1d 证据弱。快筛不过不入库。 |
| F4 | `m_stock_sector_leadership_20d` 个股 20 日动量减板块动量 | `REUSE` | 已由 `m_stock_vs_industry_mom_20d` 精确覆盖，并与 `m_mom_residual_20d` 高相关。禁止换名重复入库；先审计并复用现有因子做 h20 重评估，再决定是否研发结构不同的 leadership persistence。 |

关键策略约束保持不变：

- 标签不做板块中性化；主标签保持与目标 QE 实验一致的裸 h20 前向收益。
- 因子内部可以使用行业相对值、残差、板块内排名等结构，但不能把“因子使用相对值”和“标签板块中性化”混为一谈。
- 正交性和模型增量价值优先于单因子绝对 IC；不得为了扩充数量重复注册同公式、反向或单调变换因子。

## 2. 目标与非目标

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

候选在研发前和研发后使用统一状态，不用“计划开发”“已完成”“可用”混写：

| 状态 | 含义 |
|---|---|
| `NEW` | 公式已冻结，准备新开发。 |
| `BASELINE` | 只作比较基线，不默认新增可用因子。 |
| `REUSE` | 复用已有资产，只补缺失的 h20/相关性/模型证据。 |
| `NEGATIVE_CONTROL` | 负对照；未过快筛立即停止。 |
| `CONDITIONAL` | 只有上游数据或前一批证据通过后才开发。 |
| `PASS/MARGINAL/KILL/DUPLICATE` | 研发后的最终处置。 |

### 2.2 非目标

- 不以“开发数量”替代质量门禁。
- 不重复创建现有行业动量、行业残差或其反向副本。
- 不把 `l2_code_id` 当连续数值直接输入因子公式。
- 不用最终 out-sample 结果选择符号、窗口或公式；这些选择必须在 train/validation 阶段冻结。
- 不在本规格中授权 candidate 数据向 active/production 的自动 promotion。

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

## 4. 统一设计原则

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
| A4 | `m_sector_participation_gap_20d` | `NEW` | 典型成员与指数参与差 | close + `sw2_close` + `l2_code_id` | 20d | A |
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

#### A3 `m_sector_rs_rank_velocity_20d_5d`——板块排名速度

- 在板块面板计算 `ret20[s,t] = sw2_close[s,t] / sw2_close[s,t-20] - 1`。
- 每日等权跨板块排名：`rank20[s,t] = CsRank(ret20[:,t])`。
- 主公式：`velocity[s,t] = rank20[s,t] - rank20[s,t-5]`。该值已经由两个截面分位之差归一化，主版本不再二次 rank。
- 预期方向：正；正在进入领涨区比“已经处于高位”更接近轮动速度。
- 相关性重点：与 `Industry_Momentum`、`SW2_MOM5`、`m_industry_reversal_20d` 同时检查，rank 变换不能被当作天然正交证明。

#### A4 `m_sector_participation_gap_20d`——成员参与差

- 个股 20 日收益：`stock_ret20[i,t]`。
- 当日按 PIT 成员聚合：`member_median20[s,t] = median_i(stock_ret20[i,t])`。
- 板块指数 20 日收益在板块面板上计算：`sector_ret20[s,t]`。
- 主公式：`gap[s,t] = member_median20[s,t] - sector_ret20[s,t]`，跨板块 rank 后映射回成员。
- 预期方向：正；中位成员也参与上涨，说明轮动不是少数权重股拉动。
- 风险：可能混入小盘风格，必须额外报告与 SIZE/市值因子的相关性。

#### A5 `m_sector_residual_cohesion_10d_60d`——成员残差协同性

- 个股日收益 `stock_ret1[i,t]` 必须在单一 instrument 的连续价格序列上由 close 执行 `pct_change(fill_method=None)`；板块日收益 `sector_ret1[s,t]` 必须在 4.1 的板块面板上由 `sw2_close` 执行同一计算。两者都不使用预填充收益列。
- 日残差：`resid[i,t] = stock_ret1[i,t] - sector_ret1[s,t]`。
- 当日板块离散度：`mad[s,t] = median_i(abs(resid[i,t] - median_i(resid[i,t])))`。
- 主公式：`cohesion[s,t] = -log(MA10(mad[s,t]) / MA60(mad[s,t]))`；分母为 0 或样本不足时置 NaN，不使用任意 epsilon 掩盖异常。
- 每日跨板块 rank 后映射回成员。
- 经济含义：高值表示近期成员残差相对长期收敛。它是状态特征，本身不预设涨跌方向；方向由 train/validation 冻结。
- 风险：可能退化为板块低波风格，必须检查与波动率因子及 A6 的相关性。

#### A6 `m_sector_vol_compression_5d_20d`——板块波动压缩

- 在板块面板以 `pct_change(fill_method=None)` 计算行业日收益 `sector_ret1`。
- 冻结定义：`RVw[s,t] = rolling_std(sector_ret1[s], window=w, min_periods=w, ddof=1)`，其中 `w ∈ {5, 20}`；不得在实现时替换为 RMS、平方和或年化波动。
- 主公式：`compression[s,t] = -log(RV5[s,t] / RV20[s,t])`；任一窗口样本不足、`RV5 <= 0` 或 `RV20 <= 0` 时置 NaN。
- 每日跨板块 rank 后映射回成员。
- 方向：作为原子状态信号，不在因子内部预先乘动量；h20 方向由 train/validation 冻结。
- 价值：把已验证有效的波动压缩方向迁移到板块层，降低对简单行业动量的依赖。

### 6.2 Batch B：条件扩展候选

Batch B 只在 Batch A 完成快筛、相关性和失败归因后启动。

| 编号 | 因子名 | 状态 | 类型 | 最小历史 | 优先级 |
|---|---|---|---|---:|---|
| B1 | `m_sector_turnover_breadth_accel_5d` | `CONDITIONAL` | 自由流通换手异常广度 | 65d | B |
| B2 | `m_stock_sector_leadership_persistence_20d_10d` | `CONDITIONAL` | 板块内领导持续性 | 30d | C |

#### B1 `m_sector_turnover_breadth_accel_5d`——自由流通换手异常广度

- 数据：`db_turnover_rate_f` + `l2_code_id`。
- 个股异常：`x = log1p(db_turnover_rate_f)`，`z60 = (x - MA60(x)) / STD60(x)`；只在 60 日均值/标准差有效且标准差大于 0 时计算 `hot[i,t] = 1(z60[i,t] > 1)`，否则保持 NaN。
- 板块参与率：`turn_breadth[s,t] = mean_i(hot[i,t])`。
- 主公式：`turn_breadth[s,t] - turn_breadth[s,t-5]`，跨板块 rank 后映射。
- 预期方向：正；关注度从少数个股向更多成员扩散。
- 风险：极端换手可能是出货；必须检查非线性和与换手率 Top 因子的相关性。

#### B2 `m_stock_sector_leadership_persistence_20d_10d`——板块内领导持续性

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
| candidate `factor_implementation_source_data_20260428_candidate` | 23 列，含 `l2_code_id` | 122 列，无 `l2_code_id` | `sector_data.h5` 已满足，bundle 尚未闭环。 |

### 7.2 必须完成的数据 gate

1. 使用 candidate `sector_data.h5` 重跑 `F:/Dev/RD-Agent-main/tools/generate_static_factors_bundle.py`，输出到隔离 candidate 目录。
2. 验证 `static_factors.parquet` 包含 `l2_code_id`，所有有限值保持整数语义，`-1` 处理明确。
3. 记录数据快照指纹、日期范围、行数、股票数、板块数、逐日覆盖率和最小/中位成员数。
4. 在同一 candidate 快照上生成离线因子结果；禁止 sector、price、basic 等数据混用不同截点。
5. candidate → active/production promotion 必须由用户单独确认；本因子研发流程不隐式执行。

QE DB loader 已能返回 `l2_code_id`，但离线因子源未闭环前不能宣称完整研发链可用。自动 transformation/review 提示也必须显式列出 `l2_code_id`，避免 loader 实际支持而转换器错误拒绝或遗漏。

GATs embedding 在研究期可以使用同一实验内稳定映射；进入模拟盘/实时前，必须统一 embedding 侧和导出侧 `industry_code_map`，并验证未知值、增量新行业和重启后的映射稳定性。

## 8. 研发流程

### Stage 0：预检与去重

1. 数据 gate 全部通过。
2. 用因子 MCP 对名称、描述、公式和同族因子定向搜索；搜索摘要必须下钻到明确窗口指标。
3. 对复用基线读取代码与 out-sample 指标，禁止换名重复开发。
4. 为每个新候选写入预注册卡：公式、字段、窗口、方向假设、最小成员数、缺失值规则、主要相关性对照。

### Stage 1：离线执行与双周期快筛

1. 在任务隔离 workspace 生成离线 `code_text` 和 `result.h5`。
2. 检查索引、列名、日期、股票数、板块覆盖、unknown 处理和非空率。
3. 主快筛使用与目标实验一致的 h20 裸标签；1d 只作短周期诊断。
4. 若当前 `quick_ic_screen.py` 尚不支持 `--horizon 20`，必须先补充该能力或使用等价、受测的 h20 脚本；不得用 1d PASS 替代 h20 PASS。

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
4. 补充 out-sample h20 IC、RankIC、HAC/block ICIR；主筛选不得只读 `return_horizon=1d`。
5. 执行 LLM 分类和增量相关性；记录 catalog、metrics、classification、correlation 的完整性 receipt。

### Stage 3：双层相关性与筛选

- 股票映射层和板块原生层均要求与基线/Top 因子 `|corr| < 0.8`。
- 同族候选高相关时只保留 h20 更稳定、覆盖更高、模型增量更好的一个。
- 沿用 Stage 1 冻结方向 `d`，定义 `IC_d = d * IC_h20`、`RankIC_d = d * RankIC_h20`、`ICIR_d = d * ICIR_h20`；不得在 Stage 2/3 重新选择符号或覆写 `d`。
- out-sample h20 目标：`IC_d >= 0.02`、`RankIC_d >= 0.02`，且 block/HAC `ICIR_d > 0.3`；`IC_d` 或 `RankIC_d >= 0.03` 可标记为优秀，但不得忽略显著性与模型增量。
- full 与 out-sample 的 `IC_d`、`RankIC_d` 均应为正且方向一致；近期窗口漂移必须解释。
- 任何因子都不能仅因 QE archive 共现表现良好而跳过独立门禁。

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

### 9.3 结果门禁

报告 h20 IC/RankIC、ICIR、CAGR、Sharpe、最大回撤、换手和容量相关指标，并分训练/验证/测试及主要市场 regime。只有跨合理种子/切分稳定的增量才进入 Tier2/IC 审核。

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

## 11. 验收与交付物

### 11.1 数据与接口

- candidate `sector_data.h5` / `static_factors.parquet` 的 schema、指纹与 `l2_code_id` receipt；
- transformation/review 对 `l2_code_id` 的兼容性 receipt；
- 离线/实时代码 parity 结果；
- unknown、PIT 行业切换、最小成员数和板块字段一致性测试。

### 11.2 因子研发

- Batch A 的 6 个候选代码；Batch B 仅在 gate 通过后交付；
- R1/R2 复用基线的 h20 重评估，不新增重复 catalog 项；
- N1 negative control 的快筛与最终 disposition receipt：KILL 时记录淘汰依据，PASS 时记录后续门禁；
- 每个候选的预注册卡、h20/1d 快筛、统一指标、双层相关性、分类与最终 disposition。

### 11.3 因子库完整性

仅对通过者要求：

- `aistock_factor_catalog`：`is_available=true`，`asset_path` 指向实际可执行源码；
- `aistock_factor_metrics`：官方窗口齐全，并有明确 h20 结果；
- `qe_factor_classification`：至少一条有效分类；
- `qe_factor_correlations`：股票映射层和板块原生层的增量相关性 receipt；
- 失败者不得以空代码、占位实现或仅元数据记录伪装成已交付因子。

### 11.4 模型验证与状态报告

- GATs 2×2 消融和 LGBM 对照结果；
- Tier2/IC 审核结论与未满足项；
- 分别报告：文档/代码合并状态、candidate 数据状态、active promotion 状态、QE 实验状态、模拟盘/实时状态；
- 未完成 h20 指标、数据 promotion 或 train/serve mapping 统一时，不得宣称板块轮动能力已生产就绪。
