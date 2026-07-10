# 板块轮动因子研发指引 (F1–F4)

- 文档类型: 因子研发规格 / 开发指引 (develop-factor)
- 主线: 板块轮动 (sector-rotation) — 让模型显式理解板块归属并捕捉板块轮动 alpha
- 日期: 2026-07-10
- 面向: Codex 因子开发 (按本文规格开发 → develop-factor 全流程 → 我 Tier2/IC 审核)
- 关联: `develop-factor` skill v3.0.0 · `sector-rotation-plan` (记忆) · #1939/#1940/#1941/#1943 (l2_code_id 链路)

---

## 1. 背景 (Background)

策略目标是**捕捉板块轮动 alpha**:让选股信号显式利用"哪个板块在领涨/资金在往哪个板块轮动"。此前:

- **GATs 关系模型**已加 `l2_code` embedding(#1939 合并),从权威 PIT 表 `market.sw_index_member` 直读真实申万 L2 行业码,模型侧可显式理解板块归属。
- **导出侧 `l2_code_id`** 已入 `sector_data.h5`(#1940/#1943/#1941 合并,pandas as-of PIT 编码,稳定映射来自 `sw_index_classify`,未知=-1)。候选已验证 (7334829, 23) / coverage 1.0 / 131 distinct。
- **方向 A(签名 fallback 邻接偏置)已被反证无效**:实验 `qe_20260710_005329_4b05` off≈industry_bias(0.0930 vs 0.0927,深在噪声带内),证明"用 sw2_* 因子签名分组当同业"不带 alpha。→ 必须走**真实离散板块码 + 显式板块因子**路线。

关键约束(用户 2026-07-10 确认):
- **不做板块中性化**(中性化会剥离轮动收益);标签保持**裸前向收益**(20 日,与 direction-A/实验同口径)。
- 现有 22 个 `sw2_*` 字段是**个股所属板块的指数聚合值**(导出时按 PIT 归属展开到个股);`l2_code_id` 是新增的**显式整数板块分组键**(0..~133,未知=-1),用于跨板块排名与板块内聚合。

## 2. 目标 (Goals)

产出 4 个显式板块轮动因子,作为 G12 基座之外的**正交板块信号腿**,供步骤⑤对照实验(G12 基准 vs G12+4因子+l2 embedding,裸标签,LGBM+GATs):

| 编号 | 因子名 (m_ 前缀) | 语义 | 主数据源 | 经验对标 |
|------|-----------------|------|---------|---------|
| F1 | `m_sector_rs_rank_20d` | 板块间相对强度截面排名(哪个板块领涨) | sw2_close + l2_code_id | 板块动量 |
| F2 | `m_sector_breadth_ma20` | 板块内成员站上均线比例(轮动强度确认) | 个股 close + l2_code_id | 广度 |
| F3 | `m_sector_flow_rotation_10d` | 板块主力资金净流入加速 + 跨板块排名 | sw2_mf_net_amt + l2_code_id | ⚠️ mf-sector(见风险) |
| F4 | `m_stock_sector_leadership_20d` | 个股动量 − 所属板块动量(板块内龙头) | 个股 close − sw2_close | ✅ P0 行业残差(已验证) |

> 命名遵循 skill 规则 `^[a-z][a-z0-9_]{2,80}$`,`m_` 前缀。窗口后缀反映主周期,可再派生 5d/10d 变体。

## 3. 设计原则 (Design Principles)

1. **`l2_code_id` = 显式分组键,不是连续特征**。F1/F3 需按 `l2_code_id` **去重**得到"每板块每日一个值"再做跨板块截面排名;F2 需按 `l2_code_id` **分组**个股算板块内比例。**未知码 -1 必须排除出分组/排名**(置 NaN 后 dropna),不得当作一个真实板块。
2. **对齐已验证方向 (P_succ),避开已证伪 (P_fail)**:
   - ✅ **P0 行业残差 (pv+sw), IC≈0.031, `m_ind_pb_rel_mom` 0.037, ICIR>0.3, 与 Top 因子低相关** → **F4 是最高优先级**(个股动量相对板块动量 = 同族 momentum 版)。
   - ⚠️ **mf 三源组合 (mf+pv+sw) 已两次证伪 (IC 0.003)**(`m_ind_flow_deviate`/`m_ind_flow_residual_mom`)→ **F3 高风险**,必须设计为"板块级净流入**加速** + 跨板块**排名**"(而非个股 flow 残差),按 `sw2_amount`/`sw2_total_mv` 归一化去量纲,**标记为实验性、低优先级**,快筛不过即淘汰。
   - **避开 Correlation Red Sea**:短期反转 (5-20d pct_change) 已饱和(占 Top 因子 44%,corr>0.8)。F1/F4 用多周期收益但落点在**板块级/板块内相对**,与个股短期反转正交;开发前用 `analyze-factor-library` 与已有行业因子(`m_ind_pb_rel_mom` / `Industry_Momentum` / `IndustryMomentumExcessReturnCross` / `industry_stock_momentum_diff_10d` / `m_sw2_net_vol_momentum`)做 pairwise 相关。
3. **裸标签、PIT、无未来泄露**:严禁 `shift(-N)` 用于特征;所有 rolling 只用历史;`l2_code_id` 本身是 PIT 编码(as-of),直接按当日取用即可。
4. **正交优先于绝对 IC**:A 股 |IC|≈0.03-0.06 已优秀;更看重与全库 Top 板块因子低相关(<0.8)+ ICIR>0.3。这 4 个的价值在于给组合层/GATs 提供**板块维度的正交信号**,不是单腿刷 IC。

## 4. 因子详细设计 (口径)

> 以下给"计算口径",不给完整代码;Codex 按 develop-factor 代码模板 + 数据接口实现,向量化(`unstack("instrument")` + rolling / `groupby(level).transform`)。全部输出单列 `MultiIndex(datetime, instrument)`。

### F1 `m_sector_rs_rank_20d` — 板块间相对强度排名
- **意图**:识别领涨板块;领涨板块的成员股给高分(板块动量延续)。
- **数据/列**:`_STATIC_FACTORS_LOADER` `columns=["sw2_close", "l2_code_id"]`。
- **口径**:
  1. 板块 20 日收益 `sec_ret = sw2_close / sw2_close.shift(20) - 1`(每股=其板块指数收益)。
  2. **去重到板块级**:按 `(datetime, l2_code_id)` 取一个 `sec_ret`(同板块 sw2_close 相同,`groupby([datetime,l2_code_id]).first()`);排除 `l2_code_id==-1`。
  3. **跨板块截面排名**:每个 datetime 对~131 个板块的 `sec_ret` 做 `rank(pct=True)`。
  4. **映射回个股**:每股取其 `l2_code_id` 对应的板块排名分位。
  5. 可选:5/10/20d 排名等权合成成一个更稳的 RS(留 20d 主口径,合成版另开变体)。
- **IC 符号预期**:**正**(高 RS 板块延续领涨)。若快筛为负且稳定,说明板块层反转,取负后保留。
- **相关性风险**:与 `Industry_Momentum` 类可能相关;必须 pairwise 校验 <0.8(去重+跨板块排名口径应带来正交性)。

### F2 `m_sector_breadth_ma20` — 板块内广度
- **意图**:确认板块轮动强度——领涨板块里"多少成员真的在涨"(广度高=轮动扎实,广度背离=虚涨)。
- **数据/列**:个股 `close` (`_REALTIME_LOADER fields=["close"], adjust="qfq"`) + `_STATIC_FACTORS_LOADER columns=["l2_code_id"]`。
- **口径**:
  1. 个股 `above_ma = (close > close.rolling(20).mean()).astype(float)`。
  2. **板块内比例**:每 `(datetime, l2_code_id)` 对成员 `above_ma` 取均值 = 板块广度(排除 -1)。
  3. 每股取其板块广度值。可选:减去板块广度的 60 日均值 → 广度**异常**(去板块基线)。
- **IC 符号预期**:**正**(广度高=参与度高,趋势健康);极端高可能超买反转,观察 ICIR。
- **相关性风险**:与个股动量相关性应低(这是板块聚合量);校验。

### F3 `m_sector_flow_rotation_10d` — 板块资金轮动(⚠️ 实验性)
- **意图**:主力资金正在往哪个板块**加速**流入(资金轮动先行信号)。
- **数据/列**:`_STATIC_FACTORS_LOADER columns=["sw2_mf_net_amt", "sw2_amount", "l2_code_id"]`。
- **口径**:
  1. 归一化板块净流入 `flow = sw2_mf_net_amt / (sw2_amount + eps)`(去量纲,避免大板块主导)。
  2. **加速度**:`accel = flow.rolling(10).mean() - flow.rolling(10).mean().shift(10)`(近 10 日均净流入 − 前 10 日,即环比加速),或 `Slope(flow, 10)`。
  3. **跨板块排名**:去重到 `(datetime, l2_code_id)`,截面 `rank(pct=True)`;映射回个股(排除 -1)。
- **IC 符号预期**:**正**(资金加速流入板块→跟涨)。
- **⚠️ 强制风险控制**:mf-sector 属 P_fail(IC 0.003 两次失败)。**快筛 |IC|/|RankIC| < 0.015 直接 KILL,不入库**;仅当明显 PASS 才进全量。设计上必须是"加速+排名"而非原始 flow 或残差。

### F4 `m_stock_sector_leadership_20d` — 板块内领先度(✅ 最高优先级)
- **意图**:在(轮动的)板块里选**龙头**——个股相对其所属板块的超额动量。属已验证的"P0 行业残差 (pv+sw)"族(`m_ind_pb_rel_mom` +0.037)。
- **数据/列**:个股 `close` (`_REALTIME_LOADER`) + `_STATIC_FACTORS_LOADER columns=["sw2_close"]`(l2_code_id 非必需,因 sw2_close 已是该股板块指数)。
- **口径**:
  1. 个股 20 日动量 `stk_mom = close/close.shift(20)-1`。
  2. 板块 20 日动量 `sec_mom = sw2_close/sw2_close.shift(20)-1`。
  3. **领先度 `lead = stk_mom - sec_mom`**(个股相对板块的超额)。
  4. 可选:每日截面 `rank(pct=True)` 或按 `l2_code_id` 做**板块内**排名(板块内龙头,更贴轮动语义,但需 l2_code_id 分组)。
- **IC 符号预期**:**正**(板块内领先者延续)。与 `m_ind_pb_rel_mom` 同族但用动量,预期同为正、ICIR>0.3。
- **相关性风险**:与 `m_ind_pb_rel_mom`(PB 相对)、`Industry_Momentum` 校验 <0.8;动量口径应与 PB 口径正交。

## 5. 详细要求 (Detailed Requirements)

### 5.1 代码接口与硬约束(QE/Qlib 兼容,来自 develop-factor skill)
- 函数签名固定:`def calculate_{factor_name}(instruments: list, start_date: str, end_date: str) -> pd.DataFrame:`。
- 数据仅通过注入的 `_REALTIME_LOADER`(行情 OHLCV,`fields=[...]`,`adjust="qfq"`,列名无 `$`)与 `_STATIC_FACTORS_LOADER`(`columns=[...]` 显式取列,含 `sw2_*` / `l2_code_id` / `db_*` 等)获取。
- **严禁**(违反即判失败):`try-except` 兜底 / 空值兜底(`float('nan')` 填列、`if df.empty: return`)/ `shift(-N)` 未来泄露 / 文件读写(`read_hdf`/`read_parquet`/`to_*`)/ 硬编码股票或日期 / `$` 前缀。
- **向量化**:用 `unstack("instrument")` + `rolling` 或 `groupby(level=...).transform`;禁 `groupby.rolling.corr()` 慢写法。
- 输出:单列 DataFrame,列名 = 因子名,`index.names` 继承自 loader(禁手写 `["datetime","instrument"]`),末尾 `dropna()`。

### 5.2 `l2_code_id` 使用口径(本主线关键)
- 作为**整数分组键**:`groupby(level="datetime")` 内再按 `l2_code_id` 分组/去重(用 `columns` 里取到的 `l2_code_id` 列,不是 index)。
- **未知/未匹配 = -1**:分组/排名前必须 `mask(l2_code_id == -1)` 或过滤,**不得**把 -1 当一个真实板块参与截面排名(否则污染分位)。coverage 已 1.0,-1 极少但须显式处理。
- 不得把 `l2_code_id` 当连续数值直接进因子公式(它是类别码)。

### 5.3 命名与批次
- 4 个主因子如表;如派生多周期变体(5d/10d),用 `m_sector_rs_rank_10d` 等。
- 按 skill "批量开发 5-10 个 → 快筛淘汰 �� 通过进全量"。本批 4 个(+可选变体)一批。

## 6. 验收标准 (Acceptance)

按 develop-factor 全流程,逐因子:
1. **执行验证**:WSL rdagent-gpu 无错;`index.names==["datetime","instrument"]`;列名=因子名;日期覆盖训练+测试期;股票数 >3000。
2. **Stage 1 快筛**(out_sample,RobustZScore):`|IC|>=0.015 且 |RankIC|>=0.015` = PASS 进全量;`|IC|>=0.005 或 |RankIC|>=0.010` = MARGINAL 低优先;否则 KILL。**F3 未 PASS 直接淘汰**。
3. **Stage 2 全量**(4 窗口×指标):out_sample **|IC|>=0.02 为目标**(>=0.03 优秀),**ICIR>0.3**;`full` 与 `out_sample` IC 方向一致(防过拟合);IC 符号与因子含义一致(§4 预期,不一致须取负并复核逻辑)。
4. **正交性**:与全库 Top 板块因子(`m_ind_pb_rel_mom`/`Industry_Momentum`/`IndustryMomentumExcessReturnCross`/`industry_stock_momentum_diff_10d`/`m_sw2_net_vol_momentum`)相关性 **<0.8**;>=0.8 视为红海冗余,重设计或淘汰。
5. **入库完整性**:`aistock_factor_catalog`(is_available)+`aistock_factor_metrics`(4 窗口)+`qe_factor_classification`(1 条)齐全;`asset_path` 指向 `rdagent_assets/manual_factors/{name}.py`。

## 7. 数据前置依赖 (Prerequisite) 🔴

这 4 个因子**读取 `l2_code_id` / `sw2_*`**,因此 develop-factor 验证(读因子源数据目录文件)与步骤⑤ QE 实验前,**因子源数据必须已含 `l2_code_id`**:
- 已完成:新 `sector_data.h5`(23 列,含 `l2_code_id`)已放入 rdagent **候选** 因子源目录 `factor_implementation_source_data_20260428_candidate`。
- **待办(deploy/数据 prep)**:用新 `sector_data.h5` **重跑 `generate_static_factors_bundle.py`** 生成含 `l2_code_id` 的因子面 `static_factors.parquet`;并把候选源(或其内容)提供给验证/实验实际读取的目录(候选→active 由用户确认口径:04-28 实验同窗 vs 生产 06-30)。
- QE 演进路径(`_STATIC_FACTORS_LOADER`→`build_static_factors`)已直读 DB 含 `l2_code_id`(#1943 合并),故 QE 实验侧不被 h5 阻塞;但 develop-factor 的原始 factor.py 快筛读文件,需源目录文件含 `l2_code_id`。

## 8. 风险与注意 (Risks)

1. **F3(mf-sector)大概率证伪**:P_fail 已两次失败;作为实验腿,快筛不过即弃,不强留。
2. **相关性饱和**:F1/F4 若与既有行业动量因子 corr>=0.8 则冗余;去重+跨板块/板块内排名口径是拉开正交的关键。
3. **`l2_code_id==-1` 处理**:必须排除出分组/排名,否则截面分位被污染。
4. **板块截面样本**:~131 个板块,跨板块 rank 的样本量足够;但单日若某板块只 1 只成员,F2 广度=0/1 噪声大,可加最小成员数阈值(如 <3 只置 NaN)。
5. **train/serve parity**:上线(模拟盘)时运行期须用同一 `l2_code_id` 编码 + 同 PIT 口径;GATs embedding 侧目前是出现顺序映射(与导出的 `industry_code_map` 稳定映射不同),进模拟盘前需统一(记忆 Q2 待办,不阻塞本批研发/回测)。

## 9. 交付物 (Deliverables)

- 4 个因子代码(+可选周期变体),经 develop-factor 全流程:执行验证 → 快筛 → 入库(catalog)→ 指标(4窗口)→ 增量相关性 → LLM 分类 → IC 筛选。
- 快筛/全量 IC、ICIR、与 Top 行业因子相关性结果,交我 Tier2/IC 审核。
- 通过的因子供步骤⑤对照实验:**G12 基准 vs G12 + {F1..F4 通过者} + l2 embedding**,裸标签 h20,LGBM + GATs(GATs 必须 1-parallel)。
</content>
