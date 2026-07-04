# QE 关系/图模型接入设计（Track B/C：截面 Transformer 与概念图模型）

- 文档类型：设计（docs/design）
- 日期：2026-07-05
- 状态：草案（待评审）
- 关联：多 Alpha 趋势策略（类型 B 上升趋势）Phase0 结论、BUG-592/BUG-593（QE 内置时序模型接入修复）
- 生产影响门：backend=noop / ddl=noop / frontend=noop（本文件仅设计，不改代码）

---

## 1. 背景与动机

Phase0 趋势方向验证（全样本 filtered_pool_20260428，disable_alpha158，8–10 趋势因子）已得出稳定结论：

- 趋势方向成立：LGBM/LSTM/TCN 三模型的 rank_ic 随 label horizon 单调走强，**h20（QE 上限）最优**。
- 最优单腿（单窗单种子）：
  - LSTM h20：rank_ic 0.098 / 年化 0.42（cagr 0.80）/ Calmar 6.53
  - LGBM h20（10 因子）：rank_ic 0.098 / 年化 0.41 / Calmar 5.2，且**双种子 0.0976 vs 0.0979 极稳**（种子稳定性远胜神经网）
- **时间维 Transformer（qlib `pytorch_transformer_ts`）实测失败/无效**：6 个 loop rank_ic ≈ 0（−0.02 ~ +0.01），年化 0.01–0.17，Calmar 0.5–3.2，全面弱于 LSTM/TCN。

### 1.1 关键判别：为什么「时间维 Transformer 无效」与「研究称 Transformer 有价值」并不矛盾

「Transformer」在选股文献中指两类**结构完全不同**的模型：

| | 时间维 Transformer（已实测） | 截面/关系 Transformer（研究价值所在） |
|---|---|---|
| 代表实现 | qlib `pytorch_transformer_ts` | MASTER、DTML |
| 注意力对象 | 单只股票自身的时间窗口 | **跨股票**（同一截面日股票间互相 attention） |
| 能否表达板块内关联/轮动 | 否（结构上不跨股票） | 是（为此设计） |

MASTER（arXiv 2312.15235）摘要明确批评「先从单只股票序列学时间模式再混合」的做法——正是时间维 Transformer 的结构。因此本设计的目标是接入**截面/关系/图模型**，而非继续调时间维 Transformer。

### 1.2 数据现状（已在库中只读核验，2026-07-05）

- **行业关联邻接已存在且 PIT**：`market.sw_index_member`（列：`l1_code/l1_name/l2_code/l2_name/l3_code/l3_name/ts_code/name/in_date/out_date/is_new`）。含 `in_date/out_date` → 可构建**逐期点对时（point-in-time）成分/邻接矩阵**，零采集成本。
- **行业树**：`market.sw_index_classify`（parent_code/level）。
- **行业聚合序列**：`market.sector_data`（SW2 OHLCV+资金流，已进因子）。
- **概念板块成分缺失**：`market.sina_board_daily` 仅 12 天板块级示例，**无 stock↔concept 成分映射**；概念邻接需另采（见 §7，THS 同花顺为主源）。

---

## 2. 目标与非目标

### 2.1 目标
1. 在 QE 自定义模型体系内，接入**至少一个关系/图模型**（首选顺序 GATs/HIST-industry → MASTER），用**现成行业邻接**验证「板块内关联」是否在**我们自己的 A 股趋势数据**上产生**可复现的增量**（相对 LGBM h20 / LSTM h20 基线）。
2. 建立**PIT 行业邻接构建器**（离线、只读、可复算），杜绝未来函数。
3. 给出概念图（Track C）的扩展路径与数据采集口径（THS）。

### 2.2 非目标
- 不改 `GeneralPTNN` 本体、不改 LGBM/LSTM/TCN 现有路径、不改 DatasetH/TSDatasetH 分支语义。
- 不在本阶段采集概念数据、不改动生产选股/模拟盘绑定。
- 不追求「一定超过 LGBM」——增量与否是**待验证的实证问题**，设 fail-fast 门。
- 股票池不变（`filtered_pool_20260428`，IPO 满 1 年 + ST/退市规避 `shsz_st_pit_active_v1`）。

---

## 3. 模型选型与理由

| 模型 | arXiv | 结构 | 邻接需求 | 选型定位 |
|---|---|---|---|---|
| **HIST** | 2110.13716 | 股票-概念**动态**图（预定义+隐藏概念） | 概念/行业成分 | **Phase1 首选（行业变体）→ Phase3（概念）** |
| **GATs** | 图注意力（qlib 内置） | 截面图注意力 | 行业邻接（或学习） | 更轻备选探针 |
| **MASTER** | 2312.15235 | 截面 Transformer（市场引导，跨股票+跨时间） | 隐式（可无显式图） | Phase2：对照「正确的 Transformer」 |
| RSR（关系排序） | 1809.09441 | 关系图 + ranking loss | 行业/知识图 | 备选：ranking loss 对齐 topk 目标 |
| STHAN-SR（超图） | 2107.14033 | **超图**三重注意力 | 多归属概念超图 | Phase3：多归属概念的正解 |

**推荐路径（已决策 2026-07-05）**：Phase1 采用 **HIST-industry**（吃现成 `sw_index_member` 行业成分作预定义概念图 + 隐藏概念模块学潜在联动；动态相关性设计正对 A 股题材轮动；O(N×C) 比全连接 GATs 在 ~4000 股规模更省；一份实现同时铺好 Phase1 行业与 Phase3 概念，衔接无返工）。GATs 作为更轻的备选探针保留。Phase2 引 **MASTER** 对照「正确的 Transformer」，Phase3 采 THS 概念数据上 **HIST/STHAN-SR 概念超图**。

> GATs vs HIST-industry 取舍：GATs 更轻/更快跑通但学出的关系在 A 股噪声下不稳、全连接 O(N²) 重、不利用已知行业结构；HIST-industry 更贴合「概念/行业共享信息 + 动态轮动」诉求、直接用免费行业成分、可发现隐藏分组、是概念超图的同框架前身，代价是复杂度更高。综合选 HIST-industry。

---

## 4. 系统架构与接入方式

### 4.1 QE 自定义模型路径（沿用现成能跑的通路）
现状：能跑的 LSTM/TCN 是 **`model_type=TimeSeries` + `code_text`（自定义 model.py）+ `dataset_config=TSDatasetH`** 的自定义模型路径（`pt_model_uri: model.model_cls`）。图模型同样走**自定义 code_text 路径**注册到 `aistock_model_catalog`，由 `config_composer` 合成 conf.yaml、在节点 `qrun` 执行。

### 4.2 核心技术风险（务必先坐实）：截面批 vs 逐股票窗口
- QE 现管线的 `TSDatasetH` 采样单元 = **单只股票的时间窗口** `[N样本, T, F]`，样本间无跨股票关系。
- 图/截面模型需要**按交易日的截面批** `[num_stocks_t, F(,T)]` + 当日邻接 `A_t`，即「同一天所有股票一起进网络」。
- **这是与现有 QE 数据装载最根本的差异，也是本接入的最大风险点**。两条落地方案：
  - **方案 A（推荐，隔离）**：自定义 model.py 内自带 **cross-sectional 数据整形层**——在 workspace 里把 qlib 提供的面板数据（TSDatasetH/DatasetH 输出）在 `fit/predict` 内**按 trade_date 重组为截面批**，邻接 `A_t` 从随 workspace 落地的 PIT 邻接 artifact 读取。对 QE composer/dataset 改动最小（仅新增自定义模型 + 一个邻接 artifact），fail-fast。
  - **方案 B（重）**：在 config_composer 增加 `dataset_type=CrossSectionalDatasetH` 分支 + 新 loader。改动面大、回归风险高，**非本阶段目标**，仅当方案 A 被证明不可行再评估。
- 结论：**Phase1 用方案 A**，把「截面重组 + 邻接注入」封装在自定义模型工作区内，不动 QE 主干。

### 4.3 邻接矩阵构建（PIT，离线只读 artifact）
- 输入：`market.sw_index_member`（`ts_code, l1_code, l2_code, l3_code, in_date, out_date`）。
- 规则：对每个调仓/交易日 `t`，成员判定 = `in_date <= t AND (out_date IS NULL OR out_date > t)`（**严格 PIT，杜绝未来函数**）。
- 产物：按行业层级（默认 L2 申万二级，与现有因子口径一致）构建**同业邻接** `A_t[i,j]=1 iff 同一 L2`（可选行内归一化 / 加自环）；稀疏存储 + 版本化（含数据快照日期、股票池版本 sha）。
- 落地：随 loop workspace 分发的只读 artifact（与因子缓存同机制），可复算、可校验。
- 反泄漏校验：邻接构建只能用 `t` 之前可见的成分状态；单测覆盖「out_date 边界当日不计入」。

### 4.4 特征集（关键：不能只喂 10 因子，但要避开拥挤因子）

图/注意力模型需要比 10 因子更丰富的输入，否则「饿着」学不到东西（这也是时间维 Transformer 无效的部分原因）。但「丰富」不等于「堆公开手工因子」——须区分**原始底料**与**拥挤 alpha**：

| 层级 | 内容 | 定位 | 拥挤风险 |
|---|---|---|---|
| 原始底料 | **Alpha360 原始量价**（`Ref($close,i)/$close`、`Ref($open,i)/$close`…`Ref($volume,i)/$volume`，6 字段 × 60 天滞后归一化 = 360 维；源自 qlib `Alpha360DL.get_feature_config`） | 给时序编码器的输入表示，**非可交易 alpha** | 无（原始价量人人相同，边来自模型+关系+私有因子） |
| 差异化 alpha（主力） | **私有趋势/筹码/资金流/基本面因子**（Price_Deviation_Historical_High、elg_flow_high_price_interaction、cp_winner_rate_momentum、m_sw2_net_vol_momentum、FundamentalEpsIndustryMomentum…） | 真正差异化信号来源 | 低（较 Alpha158 手工因子不拥挤） |
| 补充（非主力） | **Alpha158 择优子集** | 仅补充，不当主信号 | 高（公布多年、机构广用，尤其短周期动量/反转） |
| 核心赌注 | **关系结构（板块关联/轮动）** | 任何公开因子都不含的维度 | 不可被公开因子套利 |

**决策（2026-07-05）**：
- **Alpha360 作原始底料，但不塞进因子库**：其本质是 `Ref($field, lag)/$close` 的滞后原始量价（数据装载 spec），不是命名截面因子；把 360 个近重复滞后列注册为因子库因子是错误抽象且冗重。采用 **QE 自定义模型内的数据 handler，按 qlib `Alpha360DL` 规范从 qlib bin 的原始量价即时生成**（qlib bin 已含 `$close/$open/$high/$low/$volume/$vwap`，无需新增因子库列）。
- **私有趋势因子 + Alpha158 择优子集**走因子库常规流程（develop-factor / factor_library_register），作为差异化 alpha 与补充。
- **拥挤性判断**：本策略是 **h20（月级）趋势 + 基本面 + 筹码**，属容量受限、更难套利、更不拥挤地带；且实证 rank_ic 0.098（h20 > h5 0.064）证明残余信号存在。故不依赖 Alpha158 手工因子作主力，用原始底料 + 私有因子 + 关系结构。
- 保持股票池、label horizon（h20）、回测口径与基线一致，确保可比。

> Alpha360 底料的 QE 落地计划（源码移植 vs 因子库注册 vs 即时 handler）见 §11。

---

## 5. 评估口径与判定门（fail-fast）

- 基线（须先由 A 硬化实验确定其种子均值±std）：**LGBM h20（10 因子）与 LSTM h20** 的 rank_ic / 年化 / Calmar / topk。
- 图模型判定（同池同窗同 h20）：
  1. **信号**：rank_ic 是否 ≥ 基线均值 + 1×种子 std（即在噪声之上）。
  2. **绝对年化**（第一优先级）：年化是否 ≥ 基线，且 Calmar 不显著恶化。
  3. **正交性**：图模型预测与 LGBM/LSTM 腿的截面预测相关性——**低相关才有做独立腿的价值**（这是多腿组合的核心，不是单腿最强）。
  4. **鲁棒性**：多种子均值±std + 多窗（walk-forward）分窗一致性（沿用 macb 鲁棒 KPI，不看单窗）。
- **任一门不过即 no-go**：诚实记录「关系模型在我们数据上未见增量」，不软化、不硬凑。

---

## 6. 分阶段实施步骤（建议）

### Phase 0 — 基线与邻接准备（可与 A 硬化实验并行，低成本）
- P0.1 等 A 硬化实验（GPU `qe_20260705_004409_4437` / CPU `qe_20260705_004509_561a`）出结果，锁定 **LGBM h20 / LSTM h20 的种子均值±std** 作为「待超越的横杆」。
- P0.2 实现 **PIT 行业邻接构建器**（离线脚本，读 `sw_index_member`，产出按日 L2 邻接稀疏 artifact + 版本元数据）。只读、不入生产库。
- P0.3 定 Phase1 特征口径（Alpha158/360 + 10 趋势因子）与评估脚本（rank_ic/年化/Calmar/topk + 预测相关性）。
- 交付：邻接 artifact + 基线数字表 + 评估脚本。**门：邻接 PIT 反泄漏单测通过。**

### Phase 1 — 行业图 MVP（免采集数据，正式走 feature workflow）
- P1.1 按 `aistock-feature-workflow` 建隔离 worktree，新增 **HIST-industry 自定义模型**（code_text，方案 A 截面重组封装在 model.py；预定义概念图 = `sw_index_member` PIT 行业成分；隐藏概念模块学潜在联动），注册到 `aistock_model_catalog`。GATs 作更轻备选。
- P1.1b 特征底料：自定义 handler 按 qlib `Alpha360DL` 规范即时生成原始量价（§4.4/§11），叠加私有趋势因子；Alpha158 择优子集可选补充。
- P1.2 QE 自定义实验：HIST-industry，h20，单种子（先验证能训练、能过 Epoch0、能上传），同池同窗。
- P1.3 判定门：rank_ic / 年化是否达到 §5 门（≥ 基线 + 噪声）。**过 → Phase2；不过 → 记录 no-go，Track B 暂止，回到 Track A 因子精修。**
- 交付：可跑的图模型 + 首轮对照结论。**产线门：backend/ddl/frontend = noop（仅新增模型与实验）。**

### Phase 2 — 多种子 + MASTER 对照 + 正交性（仅当 Phase1 见增量）
- P2.1 GATs-industry 多种子（5）+ 多窗（2–3 walk-forward）。
- P2.2 引入 **MASTER**（截面 Transformer）同口径对照，回答「正确的 Transformer」是否优于 GATs。
- P2.3 计算图模型腿与 LGBM/LSTM 腿的**预测相关性**；判是否具备做独立第 3 腿的正交价值。
- 交付：关系模型鲁棒性 + 正交性结论；给出是否纳入多腿的建议。

### Phase 3 — 概念动态超图（需采集 THS 概念数据）
- P3.1 走 `add-tushare-dataset` skill 采 **THS 同花顺概念成分**（`ths_index/ths_member/ths_daily`），落 PIT 成分（`in_date/out_date`），东财 DC 作短周期情绪叠加层备选。
- P3.2 构建**动态概念超图**（多归属：一只股票属多个概念），上 **HIST / STHAN-SR**。
- P3.3 判定：概念图相对「仅行业图」是否再增量（尤其题材/轮动场景）。
- 交付：概念图腿评估；概念数据的 PIT 治理与更新机制。

### Phase 4 — 纳入多 Alpha 与治理（仅当图腿被证明正交且增量）
- 将图模型腿作为**趋势腿候选 C** 纳入多 Alpha combine-backtest；通过策略包治理（governance）走验证 → 选股 → 模拟盘链路。与已上线反转两腿、趋势腿 A（LSTM/TCN h20）、腿 B（LGBM h20）统一在组合层评估。

---

## 7. 概念板块数据源（Track C 采集口径）

| 源 | tushare 接口 | PIT 成分 | 历史 | 定位 |
|---|---|---|---|---|
| **THS 同花顺（主源）** | `ths_index`/`ths_member`（含 in_date/out_date）/`ths_daily` | 有 | 长（~2016+） | 回测骨架 + PIT 概念超图 |
| DC 东财 | `dc_index`/题材成分 | 部分 | 短（~2020+） | 短周期情绪/涨停/游资叠加层 |
| TDX 通达信 | tdx 概念板块分类/成分/行情 | 弱 | 弱/漂移 | 不作回测主源（本地 tqcenter.py 可直连实时） |

采集与行业邻接同构：产出按日 PIT 成分矩阵（多归属 → 超图/二部图），版本化、反泄漏单测。

---

## 8. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 截面批与 QE TSDatasetH 不兼容（最大风险） | 方案 A：截面重组封装在自定义 model.py 内，不动 QE 主干；先做能训练/过 Epoch0 的冒烟 |
| 10 因子太薄导致图模型学不到 | Phase1 改用 Alpha158/360 + 趋势因子的丰富特征集 |
| 邻接未来函数/幸存者偏差 | 严格 PIT（in_date/out_date 边界）+ 反泄漏单测 + 版本化 artifact |
| 图模型在我们数据上无增量 | fail-fast 判定门；no-go 即止，回 Track A 因子精修，不硬凑 |
| 概念数据采集与 PIT 治理成本 | 延后到 Phase3，且以行业图先验证「关系是否有 alpha」再投入 |
| 污染生产/主目录 | 全程隔离 worktree + docs/feature workflow；本设计文件 noop |

---

## 9. 参考文献

- MASTER: Market-Guided Stock Transformer for Stock Price Forecasting — arXiv:2312.15235（AAAI 2024）
- HIST: A Graph-based Framework for Stock Trend Forecasting via Mining Concept-Oriented Shared Information — arXiv:2110.13716
- Temporal Relational Ranking for Stock Prediction (RSR) — arXiv:1809.09441
- Temporal-Relational Hypergraph Tri-Attention Networks for Stock Trend Prediction (STHAN-SR) — arXiv:2107.14033
- Network Momentum — arXiv:2501.07135
- qlib 官方 benchmark（Alpha360 · CSI300）：Transformer / GATs / HIST 等 IC/ICIR/Rank IC/年化对照（须在本项目 A 股数据上复测，不照搬）

---

## 10. 决策记录（2026-07-05 已定）

1. ✅ Phase1 模型 = **HIST-industry**（吃现成行业成分 + 动态相关性贴合轮动 + 概念超图前身）；GATs 作更轻备选。
2. ✅ 特征口径 = **Alpha360 原始量价底料（即时 handler，不入因子库）+ 私有趋势因子（主力）+ Alpha158 择优子集（补充非主力）+ 关系结构（核心赌注）**；不依赖 Alpha158 手工因子作主力。
3. ✅ 接入采用**方案 A（自定义模型内截面重组）**，不改 QE 数据装载主干。

## 11. Alpha360 底料的 QE 落地计划（源码移植方案对比）

背景：qlib `Alpha360DL.get_feature_config()` 是确定性表达式列表——`Ref($close,i)/$close`、`Ref($open,i)/$close`、`Ref($high,i)/$close`、`Ref($low,i)/$close`、`Ref($volume,i)/$volume`（i=59..0），即**最近 60 天原始量价按当日归一化**，6 字段 × 60 = 360 维。本质是**原始底料，不是命名截面 alpha**。

三种落地方式对比：

| 方案 | 做法 | 优 | 劣 | 采纳 |
|---|---|---|---|---|
| **A 即时 handler（推荐）** | 自定义模型工作区内按 `Alpha360DL` 规范从 qlib bin 即时生成 360 维底料 | 与 qlib HIST/GATs 原生一致；零因子库改动；无冗余列；可复算 | 每次训练重算（成本低，表达式简单） | ✅ 主 |
| B 因子库注册 360 列 | 把 360 个 `Ref` 表达式按 QE 因子库流程注册为命名因子 | 与因子库治理统一、可被其他实验复用 | 360 近重复滞后列冗重、语义错位（非 alpha）、覆盖率/指标计算无意义 | ✗ |
| C 折中：注册「基础特征包」 | 仅登记 6 个原始字段 + 一个文档化的滞后窗口 spec（不落 360 列） | 治理可见 + 轻量 | 仍需 handler 展开；额外抽象层 | 可选（若治理强制） |

**结论**：Alpha360 底料用**方案 A（即时 handler，源码规范移植自 qlib `Alpha360DL`）**，不塞因子库；真正的差异化 alpha（私有趋势/筹码/资金流/基本面因子 + Alpha158 择优子集）才走因子库常规注册流程。qlib bin 已含 `$close/$open/$high/$low/$volume/$vwap`，无需新增数据。
