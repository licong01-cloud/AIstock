# P2 关系型模型(HIST / MASTER)接入方案与可行性评估 (2026-07-08)

> 分类：分析报告 / 设计方案（docs/analysis）
> 作者：strategy session · 状态：待评审（先出方案，接入实现后续按 feature workflow 拆单）

## 变更记录 (Changelog)
| 版本 | 日期 | 内容 | 作者 |
|---|---|---|---|
| v1.0 | 2026-07-08 | 首次落档：动机、候选模型、QE 管线 gap、分阶段接入方案、可行性+成本、验收 | strategy session |

---

## 1. 背景与动机

### 1.1 单腿逐股模型已饱和
截至 2026-07-08，单腿逐股模型的因子/模型/超参/训练周期/seed 空间已系统扫完并**饱和**：
- 最优单腿收敛在 **年化 0.45–0.49 / rank_ic ~0.11**（LGBM-F12 0.466、TCN-def 0.492、LSTM-def 0.433）。
- 配置间差异多在 ±0.02–0.05（单窗+少 seed 下统计接近）。
- **普通 Transformer（Phase0 qe_20260704_125601_75f5）与 ALSTM（R1 L9/10）均退化**：rank_ic ≈ 0 甚至转负，学不出信号。

### 1.2 逐股模型的结构性局限
LGBM / LSTM / TCN / 普通 Transformer **全部逐股独立预测**，不建模跨股/板块关系。A 股的**板块联动、行业轮动、概念传染、龙头-跟风**是强结构性机制——逐股模型天然拿不到这部分 alpha。

### 1.3 文档方向对齐
`phase0_multi_alpha_orthogonality_matrix_20260615.md` 已指令"重心转向多 Alpha 组合回测 + 关系型探针"。关系型（P2「关系双探针 HIST-industry + MASTER」）是路线图中**唯一未测的新信号机制**，且**连详细设计都缺**——本文档补齐。

---

## 2. 候选关系型模型

### 2.1 HIST（预定义概念的层次信息聚合）
- **可得性**：`qlib.contrib.model.pytorch_hist.py` **已内置**。
- **机制**：预定义概念图（行业/概念板块）+ 隐式概念挖掘，聚合"同概念股票"的信息增强个股预测。
- **额外输入**：`stock2concept` 矩阵（`np.load(self.stock2concept)`，forward 时按 `stock_index[batch]` 索引取每只股的概念向量）。
- **数据**：`market.sw_index_member`（个股↔SW二级行业成分）可直接构建行业版 stock2concept；概念版需 stock↔概念成分源（见 §5）。
- **训练接口**：HIST 有**自己的 fit/predict**（内部处理 concept 矩阵 + stock_index），**不走 GeneralPTNN**。

### 2.2 GATs（图注意力）
- **可得性**：`qlib.contrib.model.pytorch_gats(_ts).py` **已内置**。
- **机制**：图注意力**从特征学习**股间关系，**无需外部概念矩阵**。
- **额外输入**：仅 per-stock 特征（每日截面批）。**数据成本最低**。

### 2.3 MASTER（市场引导的股票 Transformer）
- **可得性**：qlib **无内置** → **需自研代码**（2024 论文实现）。
- **机制**：市场信息门控的跨股注意力 + 时序注意力。
- **额外输入**：per-stock 特征 + **市场指数特征**（gating）。**成本最高**。
- **可得替身**：qlib 内置 `pytorch_igmtf.py`（IGMTF，跨时序特征关系）、`pytorch_tra.py`（TRA，时序路由）可作 MASTER 的近似先验探针，避免先投自研。

---

## 3. 现状 Gap：QE 管线喂不了关系型模型

| Gap | 说明 |
|---|---|
| **只传特征** | QE 的 NN 走 `GeneralPTNN + pt_model_uri + TSDatasetH`，只把**特征张量**传给内层模型。 |
| **额外输入无通道** | HIST 需 `stock2concept` + `stock_index`；MASTER 需市场特征——GeneralPTNN **不传这些**。 |
| **模型类分支缺** | `config_composer.py` 只有 `LGBModel / GeneralPTNN / AIStockXGBModel / LambdaRankModel` 分支，**无 `HIST` / `GATs` 类分支**；HIST 不走 GeneralPTNN，需新分支。 |
| **关系矩阵无产物** | 无 PIT 的 stock2concept `.npy` 产物；需新增数据构建步骤。 |
| **stock_index 对齐** | concept 矩阵行序必须与 qlib instruments 顺序严格对齐（最易错工程点）。 |
| **截面批** | HIST/GATs 需每日**全截面批**；TSDatasetH 的批构造/内存需验证。 |
| **分钟执行兼容** | 关系型出**日频信号** → V25 分钟执行（与 LGBM 同构，应兼容但须 smoke）。 |

---

## 4. 分阶段接入方案

### Phase R0：GATs 探针（最低成本，先验证"关系型有没有用"）
- **为何先做**：GATs **不需外部概念矩阵**（从特征学关系），改动最小，能最快回答"关系型注意力在本因子集上是否 > 逐股"。
- **改动**：`config_composer` 加 `class: GATs`（`qlib.contrib.model.pytorch_gats_ts`）分支；验证 TSDatasetH 截面批可喂 GATs。
- **判赢**：GATs top-K 年化 / rank_ic 相对最强逐股腿（LGBM-F12 0.466 / TCN 0.492）是否升 **且** 与逐股腿预测正交（orth_IC > 0.02）。
- **止损**：若 rank_ic 仍 ≈ 0（如普通 Transformer/ALSTM）→ 诚实证伪，关系型注意力在本设置无效，不投 R1+。

### Phase R1：HIST-industry（行业图，数据已备）
- **前提**：R0 证关系型有正 alpha。
- **数据**：构建 `stock2concept` = `sw_index_member` 的 SW 二级行业 one-hot（首版静态快照，标注 PIT 风险；正式版 PIT 化）。
- **改动**：`config_composer` 加 `HIST` 分支 + concept 矩阵路径注入 + `stock_index` 与 qlib instruments 对齐 + HIST fit/predict 适配 QE 回测/上传契约。
- **判赢**：同 R0，且 vs GATs 谁更强（显式行业先验 vs 学习关系）。

### Phase R2：HIST-concept / MASTER（高成本，条件触发）
- **仅当 R0/R1 证关系型有效**才投。
- **HIST-concept**：需 stock↔概念成分源（核 tushare THS 概念成分；`sina_board_daily` 只有板块聚合无个股成分，不够）。
- **MASTER**：自研模型代码 + 市场指数特征管线 + composer 接入；或先用内置 IGMTF/TRA 作替身探针。

---

## 5. 可行性 + 成本评估（诚实）

| 模型 | qlib内置 | 额外数据 | QE 改动 | 综合成本 | 阶段 |
|---|---|---|---|---|---|
| **GATs** | ✓ | 无（截面批） | composer 分支 + 截面批验证 | **低-中** | R0 首选 |
| **HIST-industry** | ✓ | stock2concept（`sw_index_member` 已备） | composer HIST 分支 + 矩阵注入 + stock_index 对齐 + fit/predict 适配 | **中** | R1 |
| **HIST-concept** | ✓ | stock↔概念成分（**待核 tushare THS**） | 同 R1 + 概念成分数据管线 | **中-高** | R2 |
| **MASTER** | ✗ 自研 | 市场指数特征 | 自研模型 + 市场特征管线 + composer | **高** | R2 条件触发 |

### 关键工程风险
1. **stock_index 对齐**：concept 矩阵行序 ↔ qlib instrument 顺序，最易错，**必须单测**（错位=喂错概念=静默错误）。
2. **PIT 未来函数**：行业/概念成分随时间变；stock2concept 应 PIT，否则用未来成分过滤历史=未来函数。首版可静态快照 + **显式标注风险**，正式版 PIT 化。
3. **截面批内存**：HIST/GATs 每日全截面（~5000 股），批构造与显存需实测。
4. **RDAgent 动态加载**：v3 §6.2"二期拓展新架构"能力未落地。HIST/GATs 走 **qlib 内置类**可绕过自研加载；MASTER 自研则必须落地该能力。
5. **分钟执行**：日频信号 → V25，同 LGBM，应兼容但须 smoke 一轮。

---

## 6. 验收标准（反过拟合，与既有腿一致）
- **判赢**：关系型 top-K 年化 / rank_ic 相对最强逐股腿（0.466/0.492）**升**，**且** 与逐股腿预测**正交**（orth_IC > 0.02）→ 可作新腿补组合广度。
- **鲁棒**：≥3 seed；多窗（非单窗）稳定。
- **诚实止损**：若 rank_ic 仍 ≈ 0（Transformer/ALSTM 式退化）→ 证伪，不强行上线。
- **数据诚信**：PIT 风险若用静态快照必须在结果里显式标注，不得当作真 PIT 结论。

---

## 7. 建议路径与分工
1. **R0 GATs**（~1-2 周，纯用现有能力 + composer 小改）→ 若正 → **R1 HIST-industry** → 若正交且升 → **组合层纳入**（与 LGBM-F12/TCN/LSTM 组合）→ **R2 视情**。
2. **MASTER 降级为条件触发**：R0/R1 证关系型有效后再投自研；先用内置 IGMTF/TRA 作近似。
3. **分工**：composer/管线改动 + 模型接入 = Codex（走 feature/BUG workflow）；stock2concept 矩阵数据构建 + 实验编排 + Tier2 = strategy session。

## 8. 决策点（待用户）
- 是否按 R0(GATs)→R1(HIST-industry) 顺序推进？
- MASTER 是否确认降级为条件触发（先不自研）？
- 首版 stock2concept 允许静态快照（标注 PIT 风险）先探路，还是要求一次到位 PIT？
