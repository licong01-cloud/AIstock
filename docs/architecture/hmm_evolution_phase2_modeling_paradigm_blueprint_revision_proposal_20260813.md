# HMM 演进 Phase 2 建模范式与验收门禁 蓝图修订提案

- 文档类型：蓝图修订提案（Blueprint Revision Proposal，架构分析 / 决策输入）
- 日期：2026-08-13
- 状态：`PROPOSAL_PENDING_USER_APPROVAL`（仅为决策输入；不修改蓝图、不改模型、不写代码/DDL/runtime）
- 提案人：Claude Code（应用户要求生成，供用户裁决）
- 父级唯一产品目标权威：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md` v2.24
- 关联实现级设计：`docs/architecture/hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md`
- 关联 Design Acceptance Index：F-011（引用，不新增）、F-012、F-013
- 任务级别：T3（架构 / 生产关键路径决策输入），本提案本体按 T0 docs-fast-new 交付
- 授权边界：本文件是**分析与建议**，不是已批准的设计变更。它不修改父蓝图或 Phase 2 详细设计正文，不选择 seed、不训练/重训 HMM、不写 model/READY、不执行 selection/D5/D6、不触碰 `hmm_risk.*`/`hmm_evolution.*` 数据、不安装依赖、不激活 runtime。所有生产门禁在本提案范围内均为 `noop`。

---

## 0. 摘要（Executive Summary）

Phase 0/1 已真实外部验收并进入生产（worker v3），方向正确、执行扎实。**Phase 2 的产品价值（板块状态预测 + 风险预警）当前功能交付为 0**，卡在 F-011「模型验收」，自 2026-07-22 详细设计定稿至今持续 `blocked`。

大量诊断（C-008-A/B1、DIAG-02/03/04、REFIT-01/02/03、TRAIN-STABILITY-DIAG-01、TRANSITION-DWELL-B，累计数千次可复现 fit）已**穷尽性地证明**：这不是代码 bug、不是数据/NA 缺失、不是 seed 选择、也不是阈值问题（P2-1 独立重算 0 mismatch）。真正的 blocker 是**建模范式与验收门禁之间的统计阻抗失配**——当前门禁要求「131 个 L2 + 31 个 L1 各自独立的 3 态对角高斯 HMM、family-global 单一 seed、禁止 per-sector 拼接、全或无 READY」，而固定训练/验证窗口内的申万板块数据在统计上无法整体满足该门禁（完整候选 seed = 0）。

本提案给出：根因分析（依据 + 分析过程 + 结论）、与产品目标的一致性检验、五个整改方向（各自映射到 F-011/F-012/F-013 blocker、最小替代、成本、不做的业务影响）、建议的蓝图修订条目（proposed change-set，供用户裁决后再执行）、验收/测试方案与本提案的可合入标准。

**核心建议**：将「建模范式选择」上升为蓝图级决策；解锁一个受控的 modeling-paradigm spike（首选统计跳跃模型 / 分层 HMM），并允许「诚实子集交付」路径（eligible sector 出状态、其余显式 `insufficient_structure`），以在不违反 advisory-only / PIT / 禁止静默失败原则的前提下解锁产品价值。本提案不改变任何既有 hard semantic authority，也不主张放宽已有诚实性边界。

---

## 1. 目的、范围与非目标

### 1.1 目的

为「Phase 2 F-011 长期 blocked」提供一份可审计的根因分析与整改方向决策输入，使用户能够在**建模范式层面**（而非继续在同一合同内做诊断）做出裁决。

### 1.2 In scope

- 基于两份权威文档（父蓝图 v2.24、Phase 2 详细设计）与其登记证据的事实基线复述与根因归纳。
- 建模范式层面的替代方向评估，含量化行业最佳实践与公开研究依据。
- 面向父蓝图与 Phase 2 详细设计的**建议修订条目**（proposed change-set，非执行）。
- 本提案本体的验收/测试方案与可合入标准。

### 1.3 Non-goals（硬边界）

- 不修改 `hmm_evolution_and_risk_management_system_design_20260716.md` 或 `hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md` 的正文；建议以「proposed change-set」表格呈现，由用户批准后另起实现级 F2 设计与 feature workflow 执行。
- 不改变 Phase 2 已批准的业务语义：advisory-only（F-012）、PIT/因果、禁止静默失败、hard semantic authority、两 family 完整性的**原则**保持不变；本提案讨论的是达成这些原则的**建模手段**是否需要更换，以及「完整性」的验收口径是否应在产品目标层重新定义。
- 不新增任何未经批准的模型阈值、研究淘汰门禁、运行时审批或发布阻断（遵守 DESIGN-COMPLIANCE-001 第 4 项）。
- 不执行 fit/selection/D5/D6、不写 model/READY、不做 DDL/DML/依赖/runtime 动作。

---

## 2. 事实基线（Evidence Baseline）

以下事实全部引自两份权威文档及其登记的 append-only 证据，用作后续分析的**依据**。数值与 identity 保留原文口径以便审计。

| 编号 | 事实 | 来源 |
|---|---|---|
| E-1 | 严格验收进度 `11/17=64.71%`；F-001~F-010A 已 verified，Phase 1 production v3 已激活。 | 父蓝图 §1.2、§13 |
| E-2 | 正式 B3 producer `e2c01bae…`，两次 fresh-process 合计 `5184/5184` fits，bitwise receipt 一致；D5 已执行、validation/future utility 未用于 selection、selection 后未 refit。 | 详细设计 §24 |
| E-3 | D5 仅 `legacy_covfix:L1` 选中 seed 43；`autocycle_all_core:L1/L2` 与 `legacy_covfix:L2` 无 eligible D5 candidate；formal rejection 为 9/74/67 seed-sector pairs。 | 详细设计 §24 |
| E-4 | P6（producer `0ab6dec3`，`autocycle_all_core:L2`）BUG-1029 后零 refit D6：assignment `131/131` accepted，evidence `120/131` accepted、`11/131` failed；`fits=0`、`refit_count=0`、`selection_reexecuted=false`。11 个失败 sector：`801038.SI`、`801127.SI`、`801204.SI`、`801223.SI`、`801231.SI`、`801711.SI`、`801723.SI`、`801733.SI`、`801738.SI`、`801743.SI`、`801971.SI`。（注：与 E-3 的 5184-fit `legacy_covfix:L1` seed43 是不同 family/level 的两次运行，均为 seed 43。） | 详细设计 §11、§24 |
| E-5 | `TRAIN-STABILITY-DIAG-01`（producer `7d57d57e…`）：8 个 seed 的双窗口 stable sector 数为 `108/108/97/103/109/105/104/106`，**完整 seed = 0**；故「仅加 D5 stability gate」会清空候选集，不采用。 | 详细设计 §24、父蓝图 §11.2 |
| E-6 | `TRANSITION-DWELL-B`（treatment producer `29417ceb…`、冻结 source `2ae9df85…`）：双 fresh-process `2096/2096` fits、payload hashes bitwise 一致；**完整候选 seed 仍为 0**，状态 `diagnostic_complete_no_complete_candidate`。未执行 selection/D5/D6、未写 model/READY/DB/runtime。 | 详细设计 §11、§24 |
| E-7 | P2-1 根因闭合：11 项均为 182/182 完整输入、availability event=0、assignment 131/131 accepted、独立 structure 重算 0 mismatch、full-train D4-03 均 accepted。结论：selected seed 43 在 validation 的真实 hard-state 结构能力不足，**非程序/数据 bug**，不改阈值或 reselect。 | 详细设计 §24.1、父蓝图 §11.2 |
| E-8 | C-008-A：legacy/covfix 在 seed 42..49 中无 31/31 seed，`801780.SI` 八 seed 均缺至少一个 hard validation state；autocycle 部分 seed 达 hard-label 31/31，但两 family 每个 seed 都至少一个 sector 出现 negative likelihood delta。 | 详细设计 §4.3.2 |
| E-9 | 观测维度：legacy/covfix family = 7 维 base observation；autocycle/all-core family = 20 维（7 base + 13 sector-factor）；3 态对角 GaussianHMM。 | 详细设计 §4.3.1 D/E |
| E-10 | 训练窗口 `2022-01-01..2024-06-30`（约 600 交易日）；唯一 semantic validation 窗口 `2024-07-01..2025-03-31`（约 180 交易日）。 | 详细设计 §4.3.1 F、D2 |
| E-11 | 数值补丁事实：DIAG-04 legacy 有 8 个 raw cluster zero-variance cell 被 scale-aware initialization 收缩；self-transition floor `0.3`、transition Dirichlet `alpha=0.1`、MAP prior `ν=1.0`；L2 历史 artifact `covariance_fixed=true`、legacy `covariance_anomaly_count=1764`、autocycle `416`。 | 详细设计 §4.3.1 F、D3、D4-L2-AUDIT-01 |
| E-12 | 父蓝图 §11.1 自审已登记三类偏离：**权威倒置、横向平台优先、大体积证据投入**；v2.20 已做目标收敛，但同样模式（又一轮 2096-fit 的 TRANSITION-DWELL-B）在 v2.23/v2.24 仍在继续。 | 父蓝图 §11.1、变更历史 |
| E-13 | 明令禁止方向：不自动改 tau/self-center/seed/grid、不放宽 D4/D6、不 validation reselect、不 per-sector stitching、**不进入 HSMM**。 | 详细设计 §24.1 第 2 条、父蓝图 §11.2 |
| E-14 | F-012 `DESIGN_READY_USER_APPROVED`、F-013 `PENDING_UPSTREAM_MODEL_SET`：API/UI/分析源码未实施，且被 F-011 READY model set 阻塞。 | 详细设计 §24、父蓝图矩阵 |

---

## 3. 根因分析（依据 → 分析过程 → 结论）

### 3.1 现象（Symptom）

在方法学正确、可复现性极强、PIT/因果纪律严格的前提下，自 2026-07-22 详细设计定稿以来经数千次 fit、多轮 fresh-process 受控实验，**没有任何 seed 能产出满足门禁的完整 model set**（E-2~E-6）。F-011 因此持续 `blocked`，进而 F-013 的全部产品价值（预测/预警/API/UI/回测报告）为 0（E-14）。

### 3.2 分析过程：逐一排除已被证据否定的假设

项目已经用高质量证据完成了大量假设检验。本节把这些既有证据重组为「排除法」链条——这正是根因定位的**分析过程**：

1. **假设：程序或数据缺陷 → 已排除。** P2-1 独立重算 0 mismatch、182/182 完整输入、availability event=0（E-7）。
2. **假设：换全局 seed 可恢复 → 已排除。** C-008-A 证明 seed 42..49 无完整 seed（E-8）；TRAIN-STABILITY 证明双窗口 stable sector 完整 seed=0（E-5）。
3. **假设：加 D5 stability gate 可筛出稳定候选 → 已排除（会清空候选集）**（E-5）。
4. **假设：transition/dwell 先验修订（TRANSITION-DWELL-B）可恢复 → 已排除。** 双 fresh-process 2096 fits 后完整候选 seed 仍为 0（E-6）。
5. **假设：放宽阈值 / validation reselect / per-sector stitching → 被诚实性边界与既有决策拒绝**（E-13，且这些会破坏 hard semantic authority 与两 family 完整性，属于 DESIGN-COMPLIANCE 违规）。

**分析过程的关键推论**：上述 1–5 已覆盖「在当前建模范式（每 sector 独立 3 态对角高斯 HMM + family-global 单 seed + all-or-nothing）之内」几乎所有可动的自由度。既然范式内的每个杠杆都被证据否定，**剩余的唯一变量就是范式本身与门禁定义**。继续在范式内做第 N 轮诊断，其边际信息量已趋近于 0（E-12 印证：同一模式在自审后仍在重复）。

### 3.3 根因结论

**根因 = 建模范式与验收门禁之间的统计阻抗失配。** 具体分解为四个相互叠加的子因，每个都有直接依据：

- **RC-1（门禁刚性，主因）**：`L1 31/31 ∧ L2 131/131`、`family-global 单 seed`、`禁止 per-sector 拼接`、`任一 sector 失败即整 family blocked` 是**自设约束**，不是产品目标要求。它把 162 个（31 L1 + 131 L2）统计难度不均的子问题串联成一个合取，任一薄弱环节即全局失败。依据：E-4 显示失败只是少数 sector（11/131），但全或无门禁把它们与整体绑定，使多数已可建模的 sector 无法交付。
- **RC-2（固定窗口内 hard-state 结构不足）**：源文档确立的失败性质是「selected seed 在 validation 的真实 hard-state 结构能力不足」（E-7），即在固定 ~600 训练日 + ~180 单一 validation 日内，部分 sector 无法稳定地以足够 hard occupancy 访问全部三态。依据：`801780.SI` 八 seed 均缺至少一个 hard validation state（E-8，legacy C-008-A 例）、`801970.SI` 在 DIAG-02/D4-03-B validation singleton（详细设计 §4.3.2 DIAG-02/D4-03-B）、TRAIN-STABILITY 双窗口完整 seed=0（E-5）。这是「固定窗口 × 三态硬门禁」下的结构属性，不是拟合技巧能补的。**注**：源文档并未把失败归因于板块「规模窄/流动性低」；`801780.SI` 等被举证的失败项并非全部为窄板块，因此「窄 sector 更易失败」只能作为**待 spike 验证的假设**（见 §9），不作为已确立结论。恰因如此，RC-2 更指向门禁刚性（RC-1）而非板块规模。
- **RC-3（emission 过参数化 + 分布失配）**：autocycle family 用 3 态对角高斯拟合 20 维、~600 样本，且对角假设要求 20 个相关因子条件独立，金融日收益又是尖峰厚尾。依据：zero-variance cell、`covariance_anomaly_count` 1764/416、self-transition floor/Dirichlet/MAP prior 全是给病态协方差与低 occupancy 打的补丁（E-11）。补丁越多，越说明范式与数据不匹配。
- **RC-4（sector 独立、无跨 sector 借力）**：131 个 L2 各训各的，是一组**短且异质**的独立序列，样本不足的 sector 无法向样本充足的 sector/市场借强度。这既是 RC-2 的放大器，也是信息浪费。依据：范式设计本身（E-9），以及失败集中在少数 sector（E-4）。

### 3.4 明确不属于根因（防止误修）

- **不是** seed/tau/self-center/grid（E-5/E-6/E-8 已证）。
- **不是** 阈值过严可放宽（放宽会违反诚实性，且 DIAG-04 显示阈值敏感性不构成 gate）。
- **不是** 证据不足需再补诊断（边际信息量≈0，E-12）。
- **不是** 数据/NA/程序缺陷（E-7）。

---

## 4. 与产品目标的一致性检验

父蓝图 §1.0 将 Phase 2 最终产品结果固定为：「基于 t-1/PIT 数据生成申万 L1/L2 板块**状态预测**，识别状态转移并形成可解释**风险/机会预警**，提供历史时序、误报/漏报和稳定性分析，并通过真实 API/UI 展示；只作研究分析，不进入交易决策链。」

据此逐条检验当前门禁是否为产品目标所必需：

| 门禁约束 | 是否产品目标所必需 | 结论 |
|---|---|---|
| L1/L2 板块状态可解释、PIT/因果、advisory-only | 是（产品语义核心） | **保留** |
| 每个 sector 各自独立 3 态高斯 HMM | 否（是「如何得到状态」的一种实现选择） | 可替换 |
| family-global 单一 seed | 否（是防 cherry-pick 的手段，非目标） | 可用预注册协议替代 |
| 131/131 ∧ 31/31 全或无 READY | 否（产品只需「可解释的板块状态」，不要求每个板块都必须可建模） | 可改为「诚实子集 + 显式不可建模」 |
| 禁止 per-sector 拼接 | 部分（防过拟合合理，但与「分层/预注册 per-sector」不冲突） | 精化定义 |

**检验结论**：真正服务产品目标的是「可解释、PIT、advisory-only 的板块状态与预警」；而阻塞交付的 RC-1（全或无门禁 + 单 seed + 禁 per-sector）**不是产品目标要求，是自设的防过拟合工程约束**。防过拟合的目标应当保留，但其**实现手段**（全或无 + 单 seed）恰好是 blocker，可用更优手段达成同样的防过拟合目的（见 §5）。

> 说明：防 cherry-pick 的正确形态是「**看 validation 之前就固定的预注册协议**」。在固定协议下允许 per-sector 参数（如分层模型的 sector-level 参数）**不构成 cherry-pick**；当前设计把「per-sector 参数」与「per-sector 事后挑 seed」混为一谈，因而过度约束。

---

## 5. 整改方向（Remediation Options）

每个方向按父蓝图 §11.1 的要求给出：**直接解除的 F-011/F-012/F-013 blocker、最小替代方案、额外成本、不做的业务影响**。所有方向均保持 advisory-only / PIT / 禁止静默失败。

### 方向 A（首选）：更换模型类为统计跳跃模型（Statistical / Jump Models）

- **做法**：以带跳跃惩罚的状态序列估计（jump model / 统计跳跃模型，及其连续/软变体）替代逐 sector 高斯 HMM。用一个跳跃惩罚参数 λ 内生控制状态持续性。
- **直接解除的 blocker**：RC-1/RC-2/RC-3。跳跃模型参数极少、对短序列与初始化/seed 鲁棒、原生控制持续性——直接消解 seed 不稳定、协方差病态、self-transition floor/dwell 手工补丁（E-6/E-8/E-11 所示痛点）。连续/软跳跃模型可给出软状态概率，正好填补 `state_confidence` 目前只能置 null 的空缺（详细设计 §4 状态置信度语义）。
- **依据（公开研究）**：Nystrup, Madsen & Lindström, "Learning hidden Markov models with persistent states by penalizing jumps" (Expert Systems with Applications, 2020)；Nystrup, Kolm & Lindström 关于 jump models / feature selection in jump models 的系列工作（2020–2021）；这是金融 regime detection 的当前主流稳健方法，直接针对「持续性与短窗口可识别性」而设计。
- **最小替代方案**：若不换整类，可只引入「跳跃惩罚」作为 HMM 之上的状态平滑层（次优，仍受 RC-3 影响）。
- **额外成本**：一个受控 spike（新 estimator 实现 + 在代表性 sector 上的对比实验），估计中等；需新的 F2 spike 设计与验收行。
- **不做的业务影响**：F-011 大概率维持 blocked，Phase 2 产品价值持续为 0。

### 方向 B（首选并行）：分层 / 部分池化 HMM（Hierarchical / partial-pooling）

- **做法**：不再训 131 个独立模型；让 transition/emission 的先验在 sector 间共享（经验贝叶斯 / 分层贝叶斯），每个 sector 参数向 pooled 均值收缩。
- **直接解除的 blocker**：RC-2/RC-4（并缓解 RC-1）。样本不足的 sector 自动向样本充足的 sector/市场借强度，缺态问题在建模层被吸收；这是「per-sector 参数」的**原理性正确、且不构成 cherry-pick** 的形态（§4 说明）。
- **依据**：分层贝叶斯 HMM / 经验贝叶斯 pooling 是处理「大量短异质序列」的标准手段；sticky HDP-HMM（Fox, Sudderth, Jordan & Willsky, "A Sticky HDP-HMM with Application to Speaker Diarization," Annals of Applied Statistics, 2011）进一步允许数据决定状态数与持续性，可作为「是否强制 3 态」的对照。
- **最小替代方案**：仅共享 transition 先验（保留 per-sector emission），实现更轻。
- **额外成本**：中等偏高（需分层估计实现与收敛诊断）。
- **不做的业务影响**：同方向 A。

### 方向 C（低成本缓解，可与 A/B 叠加）：降维 + 厚尾 emission

- **做法**：把 autocycle 的 20 维压到 3–5 个鲁棒 regime 特征（趋势、波动、breadth、资金流）；对角高斯 emission 换 Student-t。
- **直接解除的 blocker**：RC-3。降维直接缓解协方差不可识别与 zero-variance；t-emission 内生厚尾，减少尾部事件破坏 occupancy/协方差（E-11）。
- **依据**：Bulla, "Hidden Markov models with t components" (Quantitative Finance, 2011) 证明 t-emission 提升金融序列的状态持续性与稳健性；20 维对角高斯在 ~600 样本 3 态下的过参数化是公认风险。
- **额外成本**：低（特征与 emission 替换，复用现有训练骨架）。
- **不做的业务影响**：即使不换模型类，也会持续与协方差病态搏斗。

### 方向 D（首选并行，解锁产品价值）：诚实子集交付

- **做法**：将 F-011 READY 的「全或无」改为「**eligible sector 出状态、不 eligible 的 sector 显式返回 `insufficient_structure` typed status**」，family/整体 READY 定义为「所有 eligible sector 均通过 + 不可建模 sector 均有显式 typed 证据」。
- **直接解除的 blocker**：F-013（产品价值）。允许 API/UI/回测分析基于**子集 READY** 先落地（E-14 当前被 all-or-nothing 卡死），用户可看到真实价值；11 个失败 sector（E-4）以诚实状态呈现。
- **与既有原则的一致性**：这**恰恰符合**详细设计的「禁止静默失败、禁止 neutral 补态、如实保留 11 个失败 sector」价值观——把「不可建模」如实展示，比「全或无导致零展示」更符合诚实性原则，也不改变任何 hard semantic authority。
- **最小替代方案**：先只在内部报告层面呈现子集，UI 暂不注册（更保守）。
- **额外成本**：低–中（需在 F-011/F-013 验收口径与 UI 状态机中增加 `insufficient_structure` 语义，属于既有 error-contract 家族的扩展）。
- **不做的业务影响**：即使 A/B/C 让多数 sector 可建模，只要仍有个别顽固 sector，全或无门禁就会继续卡死整个产品。

### 方向 E（可选，面向「预警」子产品）：变点检测（Changepoint Detection）

- **做法**：对「trending→fading 触发 HIGH」这类**告警**语义，用贝叶斯在线变点检测（BOCPD）或在线跳跃检测，直接检测 regime 切换点，而非要求全时段逐 sector 状态。
- **直接解除的 blocker**：为 F-011 提供一条与「完整 state model set」解耦的**预警**路径，使 F-013 的预警面板可独立于「131/131 READY」交付。
- **依据**：Adams & MacKay, "Bayesian Online Changepoint Detection" (2007)；变点检测的可识别性与在线增量性质与「预警」产品高度契合。
- **额外成本**：中（新增独立 detector 与其验收）；需明确它与「板块状态热力图」的关系，避免语义混淆。
- **不做的业务影响**：预警产品继续被「完整状态模型」阻塞。

### 5.1 推荐组合

**A（跳跃模型 spike）+ B（分层）择一先做 spike，并行 D（诚实子集交付）+ C（降维/t-emission 作为低成本增强）**；评估协议改为 walk-forward 多折（§9）。E 作为预警子产品的可选增强。理由：A/B 攻 RC-1/2/3/4 的建模根因，D 立刻解锁 F-013 产品价值且强化而非削弱诚实性，C 成本最低可立即叠加。

---

## 6. 建议的蓝图修订条目（Proposed Change-Set，非执行）

以下为**建议**，供用户批准。批准后应另起实现级 F2 spike 设计并走 feature workflow；本提案不代为修改正文。

| 序号 | 目标文件 / 位置 | 现状 | 建议修订 | 映射 blocker |
|---|---|---|---|---|
| CS-1 | 父蓝图 §1.0 反过度工程边界 | 只允许在既有范式内推进；范式变化无显式授权口径 | 增加一条：「当范式内自由度已被证据穷尽否定（引用 E-5/E-6/E-8）时，允许提出 modeling-paradigm spike 作为解除 F-011 的候选路径，须经用户批准」 | RC-1 |
| CS-2 | 父蓝图 §Gate 2 / P2-2；详细设计 §24.1 第 2 条 | P2-2 锁定为「仅聚合既有 child evidence」「不进入 HSMM/不改 seed/不放宽 D4/D6」 | 将 P2-2 的目标从「在范式内找精确 seed 决策」改为「**范式决策**」：解锁受控 spike（方向 A/B/C 至少其一），HSMM/跳跃模型/分层作为**候选**而非禁止项；仍禁止未预注册的事后挑 seed | RC-1/2/3/4；E-13 |
| CS-3 | 父蓝图 F-011 验收；详细设计 §4.3 READY 定义 | READY = L1 31/31 ∧ L2 131/131 全或无 | 增加**可选完成路径**：READY 可定义为「所有 eligible sector 通过 + 不可建模 sector 均有 `insufficient_structure` typed 证据」；保留 all-or-nothing 作为「强完整性」标签，新增「诚实子集」标签 | F-013；RC-1 |
| CS-4 | 详细设计 §4.3.1 观测契约 | autocycle 固定 20 维对角高斯 | 允许 spike 内评估降维特征集与 t-emission；正式采用须新 model-set version（不原地漂移） | RC-3 |
| CS-5 | 父蓝图 §1.2 进度口径 | 单一 `11/17` 严格进度 | 增加「产品价值交付」维度（Phase 2 功能完成度独立计数），使「关键路径阻塞」不被 Phase 0/1 完成稀释 | 报告口径 |
| CS-6 | 父蓝图 Design Acceptance Matrix / 详细设计 §19 | 无 spike 验收行 | 新增 proposed 验收行（spike 对比实验 + walk-forward 协议 + 诚实子集口径），状态 `PROPOSED_PENDING_USER_APPROVAL` | 追踪 |

> 说明：CS-2 保留「预注册、看 validation 前固定协议、禁止事后挑 seed、禁止 per-sector 拼接式 cherry-pick」的防过拟合意图；它解锁的是**范式选择**，不是放宽诚实性。

---

## 7. 进度口径修订建议

`11/17=64.71%` 在报告层面具误导性：Phase 0/1（11 行）已 verified 是真的，但 Phase 2 的**产品价值功能完成度为 0** 且卡死。建议（CS-5）在保留 17 行严格进度的同时，并列一个「Phase 2 产品价值 = 0/N（阻塞于 F-011 建模范式决策）」的独立口径，避免「六成完成」掩盖「关键路径完全阻塞」。此建议不改变任何验收计数规则，仅增加报告维度，符合 DESIGN-COMPLIANCE-001 第 4 项（不新增门禁）。

---

## 8. 风险与缓解（本提案与整改方向的风险）

| 风险 | 影响 | 缓解 |
|---|---|---|
| 换模型类引入新复杂度或新的过拟合面 | 中 | spike 用预注册协议 + walk-forward 多折 + 与现有 HMM 同冻结输入对照（§9）；正式采用须新 version 与完整验收 |
| 「诚实子集」被误用为静默降级 | 高 | `insufficient_structure` 必须是 typed、可见、带证据的显式状态；UI/报告如实标注，禁止 neutral 补态（沿用既有 error-contract） |
| spike 沦为又一轮无收敛诊断 | 中 | spike 有明确的 kill-criteria：若在代表性顽固 sector（含 E-4 的 11 个 D6 失败 sector 与 E-8 的 legacy 缺态例 `801780.SI`）上仍不达标，则结论直接回到「诚实子集交付」而非再开诊断 |
| 修订被解读为放宽 hard semantic authority | 高 | 明确：CS-1~CS-6 不改任何 hard assignment/PIT/advisory-only 语义；只改「建模手段」与「完整性口径」 |
| 提案本体越权（改蓝图/跑模型） | 高 | 本提案仅为 proposed change-set，不改正文、不跑 fit（§1.3 边界；§12 门禁全 noop） |

---

## 9. 测试方案与结果验证方法（供批准后的 spike 使用）

本提案本体不含代码测试；本节定义**批准后 spike 的验收协议**，供 CS-6 引用（现在不执行）。

- **对照实验设计**：固定与现行 B3 相同的冻结输入、PIT/因果纪律、train/validation 窗口来源；在一组**代表性 sector**上对比「现行 3 态高斯 HMM」vs「方向 A 跳跃模型」/「方向 B 分层」/「方向 C 降维+t」。代表性 sector 必须包含：(a) E-4 的 11 个 D6 失败 sector（`801038.SI`、`801127.SI`、`801204.SI`、`801223.SI`、`801231.SI`、`801711.SI`、`801723.SI`、`801733.SI`、`801738.SI`、`801743.SI`、`801971.SI`）中的若干项；(b) E-8 的 legacy 缺态例 `801780.SI`；(c) 若干当前已通过的 sector 作为不退化对照。
- **验证「窄板块」假设（RC-2 注）**：在代表性 sector 上同时记录板块的样本规模/流动性代理指标，检验失败是否与板块规模相关。若无显著相关，则确证 blocker 属门禁刚性（RC-1）而非板块规模。
- **评估指标（研究口径，非交易门禁）**：状态持续性（平均 dwell、跳跃次数）、三态 occupancy 充分性、跨 seed/跨 fold 稳定性、regime 的前向 excess-return 分离度与经济显著性、可识别性（协方差健康度）。
- **评估协议改为 walk-forward 多折**：以 expanding-window 多个 validation fold 替代单一 9 个月窗口，评估 regime 的经济显著性，避免「单窗口三态齐全」这一隐性硬约束（对应 RC-2）。
- **kill-criteria**：若 spike 在预注册协议下仍无法让顽固 sector 达标，则结论收敛到方向 D（诚实子集），不再新增诊断轮次。
- **复现要求**：沿用现有 content-addressed / 双 fresh-process bitwise 一致纪律。

---

## 10. 本提案的可合入标准（Merge Criteria）

本提案作为 docs-fast-new（新增 `docs/architecture/` 设计文档，非 controlled path）交付，合入标准如下：

- [x] 文档归属正确：位于 `docs/architecture/`，符合 [DOC-LOCATION-001]。
- [x] 不修改父蓝图或 Phase 2 详细设计正文；不含代码/schema/DDL/runtime 变更。
- [x] 所有事实（E-1~E-14）可回溯到两份权威文档及其登记证据。
- [x] 每个整改方向给出 blocker 映射、最小替代、成本、不做的影响（满足父蓝图 §11.1 规则）。
- [x] 通过 DESIGN-COMPLIANCE-001 四项自审（§11）。
- [x] `git diff --check` 无空白/冲突标记错误（§12 报告）。
- [x] 生产门禁全 `noop`（§12）。
- [ ] 用户确认合入（**保留给用户**）。

后续（批准后、非本提案范围）：CS-1~CS-6 的正文修订与 spike 走独立 F2 设计 + `python scripts/aistock_feature_workflow.py validate --design <path> --tier F2` + feature workflow。

---

## 11. DESIGN-COMPLIANCE-001 自审

- **1 禁止简化交付：PASS**。本提案完整给出根因分析、依据、分析过程、结论、五个整改方向与建议修订条目；未把任何子集或占位当作完整交付。它明确自身是「决策输入」而非「已批准变更」，边界清晰。
- **2 禁止静默错误：PASS**。所有 blocker、失败 sector、无完整候选 seed 的事实均如实引用（E-3~E-8）；方向 D 明确要求 `insufficient_structure` 为显式 typed 状态，反对静默降级。
- **3 禁止改变业务逻辑：PASS（附显式声明）**。本提案不擅自改变任何已批准业务语义：advisory-only、PIT、hard semantic authority、禁止静默失败均原样保留。**须显式声明**：CS-3 确实**提议**把 F-011 READY 的完整性口径从「全或无 131/131 ∧ 31/31」改为「eligible sector 通过 + 不可建模 sector 显式 `insufficient_structure`」——这正是父蓝图 §11.2 当前明令禁止的「单 family / 120-of-131 完成声明」。因此 CS-3 是一个**需用户批准的完整性口径变更提议**，不是本提案单方面生效的改动；DESIGN-COMPLIANCE-001 第 3 项要求「批准后的范围调整同步回设计」，本提案严格停留在「提议 + 待批准」阶段，未修改蓝图正文、未按新口径宣称任何完成，故不构成违规。用户批准 CS-3 即等于批准该完整性口径调整。
- **4 禁止私增门禁审批：PASS**。不新增模型阈值、研究淘汰门禁、运行时审批或发布阻断；CS-5 的进度口径只增加报告维度不增加门禁；spike 的 kill-criteria 是收敛条件而非新增审批。

**结论**：`PASS_PROPOSAL_ONLY_NO_BLUEPRINT_OR_MODEL_CHANGE_AUTHORIZED`。该结论只确认本提案自身的设计符合性，不表示 CS-1~CS-6、spike、F-011 READY 或任何模型/DDL/runtime 已获批准或已完成。

---

## 12. 生产门禁与变更声明

- `production_ddl_gate`：`noop`（无 schema 变化）。
- `production_backend_dependency_gate`：`noop`。
- `production_frontend_dependency_gate`：`noop`。
- `runtime_activation_gate`：`noop`（不激活任何 runtime）。
- `data_write_gate`：`noop`（未写 `hmm_risk.*`/`hmm_evolution.*` 或任何库）。
- 变更文件：仅新增本文件一份。验证：`git diff --check`。
- 模型/READY/selection/D5/D6/fit：均未执行。scratch：无。

---

## 13. 参考

### 13.1 内部权威文档

- `docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md` v2.24 — 父级唯一产品目标蓝图。
- `docs/architecture/hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md` — Phase 2 实现级详细设计与模型验收权威状态。
- `docs/standards/aistock_development_standard_v1.5_20260523.md` — 唯一开发规范（[DOC-LOCATION-001]、[DESIGN-MAIN-001]、[DESIGN-COMPLIANCE-001]、[TRADING-FALLBACK-001]、[ERR-FALLBACK-001]）。

### 13.2 公开研究依据

- P. Nystrup, H. Madsen, E. Lindström. "Learning hidden Markov models with persistent states by penalizing jumps." *Expert Systems with Applications*, 2020.
- P. Nystrup, P. N. Kolm, E. Lindström. Jump models / feature selection in jump models 系列工作, 2020–2021.
- E. B. Fox, E. B. Sudderth, M. I. Jordan, A. S. Willsky. "A Sticky HDP-HMM with Application to Speaker Diarization." *Annals of Applied Statistics*, 2011.
- J. Bulla. "Hidden Markov models with t components. Increased persistence and other aspects." *Quantitative Finance*, 2011.
- R. P. Adams, D. J. C. MacKay. "Bayesian Online Changepoint Detection." arXiv:0710.3742, 2007.
- J. D. Hamilton. "A New Approach to the Economic Analysis of Nonstationary Time Series and the Business Cycle." *Econometrica*, 1989（Markov switching 基础）。

---

## 14. 变更历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.0 | 2026-08-13 | 初始提案：Phase 2 F-011 根因分析（建模范式与验收门禁统计阻抗失配）、依据/分析过程/结论、五个整改方向、建议蓝图修订条目 CS-1~CS-6、spike 验收协议与可合入标准。状态 `PROPOSAL_PENDING_USER_APPROVAL`；不修改蓝图，DDL/DML/依赖/runtime 均 noop。 |

---

**文档归档路径**：`docs/architecture/hmm_evolution_phase2_modeling_paradigm_blueprint_revision_proposal_20260813.md`
