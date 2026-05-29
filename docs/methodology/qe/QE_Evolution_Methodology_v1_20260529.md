# QE 策略演进方法论 v1（QE Evolution Methodology）

- **文档类型**：领域方法论 / 实验设计指导（非开发标准）
- **版本**：v1.0
- **创建日期**：2026-05-29
- **适用对象**：AIstock 智能助手、Codex、研究人员、任何能调用 QE MCP / API 的工具
- **目标**：把"靠运气跑出的偶然冠军"转化为"可复现、可解释、可晋升"的结构化科学流程。任何模型或工具读完本文，都能独立设计出一轮有意义的 QE 演进实验、定义考核指标、并判断是否可晋升。

> 配套文档：
> - `QE_Experiment_Template_Schema_v1_20260529.md` —— 机器可读的实验模板契约（路线 → QE custom task 配置）
> - `QE_DataWarehouse_Analytics_Design_v1_20260529.md` —— 数仓分析层（视图/报表）设计，方法论所需指标的取数来源

---

## Part 0 · 实证基础（方法论的出发点）

本方法论不是凭空设计，而是从三个真实 QE 任务的对照中归纳出来的。理解这段，才能理解后面所有原则的"为什么"。

### 0.1 三个关键任务

| 任务 ID | 性质 | 关键发现 |
|---------|------|----------|
| `qe_20260520_215627_abbc` (18 loop) | 模型/horizon/因子广搜 | L16(LSTM-10D, 57因子, **未固定seed + epoch=1 欠训练**)拿到全局最高回测收益 AnnRet≈53.7%（官方 CAGR≈103.7%）、IR≈2.65 |
| `qe_20260522_012542_90fb` (16 loop) | 复现 + 固定 seed + capacity + HMM | 用固定 seed 复现 abbc/L16 配置 12 次，收益**稳定收敛到 44~46%**，再未出现 53%/103%；L14(label_horizon=20)拿到全局最高 **IC=0.0895 / ICIR=1.11** |
| `qe_20260528_141812_ad82` (10 loop, pending) | seed × 训练深度解耦实验 | Theme A(4固定seed正常训练) vs Theme B(4固定seed欠训练 epoch=1/3/5) vs Theme C(topk)。**专为回答"欠训练冠军是运气还是正则化"而设计** |

### 0.2 四条硬事实

1. **abbc/L16 的 103.7% 是方差尾部，不是能力。** 90fb 用固定 seed 复现同一配置 12 次，收益收敛到 44~46%，方差被锁死后再没出现极值。说明那一次是"未固定 seed × epoch=1 欠训练"恰好踩中回测路径高方差正尾的一次抽样。

2. **"欠训练更好"可能是真实现象。** epoch=1 的神经网络接近随机初始化 + 极弱拟合；在 A 股这种极低信噪比数据上，**早停 = 强正则化**，确实可能比训练到收敛（epoch=200）泛化更好。这不是 bug，是一个值得当作一等超参来搜的真信号。`ad82` 实验就是用来区分这两种解释的。

3. **IC 与收益是两条不重合的轴。** 信号质量最强点在 `label_horizon=20`（90fb/L14 IC=0.089, ICIR=1.11），回测收益最强点在欠训练的 abbc/L16。**只优化一个指标，会被另一个指标背刺。**

4. **因子重要性跨 seed 剧烈漂移。** 数仓 `run_factor_importance` 聚合显示，同一因子在 4 个 seed 下排名可以从 best_rank=1 漂到 avg_rank=40（如 `LargeOrder_Cost_Interaction`，std_normalized=0.34）。**单次实验里"某因子重要"的结论几乎不可信**，必须跨 seed 看稳定性。

### 0.3 由此推出的核心命题

> **任何单次 Loop 的高收益都只是"假设"，不是"结论"。一个配置是不是真的好，取决于它在种子集合（seed-ensemble）上的均值与方差，以及在多时间窗（walk-forward）上的稳健性——而不是它某一次跑得多高。**

后面所有内容都是这条命题的工程化展开。

---

## Part 1 · 三大第一性原则（所有路线必须遵守）

### 原则 1 —— 单次结果只是假设，冠军必须经 seed 鲁棒性检验
任何 Loop 的 AnnRet/IR 在通过 seed-鲁棒性检验前，状态一律为 `unverified`。
**冠军的定义** = "种子集合（N≥5）的**均值**高 **且** 标准差低"，而非"某一次最高"。
- 量化判据：`mean(metric)` 进入候选前 top；`cv = std/mean < 0.25`（收益类）或 `std < 0.15`（IC 类）。

### 原则 2 —— 把 seed 从噪声源变成资产（Seed-Ensemble）
不追求"找到那个幸运 seed"（不可复现），而是做 **seed 集成**：同配置跑 N 个 seed，预测层取平均（或排序平均）后入库为生产模型。
- 这样既在**期望**上吃到上行，又把**方差**打掉，把 abbc/L16 的运气工程化为可复现能力。
- seed 集成的均值收益是诚实的生产预期；单 seed 极值只用于发现"值得深挖的配置区域"。

### 原则 3 —— 双轴指标，分别守门，禁止单轴爆表晋升
- **信号轴**（IC / ICIR / RankIC / RankICIR）：衡量"模型懂不懂"，对应稳健、容量、可迁移。
- **组合轴**（AnnRet / IR / Sharpe / MaxDD / Calmar）：衡量"策略赚不赚"，但极易过拟合回测路径。
- **晋升必须两轴同时过线**；任何只有组合轴爆表、信号轴平庸的结果，直接进 `suspicious` 队列复检（大概率是过拟合或方差尾部）。

---

## Part 2 · 实验基因：搜索轴定义（泛化到任意模型）

任何 QE 实验都是在下列**正交搜索轴**上取值的组合。把它当作"基因组"，新模型、新策略只是往某条轴上加一个新等位基因，方法论本身不变。

| 轴 | 取值示例 | 归属路线 | 是否影响信号轴 | 是否影响组合轴 |
|----|----------|----------|----------------|----------------|
| **A. 模型架构** | LSTM / GRU / ALSTM / TCN / Transformer / LGB / XGB / CatBoost / MLP | E | ✔ | ✔ |
| **B. 模型超参** | hidden_size, layers, dropout, lr, weight_decay, batch_size | B/E | ✔ | ✔ |
| **C. 训练深度/正则** | max_epochs, early_stop patience, undertrain_mode | **B** | ✔ | ✔✔(高方差) |
| **D. 随机种子** | seed 值 + seed_policy(fixed/ensemble) | **C** | ✔(方差) | ✔✔(方差) |
| **E. 标签周期** | label_horizon = 1 / 5 / 10 / 20 | A | ✔✔ | ✔ |
| **F. 因子组合** | factor_keys 子集（来自因子库） | **A** | ✔✔ | ✔ |
| **G. Alpha158 开关** | disable_alpha158 | A | ✔ | ✔ |
| **H. 组合构建** | strategy_id, topk, n_drop, 加权方式, capacity 约束 | **F** | ✘ | ✔✔ |
| **I. Regime 叠加** | enable_sector_hmm, hmm_signal_preset, risk_policy | **D** | ✘ | ✔(降尾部) |
| **J. 执行算法** | execution_algo (V25_1 等), 分钟级处理 | F | ✘ | ✔(实盘衰减) |
| **K. 股票池/风控** | stock_pool, sector_blacklist, st_pit risk_policy | A/F | ✔ | ✔ |
| **L. 数据划分** | train/valid/test 窗口, walk-forward 折 | 横切(考核层) | ✔✔ | ✔✔ |

**设计实验的第一步永远是：明确这一轮只动哪 1~2 条轴，其余全部锁定为基线。** 一次动多条轴 = 无法归因 = 浪费算力。这是"有意义的演进" vs "随机尝试"的分水岭。

---

## Part 3 · 六条演进路线（细化为可执行规格）

每条路线给出：目标函数、动作轴、进入条件、单 Loop 配方、成功判据、停止/退出规则。智能助手据此可直接生成 loops 清单（见模板契约文档）。

### 🛤️ Route A — 信号质量路线（IC/ICIR 驱动，求稳健与容量）
- **目标函数**：max ICIR（其次 RankICIR），约束 `AnnRet 不低于基线 90%`。
- **动作轴**：E(label_horizon) → F(因子精修) → G(Alpha158 子集)。
- **进入条件**：要做 multi-alpha 基座、上大资金(500–1000万)、追求可复现长期 edge。
- **单 Loop 配方**：锁定模型+seed_policy，只扫 horizon∈{5,10,20} 或因子子集。
- **成功判据**：找到 ICIR 显著高于基线且 seed 稳定（原则 1）的配置。
- **当前锚点**：90fb/L14（h=20, ICIR=1.11）= 该路线现役最佳基线。
- **退出**：horizon/因子扫完，ICIR 无提升 → 转 Route E（换模型族）或 Route F（榨组合层）。

### 🛤️ Route B — 训练深度/正则化路线（把"欠训练之谜"工程化）
- **目标函数**：找到 (IC, 收益, 方差) 联合最优的训练深度档位，并验证其 seed 稳定。
- **动作轴**：C(max_epochs / early_stop / undertrain_mode)，其次 B(dropout, weight_decay)。
- **进入条件**：现在 —— 这是当前信息增益最高的实验（`ad82` 即此路线）。
- **单 Loop 配方**：固定模型+因子+horizon，网格 max_epochs∈{1,3,5,10,收敛}，每档至少 2 seed。
- **成功判据**：产出"训练深度 × (IC, 收益, 收益std)"曲线；明确 epoch=1 是运气（高 std）还是正则化（高 mean 且可控 std）。
- **关键产出**：一个固化的"最优训练深度"超参档位，写回模型库。
- **退出**：曲线给出明确拐点 → 该档位送 Route C 做 seed 集成。

### 🛤️ Route C — Seed-Ensemble 路线（方差消除，主力生产路线）
- **目标函数**：min(收益方差)，约束均值收益不低于候选的 80%。
- **动作轴**：D(seed)，固定其余全部轴。
- **进入条件**：任何配置在 A/B/E/F 胜出后，**强制**经此路线才能进 paper。
- **单 Loop 配方**：同配置 × N seed(5~10)，预测层平均/排序平均 → 入库为集成模型。
- **成功判据**：集成 IR 的下尾（worst-seed）可接受；`cv < 0.25`；集成均值 ≥ 单 seed 中位数。
- **价值**：直接回答用户的核心问题——把"未固定 seed 的偶然冠军"替换为"多 seed 集成的稳定冠军"。
- **退出**：集成稳定 → 进晋升漏斗稳健层（walk-forward）。

### 🛤️ Route D — Regime / HMM 自适应路线（只做风控减震）
- **目标函数**：min MaxDD、提升 IR 下尾，约束 AnnRet 不显著下降。
- **动作轴**：I(HMM 状态/overlay 强度/risk-gate)。
- **进入条件**：A/C 产出稳定 alpha 基座后，作为**叠加层**，绝不作基座。
- **历史教训**：2026-05 多轮 HMM 对比结论是"多数 overlay 跑不赢 no-HMM"——因此本路线**只允许做风控减震，禁止当收益增强器**。
- **成功判据**：在不损失 >10% AnnRet 的前提下改善 MaxDD 或 Calmar。
- **退出**：HMM 无法改善尾部 → 放弃叠加，回 no-HMM 基线。

### 🛤️ Route E — 模型/Multi-Alpha 多样性路线（突破单模型天花板）
- **目标函数**：max 组合层 IR，路径是"低相关 alpha 叠加"而非单模型调优。
- **动作轴**：A(架构) + E(不同 horizon 模型) + 分组建模(sw2 方案 B/C)。
- **进入条件**：单模型 IC 见顶（历史天花板 IC≈0.06~0.09）时。
- **准入闸**：新模型/rdagent 模型纳入演进前，先过"架构准入体检"——abbc/L3-L4 的 ALSTM 出现负 IC，证明不体检直接用会污染实验。
- **成功判据**：找到与现役 alpha 预测相关性低（|corr|<0.6）且单独 ICIR 达标的新 alpha，叠加后组合 IR 提升。
- **退出**：无低相关增量 alpha → 回 Route A 精修现有基座。

### 🛤️ Route F — 容量/可交易性路线（回测→实盘衰减最小）
- **目标函数**：min（回测收益 − 实盘可实现收益）的衰减，约束 AnnRet。
- **动作轴**：H(topk/加权/capacity) + J(执行算法/分钟级)。
- **进入条件**：策略进 paper 前的最后一关（`ad82` Theme C 的 topk 40/50/80 属于此）。
- **成功判据**：在 500–1000万资金下，换手/冲击成本可承受，capacity 约束后收益衰减 < 阈值。
- **退出**：容量达标 → 进 paper trading。

### 路线编排建议（不是唯一路线）
```
            ┌─ Route A (信号/horizon/因子) ─┐
基线 ──────►│  Route B (训练深度)            │──► Route C (seed集成) ──► walk-forward
            │  Route E (模型多样性)          │                              │
            └──────────────────────────────┘                              ▼
                                                      Route D(HMM减震) + Route F(容量) ──► paper
```
路线**可并行、可组合、可跳过**；唯一强制的是：进 paper 前必须经过 Route C（seed 集成）与 walk-forward。

---

## Part 4 · 因子组合筛选方法论（回答 Q1）

> **问题**：因子库已有大量因子（注：因子库累计经多轮清理仍达数百量级，生产常用 57 因子集），如何实现"有意义的因子组合演进"而非随机试各种组合？

### 4.1 为什么不能随机试
- 组合空间是 2^N，N=数百时是天文数字，随机采样命中率趋近 0。
- 单次实验的因子重要性不可信（Part 0.2 事实 4），靠"看哪个因子重要再删"会被 seed 噪声带偏。
- 因子高度冗余（记忆中已知存在 corr≈-0.82 的对、39 个相关簇/56 个冗余因子），随机组合大量是"换汤不换药"。

### 4.2 可参考的成熟量化实践与论文
本方法论的因子筛选部分建立在以下公认框架上（智能助手在写实验理由时可直接引用）：

1. **主动管理基本定律（Fundamental Law of Active Management, Grinold & Kahn 1999）**：`IR ≈ IC × √Breadth`。启示：组合的价值来自"有效因子数(广度) × 平均 IC"，而非堆因子数量。**追求的是低相关的有效广度，不是因子总数。**
2. **因子正交化 / Gram-Schmidt 残差化**：新因子相对已选因子做横截面回归取残差，只保留增量信息。对应数仓 `run_factor_pair`（spearman 相关）与 `factor_corr` MCP 簇。
3. **层次聚类 + 簇内择优（López de Prado, HRP 思想）**：先按相关性做层次聚类（complete-linkage），每簇只留 1~2 个代表，天然去冗余。项目已有 39 簇/56 冗余的聚类结果可直接复用。
4. **前向选择 / 边际 IC 贡献（Forward Stepwise by marginal ICIR）**：从空集开始，每步加入"使组合 ICIR 提升最大"的因子，提升 < 阈值即停。这是"有意义演进"的标准做法。
5. **最大化组合 ICIR 的子集搜索（贪心 + 相关性惩罚）**：目标 `maximize ICIR(subset) − λ·avg_pairwise_|corr|`。
6. **Qlib / Alpha158 工业实践**：少量精选基础特征（20 个 Alpha158）+ 领域 alpha 已被验证有效；不盲目扩特征维度。
7. **稳定性选择（Stability Selection, Meinshausen & Bühlmann 2010）**：在多个 seed/子样本上重复选择，只保留"被高频选中"的因子——直接对应本项目 seed 漂移问题的解药。

### 4.3 因子组合筛选整体方法论（落地流程）

```
[Step 1 去冗余] 用 factor_corr 簇 + 层次聚类，把数百因子压到"每簇代表"层级
        │  产出：候选因子池（去冗余后 ~60-100 个）
        ▼
[Step 2 单因子体检] 用 aistock_factor_metrics 过滤：ICIR、IC正比例、IC衰减半衰期、覆盖率
        │  门槛：单因子 ICIR ≥ 阈值 且 coverage 达标
        ▼
[Step 3 前向选择] 贪心：每步加入边际 ICIR 提升最大的因子，带相关性惩罚 λ
        │  产出：候选组合（按边际贡献排序）
        ▼
[Step 4 稳定性筛选] 候选组合 × N seed，只保留"跨 seed 重要性稳定"的因子
        │  取数：v_factor_importance_stability 视图（见数仓设计文档）
        │  剔除：avg_rank 差、std_normalized 高（如 >0.35）的不稳定因子
        ▼
[Step 5 组合验证] 形成 2~4 个互相低相关的候选组合 → 进 QE 实验做 A/B
        │  双轴考核（Part 6）+ seed 集成（Route C）
        ▼
[Step 6 固化] 胜出组合写回因子集快照（factor_set_hash），作为新基线
```

### 4.4 因子演进的"动作清单"（供智能助手生成实验）
- **替换**：用 `factor_corr_suggest_replacements` 找低相关替代，换掉不稳定因子。
- **精简**：从 57 因子裁到高稳定子集（如 23 因子，参考 abbc/L6），验证是否"更少但更稳"。
- **扩展**：从去冗余池前向选择加入 1~2 个新簇代表。
- **每次只动一类动作**，对照基线，归因清晰。

---

## Part 5 · 模型演进方法论（回答 Q2）

> **问题**：模型库大量是 rdagent 演进出来的模型，参考价值不大。未来是否需要补充新模型/新超参，在现有模型类型上做集成演进？有无成熟思路？

### 5.1 现状诊断
- 模型库（`aistock_model_catalog`）历史模型多为 rdagent 早期产物，训练诊断字段（`training_failed`, `convergence_ratio`, `overfit_ratio`）显示质量参差；ALSTM 等架构在 QE 实验中出现负 IC。
- 现役有效模型集中在少数"seed 模板"（如 `__seed_LSTM_10D_hs64_d02__`, `__seed_GRU_10D_hs96_d03__`）。
- **结论**：不是"补很多新模型"，而是"建立一个被严格体检、可复现、带集成能力的精选模型库"。

### 5.2 成熟的模型演进思路（分层）

**第一层：模型库治理（先做减法）**
- **架构准入体检**：任何模型（含 rdagent 产物）入演进前，必须在标准因子集+多 seed 上跑通，IC 为正且 seed 稳定，否则标记 `deprecated`，不进 QE 选择池。
- 用 `model_registry` 的 `seed_stability` / `hyperparam_history` 做体检，淘汰 `training_failed=true` 或 IC 不稳的历史模型。

**第二层：超参搜索（结构化，非随机）**
- 用**贝叶斯优化 / Optuna**（数仓已有 `run_model_trial.optimizer_*` 字段为此预留）替代手工试参。
- 搜索空间用记忆中已验证的黄金配置为先验中心：GRU hidden=64/lr=3e-4/wd=1e-5；Transformer d_model=64/nhead=4；**关键护栏：GRU wd≥1e-3 会崩**、batch_size 128-512 对 TSDatasetH 无效。
- **训练深度（Route B）是一等超参**，必须纳入搜索空间。

**第三层：集成演进（在现有模型类型上做集成，这是重点）**
1. **Seed 集成（最优先）**：同架构同超参 × N seed 预测平均 —— Route C，方差杀手。
2. **Horizon 集成**：不同 label_horizon（5/10/20）模型预测加权 —— 捕捉不同节奏 alpha。
3. **架构集成（Multi-Alpha）**：LSTM + GRU + LGB 等低相关模型的预测层融合 —— Route E，突破单模型天花板。
4. **分组建模**：sw2 方案 B（行业门控 Meta-Model）/ 方案 C（SectorAwareGRU）—— 解决扁平张量无法区分特征组的问题。

### 5.3 模型演进的考核
- 新模型/新超参档位必须在**统一因子集 + 统一数据划分 + 多 seed** 下，与现役基线做受控 A/B。
- 入库前更新 `model_catalog` 训练诊断字段 + seed 稳定性，写明 `qe_selectable` 准入结论。

---

## Part 6 · 每次实验后的考核指标与数据分析方法论（核心新增）

> 这一节定义"实验跑完后看什么、怎么判断"。任何工具按此清单即可机械执行，无需主观发挥。

### 6.1 双轴核心指标（每个 Loop 必看）
| 轴 | 指标 | 取数来源 | 方向 |
|----|------|----------|------|
| 信号轴 | IC, ICIR, RankIC, RankICIR, IC正比例, IC衰减半衰期 | `run_metric` / 因子metrics | 越高越好 |
| 组合轴 | AnnRet(CAGR), Sharpe, IR, MaxDD, Calmar, 换手率, 现金占比 | `run_account_summary` / `run_metric` | 收益类高/回撤低 |

### 6.2 seed 鲁棒性分析（原则 1 的执行）
对同配置的 N 个 seed run：
- 报告 `mean ± std` 与 `cv = std/mean`；报告 worst-seed（下尾）。
- **判据**：组合轴 `cv < 0.25`、信号轴 `std < 0.15` 才算"稳定"。
- 取数：`v_seed_robustness` 视图（数仓设计文档）。

### 6.3 因子归因稳定性分析（原则的因子版）
- 跨 seed 看每个因子的 `avg_rank` / `std_normalized` / `best_rank`。
- 标记不稳定因子（`std_normalized > 0.35`）为"待替换"。
- 取数：`v_factor_importance_stability`（已有同名 MCP 聚合，本方法论将其固化为 SQL 视图）。

### 6.4 过拟合 / 方差尾部检测（防 abbc/L16 陷阱）
触发"可疑"复检的红旗：
- 组合轴爆表但信号轴平庸（IR 高、ICIR 平）。
- `training_failed=true` 或 `convergence_ratio` 极低却收益极高（欠训练偶然命中）。
- 单 seed 远超 seed 集成均值（> mean + 2·std）。
- 任一红旗 → 状态置 `suspicious`，必须 seed 集成 + walk-forward 复核后才可继续。

### 6.5 walk-forward / 多窗稳健性
- 不依赖单一 test 段；至少 2~3 个滚动窗口（或扩展窗）评估。
- **判据**：无单窗 IR 崩盘（如某窗 IR<0.5）；窗口间 IC 符号一致。

### 6.6 容量与可交易性
- 换手率、平均持仓数、单票冲击成本估计、capacity 约束后收益衰减。
- 取数：`run_trade` / `run_position` / paper_v2 fills。

### 6.7 实验结论模板（每轮实验必须产出）
```
实验ID / 动作轴 / 基线对照
├─ 双轴指标表（vs 基线 Δ%）
├─ seed 鲁棒性（mean±std, cv, worst）
├─ 因子归因稳定性（不稳定因子清单）
├─ 红旗检查（是否 suspicious）
├─ walk-forward 结论
└─ 决策：晋升 / 复检 / 否决 + 下一轮动作建议
```

---

## Part 7 · 晋升漏斗与决策门（统一收口）

```
[探索层]  Route A/B/E 广撒网，单 seed 快跑
   门：IC≥0.06 且 IR≥1.5（双轴初筛）
        ▼
[验证层]  Route C 强制 seed 集成（N≥5）
   门：cv<0.25 且 集成均值≥候选80%
        ▼
[稳健层]  walk-forward / 多窗 OOS
   门：无单窗崩盘，窗间 IC 符号一致
        ▼
[减震层]  Route D(HMM) + Route F(容量)
   门：MaxDD 不恶化，容量衰减可接受
        ▼
[Paper]   模拟盘 → [生产]
```
**每层是"门(gate)"，不是"加分项"。** 单 seed 的 103% 会卡在验证层之外——这正是当前体系缺失的环节，也是本方法论要补的核心闸门。

---

## Part 8 · 给智能助手的执行契约（如何把方法论变成实验）

智能助手设计一轮 QE 实验时，按以下步骤机械执行：

1. **选基线**：从数仓 `v_evolution_leaderboard` 取当前最佳配置（信号轴锚点 90fb/L14，收益轴锚点待 Route C 确立）。
2. **选路线**：根据目标（稳健/收益/容量/突破天花板）从 Part 3 选 1 条路线。
3. **锁轴**：按 Part 2，确定本轮只动的 1~2 条轴，其余继承基线。
4. **生成 loops**：按 `QE_Experiment_Template_Schema_v1` 的契约，把"路线 + 动作轴取值网格"展开为 loops 清单；凡涉及"挑冠军"的轴，自动追加 Route C 的多 seed loops。
5. **设考核**：按 Part 6.7 预登记本轮要看的指标与判据。
6. **创建任务**：调用 `qe_custom_evo` 创建 custom task，`auto_start=false`，交人审核。
7. **跑后分析**：实验完成后从数仓视图取数，产出 Part 6.7 结论模板，给出晋升/复检/否决决策与下一轮建议。

> **关键纪律**：一次只动 1~2 条轴；任何挑冠军的结论先标 `unverified`，经 Route C + walk-forward 才能 `verified`；单轴爆表一律先 `suspicious`。

---

## 附录 · 与现有系统的对接点
- **QE 实验创建**：`mcp__aistock-qe-experiment__qe_custom_evo_*`（create/run/get_config/loop_comparison）。
- **数仓取数**：`mcp__aistock-qe-archive__*` + 本方法论配套新增的 SQL 视图（见数仓设计文档）。
- **因子库**：`mcp__aistock-factor-library__*` + `mcp__aistock-factor-correlation__*`（相关性/替换建议）+ `aistock_factor_metrics`。
- **模型库**：`mcp__aistock-model-registry__*`（seed_stability / hyperparam_history / 准入）。
- **数据划分对齐**：Train 2018-08~2022-12 / Valid 2023-01~2024-06 / Test 2024-07~2026-03（见 RDAGENT_DEFAULT_DATA_SPLIT）。

---
*本方法论是活文档。每完成一轮里程碑实验（尤其 `ad82` seed×训练深度解耦），应回写"实证基础"与"现役锚点"两节。*
