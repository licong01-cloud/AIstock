# 多 Alpha 预测存储 + MLflow 集成 · 下一阶段实施蓝图

> **类型**：架构设计蓝图（design）· 下一阶段实施总纲
> **日期**：2026-06-16
> **作者**：战略 session（AIstock + RDAgent + QE + Paper v2 总指挥）
> **关联**：`docs/analysis/ma1_multi_alpha_sourcing_analysis_20260615.md`、`docs/analysis/phase0_multi_alpha_orthogonality_matrix_20260615.md`、`docs/analysis/multi_alpha_architecture_audit_20260518.md`、memory `pending_qe_r20_multi_alpha_20260611` / `multi_alpha_architecture_analysis`
> **状态**：蓝图已批准（用户 2026-06-16 确认存储分层方向）；MLflow 模型库/注册权重提升需后续深化；线路 A 实验设计待确认后启动

---

## 0. TL;DR

1. **多 Alpha 信号源已基本备齐**：5 个信号域经 Phase0 实测两两独立（持仓 Jaccard≤0.26 / 共同持仓 PnL Spearman≤0.40），组合 Sharpe 代理上界 ≈3.44，远超单腿 SOTA 2.45。**结论：不再盲目找新域，转向夯实 + 工程落地。**
2. **预测分数其实一直在生成**（Qlib `pred.pkl` = 全截面 [datetime, instrument, score]），只是落在每个 workspace 的本地 `mlruns/` 临时目录、跑完即弃，且 QE 数仓只存聚合指标不存逐日分数。**真正缺的不是"产生预测"，而是"把预测固化 + 集中 + 可查询"。**
3. **存储方案（Q2）**：**不**把全截面分数塞进关系型数仓（量级数十亿行，错配）。采用 **MLflow 主存重产物（pred.pkl / 模型权重 / config）+ qe_archive 关系库存指标和指针** 的标准分层。qe_archive 已预留 `mlflow_tracking_uri / artifact_uri / version` 三列，填坑即可。
4. **MLflow 定位（Q3）**：MLflow 接管 **训练/回测监控 + 模型注册治理 + 产物血缘**（本职，强烈推荐，与现有 StrategyPackage 生命周期高度同构）；**但绝不**用回测预测当实盘信号 —— 信号仍由现有"当日重算因子 + 冻结模型推理"（`live_inference`）产生，MLflow 只作"用哪个模型"的 source of truth。
5. **双线并行**：线 A（信号发现，GPU/CPU 算力）夯实 5 腿 + 补样本；线 B（架构/数据采集，工程）建中心化 MLflow + 离线组合回测器。两线在"预测存储层"接口汇合。

---

## 1. 背景与现状真相

### 1.1 三层预测真相（本轮代码探查结论）

> 探查覆盖 `F:\Dev\RD-Agent-main`（RDAgent + QE + Qlib）与 `F:\Dev\AIstock`（后端 + Paper v2 + advisory）。下列结论为三路独立探查交叉印证，**具体 file:line 在实施前需再复核**（代码可能已变动）。

| 层 | 是否有逐日逐股全截面预测分数 | 现状与证据 |
|---|---|---|
| **Qlib / mlruns（回测产物）** | ✅ **有** `pred.pkl` = `[datetime, instrument, score]` 全截面 | Qlib `SignalRecord` 经 `recorder.save_objects("pred.pkl", "label.pkl")` 写入。但 `MLFLOW_TRACKING_URI` 被设为 `file://<workspace>/mlruns`（每 workspace 各一份、临时），loop 完成后 workspace 清理即弃；**仅 TopK 的 `signals.parquet` 留在工作目录**。RDAgent 自身 `enable_mlflow=False` 只影响其 loop 级自定义指标，不影响 Qlib 原生 recorder 写 `pred.pkl`。 |
| **QE 数仓（qe_archive）** | ❌ 无逐日分数（`priority_score_count=0`） | 只存聚合指标（IC/RankIC/CAGR/Sharpe/turnover/MDD）、`enhanced_metrics.all_stocks`（持仓全集 + profit_pct）、`stock_trades`（逐笔）。**已预留指针列** `mlflow_tracking_uri / mlflow_artifact_uri / mlflow_version`（`init_qe_archive_schema.py` / `qe_archive/models.py`），当前未填充使用。 |
| **AIstock 生产（Paper v2）** | ✅ **有**，但走**实时推理**而非回测回放 | `strategy_package/live_inference.py` 明确拒绝把回测 `pred.pkl` 当当日信号，坚持"用当日最新 DB 数据重算因子 + 加载冻结 QE 模型 `model.predict()`"→ 落 `selection_score_artifact` 表（键 = package_id + manifest_sha256 + trade_date + data_source + runtime_config_hash）。 |

### 1.2 三个被纠正的认知（相对上一轮分析）

1. **预测分数并非"根本不存"**：`pred.pkl` 一直在生成，问题是**临时 + 分散 + 未归档**。这把"从零建预测库"降级为"固化既有临时产物"，工程量小一个量级。
2. **数仓早有指针列**：`qe_archive` 的 `mlflow_artifact_uri` 等列是为这件事预留的坑，方案天然契合既有 schema。
3. **多 Alpha 组合基础设施已部分存在**：StrategyPackage 带 `alpha_mode = SINGLE_ALPHA | MULTI_ALPHA`；advisory 已支持 `weighted_rank_fusion / fusion_pool / union / intersection` 融合模式 —— 作用在**实时推理分数**上。即"生产侧多 Alpha 组合"大体已建，缺的是"离线侧组合研究"和"把验证好的腿注册成包"。

### 1.3 多 Alpha 信号源现状（线 A 起点）

| α | 信号域 | 因子集×模型 | CAGR | Sharpe | ICIR | 种子CV | n | 状态 |
|---|--------|------------|------|--------|------|--------|---|------|
| α1 | 技术量价 | PLUS3×LSTM | 96.2% | 2.45 | 0.71 | 11% | 30 | ✅ SOTA 主力 |
| α2 | 机构资金流 | IF18×LGBM_C | 83.6% | 2.01 | 0.62 | 5% | 8 | ✅ 稳 |
| α3 | 基本面动量 | FM12+×LSTM | 86.5% | 2.15 | 0.74 | **24%** | **3** | ⚠️ 样本不足（最大短板） |
| C/α6 | 基本面估值 | FundVal12×LSTM | 83.8% | 2.20 | **0.81** | **2.7%** | 5 | ★ 最佳正交锚 |
| A/α7 | 微观资金流 | Flow12×LGBM | 78.6% | 2.06 | 0.58 | 5% | 5 | ★ 真·新独立源 |
| α4 | 波动率 | VOL12 | ~25% | — | — | — | 4(LGBM) | ❓ 弱，LSTM 未测 |
| α5 | 融资融券情绪 | MARG10 | best 83.5% | — | — | — | 4(LGBM) | ❓ LSTM 未测，覆盖率 87% |

Phase0 正交矩阵：平均非对角 PnL 相关仅 0.249，分散空间真实；C_FundVal 是最佳正交锚（与各域相关全场最低且自身最稳）。

---

## 2. 双线战略总览

| | 线 A：信号发现（夯实） | 线 B：架构 / 数据采集 |
|---|---|---|
| 目标 | 把 5+ 腿的样本补足、锁定可部署均值配置 | 把临时 `pred.pkl` 固化为可查询预测存储 + MLflow 集成 |
| 资源 | GPU(wsl2-5080 parallel_2) + CPU(rdagent-node1 parallel_4) | 工程（后端 + RDAgent workspace + DB） |
| 内涵修正 | **不再盲目找新域**（正交性已证递减），转"补样本 + 探针验新域" | 先做"预测固化 + 数仓指针接线"，再做"离线组合回测器" |
| 依赖 | 独立可跑，不等架构 | 不阻塞线 A；但线 A 产出的腿最终经线 B 存储被组合 |
| 汇合点 | **预测存储层**：线 A 每条腿的 `pred.pkl` 经线 B 固化 → 离线组合研究直接拉取，不重训 | |

> 执行顺序（用户指定）：本蓝图完成 → 线 A 实验设计（§5）→ 用户确认 → 实验运行 → 再展开线 B 详细规划（§3/§4 为框架，§3.4 之后的工单在实验启动后细化）。

---

## 3. Q2 详细设计：预测存储层（MLflow 主存 + 数仓指针）

### 3.1 为什么不进关系型数仓

全截面分数体量：≈4000 股 × ≈450 交易日 × 616 runs ≈ **十亿级行**，且随每轮实验线性增长。关系型指标库（qe_archive）定位是"快查、排行、谱系"，塞入全截面分数会被打爆且查询退化。**职责必须分层**：

- **关系数仓（metadata DB）**：指标 + 指针 + 谱系（小、快、可索引）。
- **artifact store（重产物）**：`pred.pkl` / 模型权重 / config（大、按需拉取）。

这是业界标准的 "metadata DB + artifact store" 模式，MLflow 正是为此而生。

### 3.2 方案对比

| 方案 | 增量工程 | 量级契合 | 评价 |
|------|---------|---------|------|
| (a) 全截面分数入 qe_archive 关系表 | 高（新表 + 写入 + 分区） | ❌ 十亿行错配 | 否决 |
| **(b) 中心化 MLflow 主存 + 数仓指针** | **低**（`pred.pkl` 已生成；数仓指针列已存在） | ✅ artifact store 本职 | **推荐** |
| (c) 自建 parquet 预测湖（仿 `factor_values/` 缓存） | 中（重造轮子） | ✅ | 兜底；与 MLflow 重复 |

### 3.3 目标架构

```
┌─────────────────────── 线 A：QE custom_evo 双节点 ───────────────────────┐
│  每个 loop 回测 → Qlib SignalRecord 产出 pred.pkl / label.pkl / params.pkl │
└───────────────────────────────────┬──────────────────────────────────────┘
                                     │  (改造点①②③)
                     ┌───────────────▼────────────────┐
                     │   中心化 MLflow Tracking Server  │
                     │   + 共享 Artifact Store          │
                     │   · pred.pkl 全截面分数(离线研究) │
                     │   · params.pkl 模型权重(Registry)│
                     │   · conf.yaml / 因子集 / 指标     │
                     └───────┬───────────────┬──────────┘
            指标+指针写回      │               │  模型来源(Q3/§4)
                     ┌────────▼─────┐   ┌──────▼─────────────┐
                     │ qe_archive   │   │ AIstock live_infer │
                     │ (关系: 指标   │   │ (当日重算因子+冻结  │
                     │  + mlflow指针)│   │  模型 predict)      │
                     └──────┬───────┘   └──────┬─────────────┘
          离线组合研究 ◄─────┘                  └──► selection_score_artifact
   (按 run_id 拉各腿 pred.pkl                          │
    → rank 融合 → 一次组合回测,不重训)                 ▼
          │ 输出: 多Alpha融合权重 ──────────► advisory 多Alpha融合(已存在)
          └────────────────────────────────────────► Paper v2 / 未来 QMT
```

### 3.4 改造点清单（线 B 工单雏形）

1. **改造点①：MLflow tracking_uri 从 per-workspace 本地改中心化后端。**
   - 现状：`workspace.py` 把 `MLFLOW_TRACKING_URI` 设为 `file://<workspace>/mlruns`（探查定位，实施前复核）。
   - 目标：统一指向中心化 tracking 后端（sqlite/postgres 元数据 + 共享 artifact 根目录，或对象存储）。本机内网部署，**禁止启动服务由我执行 → 提醒用户启动**。
   - 注意：保留 per-run 的实验/run 命名映射到 QE `task_id / loop_index / group`，便于反查。

2. **改造点②：loop 完成后不清理对应 `mlruns`（或显式 archive `pred.pkl`）。**
   - 现状：workspace 清理使 `pred.pkl` 即弃。
   - 目标：完成时把 `pred.pkl` / `label.pkl` / `params.pkl` 落中心 artifact store（或保留 mlruns）。需评估磁盘容量（见 §3.5 体量）。

3. **改造点③：qe_archive 指针列接线。**
   - 现状：`mlflow_tracking_uri / artifact_uri / version` 列已存在但未填。
   - 目标：归档管线（`payload_extractor` / outbox）在 run 入仓时填充指针，使"指标行 → pred.pkl"可一跳定位。
   - 顺带核查：当前 `pending_outbox_count = 17542` 全 pending，归档 outbox worker 疑似停摆，需先诊断（只读）再决定是否影响指针回填。

4. **改造点④：禁止 silent fallback。** 若某 run 的 `pred.pkl` 缺失/损坏，指针写 null 且记录显式错误，**不许**用空 DataFrame 或聚合指标兜底冒充预测（遵 [[No Silent Errors]]）。

### 3.5 pred.pkl 产物契约

- **格式**：`pd.DataFrame`，MultiIndex `(datetime, instrument)`，列含 `score`（模型截面打分）；配套 `label.pkl` 为对齐 label。
- **粒度**：全股票池全交易日（区别于 `signals.parquet` 仅 TopK）。
- **体量估算**：单 run ≈ 4000×450 ≈ 180 万行；float32 score ≈ 单 run 几十 MB（parquet 压缩后更小）。616 runs 量级 GB~十 GB 级，**artifact store 可承载，关系库不可**。
- **保留策略（待定）**：全量保留 vs 仅保留"研究有效 + 作为 alpha 腿候选"的 run。建议先全量留近 N 轮 + 候选腿永久留。

### 3.6 存量 616 runs 回填

- 多数存量 run 的 `mlruns` 已随 workspace 清理 → `pred.pkl` 可能已不可恢复。**回填能力取决于历史 workspace 是否尚存**，需先盘点。
- 对**确为 alpha 腿代表的关键 run**（Phase0 用的 5 个代表 run + 各腿种子集合），优先确认其 `pred.pkl` 是否可重生成（必要时按相同 seed + config 重跑回测，引擎同 seed 可复现）。
- 回填非阻塞：新实验从改造完成起即自动固化；存量按价值排序补。

### 3.7 离线组合回测器（组合研究 harness）接口

这是把 3.44 理论上界变成可部署数字的关键工具，**纯预测分数运算 + 一次回测，不碰 GPU**：

```
输入: legs = [ {leg_id, run_id/experiment_id, weight}, ... ]   # 已验证的 alpha 腿
步骤:
  1. 按 run_id 从 MLflow 拉每条腿 pred.pkl（全截面分数）
  2. 截面标准化（rank 或 zscore）对齐 (datetime, instrument)
  3. 按 weight 融合 → 组合分数
  4. TopK 选股 + 既定策略框架(topk25/nd2/h20/V25/no-HMM) → 一次组合回测
  5. 输出: 组合 NAV / Sharpe / turnover / MDD / 与各单腿对比
扫描: 权重方案(等权 / 风险平价 / Sharpe-CV)秒级迭代，因相关已低预期差异小
```

- **首个验收实验**：用现有 5 腿（α1/α2/α3/C/A）跑通，验证理论 3.44 落地到多少（真实值会因执行摩擦/容量冲突/再平衡换手低于 3.44）。
- 与生产 advisory 融合的关系：离线 harness 用**回测 pred.pkl**做研究/定权重；生产用**实时推理分数**做交易。两者共用"融合权重"配置，但分数来源不同（见 §4.3 边界）。

### 3.8 开放设计问题（线 B 详细设计阶段回答）

1. 中心化 MLflow 后端选型：sqlite（轻、单机）vs postgres（多写、可并发归档）？artifact store：本地共享目录 vs MinIO/对象存储？
2. 保留策略与磁盘预算上限？
3. 存量回填范围（仅候选腿 vs 全量近 N 轮）？
4. 离线 harness 落在哪个模块（RDAgent scripts vs AIstock backend service）？建议 AIstock backend（贴近 advisory 融合复用）。
5. outbox 积压 17542 是否影响指针回填，是否需先修归档 worker？

---

## 4. Q3 框架：MLflow 在模型库与注册管理中的定位（需后续深化）

> 用户指示：**提升 MLflow 在今后模型库、模型注册管理中的权重，需后续详细分析。** 本节给出框架与边界，详细工单在线 B 阶段单独成文（建议 `docs/design/mlflow_model_registry_governance_design_<日期>.md`）。

### 4.1 监控 / 管理（本职，强烈推荐）

现状是"退化态"：本地散落 mlruns、无中心 UI、跑完即弃。中心化后立即获得：
- **实验追踪 UI + run 对比**：R1→R20/MA1 全谱系可视化，替代每次拉巨大 `loop_comparison` 文件。
- **指标趋势 / 演进轨迹**：与现有 qe-evolution-diagnostics 互补。
- **产物血缘**：一条 run_id 串起 `pred.pkl ↔ params.pkl ↔ conf.yaml ↔ 因子集`，审计/复现/回滚有据。

### 4.2 Model Registry ↔ StrategyPackage 生命周期映射（核心契合点）

MLflow Model Registry 的 stage 与 AIstock 既有 StrategyPackage 治理生命周期**几乎一一对应**：

| MLflow Registry | AIstock StrategyPackage | 含义 |
|---|---|---|
| None / Staging | DRAFT / candidate | 候选模型 |
| Staging（验证中） | REVIEWING / validation-runs | 验证 |
| Production | ENABLED（paper/selection enabled） | 上线 |
| Archived | ARCHIVED / RETIRED | 退役 |

**深化分析要回答**：是让 MLflow Registry 成为模型权重的**底层 source of truth**（StrategyPackage 引用 Registry model version），还是仅作镜像/旁路？这决定改造深度，需评估对现有 `model_asset_resolver` / QE-node-API 取模型路径的影响。

### 4.3 信号生成边界（关键纠偏）

**不能用 MLflow 里存的回测 `pred.pkl` 当模拟盘/实盘当日买入信号** —— 回测预测是在历史窗口算出的，直接当今天信号 = 前视/过期。**现有设计是对的**：`live_inference` 坚持当日重算因子 + 冻结模型推理。此边界**不可破**。

正确分工：
```
MLflow Registry  →  提供"用哪个模型"(版本/来源/回滚)
当日最新因子      →  model.predict() = 信号在此产生(实时推理, 非 MLflow 产生)
selection_score_artifact → advisory 多 Alpha 融合 → 买入清单 → Paper v2 / QMT
```
即 **MLflow = 模型 source of truth；信号 = 实时推理**。MLflow 不是信号生产者。

### 4.4 模型来源迁移（候选改造）

现状 `live_inference` 经 QE-node API / cache 兜底取模型权重（脆弱，依赖 RDAgent workspace 内部结构）。**目标**：改为从 MLflow Registry 加载"生产版"模型 → 提升 provenance、支持版本回滚 / A-B / 灰度，并解耦 AIstock 对 RDAgent 内部的依赖。此为 §4.2 决策的下游工单。

### 4.5 待深化分析清单（线 B 出独立设计文档）

1. Registry 作 source-of-truth vs 镜像，对 `model_asset_resolver` / `selection_artifact` 的改造面。
2. 模型版本命名/语义（因子集 + 模型族 + seed 集成）如何映射 Registry name/version/tag。
3. 与 strategy_packages 既有 manifest_sha256 冻结机制的关系（双重 source of truth 风险）。
4. 灰度/回滚/A-B 在 Paper v2 与未来 QMT 实盘的落地路径。
5. 治理审批门控（生产交易动作受外部门控）如何与 Registry stage 转换对齐。

---

## 5. 线路 A 实验设计（待用户确认后启动）

### 5.1 目标与优先级（依据 Phase0 指令）

| 优先级 | 动作 | 理由 |
|---|---|---|
| **P0** | α3_FM12+ × LSTM 补到 **n=5** | 5 腿中最大短板（n3 / CV24%），组合权重可信度依赖它 |
| **P0** | MARG10 × LSTM 3-seed **探针** | R20A GPU 失败缺口，α5 唯一未进正交矩阵的真新域 |
| **P1** | VOL12 × LSTM 3-seed **探针** | R20A GPU 缺口；B_TurnMom 已预判偏弱，仅探针验证 |
| **P1** | A_Flow（α7）补种子向 **n≥8** | 真·新独立源，要进生产组合，种子集成需足够样本 |
| **P2** | C_FundVal（α6）补种子向 **n≥8** | 最佳正交锚，已 CV2.7%，补样本为生产锁定 |

> 不再设计"找全新信号域"的 loop。规模相对原 MA2（20 loops）收敛。

### 5.2 实验矩阵（双节点，遵 QE 并行配置：GPU parallel_2 / CPU parallel_4）

**线 A-GPU（wsl2-5080，parallel_2，`__seed_LSTM_10D_hs64_d02__` cuda）：**

| Group | 组合 | seeds（避开已用） | loops | 目的 |
|-------|------|------|-------|------|
| G1 | FM12+ 24f × LSTM | 补 2 新 seed → n=5 | 2 | P0 补 α3 短板 |
| G2 | MARG10 10f × LSTM | 3 seed 探针 | 3 | P0 α5 GPU 缺口 |
| G3 | VOL12 12f × LSTM | 3 seed 探针 | 3 | P1 α4 GPU 缺口 |
| G4 | Flow12 × LSTM | 补 3 新 seed → n=8 | 3 | P1 α7 生产样本 |
| **合计** | | | **11** | |

**线 A-CPU（rdagent-node1，parallel_4，`__seed_LGBModel_conservative_v1__` cpu）：**

| Group | 组合 | seeds | loops | 目的 |
|-------|------|------|-------|------|
| H1 | Flow12 × LGBM_C | 补 3 新 seed → n=8 | 3 | P1 α7 双模型族集成候选 |
| H2 | MARG10 × LGBM_C | 补到统一 5-seed（R20B 已 4） | 2 | α5 CPU 对照 + 正交性 |
| H3 | VOL12 × LGBM_C | 补到统一 5-seed（R20B 已 4） | 2 | α4 CPU 对照 |
| H4 | C_FundVal × LGBM_C | 补 3 新 seed | 3 | α6 CPU 对照（双模型族） |
| **合计** | | | **10** | |

- 锁定配置：`topk=25 / n_drop=2 / label_horizon=20 / V25_1_SMALL_CAP / no-HMM / 10M`，回测 `2024-07-01 ~ 2026-04-27`，`stock_pool=filtered_pool_20260428`（与 MA1/Phase0 严格一致，保证可比 + 可并入正交矩阵）。
- 因子集取自 §同 MA1/R20B 已验证可运行配置（FM12+ / Flow12 / MARG10 / VOL12 / FundVal12）。
- 复用 MA2 已落盘 loops 工件（`docs/analysis/ma2_loops/`）作为 VOL12/MARG10 起点，按本矩阵调整 seed/group。

### 5.3 成功标准

- α3_FM12：n≥5，CAGR 均值 >80%，CV<15%（脱离彩票嫌疑）。
- α5 MARG10：探针 CAGR 均值 >0.65 且与现有 5 域 Jaccard<0.3 / PnL<0.5 才升格，否则降级存档。
- α4 VOL12：同上阈值；预判偏弱，达不到即不纳入组合。
- α6/α7：n≥8，锁定可部署均值配置（用于生产组合权重）。
- 所有新 run 入仓后，更新 Phase0 正交矩阵（纳入新腿）+ 重算组合 Sharpe 代理。

---

## 6. 分阶段实施顺序与里程碑

| 阶段 | 内容 | 依赖 | 状态 |
|---|---|---|---|
| M0 | 本蓝图（§1-§5）+ 用户确认存储分层 | — | ✅ 完成 |
| M1 | 线 A 实验设计（§5）确认 → 双节点启动运行 | 用户确认 | ⏳ 待确认 |
| M2 | 线 B 详细规划：中心化 MLflow 选型 + 改造点①②③ 工单化 | M1 实验运行后 | 计划 |
| M3 | 离线组合回测器（§3.7）实现 + 5 腿首次组合回测 | M2 预测固化可用 | 计划 |
| M4 | MLflow Model Registry 治理深化（§4）独立设计文档 | M2 | 计划 |
| M5 | 模型来源迁移（live_inference ← Registry，§4.4） | M4 决策 | 计划 |

---

## 7. 风险与门控

- **禁止启动服务**：MLflow tracking server / 后端改动后由用户重启，我只提醒（[[No Service Start]]）。
- **模块边界**：线 B 跨 RDAgent（workspace/mlruns）与 AIstock（qe_archive/live_inference）两侧，按模块拆 PR，避免与 Codex 并行冲突；跨模块只报告不擅改。
- **禁止 silent fallback**：预测缺失/模型缺失必须显式报错传播。
- **生产交易门控**：QMT 实盘受外部门控，Registry stage→Production 不等于自动实盘。
- **可比性铁律**：线 A 所有 loop 锁定配置必须与 MA1/Phase0 一致，否则无法并入正交矩阵。
- **回填不可逆性**：存量 `pred.pkl` 多已随 workspace 清理，回填能力受限，不承诺全量；以新实验固化为主。

---

## 8. 待决策点（用户）

1. **线 A 实验矩阵（§5.2）确认/调整**？（GPU 11 loops + CPU 10 loops，或进一步收敛）
2. **MLflow 后端选型倾向**（sqlite 轻量单机 vs postgres 可并发）—— 留待 M2 深化，是否现在定调？
3. **存量回填范围**：仅候选腿 vs 全量近 N 轮？
4. **是否先诊断 outbox 积压 17542**（只读）以确认不影响指针回填？

---

---

# 附录 A：基础设施深化（2026-06-16 第二轮，回应用户 5 问）

> 本附录回应用户对蓝图的细化提问，并以一次代码探查 + MCP 取数为依据。所有 file:line 实施前需复核。

## A1. QE 数仓 vs MLflow 边界：并存、零重复

**铁律：一个标量能表达的 → 数仓；需要按 (日期×股票) 展开或二进制 → MLflow。两边都不"维护"同一份数据。**

| 数据 | 归属 | 形态 | 是否在另一侧 |
|------|------|------|------------|
| 聚合标量指标（IC/RankIC/ICIR/CAGR/Sharpe/MDD/turnover） | **qe_archive（SoT）** | 关系行 | MLflow 的 metrics 仅作**派生/诊断**，不作权威、不双向同步 |
| 谱系/血缘（task/loop/run/seed/因子集/model_id） | **qe_archive** | 关系行 | 否 |
| 指针（mlflow_tracking_uri/artifact_uri/version） | **qe_archive（已预留列）** | 关系行 | 指向 MLflow，不复制内容 |
| **pred.pkl 全截面分数**（日×股×score） | **MLflow（SoT）** | parquet/pkl artifact | 数仓只存指针 |
| **params.pkl 模型权重** + conf.yaml | **MLflow（SoT）** | artifact | 数仓只存指针 |
| 持仓/逐笔（enhanced_metrics.all_stocks / stock_trades） | qe_archive（现状已存） | 关系/JSON | 与 pred.pkl 不重复（持仓是 pred 的下游裁剪，二者语义不同，都保留） |

**关键去重动作**：MLflow 原生 recorder 会写一份 metrics（IC 等），与 qe_archive 重叠。处理 = **以 qe_archive 为指标 SoT**，MLflow metrics 不进任何报表/排行/考核，仅留作 run 内诊断；归档管线只从 MLflow 取**指针 + 二进制产物**，不回灌标量。

## A2. 资产治理与磁盘清理（承重清单 + 分两步清理 + 门控）

**MCP 取数结论 — 策略包仅 4 个，承重源实验（绝不可删）：**

| 包 | 源实验 | 状态 | paper 组合数 | 备注 |
|----|--------|------|-------------|------|
| pkg_a2f5… | qe_20260601_172505_fe17 (R6) | BACKTEST_APPROVED | **144** | advisory 单/双包成员，重度使用 |
| pkg_0975… | qe_20260607_093306_1f70 (R14A) | BACKTEST_APPROVED | 0 | advisory 双策略包成员 |
| pkg_378e… | qe_20260520_215627_abbc | BACKTEST_APPROVED | 2 | 57f |
| pkg_2a9f… | qe_20260513_151128_12ea L1 | **SELECTION_ENABLED** | 9 | 已进选股 |

**另需保留的研究资产**：各 alpha 腿代表 run（α1 c36b_L6 / α2 0399_L4 / α3 0daa_L8 / α6/C edaf_L12 / α7/A 433d_L1 + R21 新 run）—— 它们是多 Alpha 组合的种子配置来源。

**⚠️ 删除的级联风险（探查结论）：** 删 QE task 会**级联删远端 workspace + mlruns + AIstock 缓存 + DB 行**（`qe_evolution_service.py:3499-3603`）。而策略包模型权重**默认 REFERENCE 源 workspace**（`model_asset_resolver`，`resolve_runtime_assets=False` 默认），除非已 copy 到 `rdagent_assets/model_cache/execution/`。`delete-dependencies` 只查下游（paper/qmt/approval），**不保证源 workspace 可删**。→ 删任何源实验前，必须确认其模型权重已固化为 AIstock 本地拷贝，否则 paper/选股会断。

**"用了 MLflow 后多 Alpha 数据能否固化、不再保留实验?"——是，这正是预测存储层的目的。** 一旦 pred.pkl + params.pkl 固化进中心 MLflow（改造点②），alpha 腿的研究价值与可复现性不再依赖原 workspace，即可删 experiment 释放磁盘。**但前提是固化已落地（线 B M2）。现状未固化 → 现在删 = 丢已弃的 pred.pkl + 可能断模型引用。**

**因此清理分两步：**
1. **立即可做（低风险，需先出清单经确认）**：删「无下游包引用 + 非 alpha 腿代表 + 已被取代的探索期失败 loop」（大量 R2–R5 探索 loop）；以及 model_registry 中 33 个 legacy `rdagent_task_sync` 条目（见 A5）。
2. **固化后可做（线 B M2 之后）**：把保留的 alpha 腿 pred.pkl + 权重固化进 MLflow，再删其 workspace。

**门控：本轮不直接删。** 下一步我出一份 `qe_experiment_cleanup_plan_<日期>.md`（删除清单 + 保留清单 + 每条待删项的 delete-dependencies 预检结果），你确认后才执行。磁盘真实占用在 WSL/远端节点（Windows F: 看不到），清单阶段用 MCP + 节点侧 du 核实。

## A3. 真·多 Alpha 架构路径（回测验证 → 荐股实现 → 模拟盘）

**确认此路径，并澄清关键区别：**

- **现状 = 包级 rank 融合**：advisory `weighted_rank_fusion` 让每个策略包**各自独立选股**再融合排名。这不是多 Alpha。
- **目标 = alpha 级分数融合**：多条正交 alpha 腿的**截面预测分数**→标准化→加权→**统一打分**→再选股。这才是多 Alpha 架构，且依赖预测存储层。

**落地顺序（与 M3–M5 一致）：**
1. **回测验证**（M3）：离线组合回测器用 5+ 腿 pred.pkl 做 alpha 级融合回测，验证 3.44 上界落地值。
2. **荐股实现**（M4 后）：把 advisory/selection 的融合从"包级 rank"扩展到"alpha 级分数"，复用预测存储 + 实时推理分数。
3. **再上模拟盘 → 实盘**。

## A4. 中心化 MLflow 的形态：嵌入式，不起独立服务，不用 MLflow 自带 UI

**确认探查：MLflow 可纯库模式（file:// 或 sqlite）读写，无需 `mlflow server`**（`read_exp_res.py` 已这么用）；AIstock 现无任何 mlflow UI 路由。

**设计定调（符合"尽量集成进 AIstock、不单独起 mlflow 服务"）：**
- **存储（修订 2026-06-16：复用现有 PG，不新建 sqlite）**：tracking backend 用**现有 AIstock PostgreSQL**（MLflow 的 `experiments/runs/metrics/params/tags` 等表落在**独立 schema `mlflow`** 内，与 qe_archive/app 表命名隔离）；artifact store 用**共享文件系统目录**（pred.pkl/params.pkl/conf.yaml 等二进制，PG 不存 artifact）。**理由**：① QE 双节点并行写 run，sqlite 单写锁会争用，PG 原生并发；② 复用现有 DB 备份/运维，零新增进程与文件；③ 与 qe_archive 同库不同 schema，指针 join 更顺。仍**不起 mlflow server**——后端以 MlflowClient 库模式直连 PG backend + 读 artifact 目录。
- ⚠️ 去重红线：MLflow 在 PG 里会写自己的 metrics 表，**不与 qe_archive 指标表互相同步**，MLflow metrics 仅诊断，qe_archive 仍是指标 SoT（见 A1）。
- **访问**：后端封装一个内部服务模块 `services/model_store`（或并入 quantevolver），统一「产物写入 + 读取 + 指针登记」，作为线 A 产出与线 B 消费的接口。**这是内部模块，非独立进程。**
- **UI**：**复用 AIstock 前端**。新增轻量 FastAPI 路由（`routers/mlflow_artifacts.py` 或并入 quantevolver）读 mlruns + qe_archive 指针，在现有 QE/实验页面内展示 run 对比 / pred 产物 / 模型版本。**不引入第二套 UI、不用 mlflow ui。**
- 一句话：**MLflow 降格为"嵌入式产物存储库 + 模型注册表后端"，治理与展示全在 AIstock 内；唯一新增的是一个 sqlite 文件（甚至可先用纯文件）。**

## A5. 模型库重构（数量不足 + 多为 rdagent 旧模型 + 注册 10D/20D spec）

**MCP 取数证实用户判断：qe_selectable=50 个 spec 中——**
- **33 个（66%）= legacy `rdagent_task_sync`**（model_type=`TimeSeries`，2026-03 旧 rdagent 任务），**污染选择目录**；
- 真正 curated 仅 **17 个**：`manual_10D`×10 + `manual_seed_multi_alpha`×5 + `manual_seed`×2（即 `__seed_LSTM_10D_hs64_d02__` / `__seed_LGBModel_conservative_v1__` 等）；
- **全部是 10D**——无原生 20D spec。QE 跑 h20 是靠 loop 级 `label_horizon=20` 覆盖，不是独立 spec。
- model_registry 是「**spec 定义（类型+超参 schema+qe_selectable）+ trial 记录（每 loop 一条 trained 实例）**」两层；新增 spec = catalog/config 写入（`AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED`）。

**建议（不无脑加，按已验证获胜组合 + 明确实验需求登记）：**
1. **清理**：33 个 legacy rdagent TimeSeries spec 置 `qe_selectable=false` 或 deprecate（无 QE/包引用，纯噪声）。
2. **补登记 curated 体系**（模型族 × horizon × 关键超参）：
   - **20D-horizon 原生 spec**：把现在靠 override 的 h20 固化为与 10D 并列的正式 spec（多 Alpha 用 h20，应一等公民）。
   - **各 alpha 腿"获胜模型×超参"正式 spec**：α1 PLUS3×LSTM / α2 IF18×LGBM_C / α3 FM12×LSTM / α6 FundVal×LSTM / α7 Flow×LGBM —— 作为生产候选 spec。
   - 探针候选：更大 LSTM（hidden128/多层）、TCN。
3. **与 MLflow Model Registry 的分工（线 B M4，零重复）**：
   - AIstock `model_registry` = **spec 定义**（选什么去训：类型+超参 schema+qe_selectable）。
   - MLflow Model Registry = **trained weight 实例 + stage**（训出来的权重版本/回滚/Production 标记）。
   - 二者不重复：catalog 管"训什么"，Registry 管"训出的权重治理"。

## A6. 因子相关性数据（已就绪，作未来参考）

因子相关性计算已完成，**MCP 可访问**（`aistock-factor` / `factor_corr_*`）：as_of `2026-04-30`、computed `2026-06-16`、universe `shsz_st_pit_active_v1`、method `spearman_ewma`、252 日窗口；>0.7 的高相关对 **1978 个**（`factor_corr_get_top_pairs/get_clusters/suggest_replacements`）。

**用途定位**：这是**因子层去重**的参考（构造/精简 alpha 腿因子集时避免塞入冗余因子，如已发现 `m_free_turnover_rate ↔ Industry_Volatility_Liquidity_Cross_Factor = -0.9999`），**区别于** Phase0 的**alpha 层正交性**（持仓/PnL 代理）。两者分属不同层次，都要用：因子相关性保证单腿内部不冗余，alpha 正交性保证腿之间独立。后续 alpha 腿因子集设计与 factor_corr_suggest_replacements 联动。

## A7. 蓝图增补的里程碑

| 阶段 | 新增内容 |
|------|---------|
| M1.5 | 出 `qe_experiment_cleanup_plan` 清单（删除/保留 + delete-dependencies 预检），用户确认后执行**第一步**低风险清理 + 33 legacy spec 下线 |
| M2 | 中心化 MLflow（sqlite backend + 共享 artifact）+ `services/model_store` 模块 + 改造点①②③ + **固化后执行第二步清理** |
| M4 | MLflow Model Registry 治理 + **20D/alpha 腿 spec 补登记** + 嵌入式 UI 路由（不起独立服务） |
| M4.5 | 荐股从"包级 rank 融合"升级"alpha 级分数融合"（依赖预测存储 + 实时推理） |

## A8. 本附录新增待决策点

1. **清理范围**：本轮我先出 cleanup_plan 清单（不删），你确认后执行第一步；可接受?
2. **MLflow backend**：~~sqlite~~ → **复用现有 PostgreSQL（独立 `mlflow` schema）+ 共享 artifact 目录**（用户 2026-06-16 提议，已采纳：并发更好、零新增进程，见 A4 修订）。
3. **模型 spec 补登记清单**：是否现在就拟一份（20D + 5 alpha 腿获胜组合 + 探针）待你审?
4. **alpha 级融合**在 advisory 还是 selection 层实现（M4.5）——留待 M3 回测验证后定。

---

---

# 附录 B：闭环路线图 + 横切方向 + 前沿最佳实践（2026-06-16 第三轮）

> 用户认可六阶段闭环划分，并补充 4 个横切方向（滚动再训练 / Top-K 目标对齐 / 制度感知 / 事件驱动），要求结合前沿论文与量化机构最佳实践更新。本附录据此固化路线图并补充建议。引用见 B7。

## B1. 全流程闭环 · 六阶段路线图（已认可，固化）

```
P1 单Alpha演进 ──┐                                        ┌──→ P6 实盘(QMT)
  (造零件,在跑)  │                                        │      ↑门控:paper长期达标+审批
                 ▼                                        │
P2 预测存储基础设施 ──→ P3 多Alpha离线组合验证 ──→ P4 荐股 ──→ P5 模拟盘
  (MLflow+pred固化)     (组合回测器,验3.44落地)   (alpha级融合)  (真实摩擦验证)
                 ▲                                                    │
                 └──────────── 闭环反馈:衰减腿retire/重演进/重加权 ←──┘
```

| 阶段 | 目标 | 现状 | 新建/复用 | 门控→下一阶段 |
|------|------|------|----------|--------------|
| P1 单Alpha演进与固化 | custom_evo 找+夯实独立腿，按"均值+种子CV+passes_gate"准入，每腿→single-alpha 包 | ✅在跑(R21);C_FundVal已成包 | 复用 | n≥5/CV<15%/gate通过 |
| P2 预测存储基础设施 | 中心化MLflow(复用PG+文件)+固化pred.pkl+数仓指针 | ⏳设计完;磁盘治理前置 | 新建(改造点①②③) | pred.pkl可按run_id拉取 |
| P3 多Alpha离线组合验证 | 组合回测器:拉pred.pkl→截面融合→一次回测;验3.44落地+定权重 | ❌从未跑通(最大缺口) | 新建(最高杠杆) | 组合Sharpe显著>2.45+权重锁定 |
| P4 荐股(alpha级融合) | advisory从"包级rank融合"升级"alpha级分数融合" | ⚠️现rank融合≠多alpha | 升级 | advisory-first达标 |
| P5 模拟盘 | 多Alpha绑paper v2,测真实成本/滑点 | ✅paper v2已建 | 复用 | paper跟踪≈回测预期 |
| P6 实盘(QMT) | 晋升live,Registry→Production,风控+渐进资金 | ✅QMT已铺(gated) | 复用-gated | 外部审批 |

**关键路径 = P2→P3**（唯一真缺的一段）。**磁盘治理是 P2 隐形前置**（WSL 95% 危急）。P1∥P2 并行（算力 vs 工程）。**闭环反馈臂**：P5/P6 leaderboard → 衰减腿 retire / 重演进(回P1) / 重加权(回P3)，让闭环真闭合。**advisory-first 三级门控**：P4(非交易)→P5(paper)→P6(live)。

---

## B2. 横切方向①：MLflow 驱动的滚动再训练（Rolling Retraining）

**用户洞察正确**——用最新数据滚动再训练，使预测最接近实盘。这是对抗 concept drift（市场非平稳）的标准量化实践（walk-forward / 增量再训练）。

**MLflow 的角色（与信号边界一致）：治理而非产信号。** Model Registry 是滚动再训练的天然骨架：
```
定时(月/周)再训练(最新expanding/rolling窗) → 注册新model version
   → walk-forward OOS验证(embargo+purge重叠label) → champion/challenger门
   → 新版OOS top-K指标须超现役 → 晋升Production stage / 否则回滚
```
- **champion/challenger + 自动回滚**：新模型须在近期 OOS 上击败现役才上线，否则保留现役。MLflow stage 转换 ↔ StrategyPackage 生命周期。
- **再训练超参当一等公民**：lookback 窗长 / 再训练频率 / 预测 horizon 都作为可调超参（best practice）。
- **A股特化**：政策/制度切换频繁，滚动再训练比固定模型更贴近实盘。
- 落地：扩展 P1 为"连续/滚动模式"，由 MLflow Registry 治理；服务 P4-P6 的模型新鲜度。**新增能力，纳入 P2(Registry)+P1(滚动)**。

---

## B3. 横切方向②：Top-K 目标对齐（荐股看 top-20，≠ 全局 RankIC）⭐

**用户洞察被前沿论文直接证实，是真实且重要的目标错配。** LambdaRankIC（2026）明确："现有排序指标与目标无一与 Rank IC 对齐"——NDCG 用位置折扣**集中于头部**（top-heavy），pairwise(RankNet) 等权所有错序(近Kendall's τ)，而 **Rank IC 全截面等权**。有研究用同一模型分别按 NDCG@30 与 IC 训练，证实目标选择显著影响**集中度/稳定性/经济收益**。

**对我们的含义（关键区分）：**
- **组合层 CAGR/Sharpe/turnover 已 top-K 对齐**（策略只持 topk，回测指标反映头部）——这部分没问题。
- **错配在信号/选模/组合权重层**：QE 现用 IC/RankIC/ICIR 作主指标做"选最佳loop / alpha腿排序 / 组合权重优化"——这些是**全局**指标，与"只部署 top-20"不对齐。
- **改进（纳入 P3 + 评估框架）：**
  1. qe_archive/eval **新增 top-K 指标**：precision@20、top20-fwd-return、NDCG@20、top-20 hit-rate、top-K 衰减曲线。
  2. **P3 组合权重优化用 top-K 目标**（而非全局 IC）——直接优化"前20只是否更好"。
  3. **选模/选腿改用 top-K 加权**（不再唯 IC）。
  4. 排序模型可用 **LambdaMART（刚注册）/ LambdaRankIC 式目标**直接优化排序（注意 NDCG 非光滑，用 ApproxNDCG sigmoid 近似）。
- **跨 backtest/paper/live 统一 top-K**：三者都只部署 top-K，评估口径应一致。这是方法论级精化。

---

## B4. 横切方向③：制度感知(HMM)与多 Alpha 结合

**应结合，但重新定位 + 后置。** 前沿强烈支持制度条件因子/alpha 轮动（HMM 识别制度→选最适配该制度的因子组合，跑赢任何单一模型）。

- **重新定位 HMM**：从现状"板块加减分 overlay"→ **"制度条件 alpha 加权 + risk-off 缩放"（组合层）**。不同 alpha 适配不同制度（动量→趋势市，价值/反转→震荡市）。
- **用软后验加权（非离散态）**：权重 = f(制度后验概率)，平滑过渡、降换手（best practice）。"Liberation Day"2025抛售中制度框架动态降权益、转防御，显著抑制回撤。
- **⚠️ 与 Phase0 的关键连接**：研究证实**分散收益/相关性是制度依赖的、会漂移**（2024中相关转正、2025转负）。Phase0 的 0.25 平均相关是**点估计**，制度切换时会上升 → 多 Alpha 组合**必须加滚动相关监控**，不能假设 0.25 恒定。
- **时序**：静态多 Alpha 先跑通(P3/P4)，再加制度层(P3.5/P4.5)，**不早耦合**（先让base验证）。既有板块 HMM 可共存：制度→alpha权重(新) + 制度→板块倾斜(旧)。

---

## B5. 横切方向④：事件驱动信号

**应持续完善，作并行信号源轨道。** 三种集成方式：
1. **事件 α 腿**：事件驱动信号作为一条独立 alpha 腿，走同样的 custom_evo + 验证 + 成包流程，进多 Alpha 组合。
2. **事件风险 overlay**：扩展现有 risk_policy（已处理 ST/停牌事件）到财报/公告/重大事件（财报前 block_buy、正向超预期加分）。
3. **制度输入**：新闻/事件→制度判别（前沿 LLM agentic 框架：感知市场+新闻→推断制度→调目标/风险预算/仓位上限+摩擦感知执行，walk-forward Sharpe +0.373）。
- **重启停滞的 event-signal 工作**，scoped 为"事件 α 腿 + 事件风险 overlay"，成熟后并入多 Alpha 组合。LLM agentic 事件信号是值得跟踪的前沿。

---

## B6. 其他前沿最佳实践改进建议（论文支撑）

1. **走步严谨性（直接针对我们 96%CAGR 单期回测的过拟合风险）**：QE 回测应引入 **embargo + purge**（h20 标签重叠，必须 purge）+ **两段冻结 holdout**（DEV 调参/选阈/校准，FINAL 仅评一次、参数全冻结、不再迭代）。这是对抗"回测过拟合陷阱"的核心实践——尤其我们的高 CAGR 来自 2024-07~2026-04 单一强势期。
2. **安全部署的两级不确定性**（《When Alpha Breaks》2026）：给 ranker 部署加**置信度/不确定性估计**，P5→P6 门控除收益外**加 OOS 稳定性 + 预测置信度**门槛。alpha 在制度切换时会断（论文实测 FINAL 期 60/90d RankIC 转负）。
3. **因子拥挤监控**：我们的腿有共享因子（A_Flow↔MARG10 重叠6因子）；监控拥挤。前沿用 **GAN 合成因子 / 新颖性**避免拥挤、增分散。
4. **容量/摩擦感知从一开始**（《Forecast-to-Fill》）：1.8 年 96% 在容量下不可持续；P3 组合回测器**必须含真实摩擦+容量+换手预算**（我们已有 turnover 指标，好）。
5. **制度依赖相关性监控**（见 B4）：多 Alpha 组合加滚动相关序列监控，分散收益会随制度漂移。
6. **alpha 挖掘前沿**：AlphaPROBE（检索+图上演化的 alpha 挖掘）可作 custom_evo 未来增强；VAE 降维 + 集成特征选择处理高维因子。
7. **降维/合成**：高维因子用 VAE 学低维潜表示，避免维度灾难（多 Alpha 扩腿时参考）。

---

## B7. 参考文献（2025-2026 前沿，实施前需复核结论适用性）

- LambdaRankIC: Directly Optimizing Rank IC for Financial Prediction — https://arxiv.org/html/2605.00501 （B3 核心：排序目标 vs Rank IC 错配）
- When Alpha Breaks: Two-Level Uncertainty for Safe Deployment of Cross-Sectional Stock Rankers — https://arxiv.org/pdf/2603.13252 （B6 安全部署）
- Explainable Regime Aware Investing (Wasserstein HMM) — https://arxiv.org/pdf/2603.04441 （B4 制度感知）
- Regime-Based Portfolio Allocation Using HMMs and RL — https://arxiv.org/abs/2605.27848 （B4）
- Unified Agentic Framework for Regime-Aware Portfolio Optimization with LLM Signals — https://link.springer.com/article/10.1007/s41060-026-01066-0 （B5 事件/新闻+制度）
- Increase Alpha: Performance and Risk of an AI-Driven Trading Framework — https://arxiv.org/html/2509.16707v1 （B4 制度稳健性）
- Forecast-to-Fill: Benchmark-Neutral Alpha and Capacity — https://arxiv.org/html/2511.08571v1 （B6 容量/摩擦）
- AlphaPROBE: Alpha Mining via Principled Retrieval and On-graph biased evolution — https://arxiv.org/pdf/2602.11917 （B6 alpha挖掘）
- Constructing long-short portfolio with listwise learn-to-rank — https://arxiv.org/pdf/2104.12484 （B3 LTR选股）
- Walk-Forward Analysis: Production-Ready Comparison (Static/Rolling/Expanding) — https://medium.com/@NFS303/walk-forward-analysis-a-production-ready-comparison-of-three-validation-approaches-69cd25fc9fc7 （B2 走步）

---

*本蓝图遵循 AIstock 文档规范（`docs/design/`）。落于 worktree `docs/ma1-multi-alpha-sourcing-20260615`。附录 A 回应基础设施 5 问；附录 B 固化六阶段闭环路线图 + 4 横切方向 + 前沿最佳实践。前沿论文结论需在我方数据/约束上复核后再采纳（external_evidence_only，非最终结论）。实施前所有 file:line 引用需复核当前代码。*
