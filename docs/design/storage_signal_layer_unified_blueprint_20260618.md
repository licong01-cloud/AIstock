# 统一存储 + 信号层 + Alpha 腿库 架构蓝图(独立框架·待确认)

> 文档类型:设计蓝图(DRAFT)。日期 2026-06-18。作者:strategy session。
> 状态:**独立蓝图,待用户确认**。确认后再与既有方案(P1-P6 实施计划 / P2 预测存储 / P3 正交+组合 / eval 口径)整合为后续步骤(见 §10「待整合」,本文不做整合)。
> 范围:QE 实验数据的统一存储、可删除 workspace、Alpha 腿库(含每腿独立指标)、信号层解耦、两类"补到最新"流水线。**本文只描述目标架构与差距,不含代码改动。**

---

## 1. 背景与目标

当前存储按历史演进堆叠为多层(workspace / qe_archive / MLflow / prediction-store / 策略包),存在大量重复、workspace 体积失控(≈903G,F: 仅余 221G),且多 Alpha 组合与"补到最新/滚动训练"缺少统一的数据契约与腿级记录。

**目标**:
1. **零重复存储**:每类数据有唯一权威源。
2. **workspace 可删除**:降级为临时缓存,数据入库后可 GC。
3. **Alpha 腿库**:每条腿是信号级一等实体,**记录其独立指标**,支持分类、查询、从 QE 添加、组合选择、生命周期迭代。
4. **信号层解耦**:多 Alpha 组合对模拟盘/选股/荐股**零大改**。
5. **两类补最新**:研究期重训(A)与上线滚动训练(B)各有方案。

---

## 2. 各存储层数据清单(现状)

| 层 | 主要内容 | 结构 | 权威源? | 量级 |
|---|---|---|---|---|
| 实验 workspace(qe_workspace/mlruns) | 训练模型、pred.pkl、label.pkl、params.pkl、positions/stock_trades/return_curves、portfolio_analysis、日志、分钟级回测中间件 | 文件树/每 run | 否(中间产物) | **≈903G(主体)** |
| QE 数仓 qe_archive(PG) | run 指标(CAGR/MDD/IC/TopK)、loop 配置、factor_set_hash、指针、分析视图、因子重要性/血缘/seed 稳定性 | 关系表/视图 | **是**(指标/元数据) | 小(估 <2G) |
| MLflow | (设计中)params/metrics/artifact 追踪 | PG+artifact | **当前 deferred(M4,关)** | ≈0 |
| prediction-store(P2) | pred.pkl + params.pkl + label.pkl + manifest(sha256/bytes),指针入 qe_archive | F: 文件 + PG 指针 | **是**(预测/权重 artifact) | 现空,见 §4 |
| 策略包 strategy_packages | factor_keys、模型 spec、strategy/执行策略、selection artifacts、validation runs、model_state、assets | PG + artifact | **是**(部署契约) | MB 级 |

---

## 3. 重复数据分析(冗余热点)

1. **label.pkl(最大冗余)**:label 只取决于 (区间, horizon, universe, label 定义),**与模型无关**;同口径所有 run 完全相同,却每 run 各存一份。600+ run × 同份 label。
2. **pred.pkl / params.pkl**:workspace ↔ prediction-store ↔(部署腿)model_registry,2–3 副本。
3. **指标/配置**:qe_archive(权威)↔ workspace mlruns 内嵌 ↔ 策略包内嵌 ↔(MLflow 若开)。
4. **因子快照**:已共享 `single/`(相关性复用),冗余小 —— 现状正确。
5. **workspace 明细**(positions/trades/return_curves/日志):指标入 qe_archive 后基本为可弃中间件。

---

## 4. 目标统一存储架构

### 4.1 三个 canonical 库(各管一类,互不冗余)
- **qe_archive** = 指标 / 元数据 / 血缘 / 指针(唯一权威)。
- **prediction-store** = pred / params / label artifact(唯一权威);**label 改为内容寻址共享对象**(按 sha256;同口径仅一份,manifest 存指针),或不存按需重算。
- **model_registry** = 可部署/可再推理的**完整模型 artifact**,按 (腿, train_cutoff) **版本化**。

### 4.2 workspace 降级为临时缓存 + GC
- run 完成 → 必要 artifact 入三库 + 指标入 qe_archive → 进入可回收态。
- **GC 守卫**:① 被某 strategy_package / paper / registry / alpha_leg 引用 → 保护位不删;② 入库 sha256 校验通过才允许删;③ 留宽限期(默认 7–14 天);④ 需明细归因则先压缩归档到冷盘。

### 4.3 冷热分层
- **hot(F: SSD)**:近期 run + 已验证/部署腿 + 共享 label。
- **cold(X: 4TB 机械盘)**:历史 run 压缩归档(按需明细复盘)。

### 4.4 统一寻址与网关
- `prediction-store URI + sha256` = **跨模块唯一引用**;qe_archive 存指针。
- AIstock 后端将 `model_store`/prediction-store 路由扩为**统一存储网关**;各模块只经网关读写,**禁止直接访问 workspace**。

---

## 5. Alpha 腿库(信号级一等实体)

### 5.1 定位
- **腿 = 信号级实体**(factor_set × model 产出预测);**策略包 = 部署级实体**(含执行策略)。腿在包上游。
- 多 Alpha 组合 = 选 N 条腿 → 加权融合 → 产出**一个合成包**(见 §7)。

### 5.2 分类(taxonomy):按**信号域**
量价综合 / 基本面动量 / 波动率 / 融资融券情绪 / 微观资金流 / 估值(当前 6 域,可扩)。**按域分类是正交组合的基础。**

### 5.3 实体 schema(建议字段)
- 标识:`leg_id`、`signal_domain`、`factor_set_hash`、`model_family`、`canonical_config`(factor_keys + 模型 spec + 超参 + strategy/执行配置)。
- 状态:`status`∈{candidate, validated, deployed, degraded, retired};`data_vintage`/`train_cutoff`;时间戳;血缘 `source_run_ids`(验证它的 loop)。
- 指针:`prediction_pointer`(各 vintage 的 pred)、`model_artifact_ref`(registry)、`label_ref`(共享)。
- 正交:`orthogonality_group` + 与其它腿的成对相关/Jaccard(见 §5.4)。

### 5.4 **每条腿的独立指标(必须记录 —— 用户强调,用于组合与选择)**
每腿、每 data_vintage、**多 seed 聚合(均值 + CV + 最小/最大)** 记录:
- **收益/风险(主)**:CAGR、年化、最大回撤 MDD、Calmar、Sharpe、年化换手。
- **Top-K(荐股口径)**:topk_return@20/@50、topk_hit_rate@20/@50、topk_decay、within_portfolio_rankic、topk_dispersion@20/@50、topk_observation_count。
- **信号诊断(辅)**:IC、ICIR、RankIC、RankICIR。
- **稳健性**:seed 数、CAGR 均值/CV、是否种子彩票标记。
- **成对正交(腿×腿)**:预测值 Spearman、持仓 Jaccard(来自 P3-A),作为组合选择依据。
- 来源:run 完成写 qe_archive(已有大部分),腿库**按 leg 聚合多 run/多 seed** 落一份"腿级指标快照"。

### 5.5 生命周期与迭代(不固定 6 腿)
- candidate → validated(多 seed 达标)→ deployed(进组合/实盘)→ degraded(新数据衰减)→ retired。
- 周期性重验(场景 A)刷新腿级指标;衰减腿降级/退役;组合随名册变化重优化权重。
- **防膨胀**:每腿须为独立信号域(正交);增腿由正交性 + 边际组合 Sharpe 贡献门控,拒绝高相关冗余腿。

### 5.6 录入与查询
- **从 QE 实验添加**:复用既有 `strategy_packages_create_candidate_from_qe_loop/_experiment` 的"从 QE loop 快照配置"模板,新增"loop → candidate 腿"登记(快照 config + pred 指针 + 腿级指标)。
- **专门 DB**:`alpha_leg` 登记表(+ 腿级指标表 + 成对正交表)。
- **专门 UI**:"Alpha 腿库"页 —— 按域/状态/指标过滤、看正交矩阵、多选进组合、下钻源 run;支持从 QE 实验页一键登记。
- **MCP**:只读查询腿库 + 登记候选腿(写走确认门)。

---

## 6. 两类"补到最新"流水线

### 6.1 场景 A — 研究期:训练/回测数据集补到最新(离线重验)
- 触发:人工/周期。机制:用腿库的 `canonical_config` 把结束日期延到最新,QE 重跑 → 产出**新 vintage** 的 pred/指标。
- 所需:配置 + 因子管线(新日期重算)+ 可延长快照;label 重算;**不需旧模型**。
- 产物:同腿新版本,刷新 §5.4 腿级指标,供组合用更近数据。

### 6.2 场景 B — 上线后:滚动训练(champion/challenger)
- 触发:**自动调度**。机制:定时对部署腿用最新数据重训 challenger → 近端样本外验证 → 过准入门晋升 champion(实盘信号热切换)→ 旧 champion 保留可回滚。MLflow Model Registry champion/challenger。
- 比 A 多:调度器 + 自动准入门 + champion 指针 + 回滚 + 信号层热切换。
- 存储:每次重训新模型 artifact 进 registry(按 train_cutoff 版本化)+ 新 pred;旧版本留存。

### 6.3 共性约束
库须按 (腿, vintage/train_cutoff) **版本化**存:配置(重训用)+ 完整模型 artifact(免重训再推理/扩展用)+ pred(同区间组合回测用)。**"扩到最新不重训"必须有完整模型 artifact**(仅 pred 不够)。

---

## 7. 信号层解耦(对下游零大改)

- 信号层 = prediction-store(各腿 pred)+ 组合引擎(P3-B 多权重融合)+ **合成包装器**。
- 单腿与多 Alpha 组合**产出同一形态**:combined score → 包装成**与 strategy_package 同结构的合成包**。
- 下游(selection_center 选股 → advisory 荐股 → paper 模拟盘)**消费同一契约,无需感知背后是单腿还是 N 腿组合**。
- "把信号层改成多 Alpha" = 信号层内部融合后仍输出标准包 → **paper/选股/荐股零大改**。

---

## 8. 与现有代码的差距 + 修复方向(高层;分期待整合)

| 差距 | 现状 | 修复方向 |
|---|---|---|
| label 去重 | 每 run 一份 | 内容寻址共享 label / 按需重算(改 model_store + 上传) |
| workspace GC | 无 | GC 服务 + 引用索引 + sha256 校验 + 宽限期 + 冷盘归档 |
| 模型 artifact registry 化 | params.pkl 是否=完整模型存疑;未自动注册 | run 完成对目标腿自动注册完整模型(支撑场景 B/扩展) |
| 单一真相源 | 指标/明细仍内嵌 workspace | 删前确认权威家;明细归档或接受丢失 |
| store 落库 | 现空(#1237 未合) | 先合 #1237 + 重启 + 验证 |
| 腿库 | 无正式实体 | 新增 alpha_leg 库 + 腿级指标 + 正交表 + UI/MCP |
| 滚动训练 | 无自动管线 | 场景 B 调度 + champion/challenger + 回滚 |

---

## 9. 兼容性矩阵(模拟盘 / 策略包选股 / 荐股)

| 模块 | 依赖契约 | 本蓝图影响 | 兼容 |
|---|---|---|---|
| 策略包选股(selection_center) | strategy_package + selection artifacts | 合成包同结构;artifact 经网关 | ✅ 零大改 |
| 荐股(advisory) | package + 行情只读 | 同上 | ✅ |
| 模拟盘(paper v2) | package + 执行策略 + 行情 | champion 热切换走信号层,不改 paper | ✅(B 场景需信号层热切换钩子) |
| 存储 GC | —— | 被引用 artifact 打保护位 | ✅(GC 前必须晋升被引用 artifact) |

---

## 10. 待整合(确认后再做,不在本文)
- 与 `multi_alpha_phased_implementation_plan_20260616`(P1-P6)合并:本蓝图补 P2.5(存储统一)+ P3.5(腿库)+ P5.5(滚动训练 B)。
- 与 P2 预测存储 doc:label 去重 + registry 化为增量。
- 与 P3 正交(#1227)+ P3-B 组合:腿库 + 合成包为其上下游。
- 与 eval 口径(#1184):腿级指标直接复用其 Top-K/CAGR/MDD 口径。
- 复用既有 `MultiAlphaEngine`(reuse_prediction + `--pred-backtest` + combiner),不重造。

---

## 11. 待确认决策(请逐条拍板)
1. label:**共享去重存一份** vs **不存按需重算**?(影响 store schema)
2. workspace GC 宽限期与"明细归档"范围(全弃 / 压缩归档关键明细到冷盘)?
3. 腿库是**独立新表** vs 复用/扩展 `strategy_packages_candidates`?
4. 场景 B 滚动训练是否本期纳入,还是先做 A + 腿库 + 组合,B 留后?
5. 冷盘(X: 4TB)是否作为正式冷存层纳入网关寻址?
6. 模型 artifact registry 化:对**所有 run** 还是只对**候选/部署腿**?
