# 统一存储 + 信号层 + Alpha 腿库 架构蓝图(独立框架)

> 文档类型:设计蓝图。日期 2026-06-18。作者:strategy session。
> **Rev 1(2026-06-18):§11 六项决策已确认并回填;label/workspace/X盘/registry/策略包 章节据此更新。**
> 状态:**独立蓝图,框架与决策已确认**;待与既有方案(P1-P6 实施计划 / P2 预测存储 / P3 正交+组合 / eval 口径)整合为分期实施步骤(见 §10「待整合」,本文仍不做整合、不含代码改动)。
> 范围:QE 实验数据统一存储、可删除 workspace、Alpha 腿库(含每腿独立指标)、信号层解耦、两类"补到最新"流水线。

---

## 1. 背景与目标

存储按历史演进堆叠为多层(workspace / qe_archive / MLflow / prediction-store / 策略包),重复多、workspace 体积失控(≈903G,F: 仅余 221G),多 Alpha 组合与"补最新/滚动训练"缺统一数据契约与腿级记录。

**目标**:① 零重复;② workspace 可删除(降级为缓存);③ Alpha 腿库(每腿独立指标、分类、查询、从 QE 添加、组合选择、生命周期迭代);④ 信号层解耦(多 Alpha 对模拟盘/选股/荐股零大改);⑤ 两类补最新(研究重训 A + 上线滚动 B,**B 本期纳入**)。

---

## 2. 各存储层数据清单(现状)

| 层 | 主要内容 | 权威源? | 量级 |
|---|---|---|---|
| 实验 workspace(qe_workspace/mlruns) | 训练模型、pred.pkl、label.pkl、params.pkl、positions/trades/return_curves、日志、分钟级中间件 | 否(中间产物) | **≈903G(主体)** |
| QE 数仓 qe_archive(PG) | run 指标(CAGR/MDD/IC/TopK)、配置、factor_set_hash、指针、分析视图、因子重要性/血缘/seed 稳定性 | **是**(指标/元数据/**实验史**) | 小(<2G) |
| MLflow | params/metrics/artifact 追踪 | **当前 deferred(M4)** | ≈0 |
| prediction-store(P2) | pred/params/label artifact + manifest(sha256)+ 指针 | **是**(预测/权重 artifact) | 现空,见 §4 |
| 策略包 strategy_packages | manifest(alpha_components[] / alpha_combination_policy / factor_set / model_asset / 各策略·执行·风控政策 / metrics)、selection artifacts、validation runs、model_state、assets | **是**(部署契约) | MB 级 |

---

## 3. 重复数据分析

1. **label(最大冗余)**:label = 未来 horizon 前瞻收益,**与模型无关、不同周期(horizon)不同**;同口径所有 run 完全相同却每 run 一份。→ §4.1 改为共享 canonical 序列 / 按需重算。
2. **pred / params**:workspace ↔ store ↔ registry,2–3 副本。
3. **指标/配置**:qe_archive(权威)↔ workspace 内嵌 ↔ 策略包内嵌。
4. **因子快照**:已共享 `single/`,冗余小(现状正确)。
5. **workspace 明细**(positions/trades/curves/日志):指标入 qe_archive 后基本可弃。

---

## 4. 目标统一存储架构

### 4.1 三个 canonical 库 + label/registry 策略
- **qe_archive** = 指标 / 元数据 / 血缘 / 指针 + **实验史(所有 loop 全进,见 §4.2)**。
- **prediction-store** = pred artifact(候选腿)。
- **model_registry** = 完整可再推理模型 artifact,按 (腿, train_cutoff) 版本化。
- **label 策略**:**每 (label定义, horizon, universe) 一条 canonical 时间序列**,区间延长时**追加(append)新日期**,run 引用切片;label 不在实时/信号路径(§7),纯评估侧,**可按需由价格重算**。→ **每 run 零 label 存储**。
- **registry 门控**:**只对 candidate 及以上的腿注册完整模型**(非候选 run 不注册,见 §6 分析)。

### 4.2 workspace 降级为缓存 + 全量入数仓 + 候选才进其他库
- **所有 loop → qe_archive 完整分析记录**(指标含 Top-K、配置、factor_set、因子重要性、seed 稳定性、交易统计摘要、血缘、**data_snapshot_hash/data_vintage**)→ 实验史可分析/查询,**不依赖 workspace**。
- **只有候选腿 → prediction-store(pred)+ registry(模型)**;非候选 loop 仅留 qe_archive。
- **候选判定时机(关键)**:候选常跑完后才定 → **workspace 留宽限期(默认 7–14 天)**,期内分析→标记候选→**回填**(#1237 脚本)该候选的 pred/模型到 store/registry→再 GC。
  - 子策略:模型 artifact **严格候选门控**(大/贵);pred **倾向候选回填**(贴合"只候选进其他库");#1237 当前全量自动入库,可调为"候选回填"或"全量后过期清非候选 pred"。
- **GC 守卫**:被 strategy_package/paper/registry/alpha_leg 引用 → 保护位;入库 sha256 校验通过才删;留宽限期;需深度明细则压缩归档冷盘。
- **范围界定**:qe_archive 存**可查询分析字段**,非 workspace 全量字节;逐笔/净值曲线明细→冷盘归档或接受丢失。**GC 前必须核对 qe_archive 字段覆盖度**。
- **可复现锚点**:非候选 run 将来重跑靠"配置 + 数据";故 qe_archive 必须记 **data_snapshot_hash**,否则数据更新后无法精确复现。

### 4.3 冷热分层(盘符已校正)
- **hot — F: SSD**:workspace 缓存(宽限期内)+ 近期数据。
- **hot — X: M.2 SSD**:**MLflow artifact 库(M4 启用时)+ prediction-store + model_registry + 共享 label** 等"除 workspace 外的热数据";规划 **WSL 以文件形式挂载 X**(同 F:),统一网关按挂载点寻址。(缓解 F: 95% 满。)
- **cold — E: 机械盘**:历史 run 明细压缩归档(按需复盘);**禁作热数据**。

### 4.4 统一寻址与网关
`prediction-store URI + sha256` = 跨模块唯一引用;qe_archive 存指针。后端 `model_store`/prediction-store 路由扩为**统一存储网关**,各模块只经网关读写,**禁直接访问 workspace**。

---

## 5. Alpha 腿库(信号级一等实体)+ 策略包字段缺口

### 5.1 定位与腿↔包映射
- **腿 = 信号级实体**(factor_set×model 出预测);**策略包 = 部署级实体**(含执行策略)。
- **每条独立腿 = 一个单组件 candidate 策略包**(**复用扩展 `strategy_packages_candidates`**),打 signal_domain + 补全腿级指标;**多 Alpha = 一个 N 组件包**引用这些腿。腿库 = candidates 之上的目录视图 + UI + signal_domain 分类。

### 5.2 现有策略包 manifest **已支持多 Alpha 骨架**(无需重建)
已具备:`alpha_mode`、`alpha_components[]`(alpha_id/component_weight/factor_ids/model_ref/holding_period/score_direction/score_normalization/**metrics_snapshot**/lineage)、`alpha_combination_policy`(method/weights/conflict_resolution/explainability)、factor_set、model_asset、各策略·执行·风控政策、backtest_summary。当前 C_FundVal = 单组件包。

### 5.3 需新增/增强字段(增量,向后兼容)
| # | 字段 | 加在哪 | 原因 |
|---|---|---|---|
| a | `signal_domain` | alpha_component / 腿库 | 正交分类与选腿(现仅 risk_tags) |
| b | metrics_snapshot **补全** | alpha_component | 现仅 ic/rank_ic/icir/sharpe/annual_return/mdd/turnover,**缺 Top-K 全套 + 多seed均值/CV**(§5.4) |
| c | `prediction_ref` | alpha_component | 组合需 pull 各腿 pred(现仅 model_ref) |
| d | `data_vintage`/`train_cutoff` | component + model_asset | 两类补最新 + 版本化 |
| e | champion/challenger 版本指针 | model_asset | 场景 B 热切换/回滚 |

### 5.4 **每腿独立指标(必须记录 —— 组合与选择依据)**
每腿、每 data_vintage、**多 seed 聚合(均值+CV+min/max)**:
- 收益/风险(主):CAGR、年化、MDD、Calmar、Sharpe、年化换手。
- Top-K(荐股口径):topk_return@20/@50、hit_rate@20/@50、decay、within_portfolio_rankic、dispersion@20/@50、observation_count。
- 信号诊断(辅):IC、ICIR、RankIC、RankICIR。
- 稳健性:seed 数、CAGR 均值/CV、种子彩票标记。
- 成对正交(腿×腿):预测 Spearman、持仓 Jaccard(P3-A)。
- 落地:run 完成写 qe_archive(已有大部分),腿库**按 leg 聚合多 run/多 seed** 落"腿级指标快照"。

### 5.5 生命周期(不固定 6 腿)与候选门
- candidate → validated → deployed → degraded → retired;周期重验刷新指标;衰减降级/退役;组合随名册重优化权重。
- **晋升候选门**:多 seed CAGR 均值≥阈 + CV<阈 + **独立信号域** + 与既有腿正交(P3-A 低相关)。**此门决定哪些 loop 越过 qe_archive 进 store/registry。**
- 防膨胀:每腿须独立信号域,拒高相关冗余腿。

### 5.6 录入与查询
- **从 QE 实验添加**:复用 `strategy_packages_create_candidate_from_qe_loop/_experiment` 的"从 QE loop 快照配置"模板,新增"loop→候选腿"登记(配置 + pred 指针 + 腿级指标 + signal_domain)。
- **DB**:`strategy_packages_candidates` 扩展 + 腿级指标表 + 成对正交表。
- **UI**:"Alpha 腿库"页(按域/状态/指标过滤、看正交矩阵、多选进组合、下钻源 run、从 QE 一键登记)。
- **MCP**:只读查询腿库 + 登记候选腿(写走确认门)。

---

## 6. 两类"补到最新"流水线(B 本期纳入)

### 6.1 场景 A — 研究期:数据集补到最新(离线重验)
用腿库 `canonical_config` 把结束日期延到最新,QE 重跑 → 新 vintage 的 pred/指标;因子按新日期重算、label 追加重算、快照延长;**不需旧模型(重训)**。产物刷新 §5.4 腿级指标。

### 6.2 场景 B — 上线后:滚动训练(champion/challenger)
**自动调度**对部署腿用最新数据重训 challenger → 近端样本外验证 → 过准入门晋升 champion(实盘信号热切换)→ 旧 champion 保留可回滚(MLflow Model Registry)。多出:调度器 + 自动准入门 + champion 指针 + 回滚 + 信号层热切换。每次重训新模型 artifact 进 registry(按 train_cutoff 版本化)。

### 6.3 共性 + registry 门控分析(决策 6)
库按 (腿, vintage/train_cutoff) 版本化存:配置(重训)+ 完整模型 artifact(免重训扩展/B)+ pred(同区间组合回测)。**"扩到最新不重训"必须有完整模型 artifact**(仅 pred 不够)。
- **是否所有 run 都 registry?——否**:探索/seed变体/失败 run 几乎不再用;研究重验(A)本就重训不需旧模型;全量注册 = 600+×多seed×模型 → 膨胀浪费。→ **只对 candidate 及以上注册完整模型**;探索 run 只留 qe_archive 指标(+config),pred 随 workspace GC。

---

## 7. 信号层解耦(对下游零大改)

- 信号层 = prediction-store(各腿 pred)+ 组合引擎(P3-B 多权重融合)+ **合成包装器**。
- 单腿与多 Alpha **产出同一形态**:combined score → 包装成 **strategy_package 同结构合成包**。
- 下游(selection_center 选股 → advisory 荐股 → paper 模拟盘)消费同一契约,**无需感知背后单腿/多腿**。
- **label 不在此路径**(§1 决策):实时/模拟盘决策只用 score,label 仅评估/归因用。
- "信号层改成多 Alpha" = 内部融合后仍输出标准包 → **paper/选股/荐股零大改**;**单腿模拟盘**直接走单组件包(C_FundVal 已验证)。

---

## 8. 与现有代码的差距 + 修复方向(高层;分期待整合)

| 差距 | 现状 | 修复方向 |
|---|---|---|
| label 去重 | 每 run 一份 | canonical 追加序列 / 按需重算(改 model_store + 上传) |
| workspace GC | 无 | GC 服务 + 引用索引 + sha256 + 宽限期回填 + 冷盘归档 |
| 全量入数仓 | 指标/明细部分仅在 workspace | 确认 qe_archive 覆盖分析字段 + 记 data_snapshot_hash |
| registry 化 | 未自动;params 是否=完整模型存疑 | **候选门控**自动注册完整模型 |
| store 落库 | 现空(#1237 未合) | 合 #1237 + 重启 + 验证 |
| 腿库 | 无正式实体 | 扩展 strategy_packages_candidates + 腿级指标 + 正交表 + UI/MCP |
| 策略包字段 | 缺 signal_domain/Top-K指标/pred_ref/vintage/champion | §5.3 增量加字段 |
| 滚动训练 B | 无 | 调度 + champion/challenger + 回滚 |
| X 盘热层 | 未纳入网关 | X(M.2)纳入热层 + WSL 文件挂载 |

---

## 9. 兼容性矩阵

| 模块 | 依赖契约 | 兼容 |
|---|---|---|
| 策略包选股(selection_center) | package + selection artifacts | ✅ 零大改(合成包同构) |
| 荐股(advisory) | package + 行情只读 | ✅ |
| 模拟盘(paper v2) | package + 执行策略 + 行情 | ✅(单腿包直接跑;B 需信号层热切换钩子;label 不参与) |
| 存储 GC | 引用保护位 | ✅(GC 前晋升被引用 artifact) |

---

## 10. 待整合(确认后再做,不在本文)
- 并入 `multi_alpha_phased_implementation_plan_20260616`(P1-P6):补 P2.5(存储统一)+ P3.5(腿库)+ P5.5(滚动训练 B)。
- 与 P2 doc:label 去重 + registry 候选门控为增量。
- 与 P3 正交(#1227)+ P3-B 组合:腿库 + 合成包为上下游。
- 与 eval(#1184):腿级指标复用其 Top-K/CAGR/MDD 口径。
- 复用既有 `MultiAlphaEngine`(reuse_prediction + `--pred-backtest` + combiner),不重造。

---

## 11. 决策(Rev 1 已确认)
1. label:**共享 canonical 追加序列 / 按需重算,零 per-run 存储;不在实时路径(模拟盘不用)**。✅
2. workspace:**所有 loop 全进 qe_archive(实验史)**;**仅候选腿进 store/registry**;宽限期 + 回填 + GC;非候选直接删。✅
3. 腿库:**复用扩展 `strategy_packages_candidates`**;每腿↔单组件包,多 Alpha=N 组件包;策略包按 §5.3 增量加字段。✅
4. 场景 B 滚动训练:**本期纳入**。✅
5. **X(M.2 SSD)= 热层**:放 MLflow/store/registry/共享 label;规划 WSL 文件挂载。冷归档用 E: 机械盘。✅
6. registry 模型 artifact:**只对 candidate 及以上**,非全量。✅
