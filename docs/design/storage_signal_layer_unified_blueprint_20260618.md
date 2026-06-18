# 统一存储 + 信号层 + 策略包统一模型 架构蓝图(独立框架)

> 文档类型:设计蓝图。日期 2026-06-18。作者:strategy session。
> **Rev 2(2026-06-19):统一为单一一级概念「策略包」(`alpha_mode` 区分 单/组合);取消独立的"腿/Alpha 组件库"实体。**
> Rev 1(2026-06-18):§11 六项决策确认。
> 状态:**独立蓝图,框架与决策已确认**;待与既有方案(P1-P6 / P2 / P3 / eval)整合为分期实施步骤(§10「待整合」,本文不做整合、不含代码改动)。
> 范围:QE 实验数据统一存储、可删除 workspace、策略包统一模型(每包独立指标)、信号层解耦、两类"补到最新"流水线。

---

## 1. 背景与目标

存储按历史演进堆叠(workspace / qe_archive / MLflow / prediction-store / 策略包),重复多、workspace 失控(≈903G,F: 余 221G);多 Alpha 组合与"补最新/滚动训练"缺统一数据契约与包级独立指标。

**目标**:① 零重复;② workspace 可删除(降级缓存);③ **策略包统一模型**(单/组合一个概念,每包独立指标,分类、查询、从 QE 添加、组合选择、生命周期迭代);④ 信号层解耦(多 Alpha 对模拟盘/选股/荐股零大改);⑤ 两类补最新(研究重训 A + 上线滚动 B,**B 本期纳入**)。

---

## 2. 各存储层数据清单(现状)

| 层 | 主要内容 | 权威源? | 量级 |
|---|---|---|---|
| 实验 workspace(qe_workspace/mlruns) | 训练模型、pred.pkl、label.pkl、params.pkl、positions/trades/return_curves、日志、分钟级中间件 | 否(中间产物) | **≈903G(主体)** |
| QE 数仓 qe_archive(PG) | run 指标(CAGR/MDD/IC/TopK)、配置、factor_set_hash、指针、分析视图、因子重要性/血缘/seed 稳定性 | **是**(指标/元数据/**实验史**) | 小(<2G) |
| MLflow | params/metrics/artifact 追踪 | **当前 deferred(M4)** | ≈0 |
| prediction-store(P2) | pred/params/label artifact + manifest(sha256)+ 指针 | **是**(预测/权重 artifact) | 现空,见 §4 |
| 策略包 strategy_packages | manifest(alpha_mode / alpha_components[] / alpha_combination_policy / factor_set / model_asset / 各策略·执行·风控政策 / metrics)、selection artifacts、validation runs、model_state、assets | **是**(部署契约 + **统一模型**) | MB 级 |

> 现状核实(2026-06-19):策略包库共 5 个,**全部 `alpha_mode=single_alpha`、component=1,尚无组合包**;manifest 已 `alpha_core_v1`(为多 Alpha 预留)。

---

## 3. 重复数据分析

1. **label(最大冗余)**:label = 未来 horizon 前瞻收益,**与模型无关、不同周期(horizon)不同**;同口径所有 run 相同却每 run 一份。→ §4.1 共享 canonical 序列 / 按需重算。
2. **pred / params**:workspace ↔ store ↔ registry,2–3 副本。
3. **指标/配置**:qe_archive(权威)↔ workspace 内嵌 ↔ 策略包内嵌。
4. **因子快照**:已共享 `single/`,冗余小(现状正确)。
5. **workspace 明细**(positions/trades/curves/日志):指标入 qe_archive 后基本可弃。

---

## 4. 目标统一存储架构

### 4.1 三个 canonical 库 + label/registry 策略
- **qe_archive** = 指标 / 元数据 / 血缘 / 指针 + **实验史(所有 loop 全进,见 §4.2)**。
- **prediction-store** = pred artifact(晋升为候选策略包的 run)。
- **model_registry** = 完整可再推理模型 artifact,按 (策略包, train_cutoff) 版本化。
- **label 策略**:**每 (label定义, horizon, universe) 一条 canonical 时间序列**,区间延长 **append**,run 引用切片;label 不在实时/信号路径(§7),纯评估侧,**可按需由价格重算** → **每 run 零 label 存储**。
- **registry 门控**:**只对 candidate 及以上的策略包注册完整模型**(非候选 run 不注册,见 §6)。

### 4.2 workspace 降级为缓存 + 全量入数仓 + 候选才进其他库
- **所有 loop → qe_archive 完整分析记录**(指标含 Top-K、配置、factor_set、因子重要性、seed 稳定性、交易统计摘要、血缘、**data_snapshot_hash/data_vintage**)→ 实验史可分析/查询,不依赖 workspace。
- **只有晋升为候选策略包的 run → prediction-store(pred)+ registry(模型)**;其余 loop 仅留 qe_archive。
- **候选判定时机**:候选常跑完后才定 → **workspace 留宽限期(默认 7–14 天)**,期内分析→晋升候选策略包→**回填**(#1237)pred/模型到 store/registry→再 GC。
  - 子策略:模型 artifact **严格候选门控**(大/贵);pred **倾向候选回填**;#1237 当前全量自动入库,可调为"候选回填"或"全量后过期清非候选 pred"。
- **GC 守卫**:被 strategy_package/paper/registry 引用 → 保护位;入库 sha256 校验通过才删;留宽限期;深度明细则压缩归档冷盘。
- **范围界定**:qe_archive 存可查询分析字段,非全量字节;逐笔/净值明细→冷盘归档或接受丢失;GC 前核对字段覆盖度。
- **可复现锚点**:非候选 run 重跑靠"配置+数据",故 qe_archive 必须记 **data_snapshot_hash**,否则数据更新后无法精确复现。

### 4.3 冷热分层(盘符已校正)
- **hot — F: SSD**:workspace 缓存(宽限期内)+ 近期数据。
- **hot — X: M.2 SSD**:**MLflow artifact(M4)+ prediction-store + model_registry + 共享 label** 等"除 workspace 外热数据";规划 **WSL 文件挂载 X**,网关按挂载点寻址(缓解 F: 95% 满)。
- **cold — E: 机械盘**:历史 run 明细压缩归档;**禁作热数据**。

### 4.4 统一寻址与网关
`prediction-store URI + sha256` = 跨模块唯一引用;qe_archive 存指针。后端 `model_store`/prediction-store 扩为**统一存储网关**,各模块只经网关读写,**禁直接访问 workspace**。

---

## 5. 策略包统一模型(唯一一级概念)

### 5.1 单一概念,两个正交维度
系统内**只有「策略包(Strategy Package)」一个一级实体**(消除"腿/组件库"等近似孪生概念的混淆):
- **维度① `alpha_mode`**:`single_alpha`(单)/ `multi_alpha`(组合)。
- **维度② `status`**:candidate → backtest_approved → selection_enabled → paper → retired(沿用现有)。

| 形态 | 定义 | 角色 |
|---|---|---|
| **单 Alpha 策略包**(single,component=1) | 自含 factor_set+model+pred,属一个信号域 | **基础构件**:可独立部署,**也可被组合引用** |
| **组合(多 Alpha)策略包**(multi,component=N) | `alpha_components[]` 每项 = **引用某单 Alpha 包(package_id+vintage)+ 权重 + 归一** + `alpha_combination_policy`;pred=合成、回测=组合 | **组合**:由若干基础包拼成 |

- **"Alpha 组件"不是一级实体**,仅是组合包 manifest 内部 `alpha_components[]` 的**引用条目**(package_id+权重)。
- **固定组合 = 冻结的多 Alpha 策略包**(有 manifest_sha256):同表、同生命周期、同 UI、同下游契约,**无需新命名**。

### 5.2 现有 manifest 已支持本模型(无需重建)
已具备 `alpha_mode`、`alpha_components[]`(alpha_id/component_weight/factor_ids/model_ref/score_direction/score_normalization/**metrics_snapshot**/lineage)、`alpha_combination_policy`、factor_set、model_asset、各政策、backtest_summary。当前 5 包均 single(component=1)。

### 5.3 需新增/增强字段(增量,向后兼容)
| # | 字段 | 加在哪 | 原因 |
|---|---|---|---|
| a | `signal_domain` | 单包(组合包=跨域) | 正交分类与选包组合 |
| b | metrics **补全** | 单&组合 | 现仅 ic/rank_ic/icir/sharpe/annual_return/mdd;**缺 Top-K 全套 + 多seed均值/CV**(§5.4) |
| c | `prediction_ref` | 单=自身pred / 组合=合成pred | 组合/回测取数 |
| d | `data_vintage`/`train_cutoff` | 包 + model_asset | 两类补最新 + 版本化 |
| e | champion/challenger 指针 | model_asset | 场景 B 热切换/回滚 |
| f | `alpha_components[].ref_package_id` | 组合包 | 组合按引用拼装 + 血缘 |

### 5.4 **每个策略包独立指标(必须记录 —— 组合与选择依据)**
每包、每 data_vintage、**多 seed 聚合(均值+CV+min/max)**(组合包记组合后指标):
- 收益/风险(主):CAGR、年化、MDD、Calmar、Sharpe、年化换手。
- Top-K(荐股口径):topk_return@20/@50、hit_rate@20/@50、decay、within_portfolio_rankic、dispersion@20/@50、observation_count。
- 信号诊断(辅):IC、ICIR、RankIC、RankICIR。
- 稳健性:seed 数、CAGR 均值/CV、种子彩票标记。
- 成对正交(包×包,仅单包间):预测 Spearman、持仓 Jaccard(P3-A),供组合选择。
- 落地:run 完成写 qe_archive(已有大部分),策略包**按包聚合多 run/多 seed** 落"包级指标快照"。

### 5.5 库 = 全部策略包(过滤即视图)
- `alpha_mode=single` → 可组合的**基础信号集**(取代原"组件库"概念,实为策略包过滤视图)。
- `alpha_mode=multi` → 已成型**组合**。
- 叠加 `status` / `signal_domain` 过滤。

### 5.6 生命周期(不固定;持续迭代)与候选门
- candidate → backtest_approved → selection_enabled → paper → degraded → retired;周期重验刷新指标;衰减降级/退役;组合随名册重优化权重。
- **晋升候选门**:多 seed CAGR 均值≥阈 + CV<阈 + 独立信号域 + 与既有单包正交(P3-A 低相关)。**此门决定哪些 loop 越过 qe_archive 成为(候选)策略包、进 store/registry。**
- 防膨胀:每个单包须独立信号域,拒高相关冗余。

### 5.7 录入与查询
- **从 QE 实验添加**:沿用 `strategy_packages_create_candidate_from_qe_loop/_experiment`,创建**单 Alpha 候选策略包**(快照配置 + pred 指针 + 包级指标 + signal_domain)。
- **组合创建**:选若干**单 Alpha 策略包** + 权重方案 → 生成**多 Alpha 候选策略包** → `--pred-backtest` 组合回测 → 晋升。
- **UI**:统一"策略包库"页(alpha_mode/status/signal_domain/指标 过滤、看正交矩阵、多选基础包组合、下钻源 run、从 QE 一键登记)。
- **MCP**:只读查询策略包库 + 登记/组合(写走确认门)。

---

## 6. 两类"补到最新"流水线(B 本期纳入)

### 6.1 场景 A — 研究期:数据集补到最新(离线重验)
用策略包 `canonical_config` 把结束日期延到最新,QE 重跑 → 新 vintage 的 pred/指标;因子按新日期重算、label 追加、快照延长;**不需旧模型(重训)**。刷新 §5.4 包级指标。

### 6.2 场景 B — 上线后:滚动训练(champion/challenger)
**自动调度**对部署策略包用最新数据重训 challenger → 近端样本外验证 → 过准入门晋升 champion(实盘信号热切换)→ 旧 champion 留存可回滚(MLflow Model Registry)。多出:调度器 + 自动准入门 + champion 指针 + 回滚 + 信号层热切换。每次重训新模型 artifact 进 registry(按 train_cutoff 版本化)。

### 6.3 registry 门控(决策 6)
库按 (策略包, vintage/train_cutoff) 版本化存:配置(重训)+ 完整模型 artifact(免重训扩展/B)+ pred(同区间组合回测)。**"扩到最新不重训"必须有完整模型 artifact**(仅 pred 不够)。**只对 candidate 及以上的策略包注册完整模型**;探索 run 只留 qe_archive 指标(+config),pred 随 workspace GC。

---

## 7. 信号层解耦(对下游零大改)

- 信号层 = prediction-store(各单包 pred)+ 组合引擎(P3-B 多权重融合)+ **合成包装器(输出多 Alpha 策略包)**。
- 单包与组合包**同结构**:combined score → 包装成 strategy_package(multi)。
- 下游(selection_center 选股 → advisory 荐股 → paper 模拟盘)**消费"策略包",不区分单/组合** → **零大改**。
- **label 不在此路径**:实时/模拟盘决策只用 score;label 仅评估/归因。
- **单 Alpha 模拟盘 = 跑单包(已验证,如 pkg_a2f5… 已 150 paper 组合);多 Alpha 选股 = 跑组合包**。

---

## 8. 与现有代码的差距 + 修复方向(高层;分期待整合)

| 差距 | 现状 | 修复方向 |
|---|---|---|
| label 去重 | 每 run 一份 | canonical 追加序列 / 按需重算(改 model_store + 上传) |
| workspace GC | 无 | GC 服务 + 引用索引 + sha256 + 宽限期回填 + 冷盘归档 |
| 全量入数仓 | 指标/明细部分仅在 workspace | 确认 qe_archive 覆盖分析字段 + 记 data_snapshot_hash |
| registry 化 | 未自动;params 是否=完整模型存疑 | **候选门控**自动注册完整模型 |
| store 落库 | 现空(#1237 未合) | 合 #1237 + 重启 + 验证 |
| 策略包字段 | 缺 signal_domain/Top-K指标/pred_ref/vintage/champion/ref_package_id | §5.3 增量加字段 |
| 组合引擎/库 UI | 无组合包;无统一库 UI | 复用 MultiAlphaEngine(`--pred-backtest`+combiner)出组合包 + 统一策略包库页 |
| 滚动训练 B | 无 | 调度 + champion/challenger + 回滚 |
| X 盘热层 | 未纳入网关 | X(M.2)纳入热层 + WSL 文件挂载 |

---

## 9. 兼容性矩阵

| 模块 | 依赖契约 | 兼容 |
|---|---|---|
| 策略包选股(selection_center) | 策略包(单/组合同构)+ selection artifacts | ✅ 零大改 |
| 荐股(advisory) | 策略包 + 行情只读 | ✅ |
| 模拟盘(paper v2) | 策略包 + 执行策略 + 行情 | ✅(单包已跑;组合包同构;B 需信号层热切换钩子;label 不参与) |
| 存储 GC | 引用保护位 | ✅(GC 前晋升被引用 artifact) |

---

## 10. 待整合(确认后再做,不在本文)
- 并入 `multi_alpha_phased_implementation_plan_20260616`(P1-P6):补 P2.5(存储统一)+ P3.5(策略包统一模型/库 UI)+ P5.5(滚动训练 B)。
- 与 P2 doc:label 去重 + registry 候选门控为增量。
- 与 P3 正交(#1227)+ P3-B 组合:组合引擎产出多 Alpha 策略包。
- 与 eval(#1184):包级指标复用其 Top-K/CAGR/MDD 口径。
- 复用既有 `MultiAlphaEngine`(reuse_prediction + `--pred-backtest` + combiner)与 `strategy_packages_*`,不重造。

---

## 11. 决策(Rev 2 已确认)
1. label:**共享 canonical 追加序列 / 按需重算,零 per-run 存储;不在实时路径(模拟盘不用);不同周期(horizon)各一份**。✅
2. workspace:**所有 loop 全进 qe_archive(实验史)**;**仅晋升候选策略包的 run 进 store/registry**;宽限期 + 回填 + GC;其余直接删;qe_archive 记 data_snapshot_hash。✅
3. **统一模型**:系统只有一个一级概念「策略包」,`alpha_mode` 区分 单/组合;**取消独立"腿/Alpha 组件库"实体**;"Alpha 组件"仅为组合包 manifest 内部引用;固定组合=多 Alpha 策略包,无需新名;复用扩展 `strategy_packages_candidates` + §5.3 增量字段。✅
4. 场景 B 滚动训练:**本期纳入**。✅
5. **X(M.2 SSD)= 热层**(MLflow/store/registry/共享 label;规划 WSL 文件挂载);冷归档用 E: 机械盘。✅
6. registry 模型 artifact:**只对 candidate 及以上策略包**,非全量。✅
