# Multi-Alpha 架构审计与改进方向

> 日期: 2026-05-18 | 状态: 分析完成，待实施
> 范围: AIstock QE Multi-Alpha 全链路 (调度 / 训练 / 合并 / 回测 / 持久化 / 演进)
> 基于实际代码: `multi_alpha_engine.py` / `multi_alpha_result_collector.py` / `meta_model.py` / `quantevolver.py:_run_multi_alpha_experiment`

---

## 0. 文档目的

本文档不针对单 Loop bug 修复，而是回答两个战略问题：

1. **当前多 Alpha 架构距离"超越单 Alpha"还差什么？**
2. **要让多 Alpha 成为 QE 主路径并接入 MCP 进行自动演进，必须先解决哪些阻断项？**

所有结论严格基于 2026-05-18 仓库中可见的代码逻辑，不引用过期记忆中的 file:line。

---

## 1. 当前架构现状（事实陈述）

### 1.1 训练-合并-回测三段式管线

`MultiAlphaEngine.run()` (`multi_alpha_engine.py`) 生成的 workspace 结构是：

```
loop_root/
├── group_<G1>/    # train-only：训练 + pred.pkl（不回测）
├── group_<G2>/    # train-only
├── ...
├── meta_model_runner.py        # 主节点执行
├── conf.yaml                    # 主节点统一回测配置
├── qrun_limit_minute.py         # 支持 --pred-backtest
├── prepare_factors.py           # 主节点统一回测的因子准备
└── combined_prediction.pkl      # meta_model_runner 产出
```

`_compose_group_experiment()` 强制 `train_only=True, execution_algo=None`。
`_generate_unified_backtest_files()` 用 `first_group` 的因子构造主节点回测 bundle。
`meta_model_runner.py` 末尾 `subprocess.run(["qrun_limit_minute.py", "--pred-backtest", "combined_prediction.pkl"])`。

> **架构方向是正确的**：组训练分散 → 主节点统一回测。这与用户提出的"模型分开训练，合并后执行回测"完全一致。

### 1.2 结果收集分两路

`MultiAlphaResultCollector.collect_and_persist()`：

- `is_distributed = len({assigned_node_id}) > 1`
- **单节点路径**：`_validate_single_node_artifacts` → `_fetch_combined_metrics` → `_fetch_multi_alpha_results_json` → `_fetch_single_node_group_enhanced`，对 4 类必需产物做强校验（含 `combined_prediction.pkl` / `multi_alpha_results.json` / 主 `qlib_results_enhanced.json` / 各组 `qlib_results_enhanced.json`）。
- **分布式路径**：`_collect_distributed` 跨节点拉 `pred.pkl` + `qlib_results_enhanced.json` + 一份 `label.pkl`，本地跑 `MetaModelCombiner`，再用 `qrun_limit_minute.py --pred-backtest` 做合并后回测（这部分需在主节点流程中完成，当前 collector 内部并未触发统一回测）。

### 1.3 Meta-Model

`meta_model.py` 与 `meta_model_runner.py` 都实现了 `equal / ic_weighted / ols / stacking`。
但 `_collect_distributed` 在 `label` 不可用时退化为 `abs(Rank IC)` 加权（绝对值，不抛异常）。
`meta_model.py::_ic_weighted` 仍然在遇到 `avg_ic <= 0` 时直接抛 `ValueError`。
`meta_model_runner.py::combine` 同样在 `mean_ic <= 0` 时抛 `RuntimeError`。

> **两套实现策略不一致**：collector 用 `abs()` 容忍负 IC；runner 不容忍。

### 1.4 因子缓存

`compute_nodes` 表已有 `factor_cache_dir` 字段，主节点路径走 `${FACTOR_CACHE_DIR:-默认}`，远端节点走显式 `export FACTOR_CACHE_DIR`。理论上多 Alpha 各组训练命中缓存即可，**不应**触发因子重算。

### 1.5 演进/MCP 接入

当前没有"多 Alpha 专用演进 action"。
QE 的 `factor_adjust / param_tune / model_switch / factor_model_joint` 只针对单 Alpha 配置。
MCP 工具列表中目前没有暴露"创建多 Alpha 实验 / 查看多 Alpha 诊断 / 触发多 Alpha 演进"的端点。

---

## 2. 用户纠正后的关键认识修正

| 上一版误判 | 修正后正确认识 |
|-----------|---------------|
| 负 IC 因子应被 0 权重剔除 | **不能直接置 0**。LGBM/树模型自身可学习负向单调关系；负 IC 在因子层是合理的强信号，剔除会造成大量信息丢失。处理应在 *组级* 而非 *因子级* |
| 多 Alpha 必须独立做"全链路（训+回）" | 用户要求与现状一致：**组级只训不回，主节点合并后统一回测一次** |
| 因子缓存内存泄漏是阻断 | 内存问题已修；缓存命中路径下 train 阶段不应再触发重算。若仍发生，是缓存命中失败的次生问题，而非内存管理本身 |
| 动态权重是必备 | 动态权重是 *研究方向*，不是上线必备。MVP 用静态 IC 加权完全可上线 |
| Regime-aware 是 P0 改进 | Regime-aware 需要独立训练/学习模块，归入研究方向（与 HMM 项目可能耦合） |

---

## 3. 阻断性问题（P0，必须先解决）

### P0-A：组级负 IC 处理策略不一致 + 错误地"非正即抛"

**事实**：
- `meta_model.py::_ic_weighted` L123-124：`if avg_ic <= 0: raise ValueError`
- `meta_model_runner.py::combine` 同样在 `mean_ic <= 0` 时 raise
- `_collect_distributed` 退化路径却用 `abs(Rank IC)` 容忍负值

**问题**：
- 单 Alpha 中树模型可吃下负 IC 因子；多 Alpha 中"组"的负 IC 也并不必然意味着该组无价值——它可能是**反向有效信号**（特别是反转/拥挤度组）。
- 当前实现一旦有任意一组训练出负 IC，**整个多 Alpha 实验直接崩溃**，无法进入回测阶段。
- 而且分布式 `abs()` 与单节点 `raise` 行为不一致，结果不可重复。

**正确做法**：
1. **组级权重计算统一改为允许负 IC**：
   - 方案 A（推荐）：`weight_g = max(IC_g, 0) / Σ max(IC_k, 0)`，对负 IC 组隐式 0 权重，但不抛异常。
   - 方案 B：保留符号 `weight_g = IC_g / Σ |IC_k|`，允许"负权重组"实质做空合成。
   - 方案 C：用 `abs(IC_g)`，前端展示原始符号供分析。
2. **MVP 选 A 或 C**，把"是否启用反向组"作为 `meta_model.allow_negative_weight: bool` 配置项，默认 false。
3. **统一 `meta_model.py` / `meta_model_runner.py` / `_collect_distributed` 三处实现**：必须共用同一个权重函数（拉到 `meta_model.py` 单一来源），消除三套语义。

---

### P0-B：训-合-回链路的端到端可达性

**事实**：
- `MultiAlphaEngine` 已经把统一回测的依赖（conf.yaml/qrun_limit_minute.py/prepare_factors.py/strategy modules/.b64 payloads）放到 loop_root。
- `meta_model_runner.py` 末尾会调用 `qrun_limit_minute.py --pred-backtest combined_prediction.pkl`。
- 但是 **`_run_multi_alpha_experiment` 只把组任务下发到各节点**，主节点的 `meta_model_runner.py` 由谁、在何时、在哪里执行，需要在代码中显式定位（提交单个含 meta-runner 的"主任务"，还是各节点训完后再回调主节点拉结果跑 runner）。
- `_collect_distributed` 只到"算 combined 指标 + 写表"为止，并不触发 `qrun_limit_minute.py --pred-backtest`。

**风险**：
- 单节点路径（serial / local_parallel）：依赖 RD-Agent 把 `meta_model_runner.py` 当作普通 loop 末尾脚本执行。需要确认 `qrun_limit_minute.py` 的 entrypoint 是否会自动运行根目录的 meta_model_runner（否则需要在生成的 entrypoint 中显式串联）。
- 分布式路径：N 节点训完后，组级 pred 在各远端节点。**主节点需要在所有组 status=completed 时**：
  1. 把所有 pred 拉到主节点 loop_root；
  2. 在主节点跑 `meta_model_runner.py` → 生成 combined_prediction.pkl；
  3. 在主节点跑 `qrun_limit_minute.py --pred-backtest`；
  4. 把回测产出的 mlruns + qlib_results_enhanced.json 汇入 collector。

**当前 `_collect_distributed` 缺第 2 步和第 3 步的显式触发**，导致历史上观察到的"远端训练完成但主节点没有回测"问题。

**修复策略**：
- 在 `_collect_distributed` 的 step 2 之后 step 3 之前插入一个"在主节点 loop_root 执行 unified backtest"的 stage。该 stage 可以复用 `meta_model_runner.py` 但需要保证：
  - 主节点 loop_root 已经持有所有组的 pred.pkl 副本；
  - 主节点的 `prepare_factors.py` 跑得通（命中因子缓存而不是重算）；
  - 主节点能拿到 mlruns artifacts，再让 `_validate_single_node_artifacts` 改造的"分布式 unified 校验"通过。

---

### P0-C：UI 多节点日志不可见

**事实**：UI 当前只能拉到"提交节点"的实时日志流。分布式下另一节点的 train 进度无法在前端看到。

**影响**：训练阶段长达数十分钟，用户无法判断卡在哪个节点。日常排错完全依赖 SSH。

**修复方向**：
- 工程层：`compute_nodes.callback_url` 已有；为各节点的 RD-Agent task 注册 *统一日志聚合 endpoint*，让主节点轮询/接收各节点 stdout 增量并写入 `qe_evolution_loops.log_text` 或独立的 `qe_node_logs` 表。
- UI 层：实验详情页按节点 tab 展示日志流；日志聚合表写入即推（websocket 或 polling）。

> 这是 P0 但不阻塞收益，可与 P0-A、P0-B 并行修。

---

### P0-D：因子缓存自动同步缺位（issue 化提交）

**事实**：跨节点共享靠 `compute_nodes.factor_cache_dir` 注入环境变量，但**远端节点本地缓存**何时与主节点同步、由谁触发，目前是**手动 rsync**。

**影响**：
- 缓存未同步 → 远端命中失败 → 走回到 prepare_factors.py 重算因子 → 之前已修复的内存优化在缓存未命中时无效，重新出现 OOM 风险。
- 多 Alpha 上线后训练任务密度上升，手动同步不可持续。

**应做**：
1. 确认设计文档中的"自动同步"模块是否真的在生产路径上启用；如已实现但未触发，定位 hook 缺失点。
2. 若未实现，作为独立 issue 提交到流水线，要求：
   - 实验提交前对各 `assigned_node_id` 做 cache freshness 校验（compare `_meta.json` source_hash + date_range vs 实验所需）。
   - 不一致时自动 rsync 增量；rsync 失败 fail-fast，不允许"悄悄重算"。
   - 把 cache freshness 校验结果写入 `qe_multi_alpha_groups`（新增列 `cache_status`），方便诊断。

---

## 4. 业务/性能改进项（P1，上线后立刻补）

### P1-A：label_horizon 与单 Alpha 对齐（10D）

**事实**：
- 单 Alpha 已基本验证 `label_horizon=10` 效果最佳（参见 `qe_label_horizon_unified_config_plan.md`）。
- `MultiAlphaEngine._compose_group_experiment` 走 `compose_experiment_in_memory`，`label_horizon` 来源于 `custom_params`，由 `_run_multi_alpha_experiment` 注入。
- `experiment_config_builders.build_config_from_multi_alpha` 接收 `label_horizon`，与单 Alpha 同源。

**风险**：
- 各组共用一个 `label_horizon` 没问题。
- 但如果未来允许"组级 label_horizon 不同"（如基本面组 20D + 价量组 5D），meta 合并时**必须用同一个 label**计算 IC 权重和 combined IC，否则权重不可比。

**对齐策略**：
- MVP：硬约束**全实验单一 label_horizon**，在 `MultiAlphaEngine.__init__` 校验各组 `model_params` 中不得各自指定 `label_horizon`。
- 默认值与单 Alpha 一致：当前 `label_horizon=10`（在 `init_catalog_db.py` / 前端默认中固化）。
- 后续如要做"多 horizon 组合"，需独立设计，不在本次范围。

---

### P1-B：组合阶段的策略 / topk 一致性

**事实**：
- 组训练 `train_only=True, execution_algo=None`。
- 主节点统一回测从 `first_group` 取因子 + 实验级 `strategy_id` + `execution_algo` + `strategy_params`。

**问题**：
- 主节点回测的 strategy/execution_algo 是 *实验* 级，与单 Alpha 实验完全一致 → 这没问题。
- 但 `disable_alpha158=True` 默认在多 Alpha 是硬注入的（`multi_alpha_engine.py` L219-220 与 L302-303），与单 Alpha 默认是否一致需要核对。如果单 Alpha 仍开 Alpha158，多 Alpha 关 Alpha158，两者 IC 不可比，"超越单 Alpha"的对照就不公平。

**应做**：
- 把 `disable_alpha158` 提到 `MultiAlphaConfig` 顶层，前端可见可改，默认与单 Alpha 一致；
- 或在前端显式提示用户：当前多 Alpha 默认禁用 Alpha158，如要做对照，请同步单 Alpha 配置。

---

### P1-C：组配置时的相关性硬闸（事前约束）

**事实**：当前 `qe_group_prediction_correlations` 表只在跑完后记录，UI 在诊断页可见，但**组配置阶段没有相关性检查**。

**影响**：用户/Agent 容易构造出"两个组 95% 重叠"的伪多样化配置，多 Alpha 退化为"双倍训练成本 + 接近单 Alpha 收益"。

**应做**：
- compose 阶段提供 `/multi-alpha/preview-correlation` 端点：用因子值缓存对组合候选做 *因子层平均相关性*（所有 cross-group pair Spearman），超阈值（如 |ρ|>0.7）拒绝或警告。
- 用户/MCP 调用时强制看到这一指标。

---

### P1-D：合并方法选择与 stacking 复用

**事实**：`meta_model.py` 已有 `stacking`（LightGBM 二层 + Purged K-Fold），但 `meta_model_runner.py` 只实现 `equal / ic_weighted`。

**影响**：用户配置 `meta_model.method=stacking` 时：
- 单节点路径靠 RD-Agent 末尾脚本，runner 内不支持 stacking → 落到 ic_weighted。
- 分布式路径 `_collect_distributed` 在主节点跑 `MetaModelCombiner` → 支持 stacking。

两条路径行为不一致，记忆中"两套语义"现象的另一处。

**应做**：
- runner 内 import `MetaModelCombiner` 而不是重写。考虑到从节点 WSL 容器中无法 `import aistock.*`，需要把 `meta_model.py` 拷贝/打包到 loop_root（与 `meta_model_runner.py` 同步生成）。这把"standalone runner"的设计转为"runner+依赖文件"，但消除两套实现。

---

## 5. 必要功能 vs 研究方向

用户明确提出："动态权重是否是必要条件？regime-aware 是否需要独立学习？"
按是否阻塞 MVP 上线区分如下：

### 5.1 MVP 上线必要条件

| # | 条目 | 状态 |
|---|------|------|
| 1 | 训-合-回端到端跑通（P0-B） | 阻断 |
| 2 | 负 IC 组不抛异常（P0-A） | 阻断 |
| 3 | 多节点日志可见（P0-C） | 强烈建议 |
| 4 | 因子缓存自动同步（P0-D） | 强烈建议 |
| 5 | label_horizon 全实验统一为 10（P1-A） | 必须 |
| 6 | meta_model 三套实现合一（P1-D） | 必须 |
| 7 | 组配置相关性事前闸门（P1-C） | 建议 |
| 8 | 合并方法默认 ic_weighted，可选 equal/ols/stacking | 必须 |

### 5.2 研究方向（不阻塞 MVP，独立立项）

| # | 研究方向 | 与现有项目耦合 | 价值 |
|---|---------|---------------|------|
| R1 | **动态权重 / 在线学习** | 改造 `MetaModelCombiner.fit_and_combine`，引入 EWMA 或卡尔曼滤波；可选 LightGBM stacking 已有雏形 | 中（视市场状态切换频度） |
| R2 | **Regime-aware 权重切换** | 强耦合现有 HMM 项目（`sector_hmm_model_path`/`hmm_signal_preset` 已在 custom_params）。可让 HMM regime 输出作为 meta 层 gating | 高（A 股牛熊切换显著） |
| R3 | **智能因子分组** | 用因子值缓存做层次聚类（complete linkage），自动产出低相关组配置。已有 `factor_cluster_backfill_20260423` 工作可复用 | 高（直接提升组多样性） |
| R4 | **风险平价 / MVO 在合并层** | 引入 `multi_alpha_strategy_design_20260308.md` Phase 2 的 cvxpy/LedoitWolf | 中（提升 Sharpe 不一定提升年化） |
| R5 | **组级模型异构（LGBM + GRU + 线性）** | 已经支持组级 model_id，但生产基本只用 LGBM | 中（多样性收益） |
| R6 | **增量演进与热启动** | 当前 reuse_prediction 已能复用整组预测；reuse_model 暂未支持 | 中（节省训练时间） |

> R1-R6 都需要独立设计文档与训练数据；不应阻塞 MVP。

---

## 6. 数仓驱动的演进决策（核心设计）

### 6.1 问题：多 Alpha 演进需要什么数据？

单 Alpha 的 QE 演进已有完整的"数据驱动决策"链路：

```
_build_full_evolution_history(task_id)
  -> 查询 qe_evolution_loops 全部已完成 loop
  -> 构建: ic_trend / action_type_stats / failed_approaches / consecutive_same_action / unexplored_directions
  -> 传入 Analyst.run_analyst() 的 evolution_history 参数
  -> LLM 基于历史数据做诊断 + 方向决策
```

**多 Alpha 演进同样需要基于历史数据做决策**，但决策维度更多：

| 决策维度 | 数据来源 | 单 Alpha 有 | 多 Alpha 需要 |
|---------|---------|------------|--------------|
| IC 趋势 | `qe_evolution_loops.metrics_json` | Y | Y combined IC 趋势 |
| 方向有效性 | `action_type_stats` (win_rate) | Y | Y 但 action_type 不同 |
| 失败方向回避 | `failed_approaches` | Y | Y 需记录"哪个组的什么改动失败了" |
| **组级 IC 变化** | `qe_multi_alpha_groups` | N | Y 哪个组在拖后腿 |
| **组间相关性趋势** | `qe_group_prediction_correlations` | N | Y 相关性是否在收敛 |
| **Meta 权重漂移** | `qe_meta_model_weights` | N | Y 权重是否越来越集中 |
| **单 Alpha baseline** | `qe_experiments WHERE alpha_mode='single'` | N | Y 多 Alpha 是否真的超越了单 Alpha |
| **因子库全局 IC** | `aistock_factor_catalog` | 部分 | Y 选因子时需要全局视角 |

### 6.2 多 Alpha 演进的 action_type 体系

单 Alpha 有 4 种 action：`factor_adjust / param_tune / model_switch / factor_model_joint`。

多 Alpha 需要**组合级**的 action 体系：

| action_type | 含义 | 决策依据 |
|-------------|------|---------|
| `group_factor_adjust` | 调整某组的因子列表 | 该组 IC 低 + 因子库有更好候选 |
| `group_model_switch` | 切换某组的模型 | 该组 ICIR 低 + 其他模型在类似因子上表现更好 |
| `add_group` | 新增一个组 | 现有组间相关性低 + 因子库有未覆盖的数据源 |
| `remove_group` | 删除一个组 | 该组 weight 接近 0 + 连续 N 轮无改善 |
| `merge_groups` | 合并两个高相关组 | 组间 corr>0.7 + 合并后因子数合理 |
| `meta_tune` | 调整 meta 方法/参数 | 权重垄断 + ICIR-weight mismatch |
| `rebalance` | 重新分配因子到各组 | 全局相关性分析后的最优分组 |

### 6.3 现有规则引擎现状（multi_alpha_diagnostics.py）

`MultiAlphaDiagnostics.identify_bottlenecks()` 已经实现了 4 条规则，但只能覆盖 7 种 action_type 中的 4 种，且全部基于"当前实验快照"判断，**没有时序数据和因子库视角**。

| Rule ID | 触发条件 | 输出 action_type | 严重度 |
|---------|---------|-----------------|------|
| `zero_weight` | `meta_weight < 0.05` 或 `abs(IC) < 0.01` | `remove_group` / `switch_model` | high / medium |
| `high_correlation` | 组间 `abs(corr) > 0.7` | `merge_groups` | medium |
| `icir_weight_mismatch` | ICIR top-2 但 weight bottom-2（需 >= 4 组） | `tune_meta` | low |
| `weight_monopoly` | 单组 `weight > 0.6` 且 >= 3 组 | `add_factors` | medium |

### 6.4 7 种 action_type 的覆盖情况

| action_type | 已覆盖？ | 缺口 |
|-------------|---------|------|
| `remove_group` | 部分 | `zero_weight` 触发；缺"连续 N 轮无改善"的时序判断，当前只看单次快照 |
| `group_model_switch` | 部分 | `zero_weight` 中 IC<0.01 触发；缺"其他模型在类似因子上表现更好"的对比逻辑 |
| `merge_groups` | 是 | 基本够用 |
| `meta_tune` | 是 | 缺"lookback_days 不合适"和"method 选择不当"的判断 |
| `group_factor_adjust` | **否** | 完全缺失 |
| `add_group` | **否** | `weight_monopoly` 建议"增强其他组"但不是真正的"新增组" |
| `rebalance` | **否** | 完全缺失 |

### 6.5 需要新增的规则

#### Rule 5: `group_factor_adjust` — 组内因子质量退化

```
触发条件:
  - 某组 group_ic 在最近 3 轮实验中持续下降（需要历史数据）
  - 或: 该组内某些因子的独立 IC（来自 aistock_factor_catalog）远低于组平均
  - 或: 因子库中有同 category 但 IC 更高的因子未被该组使用

数据依赖:
  - qe_multi_alpha_groups 历史记录（同 task 下多轮实验的同名组 IC 变化）
  - aistock_factor_catalog（因子独立 IC + category）
  - 当前组的 factor_names 列表

输出:
  action_type: "group_factor_adjust"
  action_params: {
    target_group: "tech_group",
    weak_factors: ["factor_a", "factor_b"],
    candidate_factors: ["factor_x", "factor_y"],
    reason: "group IC declined 3 consecutive rounds; factor_a IC=0.005 far below group avg 0.035"
  }
```

**当前缺失原因**：`identify_bottlenecks` 只接收当前实验的 `groups` 和 `correlations`，没有历史数据输入，也没有因子库查询。

#### Rule 6: `add_group` — 因子库有未覆盖的独立数据源

```
触发条件:
  - 因子库中存在某个 category/source 的因子完全未被任何组使用
  - 且该 category 的平均 abs(IC) > 0.02（有价值）
  - 且该 category 与现有各组的因子相关性 < 0.5（真正独立）
  - 或: weight_monopoly 触发 + 现有组数 < max_groups

数据依赖:
  - aistock_factor_catalog（全量因子 + category + source）
  - 当前所有组的 factor_names（已覆盖的因子）
  - 因子值缓存（计算候选组与现有组的预估相关性）

输出:
  action_type: "add_group"
  action_params: {
    suggested_factors: ["new_factor_1", "new_factor_2", ...],
    suggested_category: "sentiment",
    estimated_correlation_with_existing: 0.32,
    reason: "sentiment category (12 factors, avg IC=0.028) not covered by any group"
  }
```

**当前缺失原因**：`identify_bottlenecks` 不查询因子库，不知道"还有什么因子没用上"。

#### Rule 7: `rebalance` — 全局分组方案次优

```
触发条件:
  - 组间平均相关性 > 0.5（整体多样性不足，但没有单对 > 0.7）
  - 或: 某组因子数远超其他组（如一组 20 因子，另一组 5 因子），且大组 IC 不成比例地高
  - 或: 因子聚类分析显示当前分组不是最优（需要因子值缓存做聚类）

数据依赖:
  - qe_group_prediction_correlations（全部组对）
  - 各组 factor_names + factor_count
  - 因子值缓存（做层次聚类，判断最优分组方案）

输出:
  action_type: "rebalance"
  action_params: {
    current_avg_correlation: 0.55,
    suggested_grouping: [["f1","f2","f3"], ["f4","f5","f6"], ["f7","f8"]],
    estimated_avg_correlation: 0.31,
    reason: "current avg inter-group correlation 0.55; hierarchical clustering suggests 3-group split with avg corr 0.31"
  }
```

**当前缺失原因**：需要因子值缓存做聚类计算，`identify_bottlenecks` 目前是纯 DB 查询 + 简单阈值判断，没有计算密集型逻辑。

#### Rule 5b: `group_model_switch` 增强 — 模型适配性判断

```
触发条件（增强现有 zero_weight 规则）:
  - 某组 IC 低但因子质量不差（因子独立 IC 均值 > 0.02）
  - 说明不是因子问题，是模型不适配
  - 或: 同样因子在其他实验中用不同模型取得了更好 IC

数据依赖:
  - aistock_factor_catalog（因子独立 IC）
  - qe_experiments 历史（同因子不同模型的对比）
  - aistock_model_catalog（可用模型列表）

输出:
  action_type: "group_model_switch"
  action_params: {
    target_group: "fund_group",
    current_model: "lgbm",
    suggested_models: ["gru", "linear"],
    reason: "group factors avg IC=0.031 but group IC=0.008; model underperforming factors"
  }
```

### 6.6 规则引擎的分层改造

现有 `identify_bottlenecks` 签名只接收**当前实验快照**：

```python
def identify_bottlenecks(
    self,
    groups: list[GroupMetrics],
    correlations: dict[str, float],
) -> list[Bottleneck]:
```

无法做时序判断和因子库查询。改造方案是**两层规则引擎**：

```
Layer 1: 快照规则（现有 4 条，只看当前实验）
  → identify_bottlenecks(groups, correlations)
  → 不需要额外数据，保持现有接口
  → 前端诊断页继续使用，零改动

Layer 2: 演进规则（新增 4 条：Rule 5/5b/6/7，需要历史+因子库）
  → identify_evolution_opportunities(experiment_id)
  → 内部自行查询数仓（_build_multi_alpha_evolution_context）
  → 内部查询因子库 + 因子值缓存
  → 返回同样的 list[Bottleneck] 结构
```

MCP 工具 `qe_ma_diagnose` 同时调用两层，合并结果后按 severity 排序返回：

```
qe_ma_diagnose(experiment_id):
  layer1 = MultiAlphaDiagnostics.identify_bottlenecks(snapshot)      # 现有
  layer2 = MultiAlphaDiagnostics.identify_evolution_opportunities(   # 新增
              experiment_id)
  bottlenecks = sorted(layer1 + layer2, key=severity_rank)
  return {bottlenecks, recommendations: prioritize(bottlenecks)}
```

**好处**：
- 现有前端诊断页继续用 Layer 1，零侵入；
- MCP Agent 拿到 Layer 1 + Layer 2 的合并结果，决策维度更丰富；
- Layer 2 是计算密集型（需要因子聚类），可单独缓存或异步计算，不影响 Layer 1 的实时响应。

### 6.7 7 种 action_type 改造完成后的覆盖

| action_type | 由哪条规则产出 | 所在 Layer |
|-------------|---------------|----------|
| `remove_group` | `zero_weight`（增强：加时序判断） | Layer 1 + Layer 2 |
| `group_model_switch` | `zero_weight` + Rule 5b（新增） | Layer 1 + Layer 2 |
| `merge_groups` | `high_correlation` | Layer 1 |
| `meta_tune` | `icir_weight_mismatch`（增强：加 method/lookback 判断） | Layer 1 + Layer 2 |
| `group_factor_adjust` | Rule 5（新增） | Layer 2 |
| `add_group` | `weight_monopoly`（修正 action_type）+ Rule 6（新增） | Layer 1 + Layer 2 |
| `rebalance` | Rule 7（新增） | Layer 2 |

### 6.8 数仓查询：多 Alpha 演进历史构建

类比 `_build_full_evolution_history`，多 Alpha 需要一个 `_build_multi_alpha_evolution_context`：

```sql
-- 1. 当前实验的组级指标
SELECT group_name, group_ic, group_icir, meta_weight, model_id, factor_names
FROM qe_multi_alpha_groups
WHERE parent_experiment_id = :current_exp_id AND status = 'completed';

-- 2. 历史多 Alpha 实验的 combined IC 趋势（同一 task 下）
SELECT e.experiment_id, e.ic AS combined_ic, e.icir, e.annualized_return,
       e.created_at
FROM qe_experiments e
JOIN qe_evolution_loops l ON l.task_id = :task_id
WHERE e.alpha_mode = 'multi' AND e.status = 'completed'
ORDER BY e.created_at;

-- 3. 组间相关性趋势（跨实验对比）
SELECT experiment_id, group_a, group_b, correlation
FROM qe_group_prediction_correlations
WHERE experiment_id IN (:recent_experiment_ids);

-- 4. Meta 权重漂移
SELECT experiment_id, weights, combined_ic, as_of_date
FROM qe_meta_model_weights
WHERE experiment_id IN (:recent_experiment_ids)
ORDER BY as_of_date;

-- 5. 单 Alpha baseline（同 label_horizon、同 data_split 的最优单 Alpha）
SELECT experiment_id, ic, icir, annualized_return
FROM qe_experiments
WHERE alpha_mode = 'single' AND status = 'completed'
ORDER BY ic DESC LIMIT 5;

-- 6. 因子库全局视角（可用因子 + 分类 + IC）
SELECT factor_name, category, source, ic_mean, ic_std, grade
FROM aistock_factor_catalog
WHERE status = 'active'
ORDER BY abs(ic_mean) DESC;
```

### 6.9 决策流程设计

```
Step 1: 数仓查询
  _build_multi_alpha_evolution_context(task_id, current_exp_id)
  -> 组级指标 + 相关性 + 权重趋势 + 单Alpha baseline + 因子库

Step 2: 规则引擎（确定性，不依赖 LLM）
  MultiAlphaDiagnostics.identify_bottlenecks()
  -> bottlenecks + action recommendations

Step 3: 方向决策（MCP Agent 或 LLM Analyst）
  输入: evolution_context + bottlenecks + 历史 failed_approaches
  输出: 选定的 action_type + target_group + 具体参数

Step 4: 配置生成
  根据 action_type 修改 multi_alpha_config
  -> 新增/删除/修改组 -> 生成新 experiment_id

Step 5: 执行 + 收集 + 对比
  _run_multi_alpha_experiment -> collect_and_persist
  -> 与上一轮 + 单Alpha baseline 对比

Step 6: SOTA 判定
  combined_ic > best_historical_combined_ic?
  combined_ic > best_single_alpha_ic?
  -> 更新 SOTA 注册表
```

### 6.10 与单 Alpha 演进的关系

**多 Alpha 演进不是独立于单 Alpha 的**，它需要：

1. **读取单 Alpha 的 SOTA 作为 baseline**：多 Alpha 的价值在于"超越单 Alpha"，如果 combined IC 不如单 Alpha SOTA，说明分组/合并策略有问题。
2. **复用单 Alpha 的因子评估数据**：`aistock_factor_catalog` 中的 IC/ICIR/grade 是单 Alpha 演进过程中积累的，多 Alpha 选因子时直接复用。
3. **共享 SOTA 注册表**：`qe_sota_registry` 应该同时记录单 Alpha SOTA 和多 Alpha SOTA，供全局对比。

**但多 Alpha 演进不应修改单 Alpha 的配置**：两者是并行的实验路径，各自演进，最终由用户/Agent 选择哪个上生产。

---

## 7. MCP 接入计划（功能完成后）

### 7.1 MCP Server 需要实现的能力层次

```
Layer 1: 数据查询（只读，Agent 获取决策依据）
  - 查询历史实验指标
  - 查询组级详情
  - 查询单 Alpha baseline
  - 查询因子库可用因子
  - 查询相关性/权重趋势

Layer 2: 诊断分析（只读，Agent 获取结构化建议）
  - 瓶颈识别（规则引擎）
  - 演进方向推荐
  - 组配置相关性预检

Layer 3: 实验操作（写入，Agent 执行演进）
  - 创建多 Alpha 实验
  - 提交执行
  - 查询状态
  - 单步演进（基于上一轮结果）

Layer 4: 对比评估（只读，Agent 判断是否继续）
  - 横向对比（多 Alpha vs 单 Alpha）
  - 纵向对比（本轮 vs 历史最优）
  - 演进收敛判断
```

### 7.2 推荐暴露的 MCP 工具

| 工具名 | Layer | 输入 | 输出 | 用途 |
|--------|-------|------|------|------|
| `qe_ma_query_history` | 1 | `task_id` or `experiment_ids` | 演进历史 + IC 趋势 + 权重漂移 | Agent 了解当前状态 |
| `qe_ma_query_baseline` | 1 | `label_horizon`, `data_split` | 单 Alpha SOTA top-5 | Agent 设定超越目标 |
| `qe_ma_query_factor_pool` | 1 | `category?`, `min_ic?`, `exclude?` | 可用因子列表 + IC/grade | Agent 选因子 |
| `qe_ma_diagnose` | 2 | `experiment_id` | 瓶颈 + 建议 + 相关性矩阵 | Agent 决定下一步 |
| `qe_ma_preview_correlation` | 2 | `factors_per_group: list[list[str]]` | 预估组间相关性 | Agent 验证分组方案 |
| `qe_ma_create` | 3 | 分组配置 + meta_method + label_horizon | `experiment_id` | Agent 创建实验 |
| `qe_ma_submit` | 3 | `experiment_id`, `execution_mode` | `qe_task_id` | Agent 触发执行 |
| `qe_ma_status` | 3 | `experiment_id` | per-group status + 日志摘要 | Agent 轮询 |
| `qe_ma_evolve` | 3 | `parent_exp_id`, `action_type`, `params` | new `experiment_id` | Agent 单步演进 |
| `qe_ma_compare` | 4 | `experiment_ids` | 横向对比表 | Agent 判断效果 |
| `qe_ma_convergence` | 4 | `task_id` | 是否收敛 + 建议停止/继续 | Agent 决定是否终止 |

### 7.3 Agent 演进 workflow 示例

```
Agent (Claude/Codex) 调用 MCP 的典型 workflow:

1. qe_ma_query_baseline(label_horizon=10)
   -> 获取单 Alpha SOTA: IC=0.0607, ICIR=0.73

2. qe_ma_query_factor_pool(min_ic=0.02)
   -> 获取 200+ 可用因子，按 category 分组

3. qe_ma_preview_correlation([技术组因子, 基本面组因子, 情绪组因子])
   -> 预估组间相关性: tech|fund=0.35, tech|sent=0.28, fund|sent=0.42

4. qe_ma_create(factors_per_group=..., models=["lgbm","lgbm","lgbm"],
                meta_method="ic_weighted", label_horizon=10)
   -> experiment_id = "ma_exp_001"

5. qe_ma_submit("ma_exp_001", execution_mode="local_parallel")
   -> qe_task_id = "ma_exp_001_task"

6. [轮询] qe_ma_status("ma_exp_001") -> completed

7. qe_ma_diagnose("ma_exp_001")
   -> bottleneck: group_fund IC=0.008 (low), recommendation: switch_model

8. qe_ma_compare(["ma_exp_001", "single_alpha_sota_exp"])
   -> combined IC=0.065 > single 0.061

9. qe_ma_evolve("ma_exp_001", action_type="group_model_switch",
                params={"target_group": "fund", "new_model": "gru"})
   -> new experiment_id = "ma_exp_002"

10. [重复 5-8 直到收敛]
```

### 7.4 数仓分析在 MCP 中的实现位置

**答案：数仓分析逻辑应在 MCP Server 的后端服务层实现，不在 MCP 工具本身。**

```
MCP Tool (qe_ma_diagnose)
  -> 调用 MultiAlphaDiagnostics.analyze()
    -> 内部查询 6 张表 (6.8 的 SQL)
    -> 规则引擎 identify_bottlenecks()
    -> 返回结构化 JSON

MCP Tool (qe_ma_query_history)
  -> 调用 _build_multi_alpha_evolution_context()
    -> 内部查询 qe_evolution_loops + qe_multi_alpha_groups + qe_meta_model_weights
    -> 构建 ic_trend / weight_drift / correlation_trend
    -> 返回结构化 JSON
```

Agent 拿到结构化数据后自行做决策（选 action_type + 参数），不需要 MCP Server 内部跑 LLM。
这样 Claude/Codex 都可以用同一套 MCP 工具，决策逻辑在 Agent 侧。

### 7.5 接入前置条件

- 3 的全部 P0 完成（训-合-回跑通 + 负 IC 不崩）。
- 4 的 P1-A / P1-D 完成（否则 Agent 看到的指标不可比）。
- `qe_experiments` 主表保证：单 Alpha 与多 Alpha 行可用同一 SQL 排序对比。
- `_build_multi_alpha_evolution_context` 函数实现并测试。
- `MultiAlphaDiagnostics` 的规则引擎（Layer 1 + Layer 2）覆盖 6.2 的全部 7 种 action_type。

### 7.6 MCP 工具应避免的反模式

- **不要**让 Agent 直接执行回测命令；走 `qe_ma_submit` 端点。
- **不要**让 Agent 直接读 mlruns；走诊断端点拿结构化指标。
- **不要**暴露任何"删全部组"或"清缓存"破坏性操作。
- **不要**让 Agent 自行决定 `label_horizon`；锁死实验级一个值。
- **不要**在 MCP Server 内部跑 LLM 做决策；决策权在调用方 Agent。
- **不要**让 Agent 跳过 `preview_correlation` 直接创建实验；强制事前校验。

---

```
[P0-A] 负IC组权重一致化 + meta三处实现合一
   | blocks
[P0-B] 主节点 unified backtest 在分布式路径中显式触发
   | blocks
[P0-C] 多节点日志聚合到 UI（与 B 并行可做）
   | blocks
[P0-D] 因子缓存自动同步（与 B 并行可做，issue 化）
   | blocks
[P1-A] label_horizon 全实验统一硬约束
[P1-B] disable_alpha158 默认值对齐单 Alpha
[P1-C] 组配置阶段相关性事前闸门
[P1-D] meta_model_runner 内联 stacking（去 standalone）
   | unlocks
[MVP] 多 Alpha 上线，与单 Alpha 横向对比
   | unlocks
[数仓] _build_multi_alpha_evolution_context + 规则引擎 Layer 2 (Rule 5/5b/6/7)
   | unlocks
[MCP] 11 个工具接入（4 层），Agent 可自动演进
   | later
[研究 R1-R6] 动态权重 / Regime-aware / 智能分组 / 风险平价 / 异构模型 / 增量训练
```

---

## 9. 不做的事（明确范围）

- **不**改 RD-Agent 因子研发循环；多 Alpha 是消费侧。
- **不**引入 LLM 驱动的自动演进；Agent 演进由用户/Codex 触发，每步都经过 MCP 工具。
- **不**在 MCP Server 内部跑 LLM；决策权在调用方 Agent。
- **不**重新实现因子缓存；只补"自动同步"。
- **不**在本轮做 R1-R6 任何研究项；只为它们留位置。
- **不**让多 Alpha 演进修改单 Alpha 配置；两者并行实验路径。

---

## 10. 验收标准（MVP）

1. 至少 1 个 2-3 组的多 Alpha 实验跑通：组训练（含远端）-> pred 汇聚 -> 主节点合并 -> 主节点 `--pred-backtest` -> `qe_experiments` 写入 combined IC/ICIR/年化/回撤；
2. 故意构造一组 IC<0 的实验不再崩溃，权重为 0 并完成回测；
3. UI 可见多节点训练日志；
4. 与单 Alpha 同 `label_horizon=10`、同 `disable_alpha158` 的对照实验，多 Alpha combined IC >= 单 Alpha IC + 0.005（或 ICIR + 0.05），否则视为伪多样化，触发 P1-C 相关性阈值检查。

## 11. 验收标准（MCP 接入）

1. Agent 可通过 `qe_ma_query_baseline` 获取单 Alpha SOTA 作为超越目标；
2. Agent 可通过 `qe_ma_query_history` 获取完整演进历史（含组级 IC 变化 + 权重漂移 + 相关性趋势）；
3. Agent 可通过 `qe_ma_diagnose` 获取规则引擎的瓶颈识别和 action 建议；
4. Agent 可通过 `qe_ma_evolve` 执行单步演进，新实验自动继承上一轮的 reuse_prediction；
5. Agent 可通过 `qe_ma_convergence` 判断是否停止演进（连续 N 轮无 SOTA 突破）；
6. 完整 workflow（baseline -> 分组 -> 预检 -> 创建 -> 执行 -> 诊断 -> 演进）可由 Agent 无人工干预完成。

---

文档结束。
