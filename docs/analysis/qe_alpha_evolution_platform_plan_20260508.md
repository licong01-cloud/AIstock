# QE Alpha 自动演进平台分阶段方案

日期：2026-05-08
状态：Draft / 作为后续实现分阶段目标
关联修复：V25.1 QE wrapper 已兼容 UI/DB 通用参数 `min_cost` / `max_buckets` / `commission_rate` / `tolerance_bps`

## 0. 文档发现与当前可用能力

### 0.1 本次阅读的本地来源

- `docs/codex_project_memory.md`：QE/交易相关修改必须隔离生产 main，禁止静默修改 protected assets，验证必须有 run record。
- `docs/architecture/minute_execution_algo_standard_contract.md`：分钟线执行策略必须 fail-fast、无 silent fallback、QE 与 Paper v2 语义一致。
- `tests/aistock_validation/modules/qe.md`：QE 验证红线包括不得直接读取 worker workspace、不得伪造可用结果、开发验证不得影响生产 8001。
- `backend/services/quantevolver/config_composer.py`：当前 QE 通过 `compose_experiment_in_memory()` / `_compose_conf_yaml()` 生成 loop payload 和 `conf.yaml`；V25/V25.1 执行参数进入 `NestedExecutor.inner_strategy.kwargs`。
- `backend/services/quantevolver/executors/backtest.py`：`BacktestExecutor.submit()` 是统一提交入口，负责把 `ExperimentConfig` 变成 RDAgent loop payload 并调用 `create_and_run_loop()`。
- `backend/services/quantevolver/experiment_config.py`：`ExperimentConfig` 已能承载 factor/model/strategy、HMM、执行算法、backtest-only、Multi-Alpha 等运行配置。
- `backend/services/quantevolver/qe_evolution_service.py`：`AutoEvolutionScheduler` 已实现 loop 状态机、历史上下文、SOTA 回滚、训练诊断、Agent 分析与下一轮提交。
- `backend/routers/quantevolver_evolution.py`：UI/API 层已暴露 evolution task 创建、execution_algo、execution_algo_params、label_horizon、HMM、Multi-Alpha 等字段。
- `backend/services/quantevolver/multi_alpha_engine.py`：已有 Multi-Alpha 分组训练、统一回测和 meta-model 合成雏形。
- `backend/services/quantevolver/completion_contract.py`：已有 QE 完成契约、artifact manifest、reproducibility_level 等字段概念。
- `backend/services/quantevolver/model_analyst.py` 与 `qe_evolution_agents.py`：已有 `best_epoch`、`convergence_ratio`、`overfit_ratio`、`training_failed` 等训练质量诊断，但还不是完整的可复现实验治理。

### 0.2 外部方法参考

- Qlib Workflow / Recorder 文档：Qlib 的工作流强调 Recorder/Experiment 记录参数、指标和产物，适合作为 QE artifact lineage 的底层思想来源。
  https://qlib.readthedocs.io/en/latest/component/workflow.html
- Bailey, Borwein, López de Prado, Zhu：The Probability of Backtest Overfitting，提示多维策略搜索会显著提高样本内过拟合概率。
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253
- Bailey and López de Prado：The Deflated Sharpe Ratio，提示多重试验和非正态收益会夸大传统 Sharpe 可信度。
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551

### 0.3 当前允许复用的本地 API / 模块

- 生成实验：`ConfigComposer.compose_experiment_in_memory(...)`
- 统一提交：`BacktestExecutor.submit(config, ctx, mode=FULL_TRAIN | BACKTEST_ONLY)`
- 配置对象：`ExperimentConfig`
- 演进状态机：`AutoEvolutionScheduler.submit_next_loop()` / `process_completed_loop()`
- 多 Alpha：`MultiAlphaEngine`
- 完成契约：`QECompletionPayload` / `ArtifactManifestItem`
- 配置真值测试：`backend/tests/unified_engine/test_qe_config_truth.py`

### 0.4 必须避免的反模式

- 不得把同一实验中的预测差异、执行差异、seed 差异混在一起解释收益差异。
- 不得用一次训练、一次回测直接宣布 SOTA。
- 不得把 seed 当作最终实盘 alpha 的主要优化目标；seed 搜索如果发生，必须视为超参数搜索并用独立 OOS / walk-forward 重验。
- 不得用 future data、最新全样本排序、回测结果反向筛选因子。
- 不得让 UI 显示的参数被父类 `**kwargs` 吞掉或由 wrapper 默认值替代。
- 不得从 Windows 直接读写 RDAgent/WSL worker workspace；artifact 必须通过 API 或 AIstock-owned cache 流转。

## 1. 对当前问题的结论

### 1.1 V25.1 参数别名 bug

这是需要修复的真实 bug。UI/DB/catalog 使用的是通用参数名：

```text
min_cost / max_buckets / commission_rate / tolerance_bps
```

而原 V25.1 wrapper 只读取：

```text
v25_1_min_cost / v25_1_max_buckets / v25_1_commission_rate / v25_1_tolerance_bps
```

由于父类接受普通 `**kwargs`，通用参数可能被静默吞掉，导致实际执行使用默认值。这会影响 V25.1 的真实执行行为，尤其是 `max_buckets` 与 `commission_rate`，必须 fail-fast 或正确消费别名。本次修复选择兼容 UI/DB 通用参数，并在通用名与 prefixed 名同时出现且数值冲突时抛 `ValueError`。

### 1.2 `seed: None` 的含义

`seed: None` 不代表“模型训练不充分”，而代表本次训练没有被强制绑定到一个确定随机种子，也没有把随机状态作为可复现契约记录下来。它的直接风险是：同一配置再次训练，可能得到不同权重、不同 `pred.pkl`、不同回测收益。

模型是否训练不充分，应看：

- `best_epoch`
- `total_epochs`
- train/valid loss 曲线
- OOS IC / RankIC / ICIR
- 因子重要性是否稳定
- 多 seed 预测相关性与 topK overlap

### 1.3 `best_epoch=0` 的含义

`best_epoch=0` 是严重训练诊断信号，但不能单独等价于“模型必须废弃”。它通常意味着验证集在训练早期没有出现有效改善，可能原因包括：

- 特征/标签关系弱或标签 horizon 不匹配；
- 学习率、batch size、正则、early stop 设置不合适；
- 模型结构与因子类型不匹配；
- 数据切分或数据质量问题；
- 随机初始化使本次训练落到差路径。

正确处理方式是：同配置固定 seed 复现一次，再用 3-5 个 seed 做小规模鲁棒性检查。如果大多数 seed 都 `best_epoch=0` 或 OOS 退化，才把问题升级为模型/特征/标签方案问题。

## 2. 正常量化机构的模型选择与训练治理

成熟量化团队通常不会把“因子组合、模型类型、超参数、seed、执行算法”放进一个无约束大网格里暴力搜索，而是分层治理：

```text
数据与标签冻结
  -> 因子单体/相关性/容量预筛
  -> 模型族少量候选
  -> 超参数受限搜索
  -> 多 seed 稳定性检验
  -> walk-forward / rolling OOS
  -> 执行算法固定信号 A/B
  -> Paper / 小资金 champion-challenger
```

核心原则：

- “一次最好结果”不重要，“在多个样本切片和多个 seed 下仍然不差”才重要。
- seed 是鲁棒性维度，不是 alpha 的主要来源。
- 训练产物必须可追溯：代码 commit、数据快照、特征 schema、label horizon、seed、模型权重 hash、预测文件 hash。
- 执行算法比较必须使用同一个预测信号；否则比较的是“模型训练 + 执行”的混合差异。
- 实盘滚动训练前，需要先证明 rolling retrain 下信号分布、topK overlap、换手、行业暴露、容量和成本都稳定。

## 3. 多维探索的真实可行方案

### 3.1 不可行做法

如果把所有维度同时打开，组合数量会失控：

```text
因子组合 N 种 * 模型 M 种 * 超参数 H 种 * seed S 种 * 执行算法 E 种 * 窗口 W 种
```

这种搜索会导致两个问题：

- 算力和时间不可控；
- 多重试验让样本内最优结果很可能只是随机幸运。

### 3.2 可行做法：阶段漏斗

```text
Stage 0  配置真值与复现契约
Stage 1  固定预测信号，只比较执行算法
Stage 2  固定因子/模型/超参，做多 seed 稳定性
Stage 3  固定 seed policy 后探索超参数
Stage 4  固定模型族后探索因子组合
Stage 5  少量模型族 challenger 对比
Stage 6  walk-forward / rolling retrain
Stage 7  Paper v2 / StrategyPackage promotion
```

每一阶段只打开少数维度，上一阶段失败则不进入下一阶段。所有候选不仅看均值，也看方差、最差分位、回撤、换手、容量和 topK overlap。

### 3.3 seed 的处理原则

- 固定 seed 用于复现：每个正式 candidate 必须有 primary seed。
- 多 seed 用于鲁棒性：建议至少 3 个 seed，重要候选 5-10 个 seed。
- 不建议在最终测试集上找“收益最高 seed”。如果确实把 seed 当搜索变量，必须进入 nested validation，并在完全未参与选择的 OOS 上重验。
- 实盘 rolling retrain 可以固定 seed policy，但每次滚动训练仍要记录权重 hash 和预测 hash，并检查信号分布漂移。

## 4. 当前 QE 自动演进能否成为平台

结论：可以作为未来 alpha 演进平台的底座，但不能直接作为“稳健 alpha 搜索平台”使用。当前 QE 已具备任务编排和回测闭环，但缺少复现、固定信号 A/B、多 seed、walk-forward 和 promotion governance。

### 4.1 当前优势

```text
能力                         当前基础
---------------------------  --------------------------------------------
任务/Loop 状态机              AutoEvolutionScheduler 已有 pending/running/completed 流转
配置生成                      ConfigComposer 已统一生成 conf.yaml 和 helper payload
统一提交                      BacktestExecutor 已封装 compose + create_and_run_loop
执行算法参数                  execution_algo / execution_algo_params 已贯穿 API 与配置
训练诊断                      best_epoch / convergence_ratio / overfit_ratio 已入库/分析
SOTA 记录                     qe_sota_registry 与非 SOTA 回滚已存在
Agent 分析                    Analyst / Evaluator / ModelAgent / FactorAgent 已有雏形
Multi-Alpha                   MultiAlphaEngine 已能分组训练与统一回测
Artifact 契约                 completion_contract 已定义 artifact manifest 和 reproducibility_level
```

### 4.2 当前缺口

```text
缺口                         影响
---------------------------  --------------------------------------------
没有强制 seed 契约            同配置训练不可复现，无法判断差异来自 alpha 还是随机性
没有模型权重/pred hash 注册   无法固定信号做执行 A/B，也无法可靠复盘
没有 fixed-pred 执行模式      V25 vs V25.1 容易混入训练随机差异
没有多 seed orchestration     无法判断模型稳定性和 topK 稳定性
没有 walk-forward task 类型   难以证明 rolling retrain 后仍然有效
没有多重试验惩罚指标          SOTA 容易被单次幸运回测污染
没有 promotion gate           QE 结果不能自动、安全进入 Paper v2 / StrategyPackage
UI 缺少复现实验视图           用户看不到 seed、hash、数据快照、信号相似度、稳定性分布
```

### 4.3 需要改造的核心方向

- 新增 `experiment_mode`：`EXECUTION_AB` / `MODEL_ROBUSTNESS` / `FACTOR_SEARCH` / `HPARAM_SEARCH` / `WALK_FORWARD` / `PROMOTION_CANDIDATE`。
- 新增 reproducibility contract：`seed_policy`、`seed_list`、`code_commit`、`data_snapshot_id`、`feature_schema_hash`、`label_config_hash`、`model_artifact_hash`、`prediction_hash`。
- 新增 fixed-signal backtest：同一 `pred.pkl` 或同一 checkpoint 输出，分别跑 V25、V25.1、TWAP 等执行算法。
- 新增 multi-seed runner：同配置多 seed 训练，输出 mean/std/worst/quantile、topK overlap、prediction correlation。
- 新增 walk-forward runner：按月/季度滚动训练，记录每个窗口的 OOS 表现和信号漂移。
- 新增 promotion gate：只有通过稳定性、成本、容量、数据质量、执行一致性检查的 candidate 才能生成 StrategyPackage / Paper v2 候选。

## 5. 分阶段实现目标

### Phase 0：配置真值与执行 bug 修复

目标：保证 UI/DB 选择的执行参数真实进入执行 wrapper。

实施内容：

- V25.1 wrapper 接受通用参数别名。
- 通用名与 prefixed 名冲突时 fail-fast。
- QE config truth 测试覆盖 V25.1 参数写入。
- wrapper 级单测覆盖别名消费和冲突报错。

验收：

- `pytest backend/tests/trading_core backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_v25_1_qe_wrapper_config_alias.py -q -p no:cacheprovider`
- dry creation smoke 生成 V25.1 in-memory experiment files，并确认 `conf.yaml` 和 `tail_twap_v25_1_strategy.py` 同时存在。

### Phase 1：Reproducibility Contract

目标：任何 QE loop 都能回答“这次结果能否复现”。

实施内容：

- 在 `ExperimentConfig` 增加 `seed_policy`、`seed`、`seed_list`、`deterministic_mode`。
- 在 ConfigComposer 对不同模型族映射 seed 参数：如 LightGBM/XGBoost/CatBoost 的随机参数、PyTorch/NumPy/Python seed。
- 在 completion payload 中强制记录：code commit、data snapshot、feature schema hash、label horizon、模型权重 hash、pred hash。
- UI 显示“复现等级”：full / partial / audit_only / unreproducible。

验收：

- 同一 config + same seed 两次训练的 pred hash 或预测相关性达到预期门槛。
- seed 未设置时 UI 明确显示“不可完全复现”，不允许进入 promotion。

### Phase 2：Fixed-Signal 执行 A/B

目标：执行算法比较只比较执行，不混入训练随机性。

实施内容：

- 新增 `EXECUTION_AB` 模式。
- 输入固定 `pred.pkl` 或固定 checkpoint + deterministic inference。
- 生成多条 execution child runs：TWAP / V25 / V25.1 / CLOSE_PRICE。
- 统一输出持仓数量、换手、成本、成交失败原因、最大持仓数、平均持仓数、NAV、回撤。

验收：

- 同一个 prediction_hash 下，多个执行算法的收益差异可解释。
- 禁止 `EXECUTION_AB` 自动重新训练。

### Phase 3：Multi-Seed 稳定性

目标：把 seed 从“不确定风险”变成“可量化稳定性指标”。

实施内容：

- 新增 `MODEL_ROBUSTNESS` 模式。
- 同一因子/模型/超参跑多个 seed。
- 产出 stability report：IC mean/std、return mean/std、worst seed、topK overlap、prediction correlation、行业暴露稳定性。
- SOTA 判定从单点指标改为鲁棒指标。

验收：

- UI 展示 seed 分布而不是单次收益。
- promotion gate 要求 worst 或 p25 表现不低于阈值。

### Phase 4：受控超参 / 模型 / 因子探索

目标：让多维探索可控，而不是组合爆炸。

实施内容：

- 因子先过单体 IC、覆盖率、相关性、行业/市值暴露检查。
- 模型族候选限制在少量解释明确的 challenger。
- 超参搜索用预算约束和早停；每次只在固定维度内搜索。
- 引入多重试验惩罚或至少记录 trial_count / family_count / selection_scope。

验收：

- 每个 SOTA 都能说明来自哪个搜索空间、比较了多少候选、是否做了多 seed 和 OOS。
- 被淘汰 candidate 也入库，防止重复踩坑。

### Phase 5：Walk-Forward / Rolling Retrain

目标：验证未来实盘滚动训练的稳定性。

实施内容：

- 新增 rolling window 配置：train_window、valid_window、test_window、step。
- 每个窗口固定 seed policy，训练后记录预测分布漂移、topK overlap、换手、成本和行业暴露。
- 输出 rolling champion report。

验收：

- 不是只看全周期合并收益，而是每个窗口都有 OOS 结果。
- 单窗口极端好结果不能掩盖多数窗口退化。

### Phase 6：Champion / Challenger 与 Paper v2 接轨

目标：把研究结果变成可观察、可回滚的准生产候选。

实施内容：

- QE candidate 通过 promotion gate 后生成 StrategyPackage candidate。
- Paper v2 只消费已验证 package 和 validated execution policy。
- 记录 champion/challenger 的上线日期、冻结模型、滚动再训练计划和撤退条件。

验收：

- Paper v2 不直接使用 QE backtest pred 作为实盘权威信号。
- 每个 challenger 都有停止条件：收益退化、换手过高、持仓漂移、数据异常、训练失败。

### Phase 7：治理 UI 与自动化看板

目标：让用户能在 UI 上看懂“为什么这个 alpha 值得继续”。

实施内容：

- 实验详情展示 seed、artifact hash、data snapshot、prediction_hash、model_hash。
- 增加 fixed-signal A/B 页面。
- 增加 multi-seed 分布图和 topK overlap。
- 增加 rolling window 矩阵。
- SOTA 列表区分 research SOTA、robust SOTA、paper champion。

验收：

- 用户能区分：模型预测变好、执行变好、随机性导致的偶然变好。
- UI 不显示 raw JSON 给普通操作者，而是显示业务解释和 fail-fast 原因。

## 6. 推荐的近期落地顺序

```text
1. 完成本次 V25.1 参数别名修复，并合入 main。
2. 增加 QE reproducibility schema 草案，不急于做 UI。
3. 实现 fixed-pred execution A/B，先解决 V25 vs V25.1 的公平比较。
4. 实现 3-seed robustness 最小闭环。
5. 把 SOTA 判定从 single-run 改成 robust-score。
6. 再启动 walk-forward / rolling retrain。
7. 最后接 StrategyPackage / Paper v2 promotion。
```

## 7. 对现有 QE 自动演进的最终判断

当前 QE 自动演进已经适合做“研究执行与编排平台”，因为它具备 loop、配置生成、RDAgent 提交、指标回收、Agent 分析和 SOTA 状态。但它还不适合直接承担“未来最佳 alpha 自动发现平台”的全部责任。

成为目标平台需要补齐三层能力：

- 复现层：seed、hash、数据快照、artifact manifest。
- 鲁棒层：fixed-signal A/B、多 seed、walk-forward、多重试验惩罚。
- 晋级层：promotion gate、Paper v2 champion/challenger、滚动再训练治理。

只要按上述阶段改造，现有 QE 是最合适的底座；不需要另起一个完全独立平台。
