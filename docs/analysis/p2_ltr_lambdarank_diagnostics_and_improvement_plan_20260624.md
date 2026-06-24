# P2-LTR LambdaRank 诊断与改进设计（plan）

日期：2026-06-24
分支：`plan/p2-ltr-lambdarank-diagnostics-20260624`
状态：设计先行，仅诊断与方案；本轮不改 `aistock_models` / `config_composer` 实现代码。

## 0. 结论摘要

1. **FLOW_ACCEL × LambdaRank 的失败不是实验创建前失败，也不是 LambdaRank fit/import 失败。** 远端 `rdagent-node1` 日志显示 LightGBM LambdaRank 已训练、`SignalRecord` 已生成 `pred.pkl`，失败点在 V25_1 minute 回测初始化 `EarlyPlanNetEnhanced` 时加载 checkpoint 形状不匹配。已登记 BUG-507 / GitHub #1569。
2. **a1 PLUS3 高维腿的 RankIC 负值不是 label digitize 方向普遍错误。** `LambdaRankModel` 对连续收益做单调分桶（收益越高 relevance 越高），同一实现下 FUND_GROWTH 仍为弱正；同特征 MSE 基线显著正。当前证据更支持“全排序 NDCG 目标 + 高维/Alpha158 复杂度 + top-K 投资目标不一致”导致 OOS 排序反向。
3. **低维纯模态也不能直接证明现版 LTR 有效。** FUND_GROWTH 7 因子仅弱正且劣于 MSE；FLOW_ACCEL 7 因子在 LambdaRank 的 SignalRecord 阶段 RankIC 已为负，而 MSE 同因子稳定强正。因此设计不应只“调参”，应先把训练目标改为 top-K 对齐，并加高维护栏与诊断。
4. **推荐改进路线：** 先用 LightGBM 原生能力做 `native_ndcg_at_k` / top-K relevance 变换（低风险），再评估是否需要自定义 top-K weighted lambda；同时对 Alpha158/高维腿默认启用保守复杂度或显式禁用 LTR，除非真实 QE 证明收益。

## 1. 证据来源与已读 API

### 1.1 代码与文档证据

| 来源 | 发现 |
| --- | --- |
| `aistock_models/aistock_models/lambdarank.py:27` | `LambdaRankModel` 是现有生产级 Qlib `Model` 实现，底层为 LightGBM `LGBMRanker`。 |
| `aistock_models/aistock_models/lambdarank.py:48` | 默认 `objective="lambdarank"`，`metric="ndcg"`，`ndcg_eval_at=[10,30,50]`。 |
| `aistock_models/aistock_models/lambdarank.py:117` | 连续收益通过 percentile/digitize 转为整数 relevance。`np.digitize` 对收益单调递增：收益高 -> bin 大 -> relevance 高。 |
| `aistock_models/aistock_models/lambdarank.py:135` | query group 按 `(datetime, instrument)` 的 date level 构建，符合横截面排序语义。 |
| `aistock_models/aistock_models/lambdarank.py:147` | 所有额外 `extra_kwargs` 会并入 `LGBMRanker` 参数。 |
| `backend/services/quantevolver/config_composer.py:2738` | `LAMBDARANK` / `LAMBDAMART` 分支已接 `LambdaRankModel`，默认仍为全排序 NDCG。 |
| `backend/services/quantevolver/config_composer.py:2760` | `model_hyperparameters` 会 `model_kwargs.update(hp)`，因此可通过 QE loop 配置覆盖新超参。 |
| `scripts/qrun_limit_minute.py:697`、`backend/services/quantevolver/experiment_config.py:161` | seed 会注入 `random_state`，同配置复现实验具备基础。 |
| `docs/analysis/qe_20260430_d55f_deep_analysis_20260501_deepseek_v4.md:242` | 早期 LambdaMART 曾出现 IC/RankIC 负并被记录为“排序学习反向”，不是本次孤例。 |
| `tests/aistock_validation/bugs/20260602_BUG-202-*.json` | 远端曾修复 `aistock_models` / LambdaRank import smoke；本次 FLOW 已进入训练与预测，故不是旧 import 问题复发。 |

### 1.2 MCP / 远端只读证据

- `qe_custom_evo_get_task(qe_20260624_011723_6df8, summary)`：首验任务 3 loops，a1/FUND completed，FLOW failed。
- `qe_custom_evo_get_loop_config(qe_20260624_011723_6df8, loop_index=3)`：FLOW loop 为 `model_id="__seed_LambdaMART_20D__"`、`disable_alpha158=true`、7 个 FLOW_ACCEL 因子、`execution_algo="V25_1_SMALL_CAP"`、`early_model_path` 与 `late_model_path` 均为 `/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt`。
- 远端只读日志：`rdagent-node1:/home/lc999/projects/RD-Agent-main/qe_workspace/qe_20260624_011723_6df8/Loop3/run.log`。

## 2. Step 1 实证对照

### 2.1 LambdaRank 首验 vs MSE 基线

| 腿 | 维度/模态 | LTR 结果 | MSE 对照 | 结论 |
| --- | --- | --- | --- | --- |
| a1 PLUS3 + Alpha158 | 23 自定义因子 + Alpha158，约 181 维 | `RankIC=-0.064405`，`CAGR=0.289503` | d56e Loop4/5：`RankIC=0.127141/0.126590`，`CAGR=0.632564/0.709921` | 同特征 MSE 强正，LTR OOS 反向。 |
| FUND_GROWTH | 7 因子纯模态，`disable_alpha158=true` | `RankIC=0.010537`，`CAGR=0.459444` | 0e41 Loop1：`RankIC=0.058143`，`CAGR=0.585155` | LTR 仅弱正，仍劣于 MSE。 |
| FLOW_ACCEL | 7 因子纯模态，`disable_alpha158=true` | 回测失败；训练后 SignalRecord `Rank IC=-0.060920` | 0e41 Loop7：`RankIC=0.109657`，`CAGR=0.599926`，同任务 Loop7-12 RankIC 约 `0.1086~0.1097` | FLOW 数据本身可学；LambdaRank 预测方向已弱/负，另有执行 checkpoint bug 阻断指标入仓。 |

### 2.2 训练日志中的有效/失效信号

| Loop | 训练日志片段 | OOS SignalRecord | 解读 |
| --- | --- | --- | --- |
| a1 Loop1 | valid `ndcg@10` 从 `[20] 0.137` 提升到 best iteration `[60] 0.145688` | `IC=-0.009586`，`Rank IC=-0.064405` | valid NDCG 看似可优化，但 OOS RankIC 反向，说明当前训练目标/验证指标不能代表投资 OOS top-K 质量。 |
| FUND Loop2 | best iteration `[5]`，valid `ndcg@10=0.135767` | `Rank IC=0.010537` | 低维下未反向，但弱于 MSE，说明不是单纯高维才有问题。 |
| FLOW Loop3 | best iteration `[3]`，valid `ndcg@10=0.111759` | `IC=-0.023391`，`Rank IC=-0.060920` | 低维纯 FLOW 下 LTR 也可产生负 OOS 排序；后续 checkpoint bug 是回测失败点，不是预测失败点。 |

## 3. A. FLOW 确定性接通失败根因

### 3.1 远端日志事实

远端 `Loop3/run.log` 关键阶段：

1. 数据与因子准备成功：7 个 FLOW_ACCEL 因子写入 workspace，`disable_alpha158=True`，`num_features=7`。
2. LambdaRank 训练成功：
   - `Training until validation scores don't improve for 20 rounds`
   - `[20] valid_0's ndcg@10: 0.104607 ...`
   - `Early stopping, best iteration is: [3] ...`
3. `SignalRecord` 成功生成：
   - `Signal record 'pred.pkl'`
   - `The following are prediction results of the LambdaRankModel model.`
4. Signal diagnostics 已可见且为负：
   - `IC=-0.0233906`
   - `Rank IC=-0.0609201`
5. 真正失败点在 portfolio backtest executor 初始化：
   - `RuntimeError: Error(s) in loading state_dict for EarlyPlanNetEnhanced`
   - `Unexpected key(s) in state_dict: "gap_embedding.weight"`
   - `size mismatch for mlp.0.weight: checkpoint shape torch.Size([128, 21]) ...`
   - `size mismatch for mlp.6.weight/bias ...`

### 3.2 根因判断

`experiment_id=null` 是主系统摘要层的误导性表现；远端已创建 Qlib experiment/recorder 并生成预测。根因是 **V25_1 execution model checkpoint 与当前 execution strategy 实例形状不兼容**，且该底层错误没有被 QE loop summary loud 暴露。

这与 LambdaRank 接通本身是两个问题：

- LambdaRank import / fit / pred：已通过。
- FLOW 回测指标产出：被 V25_1 checkpoint shape mismatch 阻断。
- 错误暴露：QE summary 未给出 `reason_code`、model path、shape mismatch 等定位信息。

### 3.3 BUG 登记与修复方案

- BUG：`BUG-507`
- GitHub：`https://github.com/licong01-cloud/AIstock/issues/1569`
- BUG JSON：`tests/aistock_validation/bugs/20260624_BUG-507-qe-lambdarank-flow-pure-factor-v25-1-backtest-hides-execution-checkpoint.json`

建议修复（单独 BUG，不混入 LTR objective 实现）：

1. 在 QE minute runtime / execution algo 预检阶段校验 `execution_algo_params.early_model_path/late_model_path` 与当前 strategy 网络结构兼容；不兼容时 fail-fast。
2. reason_code 建议：`execution_model_checkpoint_shape_mismatch`。
3. 错误上下文必须包含：`task_id`、`loop_index`、`node_id`、workspace、checkpoint path、当前模型关键 shape、checkpoint shape、原始 stderr tail。
4. 不应把此类失败压缩为 `experiment_id=null` / empty metrics。
5. 若实现阶段严格限制只改 `aistock_models/lambdarank.py` 与 `config_composer.py`，则 FLOW 验收必须先等 BUG-507 修复，或临时选择已知兼容的 execution profile/checkpoint；不能在 LTR 改进 PR 内绕过回测错误。

## 4. B. a1 高维 RankIC 负诊断

### 4.1 排除项

1. **不是 label digitize 普遍反向。** 当前 `np.digitize(y_raw, bin_edges[1:-1])` 对收益是单调递增；收益越高 relevance 越高。若方向普遍错，FUND 不应为弱正。
2. **不是 a1 数据/因子本身不可学。** 同窗口、同 a1 因子族、MSE LGBM_C 的 RankIC 约 `+0.127` 且 CAGR `0.63~0.71`。
3. **不是 LambdaRank import/fit 基础接通失败。** a1/FUND 已完成，FLOW 也已训练和生成 pred。

### 4.2 支持“目标错配 + 高维泛化反向”的证据

1. **valid NDCG 与 OOS RankIC 脱钩。** a1 在 valid `ndcg@10` best iteration 达 `0.145688`，但 OOS `RankIC=-0.064405`。这说明优化全排序 NDCG@[10,30,50] 并未转化为 top-K 投资窗口的正排序。
2. **高维 a1 反向最严重。** a1（约 181 维）LTR RankIC 为 `-0.064`，FUND（7 维）为 `+0.011`。高维/Alpha158 可能放大了全排序目标与噪声特征的过拟合。
3. **低维 FLOW 也出现 LTR 预测负 RankIC。** 这说明问题不只在维度，还在当前 LambdaRank objective / relevance 构造与投资 top-K 目标不一致。
4. **历史 LambdaMART 已有负 IC 记录。** 2026-05-01 深度分析中 LambdaMART Loop7 的 IC/RankIC 为负，被标注为排序学习反向，需要优先检查 ranking group/objective/label。

### 4.3 诚实边界

当前 artifacts 没有持久化 train-window NDCG / train RankIC / valid RankIC / OOS 分段 RankIC 的完整对照，因此“高维过拟合”不能仅凭现有结果被完全证明。更准确结论是：

> 已有证据强烈支持“当前全排序 LambdaRank 目标在 OOS top-K 投资任务上失配，并在高维 a1 上放大为泛化反向”；实现阶段必须补训练/验证/OOS ranking diagnostics，才能把过拟合链路量化闭环。

## 5. C. 改进设计

### 5.1 目标与非目标

目标：

1. 让 LambdaRank 训练目标对齐 `topk25/nd2/h20` 的 top-K 选股任务。
2. 对 Alpha158 / 高维特征腿提供默认安全策略，避免“valid NDCG 可优化但 OOS RankIC 反向”。
3. 通过 `model_hyperparameters` 暴露新超参，支持 QE loop 配置覆盖。
4. 增加 no-silent diagnostics，真实 QE 验收若仍不如 MSE，明确证伪 LTR 路线。

非目标：

1. 不改 QE task/scheduler、qrun、生产 runtime 模板。
2. 不改 V25_1 execution checkpoint 兼容性；该问题由 BUG-507 单独处理。
3. 不为“兼容旧行为”做静默 fallback。
4. 不在本设计 PR 中启动服务、重启后端、跑生产 QE 或写 DB。

### 5.2 新增 LambdaRank 训练模式

建议在 `LambdaRankModel` 引入显式模式参数：

| 参数 | 默认建议 | 说明 |
| --- | --- | --- |
| `ltr_objective_mode` | `"native_ndcg_at_k"` | 新模式显式 opt-in；旧行为可保留为 `"legacy_full_ndcg"` 便于 A/B。 |
| `topk_train_k` | `25` | 与 QE topk25 对齐。 |
| `ndcg_eval_at` | `[25]` | 新模式下默认只评估 NDCG@K；不再默认 `[10,30,50]`。 |
| `relevance_transform` | `"topk_quantile"` | 将 forward return 转为更关注顶部收益的 relevance。 |
| `top_relevance_quantile` | `0.90` 或按每日 top-K 映射 | 顶部股票 relevance 拉开，中段压缩。 |
| `middle_relevance_policy` | `"compress"` | 中部/尾部不让全序误差主导 gradient。 |
| `label_gain_mode` | `"top_heavy"` | 使用 LightGBM `label_gain` 拉大高 relevance 差异。 |

分阶段实现：

1. **Phase LTR-1（低风险）：原生 LightGBM top-K 对齐。**
   - 仍使用 `objective="lambdarank"`。
   - `metric="ndcg"`，`ndcg_eval_at=[topk_train_k]`。
   - 调整 relevance 分桶：按每个交易日横截面或 train-window 分位将 top-K/top-quantile 映射到高 relevance，中段压缩。
   - 设置 `label_gain` 为 top-heavy（例如高 relevance 指数型增益），避免中尾部排序主导。
2. **Phase LTR-2（仅在 LTR-1 不足时）：自定义 top-K weighted lambda 研究。**
   - 先做 API spike，确认当前 LightGBM / sklearn ranker 是否可靠支持自定义 ranking objective。
   - 若不可靠，不在生产路径硬上自定义 objective；保留原生 objective + top-heavy relevance/weight 方案。

no-silent 要求：

- `topk_train_k <= 0`、`topk_train_k` 大于最小有效日截面、label 全 NaN、query group 异常，都必须 raise 明确 `ValueError`，消息含 `reason_code`。
- 不允许在无法构造 top-K relevance 时静默退回 legacy full NDCG。

### 5.3 高维与 Alpha158 护栏

建议新增参数：

| 参数 | 默认建议 | 说明 |
| --- | --- | --- |
| `feature_guard_mode` | `"auto"` | 根据 feature_dim / Alpha158 / custom factor count 启用护栏。 |
| `high_dim_threshold` | `64` 或 `128` | 超过阈值视为高维。 |
| `allow_high_dim_ltr` | `false` | 默认不让 Alpha158 高维腿直接用激进 LTR；需显式开启。 |
| `high_dim_policy` | `"conservative"` | 高维时使用保守树复杂度，而非直接失败；若策略要求可设 `"fail_fast"`。 |
| `feature_selection_mode` | `"train_ic_prefilter"` | 可选，只用 train window IC/coverage 选特征，不看 valid/test。 |
| `feature_selection_top_n` | `32` 或 `64` | 高维降维上限。 |

高维保守参数建议：

- `num_leaves`: 16 或 31
- `max_depth`: 4 或 5
- `min_child_samples`: 300+
- `reg_alpha/reg_lambda`: 1.0+
- `colsample_bytree`: 0.5~0.7
- `subsample`: 0.7~0.8
- `n_estimators`: 配合 early stopping，先不盲目加树

策略建议：

1. P2-LTR 初期默认只在低维纯模态（FUND/FLOW/其他单 DGP）上试 LTR。
2. a1 PLUS3 + Alpha158 只有在 `allow_high_dim_ltr=true` 且真实 QE 证明 RankIC 由负转正后，才可进入候选。
3. 若 feature selection 结果低于最小特征数或覆盖不足，fail-loud，不回退全量。

### 5.4 诊断输出

现状 `training_diagnostics` 为空，无法完整证明 train/valid/OOS 过拟合路径。建议 `LambdaRankModel` 至少输出/记录以下诊断：

| 诊断 | 用途 |
| --- | --- |
| `feature_dim`、`query_count`、`min/median/max_group_size` | 判断是否符合横截面 ranking 假设。 |
| `label_bin_edges`、`relevance_counts` | 检查 relevance 是否退化或极端不平衡。 |
| `ndcg_eval_at`、best iteration、valid NDCG@K | 与训练日志一致，可结构化对比。 |
| train/valid segment 的 RankIC / NDCG@K（若可安全计算） | 判断训练过拟合和 valid 失真。 |
| OOS SignalRecord RankIC/top-K return（由现有 QE 结果侧给出） | 真实验收指标。 |
| `feature_guard_decision` | 记录是否触发 high-dim policy / feature selection。 |

在严格模块边界下，优先在 `LambdaRankModel` 中以 structured logger 输出；若要持久化到 `qlib_results_enhanced.json`，可能需要 result parser/qrun 范围扩展，应单独审批。

### 5.5 `config_composer.py` 接线

`config_composer.py` 现有 LAMBDARANK 分支已经允许 `model_hyperparameters` 覆盖，因此实现只需：

1. 在 LAMBDARANK 默认 `model_kwargs` 中增加新显式参数（保守默认）。
2. 对关键参数做 fail-fast 校验：
   - `ltr_objective_mode` 必须属于允许集合。
   - `topk_train_k` 必须为正整数。
   - `allow_high_dim_ltr` 必须显式布尔值。
3. 将 QE loop 的 `model_hyperparameters` 原样透传给 `LambdaRankModel`；不改 task/scheduler。

示例 QE loop 覆盖：

```json
{
  "model_id": "__seed_LambdaMART_20D__",
  "model_hyperparameters": {
    "ltr_objective_mode": "native_ndcg_at_k",
    "topk_train_k": 25,
    "ndcg_eval_at": [25],
    "relevance_transform": "topk_quantile",
    "feature_guard_mode": "auto",
    "allow_high_dim_ltr": false
  }
}
```

### 5.6 reason_code 建议

| reason_code | 触发 |
| --- | --- |
| `lambdarank_invalid_topk_train_k` | `topk_train_k` 非正、非整数或大于有效截面。 |
| `lambdarank_empty_segment` | train/valid segment 为空。 |
| `lambdarank_invalid_query_index` | 非 MultiIndex 或无法按 date 构造 query group。 |
| `lambdarank_degenerate_relevance` | relevance 分桶后类别不足或全同。 |
| `lambdarank_high_dim_blocked` | `allow_high_dim_ltr=false` 且触发高维/Alpha158 禁止策略。 |
| `lambdarank_feature_selection_insufficient` | train-only feature selection 后有效特征不足。 |
| `execution_model_checkpoint_shape_mismatch` | BUG-507，执行器 checkpoint 形状不匹配（非 LTR objective 范围）。 |

## 6. 分阶段执行计划

### Phase 0：文档与 API 发现（已完成）

- 读取 `LambdaRankModel`、`config_composer` LAMBDARANK 分支、seed 注入路径、历史 LambdaMART 文档、BUG-202。
- 只读查询 QE MCP 和远端 Loop 日志。
- 登记 BUG-507 / #1569 与 enhancement #1570。

### Phase 1：FLOW 接通 BUG-507（独立）

目标：让 FLOW LambdaRank 失败 loud 且可定位，或使用兼容 checkpoint 正常产出回测指标。

验证：

- 同配置 FLOW loop 不再只显示 `experiment_id=null`。
- 不兼容 checkpoint 时 reason_code 为 `execution_model_checkpoint_shape_mismatch`，带 workspace/model path/shape。
- 修复后 FLOW 能进入 P2-LTR 真实 QE 验收。

### Phase 2：top-K 对齐 LambdaRank

目标：实现 `native_ndcg_at_k` + top-heavy relevance/label_gain。

验证：

- 单测覆盖 relevance 单调性、top-K relevance 强化、`ndcg_eval_at=[25]`。
- 不能构造 top-K relevance 时 fail-loud，绝不回退 legacy。

### Phase 3：高维护栏

目标：对 Alpha158/高维腿默认启用保守策略或阻断。

验证：

- a1 + Alpha158 在 `allow_high_dim_ltr=false` 时明确 fail 或切换保守 policy（按最终设计选择），日志有 `feature_guard_decision`。
- train-only feature selection 不读 valid/test；缺特征 fail-loud。

### Phase 4：QE 配置暴露

目标：通过 `model_hyperparameters` 可配置新参数。

验证：

- `config_composer` LAMBDARANK 分支默认值和覆盖值均进入 `conf.yaml`。
- 非法参数 loud raise。

### Phase 5：真实 QE 验收（杀手断言）

真实 QE 回测，非 mock：

- 口径：`topk25/nd2/h20/filtered_pool_20260428/V25_1_SMALL_CAP`。
- 腿：a1 PLUS3、FUND_GROWTH、FLOW_ACCEL。
- 对照：Step1 同特征 MSE 基线。

通过条件：

1. a1 RankIC 必须由负转正。
2. FUND/FLOW 的 LTR CAGR 必须大于等于对应 MSE，或给出可解释且可复现的原因。
3. FLOW 必须跑通并产出指标（依赖 BUG-507 或兼容 execution profile）。
4. 若改进后 LTR 仍全面小于等于 MSE，结论必须写为“LTR 路线在本数据/特征下证伪”，不得粉饰。

## 7. Anti-pattern guards

1. 不把 FLOW checkpoint 失败伪装成 LambdaRank objective 问题。
2. 不在低维/FLOW 未跑通前追加大规模 seed。
3. 不用全样本/valid/test 做特征筛选，避免泄漏。
4. 不在无法计算 top-K relevance 时静默回退旧 full NDCG。
5. 不修改 QE task/scheduler、qrun、生产 runtime 模板。
6. 不触碰 `research-assistant`、`assistant_`、`research_` 路径。

## 8. Issue / BUG

- BUG-507 / #1569：`QE LambdaRank FLOW pure-factor V25_1 backtest hides execution checkpoint shape mismatch`
  - URL: `https://github.com/licong01-cloud/AIstock/issues/1569`
  - 本 PR 持久化 BUG JSON，不修实现。
- Enhancement / #1570：`[multi-alpha][LTR] top-K aligned LambdaRank objective and high-dimensional safeguards`
  - URL: `https://github.com/licong01-cloud/AIstock/issues/1570`
  - 作为实现阶段设计入口。

## 9. 本轮门控

- `production_ddl_gate`: `noop`
- `production_frontend_dependency_gate`: `noop`
- `production_backend_dependency_gate`: `noop`
- 未启动/重启任何服务。
- 未写 DB / 未应用 DDL / 未跑 QE 训练或回测。
- 本轮仅文档、BUG JSON、Issue 候选。
