# Advisory P0-E SHORT_REBOUND 收益感知 Meta-label Challenger 详细设计

> 日期：2026-08-24
> Feature tier：F2
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.4
> 直接父产物：P0-C bundle `81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd`
> 参考模型：P0-D bundle `e555903ec928fd39ea09180133401a6490a4e6d5440e3ef63642909e1329e03a`
> 当前阶段：`REAL_EXPERIMENT_COMPLETE_NEGATIVE_NOT_ACTIVATED_FINAL_REVIEW_PENDING`
> 适用范围：学术研究与模拟荐股观察，不构成实时投资建议，不连接实盘交易或下单执行

## 1. Background / 真实失败归因

P0-D 在冻结 CPCV 上相对 Selection Top5 的主指标提高 `3.6556 bps`，但首个24决策日历史虚拟前向结果显示：命中率提高 `9.74pp`，累计净收益却低 `2.54pp`、最大回撤低 `5.63pp`，平均换手高 `5.33pp`。因此 P0-D 不能激活，下一轮必须优先修复收益幅度和替换质量，而不是继续增加历史证据平台。

对权威 artifact `fbf072f0d8c4a637a48aa8c2ed63c3b61c245abd08ac4e1417b2a0fcc8eb59a9` 及其24日 exact priority artifacts 的逐 episode 归因得到：

- P0-D 30个完成 episode 中11胜19负，平均盈利 `349.17 bps`、平均亏损 `-758.87 bps`；Selection 的7胜19负中平均盈利 `984.90 bps`、平均亏损 `-835.44 bps`。问题不是 P0-D 平均亏损更深，而是新增小盈利没有补偿错失的大盈利。
- P0-D 的30次入场中22次来自 Selection rank 6..20；这些被提升 episode 平均 `-411.16 bps`，Selection rank 1..5 中保留的8次平均 `-191.52 bps`。
- P0-D 实际入场概率仅位于 `0.54927..0.56614`，take probability 与最终 episode 净收益的 Spearman 相关约 `0.0976`，说明二分类概率对收益幅度几乎没有排序能力。
- P0-D 比 Selection 多使用9次 replacement budget；交易成本差仅累计 `5.52 bps`，换手是弱排序导致的附加损失，不是收益差的唯一来源。
- 现有 `take_label = net_excess_return_bps > 0` 把小盈利与大盈利、小亏损与大亏损等价处理。冻结标签本身已包含连续 `net_excess_return_bps/confidence_target`，无需创建新数据集或修改 policy。

据此，本阶段只批准一个主实验：收益幅度加权的 binary meta-label。双头回归、开放式超参搜索、策略规则调整和生产激活均不在范围内。

## 2. Scope / 交付范围

1. 在现有 frozen meta-label request 上增加向后兼容的 v3 收益权重合同；旧 v2 request 和 bundle 必须继续 exact readback。
2. 固定 `ABS_NET_EXCESS_TRAIN_MEDIAN_V1` 权重：每条 CPCV path 只用该 path train rows 拟合绝对净超额收益中位数，再构造有界、均值归一化的训练权重。
3. 两个既有 family（CORE、CORE_HMM）和三个既有 seed 保持不变，只改变 label loss weighting，形成6个预注册 trials × 28 paths。
4. validation early stopping 可以使用 validation outcome 构造权重，但缩放参数只能来自 train；模型选择仍只看 shared policy 的 `mean_daily_net_excess_return_bps`。
5. request 精确绑定当前 P0-D reference bundle；输出逐 path 配对 lift、path win rate 和均值差，不把 Selection baseline 冒充当前模型基线。
6. winner 无论正负均发布到独立 repo-external experimental root，保留完整 bundle 和 exact retry；不覆盖当前 P0-D bundle。
7. 若冻结 CPCV 结果完成，再用隔离 descriptor 在已消费24日窗口运行 `HISTORICAL_REPLAY`，只作诊断，不参与选模。
8. 更新父蓝图中的直接归因、实验状态和真实结果；不做历史证据归档或通用平台建设。

允许修改：

```text
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
docs/architecture/advisory_p0d_short_rebound_meta_label_f2_design_20260824_return_aware_challenger.md
backend/services/advisory_model_first/meta_label_contracts.py
backend/services/advisory_model_first/meta_label_training.py
backend/services/advisory_model_first/meta_label_pipeline.py
backend/services/advisory_model_first/meta_label_bundle.py
scripts/advisory_meta_label_prepare_request.py
backend/tests/advisory_model_first/test_meta_label_contracts.py
backend/tests/advisory_model_first/test_meta_label_training.py
backend/tests/advisory_model_first/test_meta_label_pipeline.py
backend/tests/advisory_model_first/test_meta_label_bundle.py
```

测试证明需要时可新增同目录直接测试文件，但必须在首次写入前加入本表并保持 `advisory.model_first` ownership。

## 3. Non-goals / 明确禁止

- 不修改 P0-C labels、candidate set、Selection Top40、shadow/cost policy、target count、止盈止损、rank exit 或 daily replacement budget。
- 不读取历史回放窗口选择 family、seed、权重公式、cap、scale 或阈值；这些值在训练前冻结。
- 不训练回归双头、LambdaRank、神经网络或第三个 family，不做网格/贝叶斯/人工循环调参。
- 不使用同一历史窗口反复挑选模型；任何重跑固定标为 `HISTORICAL_REPLAY`。
- 不修改 API/UI、数据库、scheduler、生产 descriptor、active binding、Paper、Simulation、QMT 或 Selection writes。
- 不新增通用 ModelOps、缓存、历史归档、数据平台、审批门禁或后台服务。
- 不执行 DEV/生产 DDL/DML，不重启后端，不激活 bundle。

## 4. Architecture / 数据与执行流

```text
exact P0-C policy dataset + existing PIT features/CPCV
  -> path-local train rows
  -> scale_bps = median(abs(train net_excess_return_bps))
  -> raw_weight = 1 + clip(abs(net_excess_return_bps) / scale_bps, 0, 4)
  -> sample_weight = raw_weight / mean(train raw_weight)
  -> validation weight uses the train scale and train normalization divisor
  -> existing CORE / CORE_HMM LightGBM binary training
  -> standard take_probability and unchanged runtime scorer
  -> shared dual-rank Top5 policy evaluation
  -> frozen primary-metric winner and PBO
  -> paired comparison with exact P0-D reference winner
  -> immutable experimental bundle in isolated model root

consumed historical window
  -> isolated experimental descriptor
  -> production-equivalent scorer + shared policy replay
  -> HISTORICAL_REPLAY diagnostic only
```

权重最大原始值固定为5，归一化只改变整体梯度尺度，不改变样本相对权重。`scale_bps` 非有限或不大于0、return 非有限、validation 使用自身分位数、权重非有限或不大于0均 fail closed。

## 5. Contracts / 冻结身份与兼容性

### 5.1 Request v3

`FrozenAdvisoryMetaLabelTrainingRequestV1` 保留类型名以兼容现有 loader，但允许：

- 旧 `frozen_advisory_meta_label_training_request_v2`：`outcome_weighting=None`、无 reference，functional payload 与历史 hash 完全不变。
- 新 `frozen_advisory_meta_label_training_request_v3`：必须包含 `outcome_weighting` 和 exact reference bundle identity。

固定权重合同：

```text
schema_version = advisory_meta_label_outcome_weighting_v1
mode = ABS_NET_EXCESS_TRAIN_MEDIAN_V1
scale_statistic = MEDIAN_ABSOLUTE_NET_EXCESS_BPS
base_weight = 1.0
relative_cap = 4.0
normalization = TRAIN_MEAN_ONE
```

新 reference identity：

```text
reference_meta_label_bundle_root
reference_meta_label_bundle_id
reference_meta_label_manifest_file_sha256
```

三项全部进入 request hash。reference bundle 必须与新 request 的 policy dataset、program、package、binding、manifest、shadow policy 和 feature schema 完全一致。

### 5.2 Training / prediction

- train 和 validation 的 label 均必须 `MATURED` 且 `net_excess_return_bps` 有限。
- LightGBM 训练和 validation dataset 都显式携带 sample weight；validation 权重只能复用 train scale/normalizer。
- 候选 ROC-AUC/Brier 等继续按未加权真实候选分布报告；另记录 train/validation weight scale、范围和均值。
- 运行时仍输出 `take_probability/skip_probability/advisory_model_confidence/entry_priority_rank`，不需要第二个模型文件或 scorer 分支。

### 5.3 Reference comparison

- 从 exact reference `winner_receipt.json` 找到 reference family/seed，并读取其 `cpcv_trial_metrics.parquet`。
- 新 winner 与 reference 必须具有同一28 path id集合且每path唯一。
- 报告 `candidate_minus_reference_mean_primary_metric_bps`、逐path lift、path win rate、tie count，以及 Selection/HMM/random/candidate20 baseline。
- reference comparison 是研究事实，不是生产激活门禁；历史回放也不得反向改变 winner。

## 6. Leakage and identity boundary / 泄漏边界

- 特征、HMM、categorical vocabulary、CPCV train/validation 和 context tail 沿用 P0-D 已审核合同。
- 收益 scale、训练权重均按 path train rows 拟合；validation 绝不参与 scale/normalizer。
- final refit 只使用全部 P0-C `MATURED` labels，scale 只从这些行拟合；不读取 context-only future labels。
- 已消费历史回放窗口不进入 request、训练、early stopping、family/seed selection 或权重公式。
- request 继续绑定 clean repository commit；dirty tracked source、root/commit/hash不一致均阻断正式训练。

## 7. Model experiment / 固定实验矩阵

- families：`FAMILY_CORE`、`FAMILY_CORE_HMM`。
- seeds：`20260813, 20260817, 20260823`。
- paths：P0-C 28/28 READY CPCV。
- trial-path rows：168；PBO trials：6；其它 LightGBM 参数与 P0-D 完全相同。
- 主指标：validation shared-policy `mean_daily_net_excess_return_bps`。
- tie-break：`family_id_seed_ascending`。
- reference：P0-D `FAMILY_CORE_HMM/20260817`。
- 不设置结果后新增 family、阈值或第二套 blend。

研究上的 Challenger 改善定义在训练前固定为：新 winner 的28-path配对主指标均值高于 exact P0-D reference，且 path win rate 大于0.5。未满足时结论为负面实验，不激活、不继续在已消费窗口调参。

## 8. Bundle / isolated replay

新 bundle 继续使用 `advisory_meta_label_bundle_v1` 和 `meta_label_take_skip_confidence`，因为推理输出合同没有变化；manifest 额外记录 `training_objective=OUTCOME_MAGNITUDE_WEIGHTED_BINARY_V1`，并把 `reference_challenger_comparison.json` 纳入 identity files。旧 bundle manifest 不新增默认字段，历史 bundle id/readback不得变化。

真实训练和回放使用独立 root，例如：

```text
F:/Dev/AIstock_model_artifacts/advisory_p0e_return_aware_20260824
F:/Dev/AIstock_model_artifacts/advisory_p0e_return_aware_replay_20260824
```

隔离 root 只复制 exact P0-C policy dataset、发布新 bundle 和实验 descriptor。不得旋转 `F:/Dev/AIstock_model_artifacts/advisory_model_first` 中当前 descriptor，不得改变生产 active binding。

### 8.1 Real WSL Result / 真实训练结果

| item | result |
|---|---|
| repository commit | `7e767a139bb5d34ac514dc8b93a31ec5813dc363` |
| request | `advmetareq_4d2393bcb776cf7d6a3aace2` / `4d2393bcb776cf7d6a3aace297328485167c5dbe0e7acf0f48e3dec13a854733` |
| bundle | `cb9e61e9c54d89263f76f2f2bcefb515070c96908aa2bca790c064fd339fb270` |
| trial matrix | 2 families × 3 seeds × 28 paths = 168 rows；6 trials × 8 blocks |
| winner | `FAMILY_CORE_HMM / 20260817`；4 boost rounds |
| candidate metric | mean daily net excess `18.9626236037 bps`；Selection lift `+3.1825697785 bps`；Selection path win rate `57.14%` |
| exact P0-D reference | `19.4356787838 bps`；candidate lift `-0.4730551801 bps`；candidate path win rate `35.71%`；`research_improvement=false` |
| candidate diagnostics | ROC-AUC `0.5159415721`；Brier `0.2523798050` |
| PBO | `0.8142857143`，比 P0-D 的 `0.40` 明显恶化 |
| outcome weighting | full refit scale `335.7896 bps`；train mean weight `1.0`；weight range `0.42999..2.14820` |
| resource | `346.325s`；peak RSS `2,736,852,992 bytes`，低于8GB |
| exact retry | `3.3s`；返回同一 bundle；`EXISTING_BUNDLE/activated=false` |

冻结 CPCV 结论为负面：幅度权重仍优于 Selection，但没有超过当前 P0-D，且稳定性/PBO恶化。按预注册规则不追加 family、cap、阈值或 blend，不激活该 bundle。

### 8.2 HISTORICAL_REPLAY Result / 已消费窗口诊断

最终源码 re-attestation 在 rebase 后重新冻结 request；新旧 bundle 的模型、168行 CPCV、PBO、reference comparison、winner 与 HMM receipt 功能 hash 全部一致，变化仅来自 request/source identity。隔离 descriptor `a485adefb9...` 对同一24个决策日+20日tail完成100% resolved回放，artifact 为 `6bba37f8804af38f4357c3939a380cca3be2bc915a62149108518b6d4948dba4`，exact retry 返回同hash。证据分类为 `HISTORICAL_REPLAY`，不参与选模。

| metric | P0-E weighted | Selection Top5 | 原P0-D | P0-E vs Selection |
|---|---:|---:|---:|---:|
| completed episode | 26 | 26 | 30 | 0 |
| hit rate | 23.08% | 26.92% | 36.67% | -3.85pp |
| mean episode net return | -284.91 bps | -345.35 bps | -352.59 bps | +60.43 bps |
| cumulative net return | -13.39% | -16.90% | -19.45% | +3.52pp |
| maximum drawdown | -14.92% | -16.90% | -22.54% | +1.98pp |
| mean turnover | 29.71% | 34.67% | 40.00% | -4.95pp |

episode归因显示 P0-E 平均盈利 `675.41 bps`、平均亏损 `-573.01 bps`，相对原P0-D的 `349.17/-758.87 bps` 修复了该窗口中的幅度结构；但冻结 CPCV 的配对结果没有稳定复现，因此不能用这份已消费窗口的改善覆盖正式负面结论。

## 9. API / UI / Database / Runtime impact

- API/UI：none。
- DDL/DML：none；历史回放只读现有数据库行情和交易日历。
- backend/worker/scheduler restart：none。
- dependency：none。
- production descriptor/binding/model activation：none。
- source merge 后运行时影响：none；只有未来用户单独授权 descriptor rotation 和重启后才可能激活。

## 10. Verification Plan / 验证方案

### 10.1 Compatibility and contracts

- 历史 v2 request 原 JSON 重新解析后 request id/hash/functional payload不变。
- v3 缺 weighting/reference、部分 reference identity、非法 cap/scale mode均拒绝。
- prepare CLI 默认仍生成 v2；显式 `--outcome-weighted` 才生成 v3并要求 reference root。

### 10.2 Weighting and leakage

- 手工小矩阵证明大绝对收益权重大、cap生效、train mean为1。
- validation 极端收益不能改变 train scale/normalizer。
- NaN/inf/zero scale fail closed。
- uniform v2 路径的预测与现有行为不变。

### 10.3 Pipeline, bundle and exact retry

- mock/fixture 证明168 trial路径均传入正确 weighting；reference path集合缺失/重复/identity不符 fail closed。
- 新 bundle包含 weighting/reference receipt；旧 bundle loader/scorer测试保持通过。
- 相同 v3 request exact retry 返回同一 bundle，不重算训练。

### 10.4 Real WSL and replay

- clean committed worktree、`rdagent-gpu`、真实7,716 labels、28 paths完整训练。
- peak RSS小于8GB；输出 winner/PBO/reference comparison/资源报告。
- 隔离 descriptor loader/scorer readback成功。
- 24日+20日tail回放明确为 `HISTORICAL_REPLAY`，与 Selection 和原P0-D artifact并列报告。

## 11. Implementation Plan / 实施方案

1. 先用兼容性测试扩展 frozen request v3 和 prepare CLI，证明旧 v2 request hash 不变。
2. 实现 path-local outcome weighting helper，并把同一 train receipt 传给 train/validation/final LightGBM dataset。
3. 在 pipeline 中校验 exact P0-D reference，生成28-path配对 comparison，并把它纳入 immutable bundle。
4. 执行 direct tests、F2 validator、L0/L2和三轮审核；每次只复测失败节点，稳定后运行一次相关小矩阵。
5. 以 clean commit 在 WSL `rdagent-gpu` 完整训练和 exact retry，在隔离 model root 发布实验 descriptor。
6. 使用 `WINDOW_CONSUMED_OR_UNKNOWN` 完成24日 `HISTORICAL_REPLAY`，结果回填本设计与蓝图。
7. 所有验收项通过后创建 PR；required CI 在8小时窗口内全绿时按用户授权自动合入。

## 12. Production Gates / 生产门禁

| gate | 本任务状态 | 自动执行边界 |
|---|---|---|
| source PR/merge | 验收和CI通过后允许 | 用户已授权自动合入 |
| DEV/production DDL/DML | `none` | 禁止执行 |
| backend/worker/scheduler restart | `none` | 禁止执行 |
| dependency install | `none` | 禁止执行 |
| production descriptor/binding activation | `out_of_scope` | 必须另行授权 |
| isolated experimental artifacts | `allowed` | 仅显式 repo-external root |
| worktree/branch/file cleanup | `not_authorized` | 不执行删除 |

## 13. Rollout / rollback

### Rollout

1. F2 设计校验通过后实施源码和直接测试。
2. 以 clean commit 执行正式 WSL 训练、exact retry 和隔离历史回放。
3. 至少三轮设计、代码、结果审核；全部通过才提交 PR。
4. 本任务已获源码自动合入授权；required CI 全绿且无阻断意见时可合入。
5. 合入不触发 backend restart、DDL、descriptor rotation、bundle activation 或 source cleanup。

### Rollback

- 源码回滚不改数据库、生产 descriptor 或已有 P0-C/P0-D bundle。
- 新 experimental bundle和负面结果保持 repo-external，不覆盖旧产物。
- 发现语义缺陷时旋转新 request/bundle，不原地改写 content-addressed artifact。

## 14. Risks / 风险

| 风险 | 处理 |
|---|---|
| 权重利用 validation 统计 | scale/normalizer只从path train拟合并直接测试 |
| 极端收益支配训练 | 相对增量cap=4，最终权重train mean归一化 |
| 只提高候选loss却不改善组合 | winner仍按shared policy主指标选择，并精确对比当前P0-D |
| 结果后继续调参 | 固定一个 weighting、两个既有family、三个既有seed；负面即停止 |
| 旧request/bundle身份漂移 | v2缺省字段从functional payload排除，历史hash回归测试 |
| 实验 descriptor 污染生产 | 独立model root，禁止旋转当前root descriptor |
| 历史窗口再次冒充OOT | 强制 `WINDOW_CONSUMED_OR_UNKNOWN -> HISTORICAL_REPLAY` |
| 代码成功冒充模型成功 | 源码、CPCV结果、历史回放和激活状态分别报告 |

## 15. Design Acceptance Index

| ID | requirement |
|---|---|
| F-701 | 历史回放逐episode归因直接证明收益幅度/弱排序问题 |
| F-702 | v3 outcome weighting request精确冻结且v2身份向后兼容 |
| F-703 | 权重scale/normalizer仅用path train，非有限输入fail closed |
| F-704 | 固定2 family×3 seed×28 path，不做结果后搜索 |
| F-705 | runtime概率、dual-rank entry/selection exit和shadow policy完全不变 |
| F-706 | exact P0-D reference身份与28-path配对比较完整 |
| F-707 | immutable experimental bundle包含权重与reference comparison receipt |
| F-708 | real WSL训练、8GB资源上限和exact retry真实完成 |
| F-709 | 已消费窗口仅作HISTORICAL_REPLAY，不参与选模 |
| F-710 | 零DB/API/UI/生产descriptor/activation/restart影响 |
| F-711 | 定向测试、F2 validator、L0/L2、三轮审核和DESIGN-COMPLIANCE通过 |

## 16. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-701 | 本设计 §1；artifact `fbf072...` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0d_historical_forward_replay_20260823/p0d-historical-forward/fbf072f0d8c4a637a48aa8c2ed63c3b61c245abd08ac4e1417b2a0fcc8eb59a9.json` | pass | none |
| F-702 | `meta_label_contracts.py` | `backend/tests/advisory_model_first/test_meta_label_contracts.py` | pass | none |
| F-703 | `meta_label_training.py` | `backend/tests/advisory_model_first/test_meta_label_training.py` | pass | none |
| F-704 | request/pipeline | `backend/tests/advisory_model_first/test_meta_label_pipeline.py`; artifact: `cb9e61e9.../cpcv_trial_metrics.parquet` | pass | none |
| F-705 | existing scorer/evaluator | `backend/tests/advisory_model_first/test_meta_label_bundle.py`; `backend/tests/advisory_model_first/test_meta_label_portfolio.py` | pass | none |
| F-706 | `meta_label_pipeline.py` | `backend/tests/advisory_model_first/test_meta_label_pipeline.py`; artifact: `cb9e61e9.../reference_challenger_comparison.json` | pass | none |
| F-707 | `meta_label_bundle.py` | `backend/tests/advisory_model_first/test_meta_label_bundle.py` | pass | none |
| F-708 | WSL CLI and bundle | artifact: `cb9e61e9.../resource_report.json`; exact retry `EXISTING_BUNDLE` | pass | none |
| F-709 | historical replay CLI | artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0e_return_aware_replay_20260824/p0d-historical-forward/6bba37f8804af38f4357c3939a380cca3be2bc915a62149108518b6d4948dba4.json` | pass | none |
| F-710 | import/scope/diff review | `tests/aistock_validation/catalog/file_ownership.yaml`; validation-receipt: `nox -s l0` pass，0 HIGH finding | pass | none |
| F-711 | complete diff | `backend/tests/advisory_model_first/`; validation-receipt: F2 validator and three review rounds | pending | final gate |

## 17. DESIGN-COMPLIANCE-001

1. **无简化版**：使用真实P0-C labels、真实PIT features、全部28 CPCV paths、两个family/三个seed、shared policy和真实历史回放，不使用mock结果交付。
2. **无静默错误**：缺失/非有限收益、非法scale、reference不匹配、trial/path缺失、dirty source或artifact冲突全部fail closed。
3. **无业务偏移**：只改变训练sample weight；候选召回、Selection rank、退出、policy、成本、运行时概率合同和生产baseline均不变。
4. **无额外门禁审批**：研究胜负标准在训练前固定但不构成生产审批；源码合入按用户本任务授权，生产激活仍需独立授权。

审核记录：

- Round 1（设计/泄漏）：基于真实 episode 归因否决开放式双头与调参搜索，冻结为单一 train-only magnitude weighting；实际旧 v2 request `0451bd...` 以原 hash 成功解析。
- Round 2（代码/身份）：314个 `advisory_model_first` 直接测试通过；正式 reference 168行 readback、WSL 168 trial-path、bundle exact retry和隔离 replay exact retry通过。发现测试 helper 传 dict 时产生 Pydantic serializer warning，已在 builder 入口正规化 nested contracts并针对性复测。
- Round 3（结果/生产隔离）：模块 L2 `364 passed, 8 skipped`，L0 pass；最终源码重新训练与首轮产物的模型/CPCV/PBO/reference功能hash完全一致。生产 descriptor仍为 `f98f2ded... -> e555903e...`，实验 descriptor为独立root `a485adef... -> cb9e61e9...`。负面CPCV与正向已消费回放并列披露，不用后者覆盖前者。

## 18. Completion definition / 完成定义

- F-701..F-711 均有直接实现/测试/真实产物证据且无 pending gap。
- v2 request/bundle exact兼容；v3 168 trial-path完整、PBO和reference comparison可读。
- 新模型无论优劣均有诚实结论；若不超过参考P0-D，不新增实验分支。
- 历史回放只能是 `HISTORICAL_REPLAY`，不写生产事实。
- required CI和三轮审核通过后才自动合入；DDL、重启、激活和清理保持未执行。
