# Advisory Model-First M5B Outcome Calibration F2 详细设计

> 日期：2026-08-12  
> Feature tier：F2  
> 状态：DESIGN_REVIEWED  
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`  
> 前序设计：`docs/architecture/advisory_model_first_m3_outcome_holding_period_f2_design_20260809.md`、`docs/architecture/advisory_model_first_m5_quality_iteration_f2_design_20260811.md`

## 1. Background / 背景与目标

M5B 只解决现有 M3 outcome 模型的两个真实质量问题：

1. 对五个 horizon 的 `positive_excess` 与 `signal_survival` 二分类 head 做 validation-only Platt calibration，同时保留 raw probability。
2. 对五个 horizon 的 excess-return 中央 80% 区间与 MFE/MAE q90 单侧区间做 validation-only conformal adjustment，同时保留 raw quantile。

M5B 不重新训练 M3 的 46 个 LightGBM head，不改变候选股票、M2/M5A 排名、Top5、持股周期分类、M4 价格范围或任何交易模块。校准结果仍是 `EXPERIMENTAL_SHADOW` 学术研究输出，不构成收益保证、交易建议或自动交易输入。

本阶段的第一目标是让真实 M3 预测具备可解释的概率和区间覆盖语义。不得扩展到历史数据归档、通用证据平台、跨模块统一改造、角色、审批或二次策略包准入。

## 2. 当前权威基线

### 2.1 源码与模型

- M3 request：`advoutreq_d16081c54d47b3602c89e3b2`。
- M3 bundle：`17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0`。
- M3 parent M1 bundle：`9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629`。
- M3 bundle schema：`advisory_outcome_bundle_v1`，`calibration_state=UNCALIBRATED`。
- 冻结输入：8120 条 candidates、6960 条 features、406 个 decision dates。
- split：226 train / 25 purge / 50 validation / 25 purge / 80 test。
- 模型：15 个 excess-return quantile、10 个 binary、20 个 MFE/MAE quantile、1 个 holding multiclass，共 46 个真实 LightGBM head。

### 2.2 真实 validation 可行性读回

2026-08-12 使用冻结 M3 feature、label、split 和 bundle 只读重放 validation：每个 binary head 都有 940 条 eligible validation rows，且正负类别同时存在。

| family | horizon | positive | negative |
|---|---:|---:|---:|
| positive_excess | 1 | 346 | 594 |
| positive_excess | 3 | 353 | 587 |
| positive_excess | 5 | 345 | 595 |
| positive_excess | 10 | 329 | 611 |
| positive_excess | 20 | 336 | 604 |
| signal_survival | 1 | 325 | 615 |
| signal_survival | 3 | 332 | 608 |
| signal_survival | 5 | 335 | 605 |
| signal_survival | 10 | 328 | 612 |
| signal_survival | 20 | 338 | 602 |

当前已验证 WSL `rdagent-gpu` 提供 LightGBM、scikit-learn 1.7.2 和 SciPy 1.15.3。该检查只证明拟合条件存在，不代表校准后质量必然改善。

## 3. Non-Goals / 非目标

- 不读取生产 PostgreSQL 作为训练或校准历史数据源。
- 不重建 candidates、features、outcome labels 或 Qlib 数据集。
- 不使用分钟线，不访问 Tushare，不创建 SQLite 或新缓存平台。
- 不校准 M3 holding multiclass；其概率和 20%-80% bucket range 继续明确为 `UNCALIBRATED`。
- 不处理 M4 entry-gap quantile 或 `entry_executable` binary；它们属于 M5C，且 M4 binary 负例不足时必须继续 `UNCALIBRATED`。
- 不改变 M5A reranker winner、M1/M2/M3/M4 的既有 bundle 或 binding。
- 不修改 Selection Center、Paper/Simulation Runtime、StrategyPackage、QE、RD-Agent 或共享 PIT 链路。
- 不新增 DDL、DML、后台任务、调度器、用户、角色、审批或质量阻断门禁。

## 4. 数据来源与时间隔离

M5B 只读取以下已经冻结的文件：

```text
outcome_bundles/<m3_bundle_id>/manifest.json
outcome_bundles/<m3_bundle_id>/training_request.json
outcome_bundles/<m3_bundle_id>/feature_schema.json
outcome_bundles/<m3_bundle_id>/split.json
outcome_bundles/<m3_bundle_id>/models/*.txt
outcome_runs/<m3_request_id>/outcome_labels.parquet
<m3 training request>.features_artifact.path
```

新 request 必须记录上述文件的绝对路径、SHA256、size、row count、column order、M3 manifest file hash、M3 bundle ID、M3 request ID、feature/label/split policy identity、代码 commit 和 WSL environment。该身份核对只防止拿错模型或文件，不是策略包二次验证或业务门禁。

校准拟合只允许读取 `split=validation`：

```text
train       -> M3 基模型已经使用；M5B 不再读取标签拟合新 base head
validation  -> 唯一允许拟合 calibrator / conformal adjustment 的集合
test        -> calibrator 完全冻结后只评价一次，不参与参数、算法或阈值选择
```

实现必须让 fit 函数的输入对象仅包含 validation projection，不能依赖调用方约定后在函数内部过滤全量 frame。validation fit 完成后先把 `calibration.json` 候选写入 run root，完成 canonical hash/readback 并关闭 fit stage；test evaluator 随后重新读取该冻结文件，且只接收 test projection 和已冻结 calibration spec。任何 test label 都不能出现在 fit stage 的函数参数、闭包或对象属性中。

## 5. Contracts / 冻结请求合同

新增 `FrozenAdvisoryOutcomeCalibrationRequestV1`：

```text
schema_version = frozen_advisory_outcome_calibration_request_v1
request_id / request_sha256
parent_outcome_request_id / parent_outcome_request_sha256
parent_outcome_bundle_id / parent_outcome_manifest_file_sha256
package_id / manifest_sha256 / style_profile_id / style_profile_hash
feature_schema_version / feature_schema_hash
label_policy_version / split_hash
features_artifact / outcome_labels_artifact
calibration_policy_version = advisory_outcome_calibration_policy_v1
binary_method = PLATT_RAW_MARGIN
return_interval_method = CQR_CENTRAL_80_NONNEGATIVE_EXPANSION
path_upper_method = CONFORMAL_UPPER_90_NONNEGATIVE_EXPANSION
ece_bin_count = 10
repository_root / repository_commit / output_root
resource_max_rss_bytes = 8 GiB
```

`request_sha256` 排除 `created_at` 和 `output_root`，其余功能字段全部进入 canonical identity；`request_id=advoutcal_<sha256[:24]>`。相同 request exact retry 必须读回同一 receipt/bundle，不重复生成不同身份。

## 6. Probability Calibration

### 6.1 输入与算法

每个 horizon 和 binary family 独立处理，共 10 个 calibrator：

```text
positive_excess_h{1,3,5,10,20}
signal_survival_h{1,3,5,10,20}
```

1. 使用原 LightGBM booster 对 validation matrix 调用 `predict(raw_score=True)` 得到 margin `z`。
2. 使用 validation truth `y` 拟合 `p_cal = sigmoid(a*z+b)`。
3. 固定使用 scikit-learn `LogisticRegression(penalty=None, solver="lbfgs", fit_intercept=True, max_iter=1000, random_state=20260812)`。
4. 保存 `a`、`b`、样本数、正负样本数、solver、版本、收敛状态和 validation metrics。
5. 不修改原 booster，不把 calibrated probability 回写成模型 raw prediction。

若某 head validation 缺任一类别，则该 head 保存：

```text
calibration_state = UNCALIBRATED
reason_code = ADVISORY_OUTCOME_CALIBRATION_CLASS_VARIATION_MISSING
```

并继续返回 raw probability；禁止使用常数、其他 horizon、其他 family 或全局 calibrator 代替。当前权威 M3 的 10 个 head 均有双边样本，因此真实执行预期为 10/10 calibrated。若 optimizer 不收敛、系数非有限或运行异常，则整个 request typed failure，不把技术失败伪装成合法 `UNCALIBRATED`。

### 6.2 指标

validation 与 test 对 raw/calibrated 分别报告：

- ROC-AUC：判别力，校准不会被描述为提升排序能力。
- Brier score。
- binary logloss，概率在指标计算时仅按固定机器 epsilon 裁剪，原输出仍原样保留。
- 10-bin ECE：固定 `[0.0,0.1), ... [0.9,1.0]`；空 bin 不进入加权和，报告每 bin count、mean probability、event rate 和 contribution。
- positive rate 与 row count。

不得因 test Brier/ECE 变差重新拟合、改方法、删除 head 或回退到 raw 并声称已校准。结果必须如实记录。

## 7. Quantile Calibration

### 7.1 Excess-return 中央 80% 区间

对每个 horizon，先按 M3 运行时相同方式对 validation raw q10/q50/q90 做行内单调排序，得到 `lo, mid, hi`。定义：

```text
score_i = max(lo_i - y_i, y_i - hi_i, 0)
k = ceil((n + 1) * 0.8)
delta = sorted(score)[min(k, n) - 1]
q10_cal = lo - delta
q50_cal = mid
q90_cal = hi + delta
```

`delta>=0`，所以只扩张、不收缩 validation 之外的区间，且保持 `q10_cal <= q50_cal <= q90_cal`。五个 horizon 独立保存 delta，不跨 horizon 共用。test 只报告 raw/calibrated empirical coverage、mean interval width、pinball loss 和 crossing count，不以接近 0.8 作为发布门禁。

### 7.2 MFE/MAE q90 单侧区间

现有 M3 对 `path_mfe` 和 `path_mae_loss` 只训练 q50/q90，没有 q10。M5B 不伪造 q10，也不重训新增 head。对每个 family/horizon：

```text
base_q50 = max(0, min(raw_q50, raw_q90))
base_q90 = max(base_q50, raw_q90)
score_i = max(y_i - base_q90_i, 0)
k = ceil((n + 1) * 0.9)
delta = sorted(score)[min(k, n) - 1]
q50_cal = base_q50
q90_cal = base_q90 + delta
```

该合同校准 q90 的单侧 90% coverage，不把 q50/q90 错写为中央 80% 区间。test 报告 raw/calibrated q90 upper coverage、mean upper bound、q50/q90 pinball loss、crossing count和裁剪计数。q50 保留原始点估计语义。

### 7.3 持股周期

`holding_bucket` multiclass head 不在 M5B 拟合范围。API/UI 必须继续返回其 raw probabilities、mode 和 range，同时显示 `holding_calibration_state=UNCALIBRATED`。禁止用 outcome bundle 外层的 `PARTIAL` 暗示持股概率已经校准。

## 8. Architecture / Calibration Spec 与 Bundle v2

### 8.1 自包含发布

新 bundle 使用：

```text
outcome_bundles/<outcome_bundle_id>/
  manifest.json
  calibration_request.json
  parent_training_request.json
  feature_schema.json
  label_policy.json
  split.json
  calibration.json
  models/*.txt
  metrics.json
  validation_predictions.parquet
  test_predictions.parquet
  calibration_log.json
```

46 个 `models/*.txt` 从父 M3 bundle 逐文件复制并验证 SHA256/size，内容必须字节一致。新 bundle 是自包含的原子目录，避免 runtime 同时依赖两个可变目录；父 v1 bundle 保持只读且可回滚。临时目录完整 readback 后使用 `os.replace` 发布，失败不留下可加载的半成品。

### 8.2 Manifest

```text
schema_version = advisory_outcome_bundle_v2
status = EXPERIMENTAL_SHADOW
calibration_state = PARTIAL
binary_calibration_state = CALIBRATED 或 PARTIAL
return_interval_calibration_state = CALIBRATED
path_upper_calibration_state = CALIBRATED
holding_calibration_state = UNCALIBRATED
parent_outcome_bundle_id / parent_outcome_request_id
calibration_request_id / calibration_request_sha256
calibration_policy_version
model_count = 46
files = 完整 hash/size descriptors
```

外层固定为 `PARTIAL` 是因为 holding 概率仍未校准；即使 10 个 binary head 全部成功，也不得宣称整个 M3 bundle 为 `CALIBRATED`。`binary_calibration_state=PARTIAL` 只用于 validation 类别合法缺失的 head；技术错误不产生 bundle。

`calibration.json` 对每个 head 保存独立状态和参数，并绑定 validation row identity hash。任何缺字段、未知 head、非有限参数、模型文件 hash 不一致或 calibration spec hash 不一致均 typed fail-closed，不自动加载 v1 或别的 bundle。

## 9. Runtime 与 API 合同

### 9.1 Loader

`load_exact_outcome_bundle` 显式支持 v1/v2：

- v1：保持当前 `UNCALIBRATED` 行为和返回字段，完整回归不变。
- v2：验证 46 个模型、calibration spec、逐 family/head 状态、父 M3 identity 和自包含文件。
- 未知 schema、缺失 calibrator、非有限输出、raw/calibrated 行数不一致均返回现有 outcome panel typed unavailable；不得阻断 M2/M5A rank、规则荐股或其他 Program。
- 每个 package/manifest/style 只加载其 exact outcome binding。多个策略包独立运行时分别使用各自的 M3/v2 bundle；未训练 M3/M5B 的 package 返回 typed outcome unavailable，禁止套用当前多 Alpha bundle 或其他包的 calibrator。

### 9.2 字段兼容与语义

现有字段保持 raw 语义，避免静默改变已有 API：

```text
excess_return_q10/q50/q90
positive_probability
signal_survival_probability
path_mfe_q50/q90
path_mae_loss_q50/q90
```

v2 每个 horizon 新增：

```text
excess_return_calibrated_q10/q50/q90
positive_probability_calibrated
signal_survival_probability_calibrated
path_mfe_calibrated_q50/q90
path_mae_loss_calibrated_q50/q90
positive_probability_calibration_state
signal_survival_probability_calibration_state
return_interval_calibration_state
path_mfe_calibration_state
path_mae_loss_calibration_state
```

envelope 新增：

```text
calibration_policy_version
parent_outcome_bundle_id
binary_calibration_state
return_interval_calibration_state
path_upper_calibration_state
holding_calibration_state
```

v1 不伪造 calibrated 字段；其新字段为 `null`/`UNCALIBRATED`。v2 某个 binary head 因合法类别缺失时，对应 calibrated probability 为 `null`，raw probability 保留，不能静默套用 raw 值冒充 calibrated。

### 9.3 UI

Advisory outcome panel：

- v2 主展示显式 calibrated probability 和 calibrated interval，并标注“校准”；raw 值通过同表次级文本展示，不能隐藏。
- v1 继续展示 raw 值并标注 `UNCALIBRATED`。
- 某 head `UNCALIBRATED` 时显示 raw 值和状态，calibrated 位置显示 `-`，不显示 0。
- holding 区域单独显示 `UNCALIBRATED`，不继承外层 `PARTIAL`。
- 文案保持“历史样本校准的研究预测”，不表述为保证收益、止盈止损或交易指令。

## 10. Scope / 实施文件范围

计划新增：

```text
backend/services/advisory_model_first/outcome_calibration_contracts.py
backend/services/advisory_model_first/outcome_calibration.py
backend/services/advisory_model_first/outcome_calibration_bundle.py
backend/services/advisory_model_first/outcome_calibration_pipeline.py
scripts/advisory_outcome_calibration_prepare_request.py
scripts/advisory_outcome_calibration_train_wsl.py
scripts/wsl/advisory_outcome_calibration_train.py
backend/tests/advisory_model_first/test_outcome_calibration_*.py
```

计划修改：

```text
backend/services/advisory_model_first/outcome_bundle.py
backend/services/advisory_model_first/outcome_runtime_bundle.py
backend/services/advisory_model_first/outcome_inference.py
backend/services/advisory_model_first/model_inference.py
backend/tests/advisory_model_first/test_outcome_bundle.py
backend/tests/advisory_model_first/test_outcome_runtime_bundle.py
backend/tests/advisory_model_first/test_outcome_inference.py
backend/tests/advisory_model_first/test_model_inference.py
frontend/src/lib/api/advisory.ts
frontend/src/app/paper-v2/advisory/page.tsx
frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts
本设计与父蓝图验收状态
```

禁止写入 `selection_center`、`simulation_runtime`、`strategy_package`、`quantevolver`、`paper_trading`、数据库 migration 或调度器。若实现发现必须修改上述受保护模块，应停止并修订设计，不得顺手扩大范围。

## 11. Implementation Plan / 执行顺序

1. 实现 request、validation projection、Platt/CQR/upper conformal 和纯函数测试。
2. 实现 v2 自包含 bundle 原子发布、tamper/readback 和 exact retry。
3. validation fit stage 独立写出并读回 frozen calibration spec；随后 test evaluation stage 才能读取 test projection。
4. 使用合入后的固定源码 worktree，在 WSL `rdagent-gpu` 对现有 M3 bundle 执行一次真实 calibration pipeline。
5. 如实记录 validation/test raw 与 calibrated metrics；不得根据 test 结果调整算法。
6. 实现 v1/v2 runtime loader、inference、API 和 UI。
7. 源码合入、outcome binding 激活、用户执行后端重启、deployed API/UI readback 分开执行和报告。

训练/校准命令不得启动或停止后端。真实校准预计只重放冻结 feature/label 并加载 46 个小模型，目标为分钟级、RSS 小于 8 GiB；若超出则登记性能 BUG，不用缩样本或少校准 head 伪装完成。

## 12. 错误合同与日志

新增 reason codes：

```text
ADVISORY_OUTCOME_CALIBRATION_REQUEST_INVALID
ADVISORY_OUTCOME_CALIBRATION_PARENT_MISMATCH
ADVISORY_OUTCOME_CALIBRATION_CLASS_VARIATION_MISSING
ADVISORY_OUTCOME_CALIBRATION_FAILED
ADVISORY_OUTCOME_CALIBRATION_BUNDLE_INVALID
```

日志只记录 request、stage、head/horizon、row counts、wall time、RSS、reason code 和非敏感 identity。禁止记录完整 feature rows、完整路径凭据或无价值逐行日志。未知异常保留 traceback；API 使用 typed outcome unavailable 隔离错误，不返回空成功。

## 13. Verification Plan / 测试与真实验收

### 13.1 后端

- request canonical identity、tamper、path containment、exact retry。
- fit API 只能接收 validation projection；故意混入 train/test 必须失败。
- 10 个 binary head 的 raw-margin Platt 参数、概率范围、raw/calibrated 同时保留。
- validation 单类时逐 head `UNCALIBRATED`；optimizer 错误则整体 typed failure。
- return CQR finite-sample index、非负 delta、单调性和无 test-label 访问。
- MFE/MAE q90 单侧 90% adjustment；不得新增伪 q10。
- 46 个父模型文件字节一致、v2 bundle 原子发布、manifest/spec/hash/readback/tamper。
- v1 loader/inference 完整回归；v2 calibrated/raw projection；holding 明确 uncalibrated。
- outcome panel failure 不改变 M2/M5A rank 或规则荐股结果。

### 13.2 前端

- v1 显示 raw + `UNCALIBRATED`。
- v2 同时显示 calibrated 主值和 raw 次值。
- partial binary head calibrated 为 `-` 而非 raw/0。
- holding 独立 `UNCALIBRATED`。
- 375x812、768x1024、1440x900 无溢出、遮挡、console error 或失败请求。

### 13.3 真实 WSL receipt

真实 receipt 必须记录：

- exact parent/request/bundle/commit/environment identity。
- validation/test row counts 和五个 horizons。
- 10 个 binary head 的 raw/calibrated AUC、Brier、logloss、10-bin ECE。
- 5 个 return interval 的 raw/calibrated 80% coverage 和 width。
- 10 个 MFE/MAE upper interval 的 raw/calibrated 90% coverage。
- calibration states、wall time、peak RSS、v2 bundle ID、exact retry identity。
- `outcome_binding_activated=false`。

指标差不阻止研究 bundle 发布，也不能描述为校准成功改善；算法执行和指标方向必须分别报告。

## 14. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-372 | M5B 只复用冻结 M3 bundle/features/labels/split，不重训 base heads、不读数据库或重建历史数据 |
| F-373 | 10 个 binary head 独立使用 validation raw margin 拟合 Platt；缺类逐 head uncalibrated，技术失败不静默 |
| F-374 | raw/calibrated 概率分别报告 ROC-AUC、Brier、logloss 和固定 10-bin ECE，test 不参与拟合或选择 |
| F-375 | 五个 return q10/q90 使用 validation-only 中央 80% 非负 conformal expansion，raw/calibrated 同时保留 |
| F-376 | MFE/MAE 只对既有 q90 做单侧 90% conformal expansion，不伪造 q10 或改变 q50 语义 |
| F-377 | holding multiclass 不校准并独立标记；bundle 外层为 PARTIAL，不冒充全量 calibrated |
| F-378 | v2 bundle 自包含且原子发布，46 个 base model 文件与父 M3 字节一致，v1 bundle 不改写 |
| F-379 | v1/v2 loader、API/UI 显式区分 raw 与 calibrated，旧字段语义不静默变化 |
| F-380 | M5B 错误只隔离 outcome panel，不改变 M2/M5A、规则荐股、单/多 Alpha 独立执行 |
| F-381 | 训练/校准只在 WSL rdagent-gpu，真实全量运行、分钟级目标、RSS<8 GiB，无缩样本简化 |
| F-382 | Selection、Simulation、StrategyPackage、QE、RD-Agent、DB 零写入且无反向依赖 |
| F-383 | 无审批、角色、二次准入、收益阈值或未经确认门禁；merge/binding/restart/readback 分开报告 |
| F-384 | DESIGN-COMPLIANCE-001 四项逐项有直接测试或审计证据，F2 validator 对最终文档和最终 HEAD 通过 |

## 15. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-372 | `backend/services/advisory_model_first/outcome_calibration_contracts.py`; `outcome_calibration_pipeline.py` | `backend/tests/advisory_model_first/test_outcome_calibration_pipeline.py` | design_ready | none |
| F-373 | `backend/services/advisory_model_first/outcome_calibration.py` Platt per head | `backend/tests/advisory_model_first/test_outcome_calibration.py` plus artifact: real validation counts in §2.2 | design_ready | none |
| F-374 | `backend/services/advisory_model_first/outcome_calibration.py` metrics | `backend/tests/advisory_model_first/test_outcome_calibration_metrics.py`; artifact: real WSL calibration receipt | design_ready | none |
| F-375 | `backend/services/advisory_model_first/outcome_calibration.py` return CQR | `backend/tests/advisory_model_first/test_outcome_calibration.py` | design_ready | none |
| F-376 | `backend/services/advisory_model_first/outcome_calibration.py` path upper adjustment | `backend/tests/advisory_model_first/test_outcome_calibration.py` | design_ready | none |
| F-377 | `backend/services/advisory_model_first/outcome_calibration_bundle.py`; `outcome_inference.py` | `backend/tests/advisory_model_first/test_outcome_calibration_bundle.py`; `test_outcome_inference.py` | design_ready | none |
| F-378 | `backend/services/advisory_model_first/outcome_calibration_bundle.py` | `backend/tests/advisory_model_first/test_outcome_calibration_bundle.py` | design_ready | none |
| F-379 | `backend/services/advisory_model_first/outcome_runtime_bundle.py`; `frontend/src/lib/api/advisory.ts` | `backend/tests/advisory_model_first/test_outcome_runtime_bundle.py`; `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | design_ready | none |
| F-380 | `backend/services/advisory_model_first/model_inference.py` outcome isolation only | `backend/tests/advisory_model_first/test_model_inference.py` | design_ready | none |
| F-381 | `scripts/advisory_outcome_calibration_train_wsl.py`; `scripts/wsl/advisory_outcome_calibration_train.py` | `backend/tests/advisory_model_first/test_outcome_calibration_pipeline.py`; artifact: real full M3 calibration receipt | design_ready | none |
| F-382 | protected-module changed-file scan | `backend/tests/advisory_model_first/test_outcome_calibration_boundaries.py`; `python scripts/aistock_module_ownership_scan.py --changed-only --fail-on-unmapped --fail-on-ambiguous` | design_ready | none |
| F-383 | no approval/admission implementation | `backend/tests/advisory_model_first/test_outcome_calibration_boundaries.py` | design_ready | none |
| F-384 | final DESIGN-COMPLIANCE-001 + F2 validation | `backend/tests/advisory_model_first/test_outcome_calibration_boundaries.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_model_first_m5b_outcome_calibration_f2_design_20260812.md --tier F2` | design_ready | none |

## 16. DESIGN-COMPLIANCE-001

1. **禁止简化版**：必须交付 10 个 binary calibrator、5 个 return interval adjustment、10 个 path upper adjustment、完整 v2 bundle、runtime/API/UI；不得只交一个 horizon、离线 JSON 或 mock。
2. **禁止静默错误**：单类与技术失败分开；raw/calibrated/state 明确；缺失、tamper、非有限值、unknown schema 均 typed failure，不返回空成功或伪校准值。
3. **禁止业务语义偏移**：旧字段保持 raw；M5B 不改候选、排名、Top5、持股、M4、Selection、模拟盘或 QE；校准不描述为提高 AUC 或保证收益。
4. **禁止未经确认门禁/审批**：没有角色、审批、二次准入或质量阈值。身份、时间隔离和原子 readback 是数据正确性合同，不是人工审批；指标只报告不阻断研究 bundle。

## 17. Risks / 风险与处置

| 风险 | 处置 |
|---|---|
| Platt 改善 Brier 但不改善 AUC | AUC 与 calibration metrics 分开报告；不得声称排序提升 |
| validation 样本量有限导致 test calibration 退化 | 冻结算法并如实报告 test；禁止基于 test 再调参或设置质量门禁 |
| conformal expansion 使区间过宽 | 同时报 coverage 和 width；不根据 test 缩小 delta |
| 将 MFE/MAE q50/q90 误当中央 80% 区间 | 只实现 q90 单侧 90% coverage，测试固定合同 |
| v2 改变旧 API 字段含义 | 旧字段保留 raw，新 calibrated 字段显式新增，v1 全量回归 |
| 外层 PARTIAL 被误读为 holding 已校准 | family/head state 独立返回，holding UI 单独显示 `UNCALIBRATED` |
| outcome 校准异常影响主荐股 | outcome panel typed isolation 和 rank/rule invariance 测试 |
| 实现扩展到历史平台或共享链路 | 文件范围与 protected-module scan；发现依赖时停止修订设计 |

## 18. Production Gates / Production Impact

```text
production_ddl_gate = noop
production_dml_gate = noop
production_backend_dependency_gate = noop
production_frontend_dependency_gate = noop
runtime_activation = exact outcome v2 binding，需与源码合入分开执行
backend_restart = user-owned，仅在 runtime 源码合入且 binding 激活后需要
```

校准训练和 bundle 发布不需要后端重启。

## 19. Rollout / Rollback

发布顺序固定为源码合入、真实 WSL bundle、独立 binding 激活、用户重启、deployed readback；每一步分别报告，前一步不代表后一步完成。回滚只恢复原 v1 outcome exact binding；不删除 v2 bundle，不修改数据库，不回滚 M2/M5A/M4，不触碰荐股历史任务。v2 不可用时 outcome panel typed unavailable，主荐股流程继续运行。

## 20. 正式设计审核结论

2026-08-12 已按最终正文完成正式审核并将问题回写对应章节：

| 审核项 | 结论 | 直接依据 |
|---|---|---|
| 父蓝图与 M5A/M3 前后一致 | PASS | M5B 仅处理 M3 probability/quantile calibration，M5C 与 holding 明确排除 |
| 真实输入与算法可实现 | PASS | M3 bundle/feature/label/split 已读回；10 个 binary validation head 均有双边样本；WSL 依赖存在 |
| test leakage | PASS | validation-only fit projection、spec 先冻结 readback、test evaluator 后读取的硬边界已进入 §§4、11 |
| raw/calibrated API 语义 | PASS | 旧字段保持 raw，新字段显式 calibrated；逐 family/head state 不用外层状态冒充 |
| 单/多 Alpha 与包隔离 | PASS | exact package/manifest/style binding，禁止跨 package 套用 bundle/calibrator |
| 简化版与静默错误 | PASS | 25 个校准单元、v2 bundle、runtime/API/UI 均为完整范围；单类与技术失败区分，缺值不回退 |
| 业务逻辑和模块隔离 | PASS | 不改候选、排名、Top5、M4、Selection、Simulation、StrategyPackage、QE、RD-Agent 或 DB |
| 未经确认门禁/审批 | PASS | 无角色、审批、二次准入或收益阈值；metrics 只报告，不阻断研究 bundle |
| F2 文档合同 | PASS | `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_model_first_m5b_outcome_calibration_f2_design_20260812.md --tier F2`：13 rows，0 warnings |

审核未发现剩余设计阻断。该结论只表示详细设计可进入实现，不代表源码、真实校准 bundle、binding、重启或 deployed readback 已完成。
