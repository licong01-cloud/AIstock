# Advisory Model-First M3 预期收益与持股周期详细设计

> 日期：2026-08-09
> Feature tier：F2
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 当前阶段：`COMPLETED_RUNTIME_VERIFIED`
> 适用范围：学术研究与历史回测参考，不构成实时投资建议或交易执行

## 1. Background / 背景

M2 已在 merge commit `41504a205b9372a4e709587dc2310fd8143c6c6d` 完成部署后只读验收。目标多 Alpha Program 在真实数据库 decision cutoff 输入上返回 20 个候选和真实 Top5；单 Alpha Program 无匹配 bundle 时返回 `ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`，未误用父包模型。

过去的主要偏移是把历史证据与通用平台建设放在真实模型之前。本阶段继续执行模型优先路线，直接实现真实收益、风险和周期预测，不插入历史证据、归档、通用 ModelOps 或遗留数据处理。

## 2. Scope / 范围

M3 交付顺序：

1. **M3A**：复用 M1 的 runtime-equivalent candidates 与共享 103 特征，在 WSL 训练真实 outcome bundle。
2. **M3B**：把 outcome bundle 接入 M2 的只读数据库推理、API 和 Advisory 页面。

M3A 与 M3B 属于同一完整 M3 设计。M3A 完成时只报告训练完成，不得冒充 M3 页面功能完成。

## 3. Non-goals / 非目标

本阶段不处理 Historical Range、Phase 1R、SEALED/CAS、旧 batch、旧模型注册中心、自动训练调度、概率校准、盘中分钟路径或实盘交易。M4 价格范围和 M5 质量迭代不得提前混入 M3A。

## 4. 不变业务边界

- 训练基础行情只读取既有 QE H5/Parquet/Qlib 日线 Bin、suspend sidecar 和 M1 产生的候选/特征 Parquet。
- 训练不读取生产 PostgreSQL 历史行情，不读取 Paper、模拟盘、回测收益、持仓或交易结果。
- 正式预测只读取 Program/ReviewRun/Selection 与数据库 decision-cutoff 行情，不读取历史训练 PKL 代替当前行情。
- 所有训练只在 WSL Conda `rdagent-gpu` 执行；Windows 只生成请求、启动 WSL 和读取结果。
- 单进程 RSS 必须低于 8GB；不得建设 SQLite 缓存、历史证据库、SEALED/CAS 或通用训练平台。
- 不修改 Selection、StrategyPackage、Paper、模拟盘或 QE 业务逻辑，不做策略包二次准入。
- 不新增角色、审批、收益门槛、人工 ACK、发布审批或未经确认的业务门禁。
- M3 输出保持 `EXPERIMENTAL_SHADOW / UNCALIBRATED`，概率和分位数是模型研究结果，不表述为保证收益。

## 5. Architecture / 架构

```text
M1 candidates/features Parquet + QE daily Bin/suspend
  -> M3 outcome labels
  -> purged train/validation/test matrix
  -> quantile/binary/multiclass LightGBM heads
  -> atomic OutcomeBundle
  -> M3B database FeatureSource
  -> Advisory API/UI outcome panel
```

训练与预测继续共用 `AdvisoryFeatureSchemaV1`；M3A 只新增 outcome label、trainer 和 bundle，不复制 M2 FeatureBuilder。M3B 只在 Advisory 消费层加载 outcome bundle，不反向修改候选产生链路。

## 6. Contracts / 精确输入身份

M3A request 固定：

```text
parent_request_id = advmreq_ac5959aa8dc14a25e3b8c139
parent_bundle_id = 9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629
package_id = pkg_ma_8ec5e389fa2c5e484a1ac7e9
manifest_sha256 = f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016
style_profile_id = short_rebound_pkg_ma_8ec5e389_v1
feature_schema_version = advisory_feature_schema_v1
feature_count = 103
candidate_semantics = OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2
decision_range = 2024-07-04..2026-03-10
data_cutoff = 2026-06-30
```

直接训练文件：

- `runs/advmreq_ac5959aa8dc14a25e3b8c139/candidates.parquet`
- `runs/advmreq_ac5959aa8dc14a25e3b8c139/features.parquet`
- parent bundle 内 `training_request.json` 和 `feature_schema.json`
- parent request 已绑定的 Qlib daily root、factor root 与 suspend root

M3 request 保存上述文件的 SHA256、row count、列集合、父 request SHA 和父 bundle identity。它只保证本次真实模型输入不被替换，不形成历史证据平台或人工审批。

## 7. Contracts / Outcome 标签合同

### 4.1 时间语义

固定 horizons：

```text
H = [1, 3, 5, 10, 20] trading days
decision = d
entry = next trading day open
nominal exit = d + h trading days close
latest executable exit = nominal exit + 5 trading days
```

- entry 当日停牌或一字涨停：该候选所有 horizon 为 `NO_EXECUTABLE_ENTRY`。
- nominal exit 不可执行时，最多向后寻找 5 个交易日；股票、benchmark、MFE/MAE 全部使用同一实际退出日。
- 一字跌停或停牌日不能作为实际退出日。
- 数据不足或路径缺口显式标记 `LABEL_NOT_MATURE` / `RIGHT_CENSORED_EXIT`，不得填 0 或缩短 horizon。
- 股票净收益包含现有 `OPEN_COST=0.000095` 与 `CLOSE_COST=0.000595`；benchmark 使用同期 open-to-close，不重复扣股票成本。

### 4.2 每个 horizon 输出

```text
actual_exit_date_h
actual_holding_trading_days_h
stock_net_return_h
benchmark_return_h
excess_return_h
path_mfe_h
path_mae_loss_h
utility_h = excess_return_h + 0.25*path_mfe_h - 0.50*path_mae_loss_h
positive_excess_h = 1[excess_return_h > 0]
signal_survival_h = 1[utility_h > 0]
label_status_h / label_reason_h
```

### 4.3 持股周期目标

对五个 horizon 均成熟的候选：

```text
optimal_holding_bucket = argmax_h utility_h
```

完全相同的 utility 选择更短 horizon。该目标表示本次标签合同下的相对最优离散周期，不表示真实交易必须在该日退出。

## 8. 时间切分与泄漏控制

仍使用全部 406 个 decision dates，但 M3 最长 20 日 horizon 加最多 5 日延迟退出，因此采用：

```text
train = first 226 decision dates
purge_1 = next 25 dates
validation = next 50 dates
purge_2 = next 25 dates
test = final 80 dates
```

- 同一 decision date 的 20 个候选不得跨 split。
- train/validation 的实际 exit 不得越过各自后续 purge 末端。
- test 的实际 exit 不得晚于 `data_cutoff=2026-06-30`。
- vocabulary、类别编码、模型选择和 early stopping 只使用 train/validation；test 不参与选择。

## 9. 真实模型头

所有模型共享 `AdvisoryFeatureSchemaV1` 的 103 个输入特征和 train-only 行业 vocabulary。

### 6.1 收益分位数

对每个 `h in [1,3,5,10,20]` 训练三个 LightGBM quantile regressor：

```text
q10 / q50 / q90 of excess_return_h
```

输出必须单调化为 `q10<=q50<=q90`；保留原始预测用于诊断，不能静默隐藏 crossing 数量。

### 6.2 正收益与信号存活概率

对每个 horizon 训练两个独立 binary classifier：

- `positive_probability_h`：目标 `positive_excess_h`。
- `signal_survival_probability_h`：目标 `signal_survival_h`。

第一版不做概率校准，API/UI 明确 `UNCALIBRATED`。类别无双边样本时该 head typed failure，不用常数概率代替。

### 6.3 MFE/MAE 分位数

对每个 horizon 训练：

- `path_mfe_q50_h / path_mfe_q90_h`
- `path_mae_loss_q50_h / path_mae_loss_q90_h`

预测下界裁剪为 0，并记录裁剪前负值数量。该结果为 M4 日线价格范围的模型输入，不提前生成价格建议。

### 6.4 持股周期分类器

训练一个五分类 LightGBM 模型，类别顺序固定 `[1,3,5,10,20]`。输出：

- 每个 bucket 的概率。
- `holding_mode_days`：最大概率 bucket。
- `holding_range_low_days/high_days`：预测分布累计概率的 20% 与 80% bucket。

范围只描述模型不确定性，不是固定卖出日。

## 10. 评价与真实结果状态

test 输出逐候选非空预测，并计算：

- quantile pinball loss、q10/q90 empirical interval coverage、crossing count。
- binary logloss、Brier、ROC-AUC（仅类别双边存在时）。
- holding multiclass logloss、accuracy、bucket day MAE、range coverage。
- MFE/MAE pinball loss。
- 按原 Selection Top5 与 M2 model Top5 分组的 outcome prediction 摘要。

指标不作为模型训练或页面展示的隐藏门禁。结果差时仍如实发布 `EXPERIMENTAL_SHADOW` bundle 和指标，不描述为已优化。

## 11. Contracts / Outcome Bundle

独立原子目录：

```text
outcome_bundles/<outcome_bundle_id>/
```

必含：

```text
manifest.json
training_request.json
feature_schema.json
label_policy.json
split.json
models/*.txt
metrics.json
test_predictions.parquet
training_log.json
```

`outcome_bundle_id` 是除自身 ID 外 manifest 的 canonical SHA256。manifest 绑定 parent request/bundle、package/manifest/style、feature schema、label policy、模型文件 hash/size 和训练环境。M3A 不覆盖 M1 exact binding，不自动激活 outcome bundle。

## 12. Contracts / M3B API/UI

M3B 在 M3A 真实 bundle 生成后实现：

- M2 model-shadow 服务使用同一实时 feature matrix 加载 exact outcome bundle。
- API 每个候选返回五期限收益区间、正收益概率、信号存活概率、MFE/MAE 和持股范围。
- 页面在真实 Top5 与全部候选中展示 `EXPERIMENTAL_SHADOW / UNCALIBRATED`，不覆盖规则列表。
- 缺 outcome bundle、输入或 head 时只使 outcome panel typed unavailable；M2 rank 和规则荐股继续工作。
- 单 Alpha 无匹配 outcome bundle 时仍不读取市场特征、不套用父包模型。

## 13. Contracts / 错误合同

新增 reason：

```text
ADVISORY_OUTCOME_REQUEST_INVALID
ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH
ADVISORY_OUTCOME_LABEL_NOT_MATURE
ADVISORY_OUTCOME_CLASS_VARIATION_MISSING
ADVISORY_OUTCOME_TRAINING_FAILED
ADVISORY_OUTCOME_BUNDLE_INVALID
ADVISORY_OUTCOME_BUNDLE_NOT_AVAILABLE
ADVISORY_OUTCOME_INFERENCE_FAILED
```

所有错误输出 head/horizon、阶段和有效上下文；未知异常保留堆栈。禁止返回空成功、常数模型、旧模型回退或省略失败 head。

## 14. Implementation Plan / 实施方案

1. 更新父蓝图 M2 完成状态并冻结本设计。
2. 实现 M3 request、multi-horizon labels、25 日 purge split 和 outcome trainers。
3. 实现 atomic outcome bundle、Windows launcher 和 WSL entrypoint。
4. 提交 M3A 源码后，在该提交对应的 WSL worktree 运行真实训练并读回 bundle。
5. 根据真实训练结果更新验收矩阵，不用指标好坏阻断实验 bundle。
6. 继续 M3B loader、实时推理、API/UI 和 deployed readback；不得插入其它任务。

## 15. Verification Plan / 验证方案

- label 单元测试覆盖 entry、1/3/5/10/20 日退出、延迟退出、停牌、一字涨跌停、benchmark、MFE/MAE和成熟度。
- split 测试覆盖 226/25/50/25/80、同日不跨 split 和 actual exit 边界。
- trainer 测试使用真实 LightGBM 小矩阵，验证所有 head 文件、非空预测、类别缺失 typed failure 和指标公式。
- bundle 测试覆盖原子发布、canonical identity、文件 hash、路径 containment、tamper 和 readback。
- WSL receipt 验证 Conda identity、repository commit、RSS、wall time、所有 head 和 80 日 test 非空。
- M3B 测试覆盖 exact bundle、实时 schema、错误隔离、单/多 Alpha Program 和三视口页面。

## 16. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-323 | M2 部署后真实多 Alpha API、单 Alpha隔离和运行时 commit 验证完成，M2 状态与蓝图同步 |
| F-324 | M3 只使用既有 QE 文件、M1 candidates/features 和父 bundle，不读取生产数据库历史训练数据 |
| F-325 | M3 request 精确绑定父 request/bundle、文件 hash、schema、代码 commit 和 WSL 路径 |
| F-326 | 1/3/5/10/20 日 entry/exit、成本、benchmark、涨跌停、停牌和延迟退出标签完整实现 |
| F-327 | 25/50/25 purge split 覆盖最长 horizon+延迟窗口，同日不跨 split，test 不参与选择 |
| F-328 | 五期限 q10/q50/q90 真实 LightGBM quantile heads 非空训练和预测 |
| F-329 | 五期限正收益和 signal survival binary heads 真实训练，类别缺失 typed failure |
| F-330 | 五期限 MFE/MAE q50/q90 真实模型非空训练和预测 |
| F-331 | 五分类持股周期模型输出 bucket 概率、mode 和 20%-80%范围 |
| F-332 | test 指标完整，结果差不静默、不阻断实验 bundle 输出、不冒充校准 |
| F-333 | outcome bundle 原子发布、文件 hash/readback 和 parent identity 完整 |
| F-334 | 训练仅在 WSL rdagent-gpu，峰值 RSS<8GB，目标小时级，不建设新缓存平台 |
| F-335 | M3B 使用数据库实时特征接入同一 API/UI，训练文件不进入正式预测行情输入 |
| F-336 | outcome unavailable 不阻断 M2 rank 或规则荐股，不跨 Program/包套用模型 |
| F-337 | Selection、Paper、模拟盘、StrategyPackage、QE 业务逻辑零写入和零反向依赖 |
| F-338 | 无简化版、mock-only、placeholder、常数模型、静默 fallback 或业务语义偏移 |
| F-339 | 无新增角色、审批、二次准入、收益门槛或未经确认的门禁 |
| F-340 | 无 DDL/DML；模型文件、源码合入、依赖、重启、binding激活和运行时验证分开报告并完成 deployed readback |

## 17. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-323 | blueprint M2 ledger | `backend/tests/advisory_model_first/test_model_inference.py`; deployed `/api/v1/runtime-identity` and model-shadow receipts | verified | none |
| F-324 | M3 request/pipeline uses only frozen M1 artifacts and QE files | `backend/tests/advisory_model_first/test_outcome_pipeline.py`; request `advoutreq_d16081c54d47b3602c89e3b2` | verified | none |
| F-325 | exact parent bundle/request/schema/artifact/commit binding | `backend/tests/advisory_model_first/test_outcome_contracts.py`; parent bundle full readback | verified | none |
| F-326 | `outcome_labels.py` five-horizon executable labels | `backend/tests/advisory_model_first/test_outcome_labels.py`; 8120 real labels | verified | none |
| F-327 | `outcome_split.py` 226/25/50/25/80 and exit boundaries | `backend/tests/advisory_model_first/test_outcome_split.py`; 80-day test receipt | verified | none |
| F-328 | 15 real excess-return quantile models | `backend/tests/advisory_model_first/test_outcome_training.py`; `F:/Dev/AIstock_model_artifacts/advisory_model_first/outcome_bundles/17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0/models` | verified | none |
| F-329 | 10 real positive/survival binary models | `backend/tests/advisory_model_first/test_outcome_training.py`; bundle `models` artifact above | verified | none |
| F-330 | 20 real MFE/MAE quantile models | `backend/tests/advisory_model_first/test_outcome_training.py`; `outcome_bundles/17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0/test_predictions.parquet` | verified | none |
| F-331 | real five-class holding model and probability range | `backend/tests/advisory_model_first/test_outcome_training.py`; `outcome_bundles/17ce7ceb429829f15b68b196ad76ffee08d45f93b0a72d0f2fb92e72515adba0/test_predictions.parquet` | verified | none |
| F-332 | full test metrics and honest `UNCALIBRATED` status | `backend/tests/advisory_model_first/test_outcome_training.py`; external `metrics.json` in bundle `17ce7ceb...` | verified | none |
| F-333 | atomic outcome bundle and semantic/hash readback | `backend/tests/advisory_model_first/test_outcome_bundle.py`; bundle `17ce7ceb...` | verified | none |
| F-334 | WSL `rdagent-gpu`, 108 seconds, peak RSS 655581184 bytes | `backend/tests/advisory_model_first/test_outcome_pipeline.py`; external `outcome_training_receipt.json` for `advoutreq_d16081c54d47b3602c89e3b2` | verified | none |
| F-335 | exact outcome loader, same realtime feature matrix, API/UI five-horizon projection | `backend/tests/advisory_model_first/test_outcome_runtime_bundle.py`; `backend/tests/advisory_model_first/test_outcome_inference.py`; `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | verified | none |
| F-336 | outcome typed unavailable preserves M2 ranking and persisted rule list | `backend/tests/advisory_model_first/test_model_inference.py`; Playwright outcome isolation case | verified | none |
| F-337 | Advisory-only changed-file review | `backend/tests/advisory_model_first/test_outcome_boundaries.py`; ownership scan | verified | none |
| F-338 | DESIGN-COMPLIANCE-001 review | `backend/tests/advisory_model_first/test_outcome_boundaries.py`; `nox -s advisory_modeling_backend`; real 46-head smoke | verified | none |
| F-339 | no role/approval/admission gate review | `backend/tests/advisory_model_first/test_outcome_boundaries.py`; source scan | verified | none |
| F-340 | no DDL/DML/dependency change; merge/binding/restart/readback reported separately | PR #3234; outcome binding; runtime commit `0ab6dec3...`; deployed model-shadow receipt | verified | none |

## 18. Rollout / Rollback

- M3A 训练只新增独立 outcome bundle，不修改已激活 M1 bundle 或 binding；训练失败时保留 typed receipt，不发布不完整目录。
- M3B 源码合入、outcome binding、用户重启和 deployed readback 已分开执行并分别记录。
- outcome 异常时移除或不配置 outcome binding，只关闭 M3 panel；M2 rank 和规则荐股继续运行。
- rollback 不删除训练产物、不修改数据库、不回滚 Selection/Program；只停止加载指定 outcome bundle。

## 19. Risks / 风险

| 风险 | 处置 |
|---|---|
| 20 日标签跨 split | 25 日 purge + actual exit 边界测试，违规样本显式排除 |
| 某 horizon 类别单一 | typed `ADVISORY_OUTCOME_CLASS_VARIATION_MISSING`，不训练常数模型 |
| quantile crossing | 输出单调化结果并记录原始 crossing 数量，不隐藏问题 |
| MFE/MAE 负预测 | 下界裁剪为 0 并记录裁剪数量 |
| 模型质量差 | 如实保留指标和 `UNCALIBRATED`，不变成隐藏收益门禁 |
| 内存或耗时过高 | 列投影、按候选读取、复用 features Parquet；不建设新平台 |
| outcome 影响现有荐股 | Advisory-only loader/envelope，错误隔离测试，protected-module scan |

## 20. Production Gates / 生产门禁

```text
production_ddl_gate = noop
production_dml_gate = noop
production_backend_dependency_gate = noop unless implementation changes dependency manifests
production_frontend_dependency_gate = noop unless M3B changes frontend dependencies
backend_restart = user-owned and only relevant after M3B source merge
runtime_activation = exact outcome binding, separate from source merge and training
```

## 21. Deployed Runtime Acceptance / 部署后验收

- Source：PR #3234，merge commit `84362027da8f6e87ec5b627a5b7df15b88c5763b`。
- Runtime：`GET /api/v1/runtime-identity` 返回 `0ab6dec36c6bc05f7d9655de63b07bbd5353dfd2`，包含 M3 merge。
- Environment：backend 进程由 `C:/Users/lc999/miniconda3/envs/AIstock/python.exe` 运行，LightGBM 可加载。
- Binding：父 bundle `9cf14e80...` 与 outcome bundle `17ce7ceb...` 的 exact binding 均存在。
- Business readback：目标 Program `advp_3126...` 在 `target_trade_date=2026-07-16` 返回 `EXPERIMENTAL_SHADOW`，20 个 M2 候选和 20 个 M3 outcome 候选完整对齐；horizons=`1,3,5,10,20`，父/子 reason code 均为空。
- Performance：本次真实 HTTP 推理耗时 33.236 秒；该结果是研究影子输出，不构成交易建议或执行输入。
