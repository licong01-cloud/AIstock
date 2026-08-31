# Advisory Model-First M5C Entry-Gap Coverage Calibration F2 详细设计

> 日期：2026-08-12  
> Feature tier：F2  
> 状态：IMPLEMENTED_REAL_WSL_VERIFIED_NOT_ACTIVATED
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`  
> 前序设计：`docs/architecture/advisory_model_first_m4_price_ranges_f2_design_20260810.md`、`docs/architecture/advisory_model_first_m5b_outcome_calibration_f2_design_20260812.md`

## 1. Background / 背景

M4 已在 WSL `rdagent-gpu` 训练四个真实 LightGBM heads，并通过 exact binding 接入 Advisory model-shadow。父 request 为 `advprreq_2d826a7b2704137bf3a60d9d`，父 bundle 为 `1a939f05a3410ce56d66f68245a77e9454be8bf38afe57d57330341c41c742c3`。冻结 test 的 entry-gap q10-q90 empirical coverage 为 `0.72795`，低于名义中央 80%。

M5C 只修正这个已经观察到的真实 coverage 偏差。它不重训 M4 base heads，不处理只有 4 条权威负例的 `entry_executable` binary，不改变买入、止盈、止损或法规边界算法。校准输出继续属于 `EXPERIMENTAL_SHADOW` 学术研究结果，不构成收益保证、实时投资建议或交易执行输入。

真实执行使用 request `advprcal_7cb766fe38898e12a008a328`，在 WSL `rdagent-gpu` 对父 M4 的完整冻结输入重放。父 M4 正式合同中的 validation 标签有 1000 个 `gap_modelable` 候选，其中 940 行具有冻结 M1 features、60 行无 features；M5C 精确复用同一 feature-covered cohort，并要求缺失计数与父 bundle `metrics.json` 一致。test 的 1599 个 executable rows 全部有 features，仍保持零缺失要求。

validation 原始 coverage 已为 `0.810638`，因此有限样本中央 80% CQR 得到 `delta=0`；冻结 test raw/calibrated coverage 均为 `0.727955`，区间宽度也完全不变。真实 bundle `5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead` 已发布并通过 exact retry，但 `activation_recommended=false`、`price_range_binding_activated=false`。这证明当前全局常数 adjustment 无法修正 validation 到 test 的分布漂移；现行 M4 v1 binding 必须保持不变。

## 2. Scope / 范围

完整 M5C 交付包括：

1. 冻结校准 request，精确绑定父 M4 request/bundle、输入文件、split、package/style/schema 和源码 commit。
2. 使用父 M4 四个模型只读重放 validation/test；validation 拟合 entry-gap q10/q90 中央 80% coverage adjustment，test 只做一次最终评价。
3. 发布自包含、原子、可读回的 PriceRangeBundle v2；四个父模型字节不变，新增 calibration spec、raw/calibrated test predictions 和 metrics。
4. runtime loader/inference/API/UI 同时保留 raw 与 calibrated 字段，exact binding 可选择 v1/v2；未激活时现有 v1 行为完全不变。
5. 在 WSL Conda `rdagent-gpu` 对完整冻结样本执行真实校准和 exact retry，并如实决定是否建议激活。

## 3. Non-goals / 非目标

- 不重训或调参 M4 四个 LightGBM heads，不重建 M1/M3/M4 历史数据。
- 不校准 `entry_executable_probability`；全量仅 4 条权威负例，继续明确 `UNCALIBRATED`。
- 不改变 M2/M5A 排名、Top5、M3 outcome/holding、M4 path range、法规边界或 recommendation lifecycle。
- 不读取分钟 Bin，不建模盘中触达顺序，不输出订单、委托价或自动交易参数。
- 不处理 Historical Range、Phase 1R、旧 batch/root、历史证据固化、归档或遗留状态。
- 不新增数据库表、DDL/DML、通用缓存、SQLite、ModelOps、训练调度或模型注册平台。
- 不修改 Selection、StrategyPackage、Paper、模拟盘、QE 或 RD-Agent。
- 不增加角色、审批、二次策略包准入、收益阈值或任何未经用户确认的门禁。

## 4. Architecture / 架构

```text
frozen M4 request + v1 bundle + features/labels/split
  -> load unchanged four parent models
  -> replay validation raw q10/q50/q90
  -> fit validation-only central-80 CQR delta
  -> freeze/read back calibration spec
  -> replay test once and report raw/calibrated metrics
  -> atomic PriceRangeBundle v2
  -> optional exact binding -> existing model-shadow price-range child envelope
```

校准模块只依赖 Advisory model-first M4 文件合同。在线 loader 由 v1/v2 schema 精确分派；不存在 v2 binding 时不导入或调用 M5C 校准路径。

## 5. Contracts / 权威输入与合同

校准 request 必须绑定：

```text
price_range_request_id = advprreq_2d826a7b2704137bf3a60d9d
price_range_bundle_id = 1a939f05a3410ce56d66f68245a77e9454be8bf38afe57d57330341c41c742c3
package_id = pkg_ma_8ec5e389fa2c5e484a1ac7e9
manifest_sha256 = f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016
style_profile_id = short_rebound_pkg_ma_8ec5e389_v1
feature_schema_version = advisory_feature_schema_v1
entry_gap_condition = ENTRY_EXECUTABLE
nominal_coverage = 0.8
calibration_policy_version = advisory_price_range_calibration_policy_v1
```

request 还绑定父 manifest 文件 hash、training request hash、四个模型文件 hash、M4 candidates/features/labels/split artifact 的路径、SHA256、大小、行数与列集合。路径必须位于显式 artifact/model roots 内。不得扫描 latest、猜测路径或跨 package/style/bundle 套用。

父 M4 bundle 只保存 test predictions，没有 validation predictions。M5C 必须从父 training request 所绑定的 features/labels 和 split 读取完整冻结输入，加载父 bundle 的四个模型，对 validation 和 test 分别重放；禁止重新训练 base heads。

## 6. Time Split And Leakage / 时间切分与防泄漏

M5C 精确复用 M4/M3 的 `226 train / 25 purge / 50 validation / 25 purge / 80 test` decision-date membership。只有 `split=validation` 且 `gap_modelable=true` 的行用于拟合 adjustment。`split=test` 的标签在 calibration spec 原子落盘并读回验证后才能读取评价；train/purged 不参与拟合。

实现必须提供显式 projection 函数，按 identity one-to-one 合并 feature/label，拒绝重复、缺失、未知 split、非有限标签、非 executable 条件行或父模型输出非有限值。测试必须证明把 test 行传入 fit API 会 typed failure，而不是静默过滤。

## 7. Calibration Algorithm / 校准算法

对 validation 每行先对父模型的 q10/q50/q90 raw 输出执行与 M4 相同的单调排序，得到 `l_i <= m_i <= u_i`，真实 label 为 `y_i`。中央 80% nonconformity score：

```text
s_i = max(l_i - y_i, y_i - u_i, 0)
n = validation executable row count
k = min(ceil((n + 1) * 0.8), n)
delta = sorted(scores)[k - 1]
```

`delta` 必须有限且非负。应用规则固定为：

```text
calibrated_q10 = raw_monotonic_q10 - delta
calibrated_q50 = raw_monotonic_q50
calibrated_q90 = raw_monotonic_q90 + delta
```

方法名固定 `CQR_CENTRAL_80_NONNEGATIVE_EXPANSION`。不得使用负 delta 收窄区间，不得移动 q50，不得按 test 重新选择 coverage/method/delta。raw crossing 数量必须保留；单调化与 calibration 分开记录。

## 8. Metrics And Quality Decision / 指标与质量结论

validation 和 test 均报告：raw/calibrated coverage、mean width、median width、coverage absolute error、lower miss rate、upper miss rate、row/date counts。test 只评价一次。

研究 bundle 发布不以指标改善为门禁。是否建议激活只根据冻结 test 的真实报告给出结论，不实现为程序审批或自动发布门禁：

- 明确改善：calibrated coverage absolute error 低于 raw，且没有非有限值、身份错误或区间单调违例，可以报告“建议独立评估激活”。
- 未改善：仍发布完整研究 bundle和 receipt，但 `activation_recommended=false`，现行 v1 binding 不变。

禁止用“pipeline 成功”替代“质量改善”，也禁止围绕冻结 test 再调 delta。

## 9. Bundle V2 / 产物合同

PriceRangeBundle v2 至少包含：

- `manifest.json`
- `training_request.json`（父 M4 原文件，字节一致）
- `calibration_request.json`
- `calibration_spec.json`
- `feature_schema.json`、`label_policy.json`、`split.json`
- `metrics.json`、`training_log.json`
- `test_predictions.parquet`（兼容 raw v1 列）
- `calibrated_test_predictions.parquet`（raw/calibrated/state 同时存在）
- `models/` 下四个父模型文件，SHA256/size 与 v1 完全一致

manifest schema 为 `advisory_price_range_bundle_v2`，保存 parent v1 bundle ID/manifest hash、calibration request/spec identity、package/style/schema、四模型身份、成员 hash 和 `entry_executable_calibration_state=UNCALIBRATED`。bundle ID 为除自身 ID 外 manifest canonical SHA256。

先在同 root 临时目录完整写入并读回，再原子 rename。相同 ID 内容不同、成员缺失、hash/size 不符、父模型字节变化、unknown schema、非有限 delta 或预测列不完整均 typed failure。exact retry 必须返回同一 bundle ID。

## 10. Runtime, API And UI / 在线语义

v1 loader/inference 行为保持原样。v2 exact binding 加载后，price-range 子信封新增：

```text
entry_gap_raw_q10/q50/q90
entry_gap_calibrated_q10/q50/q90
entry_gap_calibration_state = CALIBRATED
entry_gap_calibration_method
entry_gap_calibration_delta
entry_executable_calibration_state = UNCALIBRATED
```

旧 `entry_gap_q10/q50/q90` 和现有买入价格范围字段继续表达 raw v1 语义，禁止静默改写。页面在 v2 下显示 calibrated 研究区间为主值、raw 为对照，并显式标记 `EXPERIMENTAL_SHADOW / CALIBRATED_INTERVAL`；binary 继续标记 `UNCALIBRATED`。v1 下 calibrated 字段为 null，不能复制 raw 或填 0。

校准异常只关闭 M4 price-range 子信封并返回 typed reason，不改变 M2/M5A rank、Top5、M3 outcome 或规则荐股。多个策略包各自 exact binding；未训练 M5C 的包继续使用自身 v1 或 typed unavailable，禁止借用当前多 Alpha bundle。

## 11. Implementation Plan / 执行顺序

1. 新增独立 contracts、calibration、pipeline、bundle v2 与 WSL launcher/worker。
2. 完成算法、request、projection、bundle/readback/tamper 和 exact retry tests。
3. 扩展 price-range runtime loader/inference/API/UI 的 raw/calibrated 双语义及隔离 tests。
4. 运行变更模块及直接依赖测试、F2 validator、ownership、lint/compile 和 `git diff --check`。
5. 在 WSL `rdagent-gpu` 使用完整父 M4 输入执行真实校准和 exact retry，峰值 RSS 必须低于 8 GiB，目标分钟级/小时内完成。
6. 将真实 artifact/metrics/资源/quality decision 回写验收矩阵；不得未经用户确认激活 binding 或重启后端。

## 12. Error Contract And Logging / 错误与日志

新增 reason codes：

```text
ADVISORY_PRICE_RANGE_CALIBRATION_REQUEST_INVALID
ADVISORY_PRICE_RANGE_CALIBRATION_PARENT_MISMATCH
ADVISORY_PRICE_RANGE_CALIBRATION_PROJECTION_INVALID
ADVISORY_PRICE_RANGE_CALIBRATION_FAILED
ADVISORY_PRICE_RANGE_CALIBRATION_BUNDLE_INVALID
```

日志只记录 request/stage、row/date counts、delta、coverage/width、wall time、peak RSS、reason code 和非敏感 identity。未知异常保留 traceback；不得输出完整特征行、凭据、完整 DSN 或无价值逐行日志。任何失败必须显式，禁止返回空成功、默认 delta 或 raw-as-calibrated。

## 13. Verification Plan / 验证计划

- request canonical identity、path containment、parent/file hash、tamper 与 exact retry。
- fit 只接收 validation projection；test/train/purge 混入、空样本、非有限值和 shape mismatch typed failure。
- finite-sample rank、delta 非负、q50 不变、q10/q90 对称扩张、单调性和 raw crossing 可见。
- 父四模型字节一致、v2 原子 publish/readback、成员集合、spec/manifest/hash/tamper。
- validation 重放不访问 test label；spec 冻结并读回后才评价 test。
- v1 runtime 全量回归；v2 raw/calibrated 字段、CNY 投影、法规收紧和 binary uncalibrated 状态。
- price-range failure isolation；Selection/Simulation/StrategyPackage/QE/RD-Agent/protected modules 零写入。
- 前端 desktop/tablet/mobile 无溢出，v1/v2/typed unavailable 状态明确。
- 真实 WSL receipt：完整样本、父/新 bundle、delta、raw/calibrated test metrics、wall time、RSS、exact retry、`price_range_binding_activated=false`。

## 14. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-385 | M5C 只复用冻结 M4 request/bundle/features/labels/split 和四个模型，不重训 base heads、不读数据库历史训练数据 |
| F-386 | validation-only 中央 80% CQR adjustment，finite-sample rank 正确，delta 有限非负，test 不参与拟合或选择 |
| F-387 | q50 保持 raw，q10/q90 对称扩张，raw crossing 与 raw/calibrated coverage/width/miss metrics 全部可见 |
| F-388 | entry_executable binary 因负例不足保持独立 UNCALIBRATED，不训练常数模型、不伪造负例 |
| F-389 | PriceRangeBundle v2 自包含原子发布，父四模型字节一致，v1 不改写，exact retry 确定性一致 |
| F-390 | v1/v2 runtime、API/UI 显式区分 raw 与 calibrated，旧字段语义不静默变化 |
| F-391 | 多策略包 exact binding 独立；无 M5C bundle 的包不借用其它包校准器 |
| F-392 | M5C 错误只隔离 price-range 子信封，不改变候选、排名、Top5、outcome、规则荐股或其它模块 |
| F-393 | 真实完整 WSL rdagent-gpu 校准和 exact retry，RSS<8 GiB，目标小时级，无缩样本/POC/mock 替代 |
| F-394 | 冻结 test 质量结论与执行成功分开；不改善时保留 bundle 但不激活，不围绕 test 调参 |
| F-395 | Selection、Simulation、StrategyPackage、QE、RD-Agent、DB 零写入且无反向依赖 |
| F-396 | 无审批、角色、二次准入、质量硬门禁或未经确认门禁；merge/binding/restart/readback 独立报告 |
| F-397 | DESIGN-COMPLIANCE-001 四项逐项有直接证据，F2 validator 对最终文档和最终 HEAD 通过 |

## 15. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-385 | `backend/services/advisory_model_first/price_range_calibration_contracts.py`; `price_range_calibration_pipeline.py` | `backend/tests/advisory_model_first/test_price_range_calibration_contracts.py`; real request `advprcal_7cb766fe38898e12a008a328` | implemented_verified | none |
| F-386 | `backend/services/advisory_model_first/price_range_calibration.py` | `backend/tests/advisory_model_first/test_price_range_calibration.py`; artifact: `price_range_calibration_runs/advprcal_7cb766fe38898e12a008a328/calibration_spec.json` | implemented_real_verified | none |
| F-387 | `backend/services/advisory_model_first/price_range_calibration.py`; pipeline metrics | `backend/tests/advisory_model_first/test_price_range_calibration.py`; artifact: `price_range_bundles/5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead/metrics.json` | implemented_real_negative_quality_verified | none |
| F-388 | `backend/services/advisory_model_first/price_range_calibration_bundle.py`; runtime state | `backend/tests/advisory_model_first/test_price_range_calibration_bundle.py`; artifact: `price_range_bundles/5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead/manifest.json` | implemented_verified | none |
| F-389 | `backend/services/advisory_model_first/price_range_calibration_bundle.py` | `backend/tests/advisory_model_first/test_price_range_calibration_bundle.py`; artifact: `price_range_bundles/5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead/manifest.json` | implemented_real_verified | none |
| F-390 | `backend/services/advisory_model_first/price_range_runtime_bundle.py`; `price_range_inference.py`; Advisory API/UI | `backend/tests/advisory_model_first/test_price_range_runtime_bundle.py`; `backend/tests/advisory_model_first/test_price_range_inference.py`; `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | implemented_verified_not_activated | none |
| F-391 | `backend/services/advisory_model_first/price_range_runtime_bundle.py` exact loader | `backend/tests/advisory_model_first/test_price_range_runtime_bundle.py`; `backend/tests/advisory_model_first/test_price_range_calibration_boundaries.py` | implemented_verified | none |
| F-392 | `backend/services/advisory_model_first/model_inference.py` price-range isolation | `backend/tests/advisory_model_first/test_model_inference.py`; `backend/tests/advisory_model_first/test_price_range_calibration_boundaries.py` | implemented_verified | none |
| F-393 | `scripts/advisory_price_range_calibration_train_wsl.py`; `scripts/wsl/advisory_price_range_calibration_train.py` | artifact: `price_range_calibration_runs/advprcal_7cb766fe38898e12a008a328/price_range_calibration_receipt.json`; exact retry same bundle ID | implemented_real_verified | none |
| F-394 | real frozen test report | artifact: `price_range_bundles/5197ceac96c76881a506555652acc006987442024cb2d86955e7370b27968ead/metrics.json`; artifact: `price_range_calibration_runs/advprcal_7cb766fe38898e12a008a328/price_range_calibration_receipt.json` | implemented_negative_quality_verified | none |
| F-395 | protected-module scan | `backend/tests/advisory_model_first/test_price_range_calibration_boundaries.py`; command: `python scripts/aistock_module_ownership_scan.py --changed-only --include-untracked --fail-on-unmapped --fail-on-ambiguous` | implemented_verified | none |
| F-396 | no approval/admission implementation | `backend/tests/advisory_model_first/test_price_range_calibration_boundaries.py`; command: `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` | implemented_verified | none |
| F-397 | final compliance and F2 validation | `backend/tests/advisory_model_first/test_price_range_calibration_boundaries.py`; command: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_model_first_m5c_entry_gap_calibration_f2_design_20260812.md --tier F2` | implemented_verified | none |

## 16. DESIGN-COMPLIANCE-001

1. **禁止简化版**：完整交付 request、validation/test projection、CQR、v2 bundle、runtime/API/UI、真实全量 WSL 和 exact retry；单一函数、离线 JSON、mock 或只做一个日期不能冒充完成。
2. **禁止静默错误**：raw/calibrated/state 明确；缺失、tamper、非有限值、identity/split/schema 错误全部 typed failure，不提供默认 delta、空成功或 raw fallback。
3. **禁止业务语义偏移**：旧字段保持 raw；不改候选、排名、Top5、outcome、M4 binary/path/法规、Selection、模拟盘、QE 或其它模块；校准不描述为收益改善。
4. **禁止未经确认门禁/审批**：没有角色、审批、二次准入或自动质量门禁。身份、时间隔离、原子 readback 是数据正确性合同；指标只形成研究结论，binding/restart 仍是独立用户动作。

## 17. Risks / 风险与处置

| 风险 | 处置 |
|---|---|
| validation CQR 在 test 上过度扩张 | coverage 与 width 同报；test 不反向调参，不改善则不激活 |
| 父 bundle 缺 validation predictions | 只读加载父模型和冻结 validation features/labels 重放，不重训、不读取 test 拟合 |
| calibrated 值覆盖旧 raw 字段 | v1 字段保持 raw，新字段显式命名并做 v1/v2 回归 |
| binary 负例不足被误写成 calibrated | manifest、API、UI 独立固定 `entry_executable_calibration_state=UNCALIBRATED` |
| M5C 异常阻断荐股 | price-range 子信封 typed isolation，rank/outcome/rule invariance 测试 |
| 实现扩展到历史平台或共享模块 | 精确 file scope 与 protected-module test；真实阻断才修订设计 |

## 18. Production Gates / 生产影响

```text
production_ddl_gate = noop
production_dml_gate = noop
production_backend_dependency_gate = noop
production_frontend_dependency_gate = noop
runtime_activation = exact price-range v2 binding，独立用户确认
backend_restart = user-owned，仅在后续 binding 激活并需加载 runtime 代码时执行
```

源码、真实 WSL 校准和 bundle 发布均不需要数据库写入或后端重启。本阶段不得自行激活 binding、启停服务或清理历史产物。

## 19. Rollout / Rollback

发布状态必须分开：源码合入、真实 WSL bundle、quality decision、可选 exact binding、用户重启、deployed readback。前一步不代表后一步完成。若不建议激活，现行 v1 binding 原样保留。若未来激活后需回滚，只恢复 v1 exact binding；不删除 v2 bundle、不修改数据库、不回滚 M2/M3/M4，也不处理荐股历史任务。
