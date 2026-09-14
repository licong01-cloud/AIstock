# AdvisoryDailyPriceEnvelopeV1 日级价格区间 F2 详细设计 v1.2

> 日期：2026-09-15
> Feature tier：F2
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.61
> 当前阶段：`MODEL_RUN_COMPLETE_FRESH_CONFIRMATION_REQUIRED_NOT_ACTIVATED`
> 业务归属：Selection Center / Advisory
> 运行边界：日频 PIT 价格预测；不研发分钟择时或交易执行

## 1. Background / 目标与当前事实

本设计把已有 M4 能力收敛为一个可版本化、可校准、可显式不可用的日级价格预测合同。Advisory 对每只候选发布次交易日进场参考区间，以及绑定既有 M3 持有期与路径模型的止盈、保护和止损参考区间；输出只表达预测事实，不产生分钟时点、订单或成交指令。

当前实现与真实 artifact 已证明：

- M4 v1 已有一个 `entry_executable_probability` 和三个 `entry_gap_q10/q50/q90` LightGBM 头；M4 v2 已有中央 80% CQR 区间校准。
- v1 test 共 1600 行，`entry_executable` 正例率为 `0.999375`，即 1599 个正例、1 个权威负例；该二分类头不具备足够反例，不能承担买入准入或可执行概率职责。
- v2 在 validation 的区间覆盖率为 `0.810638`，在已消费 80 日 test 的覆盖率为 `0.727955`，名义覆盖率为 `0.8`，且 `activation_recommended=false`。
- 2026-09-15 正式 v3 request `advprreq_e788810c50b59802ec2344c3` 生成三头 bundle `30e8a75b4b321a0be31ea6b8530c5bbc0afd2f81d28c2977e74e57226ddf1081`：406 个决策日、8120 个候选、1600 行/80 日 test，耗时 `12.564s`、峰值 RSS `479895552` bytes；三分位数零 crossing，test 覆盖率 `0.733125`。
- validation-only v4 request `advprcal_9a82e951973c7e58a2bc738f` 生成校准 bundle `508fedfeb48a168792d6650b06cb197556b72f54c48d97a7fa9f6b2075de437f`；validation 940 行/47 日覆盖率 `0.811702`、test 1600 行/80 日覆盖率 `0.733125`，校准扩张量为 0，不能修复 validation→test 漂移。强制结论为 `FRESH_CONFIRMATION_REQUIRED`、`activation_recommended=false`、零 binding/零 runtime activation。
- 当前磁盘 binding 仍指向未校准 v1 bundle；当前 P0-D descriptor 没有组合 M4，因此生产响应保持 typed unavailable。源码存在、artifact 存在、binding 存在和当前 descriptor 激活是四个不同状态。

由此冻结两个决定：

1. `entry_executable_probability` 从 `AdvisoryDailyPriceEnvelopeV1`、页面展示和任何业务动作中退役；旧 v1/v2 bundle 与模型文件保持不可变兼容，不删除、不改写历史结果。
2. 新的正式价格模型只预测日级价格分布。买入准入由独立日频 Entry Guard/Admission 合同负责，价格模型不得以近单类概率代替准入判断。

## 2. 范围

本切片包含：

1. 冻结 `AdvisoryDailyPriceEnvelopeV1` 顶层和候选级 schema。
2. 将现有 M4 推理结果转换为该 schema，并从业务输出中移除不可识别的 executable probability。
3. 在 UI 明确显示目标交易日、日级条件、价格基准、校准状态、模型身份和不可用原因。
4. 为新三分位数日级模型冻结 v3 标签、bundle、校准和历史验证合同；旧 bundle 只作为兼容输入，不自动获得激活资格。
5. 使用既有日频 PIT 数据执行开发窗口/已消费测试窗口评价；确认性激活仍需新鲜 holdout 或自然前向。

## 3. 非目标与模块边界

- 不读取 Qlib 分钟 Bin，不训练分钟特征、最佳买卖分钟、触达先后、成交概率、拆单或滑点模型。
- 不产生 `BUY_NOW/WAIT/SELL_NOW` 分钟动作、订单数量、参与率、限价单、fill 或 broker 状态。
- 不修改 `backend/services/quantevolver/**`、QE router/registry/profile/dataset binding/composer。
- 不修改 Selection、StrategyPackage、Paper、Execution、模拟盘、QMT 或 Position Timing 的业务源码。
- 不新增 DDL/DML、数据库表、调度器、模型注册平台、证据平台、审批或人工 ACK。
- 不把规则百分比、decision close、固定区间或旧 executable probability 伪装为通过校准的模型输出。

未来 QE、Paper 或 Execution 若消费本合同，只能在其自己的设计、回测和激活流程中进行只读适配；本次不实现这些适配器。

## 4. Architecture / 决策时钟与 PIT

```text
decision_as_of_trade_date = D
target_trade_date = next_trading_day(D)
price_basis = UNADJUSTED_CNY_DECISION_CLOSE
feature_cutoff <= D close
target daily/minute market data is forbidden at inference
```

训练标签可以读取 target 日的开盘价、停牌状态和日线 high/low，但在线特征、法规边界、公司行动与输出不得读取 target 日真实行情。训练、历史回放与每日 forward 共用相同逐日价格投影内核，只允许运行信封不同。

## 5. Contracts / `AdvisoryDailyPriceEnvelopeV1` 顶层合同

```text
schema_version = advisory_daily_price_envelope_v1
objective_contract = RISK_MANAGED_ADVISORY
status = EXPERIMENTAL_SHADOW | PRICE_RANGE_UNAVAILABLE
availability_status = AVAILABLE | PARTIAL | UNAVAILABLE
decision_as_of_trade_date
target_trade_date
price_basis = UNADJUSTED_CNY_DECISION_CLOSE
calibration_state = CALIBRATED_INTERVAL | UNCALIBRATED
nominal_coverage
package_id
package_manifest_sha256
style_profile_hash
parent_bundle_id
outcome_bundle_id
price_range_bundle_id
model_version
review_policy_sha256
candidates[]
reason_code
message
```

可用响应必须具有非空 D/T、完整 package/model/policy identity 和至少一个候选。`PARTIAL` 只表示候选级 typed unavailable 与可用候选并存，不允许隐藏失败计数。顶层共同输入失败时为 `UNAVAILABLE`，候选数组为空。

## 6. 候选合同

```text
symbol
status = EXPERIMENTAL_SHADOW | PRICE_RANGE_UNAVAILABLE
availability_status = AVAILABLE | UNAVAILABLE
projection_condition = NEXT_TRADING_DAY_VALID_OPEN_AT_PREDICTED_ENTRY_MID
decision_reference_price
decision_price_trade_date
target_raw_price_multiplier
entry_price_range = {condition, low, mid, high}
calibrated_entry_price_range = {condition, low, mid, high} | null
entry_gap_calibration = {state, method, delta, nominal_coverage}
take_profit_price = {low, high, horizon_trade_days}
protective_price
stop_loss_price
tick_size
regulatory_price_range
review_policy
reason_code
message
```

`entry_price_range.condition` 固定为 `NEXT_TRADING_DAY_VALID_OPEN`。止盈、保护和止损仍明确条件于使用预测 `entry_mid` 建仓；实际成交价不同后，绝对价格必须由消费方重新投影，Advisory 不生成执行计划。

候选响应不得包含 `entry_executable_probability`。它既不是日级价格区间的一部分，也不得被 UI 解释为收益概率或买入概率。

## 7. 禁止字段

合同及嵌套对象必须拒绝下列字段族，而不是静默忽略：

- `best_buy_minute`、`best_sell_minute`、`minute_timestamp`、`minute_path`；
- `buy_now`、`sell_now`、`wait_until`、`order_quantity`、`participation_rate`；
- `order_slices`、`limit_order_plan`、`fill_probability`、`fill_result`；
- `paper_position`、`execution_plan`、`broker_order`；
- 尚未发生的 target 日 open/high/low/close/VWAP/minute bars。

## 8. 新日级模型合同

### 8.1 标签 v2

新标签只回答目标交易日开盘价格分布：

```text
if target is authoritatively suspended:
    label_status = NOT_APPLICABLE
elif target daily row is missing without authoritative reason:
    label_status = UNAVAILABLE
elif target open and decision close are finite and positive:
    label_status = AVAILABLE
    target_open_gap_return = target_open / decision_close - 1
else:
    label_status = UNAVAILABLE
```

一字涨停但存在合法开盘价的样本仍属于价格分布，不再被转换成 executable binary 负例；是否值得或能否买入由 Entry Guard/Admission 处理。正常停牌和无法解释的缺行均保留在 coverage 统计中，不删除股票或日期、不填零。

### 8.2 三个固定模型头

```text
entry_gap_q10: LightGBM quantile alpha=0.10
entry_gap_q50: LightGBM quantile alpha=0.50
entry_gap_q90: LightGBM quantile alpha=0.90
```

训练复用当前 exact package、parent/outcome、103 特征、M3 split/purge 与日频 QE 文件身份。未校准 `advisory_price_range_bundle_v3` 的模型集合必须精确为上述三个头；validation 校准后的派生容器为 `advisory_price_range_bundle_v4`，仍复用同三个模型文件，只增加不可变 calibration spec/metrics/predictions。旧四头 bundle 只能通过 legacy loader 进入兼容推理，不能冒充 v3/v4。

### 8.3 校准与证据边界

- validation 只拟合预注册的中央 80% split-conformal/CQR 非负扩张量；test 只报告，不选择模型、阈值或校准参数。
- 报告 row/date coverage、区间覆盖率、上下越界率、平均/中位宽度、pinball loss、crossing、法规裁剪率和 typed unavailable。
- 已消费旧 test 只能作为开发诊断，不能支持激活；新鲜 holdout 只能在模型/校准/合同冻结后使用一次。
- 若新鲜 holdout 尚不可用，允许发布 `EXPERIMENTAL_SHADOW/UNCALIBRATED` 或 `CALIBRATED_INTERVAL` 的自然前向观察，但不得标记业务有效或保证收益。

## 9. 旧 bundle 兼容政策

- `advisory_price_range_bundle_v1/v2`、旧 binding 和历史 receipt 保持字节不可变。
- runtime loader 可继续校验和加载旧四头 bundle；推理只消费其中三个 quantile 头，忽略 executable head 的数值，不把它写入新合同。
- 新响应必须记录 `source_bundle_schema_version` 与 `entry_admission_model_status=RETIRED_NON_IDENTIFIABLE`。
- 旧 bundle test 覆盖率不足时保持 `EXPERIMENTAL_SHADOW`；不得因为 schema 适配而升级激活状态。

## 10. API 与 UI

现有 model-shadow 的 `price_range` key 保持兼容，但其值升级为 `AdvisoryDailyPriceEnvelopeV1`：

- API 返回完整 D/T、目标合同、package/bundle/policy identity、可用性和候选级失败计数。
- UI 标题为“次交易日日级价格区间（实验影子）”，不显示 executable probability。
- UI 显示目标交易日、名义覆盖率/校准状态、未复权 CNY 基准、原始/校准买入区间及止盈/保护/止损参考。
- 页面不得使用“最佳买点”“最佳卖点”“保证成交”“实时择时”等文案，也不得新增交易操作按钮。

## 11. 实现范围

新增：

- `backend/services/advisory_model_first/daily_price_envelope_contracts.py`
- `backend/tests/advisory_model_first/test_daily_price_envelope_contract.py`
- `backend/tests/advisory_model_first/test_daily_price_envelope_boundaries.py`
- `backend/tests/advisory_model_first/test_daily_price_envelope_pit.py`

修改：

- `backend/services/advisory_model_first/price_range_inference.py`
- `backend/services/advisory_model_first/model_inference.py`
- `frontend/src/lib/api/advisory.ts`
- `frontend/src/app/paper-v2/advisory/page.tsx`
- `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`
- `scripts/wsl/advisory_price_range_train.py`
- `scripts/wsl/advisory_price_range_calibration_train.py`
- `scripts/advisory_price_range_prepare_request.py`
- `scripts/advisory_price_range_calibration_prepare_request.py`
- `backend/tests/advisory_model_first/test_price_range_request_contracts.py`
- 当前蓝图与本详细设计

本次已执行 v3/v4 训练与校准源码实现，因此修改范围包含 `price_range_contracts.py`、`price_range_labels.py`、`price_range_training.py`、`price_range_bundle.py`、`price_range_pipeline.py`、校准 contracts/bundle/pipeline、两个 WSL 显式 contract CLI 和对应定向测试。不得越界修改其它模块。

## 12. Implementation Plan / 实施顺序

1. 合同层与 forbidden-field 测试。
2. legacy v1/v2 三分位数兼容推理，移除业务 executable probability。
3. model-shadow 顶层身份、D/T、可用性与 policy 绑定。
4. UI 类型、文案和错误态。
5. v3 标签/三头训练、bundle、校准和开发窗口评价。
6. 历史日频回放；合格时发布 shadow binding，失败时保持 typed unavailable/experimental。
7. 源码合入后等待用户重启，再做 fresh-process API/UI readback；不执行 DDL。

## 13. 验证方案

- 合同：Pydantic `extra=forbid`、日期顺序、可用状态必填身份、候选完整性、禁止字段。
- PIT：改变 target 日未来行情不会改变 D 日推理；训练标签与在线推理数据边界分离。
- legacy：v1/v2 manifest 仍可加载；四头 bundle 只消费三个 quantile，不输出 executable probability。
- projection：区间单调、tick/法规边界、公司行动 multiplier、M3 horizon、硬止损不放宽。
- isolation：价格失败不改变 M2/M3、候选顺序、RecommendationList 或其它模块。
- UI：目标日、校准、身份、typed unavailable 可见；无分钟/订单/成交文案与按钮。
- 工程：changed-file ownership、Python compile/ruff、TypeScript、定向 pytest、Playwright、`git diff --check`、F2 validator。

## 14. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-360 | `AdvisoryDailyPriceEnvelopeV1` 只表达日级 PIT 价格预测，不包含分钟择时或执行字段 |
| F-361 | 顶层 D/T、目标合同、价格基准、可用性、模型/package/policy identity 完整且 fail closed |
| F-362 | 候选级进场、止盈、保护、止损区间条件、单位、tick、法规边界和错误完整 |
| F-363 | 近单类 executable binary 从新业务合同和 UI 退役，不作为收益/买入概率或门禁 |
| F-364 | 旧 v1/v2 bundle 与 binding 不改写，legacy loader 兼容且不升级证据状态 |
| F-365 | 新 v3 只含三个日级开盘缺口 quantile 头，标签覆盖正常缺失且无 binary fallback |
| F-366 | 在线推理只读 D 截止的日频 PIT 数据，target 未来数据毒化不改变输出 |
| F-367 | 校准只拟合 validation，test 只报告；旧 test、fresh holdout、自然前向证据不互相冒充 |
| F-368 | API/UI 显示日级目标、校准、身份和 typed unavailable，无最佳分钟或交易操作 |
| F-369 | 价格子信封失败不改变 M2/M3、候选、排序、列表或基线荐股 |
| F-370 | 历史评价源码必须报告 coverage、宽度、越界、pinball、crossing、裁剪和不可用率；真实数值只能由后续不可变模型运行产生 |
| F-371 | bundle/runtime 源码必须拒绝非 exact-compatible 或证据状态不合格的模型；实际 shadow binding 与运行时激活是后续独立阶段 |
| F-372 | changed files 不修改 QE、Selection、StrategyPackage、Paper、Execution、数据库或进程 |
| F-373 | 无简化/静默错误/业务偏移/私增审批；未达标结果保持实验或不可用 |

## 15. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-360 | §3、§5～§7；`daily_price_envelope_contracts.py` | `backend/tests/advisory_model_first/test_daily_price_envelope_contract.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-361 | §5；`backend/services/advisory_model_first/model_inference.py::_price_range_shadow` | `backend/tests/advisory_model_first/test_model_inference.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-362 | §6；`backend/services/advisory_model_first/price_range_inference.py` | `backend/tests/advisory_model_first/test_price_range_inference.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-363 | §1、§6、§9～§10 | `backend/tests/advisory_model_first/test_daily_price_envelope_contract.py`；`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-364 | §9；`backend/services/advisory_model_first/price_range_runtime_bundle.py` | `backend/tests/advisory_model_first/test_price_range_runtime_bundle.py`；`backend/tests/advisory_model_first/test_price_range_inference.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-365 | §8；`backend/services/advisory_model_first/price_range_training.py`；`price_range_bundle.py`；calibration modules | `backend/tests/advisory_model_first/test_price_range_labels.py`；`test_price_range_training.py`；`test_price_range_bundle.py`；calibration tests | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-366 | §4、§13 | `backend/tests/advisory_model_first/test_daily_price_envelope_pit.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-367 | §8.3；`backend/services/advisory_model_first/price_range_calibration_pipeline.py` | `backend/tests/advisory_model_first/test_price_range_calibration_pipeline.py`；`test_price_range_calibration_bundle.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-368 | §10；`frontend/src/app/paper-v2/advisory/page.tsx` | `node node_modules/typescript/bin/tsc --noEmit`；`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`定向 Playwright | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-369 | §3、§13；`backend/services/advisory_model_first/model_inference.py` | `backend/tests/advisory_model_first/test_model_inference.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-370 | §8.3、§13；训练/校准 pipeline metrics contract | `backend/tests/advisory_model_first/test_price_range_training.py`、`backend/tests/advisory_model_first/test_price_range_calibration_pipeline.py`；正式 v3 receipt `F:/Dev/AIstock_model_artifacts/advisory_model_first/price_range_runs/advprreq_e788810c50b59802ec2344c3/daily_price_envelope_training_receipt.json`；正式 v4 receipt `F:/Dev/AIstock_model_artifacts/advisory_model_first/price_range_calibration_runs/advprcal_9a82e951973c7e58a2bc738f/daily_price_envelope_calibration_receipt.json` | FORMAL_ARTIFACT_VERIFIED_NOT_ACTIVATED | none |
| F-371 | §9、§12；v3/v4 bundle 与 runtime strict validator | `backend/tests/advisory_model_first/test_price_range_bundle.py`、`backend/tests/advisory_model_first/test_price_range_calibration_bundle.py`；严格 reader 读回 v3 `30e8a75b...` 与 v4 `508fedfe...` | FORMAL_ARTIFACT_VERIFIED_NOT_ACTIVATED | none |
| F-372 | §3、§11 | `backend/tests/advisory_model_first/test_daily_price_envelope_boundaries.py`；`git diff --name-only` ownership scan | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-373 | §1～§18 | `python -m pytest backend/tests/advisory_model_first/test_daily_price_envelope_contract.py`等78项及`backend/tests/advisory_model_first/test_price_range_request_contracts.py` 9项；PR #4704/#4722 的完整`advisory_modeling_backend` CI、`python -m ruff check`、TypeScript、`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` Playwright 2项及 F2 validator 通过 | IMPLEMENTED_AND_CI_VERIFIED | none |

## 16. Rollout / Rollback 与终止条件

源码切片已由 PR #4704 合入，BUG-1504 的启动器合同转发修复由 PR #4722 合入。F-360～F-373 无未批准源码 gap；v3/v4 真实模型、校准 receipt 和严格 manifest 读回已完成。因已消费 test 仅有 `0.733125` 覆盖率且低于名义 0.8，当前终止在 `FRESH_CONFIRMATION_REQUIRED_NOT_ACTIVATED`：不发布 binding、不要求重启、不做生产 readback。后续唯一合法证据是新鲜一次性 holdout 或自然前向，不得调已消费 test。

模型终止条件：若三分位数模型在预注册评价中未达到校准与经济解释要求，则不调已消费 test、不恢复 executable binary、不输出规则替代品；保留源码能力并返回 `EXPERIMENTAL_SHADOW/UNCALIBRATED` 或 typed unavailable，等待新信息集或自然前向。

回滚只撤销新价格角色 binding 或停止返回新 envelope；不删除旧 bundle、不修改数据库、不回滚候选、排名、M3、RecommendationList 或规则基线。源码合入、binding、用户重启和运行时 readback 分开报告。

## 17. Risks / 风险与处置

| 风险 | 处置 |
|---|---|
| 近单类 probability 被继续当成买入概率 | 新合同和 UI 不提供该字段；旧模型只作 immutable compatibility |
| 区间校准在时间漂移下失效 | validation 拟合、test 只报告；自然前向持续报告覆盖和宽度，不自动升级激活 |
| 日级区间被误用为分钟执行指令 | forbidden-field 合同、无订单按钮、外部消费另立设计与激活 |
| target 日未来行情泄漏 | D/T 强类型、PIT poison 测试、在线调用不接收 target market frame |
| 价格失败影响荐股基线 | 平级子信封 typed unavailable，不改变 M2/M3/候选/列表 |
| 为改进覆盖率反复调已消费 test | test 只作开发诊断；新假设必须新 lineage 和新鲜确认数据 |

## 18. Production Gates / 生产影响

```text
production_ddl_gate = noop
production_dml_gate = noop
backend_dependency_gate = noop
frontend_dependency_gate = noop
backend_restart = user_owned_after_source_merge
runtime_activation = pending_user_restart_and_readback
```
