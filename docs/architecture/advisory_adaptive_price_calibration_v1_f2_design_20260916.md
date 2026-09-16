# AdvisoryAdaptivePriceCalibrationV1 F2 详细设计 v1.2

> 状态：源码与正式历史导航审计完成；`selected=0`，lineage终止且不激活
>
> 业务归属：Selection Center / Advisory
>
> 目标合同：`RISK_MANAGED_ADVISORY`
>
> 研究阶段：`EXPLORATORY_SCREEN / NAVIGATION_ONLY`

## 1. Background / 当前事实

日级价格信封 v3/v4 已完成真实训练、validation-only CQR 校准、历史 PIT 批量回放和首个自然前向结算。冻结 v4 bundle `508fedfe...` 在 validation 的中央 80% coverage 为 `0.811702`，校准扩张量为 0；在 test 连续模型空间 coverage 降为 `0.733125`。业务 0.01 元 tick 与涨跌停投影后的 coverage 为 `0.81375`，说明模型连续空间漂移和业务离散投影效果必须分开评价。

既有 test `2025-11-07..2026-03-10` 已由回放 `advprhist_23e8ce5a...` 消费，只能提供导航证据，不能支持 activation。当前不存在 2026-03-10 之后的正式 StrategyPackage 历史预测面板；T=`2026-09-15` 的单日 prospective artifact 只证明盘前可产性，也不足以确认校准策略。

本切片实现一个且仅一个 past-only 更新协议，使历史回放可立即验证更新算法、PIT 时钟和指标合同，并让未来独立回放或自然成熟 residual 可复用相同实现。它不伪造新 holdout，不因时间预算发布 `ENTRY_PRICE` binding。

## 2. Scope / 目标

1. 读取并严格校验一个已经发布的 `AdvisoryHistoricalPriceReplayArtifact`。
2. 比较冻结 v4 静态区间 control 与一个预注册的 adaptive candidate。
3. adaptive candidate 只使用当前决策日之前已成熟的历史 residual，保持 q50 不变，只对 q10/q90 做非负对称扩张。
4. 复用现有历史 PIT 上下文和价格投影核，分别报告连续 gap 空间与业务 tick/涨跌停空间指标。
5. 原子发布不可变 request、result、manifest、receipt 和 registry record；exact retry 必须同内容 no-op，身份漂移必须拒绝。
6. 在既有 N0 append-only registry 登记两项已评价 trial（static control + adaptive candidate），不建设新 registry 或 UI。

## 3. Non-goals / 边界

- 不训练或重跑 QE，不修改 `backend/services/quantevolver/**`、QE router、profile、dataset binding 或 composer。
- 不生成、晋升或修复 StrategyPackage。
- 不读取 sealed holdout，不创建“独立确认已完成”的虚假状态。
- 不修改数据库、不补数据、不控制 backend/worker/scheduler/WSL/远端进程。
- 不实现分钟买卖点、订单、拆单、成交或执行算法。
- 不移动 q50，不搜索 window、warm-up、coverage、阈值或模型族。
- 不发布 Program descriptor、role binding 或 runtime activation。

## 4. Architecture / Frozen study contract

### 4.1 唯一变量

两臂固定为：

- `STATIC_V4_CONTROL`：使用 source replay 中冻结的 v4 calibrated q10/q50/q90，不再扩张。
- `ROLLING_20D_MATURED_CQR`：在静态区间上追加 past-only 非负 CQR 扩张。

唯一变量为 `interval_update_protocol`。`planned/generated/evaluated_trial_count=2`，不添加第二个窗口、第二个 nominal coverage、第二个 warm-up 或第二个更新算法。

### 4.2 更新参数

固定参数如下：

- nominal coverage：`0.8`；
- residual：`max(static_q10 - y, y - static_q90, 0)`；
- lookback：最近 `20` 个已成熟 target trade date；
- 最低更新支持：`5` 个 target trade date 且 `100` 个有效 candidate residual；
- finite-sample rank：`min(ceil((n + 1) * 0.8), n)`；
- delta：选定 rank 的 residual，必须 finite 且 `>= 0`；
- candidate：`q10' = q10 - delta`、`q50' = q50`、`q90' = q90 + delta`；
- 支持不足时 `delta=0`，状态为 `WARMUP_STATIC_FALLBACK`，不得填充未来 residual。

20 日窗口约有 400 个 Top20 residual，可对近期 coverage 漂移响应，又避免单日 20 行估计。该值是本设计发布前冻结的唯一协议参数，不根据既有 80 日结果搜索。

### 4.3 PIT maturity clock

对 decision date `D`、目标日 `T>D` 的预测，只允许使用 `target_trade_date <= D` 的已结算 residual。这里的业务时钟是 D 日收盘后形成下一交易日价格信封，因此不晚于 D 的 target open 已成熟。算法必须先冻结 D/T 的 delta 和预测，再评价 T 的结果；T 的结果只能进入后续 decision date 的窗口。

实现以 source replay 的不可变 prediction/outcome identity 为输入，并按 `(decision_as_of_trade_date, target_trade_date, symbol)` 一一对齐。重复键、缺键、D/T 不一致、当前/未来 target 混入 fit window 均 fail closed。

## 5. Data and identity

request 至少绑定：

- source replay root、replay id、request/prediction/outcome/manifest/receipt SHA256；
- source `price_range_bundle_id`、v4 manifest hash及其package/policy lineage；
- objective contract、study type、decision use、consumed-window identity；
- frozen adaptive policy id/hash与全部参数；
- N0 registry path；
- repository commit和显式 output root。

正式首跑输入是 `advprhist_23e8ce5a...`，window state 固定 `HISTORICAL_REPLAY_CONSUMED`。后续如使用真正独立 replay，必须创建新 request/attempt/lineage，不能修改本次记录。

输出根固定为 `<model_root>/adaptive_price_calibration_runs/<request_id>/`。目录包含：

- `request.json`
- `result.json`
- `registry_records.json`
- `manifest.json`
- `receipt.json`

所有内容使用 canonical JSON hash；发布采用同一文件系统的临时目录 + `os.replace`。同 request 已存在时完整读回并返回 `ALREADY_MATERIALIZED`；内容或 hash 冲突返回 typed error，不覆盖。

## 6. Evaluation kernel

### 6.1 连续模型空间

每臂报告：有效日期/行数、coverage、lower/upper miss、mean/median width bps、mean/median q50 absolute error bps、crossing count。gap width 和 q50 error 统一乘 `10,000` 转 bps。

### 6.2 业务价格空间

adaptive arm 不自行复制涨跌停或 corporate-action 规则。它把更新后的 gap triplet 交给既有 `_settle_day`：

1. `PostgresHistoricalPriceReplaySource` 以只读事务加载 D/T 上下文；
2. `resolve_regulatory_price_range` 决定当时可知的板块、ST、上市日与涨跌停边界；
3. `_entry_band` 执行 0.01 元 tick floor/nearest/ceiling 与 regulatory clipping；
4. 重新读取的 actual open 必须与 source replay immutable outcome 在 `rel_tol=1e-12/abs_tol=1e-9` 内一致，状态必须完全一致，否则 `ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT`；
5. 停牌保留 `NOT_APPLICABLE`，未知缺行/上下文缺失保留 typed unavailable，不删除股票。

每臂业务指标与模型空间分开报告，并报告相对 static 的 tick rescue/harm。业务 coverage 改善不得被归因成模型连续空间改善。

## 7. Pre-registered navigation gate

本次是已消费窗口的 exploratory screen，最多只能决定 candidate 是否值得等待独立 confirmation。adaptive candidate 标记 `CANDIDATE_FOR_FRESH_CONFIRMATION` 必须同时满足：

1. adaptive-active 支持至少 60 个 decision date、1,000 个有效 candidate；active 只计 `delta` 由满足5日/100行的成熟窗口估计的日期，不含warm-up fallback；
2. 模型空间 coverage absolute error 相对 static 至少改善 2 个百分点；
3. 在相同 adaptive-active 行集合上，以 decision date 为 cluster、固定 seed `20260916`、5,000 次 bootstrap 的日级 coverage-error improvement 95% lower bound `> 0`；
4. adaptive 模型空间 mean interval width 不超过 static 的 `1.25x`；
5. lower 和 upper miss 均不高于 static，crossing 为 0；
6. 在相同 adaptive-active 行集合上，业务价格 coverage 不低于 static，且 business mean width 不超过 static 的 `1.25x`；
7. source identity、PIT maturity、完整性和 registry 写入全部通过。

任一门失败则 `NOT_SELECTED_NAVIGATION_ONLY`。门通过也只进入未来一次性 confirmation 的候选状态，`selected_trial_count` 可为 1，但 `binding_activated=false`、`activation_recommended=false`。不得在同一结果后改参数或回选。

## 8. Result and registry semantics

result 必须包含：两臂完整指标、逐日 delta/support/status、差值与 bootstrap CI、每条门禁、selected candidate、evidence boundary和下一步。

registry 记录固定：

- `study_type=EXPLORATORY_SCREEN`
- `objective_contract=RISK_MANAGED_ADVISORY`
- `decision_use=NAVIGATION_ONLY`
- `hypothesis_family_id=ADVISORY-PRICE-ADAPTIVE-CALIBRATION-V1`
- `unique_variable=STATIC_V4_CONTROL_VS_ROLLING_20D_MATURED_CQR`
- consumed window 使用 source replay 的 dataset/window identity
- evidence refs 至少包含 request、result；receipt再绑定registry-record文件，避免循环hash

registry append 发生在 immutable bundle 完成验证后；exact duplicate 为 no-op，冲突拒绝。registry 失败不得把 bundle 描述为正式交付完成。

## 9. CLI and statuses

CLI 只提供：

```text
python -m backend.services.advisory_model_first.adaptive_price_calibration_cli run \
  --env-file <repo>/.env \
  --source-replay-root <model_root>/price_range_historical_replays/<replay_id> \
  --output-root <model_root> \
  --registry-path <n0_root>/advisory_research_trial_registry_v1.jsonl
```

退出码：0=`PUBLISHED/ALREADY_MATERIALIZED`，2=合同/数据/身份/registry失败。CLI 不启动或停止任何进程，不创建 QE 任务，不修改数据库。

## 10. Error contracts

| reason_code | 含义 |
|---|---|
| `ADVISORY_ADAPTIVE_PRICE_REQUEST_INVALID` | request、参数或路径合同错误 |
| `ADVISORY_ADAPTIVE_PRICE_SOURCE_INVALID` | source replay 不完整、不一致或不是已发布历史回放 |
| `ADVISORY_ADAPTIVE_PRICE_PIT_VIOLATION` | fit window 混入当前或未来结果 |
| `ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT` | DB只读回放与immutable source outcome不一致 |
| `ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED` | 指标、bootstrap或投影失败 |
| `ADVISORY_ADAPTIVE_PRICE_ARTIFACT_CONFLICT` | 已发布目录缺失、hash冲突或非exact retry |
| `ADVISORY_RESEARCH_REGISTRY_*` | 复用N0 registry既有typed失败 |

## 11. Implementation plan

文件范围冻结为：

- `backend/services/advisory_model_first/adaptive_price_calibration_contracts.py`
- `backend/services/advisory_model_first/adaptive_price_calibration.py`
- `backend/services/advisory_model_first/adaptive_price_calibration_cli.py`
- `backend/tests/advisory_model_first/test_adaptive_price_calibration_contracts.py`
- `backend/tests/advisory_model_first/test_adaptive_price_calibration.py`
- `backend/tests/advisory_model_first/test_adaptive_price_calibration_cli.py`
- 本设计和权威蓝图的事实/验收状态更新

不得修改 QE、Selection Center、Watchlist、数据库 migration 或 runtime binding 文件。

## 12. Verification plan

单元与集成边界至少覆盖：

1. request identity、路径 containment、extra forbid和exact retry；
2. 当前/未来 outcome poison 不改变更早 date 的 delta/输出；
3. 5日/100行 warm-up，20日窗口裁剪，finite-sample rank；
4. q50不变、非负扩张、零 crossing；
5. 停牌/缺行保留与source drift fail closed；
6. 连续/业务指标分离、cluster bootstrap确定性；
7. 七项 gate 全通过与逐项失败；
8. immutable manifest/receipt tamper拒绝；
9. registry append、duplicate no-op和navigation-only不可用作activation；
10. CLI 成功/失败退出码和零进程/QE/DB mutation边界。

正式运行前再次查询 QE task list；有任何 `running` QE 实验时禁止启动。本地 synthetic tests 不属于市场研究实验，可继续执行。

## 13. Rollout / rollback

本切片仅增加离线 Advisory 研究命令，不接入后端路由和运行时。回滚为停止调用新 CLI；既有 v4 bundle、历史回放、prospective artifact、Program/descriptor均不变。已发布研究 artifact 和 registry 行只追加、不删除、不改判。

## 14. Resource and complexity / 资源与复杂度

输入上界由 source replay receipt 的 candidate/date 计数决定。实现先验证 prediction/outcome 复合键唯一且集合相等，再按日期分组；禁止 many-to-many join。20日 residual 窗口使用按target date分组的有界 deque，时间复杂度 `O(N + D*B)`，其中 N 为候选行、D 为决策日、B 最大为20日候选数；正式80日/1600行规模不需要全量笛卡尔计算。bootstrap只对 D 个日级聚合值做5000次有放回抽样，内存上限为 `O(5000*D)`，不得对股票行做伪独立bootstrap。

数据库每个D/T组只调用一次既有批量 `load_day`，不逐股票查询；连接、事务和cursor由现有source释放。CLI记录输入行数、日期数、各阶段耗时和峰值RSS；正式运行硬上限8 GiB。没有后台线程或常驻资源。

## 15. Production gates

源码合入不等于业务激活。进入 `ENTRY_PRICE` shadow binding 前仍须：

1. 新独立 historical replay 或至少20日/300行自然前向 evidence；
2. 同一冻结 candidate 的 confirmation 合同通过；
3. exact package/policy/schema/model兼容；
4. 单独实现并审核 binding/API/UI；
5. 用户执行 backend restart 后 runtime readback。

本切片没有 DDL、DML、依赖、backend restart 或生产操作。

## 16. Risks / 风险与处置

| 风险 | 处置 |
|---|---|
| hindsight residual 泄漏到同日预测 | fit 只接受 `target_trade_date <= decision_as_of_trade_date`，future-poison测试比较更早输出hash |
| 既有80日结果被冒充独立确认 | request/result/registry三层固定 `EXPLORATORY_SCREEN/NAVIGATION_ONLY`，activation字段只能为false |
| 因观察结果后调整窗口造成多重试验 | v1只接受20日、5日/100行、80% nominal；其它值校验失败，新假设必须新lineage |
| DB历史事实与immutable replay漂移 | 逐键比较actual open及状态，任一差异整体fail closed，不择行删除 |
| 业务tick coverage掩盖模型漂移 | model/business双空间独立指标，rescue/harm单独列示 |
| 正常停牌或未知缺行使任务误停 | 停牌保留NOT_APPLICABLE，缺行保留UNAVAILABLE；只有身份矛盾才失败 |
| registry写入失败但artifact被称为正式完成 | CLI只有bundle完整读回且registry append/no-op成功才返回交付成功 |
| 研究代码扩散到QE或运行时 | 文件白名单 + diff审核；不新增router、binding、migration或QE import |

## 17. Design Acceptance Index

| design_item | acceptance |
|---|---|
| F-408 | 只读取严格验证的已发布historical replay，绑定全部source hash |
| F-409 | 唯一比较为static v4与固定rolling-20D matured CQR，不搜索参数 |
| F-410 | 每日更新只消费target date不晚于当前decision date的成熟residual |
| F-411 | 5日/100行warm-up不足时显式static fallback，不填未来数据 |
| F-412 | candidate只非负对称扩张q10/q90且q50不变、零crossing |
| F-413 | 连续模型空间与业务tick/涨跌停空间独立评价 |
| F-414 | exact业务投影复用历史PIT核并对immutable outcome做drift校验 |
| F-415 | 停牌和未知缺行保留为正交状态，不删除候选 |
| F-416 | 七项navigation gate和日期cluster bootstrap均预注册且确定性 |
| F-417 | 已消费窗口固定EXPLORATORY_SCREEN/NAVIGATION_ONLY，不冒充confirmation |
| F-418 | immutable artifact exact retry no-op，任何identity/tamper冲突fail closed |
| F-419 | 复用N0 append-only registry并绑定objective/study/decision/window/evidence |
| F-420 | 无QE源码/实验、DB写入、进程控制、分钟执行或runtime binding |

## 18. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-408 | `backend/services/advisory_model_first/adaptive_price_calibration_contracts.py`; `backend/services/advisory_model_first/adaptive_price_calibration.py::read_adaptive_price_calibration_artifact` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_publish_is_immutable_and_exact_retry_is_noop` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-409 | `backend/services/advisory_model_first/adaptive_price_calibration_contracts.py` frozen literals and request validator | `backend/tests/advisory_model_first/test_adaptive_price_calibration_contracts.py::test_request_rejects_protocol_drift` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-410 | `backend/services/advisory_model_first/adaptive_price_calibration.py::_evaluate_chronologically`; `_mature_fit_rows` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_future_outcome_poison_does_not_change_earlier_predictions` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-411 | `backend/services/advisory_model_first/adaptive_price_calibration.py::_fit_delta`; `AdaptivePriceDailyStateV1` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_fit_delta_requires_one_hundred_rows` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-412 | `backend/services/advisory_model_first/adaptive_price_calibration.py::_apply_delta` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_adaptive_expansion_preserves_median_and_order` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-413 | `backend/services/advisory_model_first/adaptive_price_calibration_contracts.py::AdaptivePriceArmMetricsV1`; `backend/services/advisory_model_first/adaptive_price_calibration.py::_arm_metrics` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_adaptive_pipeline_uses_past_only_residuals_and_selects_one_candidate` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-414 | `backend/services/advisory_model_first/adaptive_price_calibration.py::_evaluate_chronologically`; `_verify_source_drift` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_static_business_projection_drift_fails_closed` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-415 | `backend/services/advisory_model_first/adaptive_price_calibration.py::_arm_metrics` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_suspend_and_missing_rows_are_preserved` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-416 | `backend/services/advisory_model_first/adaptive_price_calibration.py::_navigation_gate`; `_cluster_bootstrap` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_cluster_bootstrap_is_deterministic` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-417 | `backend/services/advisory_model_first/adaptive_price_calibration_contracts.py` request/result/receipt literals | `backend/tests/advisory_model_first/test_adaptive_price_calibration_contracts.py::test_consumed_replay_cannot_be_activation_evidence` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-418 | `backend/services/advisory_model_first/adaptive_price_calibration.py::_publish`; `read_adaptive_price_calibration_artifact` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_publish_is_immutable_and_exact_retry_is_noop` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-419 | `backend/services/advisory_model_first/adaptive_price_calibration.py::_registry_record`; `AdvisoryResearchTrialRegistryV1` | `backend/tests/advisory_model_first/test_adaptive_price_calibration.py::test_publish_is_immutable_and_exact_retry_is_noop` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-420 | isolated adaptive service/CLI files only | `backend/tests/advisory_model_first/test_adaptive_price_calibration_cli.py` | IMPLEMENTED_LOCAL_VERIFIED | none |

## 19. DESIGN-COMPLIANCE-001

交付前逐项核对 F-408～F-420。设计通过只代表允许实现；源码通过只代表离线研究能力可用；已消费窗口的真实结果仍只允许导航。独立 confirmation、binding、merge、backend restart和runtime readback必须分别报告。

## 20. 正式结果与路由（2026-09-16）

- 自然前向基线：T=`2026-09-15` settlement `advprsett_14c46af1fa2bdb92088b425c` 已完成20/20市场及模型读回，business coverage为`0.70`；仅一日，状态保持`ACCUMULATING`，不参与本历史lineage选点。
- 源码：PR #4785 已以 merge commit `8021790ab4fbbb1758d0bdc5a0904dc22686ce29` 进入 `main`；CI verdict、27项直接测试、1022项Advisory全模块测试、Ruff、L0和F2 validator均通过。
- 正式artifact：`adaptive_price_calibration_runs/advpradapt_8c81ea2bd2f70d75e1683fe9`，result SHA256为`490fdb606a0d95c3853b7858e6ac76fdfae27aff09d1045c9867f2e2a62ec433`；exact retry返回`ALREADY_MATERIALIZED`且registry为duplicate no-op。
- 支持：已消费80日/1600行；adaptive active为75日/1500行。未读取sealed holdout，数据库、binding和runtime写入均为0。
- 指标：active model coverage从`0.731333`变为`0.782667`，business coverage从`0.810000`变为`0.847333`；model/business width ratio为`1.149768/1.130019`，tick rescue/harm为`56/0`。
- 终止门：按交易日聚类bootstrap的coverage-error改善point为`0.012667`，95%区间`[-0.003333,0.030000]`，lower-bound gate失败；其余support、coverage point、width、miss、business和identity门均通过。
- 结论：`selected_trial_count=0`。保持static v4，不回选窗口、不放宽门、不派生同窗口变体，不进入independent confirmation或`ENTRY_PRICE` binding；未来只允许自然前向证据成熟或新信息集假设建立新lineage。
