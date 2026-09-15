# AdvisoryPriceProspectiveEvaluationV1 F2 详细设计 v1.2

> 日期：2026-09-15
> Feature tier：F2
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.64
> 当前阶段：`PROSPECTIVE_SETTLEMENT_WAITING_INDEPENDENT_HISTORICAL_REPLAY_VERIFIED`
> 业务归属：Selection Center / Advisory
> 运行边界：只读结算不可变自然前向价格预测；不训练、不调参、不激活、不修改数据库或生产推荐

## 1. Background / 事实与问题

PR #4732 已交付 `AdvisoryPriceProspectivePredictionV1`，首个正式盘前 prediction bundle `631d5011...` 已在 T=`2026-09-15` 开盘前发布，20/20候选可用，且预测阶段未读取结果。当前缺少的是与预测通道物理分离的成熟结算：只有目标日行情完成并通过权威刷新审计后，才允许读取实际开盘价并评价中央80%开盘区间。

v3/v4 的唯一学习目标是 `target_open / decision_close - 1`。止盈、保护、止损是条件于预测 entry mid 和冻结 review policy 的规则投影，不是本模型的监督标签；本切片不得把它们合并成一个“价格准确率”，也不得读取分钟线寻找最佳点位。

## 2. Scope / 目标

1. 严格读回一个完整且自洽的 prospective request/prediction/manifest/receipt。
2. 在 T 日 `kline_daily_raw` 与 `suspend_d` 刷新审计成功前返回 typed waiting，绝不发布部分结算。
3. 以只读、REPEATABLE READ 查询冻结候选的 T 日原始开盘价和权威停牌状态。
4. 正常停牌保留为 `NOT_APPLICABLE`；刷新完成后仍无行情且无停牌证据则 typed fail closed，不删除或填零。
5. 发布不可变 per-target settlement，报告模型开盘区间的 coverage、上下越界、宽度和 mid 误差。
6. 预注册累计 confirmation 门槛；门槛未满足时只报告 `ACCUMULATING`，不产生激活建议。

### 2.1 历史回放不是自然前向门禁的从属任务

`18:00 Asia/Shanghai` 仅约束 T 日自然前向 `PROSPECTIVE_OOS` 结算，不能阻塞功能回归、PIT 历史回放或已消费窗口的导航统计。价格通道必须同时保留两条证据等级隔离的验证路径：

- 自然前向：盘前真实发布，T 日18:00及双刷新审计后结算，只积累不可回填的 `PROSPECTIVE_OOS`；
- 批量历史回放：从冻结 v4 bundle 仅读取日期、股票及三分位预测列，先发布不可变 prediction snapshot，再查询历史数据库 D 日 PIT 价格/公司行为/ST 状态及 T 日开盘/停牌；固定为 `HISTORICAL_REPLAY/NAVIGATION_ONLY`。

历史回放不读取 `entry_gap_return` 等结果列生成预测，不访问 sealed holdout，不发布 binding，也不替代自然前向确认；但它是功能与统计验证的主动首选，不得因等待实盘数据而停工。

## 3. Non-goals / 所有权边界

- 不修改 `backend/services/quantevolver/**`、QE router/registry/profile/dataset binding/composer，不提交QE实验。
- 不写 Advisory、Selection、行情或审计数据库，不写 runtime binding/descriptor/Program。
- 不重启或控制 backend、worker、scheduler、WSL、远端API或任何实验进程。
- 不修改冻结 v4 模型、校准量、阈值、预测或历史artifact；不回填未曾盘前发布的日期。
- 不评价止盈、保护、止损是否触达，不读取 high/low/close/VWAP/minute bar。
- 不在 sealed holdout 上运行；自然结算通道只消费 `PROSPECTIVE_OOS` artifact，历史回放只消费明确登记的已消费 development/test 窗口。

## 4. Architecture / 两阶段证据

### 4.1 Per-target settlement

输入目录固定为 `<model_root>/price_range_prospective_predictions/<request_id>/`。结算服务先复用预测通道的严格读回校验，再要求：

- request、prediction、manifest、receipt identity与hash完整一致；
- `evidence_level=PROSPECTIVE_OOS` 且预测receipt的四项副作用标志均为false；
- 当前时间不早于 T 日收盘后的 `18:00 Asia/Shanghai`；
- `market.dataset_date_refresh_audit` 对 T 日 `kline_daily_raw` 和 `suspend_d` 均为success且quality不属于error/empty_invalid/low_coverage；
- 数据读取只投影 `kline_daily_raw.open_li` 与 `suspend_d.suspend_type='S'`。

满足后原子发布 `<model_root>/price_range_prospective_settlements/<request_id>/`，成员为 `settlement.json`、`manifest.json`、`receipt.json`。同内容 exact retry 返回 `ALREADY_MATERIALIZED`；任一差异拒绝覆盖。

### 4.2 Aggregate confirmation

累计器只读取已结算且共享 exact `price_range_bundle_id`、package、policy、schema和objective contract 的目录，不查询数据库。预注册最低支持度：

- 至少20个不同 target trade dates；
- 至少300个 `AVAILABLE` 候选；
- `AVAILABLE + NOT_APPLICABLE = candidate_count`，未知缺失为0；
- available覆盖至少80%的目标日期，且每个计入日期至少5个AVAILABLE候选。

未满足时只输出 `ACCUMULATING` 及缺口，不计算或选择激活结论。满足后输出完整描述指标与按目标日cluster bootstrap的95%区间；本切片仍固定 `activation_recommended=false`，后续 binding 必须另立一次性 confirmation/activation 决策合同，禁止根据结果回选日期或改门槛。

### 4.3 PIT batch historical replay

历史回放输出根为 `<output_root>/price_range_historical_replays/<replay_id>/`，分成物理独立的 `prediction/` 与 `settlement/`：

1. prediction阶段验证 exact v4 bundle 和 `calibrated_test_predictions.parquet` 文件hash，只投影 D、T、symbol、calibrated q10/q50/q90 六列并先原子发布；结果标签列禁止读取。
2. settlement阶段在prediction已落盘后，按D/T批量只读数据库；D侧仅消费决策时可见的原始收盘、PIT ST/上市状态及当时已公告的目标日公司行为，T侧只作结果结算读取开盘与停牌。
3. 正常停牌原样保留为`NOT_APPLICABLE`；不明缺行保留为typed `UNAVAILABLE`并进入支持度，不删除、不填零、不阻断其他完整日期。
4. 报告模型连续gap空间和最终0.01元tick/涨跌停投影后的业务价格空间两套coverage，单独报告tick rounding rescue/harm，禁止用舍入扩宽后的coverage冒充模型校准改善。
5. request、prediction、outcome、manifest、receipt均自hash且exact retry；所有receipt固定`binding_activated=false`、`sealed_holdout_consumed=false`。

## 5. Outcome contracts / 行级语义

每个原始预测候选必须原样保留symbol和输入身份：

```text
if authoritative suspend_d row exists:
    status = NOT_APPLICABLE
elif exact T daily row exists and open_li is finite and positive:
    status = AVAILABLE
    actual_open = open_li / 1000
    covered = calibrated_low <= actual_open <= calibrated_high
elif both dataset refresh audits passed:
    fail ADVISORY_PRICE_PROSPECTIVE_OUTCOME_UNEXPLAINED_MISSING
else:
    wait ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE
```

若同一symbol/T出现重复日线、重复且冲突的停牌身份，或 AVAILABLE 与 suspend 同时存在，整批 typed fail closed。模型预测为typed unavailable的候选仍保留，但不制造实际区间指标；其实际行情状态单独记录，避免把模型缺失混成市场缺失。

## 6. Metrics / 指标与结论边界

Per-target及累计均报告：candidate/available/not-applicable/model-unavailable计数、row/date支持度、中央80% calibrated coverage、lower/upper miss rate、mean/median interval width bps、mean/median absolute mid error bps、crossing count。

区间边界使用预测artifact中的已发布未复权CNY价格，不从模型重算；actual open仅作同一未复权价格基准比较。coverage是价格分布校准指标，不是胜率、收益概率或可成交概率。任何收益、Alpha、Entry Guard、止盈/止损命中或交易执行指标均不在本合同内。

## 7. Identity / PIT与污染防护

- settlement绑定request/prediction/bundle/hash、D/T、package/model/policy/schema和数据刷新receipt身份。
- `settled_at` 必须晚于 T 日18:00，且预测 `published_at < target_open_at`。
- 只接受预测根中真实存在的artifact；不从历史RecommendationList重建预测。
- sealed holdout访问固定false；自然结算窗口写入consumed target集合，累计器拒绝重复target或混合lineage。
- 预测、结算、累计三层hash各自不可变，任何existing-content drift返回artifact conflict。

## 8. CLI / 使用方式

新增 Advisory 自有离线CLI：

```text
python -m backend.services.advisory_model_first.prospective_price_evaluation_cli settle \
  --env-file <explicit> --model-root <explicit> --request-id <exact>

python -m backend.services.advisory_model_first.prospective_price_evaluation_cli aggregate \
  --model-root <explicit> --price-range-bundle-id <exact> --output <explicit>

python -m backend.services.advisory_model_first.historical_price_replay_cli \
  --env-file <explicit> --model-root <explicit> --output-root <explicit> \
  --price-range-bundle-id <exact-v4> \
  --decision-start <YYYY-MM-DD> --decision-end <YYYY-MM-DD> \
  --replay-as-of <YYYY-MM-DD>
```

`settle`只读数据库并写artifact；`aggregate`完全离线。typed waiting使用退出码3，合同/身份/数据错误使用退出码2，成功为0。CLI不得控制进程。

历史回放CLI不检查当前钟点，不依赖当天行情刷新；它要求全部目标日早于显式`replay-as-of`。一次调用批量处理整个窗口，不逐日重建工作区或启动进程。

## 9. Error contracts

- `ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE`
- `ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID`
- `ADVISORY_PRICE_PROSPECTIVE_OUTCOME_IDENTITY_MISMATCH`
- `ADVISORY_PRICE_PROSPECTIVE_OUTCOME_UNEXPLAINED_MISSING`
- `ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ARTIFACT_CONFLICT`
- `ADVISORY_PRICE_PROSPECTIVE_CONFIRMATION_SUPPORT_INSUFFICIENT`

waiting不得伪装为失败，数据/身份错误不得伪装为waiting或无推荐。

## 10. Implementation Plan

1. 定义冻结settlement/receipt/aggregate合同和自hash规则。
2. 增加预测artifact公开严格读回入口，不复制或弱化既有校验。
3. 实现只读Postgres outcome source与refresh-audit成熟门禁。
4. 实现per-target指标、不可变原子发布和exact retry。
5. 实现离线aggregate支持度与cluster-bootstrap描述统计。
6. 增加CLI、边界测试和正式未成熟readback；不在T日18:00前读取结果。

## 11. Verification Plan

- contracts：extra forbid、hash、计数、D/T、状态和副作用字段。
- prediction input：成员缺失/hash漂移/request不一致/非PROSPECTIVE拒绝。
- clock/readiness：18:00前waiting、两项refresh audit任一缺失waiting、失败质量拒绝。
- outcome：AVAILABLE、NOT_APPLICABLE、正常停牌保留、未知缺行/重复/冲突fail closed、只查询open字段。
- metrics：边界包含、上下越界、width/mid error、模型unavailable与市场状态正交。
- artifact：原子发布、exact retry、冲突不覆盖、receipt自hash。
- aggregate：混合lineage/重复target拒绝，20日/300行/日期支持门，目标日cluster bootstrap确定性。
- CLI：0/2/3退出码、显式路径、不控制进程、不import QE。
- 真实readback：今天18:00前只允许得到 `OUTCOME_NOT_MATURE`，不得产生settlement目录。

## 12. Rollout / Rollback

源码与测试合入不需要后端重启，因为新增入口仅为离线CLI。2026-09-15 03:59 Asia/Shanghai 对首个正式request的真实readback返回 `ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE`/退出码3，且未创建settlement目录；这证明时钟门禁，不构成结果评价。首个真实settlement只能在T日18:00后且两项审计ready时另行执行；累计支持不足是正常状态。回滚仅停止CLI调用，不删除已发布预测或结算artifact，不改变数据库、binding、Program或推荐。

## 13. Risks / 风险与处置

| 风险 | 处置 |
|---|---|
| 盘中或未完整入库就结算 | T日18:00 + kline/suspend双refresh audit门禁 |
| 停牌被删除造成虚假coverage | 候选保留为NOT_APPLICABLE，独立计数 |
| 目标结果反向污染预测 | prediction与settlement分根、先验证盘前published_at、禁止重建 |
| 规则价格被冒充模型准确率 | 只评价entry gap区间，不评价TP/SL/protective |
| 多日行被当独立样本夸大显著性 | 目标日cluster bootstrap，报告date支持度 |
| 为通过门槛回选日期 | target集合不可变、门槛预注册、禁止重复/混合lineage |

## 14. Production Gates

- DDL/DML：`noop`。
- 后端重启/runtime activation：`noop`。
- QE代码/实验：`noop`。
- 模型训练/校准/binding：`noop`。
- 首次真实outcome读取：T日18:00后且双refresh audit成功，作为独立运行状态报告。
- 任何price-role binding：不在本切片，仍需独立confirmation结论与用户后续流程。

## 15. Design Acceptance Index

| design_item | acceptance |
|---|---|
| F-389 | 只接受完整自洽且盘前发布的PROSPECTIVE_OOS预测artifact |
| F-390 | 结算时钟固定T日18:00 Asia/Shanghai之后 |
| F-391 | kline_daily_raw和suspend_d双refresh audit成熟门禁 |
| F-392 | 数据库只读REPEATABLE READ且只投影T日open与停牌身份 |
| F-393 | 停牌保留NOT_APPLICABLE，未知缺行和冲突typed fail closed |
| F-394 | 模型unavailable与市场outcome状态正交保留 |
| F-395 | 只评价entry-gap价格区间，不评价规则TP/SL或分钟执行 |
| F-396 | per-target settlement原子不可变、exact retry、冲突拒绝 |
| F-397 | settlement身份绑定预测、模型、政策、数据审计与D/T |
| F-398 | aggregate拒绝重复target和混合lineage |
| F-399 | 20日/300行/日期支持度门槛预注册且不足只ACCUMULATING |
| F-400 | 累计推断按目标日cluster bootstrap，不按股票行伪独立 |
| F-401 | 本切片固定不建议激活，binding另立一次性合同 |
| F-402 | CLI退出码区分成功/错误/waiting且不控制进程或QE |
| F-403 | 18:00只约束自然前向，不阻塞批量历史功能/统计验证 |
| F-404 | 历史prediction先冻结且禁止读取结果列，settlement后读历史DB |
| F-405 | 历史回放固定HISTORICAL_REPLAY/NAVIGATION_ONLY且不读sealed、不激活 |
| F-406 | 正常停牌与不明缺行均保留，不删除；支持度分开统计 |
| F-407 | 同时报模型连续空间与业务tick投影空间指标并披露rounding影响 |

## 16. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-389 | `backend/services/advisory_model_first/prospective_price_prediction.py::read_prospective_prediction_artifact`；`prospective_price_evaluation.py` | `backend/tests/advisory_model_first/test_price_range_prospective.py`；`test_price_range_prospective_evaluation.py` | IMPLEMENTED_VERIFIED | none |
| F-390 | `backend/services/advisory_model_first/prospective_price_evaluation_contracts.py`；evaluation service | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation_contracts.py`；2026-09-15 03:59真实waiting readback | IMPLEMENTED_WAITING_VERIFIED | approved_by_user: settlement需T日18:00后 |
| F-391 | `backend/services/advisory_model_first/prospective_price_evaluation.py::PostgresAdvisoryPriceOutcomeSource` | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation_boundaries.py`双审计测试 | IMPLEMENTED_VERIFIED | approved_by_user: 真实DB读取只在合法成熟时钟后执行 |
| F-392 | `backend/services/advisory_model_first/prospective_price_evaluation.py::PostgresAdvisoryPriceOutcomeSource` | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation_boundaries.py` SQL/readonly测试 | IMPLEMENTED_VERIFIED | none |
| F-393 | `backend/services/advisory_model_first/prospective_price_evaluation.py::_settle_candidate` | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation.py`；boundary停牌/缺行/冲突测试 | IMPLEMENTED_VERIFIED | none |
| F-394 | `backend/services/advisory_model_first/prospective_price_evaluation_contracts.py` settlement row | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation_contracts.py`正交状态与自计算测试 | IMPLEMENTED_VERIFIED | none |
| F-395 | `backend/services/advisory_model_first/prospective_price_evaluation.py::_settlement_metrics` | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation_boundaries.py` SQL forbidden字段测试 | IMPLEMENTED_VERIFIED | none |
| F-396 | `backend/services/advisory_model_first/prospective_price_evaluation.py::_publish_settlement` | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation.py` exact retry与tamper测试 | IMPLEMENTED_VERIFIED | none |
| F-397 | `backend/services/advisory_model_first/prospective_price_evaluation_contracts.py` receipt | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation_contracts.py` identity/hash测试 | IMPLEMENTED_VERIFIED | none |
| F-398 | `backend/services/advisory_model_first/prospective_price_confirmation.py` | `backend/tests/advisory_model_first/test_price_range_prospective_confirmation.py` lineage测试 | IMPLEMENTED_VERIFIED | none |
| F-399 | `backend/services/advisory_model_first/prospective_price_confirmation.py` support gate | `backend/tests/advisory_model_first/test_price_range_prospective_confirmation.py` 20日/300行测试 | IMPLEMENTED_VERIFIED | approved_by_user: 自然样本按交易日积累，不回填 |
| F-400 | `backend/services/advisory_model_first/prospective_price_confirmation.py::_cluster_bootstrap` | `backend/tests/advisory_model_first/test_price_range_prospective_confirmation.py`确定性bootstrap测试 | IMPLEMENTED_VERIFIED | none |
| F-401 | `backend/services/advisory_model_first/prospective_price_evaluation_contracts.py::AdvisoryPriceProspectiveConfirmationV1` | `backend/tests/advisory_model_first/test_price_range_prospective_confirmation.py` activation=false测试 | IMPLEMENTED_VERIFIED | approved_by_user: binding保持独立后续门禁 |
| F-402 | `backend/services/advisory_model_first/prospective_price_evaluation_cli.py` | `backend/tests/advisory_model_first/test_price_range_prospective_evaluation_cli.py`退出码/边界测试 | IMPLEMENTED_WAITING_VERIFIED | approved_by_user: 真实readback退出码3且零settlement |
| F-403 | `backend/services/advisory_model_first/historical_price_replay_cli.py` | `backend/tests/advisory_model_first/test_historical_price_replay_cli.py`；artifact `F:/Dev/AIstock_model_artifacts/advisory_model_first/price_range_historical_replays/advprhist_23e8ce5a20b6a598bf129a6d/` | IMPLEMENTED_VERIFIED | none |
| F-404 | `backend/services/advisory_model_first/historical_price_replay.py::_freeze_predictions`及两阶段目录 | `backend/tests/advisory_model_first/test_historical_price_replay.py`先落盘/禁止label测试 | IMPLEMENTED_VERIFIED | none |
| F-405 | `backend/services/advisory_model_first/historical_price_replay_contracts.py` | `backend/tests/advisory_model_first/test_historical_price_replay_contracts.py`；真实receipt `advprhist_23e8ce5a...` | IMPLEMENTED_VERIFIED | none |
| F-406 | `backend/services/advisory_model_first/historical_price_replay.py::PostgresHistoricalPriceReplaySource`及`_settle_day` | `backend/tests/advisory_model_first/test_historical_price_replay.py`停牌/未知缺行保留；真实artifact 1600/1600完整 | IMPLEMENTED_VERIFIED | none |
| F-407 | `backend/services/advisory_model_first/historical_price_replay_contracts.py::AdvisoryHistoricalPriceReplayMetricsV1` | `backend/tests/advisory_model_first/test_historical_price_replay_contracts.py`；真实artifact model/business coverage `0.733125/0.81375`、rounding rescue/harm `129/0` | IMPLEMENTED_VERIFIED | none |

## 17. DESIGN-COMPLIANCE-001

交付前逐项核对F-389～F-407；源码、waiting、历史批量回放、单日自然artifact、累计描述统计、activation与runtime分别报告；仅在完整实现和真实边界验证均通过后报告本切片完成。
