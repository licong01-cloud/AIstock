# AdvisoryPriceProspectivePredictionV1 F2 详细设计 v1.1

> 日期：2026-09-15
> Feature tier：F2
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.61
> 当前阶段：`SOURCE_IMPLEMENTED_LOCAL_VERIFIED_FORMAL_CAPTURE_PENDING`
> 业务归属：Selection Center / Advisory
> 运行边界：日频自然前向预测收集；不绑定生产 descriptor，不读取目标日结果，不研发分钟执行

## 1. Background / 事实与问题

`AdvisoryDailyPriceEnvelopeV1` 三头 v3 模型及 validation-only v4 校准 artifact 已完成。v4 在 validation/test 的中央80%区间覆盖率为 `0.811702/0.733125`，校准扩张量为0，当前固定结论为 `FRESH_CONFIRMATION_REQUIRED`、`activation_recommended=false`。因此不能发布新 price binding，也不能把已消费 test 继续调优成激活证据。

2026-09-15 的无落盘可产性 spike 使用冻结 M1 父 bundle `9cf14e80...`、M3 outcome bundle `17ce7ceb...` 与 v4 price bundle `508fedfe...`，对最新已发布 Program `advp_3126...` 的 target `2026-09-14` 完成 20/20 预测。该日期早于正式前向起点，只证明当前 Program 候选、103特征和三个 bundle 的技术兼容，不构成自然前向证据。

源码实现后的第二次无落盘预检在 `2026-09-15 09:30 Asia/Shanghai` 前解析到 target `2026-09-15` 的已发布列表 `advlv_536a...`、SelectionRun `sel_1586...` 与唯一 20 只模型候选。该预检只验证请求可产性，未写 request/prediction artifact；正式 capture 必须使用已合入源码并再次确认无 QE 实验并行。

缺口是一个独立于生产 binding 的预测收集通道：模型冻结后，每个真实未来交易日只根据 D 时点可见的已发布荐股输入生成 T 日价格信封，写入不可变 artifact；在预测阶段不得读取 T 日行情、标签、收益、coverage 或任何 outcome evaluation。

## 2. Scope / 目标

1. 冻结一次 prospective request，明确 Program/list/review/Selection、package、M1/M3/v4、policy、D/T 与生成时钟身份。
2. 通过只读数据库和冻结 bundle 生成 `AdvisoryDailyPriceEnvelopeV1`，不读取或改变当前 P0-D descriptor。
3. 原子发布 request、prediction 和 receipt，并支持同身份 exact retry。
4. 任何缺数据、身份漂移、目标日已开盘、模型冻结前日期或重复内容冲突均 typed fail closed。
5. 明确预测 artifact 不是激活证据；未来成熟评价必须另立一次性 confirmation 合同。

## 3. 非目标与所有权

- 不发布或修改 `shadow_bindings`、`outcome_bindings`、`price_range_bindings`、`program_bindings`。
- 不修改 Advisory Program、RecommendationList、SelectionRun、数据库、调度器或后端进程。
- 不读取 T 日 OHLC、收益、标签、胜率、coverage、越界率或成熟 outcome。
- 不回填 v4 冻结时刻之前的目标日，不把 2026-09-14 spike 写成 artifact。
- 不调用 TDX/行情实时源；日频输入继续使用数据库 D 时点 PIT 数据。
- 不修改或调用 QE 公共源码，不提交 QE 训练/回测，不与 QE 实验并行。
- 不在 UI/API 中展示为已激活价格角色；本切片仅交付离线日频预测收集与 CLI。

## 4. Architecture / 时钟与证据边界

时钟为 `D_CLOSE_TO_T_OPEN_V1`：

- `decision_as_of_trade_date=D`，`target_trade_date=T`，且 T 是 D 后的下一交易日。
- request/prediction 的 `created_at` 必须晚于冻结 v4 的 `model_frozen_at`，并严格早于 T 日 Asia/Shanghai `09:30`。
- request 只能引用已持久化的 T 日 RecommendationList、其 ReviewRun 和唯一 SelectionRun。
- `evidence_level=PROSPECTIVE_OOS`，`realized_outcome_accessed=false`，`sealed_holdout_consumed=false`。
- 模型冻结日之前和已开盘目标日一律拒绝；exact retry 只允许完全相同 request/prediction 内容。
- 预测收集与 sealed holdout confirmation 分离；本切片不读取、消费或写入 sealed holdout consumption receipt。

## 5. Contracts / 冻结请求合同

`FrozenAdvisoryPriceProspectiveRequestV1` 至少包含：

- `request_id/request_sha256/schema_version/created_at/model_frozen_at`；
- `program_id/binding_version_id/list_version_id/review_run_id/selection_run_id`；
- `decision_as_of_trade_date/target_trade_date/target_open_at`；
- `package_id/manifest_sha256/style_profile_hash/selection_runtime_semantics_hash`；
- `parent_bundle_id/parent_bundle_manifest_sha256`；
- `outcome_bundle_id/outcome_bundle_manifest_sha256`；
- `price_range_bundle_id/price_range_bundle_manifest_sha256`；
- `feature_schema_version/feature_schema_hash/review_policy_sha256`；
- `component_roles/terminal_weights`；
- 固定 `objective_contract=RISK_MANAGED_ADVISORY`、`evidence_level=PROSPECTIVE_OOS`、`realized_outcome_access_allowed=false`。

request hash 包含 `created_at/model_frozen_at/target_open_at`，但不包含可变输出路径；`request_id=advprpros_<hash前24位>`。因此生成时间也是不可变证据，不能通过改时间复用同一 request identity。

## 6. 只读 bundle 加载

新增两个显式研究加载入口：

- `load_frozen_outcome_bundle(...)` 按显式 outcome bundle ID 与父 bundle/package/manifest/style/schema 身份加载，不读取 outcome binding；
- `load_frozen_price_range_bundle(...)` 按显式 v4 ID 与 package/parent/outcome/manifest/style/schema 身份加载，不读取 price binding。

两个入口复用现有严格 manifest/member hash/model 校验，只接受 `EXPERIMENTAL_SHADOW`，不发布任何 binding。M1 父模型复用既有 `load_frozen_research_bundle(...)`。

## 7. 预测执行

`AdvisoryPriceProspectivePredictionService.capture(request)`：

1. 重新读取 Program active binding、目标日 RecommendationList、ReviewRun 与 SelectionRun，并逐字段匹配 request；
2. 由 request 构造只读冻结 resolution，注入现有 `AdvisoryModelShadowService`；
3. 显式加载冻结 M1/M3/v4，不解析或修改当前 P0-D descriptor；
4. 复用现有 D 时点 `PostgresRealtimeFeatureSource`、103特征 builder、M3 outcome projection 和 v4 price scorer；
5. 只保留 price envelope 及最小输入身份，不发布 M1 rerank 或 M3 outcome 数值；
6. 检查模型实际消费的 Selection Top20 候选身份与列表及股票池投影一致、D/T一致、price bundle exact、目标结果字段不存在；推荐列表中的既有持仓、退出等生命周期记录不计作模型候选；
7. 写入不可变 artifact，读回并验证 hash。

任一候选日频上下文正常缺失时保留该候选的 typed unavailable；整个输入身份或必需特征不完整时整次请求失败，不静默回退规则区间。

## 8. Artifact 与幂等

根目录：`<model_root>/price_range_prospective_predictions/<request_id>/`。目录名先由冻结请求唯一确定，目录内 manifest 再绑定由 request hash 与 prediction hash 共同生成的 `prediction_bundle_id`。

成员固定为：

- `request.json`
- `prediction.json`
- `receipt.json`
- `manifest.json`

`prediction_bundle_id` 由 request hash 与 canonical prediction payload 决定。发布先写同父目录临时目录，逐文件 fsync/读回后原子 rename；已存在且完全相同则返回 `ALREADY_MATERIALIZED`，任一差异返回 `ADVISORY_PRICE_PROSPECTIVE_ARTIFACT_CONFLICT`，不覆盖、不删除旧 artifact。T日开盘后只允许读回已经在开盘前发布的 exact artifact，不允许首次计算或重新计算。

receipt 报告 `receipt_sha256`、候选数、available/unavailable 数、D/T、生成时间、耗时、所有输入 bundle/hash、`realized_outcome_accessed=false`、`binding_activated=false`、`database_written=false`、`sealed_holdout_consumed=false`。自校验 hash 覆盖除展示态 `status` 外的全部 receipt 功能字段；禁止出现 realized return、label、coverage、hit rate 或 winner 字段。

## 9. Implementation Plan / CLI 与实施顺序

新增 Advisory 自有 CLI `python -m backend.services.advisory_model_first.prospective_price_cli`：

- `prepare`：从已发布 T 日列表和显式 bundle ID 生成不可变 request；
- `capture`：读取 request 并生成 prediction bundle；
- 两个命令必须显式传入同一 `--model-root` 与 `--env-file`，不从当前目录猜测配置；
- 命令只读数据库并写模型 artifact，不启动/停止/重启任何服务。

## 10. 错误合同

至少包含：

- `ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID`
- `ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH`
- `ADVISORY_PRICE_PROSPECTIVE_BUNDLE_INVALID`
- `ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ACCESS_FORBIDDEN`
- `ADVISORY_PRICE_PROSPECTIVE_ARTIFACT_CONFLICT`
- 复用现有日频行情、必需特征和候选组 typed reason code。

不得将身份错误、数据库查询失败、bundle损坏或模型失败转换为 `NO_ELIGIBLE_RECOMMENDATION`。

## 11. Verification Plan / 测试与真实 spike

- 合同：hash、D/T、T开盘前、模型冻结前、禁止 outcome 字段、extra forbid。
- loader：不读取 binding、identity/hash/member损坏拒绝、v4三头/校准严格读回。
- service：冻结身份一致成功；当前 P0-D descriptor 不被读取；目标行情 poison 不影响预测；已开盘/回填/列表漂移拒绝；候选级正常缺失保留。
- artifact：原子写、exact retry、冲突不覆盖、receipt 禁止结果字段。
- CLI：prepare/capture 参数与退出码。
- 实际正式前向只能选择 v4 冻结后且执行时尚未开盘的真实 T；若当前没有这样的已发布列表，源码交付后状态为 `WAITING_NEXT_PUBLISHED_TARGET`，不得回填。

## 12. Rollout / Rollback

源码合入本身不要求重启，因为 CLI 是离线入口且不挂接 FastAPI/scheduler。首次正式 capture 前需再次确认无 QE 实验并行。回滚仅停止运行 CLI；不删除已发布 request/prediction，不改变任何 runtime binding、Program、数据库或推荐结果。

本切片完成条件：F-374～F-388 全部实现并验证；F2 validator、定向测试、lint/compile、CI通过；至少完成无落盘历史可产性 spike。若没有尚未开盘的新 T 日列表，允许以 `WAITING_NEXT_PUBLISHED_TARGET` 结束，不把等待自然日期视为源码缺口。

## 13. Risks / 风险与处置

| 风险 | 处置 |
|---|---|
| 历史日期被冒充自然前向 | 模型冻结时钟与T日开盘门禁；正式artifact禁止回填 |
| 预测中偷看目标结果 | 请求与receipt固定`realized_outcome_accessed=false`；接口不接收目标行情或标签 |
| 离线通道悄悄改变生产输出 | 不读取/写入任何binding或descriptor；输出根与生产Program分离 |
| 已消费test继续调优 | v4模型/校准完全冻结；本切片只生成未来预测，不计算coverage |
| 正常停牌被删除 | 复用候选级typed unavailable并保留候选身份 |
| artifact半写或重算漂移 | 临时目录原子发布、逐成员hash、exact retry与冲突拒绝 |
| 与QE争用资源 | 每次正式capture前检查无QE实验；不导入或修改QE公共代码 |

## 14. Production Gates / 生产影响

- DDL/DML：`noop`。
- 后端重启：`noop`；新增CLI不挂接FastAPI或scheduler。
- runtime binding/descriptor：`noop`。
- 模型训练：`noop`；只消费已冻结M1/M3/v4。
- 首次正式预测artifact：源码合入后、存在尚未开盘的真实T日列表且无QE实验运行时执行。
- 未来confirmation、binding、运行时激活和用户重启均为独立后续门禁。

## 15. Design Acceptance Index

| design_item | acceptance |
|---|---|
| F-374 | request冻结Program/list/review/Selection与D/T身份 |
| F-375 | request冻结M1/M3/v4及package/policy/schema身份 |
| F-376 | 生成时间晚于模型冻结且早于T日开盘，禁止回填 |
| F-377 | evidence固定PROSPECTIVE_OOS且结果访问关闭 |
| F-378 | M1显式研究加载不读取runtime binding |
| F-379 | M3显式加载不读取或修改outcome binding |
| F-380 | v4显式加载不读取或修改price binding |
| F-381 | 复用当前Program已持久化输入和D时点103特征 |
| F-382 | 预测不解析或修改当前P0-D descriptor |
| F-383 | 输出只含price envelope和最小输入身份 |
| F-384 | 候选级正常缺失保留，系统错误typed fail closed |
| F-385 | artifact原子不可变、exact retry、冲突拒绝 |
| F-386 | receipt声明零outcome/零binding/零DB/零holdout消费 |
| F-387 | CLI prepare/capture不控制进程且不触碰QE |
| F-388 | 历史spike不冒充正式自然前向；无新T时WAITING |

## 16. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-374 | `backend/services/advisory_model_first/prospective_price_contracts.py` | `backend/tests/advisory_model_first/test_price_range_prospective_contracts.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-375 | `backend/services/advisory_model_first/prospective_price_contracts.py` | `backend/tests/advisory_model_first/test_price_range_prospective_contracts.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-376 | `backend/services/advisory_model_first/prospective_price_contracts.py` | `backend/tests/advisory_model_first/test_price_range_prospective_contracts.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-377 | `backend/services/advisory_model_first/prospective_price_contracts.py` | `backend/tests/advisory_model_first/test_price_range_prospective_contracts.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-378 | `backend/services/advisory_model_first/model_bundle.py` | `backend/tests/advisory_model_first/test_model_bundle.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-379 | `backend/services/advisory_model_first/outcome_runtime_bundle.py` | `backend/tests/advisory_model_first/test_outcome_runtime_bundle.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-380 | `backend/services/advisory_model_first/price_range_runtime_bundle.py` | `backend/tests/advisory_model_first/test_price_range_runtime_bundle.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-381 | `backend/services/advisory_model_first/prospective_price_prediction.py` | `backend/tests/advisory_model_first/test_price_range_prospective.py`；2026-09-15目标日无写入请求预检 | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-382 | `backend/services/advisory_model_first/prospective_price_prediction.py` | `backend/tests/advisory_model_first/test_price_range_prospective.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-383 | `backend/services/advisory_model_first/prospective_price_prediction.py` | `backend/tests/advisory_model_first/test_price_range_prospective.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-384 | `backend/services/advisory_model_first/prospective_price_prediction.py` | `backend/tests/advisory_model_first/test_price_range_prospective.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-385 | `backend/services/advisory_model_first/prospective_price_prediction.py` | `backend/tests/advisory_model_first/test_price_range_prospective.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-386 | `backend/services/advisory_model_first/prospective_price_contracts.py`；`backend/services/advisory_model_first/prospective_price_prediction.py` | `backend/tests/advisory_model_first/test_price_range_prospective_contracts.py`；`backend/tests/advisory_model_first/test_price_range_prospective.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-387 | `backend/services/advisory_model_first/prospective_price_cli.py` | `backend/tests/advisory_model_first/test_price_range_prospective_cli.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-388 | §1、§4、§11～§12 | `backend/tests/advisory_model_first/test_price_range_prospective.py`；历史20/20可产性spike；2026-09-15未来目标无写入请求预检 | IMPLEMENTED_LOCAL_VERIFIED | none |

## 17. DESIGN-COMPLIANCE-001 交付检查

交付前逐项确认：完整实现F-374～F-388；不把简化、静态或替代实现报告为完整交付；无静默规则回退；测试不替代真实artifact；源码、预测artifact、未来评价、binding、重启和业务有效性分别报告。
