# Advisory Phase 1R R5 API、UI 与 Legacy Cutover F2 详细设计

> 日期：2026-07-27
> 文档类型：F2 实施级详细设计
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 父设计：`docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md`
> 上游交付：R1 contracts/schema/repository、R2 candidate adapter、R3 ordered day executor、R4 outcome/summary/retrospective SEALED bridge
> 当前状态：`reviewed_design_ready_second_formal_audit_passed`
> 研究边界：`HISTORICAL_RANGE_RESEARCH`、`RETROSPECTIVE_RESEARCH_ONLY`、`execution_prohibited=true`

## 1. 背景与当前事实

Phase 1R R1-R4 已形成完整的历史范围研究业务内核。R3 可以从显式历史起点按交易日顺序执行一个或多个独立 Program，生成 candidate、ENTER/HOLD/EXIT/WATCH、list version、episode snapshot 和 DAY/RANGE receipt；R4 可以追加 candidate/episode/list/range outcome、summary version，并把精确成功事实投影为与 formal OOS 永久隔离的 retrospective SEALED snapshot。

R4 已由 PR `#2792` 合入，BUG close-sync 已由 PR `#2793` 合入。真实历史业务证据包括：

1. 单 Alpha 与原生多 Alpha 父包各 15 个交易日的独立执行；
2. 32,549 条 outcome、4 个 summary；
3. 2 个非空 `RETROSPECTIVE_RESEARCH_ONLY` SEALED snapshot；
4. 360 条 source correction 和闭合 predecessor chain；
5. bridge exact retry 零新增事实；
6. Selection、当前 Advisory、Paper、Simulation、QE/Qlib/QMT 零业务写入。

当前缺口不在算法、证据链或数据库 schema，而在产品消费层：

- `backend/routers/advisory.py` 的 `/research-batches` 仍是 Phase 0A.2D 单日 `MANUAL_HISTORICAL_RESEARCH` API，不是 Phase 1R 日期范围资源；
- R1-R4 没有一个面向 HTTP 的 typed application facade，把 create、planning、execute、resume、cancel、outcome refresh、dataset bridge 和查询投影组合为一致契约；
- 现有 `frontend/src/app/paper-v2/advisory/page.tsx` 仍以当前荐股和 legacy replay 为主，没有正式“历史验证”视图；
- `frontend/src/lib/api/advisory.ts` 只提供 legacy replay/current Advisory contracts，且通用错误处理会丢失 reason code、context 和 retryable 语义；
- legacy replay 与 Phase 1R 在页面上尚未形成明确的产品 cutover。

R5 只解决上述消费层缺口。R5 不重写 R1-R4，不复制第二套历史研究算法，也不把 UI 需求下沉到 Selection、Paper 或模拟盘。

## 2. Scope / 范围与目标

R5 必须一次性交付以下完整能力：

1. 为 Phase 1R 建立 typed HTTP API，覆盖 batch 创建、列表、详情、runs、days、lists、outcomes、summaries、resume、cancel、refresh outcomes 和 dataset bridge。
2. 创建请求立即返回 durable identity，由显式命令驱动 bounded background execution；HTTP 线程不扫描多年历史分区、不同步运行整个日期范围。
3. 新增稳定 keyset 分页和 query projection，长日期范围不通过 offset 或一次性全量 JSON 返回。
4. 在现有荐股页面新增“历史验证”tab，支持多个 Program 独立运行；每个 Program 只绑定一个单 Alpha 包或一个原生多 Alpha 父包。
5. 页面展示逐日列表演进、候选排名、ENTER/HOLD/EXIT/WATCH、episode、outcome maturity、summary 和 retrospective snapshot 状态。
6. 页面支持恢复、取消、收益刷新和生成 retrospective dataset bridge；这些是业务命令，不是审批或人工放行。
7. 从主流程移除 legacy replay 创建入口，但保留旧 API 和旧数据只读兼容，不迁移、不删除、不向新链路 fallback。
8. 使用真实 API/UI E2E 证明单 Alpha、原生多 Alpha、错误状态、恢复、exact retry 和跨模块隔离。

## 3. 非目标

R5 不包含：

- 新数据库表、字段、索引、trigger、DDL 或 DML repair；R1-R4 schema 已满足 R5；
- 修改 candidate、排名、ENTER/HOLD/EXIT/WATCH、replacement、outcome、summary 或 bridge 算法；
- Phase 0B 候选质量审计、模型训练、收益预测、持股周期预测、买入/止盈/止损区间；
- Windows 或 WSL 模型训练；模型训练仍属于后续 Phase 0B/3 且必须在 WSL Conda 环境运行；
- 发布 `RERANK_READY`、`RETURN_HORIZON_READY`、`PRICE_RANGE_READY` 或任何用户可见已校准模型数值；
- 当前荐股列表发布、自动 daily scheduler、模拟盘、Paper、QMT、订单、账户、持仓或交易执行；
- 策略包 health、asset、model、factor 二次验证或重新准入；
- 角色、RBAC、审批、双人复核、人工放行、canary、champion/challenger 或 ModelOps 门禁；
- 把最新交易日、候选数量、全部 horizon 成熟、最小 Program 数或数据复制到 DEV 作为任务创建条件；
- 自动迁移 `app.advisory_replay_run`、`run_type=REPLAY` 或 `version_status=REPLAY`；
- 物理删除 legacy API、legacy 表或历史事实。

## 4. 权威与固定决策

### 4.1 权威顺序

1. 用户已确认的学术历史研究、无交易执行、无额外审批/门禁边界；
2. 父级蓝图 Phase 1R；
3. Phase 1R 父设计的稳定 Design Acceptance Index；
4. R3、R4 子设计和已合入源码合同；
5. 本文锁定的 R5 API/UI 实施细节。

若本文与旧 replay 文档冲突，以本文为准；若本文与 R1-R4 typed domain contract 冲突，以已合入的 R1-R4 contract 为准并修订本文，禁止在实现中静默改写 domain contract。

### 4.2 固定业务决策

- 同一个 batch 可以包含多个 Program；每个 Program 形成独立 `range_run_id`、day chain、list、episode、outcome 和 summary。
- 一个 Program 只接受一个已准入单 Alpha 包或一个已准入原生多 Alpha 父包。页面不提供跨包权重、手工腿选择或随机多包融合。
- Existing Program 使用 `program_id + expected_program_version + expected_binding_version_id`；research-only Program 使用一个 `package_id` 和显式 config。两者都由现有 resolver 冻结 identity，不调用 package admission/health service。
- 开始和结束日期只要求是数据库能够按权威交易日历解析的已完成历史区间，不依赖当前最新交易日。
- 范围首日使用空 active seed；绝不读取当前 `PUBLISHED` list。
- 新 UI 只读取 Phase 1R 表和 exact artifact metadata；不读取 legacy replay 作为替代数据。
- 页面只展示研究事实和历史结果，最终是否人工买入由用户决定；页面没有下单、创建模拟盘或同步交易账户按钮。

### 4.3 现有与新资源不得混淆

| 资源 | 语义 | R5 处置 |
|---|---|---|
| `POST /api/v1/advisory/research-batches` | Phase 0A.2D 单日手工历史研究 | 保留原样，不改名、不返回 Phase 1R 数据 |
| `POST /api/v1/advisory/programs/{id}/replay` | legacy 同步 replay 诊断 | 保留兼容并标记 deprecated/`legacy_diagnostic=true` |
| `/api/v1/advisory/historical-range-*` | Phase 1R 正式历史范围研究 | R5 新增的唯一产品 API |
| 当前 Advisory review/list/episode | 当前荐股流程 | R5 只读 Program identity，不写当前列表 |

任何一条链失败都不得调用另一条链重试或伪造成功。

## 5. 目标架构

```text
Advisory page / HistoricalRangeResearchView
  -> frontend/src/lib/api/advisory.ts typed Phase 1R client
  -> backend/routers/advisory.py thin HTTP adapter
  -> HistoricalRangeApplicationService
       -> HistoricalRangeQueryService
            -> PostgresHistoricalRangeQueryRepository (read-only projections)
       -> HistoricalRangePlanningService                 # CREATE/catalog/seal
       -> HistoricalRangeBatchExecutionService           # execute/resume/cancel
       -> HistoricalRangeOutcomeApplicationService       # refresh outcomes/summary
       -> HistoricalRangeDatasetBridgeApplicationService # retrospective bridge
       -> ResponseBoundHistoricalRangeDispatcher          # explicit command only
  -> app.advisory_historical_range_* tables + exact CAS refs

Legacy replay API
  -> AdvisoryProgramService.run_replay
  -> legacy replay tables only
  X no import/call/fallback to HistoricalRangeApplicationService
```

### 5.1 模块依赖规则

- Router 只依赖 R5 API models、application service 和 error projection。
- R5 application service 只能组合 `advisory_historical_range` 内部 R1-R4 services，以及 StrategyPackage 的只读 Program/package projection resolver。
- R5 不 import `selection_center.service`、`simulation_runtime.selection`、Paper、QE、Qlib、QMT 或交易模块。
- Query repository 使用 caller-owned `.env` connection factory 和只读事务；不猜测数据库连接。
- CAS root、policy registry、Phase 1 dataset root 等路径继续由显式环境/config composition 提供；API body 不接收路径、SQL、table、URI 或 artifact root。
- R5 不更改 `backend/main.py` scheduler 集合，也不注册日期扫描器。

## 6. Application Facade 与显式命令执行

### 6.1 `HistoricalRangeApplicationService`

新增 `backend/services/advisory_historical_range/service.py`，作为 R5 唯一 HTTP application facade，公开：

```text
create_batch(request, idempotency_key, background_tasks)
list_batch_options(cursor, limit)
list_batches(filters, cursor, limit)
get_batch(batch_id)
list_runs(batch_id, cursor, limit)
list_operations(batch_id, cursor, limit)
get_operation(operation_id)
get_run(range_run_id)
list_days(range_run_id, cursor, limit)
get_day(range_run_id, trade_date, candidate_cursor, candidate_limit)
get_list(range_run_id, trade_date, item_cursor, item_limit)
list_outcomes(range_run_id, filters, cursor, limit)
list_summaries(range_run_id, cursor, limit)
resume_batch(batch_id, command, background_tasks)
cancel_batch(batch_id, command, background_tasks)
refresh_outcomes(batch_id, command, background_tasks)
build_dataset_bridge(batch_id, command, background_tasks)
```

Facade 不重新实现 domain 状态机。所有 mutation 委托给 R1-R4 service；所有查询委托给只读 query service。

### 6.2 `ResponseBoundHistoricalRangeDispatcher`

R5 使用 FastAPI `BackgroundTasks` 作为显式命令的 response-bound dispatcher：

1. HTTP adapter 先在短事务中创建或取得 durable batch/operation；
2. 返回 `202` 前把 exact `operation_id` 对应 callable 注册到当前响应的 background tasks；
3. response 返回后执行现有 bounded planning/executor/outcome/bridge service，直到 terminal 或真实 `WAITING_INPUT/RETRYABLE_FAILED/PARTIAL` 边界；
4. dispatcher 不按日期或数据库状态自动发现任务，不是 daily scheduler；
5. 进程退出时未完成工作保留 durable operation/day attempt；用户点击 resume 或 exact retry 恢复；
6. background callable 的顶层异常必须 `LOGGER.exception` 并由 domain service 写入可恢复/失败 receipt；不得只记录字符串后吞掉异常。

显式 DB/root/registry composition 必须在持久化新 command 前完成；配置缺失时返回结构化 `503` 且零业务写入。operation 已持久化后的异常必须优先写 terminal/retryable attempt receipt；若数据库连接中断导致失败 receipt 暂时不可提交，operation 保持带 lease 的 RUNNING，租约到期后查询投影明确显示 `lease_expired=true` 并允许正式恢复，不能由 Router 或 UI 改写成成功。

Background task 只捕获 immutable `batch_id/operation_id/request_hash` 和可重新构造的 composition config，不捕获或复用 request-scoped DB connection、cursor、transaction、FastAPI request、response 或前端 session 对象。每个 background stage 使用 connection factory 打开自己的短事务并在 `finally` 释放资源。

创建响应已返回但 background task 尚未 claim 就发生进程退出时，不得留下无法恢复的 PLANNING batch：

- 相同 create body + `Idempotency-Key` exact retry 必须重新 dispatch 既有 `BUILD_SOURCE_CATALOG` operation，不创建新 batch；
- `POST resume` 在 batch 仍为 `PLANNING` 且 catalog operation 为 `QUEUED/WAITING_INPUT/expired RUNNING` 时，读取 persisted requirement-plan/request artifact 并继续既有 catalog operation；不创建需要 sealed request hash 的 R3 `RESUME` operation；
- UI 在 batch seal 前把 create semantic body 与 key 保存在 sessionStorage；即使浏览器状态丢失，服务端 planning resume 仍可仅凭 batch 和既有 artifact 恢复；
- 并发 create retry/resume 由 catalog claim、lease 和 fencing 收敛为一个有效 worker，其余调用返回当前 operation，不重复扫描或 seal。

不增加业务性的并发上限、日期上限或审批。底层 R3/R4 的 bounded slice、lease、fencing 和已有吞吐参数继续只控制资源，不改变任务是否可接受。

### 6.3 创建到完成的正向流程

```text
POST create
  -> validate client fields
  -> planning_service.create()
  -> return 202 PLANNING + CREATE/BUILD_SOURCE_CATALOG operation
  -> background: catalog bounded chunks
       -> WAITING_INPUT: persist exact unresolved reasons and stop
       -> seal: create independent Program runs
  -> background: execute_until_blocked()
       -> each Program/day commits independently
       -> terminal COMPLETED/PARTIAL/WAITING_INPUT/FAILED/CANCELLED
  -> UI polling reads query projections
  -> explicit refresh-outcomes
  -> summary versions
  -> explicit build-dataset-bridge
  -> SEALED retrospective snapshot or VALID_EMPTY receipt
```

R5 不自动执行 outcome refresh 或 dataset bridge。二者是用户可见显式命令，不是创建 batch 的隐藏副作用。

## 7. Contracts / HTTP API 契约

### 7.1 Endpoint 清单

```text
GET    /api/v1/advisory/historical-range-options
GET    /api/v1/advisory/historical-range-batches
POST   /api/v1/advisory/historical-range-batches
GET    /api/v1/advisory/historical-range-batches/{batch_id}
GET    /api/v1/advisory/historical-range-batches/{batch_id}/runs
GET    /api/v1/advisory/historical-range-batches/{batch_id}/operations
POST   /api/v1/advisory/historical-range-batches/{batch_id}/resume
POST   /api/v1/advisory/historical-range-batches/{batch_id}/cancel
POST   /api/v1/advisory/historical-range-batches/{batch_id}/refresh-outcomes
POST   /api/v1/advisory/historical-range-batches/{batch_id}/build-dataset-bridge
GET    /api/v1/advisory/historical-range-runs/{range_run_id}
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/days
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/days/{trade_date}
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/lists/{trade_date}
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/outcomes
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/summaries
GET    /api/v1/advisory/historical-range-operations/{operation_id}
```

父设计未显式列出的 `GET historical-range-options` 和 `GET historical-range-batches` 是完成创建表单与任务列表所必需的只读投影，不改变业务语义或数据库 schema。

### 7.2 创建请求

Header：

```text
Idempotency-Key: <client generated UUID, 1..200 chars>
```

Body：

```json
{
  "program_specs": [
    {
      "source_kind": "EXISTING_PROGRAM",
      "program_id": "advp_...",
      "expected_program_version": 3,
      "expected_binding_version_id": "advbind_..."
    },
    {
      "source_kind": "RESEARCH_PROGRAM_SPEC",
      "program_name": "长期趋势父包历史验证",
      "package_id": "pkg_...",
      "target_count": 5,
      "review_policy": {},
      "runtime_config": {},
      "entry_price_basis": "next_open_executable",
      "exit_price_basis": "next_open_executable",
      "style_profile_ref": null,
      "style_profile_hash": null
    }
  ],
  "start_trade_date": "2026-07-01",
  "end_trade_date": "2026-07-21"
}
```

服务端补充 `request_id/requested_at/requested_by/data_source/origin/research_scope/evidence_level/execution_prohibited`，并把 Header 映射为 domain `client_idempotency_key`。客户端不得提交：

- `research_program_id`、`alpha_mode`、package version、manifest、component、weight；
- resolved request、calendar、source revision、selection/list/code hash；
- candidates、market rows、HMM snapshot、SQL、table、URI、artifact root；
- package validation、approval、role、production selector 或 scheduler 参数。

### 7.3 创建响应

新建或非终态 exact retry 返回 `202`；terminal exact retry 返回 `200`：

```json
{
  "ok": true,
  "data": {
    "batch": {},
    "create_operation": {},
    "canonical_batch_id": null,
    "exact_retry": false,
    "dispatch_state": "SCHEDULED",
    "links": {
      "self": "/api/v1/advisory/historical-range-batches/ahrb_...",
      "runs": "/api/v1/advisory/historical-range-batches/ahrb_.../runs"
    }
  }
}
```

`DEDUPLICATED` 必须返回 canonical batch link；不得把 deduplicated batch 显示为新的独立运行。

Exact retry 调度规则：

- terminal operation 直接返回既有 receipt，不注册 background task；
- unexpired RUNNING operation 返回当前 operation，不启动第二个 worker；
- QUEUED、WAITING_INPUT、RETRYABLE_FAILED 或 expired RUNNING 只注册一次当前请求的恢复 task，最终由 durable claim/lease/fencing决定唯一有效 worker；
- 同 idempotency key/不同 payload 在任何状态都返回 `409`。

### 7.4 Options 投影

`GET historical-range-options` 返回两类创建选项及 R4 研究参数：

- Existing Program：`program_id/name/version/active_binding_version_id/package_id/target_count/review_policy_summary`；
- admitted package projection：`package_id/name/alpha_mode/component_count/manifest_sha256/package_version`。
- exact R4 catalog：`catalog_version/catalog_content_hash/default_horizons/long_trend_horizons/allowed_maturity_statuses`。

该 endpoint 只读取已准入 projection，不调用 health/asset/model/factor validator，不对策略包再做一次通过/拒绝判断。包在读取期间发生版本变化时，由 create 的 expected identity CAS 返回 `409`，这是并发一致性冲突，不是 package 二次门禁。

### 7.5 Command 请求

Resume/Cancel：

```json
{
  "operation_idempotency_key": "uuid",
  "expected_row_version": 12
}
```

Refresh outcomes：

```json
{
  "operation_idempotency_key": "uuid",
  "expected_row_version": 12,
  "label_as_of_trade_date": "2026-07-24",
  "range_run_ids": [],
  "horizons": [1, 3, 5, 10, 20]
}
```

Build dataset bridge：

```json
{
  "operation_idempotency_key": "uuid",
  "expected_row_version": 12,
  "range_run_ids": [],
  "requested_horizons": [1, 3, 5, 10, 20],
  "requested_maturity_statuses": ["COMPLETE", "CENSORED", "TERMINAL"]
}
```

空 `range_run_ids` 表示 batch 内全部 runs。Horizon 只能从 options 返回的 exact catalog 集合选择；maturity status 只能使用 R4 domain enum，非法值返回 `422`。这些是用户选择的研究范围，不是 package gate。Policy bundle refs、successful day refs、candidate/outcome/summary refs、selector/schema/builder/writer/partition/compression hashes 和 artifact identity 全由 versioned R4 registry、batch facts与 composition root 派生。UI 不允许伪造这些身份。Correction 请求不进入普通 R5 UI；source/calculation correction 继续通过 R4 的正式受控服务入口执行。

Planning resume 使用既有 `BUILD_SOURCE_CATALOG` operation；sealed execution resume 使用 R3 `RESUME` operation。两者由 batch sealing state 明确分流，不能互换 cursor、attempt 或 request hash。

### 7.6 Query response

所有查询使用同一 envelope：

```json
{
  "ok": true,
  "data": {},
  "page": {
    "limit": 50,
    "next_cursor": null,
    "has_more": false
  }
}
```

核心投影：

- Batch：identity、date range、Program count、status、row_version、catalog phase/progress、day status counts、`planning_recoverable`、recoverable Program count、current operation、canonical link、timestamps；
- Operation：operation/type/status/row_version/attempt、cursor、processed count、lease/expired、result ref/status、structured error、created/started/finished/updated timestamps；
- Run：Program/package frozen identity、status、row_version、completed/total、waiting/retryable/failed counts、latest successful date、summary/snapshot refs；
- Day：ordinal、status、attempt、decision date、previous receipt/list identity、candidate/list counts、waiting/error、receipt identity；
- Candidate：symbol、membership、raw/risk/tradability/effective ranks/scores、source closure hash；
- List item：ENTER/HOLD/EXIT/WATCH、reason、previous/current rank、episode、intended execution date/basis、rule guidance；
- Outcome：subject/projection/horizon/maturity、label-as-of、next refresh、return/MFE/MAE/cost/benchmark fields、reason codes、version/predecessor identity；
- Summary：version、covered set、maturity coverage、return/drawdown/turnover/holding/Recall/industry/regime metrics、typed unavailable reasons；
- Bridge：operation/receipt、VALID_EMPTY 或 snapshot id/status、retrospective evidence scope。

API 不返回数据库 credential、absolute artifact root、SQL、内部 filesystem path 或 traceback。

查询参数固定为：

- batch list：可选 `status`、`program_id`、`created_before`，以及 `cursor/limit`；
- run list：`cursor/limit`；
- operation list：可选 `operation_type/status`，以及 `cursor/limit`；
- day list：可选重复 `status`，以及 `cursor/limit`；
- day detail/list detail：各自独立 `candidate_cursor/candidate_limit`、`item_cursor/item_limit`；
- outcomes：可选 `subject_type/projection/maturity_status/horizon`，以及 `cursor/limit`；
- summaries：`cursor/limit`。

所有 enum filter 使用 domain enum 精确值；未知字段、未知 enum 或 cursor/filter 不匹配返回 `422`，不忽略条件后返回更宽结果。

## 8. 稳定分页与一致读

- 默认 `limit=50`，最大 `500`；非法 limit 返回 `422`。
- 禁止 offset。Batch 使用 `(created_at DESC,batch_id DESC)`；Run 使用 `(research_program_id,range_run_id)`；Day 使用 `(ordinal,day_run_id)`；Candidate 使用 `(effective_rank NULLS LAST,symbol)`；Outcome 使用 `(subject_type,subject_id,projection,horizon,outcome_version)`；Summary 使用 `(summary_version DESC,summary_id DESC)`。
- Cursor 是 base64url canonical JSON，包含 `schema_version/order_key/filter_hash`。服务端验证 schema、filter hash、字段类型和顺序；无效 cursor 返回稳定 `422 ADVISORY_HR_CURSOR_INVALID`。
- Query repository 每次请求使用短 `REPEATABLE READ, READ ONLY` transaction。分页之间允许看到新追加的 outcome/summary，但 cursor 顺序必须避免重复或跳过已存在行。
- `CANCELLED_NOT_MATERIALIZED` 由冻结 date-plan ordinal 投影，明确 `projected=true`，不得伪造 day_run_id、attempt 或 receipt。
- 默认查询只读数据库 facts 和 artifact ref metadata；只有需要显示 typed outcome/summary payload 时按 exact ref readback，不扫描目录推断 latest。

## 9. HTTP、错误与日志

### 9.1 状态码

| 场景 | HTTP |
|---|---|
| create/command 已持久化并调度 | 202 |
| terminal exact retry、普通 query | 200 |
| missing batch/run/day | 404 |
| idempotency payload conflict、stale row version、code/source semantics mismatch | 409 |
| invalid body/date/cursor/state command | 422 |
| 显式环境或 artifact root 缺失 | 503 |
| unexpected internal error | 500 |

`WAITING_INPUT`、`RETRYABLE_FAILED`、finished `PARTIAL` 是正常业务状态，通过 200/202 response body 表达，不伪装为 `COMPLETED`。

所有 mutation response 必须返回 `operation_id` 和 operation link。Frontend 在后台命令完成前轮询 operation，而不是仅轮询 batch；这是 outcome refresh/bridge 错误可见性的唯一权威状态。Batch 主状态保持 COMPLETED 但最新 operation 为 FAILED 时，页面必须同时显示“范围执行已完成”和“本次收益刷新/数据桥失败”，不得互相覆盖。

### 9.2 Error envelope

```json
{
  "detail": {
    "error_code": "ADVISORY_HISTORICAL_RANGE_ERROR",
    "reason_code": "ADVISORY_HR_OPERATION_BATCH_VERSION_CONFLICT",
    "message": "...",
    "retryable": true,
    "context": {
      "batch_id": "ahrb_...",
      "expected_row_version": 4,
      "actual_row_version": 5
    },
    "correlation_id": "..."
  }
}
```

Router 保留 domain reason code，不把全部异常压成 `HTTP 500` 或只返回 `Failed to fetch`。Frontend 使用 `AdvisoryApiError` 保存 `http_status/error_code/reason_code/message/retryable/context/correlation_id`，可见错误面板展示原因和可执行动作。

Frontend response parser 还必须区分：

- HTTP error + typed JSON：保留完整 domain fields；
- HTTP error + 非 JSON/截断 body：返回 `ADVISORY_API_INVALID_RESPONSE`，保留 HTTP status 和 correlation header；
- fetch/CORS/连接失败：返回 `ADVISORY_API_NETWORK_ERROR`，不伪造 HTTP status 或 reason code；
- 200/202 + 不符合 typed envelope：返回 contract error，不以空对象继续渲染。

### 9.3 有价值日志

后台每个命令记录：

- `correlation_id/operation_id/batch_id/range_run_ids/operation_type`；
- 当前 stage、cursor、processed count、duration；
- domain reason code、exception type 和 traceback；
- exact retry/dedup/lease takeover 结果。

日志不输出 credential、完整请求 artifact、全量股票列表、绝对 artifact root 或密钥。轮询 GET 成功不逐次输出 info 噪声。

## 10. Frontend 信息架构

### 10.1 页面结构

现有 `/paper-v2/advisory` 保持同一路由，增加页面内 segmented tabs：

```text
当前荐股 | 历史验证
```

- 默认 `current`，URL query 使用 `?view=current|historical-range`；刷新和浏览器返回保持选中视图。
- “当前荐股”承载现有 Program、当前列表、复评和收益视图。
- “历史验证”承载 R5 创建、任务列表、详情、outcome、summary 和 dataset bridge。
- 不创建 marketing hero，不把主要操作放在嵌套卡片；使用现有 operator shell、full-width section、紧凑表格和 8px 以下圆角。

### 10.2 创建区

- 支持 Existing Program 多选；每个 Program 显示 exact program/binding version。
- 支持添加多个 research-only 行；每行只能选择一个单 Alpha 包或一个原生多 Alpha 父包。
- 不显示 package weight、alpha leg、融合模式、审批或授权控件。
- 日期区间可选择任何可解析的已完成历史交易日，不默认强制最新交易日。
- 提交前只做格式和本地重复检查；不调用策略包 health validator。
- 创建按钮使用 `Play` icon + “开始历史验证”；客户端生成并缓存同一请求的 Idempotency-Key，网络不确定时重试复用该 key。

### 10.3 任务列表

任务表展示：batch、Program 数、日期范围、状态、完成进度、waiting/retryable/failed 数、最近阶段和创建时间。

操作使用 lucide icons 和 tooltip：

- `Eye` 查看；
- `RotateCcw` 恢复；
- `Square` 取消；
- `RefreshCw` 刷新收益；
- `Database` 构建 retrospective dataset bridge。

按钮可用性只映射 domain 状态，不增加审批：

- batch 尚未 seal 时，catalog operation 为 `QUEUED/WAITING_INPUT/expired RUNNING` 且 `planning_recoverable=true` 时显示 resume；seal 后仅在 `recoverable_program_count > 0` 时显示 resume；
- finished PARTIAL 显示“部分结果，当前无可恢复项”；
- cancel 只在 domain contract 允许的状态启用；
- outcome refresh 在至少一个成功日存在时启用；
- bridge 在 batch 不再可恢复且至少一个成功日存在时启用。

若状态在操作前变化，409 结果更新本地 row version 并提示用户重新执行，不自动用新版本偷跑命令。

### 10.4 详情区

详情按未嵌套的四个 section 展示：

1. 概览：冻结 Program/package、日期、状态、进度、identity hashes；
2. 逐日演进：day timeline、candidate ranks、ENTER/HOLD/EXIT/WATCH 和理由；
3. Episode 与 Outcome：推荐/可执行 projection、maturity、实际历史收益、MFE/MAE、成本/benchmark；
4. Summary 与 Dataset：收益、回撤、换手、持股周期、Recall、行业/市场阶段、snapshot/valid-empty receipt。

未来实际价格只出现在 Outcome section，明确标记“历史结果，非决策日可用信息”；candidate/list section 只展示 T 日证据。`rule_default`、`model_unavailable` 和未来 `model_predicted` 使用不同 badge，R5 不显示任何模型预测数字。

### 10.5 加载、轮询与错误状态

- 只对非终态选中 batch 每 3 秒轮询；页面隐藏、切换 tab 或组件卸载时停止并 abort fetch。
- 历史验证 tab 可见且当前 batch page 含非终态项时，每 5 秒刷新一次当前 batch page；选中 batch/operation 仍按 3 秒刷新详情。轮询按页面聚合，不为每行创建独立 timer。
- 每个 mutation 按 response `operation_id` 轮询 operation terminal；operation FAILED/RETRYABLE_FAILED/WAITING_INPUT 不因 batch 已 COMPLETED 而被隐藏。
- query 使用 cursor 增量加载；切换 filter 时清空旧 cursor，不拼接不同 filter 的 rows。
- mutation 使用 sessionStorage 保存 `(batch/action/payload_hash)->idempotency_key`，收到确定响应后清理；浏览器刷新后的不确定重试仍复用 key。
- loading、`VALID_NO_CANDIDATE`、dataset `VALID_EMPTY`、waiting、partial、failed 和 network error 分开显示；禁止把合法零候选、零样本、尚未加载或请求失败混为同一个空表。
- raw JSON 只作为折叠高级调试视图，默认展示中文业务字段。
- 桌面和移动端不重叠；固定操作栏可换行，表格在窄屏水平滚动，状态 badge 和 icon button 使用稳定尺寸。

## 11. Legacy Cutover

R5 完整上线时同时执行以下 UI cutover：

1. 移除主页面底部“历史荐股生命周期回放”创建卡片；
2. 策略管理面板中的“回放验证”按钮替换为“在历史验证中研究”，只切换 tab 并预填 Program，不调用 legacy replay；
3. `advisoryApi.replay()` 保留供兼容调用和旧测试使用，但不由新页面主流程调用；
4. legacy replay response 仅增加顶层 `legacy_diagnostic=true`、`deprecated=true` 和 `replacement=/api/v1/advisory/historical-range-batches`；既有 `replay` payload 的字段、层级和 legacy rows 不变，旧客户端继续读取 `response.replay`；
5. 不把 legacy replay 结果作为 binding apply、Phase 1R seed、outcome 或 dataset bridge 输入；已有 `source_replay_run_id` 只保留旧 binding history 字段语义；
6. 不自动删除、改状态或迁移任何 legacy replay/list/episode；物理退役另立 cleanup 任务。

Cutover 必须是同一 R5 实现的一部分。不得先隐藏 legacy 入口但只交付静态新 tab，也不得同时把两套入口描述为同一正式能力。

## 12. 数据与跨模块隔离

### 12.1 允许读取

- `app.advisory_historical_range_*` facts；
- exact Phase 1R CAS refs；
- Advisory Program/binding 和 admitted package 的只读 identity projection；
- R4 versioned policy/selector/schema registry；
- 显式 `.env` 数据库和 repo-external roots。

### 12.2 禁止读取或写入

- QE/backtest dataset、回测 Parquet、策略包回测摘要；
- Paper/模拟盘收益、账户、订单、持仓；
- 当前 Selection run 作为历史范围 canonical run；
- 当前 Advisory PUBLISHED list 作为首日 seed；
- current/latest HMM 或 package identity 替代 frozen evidence；
- Selection、Paper、Simulation、QE/Qlib/QMT 业务表写入。

### 12.3 隔离验证

实现阶段必须通过 protected import/write scan 和真实 E2E 前后计数证明：

- R5 新文件不 import protected consumer services；
- Phase 1R command 只改变 `advisory_historical_range` 与已批准 retrospective Phase 1 bridge facts；
- 同一 batch 内一个 Program/day 失败只改变该 run/day 和 batch 聚合状态，其他 Program 的 operation/day/list/outcome 查询与提交保持独立；
- legacy replay API 不写 Phase 1R；
- Phase 1R API 不写 legacy replay/current Advisory；
- current Selection、当前荐股、Paper 和模拟盘直接 smoke 结果不变。

## 13. 性能与资源边界

- HTTP create 只完成 requirement-plan artifact 和短事务持久化，目标响应时间不随日期跨度线性增长。
- Batch/options/list 查询必须使用已有索引和 keyset cursor；不得在 Python 中加载全表后分页。
- Candidate/day/list/outcome detail 按字段 projection 查询，不 `SELECT *` 后把大 JSON 全量送到浏览器。
- Artifact payload 只按 exact ref 读取；不扫描目录，不猜 latest。
- UI 不一次加载完整长区间；day page 默认 50，候选/列表最大 500。
- Background task 沿用 R3/R4 bounded slice、lease、heartbeat 和 fencing；不得在 Router 写第二套循环。
- API timeout、DB cursor、background exception、AbortController 和 component unmount 均有资源释放。

这些是工程资源边界，不是业务门禁。合法日期范围和 Program 数通过排队、分块和分页推进，不因吞吐配置被业务拒绝。

## 14. 目标文件与所有权

| 文件 | 责任 | 禁止偏移 |
|---|---|---|
| `backend/services/advisory_historical_range/api_models.py` | request/response/page/error projection typed contracts | 不复制 domain 状态机 |
| `backend/services/advisory_historical_range/query_repository.py` | keyset read models、read-only SQL、artifact metadata projection | 不写 DB、不读取 legacy/current tables 作为结果 |
| `backend/services/advisory_historical_range/service.py` | R5 facade、command dispatch routing、query coordination | 不实现第二套 candidate/list/outcome 算法 |
| `backend/services/advisory_historical_range/composition.py` | 显式 DB/root/registry composition | 不猜 `.env`、不 import protected consumers |
| `backend/routers/advisory.py` | typed endpoints、HTTP/status/error mapping、BackgroundTasks registration | 不在 Router 扫描历史或运行长循环 |
| `frontend/src/lib/api/advisory.ts` | Phase 1R types/client、structured `AdvisoryApiError` | 不把 legacy response cast 为 Phase 1R |
| `frontend/src/app/paper-v2/advisory/page.tsx` | current/history segmented view 和 legacy cutover | 不把 current list 传入 history view |
| `frontend/src/app/paper-v2/advisory/historical-range/*.tsx` | 创建、任务表、详情、outcome/summary/dataset sections | 不嵌套 cards、不编辑历史事实 |
| `frontend/src/app/paper-v2/advisory/historical-range/useHistoricalRangeResearch.ts` | polling、cursor、abort、idempotency-key lifecycle | 不吞错、不无限轮询 terminal batch |
| `frontend/src/app/paper-v2/paper-v2.css` | responsive segmented tabs、status/table/action styles | 不引入单色大面积营销布局 |
| `backend/tests/advisory_historical_range/test_r5_api*.py` | API、query、error、idempotency、isolation direct tests | 不以 mock-only 冒充 real E2E |
| `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | UI contract、状态、移动/桌面和 legacy cutover | 不只断言静态标题存在 |

R5 当前不修改 migration、release schema、Selection、Simulation、Paper、QE/Qlib/QMT 文件。实现中若发现必须扩大这些范围，先更新本文并说明理由、风险和替代方案；不得静默扩大。

## 15. Implementation Plan / 实施顺序

### R5-A：Typed query/API foundation

1. API models 和 structured error；
2. keyset query repository/service；
3. options、batch list/detail、run/day/list/outcome/summary GET endpoints；
4. read-only API contract tests。

### R5-B：Command facade

1. create facade 和 BackgroundTasks dispatch；
2. planning/catalog/execution resume routing；
3. operation list/detail 和 mutation operation link；
4. cancel、refresh outcomes、dataset bridge；
5. idempotency、row-version、waiting/partial/error tests；
6. 无新 scheduler、无 package validator、无 protected import scan。

### R5-C：Frontend historical validation

1. Phase 1R types/client/error；
2. segmented tabs 和创建区；
3. task list、polling、resume/cancel；
4. day/list/episode/outcome/summary/dataset detail；
5. desktop/mobile、loading/empty/waiting/partial/error states。

### R5-D：Legacy cutover 与真实闭环

1. 移除主流程 legacy replay 创建入口；
2. legacy API deprecated metadata 和 no-cross-write tests；
3. DEV typed API command E2E；
4. 既有 R4 生产 batch 的只读 API/UI 验收；
5. 单/原生多 Alpha真实 API/UI 正向流、exact retry、错误可见与隔离；
6. DESIGN-COMPLIANCE-001 和 F2 validator。

每个批次必须完整实现其范围，不得用静态 tab、mock-only data、同步单日循环或临时脚本冒充完成。

## 16. 验证方案

### 16.1 Backend L0/L1

- Pydantic strict input：禁止派生 identity、candidate/market/SQL/root 字段；
- create Header/body idempotency、same/same、same/different；
- status/reason HTTP mapping；
- cursor canonical round-trip、filter mismatch、invalid schema；
- options 不调用 package health/asset/model/factor validator；
- batch/run/day/list/outcome/summary projection；
- `CANCELLED_NOT_MATERIALIZED` 不伪造 identity；
- terminal/WAITING/PARTIAL 正确响应；
- mutation operation link、operation 独立轮询和 batch terminal/operation failed 双状态表达；
- background exception 有 domain receipt 和 traceback；
- invalid/non-JSON/network/typed-envelope error 分类不互相冒充；
- legacy/Phase 1R 双向零 fallback。

### 16.2 Backend L2/L3

- existing Program 和 research-only Program create；
- 多 Program 各自独立 range run；
- 双 Program 一个 day 失败时另一个继续完成，API/UI 不把 batch 聚合失败覆盖成功 Program 事实；
- 单 Alpha 与原生多 Alpha父包；
- create 立即返回，background 完成 planning -> seal -> day execution；
- catalog WAITING 后同 batch resume；
- day RETRYABLE 后恢复；
- stale row version 409；
- outcome refresh 追加新版本且不改 batch 主状态；
- bridge COMPLETE/VALID_EMPTY 与 exact retry；
- query page 跨追加 outcome/summary 不重复旧行；
- `.env` 显式 DB/root 缺失时结构化 503；
- current Selection/Advisory/Paper/Simulation 直接依赖 smoke 不变。

R5 无 DDL，因此 `production_ddl_gate=noop`。DEV API mutation E2E 使用现有 DEV 数据库；生产历史 batch 可用于只读 API/UI 验收，不需要复制到 DEV。任何新的生产 DML、服务重启或 runtime activation仍按实际动作单独报告，不能由代码合入自动推导。

### 16.3 Frontend L4

- 创建多个 Existing/research-only Program；
- 不出现 weight/leg/approval/role/package health gate；
- 创建、轮询、详情、恢复、取消、refresh、bridge；
- valid empty、waiting、retryable、finished partial、failed、409、503、network error；
- `VALID_NO_CANDIDATE` 与 dataset `VALID_EMPTY` 使用不同空态；
- batch terminal 与 refresh/bridge operation failed 同时可见；
- structured reason/context 可见，非 `Failed to fetch` 空诊断；
- day candidate/list cursor；
- outcome 的历史结果与决策日证据分栏；
- model unavailable 不显示伪预测；
- legacy replay 主入口不存在，新 view 不发 legacy request；
- 375x812、768x1024、1440x900 无重叠、截断或按钮尺寸漂移；
- 三个 viewport 保存 Playwright screenshot evidence，并检查 console error、failed request、水平溢出和交互后布局稳定；
- 键盘 focus、tooltip、aria-label 和 tab 状态。

### 16.4 Real E2E

至少形成以下两条真实业务回执：

1. 单 Alpha：API create/resume/query/outcome/summary/bridge -> UI 完整展示；
2. 原生多 Alpha父包：相同路径，确认 parent package identity 与各腿证据只读展示。

使用已完成的 15 日 R3/R4 batch 可以验收 query/UI；command mutation 在 DEV 运行完整路径。不得用 fixture、mock server 或静态 JSON 替代最终 real E2E。深度跨浏览器和长区间验证交由 Validation Center/CI/nightly，交互窗口保留直接 contract 和最小真实 smoke。

## 17. Production Gates / 发布、生产影响与回滚

### 17.1 发布事实分层

分别报告：

- design merged；
- source merged；
- backend/frontend dependencies；
- DEV API/UI E2E；
- production DDL=`noop`；
- production DML；
- backend/frontend restart；
- runtime activation；
- legacy UI cutover 可见；
- cleanup。

源代码合入不等于服务已重启或 UI 已可见。

### 17.2 回滚

- 回滚 R5 source 后恢复旧页面入口；R1-R4 facts、schema、CAS、outcome、summary 和 snapshot 不变；
- 不 DELETE/TRUNCATE batch/run/day/outcome/summary；
- 已开始 background command 可通过既有 cancel/resume contract 闭合，不直接 SQL 改状态；
- legacy API 和数据始终保留，因此 source rollback 不需要数据迁移；
- R5 无 DDL rollback。

## 18. Risks / 风险与缓解

| 风险 | 后果 | 设计缓解 |
|---|---|---|
| HTTP 已返回但 background task 未 claim 就退出 | PLANNING batch 悬空 | exact create retry 和 planning resume 重新 dispatch 既有 catalog operation；artifact/lease/fencing 收敛 |
| BackgroundTasks 内长任务异常被框架吞掉 | UI 永久轮询且无原因 | 顶层 wrapper 记录 traceback，domain attempt/operation 写 retryable/failed receipt，query 返回 reason/context |
| Router 直接组合 R1-R4 参数 | 派生 identity 漂移或暴露 root/hash 给客户端 | `HistoricalRangeApplicationService` 和 versioned composition 唯一派生；Router 只做 HTTP mapping |
| 新 query repository 读取 current/legacy 表补字段 | 历史研究被未来状态污染 | query source allowlist、SQL contract tests 和 cross-write/read scan |
| cursor 排序不唯一 | 翻页重复或漏行 | 每类资源固定稳定复合 key，cursor 带 filter hash，append-only rows 使用 keyset |
| 巨型 `page.tsx` 继续膨胀 | 状态交叉、难以验证 cutover | 新功能拆入 `historical-range/` components/hook，父页面只负责 view routing 和现有 current state |
| legacy 按钮仍调用 replay | 两套历史能力继续混淆 | Playwright request assertion 禁止 historical view 和主流程发出 `/replay` 请求 |
| 通用 `apiFetch` 丢失结构化错误 | 页面只显示 Failed to fetch | Advisory typed error parser 保留 HTTP/domain fields；network error单独分类 |
| 实现期发现 schema 缺口 | 静默加入 DDL 或交付残缺 | 停止实现、更新本设计并报告精确 gap；R5 当前 `production_ddl_gate=noop` |
| UI 把 outcome 实际价格当作 T 日已知 | 形成未来数据误导 | candidate/list 与 outcome 分栏，字段标签和 contract test 固定时间语义 |

以上缓解均为正确性或可恢复性设计，不增加角色、审批、策略包复检或研究结果放行门禁。

## 19. Design Acceptance Index

| ID | 验收项 |
|---|---|
| F-740 | R5 只提供 Phase 1R product API/UI，不重写 R1-R4 算法或 schema |
| F-741 | Phase 0A.2D、legacy replay 与 Phase 1R 三类资源身份和路由永久分离 |
| F-742 | create 立即返回 durable identity，显式 background command 自动执行到真实稳定边界 |
| F-743 | 多 Program 独立；每 Program 仅一个单 Alpha 或原生多 Alpha 父包 |
| F-744 | Existing/research-only Program request 完整，派生 identity 只由服务端冻结 |
| F-745 | options 只读已准入 projection，不做 package 二次 health/asset/model/factor gate |
| F-746 | batch/run/day/list/outcome/summary/bridge typed API 完整 |
| F-747 | stable keyset cursor、分页上限和一致读完整，无 offset/全量加载 |
| F-748 | resume/cancel/refresh/bridge idempotency、row version 和 exact retry 完整 |
| F-749 | WAITING/RETRYABLE/PARTIAL/FAILED 和 unexpected error 可见且 reason/context 不丢失 |
| F-750 | 新 UI 提供创建、任务、逐日、列表、episode、outcome、summary、dataset 完整体验 |
| F-751 | UI 不编辑历史事实，不显示伪模型预测，不产生交易/模拟盘输入 |
| F-752 | 历史实际价格与 T 日决策证据分栏，避免未来数据语义混淆 |
| F-753 | legacy replay 创建入口退出主流程，旧 API/数据保留且不迁移 |
| F-754 | legacy 和 Phase 1R 双向零 fallback、零 cross-write |
| F-755 | Selection、当前 Advisory、Paper、Simulation、QE/Qlib/QMT 零副作用 |
| F-756 | API 使用显式 `.env` DB/root/registry，不猜路径、不泄漏 credential |
| F-757 | R5 无 DDL、审批、角色、自动 daily scheduler 或未经确认的门禁 |
| F-758 | 长区间通过 bounded service、分页和 polling 推进，不因吞吐配置改变业务接受语义 |
| F-759 | desktop/mobile、loading/empty/waiting/partial/error 和 accessibility 完整 |
| F-760 | 单 Alpha 与原生多 Alpha 真实 API/UI E2E，禁止 mock-only 冒充 |
| F-761 | DESIGN-COMPLIANCE-001 覆盖禁止简化、静默错误、业务偏移和私增门禁 |
| F-762 | 代码、DEV、production、restart、runtime、cutover 和 cleanup 状态分开报告 |
| F-763 | R5 完成后才能声明 Phase 1R 完成；Phase 0B/模型能力仍是独立后续阶段 |

## 20. Design Acceptance Matrix

本矩阵表示 R5 设计闭合，不代表源码已经完成。实现 PR 必须把 `implementation_refs` 和 `test_or_evidence` 更新为真实代码、测试和 E2E receipt。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-740 | §1-6、§14 | planned: `backend/tests/advisory_historical_range/test_r5_service_boundaries.py` | design_ready | none |
| F-741 | §4.3、§7、§11 | planned: `backend/tests/advisory_historical_range/test_r5_route_isolation.py` | design_ready | none |
| F-742 | §6.2-6.3、§7.3 | planned: `backend/tests/advisory_historical_range/test_r5_command_service.py` and `backend/tests/advisory_historical_range/test_r5_background_lifecycle.py` | design_ready | none |
| F-743 | §4.2、§10.2 | planned: `backend/tests/advisory_historical_range/test_r5_api_contracts.py` | design_ready | none |
| F-744 | §7.2 | planned: `backend/tests/advisory_historical_range/test_r5_api_contracts.py` | design_ready | none |
| F-745 | §7.4、§12 | planned: `backend/tests/advisory_historical_range/test_r5_package_projection.py` | design_ready | none |
| F-746 | §7.1、§7.5-7.6 | planned: `backend/tests/advisory_historical_range/test_r5_api_contracts.py` | design_ready | none |
| F-747 | §8、§13 | planned: `backend/tests/advisory_historical_range/test_r5_query_repository.py` | design_ready | none |
| F-748 | §6、§7.5、§9 | planned: `backend/tests/advisory_historical_range/test_r5_command_service.py` | design_ready | none |
| F-749 | §9、§10.5 | planned: `backend/tests/advisory_historical_range/test_r5_error_projection.py` and `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | design_ready | none |
| F-750 | §10 | planned: `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | design_ready | none |
| F-751 | §3、§10.4、§12 | planned: `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | design_ready | none |
| F-752 | §10.4 | planned: `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | design_ready | none |
| F-753 | §11、§17.2 | planned: `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | design_ready | none |
| F-754 | §4.3、§11、§12.3 | planned: `backend/tests/advisory_historical_range/test_r5_route_isolation.py` | design_ready | none |
| F-755 | §5.1、§12 | planned: `backend/tests/advisory_historical_range/test_r5_protected_module_isolation.py` | design_ready | none |
| F-756 | §5.1、§9、§12.1 | planned: `backend/tests/advisory_historical_range/test_r5_composition.py` and `backend/tests/advisory_historical_range/test_r5_background_lifecycle.py` | design_ready | none |
| F-757 | §3、§6.2、§16.2 | planned: `backend/tests/advisory_historical_range/test_r5_service_boundaries.py` | design_ready | none |
| F-758 | §6、§8、§13 | planned: `backend/tests/advisory_historical_range/test_r5_query_repository.py` | design_ready | none |
| F-759 | §10.5、§16.3 | planned: `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | design_ready | none |
| F-760 | §16.4 | planned: `backend/tests/advisory_historical_range/test_r5_postgres_e2e.py` and `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | design_ready | none |
| F-761 | §21 | artifact: `scripts/aistock_feature_workflow.py`; validation command recorded in §21 | design_ready | none |
| F-762 | §17.1 | artifact: `docs/architecture/advisory_phase1r_r5_api_ui_legacy_cutover_f2_design_20260727.md` | design_ready | none |
| F-763 | §1-3、§19 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |

## 21. DESIGN-COMPLIANCE-001 设计复核

### 20.1 禁止简化交付

PASS_DESIGN。设计覆盖完整 typed API、command facade、query projection、前端创建/任务/详情/结果、legacy cutover 和真实 E2E。明确禁止静态 tab、mock-only、同步单日循环、临时脚本或 backend-only 冒充 R5 完成。

### 20.2 禁止静默错误

PASS_DESIGN。HTTP、domain reason、retryable、context、correlation id、background traceback、UI error state 和 invalid cursor/row version 均有明确合同；WAITING/PARTIAL 不冒充 COMPLETED，空表不冒充加载成功。

### 20.3 禁止业务逻辑偏移

PASS_DESIGN。R5 复用 R1-R4 services 和 facts，不复制算法；Existing/research-only、多 Program、一 Program 一 package、空 seed、PIT、outcome maturity、retrospective-only 和 no-fallback 与父蓝图一致。

### 20.4 禁止私增门禁审批

PASS_DESIGN。没有角色、审批、双人复核、package 二次验证、最新交易日、candidate count、Program 数、日期跨度、全部 horizon、canary 或 ModelOps 门禁。资源分块、keyset、lease 和 background task 只控制工程执行，不改变合法请求的业务接受语义。

## 22. 第二次正式审核记录（2026-07-27）

本轮重新从父蓝图和 Phase 1R 正向业务流程审核，不以首次 validator 结果替代业务判断。

### 22.1 蓝图方向核对

| 蓝图方向 | R5 设计结论 | 结果 |
|---|---|---|
| 历史范围研究是正式可重复功能，不是诊断脚本 | typed batch/run/day/operation API、持久任务和完整 UI | PASS |
| 多个策略包独立荐股 | batch 聚合多个独立 Program；一 Program 一 package、一 run/list/outcome chain | PASS |
| 支持单 Alpha 和原生多 Alpha 父包 | existing/research-only 两类 Program 均由 admitted projection 冻结 parent identity | PASS |
| 不依赖最新交易日 | 接受显式已完成历史范围，不读取 latest 或 O4 所选日期 | PASS |
| 策略包不做二次准入 | options 只读 projection；create 仅做 expected identity CAS | PASS |
| 不读取回测/Paper/模拟盘数据 | source allowlist 和 protected import/write scan 已进入设计与测试 | PASS |
| retrospective 与 formal OOS 永久隔离 | R5 只显示/触发既有 retrospective bridge，不发布 READY capability | PASS |
| 不影响 Selection、当前荐股、Paper、模拟盘或交易 | R5 application/query namespace 独立，legacy/current 双向零 fallback/cross-write | PASS |
| 无角色、审批和额外运行门禁 | UI/API 无相关字段或状态，工程分块不改变业务接受语义 | PASS |

### 22.2 本轮发现并修复

1. 修复异步 command 只轮询 batch 导致 refresh/bridge 失败不可见的问题：新增 operation 列表/详情 API、mutation operation link 和独立 operation polling。
2. 补充 batch 已 COMPLETED 但后续 operation FAILED 的双状态展示，禁止后者被前者覆盖。
3. 补充 invalid/non-JSON/network/typed-envelope 四类前端错误，禁止退化为无上下文 `Failed to fetch` 或空对象。
4. 补充创建已返回但 background 未 claim 时的 exact create retry/planning resume，避免 PLANNING batch 悬空。
5. 补充 background task 不跨 request 生命周期复用 DB connection/cursor，并规定失败 receipt、lease expiry 和资源释放。
6. 补充 task page/selected operation 分层轮询，避免非选中任务永久显示旧状态或每行创建 timer。
7. 补充 `VALID_NO_CANDIDATE`、dataset `VALID_EMPTY`、loading 和 failure 四种不同语义。
8. 补充双 Program 单日失败隔离验收，避免 batch 聚合状态覆盖另一个 Program 的成功事实。
9. 锁定 legacy deprecated metadata 为顶层增量字段，保持旧 `replay` payload 兼容。
10. 补充三个 viewport screenshot、console/network/overflow 验收，禁止静态标题或单视口冒充 UI 完成。

### 22.3 审核结论

- 禁止简化版：PASS。API、command、query、UI、legacy cutover 和真实 E2E 均为必交付范围。
- 禁止静默错误：PASS。HTTP、background operation、lease expiry、parser、polling 和 UI error state 均有权威可见路径。
- 业务逻辑与蓝图一致：PASS。没有改变 R1-R4 算法、数据身份、Program/package 关系或隔离边界。
- 禁止未经确认的门禁审批：PASS。未增加审批、角色、策略包复检、最新日、候选数、日期跨度或模型能力门禁。

## 23. 当前结论

R5 详细设计已把父蓝图中的 API、UI、历史验证和 legacy cutover 方向落实为可编码合同，并保持 R1-R4、Selection、当前 Advisory、Paper、Simulation、QE/Qlib/QMT 隔离。设计阶段不执行代码、DDL/DML、服务重启或 runtime activation。

下一步是在正式设计审核通过后，按 R5-A 至 R5-D 顺序实现，并在请求合入前逐项回填 F-740 至 F-763 的代码、测试和真实 E2E 证据。缺少任一业务闭环时不得声明 R5 或 Phase 1R 完成。
