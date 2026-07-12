# Advisory Phase 0A.2D Historical Research Multi-Program Runner F2 Design

> 文档状态：`design_ready`  
> Feature tier：`F2`  
> 父设计：[Phase 0A.2 evidence readiness bootstrap](advisory_phase0a2_evidence_readiness_bootstrap_f2_design_20260711.md)  
> 前置实现：[Phase 0A.2C prospective evidence producer](advisory_phase0a2c_prospective_evidence_producer_f2_design_20260712.md)  
> 范围：荐股历史研究；不修改模拟盘、Paper、QMT、MiniQMT 或 broker 功能。

## Background

Phase 0A.2D follows the prospective evidence producer and provides the first
multi-Program orchestration boundary for Advisory research. The parent design
previously described a daily runner; this detailed design deliberately narrows
that capability to manually requested, historical-data research only.

## Scope

### 1. 目标与约束

本阶段只实现手工触发的历史研究批处理。它在明确指定的已完成交易日，针对多个独立 Advisory Program，基于数据库中的历史数据计算并保存可复核的研究候选、证据和批次回执。

它不是实时荐股、投资建议、交易指令、模拟盘自动运行器或任何形式的下单入口。

不可协商的约束：

- 请求和所有下游 artifact 的 `data_source` 必须是 `DB_HISTORICAL`。
- 只允许 `origin=MANUAL_HISTORICAL_RESEARCH`；没有 scheduler、auto-run、replay publish 或实时触发入口。
- `decision_trade_date` 必须是数据库中已完成的交易日，且不得等于或晚于请求时的当前交易日。
- runner 不得导入或调用 `paper_trading_v2`、`simulation_runtime`、`qmt`、`miniqmt_execution_runtime`、broker、order、execution 或实时行情 provider。
- 研究结果可被独立的历史模拟任务读取，但 runner 不得创建、启动、更新或配置模拟盘任务。
- 不引入审批、角色、授权工作流或运行时 DDL。

## 2. 前置条件与非目标

### 2.1 开工前置条件

代码实现必须等待以下 Phase 0A.2C 条件全部完成：

1. Phase 0A.2C evidence producer 的代码、DEV-DB migration/readback/rollback、shared-consumer parity 和 CI 已有闭环证据。
2. `selection_score_artifact_v2` 与 `daily_selection_evidence_v2` 已在 DEV-DB 可写入、可重读且同键冲突 fail-closed。
3. 单 Alpha 和原生多 Alpha 的历史 `DB_HISTORICAL` fixture 都可产生完整 research-only DSE。

本设计只冻结 2D 契约；不因设计完成而绕过上述前置条件开始实现。

### 2.2 Non-Goals

- 不新增实时行情读取、实时通知、自动定时执行或 current-day T0。
- 不提供买入、卖出、目标仓位、订单、资金、账户、broker 或 QMT/MiniQMT 接口。
- 不改变 StrategyPackage、Selection Center、模拟盘或 Paper 的既有候选计算和行为。
- 不训练 reranker、收益、持有期或价格区间模型；这些属于后续 Phase 2+。
- 不把历史研究结果提升为正式 OOS、`READY`、实时建议或交易能力。

## 3. Design Acceptance Index

| ID | 验收项 |
|---|---|
| F-039 | 多 Program 历史研究 runner 只读 `DB_HISTORICAL`，使用确定性业务键、逐 Program 隔离、可恢复状态机和 batch receipt；不触发模拟盘或执行路径。 |
| F-040 | 每次研究必须锚定显式历史交易日，拒绝 current/future date、实时数据、非手工 origin 和 replay publish；所有输出标记为历史研究。 |

## Contracts

### 4. 领域模型与业务键

### 4.1 请求

```text
HistoricalResearchBatchRequest
  request_id                 UUID
  decision_trade_date        date                 # 已完成历史交易日
  program_ids                non-empty unique list
  data_source                Literal[DB_HISTORICAL]
  origin                     Literal[MANUAL_HISTORICAL_RESEARCH]
  requested_at               timezone-aware timestamp
  request_payload_sha256     sha256
  research_scope             HISTORICAL_RESEARCH_ONLY
  execution_prohibited       true
```

`program_ids` 仅决定本次研究范围，不改变 Program 的启用状态、binding 或 package。每个 Program 在 `decision_trade_date` 使用当日有效的 binding 与 manifest；缺失或不一致只失败该 Program。

### 4.2 业务键

```text
batch_key = sha256({
  decision_trade_date,
  sorted(program_ids),
  data_source=DB_HISTORICAL,
  origin=MANUAL_HISTORICAL_RESEARCH,
  research_scope=HISTORICAL_RESEARCH_ONLY,
})

program_run_key = (program_id, decision_trade_date, HISTORICAL_RESEARCH)
program_payload_hash = sha256({
  program_id, binding_id, binding_hash, manifest_sha256,
  policy_hash, effective_runtime_config_hash, source_watermark_hash,
  data_source=DB_HISTORICAL, research_scope=HISTORICAL_RESEARCH_ONLY,
})
```

同一 `program_run_key + program_payload_hash` 返回既有 immutable result；同 key 但不同 payload 必须报告 `ADVISORY_PHASE0A2D_RESEARCH_RUN_CONFLICT`，不得覆盖、补写或产生第二份 published list。

### 4.3 状态机

```text
PENDING -> RUNNING -> COMPLETE
                  -> WAITING_INPUT
                  -> FAILED
```

- `WAITING_INPUT` 只表示历史数据库中缺少必需、可诊断的数据；相同 payload 可以在数据补齐后恢复。
- `FAILED` 表示契约、binding、artifact、DSE 或持久化冲突；不得把失败降级为空候选。
- `COMPLETE` 可包含 `VALID_NO_CANDIDATE`，但必须由 Phase 0A.2C 的完整 raw-empty/filtered-empty 证据证明。
- batch 状态由各 Program 状态聚合；一个 Program 失败不回滚已 `COMPLETE` 的其他 Program，也不影响其独立 evidence。

## 5. 架构与隔离

```text
HistoricalResearchBatch API / CLI (manual only)
  -> HistoricalAdvisoryResearchRunner
     -> HistoricalProgramResolver
     -> HistoricalSelectionEvidenceAdapter (read-only contract)
     -> AdvisoryResearchRepository
     -> batch/program receipts + research list versions
```

`HistoricalSelectionEvidenceAdapter` 是 Advisory 模块内的 protocol。实现只能消费已经完成的 Phase 0A.2C selection artifact/DSE 和历史数据库读取接口；它不得调用模拟盘服务。后续若复用共享 Selection 纯计算能力，必须通过该 adapter 的只读接口，并新增静态 import test 证明 runner 没有引入上述禁止模块。

每个 Program 在独立数据库事务中执行：冻结 binding/config/source watermark，获得或验证 v2 artifact，组装 research-only DSE，创建 research list version 和 program receipt。batch receipt 只引用 immutable Program receipts，不反向修改它们。

## 6. 历史数据与时间规则

- calendar resolver 必须从数据库确认 `decision_trade_date` 是已完成交易日；输入当前或未来交易日返回 `ADVISORY_PHASE0A2D_HISTORICAL_DATE_REQUIRED`。
- 所有 source receipt 的 `available_at` 不得晚于冻结 decision clock；不得用当前数据库最新值回填历史缺口。
- artifact/DSE 必须满足 `research_scope=HISTORICAL_RESEARCH_ONLY`、`execution_prohibited=true`、`market_data_scope=DB_HISTORICAL`。
- 对单 Alpha 和原生多 Alpha 使用相同的 Program 独立执行契约；多 Alpha 父包继续要求真实、相同的子 Alpha 输入宇宙 receipt。

## 7. 持久化与 API

未来实现新增 additive 的 Advisory-only 表，建议为：

```text
advisory_research_batch
advisory_research_program_run
advisory_research_batch_receipt
```

所有 DDL 只能作为版本化 migration 在开发/部署阶段显式执行；runner 运行时绝不执行 DDL。表中不保存订单、账户、资金、仓位、broker 或 execution 字段。

最小 API：

```text
POST /api/advisory/research-batches
GET  /api/advisory/research-batches/{batch_id}
GET  /api/advisory/research-batches/{batch_id}/programs/{program_id}
```

POST 只接受上述请求模型。返回对象固定带 `research_scope`、`execution_prohibited`、`data_source` 和 reason codes；不返回交易动作、目标仓位或执行建议。

## 8. Reason Codes

| reason_code | 含义 |
|---|---|
| `ADVISORY_PHASE0A2D_HISTORICAL_DATE_REQUIRED` | 不是可用的已完成历史交易日。 |
| `ADVISORY_PHASE0A2D_HISTORICAL_DATA_REQUIRED` | 数据源不是 `DB_HISTORICAL`。 |
| `ADVISORY_PHASE0A2D_MANUAL_ORIGIN_REQUIRED` | origin 不是手工历史研究。 |
| `ADVISORY_PHASE0A2D_RESEARCH_RUN_CONFLICT` | 同一 Program/date 业务键的 payload 不一致。 |
| `ADVISORY_PHASE0A2D_PROGRAM_INPUT_UNAVAILABLE` | 必需历史数据、binding 或 evidence 缺失。 |
| `ADVISORY_PHASE0A2D_PROGRAM_EVIDENCE_INVALID` | 已存在的 binding、artifact、DSE、决策时钟或 source receipt 与历史研究契约不一致。 |
| `ADVISORY_PHASE0A2D_FORBIDDEN_EXECUTION_DEPENDENCY` | runner 发现模拟盘、broker、QMT 或实时依赖。 |

## 9. 验证方案

### 9.1 L0/L1

- request/data-source/origin/date validator 的正反用例。
- `batch_key`、`program_payload_hash` 的字段顺序确定性和同键冲突。
- Program 状态机、失败隔离、`WAITING_INPUT` 恢复和 `VALID_NO_CANDIDATE` 正反用例。
- 静态 import/forbidden-symbol test：runner 不能依赖模拟盘、Paper、QMT、MiniQMT、broker 或 realtime provider。

### 9.2 L2/L3

- 两个独立 Program 的 single Alpha / native multi Alpha 历史 fixture。
- 同日重复请求返回相同 run/list/receipt identity；不同 payload fail-closed。
- 一个 Program `WAITING_INPUT` 或 `FAILED` 时，另一个 Program 保持独立 `COMPLETE`。
- capture-on/off 不改变候选 canonical hash；research wrapper 不调用模拟盘 API。

### 9.3 L4/CI

- DEV-DB apply/readback/rollback 仅在显式 DDL 授权后执行。
- CI 运行 artifact/DSE、runner、API 和 advisory audit 的矩阵；不要求模拟盘回归作为本阶段的直接门禁。
- 不启动任何生产服务，不写生产 DB，不触发历史研究以外的运行任务。

## 10. 自动技术门禁

本阶段不引入审批或角色。保留且必须可达的自动门禁：

1. 历史日期、`DB_HISTORICAL`、manual origin 与 research-only contract 校验。
2. Program/binding/manifest/policy/config/source watermark 完整性。
3. 同 key 同 hash 幂等，不同 hash 冲突。
4. 单 Program 事务完整性与跨 Program 失败隔离。
5. 禁止模块依赖和禁止执行字段静态扫描。

正确的历史输入必须能直接 `COMPLETE`；门禁不得要求人工审批、角色授权或不可满足的运行条件。

## Risks

- historical date validation or source receipt drift could create an invalid research result; both fail closed with a reason code.
- a future import from simulation, Paper, QMT, MiniQMT or broker modules would violate the boundary; static forbidden-import tests are mandatory.
- interrupted batch persistence could create partial visibility; Program receipts are immutable and batch aggregation is recoverable by the exact business key.

## 11. Implementation Plan

1. 等待 0A.2C 的 DEV-DB、shared-consumer parity 与 CI 前置条件完成。
2. 新增 Advisory-only typed request/state/receipt/repository contract 和 additive migration。
3. 实现历史 Program resolver、read-only selection adapter、idempotent runner 和 batch receipt。
4. 新增 API read/write contract，但不增加 scheduler、实时入口或模拟盘联动。
5. 完成 L0-L4、F2 validator、DESIGN-COMPLIANCE-001 和 CI，再决定提交合入。

## Production Gates

```text
production_ddl_gate = pending
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
production_dml_gate = noop
production_runtime_gate = noop
```

No production DDL, DML, scheduler activation, service restart, or runtime research batch is part of this design delivery.

## Rollout / Rollback

- Rollout starts only after a separately authorized additive migration, completed CI, and manual verification that the API accepts only historical research requests.
- Rollback disables the Advisory research endpoint or future runner service. Immutable historical receipts are retained and never rewritten.
- No rollback action changes simulation, Paper, broker, QMT, MiniQMT, or data-provider configuration.

## 12. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-039 | §4-5、§7、§10-11：历史唯一键、Program 隔离、可恢复状态机、batch receipt、禁止依赖 | §9 的 hash/state/fixture/conflict/forbidden-import/DEV-DB/CI 验证 | design_ready | none |
| F-040 | §1-2、§4、§6、§8-10：历史日期、DB source、manual origin、research-only contract | §9 的 current/future/realtime/replay/empty-state 正反验证 | design_ready | none |

## 13. DESIGN-COMPLIANCE-001

- [x] F-039/F-040 与父设计和 research-only 边界一致。
- [x] 单 Alpha、原生多 Alpha和多个独立 Program 均有正向路径。
- [x] 运行时不包含审批、角色、执行、broker、QMT 或模拟盘调用。
- [x] 正确历史输入可完成，错误输入 fail-closed 且带 reason code。
- [x] 不存在静默 fallback、伪造空候选、current replay 或隐式实时数据。
- [x] DDL、CI、合入和运行激活保持分离。
