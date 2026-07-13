# AIstock Advisory Phase 1C-2 Fixture Source Revision And Selector F2 Design

## Background

本文档是父级设计
`advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`
在 Phase 1C 的第二个可实施切片。Phase 1A 已经提供 append-only source
availability event、exact source revision set 及 PostgreSQL 冻结能力；Phase
1C-1 已经提供 capture batch、gap、immutable observation/version/lineage 和
fixture-only observation writer。本切片不重建这些基础设施，只补齐 fixture/local
store 环境下的 source requirement 解析、as-of terminal 选择、research
readiness、observation version selector 与 capture membership 绑定。

本切片仍属于历史学术研究数据建设。它不改变荐股排名，不产生交易建议，不接入
实时荐股、模拟盘、Paper、QMT、MiniQMT 或任何订单链路。

## Feature Classification

- Tier: `F2`
- Parent design: Advisory Phase 1 PIT observation/label/sealed snapshot F2
- Predecessors: Phase 1A source ledger/revision set、Phase 1C-1 capture foundation
- Delivery boundary: fixture/local store only，默认不激活

## Parent Consistency

本设计严格继承下列父级语义，不建立第二套规则：

- source as-of 只解析 `formal_available_at <= requested_source_cutoff` 的唯一
  terminal event；无 event、fork、cycle、多个 terminal、terminal invalidated
  或质量不合法均不得猜测修复。
- observation selector 先解析 as-of terminal revision，再检查 capability；不得先
  按期望状态过滤后回退旧 revision。
- v1 只允许 `EXACT_REVISION_V1` 和 `LATEST_ELIGIBLE_REVISION_V1`。
- Observation 只接受 `MANUAL_HISTORICAL_RESEARCH`、
  `HISTORICAL_RESEARCH_ONLY`、`DB_HISTORICAL`、
  `execution_prohibited=true` 与 exact source revision evidence。
- `PARTIAL` 只能由可枚举缺口产生；identity、manifest、policy、hash 或 chain 冲突
  必须是 `BLOCKED`。
- 单 Alpha 和原生多 Alpha 都使用同一解析协议。多 Alpha 各腿允许具有不同的合法
  lookback/window、query parameters、business range、row count 和 content hash；
  仅公共 PIT identity 必须一致。
- 所有正常数据校验必须同时具备拒绝错误输入和接受合法输入的正向路径；不得形成
  正常输入永远无法满足的条件。

## Scope

- 定义不可变 `SourceRequirementSet`，显式列出每个 research scope 所需的 source
  role、query template、bound parameters、partition、cutoff requirement 和
  consumer/component identity。
- 在 fixture/local store 上实现 source availability chain 的完整性校验和唯一
  as-of terminal 解析，复用 Phase 1A `SourceAvailabilityEvent` 与
  `SourceRevisionMemberInput`。
- 生成确定性的 `SourceResolutionReceipt`：绑定 requirement set、已解析 member、
  未解析 gap、exact source revision set 和 readiness。
- 按父级表格计算 `RESEARCH_READY | PARTIAL | BLOCKED`，不把缺数据与 identity
  冲突混为一类。
- 定义 fixture-only observation version selector，完整实现
  `EXACT_REVISION_V1` 和 `LATEST_ELIGIBLE_REVISION_V1` 的 terminal-first 语义。
- 把 source revision set、resolution receipt 和 selected observation mapping 作为
  Phase 1C capture membership evidence；不得从 mutable latest row 补值。
- 为单 Alpha、原生多 Alpha、不同 lookback、source correction/invalidation、
  PARTIAL 和 BLOCKED 建立正反向 fixture golden。

## Non-Goals

- 不实现 source observer、ingestion hook、历史 available-at 回填、市场表扫描或
  current/latest fallback。
- 不实现 outcome label、terminal/cost/benchmark 计算、universe denominator、build
  attempt、Parquet/CAS、SEALED snapshot 或训练。
- 不修改荐股页面、Selection Center、策略包推理、多 Alpha live aggregation、
  Paper、模拟盘、QMT、MiniQMT、scheduler 或 broker。
- 不新增用户、角色、审批、授权、人工确认、发布审批、运行审批或业务流程门禁。
- 不执行生产 DDL、生产 DML、依赖安装、服务重启或运行时激活。
- 不把 fixture/local store 能力描述为 production observer、正式 source closure 或
  完整 Phase 1 交付。

## Existing Baseline And Gaps

### Reused without replacement

- `source_ledger.py`: event identity、append-only chain、single successor、as-of
  source selection 的内存 oracle。
- `source_revision.py`: canonical source member、revision set hash 和 exact event
  field matching。
- `source_revision_postgres.py`: revision set 原子冻结和 exact retry/conflict。
- `capture_foundation.py`: capture request、lease/fencing、membership seal、gap 和
  immutable observation schema。
- `observation_capture.py`: durable trace 到 fixture-only observation 的转换。

### Missing integration

- 当前没有显式 source requirement set，无法证明“需要哪些源”和“实际冻结哪些源”
  完全对应。
- 当前没有 requirement-to-member mapping，多 Alpha 物理 source member 去重后无法
  单独证明每个 component requirement 的满足状态。
- 当前 source revision primitives 不负责 scope readiness，也不能区分 enumerable
  missing source 与 identity conflict。
- 当前没有父级定义的 observation as-of selector policy 实现。
- 当前 capture membership 尚未冻结 source resolution receipt 与 selected mapping。
- `enforced_cutoff_predicate_hash` 是父级 source member 契约的一部分，当前模型与表
  尚未承载；实现阶段必须以 additive schema/model 变更补齐，不能省略或用空值代替。

## Architecture

```text
Phase 0A immutable handoff/admission scope
  + explicit fixture source requirements
  + explicit fixture/local availability events
       -> SourceRequirementSetBuilder
       -> FixtureSourceRevisionResolver
            -> validate complete event chain
            -> select unique terminal as-of per requirement
            -> build exact SourceRevisionMemberInput values
            -> freeze SourceRevisionSet when at least one member exists
            -> emit SourceResolutionReceipt + enumerated gaps
       -> ResearchReadinessClassifier
            -> RESEARCH_READY | PARTIAL | BLOCKED
       -> eligible CapturePlan / gap-only receipt
       -> Phase 1C-1 observation writer
       -> FixtureObservationVersionSelector
            -> terminal-first selection
            -> SelectedObservationMapping
       -> capture membership seal
```

所有输入均由调用方显式提供 immutable fixture/local-store references。解析器没有
数据库 latest 查询、StrategyPackage 推理调用、Selection 重跑或 source observer。

## Data Contracts

### SourceRequirement

每个 requirement 至少包含：

```text
requirement_id
consumer_scope_id                 # single alpha 或 native multi-alpha component
source_role                       # FEATURE_T/UNIVERSE_T/CALENDAR/TRADABILITY/...
dataset_name
query_template_id/version/hash
bound_parameters
bound_parameter_hash
partition_key
revision_kind
availability_requirement         # DECISION_CUTOFF/LABEL_AS_OF/POLICY_FROZEN
business_min_date/business_max_date
requested_cutoff
enforced_cutoff_predicate_hash
required_quality_status = PASS
research_only = true
```

`requirement_id` 由完整 canonical payload 派生。相同 ID、相同 payload 是幂等；相同
ID、不同 payload 是 conflict。`bound_parameters` 只进入 canonical hash/fixture，
不得被拼接为任意 SQL。

### SourceRequirementSet

```text
source_requirement_set_id/hash
admission_scope_id/hash
handoff_readiness_hash
program_id/binding_version_id
package_id/manifest_sha256
alpha_mode
decision_as_of_trade_date
requested_source_cutoff
query_registry_hash
requirements[]                    # 按 requirement_id 排序
research_only = true
```

同一 requirement set 内不得有重复 requirement identity。公共 PIT identity 包括
package/manifest、decision date、cutoff、calendar identity、universe policy 和 query
registry。原生多 Alpha component 的 lookback/window 属于各自 requirement 内容，
不属于公共一致性字段。

### RequirementResolution

每个 requirement 必须产生且只产生一个 resolution：

```text
requirement_id
resolution_status = AVAILABLE | UNAVAILABLE | CONFLICT
selected_availability_event_hash nullable
selected_source_member_key nullable
consumer_scope_id
reason_codes[]
resolution_content_hash
```

- `AVAILABLE` 必须同时带 exact event hash 与 member key。
- `UNAVAILABLE` 必须有稳定、可枚举的 missing reason；不得生成虚假 member。
- `CONFLICT` 用于 chain、identity、hash、quality contract 冲突，不可降级为 PARTIAL。

### SourceResolutionReceipt

```text
source_resolution_receipt_id/hash
source_requirement_set_id/hash
requested_source_cutoff
resolution_policy_version/hash
source_revision_set_id/hash nullable
resolved_requirement_count
unavailable_requirement_count
conflict_requirement_count
requirement_resolutions[]
readiness
reason_codes[]
research_only = true
```

receipt hash 覆盖完整 requirement-to-member mapping。因此，多 Alpha 各腿即使合法
共享同一个物理 member，也保留各自 requirement 的满足证据；各腿使用不同历史窗口
时则自然解析为不同 member，不做跨腿 window/hash 等值检查。

### SelectedObservationMapping

```text
selection_policy = EXACT_REVISION_V1 | LATEST_ELIGIBLE_REVISION_V1
selection_policy_hash
canonical_signal_id
requested_source_cutoff
required_capability
explicit_observation_version_id nullable
terminal_observation_version_id nullable
terminal_observation_content_hash nullable
terminal_revision_no nullable
signal_source_revision_set_hash
selection_status = SELECTED | UNAVAILABLE | CONFLICT
reason_codes[]
selected_mapping_hash
```

`selected_mapping_hash` 覆盖请求、terminal、最终状态以及全部 rejected reason codes。

## Source Resolution Algorithm

对每个 requirement 按以下固定顺序执行：

1. 校验 requirement set、handoff/admission、package/manifest、decision date、query
   registry 和 research-only identity 完整一致。
2. 按 `(dataset_name, source_role, partition_key_hash)` 取得调用方显式提供的完整
   fixture chain；不访问 mutable latest state。
3. 校验 revision 从 1 连续递增、精确 predecessor、单 successor、同 partition、无
   fork/cycle/tamper，并校验 correction/revalidation 的 revision/content 变化。
4. 只保留 `formal_available_at <= requested_cutoff` 的 event，解析唯一最大
   `event_revision_no` terminal。
5. terminal 为 `INVALIDATED`、非 `PASS` 或 cutoff 前不存在 event 时产生可枚举的
   `UNAVAILABLE`；字段与 requirement 不一致、出现多个合法 terminal 或 chain
   identity 冲突时产生 `CONFLICT`。
6. 对 AVAILABLE terminal 构造字段逐项相等的 `SourceRevisionMemberInput`；
   `enforced_cutoff_predicate_hash` 必须来自冻结 query registry，不允许临时派生空值。
7. 按 member key 排序并构建 exact `SourceRevisionSet`。多个 requirement 可映射到同一
   member，但 mapping 仍逐 requirement 保留。
8. 生成 receipt，并按 readiness 规则决定是否可进入 capture 或只能记录 gap。

future event 因 cutoff 未到不参与。cutoff 内 correction 是 terminal 时必须使用该
correction；若 terminal 已 invalidated 或质量不合法，则返回 UNAVAILABLE，不得跳回
较旧 PASS/COMPLETE event。

## Research Readiness

readiness 是确定性数据分类，不是审批、权限或人工门禁。

| Phase 0A scope | source resolution | readiness | 后续行为 |
|---|---|---|---|
| `RETROSPECTIVE_RESEARCH_ONLY` | 全部 AVAILABLE | `RESEARCH_READY` | 冻结完整 source set，可创建 capture plan |
| `RETROSPECTIVE_RESEARCH_ONLY` | 无 conflict，部分 UNAVAILABLE | `PARTIAL` | 冻结已有 member 与 gap receipt；至少一个 member 且 trace 合法时可创建 PARTIAL capture，否则仅 gap-only |
| `NONE` 且 replay eligible | UNAVAILABLE、无 conflict | `PARTIAL` | 仅 research replay/gap-only，不伪装 authoritative signal |
| 任意 | identity/hash/chain/policy conflict | `BLOCKED` | 只阻断该 scope并记录 conflict，不影响其他 scope |
| `NONE` 且 replay 不合法 | 任意 | `BLOCKED` | 仅 gap/conflict receipt |

正向可达要求：一个合法单 Alpha fixture 和一个合法原生多 Alpha fixture，在所有
required source terminal 均 AVAILABLE 时，必须确定性得到 `RESEARCH_READY`。不得因
不同 Alpha 腿的合法 lookback、row count 或 content hash 而 BLOCKED。

## Observation Version Selector

### Common terminal resolution

1. 读取调用方显式提供的同一 `canonical_signal_id` 全量 immutable revision chain。
2. 校验 revision_no 从 1 连续递增、精确 predecessor、单 successor、无 fork/cycle、
   同 signal、content hash 可复算。
3. 在 `evidence_available_at <= requested_source_cutoff` 内解析唯一最大 revision_no。
4. cutoff 内没有 revision 返回 `UNAVAILABLE`；多个 terminal、缺 predecessor、未来
   predecessor 反向污染或 hash mismatch 返回 `CONFLICT`。
5. 对 terminal 校验 admission scope、handoff readiness、exact
   `signal_source_revision_set_hash`、stage/content/hash closure。
6. 最后检查 requested capability。terminal 不满足时返回 `UNAVAILABLE`，不得回退
   较旧 COMPLETE/FULL revision。

### EXACT_REVISION_V1

request 必须显式提供 observation version id，且该 version 必须正好等于 as-of
terminal。若想复现旧 revision，必须把 frozen cutoff 放在后继 revision 的
`evidence_available_at` 之前；不得在同一 cutoff 下强制选 predecessor。

### LATEST_ELIGIBLE_REVISION_V1

`LATEST` 仅表示 as-of terminal，不表示“最新一个满足期望状态的旧版本”。selector
先确定 terminal，再检查 capability。terminal 为 PARTIAL 而请求 FULL 时返回
UNAVAILABLE，不选择旧 FULL。

## Capture Membership Binding

进入 Phase 1C-1 capture 的每个可执行 plan 必须绑定：

- `source_requirement_set` 的 id/hash；
- `source_resolution_receipt` 的 id/hash；
- exact `source_revision_set` 的 id/hash；
- trace outbox evidence；
- selector 已执行时的 `selected_observation_mapping` id/hash。

绑定通过现有 `CaptureMembership` append-only evidence role 完成，COMPLETE 时进入
排序 membership hash。不得从当前 Program、当前 binding、latest artifact 或最新
source row 补齐缺失 identity。gap-only receipt 不伪造 `CapturePlan` 所要求的
source revision set id/hash。

## Additive Schema Alignment

实现阶段只允许为父级已有契约补齐 additive 字段/表，不得创建审批或授权表：

- 给 source revision member model/schema 增加非空
  `enforced_cutoff_predicate_hash`，并纳入 member payload、set hash、持久化比较和
  migration rollback。
- requirement set、resolution receipt 和 selected mapping 在 Phase 1C fixture/local
  store 使用强类型 immutable repository oracle；进入真实 DML 前必须由后续 Phase
  1G 设计确认其最终 PostgreSQL ownership，不在本切片提前创建重复 authority 表。

本设计本身不执行任何 DDL。后续代码实现如包含 migration，只能在开发/测试验证；
生产应用是独立部署动作，不属于业务审批，也不得由运行任务隐式执行。

## Reason Codes

至少定义并稳定返回：

- `ADVISORY_PHASE1_SOURCE_REQUIREMENT_INVALID`
- `ADVISORY_PHASE1_SOURCE_REQUIREMENT_CONFLICT`
- `ADVISORY_PHASE1_SOURCE_UNAVAILABLE_AS_OF`
- `ADVISORY_PHASE1_SOURCE_CHAIN_INVALID`
- `ADVISORY_PHASE1_SOURCE_TERMINAL_CONFLICT`
- `ADVISORY_PHASE1_SOURCE_TERMINAL_INVALIDATED`（UNAVAILABLE family）
- `ADVISORY_PHASE1_SOURCE_QUALITY_INVALID`（UNAVAILABLE family）
- `ADVISORY_PHASE1_SOURCE_MEMBER_MISMATCH`
- `ADVISORY_PHASE1_SOURCE_RESOLUTION_CONFLICT`
- `ADVISORY_PHASE1_OBSERVATION_VERSION_UNAVAILABLE_AS_OF`
- `ADVISORY_PHASE1_OBSERVATION_VERSION_CHAIN_INVALID`
- `ADVISORY_PHASE1_OBSERVATION_TERMINAL_CONFLICT`
- `ADVISORY_PHASE1_OBSERVATION_EXACT_VERSION_MISMATCH`
- `ADVISORY_PHASE1_OBSERVATION_CAPABILITY_UNAVAILABLE`

source 缺失、terminal invalidated 或质量不可用等 UNAVAILABLE reason 只能形成
PARTIAL/gap；identity/hash/chain/policy 的 CONFLICT/INVALID reason 必须形成
BLOCKED。异常不得被转换为空列表、默认 member、旧 revision、成功状态或无上下文
500。

## Positive Reachability Matrix

| fixture | 关键条件 | 预期结果 |
|---|---|---|
| 单 Alpha完整 source | 每个 requirement 有唯一 PASS terminal | `RESEARCH_READY`、完整 source set、capture plan 可构造 |
| 原生多 Alpha完整 source | 公共 PIT identity 一致，各腿 lookback/window 不同且各自合法 | `RESEARCH_READY`，每腿 mapping 完整，不要求跨腿 window/hash 相等 |
| 多 Alpha共享物理 partition | 两腿 requirement 映射同一 exact member | source set 去重，receipt 保留两条 requirement mapping |
| 部分 source 缺失 | 至少一个 AVAILABLE、其余有明确 missing reason | `PARTIAL`、已有 member set + gap receipt，可构造 PARTIAL capture |
| source 全部缺失 | 无 conflict | `PARTIAL` gap-only，不伪造 source set/capture plan |
| exact selector | 显式 version 等于 cutoff terminal 且 capability 满足 | `SELECTED` |
| latest selector | cutoff terminal 满足 capability | `SELECTED` |
| future correction | correction available_at 晚于 cutoff | 选择 cutoff 内旧 terminal |

## Failure Matrix

| 情况 | 结果 | 禁止行为 |
|---|---|---|
| source chain fork/cycle/跳号 | `BLOCKED` | 不按 created_at 猜 terminal |
| cutoff 内 terminal invalidated | `PARTIAL` + enumerable gap | 不回退 predecessor，不伪造 AVAILABLE |
| cutoff 内 terminal quality 非 PASS | `PARTIAL` + enumerable gap | 不回退旧 PASS event |
| requirement/member exact field mismatch | `BLOCKED` | 不重算或覆盖 event 字段 |
| 多 Alpha 公共 PIT identity 冲突 | `BLOCKED` | 不把各腿拆成伪独立成功 |
| 多 Alpha 仅 lookback/window 不同 | 正常解析 | 不做跨腿等值校验 |
| observation cutoff 内 0 revision | `UNAVAILABLE` + gap | 不返回空成功 mapping |
| observation 多 terminal/fork | `CONFLICT` | 不按 row order 选择 |
| terminal capability 不足 | `UNAVAILABLE` | 不回退旧 COMPLETE/FULL |
| EXACT id 不是 as-of terminal | `CONFLICT` | 不强制选择 predecessor |
| source/selector receipt hash 冲突 | `BLOCKED` | 不覆盖旧 receipt |

## Implementation Plan

1. 增加 typed source requirement、resolution receipt、readiness 和 selected mapping
   models，以及 deterministic canonical hash helpers。
2. 基于现有 `InMemorySourceAvailabilityLedger`/event models 实现 fixture resolver；
   不复制 source event 状态机。
3. 增加 fixture observation chain repository 与 terminal-first selector，实现两种
   v1 policy。
4. 将 receipt/source set/selected mapping 接入现有 capture membership，并保持
   Phase 1C null/default-disabled runtime wiring 不变。
5. 补齐 `enforced_cutoff_predicate_hash` model/schema alignment 与 rollback migration，
   只做开发/测试验证，不执行生产 DDL。
6. 运行 focused unit、migration contract、独立开发/测试任务中的 DEV-DB
   rollback-only L4，以及 Selection/Paper/simulation no-wiring regression。

## Verification Plan

- 单 Alpha与原生多 Alpha正向 fixture，覆盖共享 source 和不同 lookback/window。
- event correction/invalidation/revalidation、cutoff 边界、fork/cycle、quality mismatch。
- readiness 的 READY、PARTIAL with members、PARTIAL gap-only、BLOCKED 四条路径。
- observation EXACT/LATEST terminal-first、future correction、capability no-fallback。
- request/hash idempotency、same identity/different content conflict、稳定排序。
- capture membership seal 包含 requirement、resolution、source set 和 selected mapping。
- forbidden import/call 测试证明未接入 Selection、Paper、模拟盘、QMT、broker、订单和
  runtime observer。
- migration 仅做 schema contract/rollback；数据库连接只从项目 `.env` 获取，不猜测
  连接参数，不要求每次 DDL 前创建全库备份。
- PR 阶段将共享消费者回归委派给 CI/nightly；本地只运行最小充分验证。

## Design Acceptance Index

- F-001: 显式 immutable source requirement set 可完整表达单 Alpha和原生多 Alpha
  的 source 需求，并允许各腿合法不同 lookback/window。
- F-002: fixture resolver 复用 Phase 1A ledger/revision primitives，严格解析唯一
  as-of terminal，不存在 latest/current 或 predecessor fallback。
- F-003: resolution receipt 完整绑定 requirement-to-member、gaps、exact revision
  set 和 readiness；缺失与冲突不会混淆。
- F-004: RESEARCH_READY、PARTIAL with members、PARTIAL gap-only 和 BLOCKED 均有
  明确且可测试的正向/负向路径。
- F-005: observation selector 完整实现 EXACT/LATEST terminal-first 和 capability
  post-check，选择结果进入 immutable mapping hash。
- F-006: source/receipt/selected mapping 进入 capture membership，且不修改 Phase
  1C-1 observation identity 或共享 Selection/Paper/simulation 运行链。
- F-007: 父级 `enforced_cutoff_predicate_hash` 缺口被显式补齐；不存在空值、占位
  hash 或静默省略。
- F-008: 本切片不包含审批、角色、授权、人工门禁、生产激活或运行时 DDL。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | proposed source requirement models | single/multi-alpha different-window fixture golden | design_ready | none |
| F-002 | proposed fixture source resolver reusing Phase 1A primitives | chain/as-of/correction/invalidation tests | design_ready | none |
| F-003 | proposed resolution receipt and mapping | deterministic hash, idempotency, gap/conflict tests | design_ready | none |
| F-004 | proposed readiness classifier | four reachable readiness-path tests | design_ready | none |
| F-005 | proposed fixture observation selector | EXACT/LATEST/no-fallback tests | design_ready | none |
| F-006 | existing CaptureMembership plus proposed evidence roles | membership seal and forbidden-wiring tests | design_ready | none |
| F-007 | proposed additive source member alignment | model/schema/hash/rollback contract tests | design_ready | none |
| F-008 | package and runtime boundary | forbidden approval/auth/import/runtime-DDL checks | design_ready | none |

## Operational Effects

- Code/runtime activation: `noop`，本任务只交付详细设计。
- Production DDL/DML: `noop`。
- Frontend/backend dependency: `noop`。
- Scheduler/process/service restart: `noop`。
- Approval/role/authorization/manual gate: 不存在，也不在后续实现范围。
- Existing Selection/Paper/simulation behavior: 必须保持不变。

## Production Gates

本节仅满足 AIstock F2 文档模板的状态记录要求，不新增任何产品门禁、审批流程、
角色、授权或人工确认：

- `production_ddl_gate`: `noop`；本设计不执行生产 DDL。
- `production_dml_gate`: `noop`；本设计不执行生产 DML。
- `production_frontend_dependency_gate`: `noop`。
- `production_backend_dependency_gate`: `noop`。
- `runtime_activation_gate`: `noop`；无 scheduler、observer、worker 或服务接线。
- `approval_gate`: 不存在。

## Risks

- requirement-to-member mapping 若只依赖去重后的 source set，会丢失多 Alpha component
  provenance；receipt 必须保留逐 requirement mapping。
- 把 PARTIAL 当作 conflict 会阻断合法研究数据积累；把 conflict 当作 PARTIAL 又会
  隐藏 identity 错误。两类状态必须使用不同 reason family。
- selector 若先过滤 COMPLETE/FULL 再取 latest，会产生未来修订后的静默回退；必须
  terminal-first。
- 在本切片提前接入真实 observer 或 market table 会伪造历史 available-at；必须留给
  Phase 1D/1G。
- schema alignment 若遗漏 canonical hash 版本变化，会出现相同 ID 表示不同 payload；
  实现时必须升级 schema version 并覆盖 retry/conflict 测试。

## Rollout And Rollback

- 本文档合入不会激活任何运行路径。
- 后续实现先落 fixture/local repository 与 focused tests；默认 null wiring 保持不变。
- additive migration 只在明确的开发/测试执行任务中应用并验证 rollback；生产 DDL
  不由本设计或运行程序自动执行。
- 回滚只移除未激活代码/测试和未应用 migration；已经存在的 Phase 1A/1C-1
  append-only evidence 不得修改或删除。

## DESIGN-COMPLIANCE-001

- [x] 与父级 source terminal、version selector、readiness 和 research-only 语义一致。
- [x] 未把 fixture/local-store 设计描述为完整 Phase 1、production observer 或训练能力。
- [x] 未引入 simplified、placeholder、mock-only success 或 silent fallback。
- [x] 单 Alpha和原生多 Alpha均有完整正向路径；不同腿合法 window 不会被错误阻断。
- [x] AVAILABLE、UNAVAILABLE、CONFLICT 及 READY、PARTIAL、BLOCKED 的转换唯一明确。
- [x] exact source set、resolution receipt、selected mapping 和 capture membership 的 hash
  边界明确。
- [x] 未修改或接入 Selection、Paper、模拟盘、QMT、MiniQMT、broker 或订单链。
- [x] 未新增审批、角色、授权、人工确认或额外业务门禁。
- [x] 未授权或执行生产 DDL/DML、依赖安装、服务重启或运行时激活。
