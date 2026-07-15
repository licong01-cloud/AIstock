# MiniQMT Adaptive IS Phase 0B：B0 基线观察、事实校准与数据冻结详细设计

> 文档状态：`design_ready`（仅表示设计可直接实施，不表示代码、DDL、生产配置或真实观察已完成）
> Feature Workflow：`F2`
> 任务等级：`T3 / P1 / design-driven`
> 运行边界：`SIM only / MiniQMT only / B0_QUOTE_V2 only`
> 日期：2026-07-15
> 唯一上位蓝图：
> - `docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md`
> 算法域与阶段下位输入：
> - `docs/analysis/miniqmt_intraday_execution_strategy_analysis_20260710.md`
> - `docs/architecture/miniqmt_adaptive_is_phase0_tca_design.md`
> - `docs/architecture/miniqmt_adaptive_is_phase1_quote_contract_design.md`
> - `docs/operations/miniqmt_quote_evidence_runbook.md`

## 0. 结论与红线

Phase 0B 的交付物不是新执行算法，也不是一次“看起来正常”的模拟盘运行；它是针对冻结的 `B0_QUOTE_V2` control，在运行前预注册完整观察窗口，使用 Phase 0A TCA 与 Phase 1 行情证据链形成可重复查询、可重建、可追溯且不可变的 B0 baseline receipt。Phase 0B 完成前，不允许进入 Phase 2/3 的 `ADAPTIVE_IS_L1` 代码或 dry-run，更不允许 B1 broker side effect、canary、activation 或 promotion。

本文是唯一上位蓝图的 Phase 0B 下位阶段设计。Phase 0B 实施前，必须先证明上位蓝图中与 MiniQMT 唯一 runtime、`B0_QUOTE_V2` 冻结 revision、fail-loud health、测试隔离、生产状态分离和阶段顺序有关的适用条款已经满足。LocalSIM 的独立 P0 修复不改变本阶段统计口径，但共享 scheduler 隔离和平台 health 不能以 LocalSIM/MiniQMT 任一侧的错误为代价。每个 Phase 0B 功能或 BUG PR 必须同步更新上位蓝图当前进度账本。

本设计固定以下 P0 约束：

1. 禁止交付简化版、子集版、POC、placeholder 或 mock-only 路径，并把它们宣称为 Phase 0B 完成。
2. 缺失、冲突、过期、时钟域不可信、覆盖不足、数据库写入或 readback 失败必须返回 typed failure；不得用零值、默认值、旧缓存、内存结果或日志代替成功。
3. 观察层不得创建、修改、取消、重放或节流任何 broker action；不得改变选股、策略信号、localSIM、MiniQMT B0 执行及 Phase 0A/1 的业务语义。
4. 不新增用户未要求的审批、RBAC、人工 acknowledge、人工 promote 或运行时门禁。本文的“Production Gates”是可验证的交付前置事实，不是产品审批功能。
5. 本文不授权实施代码、执行 DDL/DML、写生产配置、调用 broker 或重启服务；这些状态必须与设计完成、代码合入、生产启用和真实运行分别汇报。

## 1. Background

### 1.1 当前已具备的权威输入

Phase 0A 已建立 append-only TCA 事实与 `execution_tca_rebuild_receipt`，其回执包含 scope、generation/supersedes、source watermark/count/hash、coverage、invariant、query/code version 及 `COMPLETED/FAILED`。Phase 1 已建立 `MarketDataEvidenceV1`、五档快照、action/reject/child/trade receipt、60/300/900 秒 markout、cadence aggregate、active+archive 读取和 Phase 0B v2 export seam。

Phase 1 v2 export 以 `binding_id + trade_date` 为读取范围，在同一数据库只读快照内输出：

- control revision 及 policy/config/adapter/code/schema hash 集合；
- action input/reject、五档、age、cadence、markout 的计数与覆盖；
- parent/assignment/action/child/trade/markout 缺链；
- duplicate child、revision/hash/identity conflict；
- records hash、manifest hash 与 `quote_control_complete`。

`quote_control_complete=true` 只证明单日单 binding 的 Phase 1 结构链满足该导出的完整性规则，不证明观察窗口完整、Phase 0A final TCA 完成、terminal residual 已全部归因、延迟和流动性分母可用，也不证明 B0 baseline eligibility。

### 1.2 当前缺口

现有事实层没有表达以下 Phase 0B 权威对象：

- 在第一条纳入事件之前持久化的观察规范；
- 多交易日 control identity 冻结及每日 binding membership；
- snapshot-to-ack、cancel latency 的时间权威与质量分类；
- PIT ADV、EOD 分钟量与可执行五档深度的明确单位、分母和 coverage；
- completion、IS、markout、forced sweep、reject/cancel/reconcile 的跨日分布与预注册 bucket；
- 数据冻结 hash、日间方差、baseline eligibility 与同查询重建回执。

因此 Phase 0B 必须增加独立的“观察规范 + 聚合回执”权威，但不得复制 Phase 0A/1 原始事实或创建第二套 quote schema。

## 2. Scope

### 2.1 范围内

- `B0_QUOTE_V2` MiniQMT SIM control 的正式观察窗口预注册。
- 以稳定策略/账户槽位 lineage 约束每日动态 binding membership。
- Phase 0A TCA receipt 与 Phase 1 v2 export 的同快照读取、资格判定和 hash 固化。
- completion、IS、60/300/900 秒 markout、forced sweep、reject、cancel、reconcile、延迟及流动性参与率的预注册聚合。
- append-only baseline receipt、日级 membership、固定维度 bucket、data freeze 与 superseding generation。
- 对每个 receipt 保存 exact source records 的不可变 content-addressed data-freeze artifact，使旧 generation 不受上游更正或 archive policy 影响。
- 只读 API/CLI、bounded-cardinality metrics/alerts、operator runbook。
- migration、repository、确定性重建、真实 DEV DB 与真实 SIM 验证设计。

### 2.2 支持的正式观察对象

- `environment=SIM`
- `broker_backend=minqmt_sim`（精确复用 `SimulationBrokerBackend.MINIQMT_SIM.value`）
- `execution_policy=B0_QUOTE_V2`
- 当前策略包可以是单 alpha 或多 alpha；Phase 0B 只读取进入模拟盘后已经通过唯一一次入场完整性校验的 package identity，不在选股或执行阶段二次校验策略包内容。
- 每个 observation spec 只允许一个稳定 lineage、一个冻结 control tuple 和一组明确交易日。

## 3. Non-Goals

- 不设计或实现 `ADAPTIVE_IS_L1`、B1 参数、政策选择、在线学习或自适应 mutation。
- 不改变 signal/selection、策略包入场校验、localSIM 或 MiniQMT broker action 路径。
- 不把 localSIM 结果混入 MiniQMT B0 broker baseline。
- 不混合 `LEGACY_B0` 与 `B0_QUOTE_V2`；若要观察 LEGACY_B0，必须另建规范和 series，且不满足本阶段退出条件。
- 不新增实时行情字段合成；auction 字段仍遵守 Phase 1 capability，禁止从普通 quote 推导。
- 不新增页面、审批流、RBAC、人工 acknowledge 或人工 promotion。
- 不规定 Phase 4 challenger 相对 B0 的收益/成本胜出阈值；Phase 0B 只冻结事实、覆盖与 B0 分布。
- 不在本设计阶段执行生产 DDL/DML、注册 observation spec、启动窗口或变更服务。

## 4. Architecture

### 4.1 单一权威链

```text
Phase0BObservationSpecV1 (run 前不可变预注册)
        |
        v
每日 binding lineage + Phase 1 v2 export manifest/records hash
        |                         |
        |                         +--> MarketDataEvidenceV1 / action / child / trade / markout
        v
Phase 0A COMPLETED TCA receipt --> execution/TCA terminal facts
        |
        v
同一 REPEATABLE READ source snapshot
        |
        v
typed projection -> qualification -> aggregate -> immutable baseline receipt
        |                         |
        |                         +--> immutable Phase0B data-freeze CAS artifact
        +--> day membership
        +--> bounded bucket rows
        +--> read-only API/CLI/metrics/runbook
```

Phase 0B 保存引用、hash、coverage 和聚合，不复制 parent、child、trade、quote 或 TCA 明细。任何明细追溯都必须回到 Phase 0A/1 的 ID 链及其 active/archive 权威查询。

### 4.2 状态机

规范和回执状态按数据事实自动推导，不包含人工审批状态：

```text
REGISTERED -> COLLECTING -> READY_TO_BUILD -> MATERIALIZING -> COMPLETE
                  |                                  |   \
                  v                                  v    v
       QUALIFICATION_FAILED -> MATERIALIZING   INELIGIBLE FAILED

COMPLETE/INELIGIBLE --(late authoritative fact / watermark drift)--> STALE_SUPERSEDED_REQUIRED
STALE_SUPERSEDED_REQUIRED --(generation + 1 rebuild)----------------> COMPLETE/INELIGIBLE
```

- `REGISTERED`：规范已经原子持久化并生产 readback；窗口尚未开始。
- `COLLECTING`：当前时间进入预注册窗口，仍只运行既有 B0 路径。
- `READY_TO_BUILD`：所有预期交易日已越过 post-close finalization deadline，源事实稳定。
- `QUALIFICATION_FAILED`：某日事实不满足纳入规则，保留 typed reasons，不允许静默替日；仍应 materialize 完整 ineligible receipt。
- `INELIGIBLE`：`COMPLETED + INELIGIBLE` 回执已提交并通过独立 readback，但未满足预注册 coverage/identity/terminal/sample 条件。
- `FAILED`：materialization 的技术事务失败；它不等价于业务 `INELIGIBLE`。
- `COMPLETE`：仅表示 `COMPLETED + ELIGIBLE` 回执、日级 membership 与 bucket 已在同一写事务提交并通过独立 readback。
- `STALE_SUPERSEDED_REQUIRED`：只读计算状态，旧行不更新；新权威事实要求 generation+1 回执 supersede。

### 4.3 时间和交易日边界

1. 时间戳一律使用 timezone-aware UTC；交易日以 `Asia/Shanghai` 交易日历解释。
2. spec 显式保存由权威 TradingCalendar 展开的 `expected_trade_dates` 及 calendar snapshot hash。
3. 每日观察范围从该 binding 的 pre-open scheduler boundary 到 post-close reconcile/final markout deadline；具体 UTC 边界在 spec 中逐日保存，禁止运行时默认推导。
4. 初始正式基线窗口必须包含 5 个连续的、预注册时已知的有效交易日；5 日用于形成日间方差。少于 5 日只能作为 qualification evidence，不可形成 Phase 0B eligible baseline。
5. 休市、临时停市和交易日历更正必须形成新的 spec 或 generation；不得把非交易日当作零活动有效日。
6. 服务重启不自动使交易日失败；只有 durable recovery 后所有事实、水位和链仍完整，才允许该日纳入。

## 5. Contracts

### 5.1 `Phase0BObservationSpecV1`

规范必须在第一个 `window_start_at_utc` 之前完成持久化与独立 readback。没有规范的历史运行可用于发现问题，但不能事后登记为正式 baseline。

| 字段 | 类型 | 约束与语义 |
|---|---|---|
| `schema_version` | string | 固定 `phase0b_observation_spec_v1` |
| `spec_id` | string | `p0bspec_` + canonical payload SHA-256 前 32 hex |
| `series_key` | string | 稳定 lineage + control tuple 的 SHA-256 identity |
| `environment` | enum | 仅 `SIM` |
| `broker_backend` | enum | 仅字符串值 `minqmt_sim` |
| `execution_policy` | enum | 仅 `B0_QUOTE_V2` |
| `strategy_id` | string | 稳定策略 identity；与 `paper_v2.simulation_release_binding.strategy_id TEXT` 同型 |
| `account_group_id` | string | 稳定 SIM account group |
| `strategy_slot_id` | string | 稳定运行槽位；禁止仅用易变 binding_id 作为 series |
| `package_id` | string | 已通过入场校验的 package identity |
| `package_manifest_sha256` | char(64) | 单/多 alpha 均以 manifest hash 固化，不二次解析包资产 |
| `control_revision_id` | string | Phase 1 revision identity |
| `policy_hash` | char(64) | 冻结 B0 policy |
| `config_hash` | char(64) | 冻结有效配置 |
| `adapter_hash` | char(64) | 冻结 broker adapter |
| `code_hash` | char(64) | 冻结运行代码 |
| `evidence_schema_hash` | char(64) | 冻结 Phase 0A/1 schema/query contract |
| `expected_trade_dates` | date[] | 5 个连续有效交易日，顺序唯一 |
| `daily_boundaries` | jsonb | 每日 pre-open/start/post-close/finalization UTC 边界 |
| `calendar_source_version` | string | TradingCalendar version |
| `calendar_snapshot_sha256` | char(64) | 展开结果的 canonical hash |
| `required_markout_horizons_seconds` | int[] | 精确 `[60,300,900]` |
| `markout_finalization_grace_seconds` | int | 必填正整数；等待 Phase 1 durable capture/terminal unavailable |
| `reconcile_finalization_grace_seconds` | int | 必填正整数；等待 final broker reconciliation |
| `adv_window_trading_days` | int | 初始规范固定 20；只看观察日前完成日 |
| `bucket_spec` | jsonb | 全部分桶边界、闭开区间、单位和空值规则 |
| `coverage_thresholds` | jsonb | 每项预注册门槛；不得运行后改变 |
| `sample_sufficiency_thresholds` | jsonb | parent/child/fill/exact-latency 等最小样本数，必须显式正整数 |
| `formula_versions` | jsonb | IS/markout/latency/liquidity/variance 公式版本 |
| `source_query_versions` | jsonb | Phase 0A、Phase 1、market daily/minute 查询版本 |
| `late_fact_policy` | enum | 固定 `SUPERSEDE_NEW_GENERATION` |
| `created_at_utc` | timestamptz | 审计字段，不进入 content hash |
| `created_by_tool` | string | CLI/tool version；不是审批人 |
| `content_sha256` | char(64) | 规范 canonical payload hash |

`coverage_thresholds` 必须至少声明：expected-date、Phase 0A receipt、Phase 1 structural chain、terminal attribution、TCA benchmark/join、action quote/five-level/age、latency classification、60/300/900 terminal outcome、PIT ADV、minute volume、depth denominator、reconcile finalization 的 numerator/denominator 和要求。对于本阶段关键结构链，门槛固定为 100%；分母为零时使用 typed `NOT_APPLICABLE`，不得伪造 100%。`sample_sufficiency_thresholds` 至少声明 window total parent、child action、filled parent、callback-exact ack、可计算 IS/各 horizon markout 的最小 count；值由首个 rollout spec 在看见窗口结果前给出，schema 不提供默认值，未提供则拒绝注册。

### 5.2 Canonical identity 与 hash

所有 content hash 使用 UTF-8、RFC 8785 风格 canonical JSON：对象 key 排序；枚举大写；UTC 规范到微秒并以 `Z` 结尾；Decimal 使用无指数规范字符串；集合先按稳定 identity 排序；禁止 NaN/Infinity；审计时间、数据库自增 ID 和日志文本不进入业务 hash。

- `spec_id = p0bspec_ + sha256("miniqmt_phase0b_spec_v1" || canonical(spec_content))[0:32]`
- `series_key = sha256(environment, broker_backend, strategy_id, account_group_id, strategy_slot_id, package_id, package_manifest_sha256, execution_policy, control_revision_id, policy_hash, config_hash, adapter_hash, code_hash, evidence_schema_hash)`
- `receipt_scope_hash = sha256(spec_id, expected_trade_dates, sorted daily binding memberships, source_query_versions, formula_versions, bucket_spec)`
- `input_sha256 = sha256(sorted Phase0A receipt IDs/output hashes, sorted Phase1 manifest/records hashes, daily/minute source watermarks/content hashes)`
- completed `output_sha256 = sha256(receipt summary, sorted day rows, sorted bucket rows)`；failed `output_sha256 = sha256(canonical failure envelope)`
- completed：`baseline_receipt_id = p0brct_ + sha256(series_key, generation, receipt_scope_hash, input_sha256, output_sha256)[0:32]`
- failed：`baseline_receipt_id = p0brct_ + sha256(series_key, generation, receipt_scope_hash, failure_attempt_sha256)[0:32]`

同一 identity 与相同 payload 必须幂等返回同一行；同一 identity 不同 payload 是 `PHASE0B_IDENTITY_PAYLOAD_CONFLICT`，事务回滚并报警。相同 completed input 在相同 query/formula/code/schema version 下必须得到相同 output；已有 FAILED attempt 不阻止后续 generation 对同一 source input 成功重建。任何 control tuple 字段变化必须建立新 spec/series，禁止跨 revision 拼接。

### 5.3 每日 binding membership 与纳入规则

每个预期交易日必须解析到且只解析到一个符合 spec lineage 的有效 MiniQMT SIM binding。日级 membership 固化：`trade_date`、`binding_id`、binding lifecycle/version hash、Phase 0A receipt ID/output hash、Phase 1 manifest/records hash、源 watermark、qualification result/reasons。

允许纳入的必要条件：

1. binding 的 stable lineage、package manifest 和完整 control tuple 与 spec 完全一致。
2. Phase 1 v2 export `quote_control_complete=true`，且 revision/hash/identity/duplicate/missing-link 冲突均为零。
3. 对该日纳入的所有 execution parent，Phase 0A 均存在 `COMPLETED` 且 readback 一致的 final receipt；不存在旧 receipt 被更新冒充 final。
4. 所有 parent 均到达 `RECONCILED_FINAL` 或明确的 no-order terminal outcome；terminal residual attribution 为 100%，unattributed count 为 0。
5. active 与 archived facts 均由 bounded authoritative query 纳入；不能仅因 API 默认分页或 archive 切换而丢失。
6. 服务重启、迟到成交、迟到 markout 和 reconcile 更正均已越过 finalization boundary 并反映到 source watermark。

同日出现零个或多个候选 binding、lineage 漂移、非预期 policy、窗口中途 revision 变化时，该日 `QUALIFICATION_FAILED`；不得自动选“最新”或替换为另一交易日。

### 5.4 Phase 0A/1 关联链

Phase 0B 只通过以下稳定链重建：

```text
observation spec
 -> daily binding membership
 -> Phase 1 MarketDataEvidenceV1(runtime_id, binding_id, parent_order_id,
                                  action_id, child_order_id, trade_id,
                                  revision_id, five hashes)
 -> existing action/reject
 -> child intent/order
 -> trade/fill
 -> child receipt + 60/300/900 markout
 -> Phase 0A parent/TCA final result + rebuild receipt
```

禁止更新旧 runtime/child event 回填新 receipt ID。关联缺失必须保留具体 ID、source table、expected relation 和 reason code 的诊断计数；API 默认只返回聚合和 bounded samples，完整明细通过相同 hash-scoped export 获取。

#### 5.4.1 60/300/900 秒 markout 与交易日归属

- Phase 0B 不创建第二套 markout timer；唯一事实仍由 Phase 1 按每笔 authoritative fill time 的 `+60/+300/+900` 秒 durable schedule 产生。
- Phase 0B 对每笔 fill × horizon 要求一个 terminal outcome：`CAPTURED` 或 Phase 1 已稳定记录的 `UNAVAILABLE`；尚在 due/grace 内只能是 `PENDING_FINALIZATION`，禁止构建 final receipt。
- builder 的最早执行时刻为当日 scope `finalize_after_at_utc`，且该边界不得早于最后一笔 fill 的 900 秒 horizon、`markout_finalization_grace_seconds` 和 final reconcile boundary 三者的最大值。
- 重启后由 Phase 1 从 durable pending markout 恢复；Phase 0B 只读其恢复结果。缺 timer、缺 durable pending 或进程内 timer 丢失不能由 Phase 0B 以当前 quote 补算。
- 迟到成交以该成交的权威 fill time 归属原 parent/trade date，并重新产生/等待其三个 horizon；它使旧 head stale。不得把跨午夜到达的回调归到到达日，也不得用下一交易日开盘 quote 补前一日 session-ended markout。
- 交易时段结束使目标 horizon 无可比 quote 时，只接受 Phase 1 合同定义的稳定 unavailable reason；unavailable 与 captured 的 coverage 分开，不能作为数值零进入均值。

### 5.5 延迟契约

#### 5.5.1 Snapshot-to-ack

- 起点：实际产生该 child action 的 `MarketDataEvidenceV1.received_at_utc`；若 action 没有有效 evidence ID，记录 `ACTION_QUOTE_LINK_MISSING`，不得改用最近 quote。
- 终点优先级：同一 child 的 broker callback ACK timestamp；没有 exact callback 但有 durable reconciliation 首见 timestamp 时，只能记为 upper bound；两者均无则 `UNAVAILABLE`。
- 质量枚举：`CALLBACK_EXACT`、`SNAPSHOT_OBSERVED_UPPER_BOUND`、`UNAVAILABLE`。
- 只在相同受支持时钟域内相减。负值、clock domain mismatch、timestamp precision 不满足公式版本时为 typed invalid，不取绝对值、不截零。

#### 5.5.2 Cancel latency

- 起点：durable cancel-request event 的时间；没有请求事实不能从订单终态反推。
- 终点：broker callback 或 reconciliation 首次观察到 `CANCELED/FILLED/REJECTED` terminal state 的时间。
- 迟到成交必须单独分类 `LATE_FILL_AFTER_CANCEL_REQUEST`，并继续进入 completion/IS/TCA，不允许把它记为取消成功。
- 质量枚举与 snapshot-to-ack 相同，exact 与 upper bound 分布禁止合并。

#### 5.5.3 聚合

每种 latency kind/quality 输出 `count/null_count/invalid_count/min/p50/p90/p95/p99/max/mean`；分位数算法及 Decimal rounding 写入 `formula_versions`。任何必须质量类别 coverage 不达 spec 门槛，baseline `INELIGIBLE`。

#### 5.5.4 Runtime latency anchor payload v1

当前 runtime 已有 `CHILD_ORDER_SUBMITTED`、`CHILD_ORDER_REJECTED`、`CHILD_ORDER_CANCEL_REQUESTED` 和 `ORDER_EVENT`，但既有 payload 不能无条件证明 broker exact timestamp：submit/reject event 在 gateway 返回后 append；部分 cancel event 也在 gateway 调用后 append。Phase 0B 禁止把这些 `event_time` 直接重命名为 exact ack/cancel latency。

实施必须在不增加 broker 调用、不改变 order/cancel decision 的前提下，为新产生的既有 event payload 增加 `latency_anchor` 对象；不回填旧 event，不新增 event type/source：

| 字段 | 类型 | 语义 |
|---|---|---|
| `schema_version` | literal | `miniqmt_latency_anchor_v1` |
| `anchor_id` | string | `mqlat_` + canonical anchor SHA-256 前 32 hex |
| `anchor_kind` | enum | `SUBMIT_ACK/CANCEL_REQUEST/CANCEL_TERMINAL` |
| runtime/binding/parent/child/action IDs | string | 全部按 source event 可用性强校验 |
| `market_data_id` | string nullable | B0 quote action 的 submit/reject 必填；cancel 通过 child 原始 action 关联 |
| `gateway_call_started_at_utc` | timestamptz | 在既有 gateway 调用紧前捕获，不改变调用参数/顺序 |
| `gateway_returned_at_utc` | timestamptz | gateway 返回紧后捕获 |
| `broker_event_at_utc` | timestamptz nullable | 仅 provider 明确给出的 broker timestamp |
| `observed_terminal_at_utc` | timestamptz nullable | callback/order event 或 reconcile 首见 |
| `clock_domain` | enum | `PROCESS_UTC/BROKER_DECLARED_UTC/UNKNOWN` |
| `quality` | enum | `CALLBACK_EXACT/SNAPSHOT_OBSERVED_UPPER_BOUND/UNAVAILABLE` |
| `raw_timing_payload_sha256` | char(64) nullable | timing raw canonical hash，不复制不受控 payload |
| `anchor_sha256` | char(64) | 除 source event sequence/审计时间外的 canonical content hash |

submit/reject anchor 嵌入对应 `CHILD_ORDER_SUBMITTED/REJECTED`（`source=gateway`）；算法取消嵌入 `CHILD_ORDER_CANCEL_REQUESTED`（`source=algo`），operator 既有取消嵌入同 event type（`source=operator`）；terminal 读取 `ORDER_EVENT`（`source=gateway`）或 final reconcile。批量 cancel 必须为每个 child 生成独立 anchor，不使用整个循环结束时间代替每个请求时间。

如果 crash 发生在 broker call 与 event persistence 之间，reconcile 仍负责 broker truth，但该 latency anchor 标为 `UNAVAILABLE`；不得重复 broker call 获取延迟。新增 payload 字段会改变 code/schema hash，因此只能进入新注册的 B0 control tuple/window，不能与旧 revision baseline 拼接。Phase 1 v2 export 必须以 typed schema 解析这些 anchor 并纳入 records/manifest hash；未知字段、重复 anchor、同 ID 不同 hash均使 export incomplete。

event type/source CHECK allowlist不需要扩展：forward migration preflight 必须验证 `ck_miniqmt_event_type` 仍包含上述既有 types，`ck_miniqmt_event_source` 仍包含 `gateway/algo/operator` 及 Phase 1 的 `quote_ingress`，定义与已合入 P1-D target 完全一致；若 drift 则停止。不得为避免 preflight 而 drop/recreate 为更宽泛的任意字符串 CHECK。

### 5.6 流动性与参与率契约

#### 5.6.1 数量与单位

- 所有订单数量、分钟量、ADV 和深度统一为股；源字段是手时必须精确 `*100`。
- parent `Q_e` 是 arrival 时已通过既有 B0 业务约束的 eligible parent quantity；Phase 0B 不重新计算 eligibility。
- child `Q_c` 是 durable child action 的 requested quantity，不使用最终成交量替代请求量。
- 零、负数、非整数股、单位缺失或溢出均为 typed invalid，不做默认换算。

#### 5.6.2 ADV20

- `ADV20` 使用观察日前 20 个已经完成的权威交易日 `market.kline_daily_raw.volume_hand * 100`。
- 不包含当日或未来数据，不使用 qfq/复权成交量，不前向填充，不缩短窗口，不以五档或分钟量代替。
- 少任一预期日即 `ADV_REFERENCE_INCOMPLETE`；该 ratio 为 NULL，同时保留 coverage，不能进入 headline bucket。
- 指标为 `Q_e / ADV20_shares`，以 Decimal 计算。

#### 5.6.3 分钟量

- 分母来自 post-close 已持久化的 `market.kline_minute_raw` exact symbol/trade-date/minute bar，并复用项目既有“手到股、日期一致、严格递增、非负”的单位/质量契约。
- arrival minute 由 evidence exchange time 映射；没有可信 exchange time 不改用 receive time。
- 指标为 `Q_e / minute_volume_shares` 和 `Q_c / minute_volume_shares`；分母为零时 ratio NULL、reason=`ZERO_MINUTE_VOLUME`。
- 不调用实时行情补历史，不从 cadence count 合成成交量。

#### 5.6.4 五档可执行深度

- 买单分母为有效 ask1..ask5 quantity 之和；卖单分母为有效 bid1..bid5 quantity 之和。
- auction capability 缺失时不得从普通 quote 合成 auction price/volume；auction 行为按 Phase 1 `OBSERVE_ONLY` 证据分类并与 continuous 分开。
- 指标为 `Q_c / executable_opposite_five_level_depth_shares`。level 缺失、price/quantity 非法或能力不支持时 ratio NULL 并记录 typed reason。

### 5.7 预注册 bucket 与 cardinality

`bucket_spec` 必须在 spec 中显式给出 Decimal 边界、区间闭开规则、overflow 和 NULL/invalid bucket。运行时没有默认边界。初始正式 spec 至少包含：

- `Q_E_OVER_ADV20`
- `Q_E_OVER_MINUTE_VOLUME`
- `Q_C_OVER_MINUTE_VOLUME`
- `Q_C_OVER_L5_EXECUTABLE_DEPTH`
- `QUOTE_AGE_MS`
- `SNAPSHOT_TO_ACK_MS`（按 quality 分开）
- `CANCEL_LATENCY_MS`（按 quality 分开）
- `PARENT_SIZE_SHARES`

持久化只允许预注册的一维 bucket 及以下固定交叉视图：`liquidity_bucket × market_session`、`liquidity_bucket × terminal_outcome`、`liquidity_bucket × forced_sweep`。禁止任意维度笛卡尔积、symbol/package/order ID label 或动态 JSON key 形成无限 cardinality。

### 5.8 基线指标契约

按 window、day、预注册 bucket 输出，分子/分母和 NULL/invalid count 均持久化：

- eligible/requested/filled/canceled/rejected/reconciled quantity 与 order count；
- completion ratio；
- arrival IS、decision/benchmark IS（只使用 Phase 0A 已冻结公式）；
- 60/300/900 秒 markout，区分 `CAPTURED`、稳定 `UNAVAILABLE`、仍待 finalization；
- forced sweep count/quantity/IS，仅依据稳定 action stage `MINIQMT_EVENT_LOOP_TAIL_SWEEP` 与 durable `tail_sweep` metadata，不按时间/价格猜测；
- reject/cancel/reconcile reason 分布；
- snapshot-to-ack/cancel latency quality-separated distributions；
- Q/ADV、Q/minute、Q/depth distributions；
- 日间 count/rate/mean/quantile 的方差与有效日数。

headline 指标只有在对应 denominator coverage 达到 spec 门槛时才生成；否则字段为 NULL、回执仍可 `COMPLETED`，assessment 必须为 `INELIGIBLE`。缺数据永远不能按零成本、零延迟、零 markout 或 100% completion 处理。

### 5.9 Cadence aggregate

Phase 1 cadence aggregate 的 count 与 first/last accepted hash 只证明行情接收节奏，不提供成交量。Phase 0B 必须：

- 读取其 cadence count/first/last/coverage，按 day/session 记录观察覆盖；
- 将 cadence source watermark/hash 纳入 `input_sha256`；
- 对 gap、重复、out-of-order 和 archive coverage 输出 typed counts；
- 绝不从 cadence count 合成分钟成交量、quote age 或 market activity；
- cadence 缺失使对应 coverage 失败，但不得改变 B0 执行动作。

### 5.10 Baseline receipt 合同

`Phase0BBaselineReceiptV1` 必须包含：

- identity：receipt ID、spec ID、series key、generation、supersedes receipt ID；
- status：`COMPLETED/FAILED`；assessment：`ELIGIBLE/INELIGIBLE`；
- scope：expected/qualified/included dates 与 exact daily binding memberships；
- source：Phase 0A receipt IDs/output hashes，Phase 1 manifest/records hashes，daily/minute/calendar watermarks/hashes；
- data freeze：artifact schema/URI/content SHA-256/byte count/record counts/store version 与 verified-at-build=true；
- control：revision + policy/config/adapter/code/schema hashes；
- counts/coverage/invariants/reason histogram；
- metrics/distributions/variance summaries；
- `receipt_scope_hash/input_sha256/output_sha256/receipt_sha256`；
- query/formula/aggregator code/schema versions；
- materialization/readback timestamps（仅审计，不进入 content hash）。
- failed receipt 另含 `failure_attempt_sha256`、`failure_reason_code`、`failure_stage`、`failure_class` 与 bounded `failure_context`；completed receipt 的这些字段必须为 NULL。
- completed receipt 的 `input_sha256` 必填；failed receipt 在 source snapshot 尚未形成时允许为 NULL，但 `failure_attempt_sha256/output_sha256/receipt_sha256` 始终必填。

`COMPLETED + INELIGIBLE` 表示计算和持久化正确但数据不足；`FAILED` 表示技术构建失败且不能提供任何成功回执。`readback_verified` 是独立读取后按 receipt content/hash 计算的响应/操作证据，不通过 UPDATE 写回 append-only receipt。只有 `COMPLETED + ELIGIBLE` 且独立 readback 验证成功，才满足 Phase 0B 数据退出条件。

#### 5.10.1 `Phase0BDataFreezeManifestV1`

每次 completed build 都必须先生成不可变 data-freeze artifact；发生在 artifact 已生成之后的可持久化 failed attempt 引用同一 artifact，artifact store 本身失败时允许 FAILED receipt 的 artifact 字段统一为 NULL并记录 stage。completed artifact 包含精确计算输入；可生成的 failed artifact 至少包含已经读取的 bounded source manifest 与 failure envelope。completed artifact 的 UTF-8 canonical JSONL 顺序固定：

1. 一行 `phase0b_data_freeze_manifest_v1`，含 spec/series/receipt scope、query/formula/schema versions 与以下 section counts/hashes；
2. observation spec/scope；
3. 每日 binding membership 与 release/control identity；
4. Phase 0A completed receipt、membership 和 TCA result canonical payload；
5. Phase 1 v2 export manifest 与按稳定 identity 排序的 records（含 active/archive source）；
6. daily/minute liquidity reference rows与 calendar rows；
7. typed projection rows、qualification inputs 和 latency anchors。

每条 data line 固定为 `{record_type, identity, payload_sha256, payload}`；section 与 record 均按注册顺序排序，禁止 NaN/Infinity、非 canonical timestamp 或隐式默认字段。`data_freeze_sha256` 对完整未压缩 JSONL bytes 计算；压缩只能作为传输层，不能改变 identity。

artifact URI 只允许 `phase0b-cas://sha256/{64-lower-hex}`，不能在 API 暴露宿主机路径。存储实现 `Phase0BEvidenceArtifactStore` 必须：

- 要求显式配置的专用 evidence root，不复用 strategy package asset namespace，也没有内存/当前目录 fallback；
- 以同目录临时文件写入，flush + fsync，复算 bytes/hash 后 atomic rename 到 content address；目标已存在时逐 byte/hash 验证后幂等复用；
- 在 DB receipt transaction 前完成 artifact readback；DB commit 后独立读取 receipt 和 artifact，再复算关联 hash；
- DB 事务失败产生的未引用 content-addressed object 不代表成功，后续相同 hash 可幂等复用；不需要用 outbox 协调 broker 或消息副作用；
- 禁止删除任何被 spec/receipt 引用的对象。上游更正产生 generation+1 与新 artifact，旧 artifact 始终可重建旧回执。

### 5.11 稳定枚举与 reason registry

实现不得用自由文本参与状态判断。v1 注册以下 enum：

- derived spec state：`REGISTERED/COLLECTING/READY_TO_BUILD/QUALIFICATION_FAILED/INELIGIBLE/MATERIALIZING/COMPLETE/FAILED/STALE_SUPERSEDED_REQUIRED`；
- receipt status：`COMPLETED/FAILED`；assessment：`ELIGIBLE/INELIGIBLE`；
- day qualification：`INCLUDED/EXCLUDED/FAILED`；
- evidence validity：`CAPTURED/NOT_APPLICABLE/UNAVAILABLE/INVALID/PENDING_FINALIZATION`；
- latency quality：`CALLBACK_EXACT/SNAPSHOT_OBSERVED_UPPER_BOUND/UNAVAILABLE`。

v1 reason code 至少包含以下固定集合；新增 code 需要 schema/query version 变化和兼容性测试，未知 code 必须拒绝：

- spec/calendar：`SPEC_NOT_PREREGISTERED`、`SPEC_WINDOW_OVERLAP`、`EXPECTED_TRADE_DATE_MISMATCH`、`CALENDAR_SNAPSHOT_DRIFT`、`WINDOW_NOT_FINALIZED`、`SAMPLE_SUFFICIENCY_NOT_MET`；
- binding/control：`BINDING_NOT_FOUND`、`MULTIPLE_BINDINGS_MATCHED`、`BINDING_LINEAGE_MISMATCH`、`PACKAGE_MANIFEST_MISMATCH`、`CONTROL_IDENTITY_DRIFT`、`LEGACY_CONTROL_NOT_ELIGIBLE`；
- Phase 0A/1 chain：`PHASE0A_RECEIPT_MISSING`、`PHASE0A_RECEIPT_NOT_FINAL`、`PHASE0A_RECEIPT_HASH_MISMATCH`、`PHASE1_EXPORT_INCOMPLETE`、`PHASE1_EXPORT_HASH_MISMATCH`、`ACTIVE_ARCHIVE_COVERAGE_GAP`、`ACTION_QUOTE_LINK_MISSING`、`CHILD_TRADE_LINK_MISSING`；
- terminal/TCA/markout：`TERMINAL_UNATTRIBUTED`、`RECONCILED_FINAL_MISSING`、`TCA_BENCHMARK_MISSING`、`TCA_JOIN_INCOMPLETE`、`MARKOUT_PENDING`、`MARKOUT_UNAVAILABLE`、`MARKOUT_HASH_CONFLICT`、`LATE_FILL_AFTER_CANCEL_REQUEST`；
- latency/clock：`ACK_TIMESTAMP_UNAVAILABLE`、`CANCEL_REQUEST_TIMESTAMP_MISSING`、`CLOCK_DOMAIN_MISMATCH`、`NEGATIVE_LATENCY`、`TIMESTAMP_PRECISION_UNSUPPORTED`；
- liquidity/auction：`ADV_REFERENCE_INCOMPLETE`、`MINUTE_VOLUME_REFERENCE_MISSING`、`ZERO_MINUTE_VOLUME`、`FIVE_LEVEL_DEPTH_INCOMPLETE`、`ZERO_EXECUTABLE_DEPTH`、`AUCTION_CAPABILITY_UNAVAILABLE`；
- persistence/runtime：`SOURCE_WATERMARK_DRIFT`、`PHASE0B_IDENTITY_PAYLOAD_CONFLICT`、`MIGRATION_DRIFT_CONFLICT`、`DATA_FREEZE_ROOT_UNCONFIGURED`、`DATA_FREEZE_WRITE_FAILED`、`DATA_FREEZE_MISSING`、`DATA_FREEZE_HASH_MISMATCH`、`PHASE0B_WRITE_FAILED`、`PHASE0B_READBACK_MISMATCH`、`PHASE0B_MATERIALIZATION_RETRY_EXHAUSTED`、`LATE_AUTHORITATIVE_FACT_DETECTED`。

自由文本 message 只用于人读，不能改变 eligibility、retryability 或 HTTP status；这些均由 code registry 的版本化映射决定。

## 6. DB Schema 与迁移契约

### 6.1 表与所有权

迁移文件计划为：

- forward：`backend/migrations/miniqmt_phase0b_baseline_20260715.sql`
- rollback：`backend/migrations/miniqmt_phase0b_baseline_20260715_rollback.sql`

所有表位于 `qmt_strategy` schema，由 simulation runtime/TCA evidence 单写者拥有；API、metrics 与 operator 工具只读。表必须有 COMMENT、显式 CHECK、FK、唯一键、读取索引和 append-only trigger。

#### 6.1.1 `execution_b0_observation_spec`

| 列 | PostgreSQL 类型 | 约束 |
|---|---|---|
| `spec_id` | varchar(48) | PK，`^p0bspec_[0-9a-f]{32}$` |
| `schema_version` | varchar(64) | CHECK 固定 v1 |
| `series_key` | char(64) | NOT NULL |
| `environment` | varchar(16) | CHECK `SIM` |
| `broker_backend` | varchar(32) | CHECK `minqmt_sim` |
| `execution_policy` | varchar(32) | CHECK `B0_QUOTE_V2` |
| lineage/control 字段 | varchar/char(64) | 全部 NOT NULL，hash 为小写 hex |
| `expected_trade_dates` | date[] | NOT NULL，5 个严格递增唯一有效日 |
| `window_start_at_utc` / `window_finalize_at_utc` | timestamptz | start < finalize，start > created |
| `daily_boundaries` | jsonb | object，schema CHECK 由 repository + DB helper 双层验证 |
| `calendar_*` | varchar/char(64) | NOT NULL |
| `adv_window_trading_days` | smallint | CHECK = 20 |
| `markout_finalization_grace_seconds` / `reconcile_finalization_grace_seconds` | integer | NOT NULL，CHECK > 0 |
| `bucket_spec` / `coverage_thresholds` | jsonb | NOT NULL object |
| `sample_sufficiency_thresholds` | jsonb | NOT NULL object，所有 required key 为正整数 |
| `formula_versions` / `source_query_versions` | jsonb | NOT NULL object |
| `late_fact_policy` | varchar(32) | CHECK `SUPERSEDE_NEW_GENERATION` |
| `content_sha256` | char(64) | UNIQUE，NOT NULL |
| `created_at_utc` / `created_by_tool` | timestamptz/varchar | 审计 |

唯一键同时约束 `(series_key, window_start_at_utc, window_finalize_at_utc)`，防止同一 series 重叠注册。GiST/exclusion constraint 或等价事务 advisory lock 检查同一 series 时间范围不得相交；两者必须在 migration test 中验证并发安全。

#### 6.1.2 `execution_b0_observation_scope`

每个 spec/date 一行，注册时只保存预期日期与时间边界；materialization 时不更新旧行，而是把实际 membership 固化在 receipt day 表。该表是不可变 preregistration scope，不是运行状态表。

| 列 | 类型 | 约束 |
|---|---|---|
| `spec_id` | varchar(48) | FK spec，ON DELETE RESTRICT |
| `trade_date` | date | 与 spec date array 一致 |
| `observe_start_at_utc` / `finalize_after_at_utc` | timestamptz | start < finalize |
| `calendar_day_sha256` | char(64) | NOT NULL |
| `scope_sha256` | char(64) | UNIQUE，canonical row hash |

PK 为 `(spec_id, trade_date)`；spec 与 scope 在同一事务写入，并要求 scope 日期集合与 spec array 完全相等。

#### 6.1.3 `execution_b0_baseline_receipt`

| 列 | 类型 | 约束 |
|---|---|---|
| `baseline_receipt_id` | varchar(48) | PK，`^p0brct_[0-9a-f]{32}$` |
| `schema_version` | varchar(64) | 固定 receipt v1 |
| `spec_id` / `series_key` | varchar/char(64) | FK + NOT NULL |
| `generation` | integer | CHECK >= 1 |
| `supersedes_receipt_id` | varchar(48) | nullable self FK；generation>1 必填 |
| `status` | varchar(16) | CHECK `COMPLETED/FAILED` |
| `assessment` | varchar(16) | completed 时 `ELIGIBLE/INELIGIBLE`；failed 时 NULL |
| `failure_attempt_sha256` | char(64) | failed 必填；completed 必须 NULL |
| `failure_reason_code` / `failure_stage` / `failure_class` | varchar | failed 必填；completed 必须 NULL |
| `failure_context` | jsonb | failed 必填 bounded object；completed 必须 NULL |
| `source_snapshot_at_utc` | timestamptz | NOT NULL |
| `control_identity` | jsonb | 与 spec 严格相等 |
| `source_manifest` | jsonb | bounded IDs/hashes/watermarks |
| `data_freeze_schema_version` / `data_freeze_uri` | varchar/text | completed 必填；URI 仅允许 content-addressed scheme |
| `data_freeze_sha256` | char(64) | completed 必填，与 URI digest 一致 |
| `data_freeze_byte_count` / `data_freeze_record_count` | bigint | completed 必填，CHECK >= 0 |
| `data_freeze_section_counts` | jsonb | completed 必填，bounded registered keys |
| `counts` / `coverage` / `invariants` | jsonb | typed、schema versioned |
| `metric_summary` / `variance_summary` | jsonb | completed 必填，禁止 NaN/Infinity |
| `reason_histogram` | jsonb | bounded enum keys |
| `receipt_scope_hash` / `output_sha256` / `receipt_sha256` | char(64) | 始终 NOT NULL lower hex |
| `input_sha256` | char(64) | completed 必填；failed 可空 |
| version 字段 | varchar/jsonb | query/formula/code/schema versions |
| `materialized_at_utc` | timestamptz | 审计时间，不进入 content hash；readback 结果不 UPDATE 本行 |

唯一键包含 `(spec_id, generation)` 与 `(baseline_receipt_id, status)`；`supersedes_receipt_id IS NOT NULL` 的唯一 successor index 禁止 generation 分叉。另建 completed-only 唯一索引 `(spec_id, receipt_scope_hash, query/formula/code/schema versions, input_sha256) WHERE status='COMPLETED'`；FAILED 不占用 completed input identity，允许后续 generation 对同一 source input 恢复成功。禁止在写事务内将旧行打标 superseded；logical head 由 `NOT EXISTS(new.supersedes_receipt_id=old.id)` 计算，不能使用依赖子查询的伪 partial unique。

状态 CHECK 还必须保证：completed 时 assessment、input、完整 summary 必填且全部 failure 字段为 NULL；failed 时 assessment 为 NULL、全部 failure 字段必填、day/bucket 子行数为零。`receipt_sha256` 对除 `materialized_at_utc` 外的整行 canonical content 计算。

completed 状态 CHECK 同时要求全部 data-freeze 字段非空、URI digest 与 `data_freeze_sha256` 一致；failed receipt 若 artifact 已成功生成则填写对应字段，若 artifact store 本身是 failure stage 则这些字段必须统一为 NULL，不能部分填写。

#### 6.1.4 `execution_b0_baseline_day`

| 列 | 类型 | 约束 |
|---|---|---|
| `baseline_receipt_id` / `trade_date` | varchar/date | 复合 PK |
| `receipt_status` | varchar(16) | 固定 `COMPLETED`，与 receipt 构成复合 FK |
| `binding_id` / lineage hash | varchar/char(64) | completed receipt 必填 |
| `qualification` | varchar(24) | `INCLUDED/EXCLUDED/FAILED` |
| `reason_codes` | text[] | 非 INCLUDED 必须非空，稳定排序 |
| `phase0a_receipt_id` / `phase0a_output_sha256` | varchar/char(64) | INCLUDED 必填 |
| `phase1_manifest_sha256` / `phase1_records_sha256` | char(64) | INCLUDED 必填 |
| `source_watermarks` / `source_counts` | jsonb | NOT NULL |
| `coverage` / `invariants` / `metric_summary` | jsonb | NOT NULL |
| `day_input_sha256` / `day_output_sha256` | char(64) | NOT NULL |

每个 COMPLETED receipt 必须恰有 spec 中的 5 个 expected dates，不得只写成功日；FAILED receipt 不写 day/bucket 子行。`EXCLUDED/FAILED` 日使 completed receipt 的 assessment 为 `INELIGIBLE`，但保留完整原因。

#### 6.1.5 `execution_b0_baseline_bucket`

| 列 | 类型 | 约束 |
|---|---|---|
| `baseline_receipt_id` | varchar(48) | FK receipt |
| `receipt_status` | varchar(16) | 固定 `COMPLETED`，与 receipt 构成复合 FK |
| `scope_kind` / `scope_value` | varchar | `WINDOW/DAY` 与 bounded value |
| `view_kind` | varchar(64) | 仅预注册固定视图枚举 |
| `dimension_key` / `bucket_ordinal` | varchar/smallint | 对应 spec bucket |
| `bucket_lower` / `bucket_upper` | numeric | 可空边界，禁止 float |
| `lower_inclusive` / `upper_inclusive` | boolean | NOT NULL |
| `metric_name` | varchar(64) | bounded enum |
| `count` / `null_count` / `invalid_count` | bigint | CHECK >=0 |
| `sum_value` / `mean_value` / quantiles | numeric | 与公式 scale 一致 |
| `row_sha256` | char(64) | UNIQUE |

PK 使用 receipt + scope + view + dimension + ordinal + metric。数据库 CHECK 和 repository 都必须拒绝不在 spec 中的 bucket ordinal、动态 dimension 或超过预计算上限的行数。

### 6.2 Append-only 与保留

- 五张表禁止 UPDATE/DELETE；append-only trigger 对普通 owner 和应用角色都生效。
- spec、receipt、day、bucket 与被引用 data-freeze object 属于交易审计证据，保留期与 Phase 0A/1 TCA evidence 相同且不得短于 7 年；不能用常规 TTL 清理。
- source retention manifest 必须记录 Phase 0A/1 active/archive 和 market raw 的策略；即使上游较早清理或更正，旧 receipt 仍从其不可变 artifact 重建，禁止回退到“当前最新上游值”。
- API/export 默认只读 head，但必须支持按 receipt ID 读取历史 generation。
- metrics 不使用 spec/binding/order/symbol/package ID 作为 label；高 cardinality 只进入数据库和 bounded diagnostic samples。

### 6.3 Forward migration preflight

DDL 开始前必须在同一连接显式检查并输出：

1. 当前 schema/version、目标表/constraint/index/trigger 是否存在及定义 hash。
2. Phase 0A/1 被引用表、列、类型、CHECK 与 required indexes 是否符合设计版本。
3. 所需 extension/operator class 是否可用；禁止静默创建未批准 extension。
4. 应用角色的 read/write privilege 与所有者是否满足 single-writer；不增加 RBAC 产品功能。
5. 同名对象若定义不同立即 `MIGRATION_DRIFT_CONFLICT` 并回滚。

forward migration 必须事务化、可重复执行：第一次创建全部对象；第二次为严格 verified noop。`IF NOT EXISTS` 后仍必须 readback 对象定义，不能把名称存在当成功。

### 6.4 Rollback

rollback 只能在应用代码尚未启用或已明确停用 Phase 0B 写者时运行，并执行：

- 若任何 spec/receipt/day/bucket 行存在，立即 `ROLLBACK_EVIDENCE_ROWS_PRESENT`，禁止删除审计证据。
- 无数据时按依赖逆序删除 trigger/index/table，并 readback 确认。
- rollback 第一次成功后，第二次必须 verified noop。
- 生产 rollback 不隐含数据库导出；AIstock 已有日常备份，导出必须由用户另行明确授权。

## 7. Repository、事务与失败语义

### 7.1 组件边界

计划新增：

- `Phase0BObservationSpecRepository`：规范注册、identity collision 检查、readback。
- `Phase0BSourceProjection`：从 Phase 0A/1 与 market tables 建立 typed、只读投影。
- `Phase0BBaselineAggregator`：纯函数资格判定、bucket、distribution、variance 和 hash。
- `Phase0BEvidenceArtifactStore`：exact source/projection canonical JSONL 的 content-addressed 原子写入与 readback。
- `Phase0BBaselineRepository`：append-only receipt 原子写入、CAS、readback。
- `Phase0BBaselineReadService`：只读 head/history/diagnostics/export。

运行时 callback、selection、event loop、scheduler 和 broker adapter 不依赖这些组件；Phase 0B 构建失败不得阻断或改变 B0 行为，但必须显式失败并报警。这里的“不阻断 B0”不是吞错：错误在独立观察任务的状态、API、metrics 和日志中保持失败。

### 7.2 source snapshot

1. 使用一条 TCA-owned PostgreSQL `REPEATABLE READ, READ ONLY` 事务。
2. 先固定 spec、scope、binding memberships 与各 source watermark，再执行 bounded queries。
3. Phase 1 调用现有 v2 read seam，必须覆盖 active + archived；Phase 0A 按 receipt IDs/read model 读取。
4. daily/minute volume 使用 exact date/symbol bounded query，不做全表 JSONB 扫描。
5. 生成 source manifest 并在关闭 snapshot 前复算计数/hash。
6. snapshot 内出现 source schema/version/identity drift 时立即 typed fail。

### 7.3 materialization transaction 与 single writer

聚合 draft 在只读 source snapshot 完成后构建。写入使用独立事务：

1. `pg_advisory_xact_lock(hash(spec_id))` 串行化同 spec materialization。
2. read current logical head、相同 completed `input_sha256` 及其完整 version tuple。
3. 相同 completed input/output 已存在则幂等返回既有 receipt；相同 completed input/version 不同 output 为 deterministic conflict。FAILED attempt 不冒充 completed，也不阻塞后续成功 generation。
4. 新输入要求 `generation=head+1` 且 `supersedes_receipt_id=head.id`；无 head 则 generation=1。
5. 在 DB 写事务前，将 exact input/projection 写入 data-freeze CAS，完成 bytes/hash/URI 独立 readback；artifact 失败则不开始 receipt transaction。
6. completed 按 receipt -> 5 day rows -> bounded bucket rows 原子插入；可持久化的技术/资格构建失败只写 FAILED receipt envelope，不写子行。任何写失败全事务回滚。
7. commit 后通过新连接按 ID 读取，并重新读取 artifact，复算行数、FK、input/output/receipt/data-freeze hash；readback 失败将本次操作报告为失败，禁止返回成功，也禁止 UPDATE receipt 伪造 readback 标志。后续相同命令可读取既有行重新完成独立验证。

不存在通用 outbox 或异步第二业务写者：Phase 0B 不产生 broker side effect，也不向事件总线复制成功事实。CAS 先写后引用，未引用 object 不是成功且可按 content hash 幂等复用，因此不需要 dual-write outbox；metrics 从持久化 receipt 读取，日志不是权威。

### 7.4 重试与去重

- 只允许对显式登记的瞬时 PostgreSQL SQLSTATE（序列化失败、deadlock、连接瞬断）进行 bounded exponential backoff + jitter。
- 每次重试重新建立 source snapshot并重新验证 watermark；不得复用可能过期 draft。
- schema drift、identity/hash conflict、coverage、domain validation、clock、缺源和 readback mismatch 永不按瞬时错误自动重试为成功；它们形成 FAILED 或 completed-ineligible typed outcome。
- 达到最大次数后持久/输出 typed `PHASE0B_MATERIALIZATION_RETRY_EXHAUSTED`，不写内存替代回执。
- 去重依据 canonical identity/input/output hash，不依据异常文本、日志时间或进程内 cache。
- 若数据库故障使 FAILED receipt 本身也无法提交，CLI/task 必须非零退出并发出 operational alert，明确报告 `durable_failure_receipt_persisted=false`；数据库恢复前不得声称已有 durable failure 或 success。

### 7.5 迟到事实与重启恢复

- 60/300/900 markout 未到期时 spec 仍 `COLLECTING`，禁止提前 final receipt。
- 超过 finalization 后出现迟到成交、late cancel outcome、archive 迁移、TCA reconcile 或 market data correction，只读 head 校验会检测 source watermark/input hash 漂移并显示 `STALE_SUPERSEDED_REQUIRED`。
- builder 生成 generation+1，完整重算全部日和 bucket，并 supersede 旧 receipt；禁止局部 UPDATE。
- 进程重启从数据库 spec/scope/source facts 恢复，不从内存 timer 恢复。重复调度由 advisory lock + hash 幂等去重。
- 跨交易日未完成 parent 必须在最终 TCA 中明确 carry/terminal attribution；不能把次日 fill 静默切到新日或丢弃。

## 8. Read-only API、CLI、metrics 与 runbook

### 8.1 API

计划在 simulation runtime router 下增加只读接口：

- `GET /api/v1/simulation-runtime/miniqmt/phase0b/specs/{spec_id}`
- `GET /api/v1/simulation-runtime/miniqmt/phase0b/specs/{spec_id}/status`
- `GET /api/v1/simulation-runtime/miniqmt/phase0b/receipts/{receipt_id}`
- `GET /api/v1/simulation-runtime/miniqmt/phase0b/series/{series_key}/head`
- `GET /api/v1/simulation-runtime/miniqmt/phase0b/receipts/{receipt_id}/diagnostics`
- `GET /api/v1/simulation-runtime/miniqmt/phase0b/receipts/{receipt_id}/export`

接口没有 POST/PUT/PATCH/DELETE。export 从 receipt 固定的 CAS URI 读取并验证 byte count/content hash，不能重新查询“当前最新”上游代替旧 artifact。未找到、未完成、stale、ineligible、readback mismatch 分别使用稳定 error code；不能返回 HTTP 200 加空对象。diagnostics 默认 bounded samples，分页 cursor 绑定 receipt hash，防止跨 generation 混读。

成功响应统一为：

```json
{
  "schema_version": "phase0b_read_response_v1",
  "request_id": "...",
  "as_of_utc": "...Z",
  "spec_id": "p0bspec_...",
  "receipt_id": "p0brct_... or null",
  "state": "REGISTERED|COLLECTING|READY_TO_BUILD|QUALIFICATION_FAILED|INELIGIBLE|MATERIALIZING|COMPLETE|FAILED|STALE_SUPERSEDED_REQUIRED",
  "receipt_status": "COMPLETED|FAILED|null",
  "assessment": "ELIGIBLE|INELIGIBLE|null",
  "readback_verified": true,
  "content_sha256": "...",
  "data": {},
  "diagnostics": {"reason_counts": {}, "sample_count": 0, "next_cursor": null}
}
```

未知 spec/receipt 为 404；请求/游标/schema 无效为 422；identity/head conflict 为 409；source/DB 暂不可读为 503。已存在的 `COMPLETED + INELIGIBLE` receipt 使用 200 并明确 assessment/reasons，因为这是有效业务回执；`readback_verified` 必须由本次读取复算，不能写死为 true。错误 envelope 固定为 `detail={error_code,message,retryable,stage,context}`，context 只允许 bounded 非敏感 key。

### 8.2 Operator CLI

计划新增：

- `scripts/register_miniqmt_phase0b_observation.py`：默认 dry-run，`--execute` 只做 spec/scope evidence DML。
- `scripts/build_miniqmt_phase0b_baseline.py`：默认 plan/read-only，`--execute` materialize receipt。
- `scripts/export_miniqmt_phase0b_baseline.py`：永久只读，输出 canonical manifest/records。

CLI 必须先输出目标 DB identity、spec/series/scope hash、控制 tuple、预计行数和 source versions；`--execute` 后独立 readback。生产 DDL/DML 仍需用户在届时明确授权，但工具中不得增加审批流、人工 acknowledge 或 RBAC gate。

### 8.3 Metrics 与 alerts

建议指标只允许 bounded labels：`environment`、`policy`、`state`、`result`、`reason_code`、`metric_kind`、`horizon`、`quality`。

- `aistock_miniqmt_phase0b_spec_state_total`
- `aistock_miniqmt_phase0b_daily_qualification_total`
- `aistock_miniqmt_phase0b_receipt_build_total`
- `aistock_miniqmt_phase0b_source_coverage_ratio`
- `aistock_miniqmt_phase0b_unattributed_terminal_total`
- `aistock_miniqmt_phase0b_late_fact_stale_total`
- `aistock_miniqmt_phase0b_identity_conflict_total`
- `aistock_miniqmt_phase0b_readback_failure_total`
- `aistock_miniqmt_phase0b_data_freeze_failure_total`

alerts 至少覆盖：expected day finalization 后仍未 ready、source coverage 下降、unattributed residual > 0、identity/hash conflict、data-freeze missing/corrupt、materialization/readback failure、head stale、连续调度 retry exhausted。恢复条件由下一次成功事实自动清除；不要求人工 acknowledge。

### 8.4 Diagnostics 与 operator runbook

新增 `docs/operations/miniqmt_phase0b_baseline_runbook.md`，必须包含：

1. 设计/代码/DDL/spec 注册/窗口运行/receipt eligibility 六种状态的分离判断。
2. spec dry-run、execute、独立 readback 和“窗口开始前已注册”证明。
3. 每日 binding lineage、Phase 0A receipt、Phase 1 export 与 active/archive coverage 检查。
4. latency quality、ADV/minute/depth reference、auction capability 与 forced sweep 分类检查。
5. late fact/stale detection、generation+1 rebuild 和历史 receipt 导出。
6. data-freeze CAS root/readback、URI/hash/byte/record count、未引用 object 与 referenced object 保留检查。
7. typed reason 到 operator action 的映射；禁止删除证据、改门槛或手工把 ineligible 改成 eligible。
8. broker side-effect parity 验证与 B0 action/hash 对照。
9. rollback refusal、生产 readback、metrics/alerts 查询及证据保留。

## 9. Risks / Failure Modes

| Failure mode | 必须行为 | 禁止行为 |
|---|---|---|
| 未在窗口前注册 spec | 该运行只作 qualification，正式 baseline 拒绝 | 事后补 spec 冒充预注册 |
| binding/control/hash 漂移 | 日失败或新 series/spec | 自动选最新 binding、跨 revision 拼接 |
| Phase 0A/1 缺链 | `INELIGIBLE` + typed reasons | 用 quote_control_complete 或日志冒充完整 |
| ADV/minute/depth 分母缺失 | ratio NULL + coverage 失败 | ffill、缩窗、替代分母、零值 |
| callback 时钟不可信 | quality invalid/unavailable | 绝对值、截零、混入 exact 分布 |
| auction capability 缺失 | OBSERVE_ONLY typed unavailable | 从普通 quote 合成 auction 字段 |
| 迟到成交/markout/reconcile | head stale，generation+1 全量重建 | UPDATE 旧 receipt 或静默忽略 |
| 数据库/transaction/readback 失败 | 失败、报警、无成功回执 | 内存、文件或日志 fallback 成功 |
| data-freeze root 缺失/写入或 hash 失败 | build FAILED；DB 不引用坏对象 | 当前目录 fallback、只存 hash 冒充 artifact |
| 重复 builder | advisory lock + content hash 幂等 | 两个 writer 各自产生 head |
| 指标 cardinality 膨胀 | bounded enum labels、DB diagnostics | order/symbol/spec/binding 作为 label |
| Phase 0B 构建异常 | 观察任务失败但 B0 路径不被修改 | 阻断/改变既有 broker 业务或吞错 |
| rollback 已有证据 | fail loud，保留表和数据 | 删除审计证据 |

## 10. Implementation Plan

### Slice A：contract、schema 与 spec preregistration

- 实现 typed domain contracts、canonical JSON/hash、spec validation。
- 实现 5 表 forward/rollback migration、append-only trigger、strict preflight/readback。
- 实现 spec repository、dedicated data-freeze CAS、CLI dry-run/execute 与 concurrency/idempotency。
- 完成 schema/property/migration/DEV DB tests 后才进入 Slice B。

### Slice B：source projection 与 qualification

- 复用 Phase 1 v2 export 和 Phase 0A receipt read model。
- 为新 runtime events 增加 §5.5.4 typed latency anchor payload，并由 Phase 1 v2 export 完整读取；不增加 event type/source 或 broker call。
- 增加 typed latency projection、PIT ADV20、EOD minute volume、opposite L5 depth、auction capability、forced-sweep projection。
- 明确 active/archive、late fact、cross-day 和 clock quality。
- 缺 authority 必须产生 typed failure，不实现 fallback。

### Slice C：aggregator 与 immutable receipt

- 实现 coverage/invariant、distribution/bucket/variance 和 eligibility。
- 实现 source snapshot、CAS writer、retry allowlist、generation/supersedes、post-commit readback。
- 验证同输入确定性、乱序输入、重复调度和迟到事实全量重建。

### Slice D：read surface 与操作证据

- 实现只读 API/CLI/export、bounded diagnostics、metrics/alerts、runbook。
- 证明 HTTP/CLI 没有 mutation/broker path，错误不返回假成功。

### Slice E：production gates（需届时单独授权）

- 执行生产 DDL preflight/forward/readback；不得自动导出数据库。
- 注册首个正式 spec 并 readback；不改变 B0 config/action。
- 完成 5 日观察、post-close finalization、baseline build、独立重建和 receipt hash 对比。

任何 slice 的局部合入不得被汇报为 Phase 0B 完成；只有本文 acceptance matrix 的实现证据、生产 gates 和真实 SIM receipt 全部满足才完成 Phase 0B。

### 10.1 与既有基线及后续阶段的边界

- BUG-599/600/604/614 是已合入的 B0 可靠性基线；Phase 0B 观察其结果，不重写 watchdog、逐行写、pending tick-driver、protected marketable limit 或 tail reprice 语义。
- `LEGACY_B0` 保持独立 control identity，不能作为本文 eligible baseline，也不能在 `B0_QUOTE_V2` 失败时自动 fallback。
- P1-E/production activation wiring 提供 binding/revision/assignment、durable action/child receipt 和 Phase 0B v2 export；本文只消费这些权威 seam，不新增第二执行入口。
- Phase 0B eligible receipt 是 Phase 2/3 开始代码/harness/dry-run 的数据前置；它不授权 B1 broker submit、policy DML、canary 或 promotion。
- Phase 2 若发现输入 schema 或公式必须变化，应先形成新的 Phase 0B spec/series 并重建 baseline；不得原地改写已冻结回执。

## 11. Verification Plan

### 11.1 直接 contract 与 schema tests

计划测试文件与关键 nodeids：

- `tests/backend/services/simulation_runtime/test_phase0b_contracts.py`
  - `test_spec_identity_is_order_independent_and_audit_fields_are_excluded`
  - `test_spec_requires_five_pre_registered_trading_dates_and_exact_boundaries`
  - `test_spec_rejects_legacy_live_overlapping_window_and_control_hash_drift`
  - `test_bucket_edges_units_and_cardinality_are_explicit_and_bounded`
  - `test_receipt_completed_ineligible_is_not_failed_or_eligible`
- `tests/backend/services/simulation_runtime/test_phase0b_projection.py`
  - `test_projection_joins_action_child_trade_markout_and_tca_by_exact_ids`
  - `test_active_and_archived_evidence_share_one_bounded_snapshot`
  - `test_missing_link_clock_or_source_fails_loud_without_nearest_quote_fallback`
  - `test_callback_exact_and_snapshot_upper_bound_latencies_never_merge`
  - `test_latency_anchor_v1_hash_identity_and_exact_event_source_mapping`
  - `test_legacy_event_without_anchor_is_upper_bound_or_unavailable_not_exact`
  - `test_batch_cancel_has_per_child_request_and_return_anchors`
  - `test_crash_between_gateway_call_and_event_does_not_repeat_broker_call_for_latency`
  - `test_cancel_late_fill_remains_fill_and_is_not_cancel_success`
  - `test_auction_unavailable_is_not_synthesized_from_normal_quote`
  - `test_forced_sweep_uses_durable_stage_and_metadata_not_time_heuristic`
- `tests/backend/services/simulation_runtime/test_phase0b_liquidity.py`
  - `test_adv20_is_pit_raw_shares_and_excludes_current_or_future_day`
  - `test_missing_adv_day_does_not_fill_or_shrink_window`
  - `test_minute_volume_reuses_hand_to_share_and_exact_arrival_minute_contract`
  - `test_zero_or_missing_denominator_is_null_with_typed_coverage`
  - `test_buy_uses_ask_depth_and_sell_uses_bid_depth`
- `tests/backend/services/simulation_runtime/test_phase0b_aggregator.py`
  - `test_permutation_and_duplicate_input_produce_identical_receipt_hash`
  - `test_all_expected_days_are_preserved_when_one_day_is_ineligible`
  - `test_unattributed_terminal_or_incomplete_markout_prevents_eligibility`
  - `test_bucket_rows_match_pre_registered_edges_and_bounded_views`
  - `test_daily_variance_requires_all_five_complete_days`

### 11.2 Repository、migration 与 rollback tests

- `tests/backend/services/simulation_runtime/test_phase0b_repository.py`
  - 同 spec 并发注册只产生一个 identity；不同 payload 冲突。
  - 相同 input/output 幂等返回；相同 input/different output fail loud。
  - receipt/day/bucket 单事务原子性，任一 insert 失败无部分行。
  - generation/supersedes/head 计算、迟到事实、重启重复调度和 advisory lock。
  - post-commit readback/hash mismatch 不返回成功。
  - CAS atomic write/fsync/rename、existing-object byte verify、missing root/corrupt object、DB failure orphan reuse。
  - 只重试 allowlisted SQLSTATE，domain/schema/hash error 不重试。
- `tests/backend/services/simulation_runtime/test_phase0b_artifact_store.py`
  - `test_artifact_store_requires_explicit_dedicated_root_without_cwd_fallback`
  - `test_artifact_write_fsync_rename_and_existing_object_byte_readback_are_idempotent`
  - `test_corrupt_or_missing_object_fails_loud_and_never_returns_verified_export`
  - `test_old_generation_rebuilds_from_frozen_jsonl_after_upstream_correction`
  - `test_unreferenced_object_after_db_failure_is_not_reported_as_receipt_success`
- `tests/backend/migrations/test_miniqmt_phase0b_baseline_migration.py`
  - forward fresh、forward twice verified noop、对象定义 drift fail。
  - CHECK/FK/unique/exclusion/append-only trigger 的正反例。
  - rollback fresh、rollback twice noop、存在任一 evidence row 时拒绝。
  - migration 失败事务回滚，不留半对象。
- disposable DEV PostgreSQL E2E：真实执行 migration，写入 5 日 spec/source fixtures，经 repository materialize/readback/export，再执行 late fact generation+1。测试可以用受控数据建源事实，但不得用 mock repository 代替数据库验收。
- disposable artifact-root + DEV DB E2E 必须证明 receipt 引用的 JSONL 可在上游数据被更正后仍独立重建旧 generation，且 generation+1 使用新 artifact、不覆盖旧对象。

### 11.3 API、metrics 与无副作用 tests

- 只读 API 的 head/history/diagnostics/export schema、404/409/422/503 typed errors、pagination hash isolation。
- 对 router/service 做 AST/import + spy boundary：没有 broker gateway、submit/cancel、selection/package-validation 或 mutation endpoint。
- metrics label allowlist、reason registry、无 ID labels；缺字段不输出假零。
- alerts 触发与自动恢复；不存在 ack/RBAC/approval state。
- CLI 默认 dry-run；`--execute` 只写 spec 或 receipt 表；执行前后 broker call count 均为零。

### 11.4 覆盖率与回归

- 新增/修改的 Phase 0B 业务代码 statement coverage >= 80%，branch coverage >= 70%；contract、hash、eligibility、transaction、failure precedence 必须有直接反例。
- 定向回归覆盖 Phase 0A receipt、Phase 1 v2 export、P1-E binding/restart/parity、BUG-599/600/604/614 和既有 MiniQMT event-loop。
- changed-file `ruff`、compile、`git diff --check`、ownership、L0、module registry、F2 validator 必须通过。
- 扩大回归若有失败，必须在未修改的同 commit `origin/main` 对照复现后才能标为基线问题；不能修改任务外业务逻辑制造绿色。

### 11.5 真实 SIM 与结果验收

mock/fixture 只能验证合同，不可替代以下真实证据：

1. 首个 spec 在窗口开始前的生产持久化/readback hash。
2. 5 个连续有效交易日的真实 `B0_QUOTE_V2` MiniQMT SIM membership。
3. 每日 Phase 0A receipt、Phase 1 v2 export、active/archive、水位和 finalization readback。
4. 首次 baseline receipt 与独立进程、同查询重建的 `input/output/receipt` hash 完全一致。
5. broker side-effect parity：观察层启用前后 B0 action identity/count/quantity/price 由相同源事实重建，Phase 0B 自身产生 broker calls=0。
6. terminal unattributed=0，关键 coverage 达到 spec，receipt=`COMPLETED + ELIGIBLE`。

若真实窗口 coverage 不足，只能修复观察链并注册/重建合规窗口；不得降低事后门槛、填默认值或推进 Phase 2/3。

## 12. Rollout / Rollback

### 12.1 Rollout 顺序

1. 合入完整 Slice A-D 代码与测试；代码默认无调度写入和无 broker 依赖。
2. 用户另行授权后执行生产 DDL preflight/forward/readback；不执行数据库导出。
3. 配置并只读验证专用 Phase 0B evidence CAS root；不复用策略包 namespace，不允许 current-directory fallback。
4. 只读 dry-run 验证 lineage、calendar、hash、source query 和预计 window。
5. 用户另行授权 spec DML 后，在第一日窗口前 `--execute` 注册 spec/scope 并独立 readback。
6. 既有 B0 unattended SIM 正常运行；Phase 0B 只观察，不改变配置或 broker action。
7. 每日 post-close qualification；第五日 finalization 后 materialize baseline。
8. 独立 export/rebuild 对比 data-freeze/receipt hash，验证 metrics/alerts/runbook 和 broker parity。
9. 只有 Production Gates 全部满足，Phase 0B 才可记录完成。

### 12.2 应用回滚

- 停止/禁用 Phase 0B spec registration 和 builder 调度即可；B0 runtime 不依赖 Phase 0B，因此 broker 行为不变。
- 保留所有既有 spec/receipt；read API 仍可读历史 evidence。
- 不更新 assessment、不删除 bucket、不把 B0 切回 LEGACY。
- 若代码回滚后 schema 仍存在，属于安全兼容状态；DDL rollback 仅按 §6.4 且无 evidence 行时允许。

### 12.3 数据更正

数据更正不走“回滚旧回执”：源事实修正后由 generation+1 完整 supersede。若更正改变 control identity、calendar scope、formula 或 bucket，必须新建 spec/series，不能生成同 series 新 generation 混淆语义。

## 13. Production Gates

以下 gate 是事实校验，不是新增审批系统；本设计 PR 完成时它们仍分别为 `pending_not_executed`：

| Gate | 通过证据 | 失败行为 |
|---|---|---|
| `G0_SOURCE_MERGED` | 实现 PR 合入且 main/origin main 对齐 | 不执行生产步骤 |
| `G1_SCHEMA` | DDL preflight、forward、second-run noop、catalog/constraint/trigger readback | fail loud，不注册 spec |
| `G2_RUNTIME_COMPAT` | 新代码进程版本、query/schema version 与 Phase 0A/1 readback 一致 | 不注册 spec，不 fallback |
| `G3_ARTIFACT_STORE` | 专用 CAS root、atomic write、content readback、retention 与 no-fallback 验证 | 不 materialize receipt |
| `G4_SPEC_PREREGISTERED` | 窗口前 spec/scope DML 与独立 hash readback | 该窗口不可作为 baseline |
| `G5_DAILY_QUALIFICATION` | 5 日 exact memberships、final receipts、structural/terminal/market coverage | receipt ineligible，不替日 |
| `G6_DATA_FREEZE` | exact JSONL artifact、source watermark/input hash、memberships、bucket/formula versions 固化 | 不生成 eligible head |
| `G7_REBUILD` | 独立进程从 frozen artifact 重建 output/receipt hash 一致 | deterministic conflict，报警 |
| `G8_NO_SIDE_EFFECT` | Phase 0B broker call=0 且 B0 action parity | 停止 Phase 0B builder，调查漂移 |
| `G9_BASELINE_EXIT` | `COMPLETED + ELIGIBLE`、unattributed=0、全部预注册 coverage 满足 | Phase 2/3 不开始 |

真实 SIM 运行、服务重启、生产 DDL、spec DML 和后续 Phase 2/3 均需要届时按用户指令分别执行；本设计不把这些状态合并为“已完成”。

## 14. Design Acceptance Index

本节不创建新的 F 编号，严格复用算法域下位蓝图 §15 的稳定 `F-001`～`F-022` 语义；平台执行路径、durability、错误语义和进度状态始终服从唯一上位蓝图。

### 14.1 本阶段承接的算法域下位蓝图条款

- `F-001`：以 signed decision/arrival IS、尾部风险和 terminal residual 为权威目标；Phase 0B 负责冻结 B0 聚合与 residual=0 证据。
- `F-003`：使用 Phase 1 强类型五档 quote、exchange/receive time、quote age 与 duplicate contract，不创建第二 schema。
- `F-004`：只把 broker trade callback/reconcile 的 durable fill 事实计入 completion/TCA，不由观察层增加 filled quantity。
- `F-008`：只承接 auction capability/OBSERVE_ONLY 与收盘阶段观察，不实现或改变 Completion Governor。
- `F-009`：保持停牌、临停、涨跌停、zero depth 与数据/配置错误的不同语义并分别聚合。
- `F-010`：对 Phase 0B 自身 spec/receipt 实现 CAS、幂等、single writer、重启恢复和 superseding；不重写 action/order/trade 状态机。
- `F-011`：冻结 schema/config/policy/adapter/code 与 B0 control revision hash，禁止原地改写或静默算法切换。
- `F-012`：形成 benchmark、delay、markout、completion、residual 与 baseline 的权威 Phase 0B 聚合。
- `F-013`：只提供后续 champion/challenger 所需的预注册 B0 baseline 与隔离变量，不开展 challenger 实验。
- `F-016`：所有 source、coverage、terminal、clock、DB 和 readback failure 使用 loud reason/stage/context。
- `F-017`：保持 SIM-only 分阶段发布和显式数据/应用回滚边界；Phase 0B 不实现 LIVE 或新的审批功能。
- `F-018`：完成 Phase 0B 独立交付、退出条件、设计路径和验收边界，不把本阶段冒充整套算法完成。
- `F-019`：消费 Phase 1 watermark、背压结果、事件优先级、单位和 clock/calendar failure evidence。
- `F-020`：实现 Phase 0B logical persistence、service operations、migration、rollback 与 backward compatibility。

### 14.2 本阶段明确不拥有的算法域下位蓝图条款

- `F-002`：多 alpha broker 前唯一 parent、lineage、现金和 T+1 约束仍由 selection/runtime 既有路径与 Phase 3 拥有；Phase 0B 只读其冻结结果。
- `F-005`、`F-006`、`F-007`：动作空间、depth/haircut child sizing 与不可绕过 hard constraints 属于 Phase 2/3；Phase 0B 不生成动作或重算约束。
- `F-014`：模型权限和 Completion Governor 优先级属于 Phase 5；本阶段没有模型。
- `F-015`：新 `AdaptiveExecutionCore` 与 adapter/capability/asset 边界属于 Phase 2；本阶段不创建 core 或 broker bridge。
- `F-021`：V25 proposal adapter 属于有条件的 Phase 4；本阶段不读取 V25 artifact 或解除 broker bridge 拒绝。
- `F-022`：kill switch、风险收敛和恢复属于 Phase 2/3；本阶段只报告观察任务故障，不能增设或改动执行 kill switch。

## 15. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §5.8、§11.5、§17；计划 `Phase0BBaselineAggregator` | signed IS/tail/terminal residual 聚合、unattributed=0、真实 receipt | design_ready | none |
| F-002 | §2.2、§3、§10.1、§14.2；只读既有 parent/lineage | selection/runtime 既有回归；证明 Phase 0B 不重算现金/T+1/parent | boundary_ready | none |
| F-003 | §1.1、§5.4、§7.2；复用 `MarketDataEvidenceV1`/v2 export | exact quote ID、five-level/age/duplicate、active+archive tests | design_ready | none |
| F-004 | §5.4、§5.8；只读 durable broker fill/TCA facts | callback-only fill、reconcile、late-fill、quantity conservation regressions | design_ready | none |
| F-005 | §3、§10.1、§14.2；无 action producer | AST/broker spy 证明 WAIT/L1-L5/IOC/cancel space 未被修改 | boundary_ready | none |
| F-006 | §3、§5.6、§14.2；流动性只作 observation ratio | 证明 Phase 0B 不生成 child sizing、不改变 depth/haircut/ticks | boundary_ready | none |
| F-007 | §0、§3、§10.1、§14.2；既有 B0 hard guards unchanged | BUG-614/limit/funds/lot regressions与 no broker side effect | boundary_ready | none |
| F-008 | §5.6.4、§5.8；复用 auction capability/tail metadata | synthesis rejection、OBSERVE_ONLY、session 与 forced-sweep classifier | design_ready | none |
| F-009 | §5.3、§5.6-8；计划 typed reason projection | suspend/halt/limit/zero-depth/data/config 分离聚合 tests | design_ready | none |
| F-010 | §6.1、§7.1-5；计划 spec/baseline repositories | advisory CAS、atomicity、idempotency、restart/late-fact/reconcile tests | design_ready | none |
| F-011 | §5.1-2、§5.10；control/hash/version receipt | identity drift、cross-revision rejection、no LEGACY fallback tests | design_ready | none |
| F-012 | §5.5-10、§11；计划 TCA baseline projection/aggregator | benchmark/delay/markout/completion/residual/distribution tests | design_ready | none |
| F-013 | §4.3、§5.1、§5.7、§10.1；预注册 B0 inputs only | pre-window spec、bucket freeze、series isolation；无 challenger 路径 | design_ready | none |
| F-014 | §3、§10.1、§14.2；无模型/Completion Governor 代码 | import/route boundary 证明 model 无权且本阶段未实现 | boundary_ready | none |
| F-015 | §3、§10.1、§14.2；无 AdaptiveExecutionCore/bridge | existing core/adapter/bridge regressions与 unreachable new broker path | boundary_ready | none |
| F-016 | §7.3-5、§8、§9；typed failure registry | source/coverage/clock/DB/readback/terminal failure direct tests | design_ready | none |
| F-017 | §0、§3、§12-13；SIM-only rollout/rollback | environment CHECK、no LIVE path、no new approval/RBAC/ack review | design_ready | none |
| F-018 | §10-13、§17；独立 Phase 0B lifecycle | F2 validator、G4-G9、完整退出矩阵、5-day real SIM evidence | design_ready | none |
| F-019 | §4.3、§5.5、§5.9、§7.2；复用 Phase 1 ingress evidence | watermark/gap/backpressure/unit/clock-calendar failure tests | design_ready | none |
| F-020 | §6-8、§12；5 表、service/CLI、migration/rollback | schema、DEV DB、forward twice、rollback refusal、compatibility tests | design_ready | none |
| F-021 | §3、§10.1、§14.2；V25 完全在 scope 外 | 证明不读取 V25 artifact、不改变 broker bridge rejection | boundary_ready | none |
| F-022 | §3、§8.3、§10.1、§14.2；不拥有 execution kill switch | 证明 alerts 不写 kill-switch、不新增人工恢复/审批路径 | boundary_ready | none |

本矩阵的 `design_ready` 仅表明每个设计项已有可直接实现的 ownership、合同和验证路径；代码、迁移、生产数据与真实 SIM evidence 尚未执行，不得据此宣称 Phase 0B 已完成。

## 16. DESIGN-COMPLIANCE-001 设计阶段记录

1. **无简化版**：完整覆盖预注册、identity、Phase 0A/1 关联、延迟、流动性、auction、cadence、receipt、事务、迁移、回滚、API、metrics、runbook、测试及真实 SIM；没有把部分 export 或单日 fixture 当完成。
2. **无静默错误/假成功**：所有缺源、覆盖、时钟、identity、数据库、readback、late fact 均有 typed failure/assessment；明确禁止零值、默认值、缓存、内存/文件回执和 HTTP 200 空对象 fallback。
3. **无业务逻辑漂移**：观察层对 MiniQMT broker call 为零，不修改 selection/package validation/localSIM/B0 event-loop/action/reconcile；BUG-599/600/604/614 与 LEGACY identity 保持不变。
4. **无额外门禁审批**：没有 approval、RBAC、ack、人工 promote 状态。Production Gates 仅记录技术事实；生产写操作继续按用户当次授权执行，不把授权机制实现进产品。
5. **权威与进度同步**：本文明确服从模拟盘平台唯一上位蓝图；实现、测试、merge、DDL/config/restart/binding 和真实 SIM 状态必须在同一 PR 更新上位蓝图进度账本，不能用本阶段局部完成替代平台完成。

## 17. Phase 0B Exit 与 Phase 2 handoff

Phase 0B 只有在以下条件同时满足时才可标记完成：

- 实现与 migration 已按本文矩阵验收并合入；
- production schema/spec/readback 分别完成；
- 首个预注册 5 日窗口没有 identity 漂移；
- 每日期望 membership、Phase 0A final receipt 和 Phase 1 v2 export 均完整；
- unattributed terminal residual 为 0，所有关键 coverage 达到 spec；
- receipt 为 `COMPLETED + ELIGIBLE` 且 post-commit readback 成功；
- 独立同查询重建的 input/output/receipt hash 完全一致；
- Phase 0B 自身 broker calls 为 0，B0 行为没有被改变。

handoff 给 Phase 2 的只读输入为：spec/series/control tuple、eligible baseline receipt、daily memberships、data freeze/source hashes、bucket/formula/query versions、B0 distributions/variance 和完整 reason/coverage。Phase 2 必须另建 F2 详细设计；不能从本文直接获得 B1 broker side effect 或 activation 权限。
