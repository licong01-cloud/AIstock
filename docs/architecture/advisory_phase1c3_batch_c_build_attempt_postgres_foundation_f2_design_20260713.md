# AIstock Advisory Phase 1C-3 Batch C Build Attempt And PostgreSQL Foundation F2 Design

## 1. Background / 文档定位

本文是 `advisory_phase1c3_fixture_label_snapshot_f2_design_20260713.md` 中 Batch C 的唯一
实施级设计。它承接已合入的 Batch A 与 Batch B，冻结以下实现细节：

1. capture batch v1/v2 PostgreSQL discriminator、create/recover/readback；
2. outcome label authority header/payload 的 PostgreSQL append-only repository；
3. dataset build、attempt、file、event、gap、blob、snapshot 与 invalidation persistence；
4. additive migration/rollback、事务顺序、并发、fencing 和错误契约；
5. Batch C 与 Batch D deterministic Parquet/CAS/promotion/seal 的边界。

本批是 F2 数据基础设施切片，不是运行时荐股功能。Batch C 完成只表示合法 fixture evidence
能够通过真实 PostgreSQL repository 自动到达 `REQUESTED -> MATERIALIZED -> VERIFIED`，不表示
Phase 1C-3、模型训练、生产 observer、生产数据回填或首个 `SEALED` snapshot 已完成。

## 2. Scope / 范围

### 2.1 本批交付

- 在不改变 v1 canonical bytes/hash/SQL 默认行为的条件下，使
  `PostgresCaptureBatchRepository` 原生支持一个 v1 observation request 或一个 v2 label
  request。
- 实现 `PostgresOutcomeLabelRepository`，严格等价于 Batch B 的 in-memory revision/selector
  oracle，并持久化完整 authority header/payload join。
- 实现 `FixtureDatasetBuildRequest`、build/attempt/checkpoint/file/event/gap/snapshot/blob/
  invalidation 强类型模型与 in-memory/PostgreSQL repositories。
- 实现 REQUESTED、MATERIALIZED、VERIFIED 三个 checkpoint 的完整正向状态机；PROMOTED/SEALED
  的 schema 与 repository transaction 契约完整交付，但其真实正向 E2E 由 Batch D 的 writer、
  verifier、CAS 和 promotion receipt 驱动。
- 新增一对 additive migration/rollback；只在 DEV/test 数据库执行 apply/readback/negative/
  rollback L4。
- 提供稳定 reason code、有限且有诊断价值的结构化日志和完整失败传播，不吞掉异常。

### 2.2 允许修改范围

```text
docs/architecture/advisory_phase1c3_fixture_label_snapshot_f2_design_20260713.md
docs/architecture/advisory_phase1c3_batch_c_build_attempt_postgres_foundation_f2_design_20260713.md
backend/services/advisory_phase1/capture_foundation.py
backend/services/advisory_phase1/calculation_evidence.py
backend/services/advisory_phase1/label_builder.py
backend/services/advisory_phase1/label_builder_postgres.py                 # new
backend/services/advisory_phase1/dataset_build.py                          # new
backend/services/advisory_phase1/dataset_build_postgres.py                 # new
backend/db/migrations/add_advisory_phase1c3_label_snapshot_foundation_20260713.sql
backend/db/migrations/add_advisory_phase1c3_label_snapshot_foundation_20260713.rollback.sql
tests/advisory_phase1/test_phase1c3_batch_c_*.py                            # new
tests/advisory_phase1/test_phase1c3_batch_b_*.py                            # v1/B regression only
```

如果实现需要超出该范围，必须先在本文对应架构、契约和验收项中修订原因与影响，再重新执行
F2 design validation。不得在代码中临时增加 compatibility mode、fallback 或未记录的 helper。

### 2.3 Frozen zero-diff 边界

```text
backend/services/advisory_phase1/stage_trace.py
backend/services/simulation_runtime/selection.py
backend/services/strategy_package/multi_alpha_live.py
backend/services/strategy_package/inference_engine.py
backend/services/selection_center/
backend/services/paper_trading/
backend/routers/
frontend/
```

Batch C 不注册 API、CLI、scheduler、startup hook 或 runtime service；不读取 current
`TRACE_CAPTURE` control，不调用 `PostgresTraceAdmissionValidator`，不改变 Selection、Paper、
模拟盘、QMT 或策略包推理行为。

## 3. Non-goals / 非目标

- 不实现 deterministic PyArrow writer/verifier；属于 Batch D。
- 不配置生产 dataset store，不执行真实 CAS promotion，不生成首个生产 snapshot。
- 不执行生产 DDL/DML，不读取 `.env` 建立生产数据库连接，不启动或重启服务。
- 不扫描生产行情库，不执行历史回填，不运行 observer、capture/label worker。
- 不进行模型训练；未来训练仍只允许 WSL/Conda，Windows 训练禁止。
- 不增加角色、权限模型、RBAC、审批、人工确认、人工 override 或人工门禁。
- 不要求每次 DDL 前执行全库备份；DEV/test rollback L4 使用隔离测试库和测试数据。
- 不实现 reservation/hold/GC/consumer epoch；属于 Phase 1I。
- 不用 mock 文件、静态 JSON 或手工插入状态冒充 Batch D 的 `SEALED` 完成。

## 4. Current Architecture / 当前架构事实

### 4.1 已实现并冻结

- Batch A：`LabelPolicyBundle`、T/E/S/X_h、统一 `OutcomeEngine`、calculation evidence canonical
  bytes 和真实 local create-if-absent store。
- Batch B：`LabelCaptureBinding`、`LabelCaptureBatchRequestV2`、tagged request parser、in-memory
  capture dispatch、label append/revision/selector、candidate/universe enumeration 和
  `LabelBuilder`。
- v1 PostgreSQL capture foundation：batch/plan/membership/gap 表与 lease/fencing/CAS。
- Batch B 的 `OutcomeLabelAuthorityHeader` 与 `OutcomeLabelPayload` 是逻辑拆分 oracle，但当前
  没有 PostgreSQL physical repository。

### 4.2 当前缺口

- `PostgresCaptureBatchRepository` 的 type signature、insert 和 `_load_locked()` 仍只支持 v1。
- capture 表没有 schema/purpose discriminator，v2 不能安全 readback。
- label append 幂等、revision chain、header/payload exactly-one 只在内存成立。
- build/attempt/checkpoint/file/event/snapshot 还没有 domain 或 persistence 实现。
- Phase 1C-3 label/build/snapshot schema 还没有 migration。

### 4.3 单一权威数据流

```text
typed v1/v2 CaptureBatchRequestLike
  -> PostgresCaptureBatchRepository (explicit discriminator, no parser fallback)
  -> COMPLETE observation/label capture memberships
  -> PostgresOutcomeLabelRepository
       -> calculation evidence blob header
       -> label authority header
       -> exactly one monthly payload row
  -> frozen FixtureDatasetBuildRequest
  -> DatasetBuildRepository.create_or_get
  -> operation-scoped BuildAttemptRepository
  -> immutable attempt files + canonical file-set receipt
  -> MATERIALIZED checkpoint
  -> full verification evidence + verify receipt
  -> VERIFIED checkpoint
  -> Batch D consumes the frozen verified file set
```

不存在第二套 label 状态机、第二套 capture 状态机或“DB 不支持则退回内存”的路径。

## 5. Contracts / PostgreSQL 与状态机契约

### 5.1 Discriminator

`app.advisory_capture_batch` additive columns：

```text
capture_request_schema_version TEXT NOT NULL
  DEFAULT 'advisory_phase1_capture_batch_v1'
capture_purpose TEXT NOT NULL
  DEFAULT 'OBSERVATION_CAPTURE_V1'
```

允许组合只有：

```text
advisory_phase1_capture_batch_v1 + OBSERVATION_CAPTURE_V1
advisory_phase1_capture_batch_v2 + LABEL_CAPTURE_V1
```

历史 v1 rows 通过常量 default 获得 discriminator；两列不进入 v1 request payload/hash。v2 insert
必须显式提供两列。transition trigger 把两列加入 immutable comparison，任何 UPDATE 切换 schema
或 purpose 都失败。

### 5.2 Repository signatures

```text
create(request: CaptureBatchRequestLike) -> CaptureBatch
recover(request: CaptureBatchRequestLike, predecessor...) -> CaptureBatch
get(capture_batch_id) -> CaptureBatch
```

内部 dispatch 只使用 `capture_request_schema(request)` 与 `capture_request_purpose(request)`；
禁止 `try v1 -> except -> try v2`。

### 5.3 Create/recover transaction

1. 锁定 `capture_batch_id` 和 request hash identity。
2. 已存在同 ID 时按 discriminator 解析 persisted request，完整比较 typed request；相同返回，
   不同返回 conflict。
3. v1 写既有字段并调用 `_insert_plans()`；v2 写 `LabelCaptureBinding`、显式 discriminator，且
   不写 `advisory_capture_plan`。
4. v2 的 `control_binding_event_hash` 只复制 source observation binding 的历史 provenance；
   不检查当前 control event enabled。
5. recover 要求 predecessor request hash、schema、purpose 完全相同；v1/v2 不能跨类型 recovery。
6. commit 前 deferred closure 验证 v1 至少一条 plan、v2 恰好零条 plan。

### 5.4 `_load_locked()`

1. 先读取并验证 row discriminator。
2. 验证 `request_payload_jsonb.schema_version` 和 discriminator 相等。
3. v1 读取 plan rows，构造 frozen `CaptureBatchRequest`；v2 不读 plan payload，构造
   `LabelCaptureBatchRequestV2`。
4. 验证 request hash、binding schema/purpose、binding/request scope hashes。
5. 任何未知组合、payload 不一致或 parser 失败返回稳定 contract error；不回退其他 schema。

v1 serialized golden、默认 insert SQL 行为、receipt hash 和 admission validator 必须保持不变。

## 6. PostgreSQL Outcome Label Authority

### 6.1 Repository contract

新增 `OutcomeLabelRepository` Protocol，使 in-memory 与 PostgreSQL 实现共享：

```text
append(request, created_by_capture_batch_id) -> OutcomeLabelVersion
get(label_version_id) -> OutcomeLabelVersion | None
chain_for(label_key_hash) -> tuple[OutcomeLabelVersion, ...]
header_for(label_version_id) -> OutcomeLabelAuthorityHeader | None
payload_for(label_version_id) -> OutcomeLabelPayload | None
```

`PostgresOutcomeLabelRepository` 依赖显式 transaction connection factory 和只读
`CalculationEvidenceReader`。不得自行读取 current source、重新计算 outcome 或按 URI 猜 store。

### 6.2 Append transaction

固定顺序：

1. canonical revalidate `LabelAppendRequest`。
2. `pg_advisory_xact_lock(hashtext(label_key_hash))`。
3. 按 `label_append_request_hash` 查询 authority header；命中时完整 join payload、比较 request
   semantic fields、evidence SHA/size/backend 和 label key。相同返回首次 version/URI；不同报
   collision。
4. 未命中时 `FOR UPDATE` 锁定该 key 最大 revision，验证 expected predecessor、连续 revision、
   terminal state transition 和新 source/evidence requirement。
5. 验证 creator capture batch 当前为 `RUNNING + LABEL_CAPTURE_V1`、lease 未过期、purpose/schema
   正确。该检查是自动数据一致性，不是人工门禁。
6. exact insert/readback `advisory_dataset_blob` evidence header；same backend/hash different size
   为 conflict。
7. `SELECT clock_timestamp()` 取得数据库权威 `computed_at`，通过
   `OutcomeLabelVersion.from_append()` 产生 content hash/version ID。
8. 插入 unpartitioned header，再插入对应月份 payload。
9. 在事务内完整 join readback 并运行 `_validate_header_payload()`；commit 时 deferred constraint
   再验证 exactly-one closure。

禁止 `ON CONFLICT DO UPDATE`、调用方 revision、应用时间戳、URI 参与 append request identity，
也禁止捕获缺分区异常后写入默认分区。

### 6.3 Authority header columns

`app.advisory_outcome_label` 至少保存：

```text
label_version_id PK
label_content_hash UNIQUE
label_key_hash, label_revision_no
supersedes_label_version_id, supersedes_label_version_hash
label_append_request_hash UNIQUE
label_policy_bundle_id/hash, label_policy_hash
label_source_revision_set_id/hash
owner_type, owner_key, canonical_signal_id, observation_version_id
candidate_stage_evidence_id, symbol, decision_as_of_trade_date, evidence_scope
horizon_trading_days, projection, projection_schema_version
intended_entry_trade_date, earliest_sell_eligible_trade_date, exit_trade_date
maturity_status, outcome_event_status, entry_status
projection_payload_hash
calculation_evidence_sha256/size_bytes/store_backend_hash
created_by_capture_batch_id, computed_at
```

唯一/外键闭包：

- `UNIQUE(label_key_hash,label_revision_no)`；
- `UNIQUE(supersedes_label_version_id)`；
- predecessor FK 和 trigger 保证同 key、revision n-1、hash 相等；
- candidate owner FK/trigger 保证 observation/stage/symbol 一致；
- `UNIQUE(label_version_id,decision_as_of_trade_date)`；
- evidence composite FK 到 `(store_backend_hash,blob_sha256)`。

### 6.4 Partitioned payload columns

`app.advisory_outcome_label_payload` 不使用通用 `outcome_jsonb`。它按月分区并保存可独立校验的
scalar fields：

```text
identity:
  decision_as_of_trade_date, label_version_id, label_content_hash
  projection, projection_schema_version, horizon_trading_days
clock/status:
  scheduled_maturity_ts, source_closed_at, event_closed_at, failure_observed_at
  maturity_status, outcome_event_status, entry_status, missing_source_receipt_hash
projection:
  projection_value_decimal, projection_event_code, projection_payload_hash
price:
  entry_price_raw_yuan, entry_adj_factor, exit_price_raw_yuan, exit_adj_factor
cashflow:
  entry_quantity, exit_quantity, buy_execution_price_yuan, sell_execution_price_yuan
  buy_notional_yuan, sell_notional_yuan, buy_fee_yuan, sell_fee_yuan
  entry_cash_yuan, residual_cash_yuan, exit_cash_yuan, terminal_value_yuan
  cost_breakdown_hash, benchmark_gross_total_return, benchmark_net_total_return
barrier/path:
  entry_day_touch_status, executable_barrier_status
  executable_event_trade_date, time_to_executable_hit_trading_days
  observed_holding_trading_days
terminal/censor/settlement:
  terminal_disposition, terminal_symbol, terminal_event_trade_date, terminal_event_closed_at
  terminal_source_hash, terminal_settlement_raw_li, terminal_settlement_adj_factor
  terminal_settlement_quantity_multiplier, terminal_settlement_cashflow_yuan_per_share
  censor_reason_code
source closure:
  policy_bundle_hash, price_path_hash, corporate_actions_hash
  benchmark_bundle_hash, formula_schema_version, calculation_evidence_schema_version
evidence locator:
  calculation_evidence_uri, calculation_evidence_sha256
  calculation_evidence_size_bytes, calculation_evidence_store_backend_hash
reason_codes JSONB array
```

PK 为 `(decision_as_of_trade_date,label_version_id)`，并用 composite FK 引用 header。
projection/maturity/event/cashflow/barrier CHECK 与 `OutcomeCalculationResult` validator 同构；无关
字段必须 NULL。terminal/censor/settlement CHECK 与 `TerminalResolution` validator 同构。四个
source closure hash 从 calculation evidence canonical payload 逐项计算，禁止信任调用方另外
提供的 hash。header 与 payload 重复 identity/status/hash 必须相等。

当前权威 daily outcome 模型保存 `intended_entry_trade_date` 与 `exit_trade_date`，不具有可证明的
intraday entry/exit timestamp。schema 不创建由日期拼接出的 `entry_ts/exit_ts`，也不使用午夜
默认值；未来只有在 source evidence 增加真实成交时刻后才能通过新 schema revision 增加该字段。

calculation evidence 详情不复制为通用 JSONB。需要重建完整 `OutcomeCalculationResult` 时，
repository 通过存储的 locator、backend/hash/size 调用 `CalculationEvidenceReader` 读取真实
canonical bytes，校验 schema/hash/size 后构造 bundle；缺文件、错误 bytes 或 store identity
不一致显式失败，不返回残缺 label。reader 只允许读取配置 store root 内由该 backend identity
生成的 content-addressed path；任意 URI、路径穿越或 root 外路径直接拒绝。

### 6.5 Exactly-one 和 append-only

- DEFERRABLE constraint triggers 在 transaction end 验证每个新 header 恰好对应一条 payload，
  每条 payload 恰好对应 header。
- header、payload 都有 no-update/no-delete trigger。
- 仅预建 `2026-06`、`2026-07`、`2026-08` 三个月份分区；没有 default partition。
- 缺月份分区时显式返回 `ADVISORY_PHASE1C3_LABEL_PARTITION_MISSING`。
- rollback 在发现任一 label/build/snapshot evidence row 时拒绝继续。

## 7. Dataset Build Domain

### 7.1 `FixtureDatasetBuildRequest`

强类型 request 必须冻结：

```text
schema_version = advisory_phase1c3_fixture_dataset_build_request_v1
phase0a_audit_id/hash, phase1_handoff_bundle_hash, handoff_readiness_hash
sorted admission scope ids/hashes and admission_scope_set_hash
sorted COMPLETE observation/label capture descriptors and capture_set_hash
date_start/date_end
selected observation mapping set/hash
label policy bundle/hash, horizons, projections
selected label mapping or selector request set/hash
universe/benchmark/cost/calendar/symbol policy hashes
query_registry_version/hash
snapshot_source_revision_set_id/hash
required composite capability matrix/hash
builder_version, code_commit, writer_version
snapshot_schema_version, schema_fingerprint
partition_policy/hash, compression config/hash
requested_source_cutoff, label_as_of_ts
optional BaseSnapshotIdentity, nullable together
build_request_hash
```

attempt、lease、worker、URI、运行时间不进入 request/hash。request 构造时必须验证全部 capture
为 COMPLETE、receipt/membership 精确、scope/date/policy/source compatible；不得查询 latest 补值。

### 7.2 Identity

```text
logical_build_key_sha256 = sha256(build_request_hash,
                                  capture_set_hash,
                                  snapshot_source_revision_set_hash)
build_id = advbuild_<sha256(logical_build_key_sha256, build_generation)[:24]>
```

相同完整输入必须命中同 logical key。semantic/source/policy 改变形成新 logical key；只有前一
generation 为 ABORTED 且提供 termination receipt 时，才可在相同 key 创建 next generation。
FAILED_TERMINAL 不允许相同 key 重建。

### 7.3 Domain models

`dataset_build.py` 至少提供：

```text
BuildLifecycle = ACTIVE | SEALED | FAILED_TERMINAL | ABORTED
BuildCheckpoint = REQUESTED | MATERIALIZED | VERIFIED | PROMOTED | SEALED
AttemptOperation = MATERIALIZE | VERIFY | PROMOTE | SEAL | RECOVER
AttemptState = ACTIVE | SUCCEEDED | FAILED | EXPIRED | ABORTED

FixtureDatasetBuildRequest
BaseSnapshotIdentity
DatasetBuild
DatasetBuildAttempt
DatasetAttemptFile
MaterializeReceipt
VerifyReceipt
PromotionReceiptDescriptor
SealReceiptDescriptor
DatasetBuildEvent
DatasetBuildGap
DatasetBlobHeader
SealedDatasetSnapshot
DatasetSnapshotFile/Observation/Label/BlobRef
DatasetSnapshotInvalidation
```

所有模型 `extra=forbid,frozen=True`，canonical payload/hash validator 与 DB schema 一一对应。

## 8. Build And Attempt Repositories

### 8.1 Build create/get

`create_or_get(request, optional_rebuild_authority)`：

1. canonical revalidate request 和 capture/source descriptors。
2. 取得 logical-key transaction advisory lock。
3. 已有 SEALED generation 时重新验证未 invalidated 后返回；已有 ACTIVE 时返回当前状态。
4. 最后 generation 为 ABORTED 时仅接受 exact predecessor build/termination receipt 和 expected
   next generation；FAILED_TERMINAL 拒绝。
5. 插入 ACTIVE/REQUESTED build 和唯一 REQUESTED event；同 ID/hash retry 完整 readback。

`create_or_get` 不创建 attempt，不自动跨 checkpoint，也不执行文件 IO。

### 8.2 Attempt acquisition

```text
start_attempt(build_id, operation, expected_build_row_version,
              expected_checkpoint, lease_owner_id, lease_token,
              lease_seconds, operation_request_hash) -> DatasetBuildAttempt
```

- operation 必须对应当前 checkpoint 的合法下一步。
- 同 operation 有未过期 ACTIVE attempt 返回 `BUILD_ALREADY_RUNNING`。
- 新 attempt 在短事务中递增 build current fencing token、绑定 current attempt、写
  ATTEMPT_STARTED event。
- 数据库时间决定 acquired/expiry；客户端时间不参与 lease validity。

合法 operation：

```text
REQUESTED    -> MATERIALIZE
MATERIALIZED -> VERIFY
VERIFIED     -> PROMOTE       # Batch D positive path
PROMOTED     -> SEAL          # Batch D positive path
stale ACTIVE -> RECOVER
```

### 8.3 Attempt files and MATERIALIZED

MATERIALIZE attempt 按 logical path append `DatasetAttemptFile`。每次 append 验证：

- current ACTIVE attempt、operation、lease、fencing、expected build row version；
- path/role/partition/ordinal 唯一；
- SHA、size、rows、schema、partition content hash 和 writer/compression identity 完整；
- same key exact retry 完整比较；same key/different row conflict。

`complete_materialize()` 在一个 control transaction 中：

1. 锁定 build/attempt；
2. 重验 attempt 未过期和所有 files；
3. 对排序 file rows 计算 file-set hash 与 materialize receipt；
4. 将 attempt 置 SUCCEEDED；
5. 一次 CAS 固定 materialized attempt/receipt/file-set 并推进 checkpoint；
6. 写唯一 MATERIALIZED event。

checkpoint 成功后 file rows 不允许 UPDATE/DELETE。Batch C fixture 使用真实临时文件产生的
SHA/size/readback evidence；不使用 in-memory filename 或虚构 hash。

### 8.4 VERIFY and VERIFIED

Batch C 的 verifier contract 只消费 frozen materialized file set，不读取 mutable source。实现
完整验证 repository/文件层可在 Batch C 决定的基础不变量：

- 每个 staging URI 文件存在，完整读取后的 SHA/size 与 DB 相等；
- logical path/role/partition/ordinal、row count/schema fingerprint/file-set hash 闭合；
- verify request 精确引用 materialized attempt/receipt/file-set。

Parquet schema、sort、selected mapping 和 cross-reference 的完整 verifier 由 Batch D 实现并在
同一 `VerifyReceipt` 增加冻结的验证项集合。Batch C 不写“全部 Parquet 验证通过”的假 receipt。
因此 Batch C 的正向 `VERIFIED` receipt 必须带
`verification_profile=PHASE1C3_BATCH_C_FILESET_FOUNDATION_V1`，不能被 Batch D seal 接受；Batch D
只接受 `PHASE1C3_BATCH_D_FULL_PARQUET_V1`。profile 是内容契约 discriminator，不是人工门禁。

`complete_verify()` 原子固定 verified attempt/receipt/file-set，推进 checkpoint 并写 VERIFIED
event。verify 失败只终止 attempt，不回退 build checkpoint。

### 8.5 Failure, expiry, recovery and termination

- `fail_attempt()`：ACTIVE -> FAILED，保存稳定 error code/hash 和 ATTEMPT_FAILED event；build
  保持原 checkpoint。
- `expire_and_recover()`：仅数据库时间已过期时，将旧 attempt 置 EXPIRED，写事件并创建新
  RECOVER attempt/更高 fencing；旧 token 永久无效。
- `terminate_build()`：校验 expected row/checkpoint/current attempt/fencing/fixed sets 后，原子
  进入 ABORTED 或 FAILED_TERMINAL，写 termination receipt/event。
- checkpoint 已固定但文件损坏时只能 terminate；不能替换 file set 或回退 checkpoint。
- 不存在 last-writer-wins、后台自动重试或静默新 generation。

## 9. Snapshot/Blob/Gap Persistence Boundary

### 9.1 Blob

`advisory_dataset_blob` 以 `(store_backend_hash,blob_sha256)` 为 PK。Batch C 用于 calculation
evidence header 和未来 snapshot file identity；same key/different size 直接 conflict。无
UPDATE/DELETE。

### 9.2 Snapshot aggregate

Batch C 完整定义并持久化 final snapshot、file、observation、label、blob ref 和 build mapping
的 schema/repository transaction。正向 `seal()` 必须要求 Batch D full Parquet verify profile、
promotion receipt、manifest 和真实 CAS blobs；Batch C 自己不会构造这些成功输入。

这样 schema/repository 不是 placeholder，但 Batch C 测试只能覆盖：

- malformed/incomplete aggregate 全部拒绝；
- exact persisted fixture aggregate 的 repository transaction/constraint contract；
- 不把该测试声明为 Phase 1C-3 `SEALED golden`。

最终 `SEALED golden` 仍必须由 Batch D writer/verifier/CAS 正向链生成。

### 9.3 Base invalidation

Batch C 提供只读 `SnapshotInvalidationOracle` 和 append-only invalidation repository：

- base identity 必须 id/content/manifest/source/capture/policy hashes nullable-together；
- base 和实际复用链必须 SEALED、未 invalidated、无 cycle；
- invalidation same request exact retry，冲突失败，不支持 reinstatement；
- base invalidated 或 identity 不完整时拒绝，不静默改成 full rebuild。

### 9.4 Gap

`DatasetBuildGapRepository` append-only 保存不可形成合法 evidence 的明确 gap。gap 不生成 label、
attempt file 或 success receipt，不从 coverage 中删除。same content hash exact retry；冲突失败。

## 10. Additive Migration

### 10.1 FK/topology order

```text
1. alter capture discriminator + deferred v1/v2 plan closure
2. dataset blob
3. label authority header
4. label payload parent + 2026-06/07/08 partitions
5. build + attempt + attempt file + event + gap
6. snapshot + file + observation + label + invalidation
7. snapshot blob ref and composite closure
8. append-only/state/closure triggers and indexes
```

不使用 disabled trigger、temporary `NOT VALID`、runtime DDL、role/grant/revoke 或默认分区完成
迁移。

### 10.2 Build/attempt database invariants

- partial unique index：每个 logical key 最多一个 ACTIVE generation；
- lifecycle/checkpoint 组合及 nullable-together checkpoint fields；
- current attempt/build/fencing 双向一致；
- attempt terminal 后 immutable；ACTIVE attempt 必须有完整 lease fields；
- file insert 的 fencing/attempt/build current identity；
- checkpoint 单向、固定 attempt/receipt/set 不可替换；
- event/file/gap/snapshot/blob/ref/invalidation no-update/no-delete；
- final snapshot 只能 INSERT 为 SEALED；
- snapshot observation/label 唯一和 label membership cross-reference；
- snapshot file/blob ref composite identity closure。

### 10.3 Rollback

rollback 只用于 DEV/test：

1. 先检测所有 Batch C 新表和 v2 capture rows 是否为空；非空立即失败并列出对象计数。
2. 按 FK 反序删除 triggers/tables/indexes。
3. 删除 capture discriminator columns 前确认仅存在 v1/default rows。
4. rollback 后运行 v1 capture migration/schema/readback regression。

不要求全库备份，不访问生产库，不把 rollback availability 设计成人工审批。

## 11. Automatic Data Invariants / 自动数据不变量

以下是程序必须自动满足的数据正确性，不是角色、授权、审批或人工门禁：

| 操作 | 合法输入自动通过 | 非法输入明确失败 |
|---|---|---|
| v1 capture create/readback | frozen v1 payload/hash/plan | schema/purpose/payload drift |
| v2 label capture create/readback | exact binding/source/mapping hashes | fallback、plan row、current-control dependency |
| label append | terminal predecessor、完整 evidence、有效 label batch | fork、collision、缺分区、半 header/payload |
| build create | COMPLETE capture set、frozen source/request | latest lookup、incomplete base、same-key conflict |
| attempt start | current row/checkpoint、合法 operation | stale row、并发 lease、错误 operation |
| file append/checkpoint | active lease/fencing、真实 file evidence | old token、伪 hash、file-set fork |
| verify | frozen materialized set、profile-specific完整项 | 缺文件、hash/size/profile 不闭合 |

测试必须提供每个正向 fixture 并证明不需要人工操作即可通过。任何只写反向拒绝而没有正向合法
链路的实现都不能通过 Batch C 验收。

## 12. Error And Logging Contract

### 12.1 Stable reason codes

```text
ADVISORY_PHASE1C3_CAPTURE_DISCRIMINATOR_CONFLICT
ADVISORY_PHASE1C3_CAPTURE_PAYLOAD_CLOSURE_INVALID
ADVISORY_PHASE1C3_LABEL_APPEND_REQUEST_CONFLICT
ADVISORY_PHASE1C3_LABEL_PREDECESSOR_INVALID
ADVISORY_PHASE1C3_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID
ADVISORY_PHASE1C3_LABEL_PARTITION_MISSING
ADVISORY_PHASE1C3_CALCULATION_EVIDENCE_INVALID
ADVISORY_PHASE1C3_BUILD_REQUEST_CONFLICT
ADVISORY_PHASE1C3_BUILD_ALREADY_RUNNING
ADVISORY_PHASE1C3_BUILD_GENERATION_INVALID
ADVISORY_PHASE1C3_BUILD_TRANSITION_INVALID
ADVISORY_PHASE1C3_ATTEMPT_OPERATION_INVALID
ADVISORY_PHASE1C3_ATTEMPT_LEASE_EXPIRED
ADVISORY_PHASE1C3_ATTEMPT_FENCING_STALE
ADVISORY_PHASE1C3_ATTEMPT_FILE_CONFLICT
ADVISORY_PHASE1C3_CHECKPOINT_CONFLICT
ADVISORY_PHASE1C3_BASE_SNAPSHOT_INVALID
ADVISORY_PHASE1C3_SNAPSHOT_INVALIDATED
ADVISORY_PHASE1C3_DATABASE_INVARIANT_VIOLATION
```

### 12.2 Translation and propagation

- 已知 constraint name 映射到固定 reason code；日志保留 build/attempt/label/capture ID 与
  constraint name。
- 未知 `IntegrityError` 以 `DATABASE_INVARIANT_VIOLATION` 包装并保留 exception cause，不按
  retry/success 返回。
- 不允许 `except Exception: return None/[]/success`，不允许缺表/缺列/缺分区 fallback。
- 用户错误不输出大 payload；诊断日志只记录 operation、identity、checkpoint、fencing、reason
  code 和稳定 hash。
- 正常逐行 file/label 写入不刷 INFO；只在 attempt start/end、checkpoint、terminal failure 和
  unexpected DB invariant 记录一次有价值日志。

## 13. Concurrency / 并发与事务

- label 使用 label-key advisory xact lock + terminal row lock。
- build create 使用 logical-key advisory xact lock + build row lock。
- attempt/checkpoint 使用 build row lock、expected row version、current attempt 和 fencing。
- 数据库 clock 决定 lease/computed_at/event_at；应用 clock 只用于测试注入的 in-memory oracle。
- repository transaction 不跨文件 IO；文件先 durable close/readback，后登记短 control transaction。
- DB transaction 失败不保留部分 header/payload、checkpoint 或事件。
- 客户端超时重试只能依据 request/content hash 完整 readback 后返回既有对象。

## 14. Implementation Plan / 实施方案

### C1：capture discriminator 与 v1 regression

- migration alter/constraints；
- Postgres create/recover/load typed dispatch；
- v1 serialized bytes/hash/SQL/readback golden；
- v2 exact positive/negative/recovery tests。

### C2：label PostgreSQL authority

- evidence reader/blob header；
- label header/payload migration 与 partitions；
- append/chain/get/readback repository；
- concurrency、revision、constraint bypass negative tests。

### C3：build/attempt domain 与 repositories

- request/identity/domain models；
- create/start/fail/expire/recover/terminate；
- file/event/gap repositories；
- real-file MATERIALIZED 和 foundation-profile VERIFIED positive path。

### C4：snapshot schema/repository foundation

- snapshot/blob/ref/invalidation schema；
- base admission；
- negative and persistence transaction fixtures；
- Batch D full-profile boundary tests。

### C5：一致性审核

- DESIGN-COMPLIANCE-001 item-by-item；
- zero-diff/import/runtime registration scan；
- no approval/auth/current-control/fallback scan；
- migration apply/readback/negative/rollback L4；
- 更新父级 acceptance matrix。

## 15. Verification Plan

### 15.1 L0 static

- Ruff/py_compile/import smoke；
- migration/rollback statement and table parity；
- model/column/hash/SQL parameter parity；
- forbidden import、runtime registration、approval/auth/role/grant/revoke scan；
- v1 golden bytes/hash diff；
- `git diff --check` 和 F2 feature workflow validation。

### 15.2 L1 pure/fixture

- build request canonical ordering/hash、generation and base nullable-together；
- build/attempt complete state reachability matrix；
- stale lease/fencing、wrong operation、checkpoint fork、terminal recovery；
- file-set canonical hash、exact retry/collision；
- positive automatic REQUESTED -> MATERIALIZED -> VERIFIED using real files；
- foundation/full verification profile separation。

### 15.3 L2 PostgreSQL integration

- migration apply twice idempotently；
- existing v1 row readback and new v1 create/recover unchanged；
- v2 create/recover/load and no-plan closure；
- label serial/concurrent exact retry、fork/gap/collision/URI retry；
- header-only/payload-only/mismatched join rejected at commit；
- missing partition rejected；
- direct illegal UPDATE/DELETE/state jump/fencing bypass rejected；
- build/attempt/file/event/gap/blob/snapshot/invalidation exact retry；
- rollback refuses evidence, then succeeds after isolated fixture teardown；
- rollback 后 v1 capture positive path through。

### 15.4 Regression boundary

- Batch A/B direct suites；
- Advisory Phase 1 focused regression；
- frozen shared files zero diff；
- no Selection/Paper/模拟盘/QMT import or call path changes。

## 16. Risks / Failure Modes

| 风险 | 后果 | 设计处理 |
|---|---|---|
| v1 parse 失败后尝试 v2 | 历史行为漂移 | discriminator-first，无 fallback |
| v2 复用 trace admission | current control 阻断离线 label | 只验历史 COMPLETE provenance |
| header/payload 半写入 | label authority 不完整 | single transaction + deferred exactly-one |
| 通用 outcome JSONB | schema/约束被绕过 | scalar physical closure |
| 调用方生成 revision/time | 并发 fork | DB lock + DB time + repository revision |
| 缺分区写默认表 | 数据范围失控 | 无 default partition，明确失败 |
| attempt 失败回退 checkpoint | 已冻结 file set 被替换 | checkpoint 单向，terminate/new generation |
| stale worker 登记文件 | 混合 attempt 证据 | lease/fencing/current attempt/row version |
| Batch C receipt 冒充 full verify | Batch D seal 接受未验 Parquet | verification profile discriminator |
| 捕获 DB 异常后返回成功 | 静默错误 | stable reason + cause propagation |
| 新增审批/角色 | 单用户流程受阻 | schema/code scan 明确禁止 |
| 触碰共享选股运行链 | Selection/Paper/模拟盘回归 | zero-diff/import boundary |

## 17. Rollout / Rollback / Production Gates / 生产门禁与状态

本文和后续 Batch C 代码任务均不激活 runtime。Batch C migration 只在隔离 DEV/test DB 做 L4；
生产 schema 应用属于父级 Phase 1F 的独立任务。本批不创建生产 store、observer 或 worker。

```text
production_ddl_gate = noop
production_dml_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
runtime_activation = noop
model_training = noop
windows_model_training = prohibited_by_scope
selection_paper_simulation_qmt_impact = none
```

这些状态不是新增业务门禁。未来生产 DDL 只有在 Phase 1F 明确执行任务中应用一次，不要求每个
DDL 前全库备份。

## 18. Design Acceptance Index

- F-201：v1/v2 capture PostgreSQL dispatch 显式且 v1 行为零漂移。
- F-202：v2 create/recover/load 不依赖 current TRACE_CAPTURE，不写 plan row。
- F-203：label append 与 Batch B oracle 等价，支持并发幂等和连续 revision。
- F-204：label header/payload scalar physical closure、exactly-one、append-only 完整。
- F-205：calculation evidence blob/reader identity 完整，缺 bytes 不返回残缺 label。
- F-206：build request、logical key、generation 与 base admission 完整冻结。
- F-207：build/attempt lease/fencing/checkpoint/recover/terminate 状态机闭合。
- F-208：attempt file/event/gap append-only、exact retry 和 file-set receipt 闭合。
- F-209：合法真实文件 fixture 自动到 MATERIALIZED/VERIFIED，无人工操作。
- F-210：Batch C foundation verify 与 Batch D full verify 不可混用。
- F-211：snapshot/blob/ref/invalidation schema/repository 与 Batch D 输入契约完整。
- F-212：migration apply/readback/negative/rollback 可实施且 v1 rollback 后可用。
- F-213：无 silent fallback、fake success、简化版或未知 DB 错误吞噬。
- F-214：无角色、授权、审批、人工门禁或额外运行环境拒绝机制。
- F-215：共享 Selection/Paper/模拟盘/策略包文件零修改、零运行时接线。
- F-216：无训练；未来训练仍只允许 WSL/Conda。

## 19. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-201 | `capture_foundation.py`; Batch C migration | existing v1/v2 pure regression; PostgreSQL discriminator L4 not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted; no production DDL is permitted in this task |
| F-202 | `capture_foundation.py` typed Postgres create/recover/load | v2 in-memory contract regression; PostgreSQL readback L4 not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted |
| F-203 | `label_builder_postgres.py`; evidence readback before append | canonical append/revision/evidence logic compiled; PostgreSQL serial/concurrent L4 not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted |
| F-204 | migration header/payload/owner/blob-size triggers | schema source review; commit closure and direct mutation L4 source exists but was not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted |
| F-205 | `calculation_evidence.py`; `label_builder_postgres.py` | exact read/hash/size/root tests; DB composite blob FK L4 not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted |
| F-206 | complete frozen request identities; transactional capture admission | canonical request/generation/predecessor fixtures; DB admission L4 not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted |
| F-207 | explicit expire/recover/heartbeat and build-attempt closure | lease/fencing/recover/heartbeat local fixtures; DB transition L4 not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted |
| F-208 | exact event readback; file/event/gap persistence | exact retry/file-set fork local fixtures; DB append-only L4 source exists but was not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted |
| F-209 | `dataset_build.py` real-file verifier | automatic real-file REQUESTED->MATERIALIZED->VERIFIED fixture | verified | none |
| F-210 | verification and promotion closure discriminators | Batch C contract cannot form promoted/sealed success without Batch D receipts | verified | none |
| F-211 | snapshot aggregate closure; shared invalidation locks; exact retry readback | aggregate/base-invalidation pure contracts; PostgreSQL L4 source exists but was not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted |
| F-212 | idempotent migration/rollback pair; expanded DEV L4 | apply-twice/direct mutation/state jump/orphan attempt/incomplete snapshot checks authored but not executed | verified_source_only | isolated DEV PostgreSQL L4 remains unexecuted |
| F-213 | structured repository database diagnostics | no fallback scan; focused advisory suite 187 passed | verified | none |
| F-214 | changed-file/migration scan | no role/auth/approval/grant/revoke implementation | verified | none |
| F-215 | changed-file scope | frozen shared runtime files zero diff; advisory suite 187 passed | verified | none |
| F-216 | changed-file/training API scan | no training API or Windows training path | verified | none |

## 20. DESIGN-COMPLIANCE-001

- [x] 与 Phase 1C-3 父级 identity、label、build、snapshot、schema 和 batch 边界一致。
- [x] 复用 Batch A/B 强类型模型和 oracle，不建立第二套 capture/label 状态机。
- [x] v1 canonical bytes/hash/SQL 默认行为冻结；v2 discriminator-first 且无 parser fallback。
- [x] header/payload、revision、build/attempt/file/snapshot physical constraints 完整，不用通用
  JSONB、POC 或 mock-only success 代替。
- [x] 合法输入存在自动正向链路；所有保留的数据不变量均可满足。
- [x] 错误明确传播并有有价值日志；无静默成功或默认值。
- [x] Batch C 与 Batch D verify profile 边界明确，不提前宣称 SEALED。
- [x] 不修改 Selection、Paper、模拟盘、QMT、策略包推理或共享 stage trace。
- [x] 不新增审批、角色、授权、人工确认、人工门禁或未确认的运行环境拒绝机制。
- [x] 设计任务不执行 DDL/DML、依赖安装、服务重启或训练。

## 21. Exit Criteria

Batch C 详细设计只有在以下条件全部满足后才可进入代码阶段：

- F-201 至 F-216 均为 `design_ready` 且无未批准 gap。
- F2 feature workflow validation 通过。
- 父级状态和 acceptance matrix 与 Batch A/B 已合入事实一致。
- DB schema、transaction、state transition、reason code、allowed scope 和测试均有实施位置。
- 合法输入正向链路、失败链路和 Batch D 边界均可执行，无简化、静默错误或业务偏移。
- `production_ddl_gate`、DML、dependency、runtime 和 model training 在本设计任务均为 noop。
