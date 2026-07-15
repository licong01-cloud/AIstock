# Advisory Phase 1G G3 Transactional PostgreSQL Writer F2 详细设计

## 1. 背景、文档定位与当前状态

本文是父级
`advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md` 中 G3 的实施级详细设计，承接：

```text
Phase 1F.2 已发布 scope-aware schema/catalog
  -> Phase 1G G1 typed contracts/schema guard/result store
  -> Phase 1G G2 source replay/historical trace projection
  -> Phase 1G G3 transactional PostgreSQL writer（本文）
  -> Phase 1G G4 service/CLI/recovery orchestration
  -> Phase 1G G5 DEV evidence
```

G3 只实现 PostgreSQL transaction primitives 和单 target 原子 writer，不实现 CLI、batch service、自动调度、
启动钩子或 DEV/production 激活。G3 使用现有 Phase 1F.2 schema，不增加 migration，不在运行时执行 DDL。

当前状态：

```text
design_tier = F2
design_status = implemented_and_merged
design_review = passed_2026-07-15
implementation_status = merged_pr_2178_2026_07_15
code_merge_state = merge_commit_71d3486d1b7460262932f4f4f209e695c2b56dda
ddl_pending = none
dml_pending = none_for_g3
local_validation = 30_g3_passed_plus_59_shared_passed_1_skipped
github_ci = passed
dev_validation = not_required_for_g3_code_batch
production_activation = none
```

## 2. 目标

1. 将 G2 `Phase1GTargetProjectionSnapshot` 中冻结的 source、trace、artifact 和 stage 事实写入 Phase 1
   PostgreSQL schema，禁止重跑 Selection、推理或策略包验证。
2. 保证一个 target 的 source revision freeze、trace outbox、observation、lineage、stage、candidate、membership
   和 delivery 在一个 caller-owned transaction 中原子提交。
3. 提供 control binding、capture batch、trace outbox 和 observation 的 caller-owned cursor 原语；现有 public
   repository API 保持兼容，不反向调用 global pool。
4. exact retry 必须完整读回全部 header/child rows并重算 hash，不能只依赖 PK、row count 或 `ON CONFLICT`。
5. 同 canonical signal 的合法新 evidence 形成单一 successor revision；同 revision 不同内容 fail-fast。
6. recovery 精确复用 predecessor outbox/observation，禁止覆盖 immutable rows或生成第二份相同自然键证据。
7. commit response loss 通过新连接 exact readback 判定真实提交事实；不得把 unknown 伪装成功或错误回滚已提交事实。
8. 单 Alpha、原生多 Alpha、raw-empty、filtered-empty 均使用 G2 完整 projection，不截断、不抽样、不随机补齐。
9. 所有失败显式 reason、exception chain 和有价值后台日志；无静默 `None`、空列表或 generic success。
10. 不增加用户、角色、RBAC、审批、授权、人工确认、备份门禁或 manual bypass。

## 3. 范围与非目标

### 3.1 In Scope

- `TRACE_CAPTURE` control binding transaction primitive：current、append、get-or-append exact。
- RUNNING capture batch lock、row-version/fencing/lease/request exact check。
- caller-owned capture membership、row-version advance、complete/readback primitives。
- G2 source revision freeze intent 的同事务 freeze/readback。
- scope-aware trace outbox exact read、append、recovery reuse 和 delivery append。
- pure observation oracle 输出 PostgreSQL semantic draft，并在 canonical signal 锁内按已提交 revision chain
  物化最终 row bundle 的强类型 projection。
- canonical signal、observation version、lineage identity/payload、stage evidence、candidate identity/payload writer。
- exact retry、合法 successor、two-writer、deadlock/serialization、commit uncertainty 语义。
- post-commit 新连接完整 readback 和稳定 `Phase1GTargetCommitProjection`。
- pure、query-spy 和 disposable PostgreSQL 16 验证。

### 3.2 Non-goals

- 不实现 G4 per-target service、multi-target batch、CLI、exit code、normal rerun orchestration。
- 不创建、acquire、expire、recover 或 fail 整个 capture batch 的业务编排；G3 只提供所需原语。
- 不发布 `Phase1GCaptureResult`、attempt/batch receipts 到 external CAS store；由 G4 在 DB readback 后完成。
- 不执行真实 DEV/production DML；G5 才执行 DEV zero-residue/persistent evidence。
- 不修改 Selection Center、策略包推理、多 Alpha live、模拟盘、Paper、QMT、QE/RD/Qlib。
- 不读取回测数据、训练模型或调用实时交易功能。
- 不新增或执行 migration、DML backfill、dependency、API、UI、scheduler 或 startup hook。
- 不增加审批、操作员确认、角色授权、额外备份或人工改库流程。

## 4. 当前权威与精确缺口

### 4.1 必须复用的权威

| 权威 | 当前实现 | G3 使用方式 |
|---|---|---|
| G2 target snapshot | `phase1g_historical_trace_contract.py` | 唯一 source/trace/stage 输入 |
| source freeze | `source_revision_postgres.py` | 直接调用 `freeze_in_transaction/read_exact_in_transaction` |
| control binding | `control_binding.py` | 增加 caller-owned exact primitive，保留 public wrapper |
| capture lifecycle | `capture_foundation.py` | 增加 caller-owned lock/membership/complete primitive |
| pure observation oracle | `observation_capture.py` | 提取无 repository side effect 的 semantic-draft builder 和 identity materializer |
| trace envelope/outbox | `stage_trace.py`、`trace_outbox.py` | 增加 scope-aware in-transaction exact primitive |
| Phase 1F.2 schema | release registry v3 + 已发布 migrations | 只验证/消费，不修改 |
| Phase 1G result contracts | `phase1g_contract.py` | G4 继续生成 stable result/attempt receipts |

### 4.2 当前缺口

1. `PostgresControlBindingRepository`、`PostgresCaptureBatchRepository` 和 `PostgresTraceOutboxRepository`
   的 public 方法各自管理 transaction，不能被单 target writer 原子组合。
2. `observation_capture.py` 只有 in-memory repository，缺少 production-equivalent PostgreSQL writer。
3. 当前没有 observation header/version/lineage/stage/candidate 的 full exact readback repository。
4. Phase 1F.1 已将 lineage/candidate 拆为 identity + partitioned payload 表，但 writer 尚未实现双表原子插入。
5. 当前 outbox append 使用 repository-owned transaction；G3 需要在同一 caller cursor 中写入。
6. 当前 membership API 会单独提交；G3 必须在 observation transaction 内写入并推进 batch row version。
7. 当前没有 commit-response-loss 后的新连接完整 readback projection。

禁止用 in-memory repository、fixture writer、JSON-only 单行保存、直接 SQL 脚本或只写 observation header
替代上述缺口。

## 5. Architecture / 数据库权威与写入边界

### 5.1 G3 可写关系

单 target transaction 只允许写：

```text
app.advisory_source_revision_set
app.advisory_source_revision_member
app.advisory_selection_stage_trace_outbox
app.advisory_selection_stage_trace_delivery_event
app.advisory_signal_observation
app.advisory_signal_observation_version
app.advisory_signal_observation_lineage_identity
app.advisory_signal_observation_lineage_payload
app.advisory_signal_stage_evidence
app.advisory_signal_stage_candidate_identity
app.advisory_signal_stage_candidate_payload
app.advisory_capture_batch_evidence_membership
app.advisory_capture_batch                 # 仅 row_version/COMPLETE primitive
```

`app.advisory_signal_observation_lineage` 和 `app.advisory_signal_stage_candidate` 是 Phase 1F.1 compatibility
view，只允许 exact read，不允许 INSERT/UPDATE/DELETE。identity 与 payload 必须在同一 transaction 成对插入；
payload 通过 `decision_as_of_trade_date` 由 PostgreSQL 路由到已发布分区。

### 5.2 G3 不可写关系

- source availability event、DSE、selection artifact、package manifest；
- Selection、Paper、simulation、QMT、QE、模型或回测 schema；
- schema catalog/receipt/migration history；
- external CAS result store。

### 5.3 Schema truth

G3 不在每个 target 事务中重复执行完整 catalog verifier。G4 invocation 进入 writer 前消费 G1 已验证的
Phase 1F.2 schema receipt；G3 fixed SQL 若遇到 missing relation/column/partition/constraint，映射为稳定
`ADVISORY_PHASE1G_G3_SCHEMA_NOT_READY` 并回滚。不得 runtime `CREATE/ALTER`、猜测 legacy schema 或写 compatibility
view 作为 fallback。

## 6. G3 强类型输入与输出

### 6.1 `Phase1GTransactionalWriteRequest`

```text
schema_version = advisory_phase1g_transactional_write_request_v1
target_request_hash
phase1e_plan_id/hash
g2_target_projection_snapshot/hash
capture_batch_id
capture_request_hash
capture_attempt_no
expected_batch_row_version
capture_fencing_token
control_binding_event_hash
capture_plan_hash
trace_capture_context/hash
trace_capture_binding/hash
stage_trace_envelope/hash
observation_semantic_draft/hash
expected_rows/bytes
write_request_hash
```

构造时必须闭合：

- target/plan/scope/package/manifest/date 与 G2 snapshot 完全一致；
- batch request、control binding、capture plan 和 envelope 中三个 Phase 1E output slots 完整闭合；
- source set id/hash 与 G2 freeze intent 完全一致；
- envelope component evidence 与 G2 historical projection 完全一致；
- observation semantic draft 由 pure oracle 从同一 plan/envelope/binding 生成；事务外不得猜测数据库中的
  revision number、predecessor 或 observation/lineage identity；
- expected rows/bytes 覆盖完整 draft、全部候选以及固定 identity/version 开销，禁止 top-N 截断。

G3 不接受 loosely typed dict、selection rows、package ids 列表或手工组合多包输入。

### 6.2 `Phase1GObservationSemanticDraft`

```text
canonical_signal_header
semantic_observation_payload
semantic_observation_key
stage_semantic_rows[5]
candidate_semantic_rows[]
draft_row_count
draft_content_hash
```

draft 不含 `observation_revision_no`、`supersedes_observation_version_id`、`observation_version_id`、
`lineage_id` 或任何由这些 identity 派生的 child identity。它可在事务外完成并与当前 in-memory oracle 的
semantic hash 完全一致。

### 6.3 `Phase1GObservationRowBundle`

```text
canonical_signal_header
observation_version
lineage_identity
lineage_payload
stage_evidence_rows[5]
candidate_identity_rows[]
candidate_payload_rows[]
bundle_row_count
bundle_content_hash
```

事务内 identity materializer 接收 semantic draft、锁内解析出的 revision number 和 exact predecessor，生成最终
row bundle。builder/materializer 必须生成五个 stage：`alpha_raw`、`hmm_adjusted`、`risk_policy_adjusted`、
`selection_effective`、`advisory_model`。前四层来自 G2，`advisory_model` 按父级 observation contract 固定为
`UNAVAILABLE`，不能伪装成模型已运行。identity/payload rows 一一对应，日期、symbol 和 hash 闭合。

### 6.4 `Phase1GTargetCommitProjection`

```text
schema_version = advisory_phase1g_target_commit_projection_v1
target_request_hash/target_plan_hash
capture_batch_id/capture_request_hash/capture_attempt_no
capture_fencing_token
source_revision_set_id/hash/member_count/hash
control_binding_event_hash
trace_outbox_id/content_hash
canonical_signal_id
observation_version_id/content_hash/revision_no
lineage_id/content_hash
stage_evidence_ids/content_hashes
candidate_count/candidate_set_hash
target_membership_count/target_membership_hash
delivery_event_id/hash
post_commit_readback_hash
target_commit_projection_hash
```

`target_membership_count/target_membership_hash` 只覆盖当前 target 的三个确定性 membership，并按固定
`(membership_role, entity_id)` 排序；它不读取 batch COMPLETE receipt，也不混入其他 target 后续追加的 membership。
可变 `batch_row_version_after_write` 只进入 G4 attempt diagnostics，不进入稳定 projection/hash。该 projection 只描述
最终数据库事实，不包含 `INSERTED/REUSED` 等本次调用过程状态，因此首写、exact retry、commit-response-loss
readback 可重建同一 hash；后续 target 推进同 batch row version也不会改变既有 target projection。

## 7. Semantic Draft 与事务内 Identity Materialization

`observation_capture.py` 必须把当前 `_append_validated()` 分解为两个无 side effect 的步骤：

```python
build_observation_semantic_draft(
    *,
    plan: CapturePlan,
    envelope: StageTraceEnvelope,
    binding: TraceCaptureBinding,
) -> Phase1GObservationSemanticDraft

materialize_observation_row_bundle(
    *,
    draft: Phase1GObservationSemanticDraft,
    observation_revision_no: int,
    supersedes_observation_version_id: str | None,
    created_by_capture_batch_id: str,
) -> Phase1GObservationRowBundle
```

规则：

1. 两个函数均无数据库、时钟、文件系统、global repository 或随机输入。
2. `semantic_observation_key` 由 plan、source set、evidence bundle、trace content、policy 和完整 stage semantic
   rows 决定，但不含 revision/identity；不得用最终 child identity 反向构造 key 形成循环依赖。
3. exact retry 在锁内对每个历史 revision 重建其 semantic draft/key；找到一致 revision 后直接读回既有 row bundle，
   不重新分配 identity。
4. 只有没有历史 exact match 时，writer 才以 `max(revision_no)+1` 和唯一尾部 predecessor 调用 materializer。
5. decimal score 使用数据库 `NUMERIC(38,12)` 对应的明确量化规则；非有限值或越界直接失败。
6. identity rows 和 payload rows 使用同一 decision date、lineage id/stage id/symbol。
7. stage/candidate content hash 允许跨 observation identity 重复；不得恢复 v1 全局 UNIQUE 语义。
8. raw-empty/filtered-empty 生成完整五层 stage bundle，零 candidate 不是缺失 bundle。
9. single Alpha component 为 `NOT_APPLICABLE`；原生多 Alpha 保留 G2 的 FULL/PARTIAL/UNAVAILABLE 事实。

## 8. Caller-owned Repository Primitives

### 8.1 通用规则

所有 `*_in_transaction` / `*_in_tx`：

- 只接收 caller 提供的 `RealDictCursor` 或同一 connection 的明确 cursor；
- 不创建连接、不调用 `commit/rollback/close`、不读取 `.env`、不使用 `pg_pool`；
- 使用固定 schema、显式列和稳定 `ORDER BY`，禁止动态 SQL、`SELECT *`、latest/fallback query；
- exact retry 完整 readback；冲突抛 typed exception，不返回 bool/None；
- public repository 方法保留，作为创建 transaction 后调用 primitive 的薄包装。

single-target writer 接收专用 `transaction_connection_factory`：返回 `autocommit=false` 且退出 context 时不会隐式
commit 的连接，由 writer 唯一负责一次 `commit()` 或明确 pre-commit `rollback()`。不得直接复用当前可能在 context
退出时自动提交的 repository `ConnFactory` 作为 writer transaction ownership 契约。post-commit verifier 使用另一个
明确 `readonly=true` 的 connection factory；二者均由 G4 注入，不解析环境变量。

### 8.2 Control binding primitives

在 `control_binding.py` 增加：

```text
current_in_transaction(cur, chain_key)
append_in_transaction(cur, request)
get_or_append_exact_in_transaction(cur, desired_config)
read_exact_in_transaction(cur, binding_event_hash)
```

`get_or_append_exact` 在 `pg_advisory_xact_lock(hashtext(binding_chain_key))` 下读取有序完整 chain：无 current
创建 revision 1；current config/source/enabled 完全相同则复用；不同合法 config 创建 `revision+1` 并绑定 exact
predecessor。`enabled=true` 是 capture 配置事实，不是用户授权或审批。

Control binding 必须先于 capture batch 创建并提交，因为 batch 具有 FK；它不是 source/outbox/observation
target transaction 的一部分。G3 提供 primitive，G4 决定一次 invocation 中何时调用。

### 8.3 Capture primitives

在 `capture_foundation.py` 增加：

```text
lock_running_in_transaction(cur, batch_id, request_hash, row_version, fencing_token)
read_plan_exact_in_transaction(cur, batch_id, plan_hash)
add_membership_in_transaction(cur, membership, expected_row_version, fencing_token)
read_memberships_exact_in_transaction(cur, batch_id)
complete_in_transaction(cur, expected_row_version, fencing_token)
```

`lock_running` 使用数据库时钟检查 lease 尚未过期，并同时核对 status、row version、fencing、request hash。
membership exact reuse 不推进 row version；新增 membership 原子插入后推进一次 row version。G3 target writer 在每次
推进后使用返回的新 row version继续，不缓存旧版本。`complete_in_transaction` 供 G4 batch completion 使用，不在
单 target writer 内自动 COMPLETE。

### 8.4 Trace outbox primitives

在 `trace_outbox.py` 增加：

```text
read_exact_by_hash_in_transaction(cur, trace_content_hash)
read_exact_by_natural_key_in_transaction(cur, scope_aware_natural_key)
append_in_transaction(cur, envelope, persisted_binding, current_writer_binding)
read_delivery_chain_exact_in_transaction(cur, trace_outbox_id)
append_delivery_in_transaction(cur, request)
```

首写时 persisted/current binding相同。recovery 时 pure envelope校验使用 immutable predecessor
`persisted_binding`，transaction admission 使用当前 batch 的 `current_writer_binding`；两者必须具有相同
semantic request、scope、policy 和合法 predecessor chain。不得把当前 batch id/fencing 改写进旧 envelope。

### 8.5 Observation primitives

新增 `observation_capture_postgres.py`：

```text
lock_signal_in_transaction(cur, canonical_signal_id)
read_header_exact_in_transaction(cur, canonical_signal_id)
read_revision_chain_exact_in_transaction(cur, canonical_signal_id)
read_semantic_draft_for_revision_in_transaction(cur, observation_version_id)
append_materialized_bundle_in_transaction(cur, row_bundle, batch_id)
read_observation_bundle_exact_in_transaction(cur, observation_version_id)
read_observation_bundle_exact_readonly(cur, observation_version_id)
```

writer 直接写 identity/payload 基表；authority readback也直接读取基表，兼容 view 仅用于测试 parity read。每个 insert
都需随后从当前 transaction readback并与 materialized row逐字段一致。

`created_by_capture_batch_id` 是 identity materializer 的显式输入：首写和合法 successor 使用当前 batch；exact retry
重建历史 revision 时使用该 revision 已持久化的创建 batch；recovery 复用旧 observation 时不得把当前 batch 改写进旧
immutable row。事务内 exact read保留行锁；normal post-commit 和 commit-response-loss 使用独立 readonly connection
上的无锁 exact read，禁止在 read-only transaction 中执行 `FOR UPDATE/FOR KEY SHARE`。

## 9. 单 Target 事务算法

### 9.1 前置事实

进入 G3 target transaction 前必须已存在：

- G1/G2 validated request、Phase 1E plan和 G2 target snapshot；
- 已提交的 exact control binding；
- 已创建并 acquire 的 RUNNING capture batch；
- 当前 batch 下 exact capture plan；
- materialized real `TraceCaptureContext`、binding、envelope 和 semantic draft。

这些是业务输入，不是人工门禁。G3 不自行创建缺失事实，也不猜测替代值；G4 负责调用前编排。

### 9.2 固定锁顺序

所有 writer 使用相同顺序避免死锁：

1. capture batch row `FOR UPDATE`；
2. capture plan、control binding `FOR KEY SHARE`；
3. source revision set/member exact freeze/readback；
4. scope-aware outbox natural key / trace hash advisory lock与 row lock；
5. `pg_advisory_xact_lock(hashtext(canonical_signal_id))`；
6. canonical signal header 和 ordered revision chain；
7. observation child rows；
8. memberships；
9. delivery chain；
10. capture batch final row/readback。

禁止不同 primitive 自行改变锁顺序或打开第二连接。

### 9.3 事务步骤

```text
BEGIN
  SET TRANSACTION ISOLATION LEVEL READ COMMITTED
  lock/validate RUNNING batch
  read exact capture plan + control binding
  validate G3 request against G2 snapshot
  freeze/read exact source revision set
  read or append/reuse exact scope-aware outbox
  acquire canonical signal advisory lock
  read exact header and full ordered revision chain
  reconstruct historical semantic drafts and select exact retry / legal successor / conflict
  for successor only, materialize final row bundle from locked next revision + predecessor
  insert or reuse canonical signal header
  insert or reuse observation version
  insert/read lineage identity + payload
  insert/read five stage evidence rows
  insert/read candidate identity + payload rows
  add SOURCE_REVISION_SET membership
  add TRACE_OUTBOX membership
  add OBSERVATION_VERSION membership
  append/reuse OBSERVATION_WRITTEN delivery
  retain latest row version returned by new membership operations
  read all rows again inside transaction
COMMIT exactly once
open new read-only connection
read complete committed base-table bundle, current-batch memberships and delivery
build Phase1GTargetCommitProjection
```

事务隔离固定为 `READ COMMITTED`，配合 row/advisory locks和数据库唯一约束；不使用长时间
`REPEATABLE READ` 包住外部计算。G2 projection和 pure semantic draft在开事务前完成；事务内 identity materialization
只使用已锁定 revision chain做确定性短计算，其余仅为 bounded validation、fixed SQL 和 DML。

## 10. Observation Identity、Retry 与 Successor

### 10.1 Canonical signal header

相同 `canonical_signal_id` 的 header 必须逐字段相等。PK 存在但 scope/package/date/calendar/runtime identity 任一
不同即 conflict，不允许 UPDATE 或另建随机 id。

### 10.2 Exact retry

在 canonical signal lock 内读取完整 revision chain，逐 revision 读取：

- observation version全部字段；
- exact lineage identity/payload；
- 五个 stage evidence；
- 每层所有 candidate identity/payload；
- source/outbox immutable binding。

任一历史 revision 重建的 semantic draft/key 与请求完全一致后，还必须完整 readback该 revision 的 materialized row
bundle并验证全部 identity/payload/hash；一致才复用，即使其后已有其他合法 revision。只比较最新 revision、content
hash、PK 或 row count均不算 exact retry。当前 capture batch 的 memberships 和 delivery 在选定 observation revision
之后按 §12 校验/追加；恢复批次尚无 membership 不能阻止 observation exact reuse。

### 10.3 Legal successor

仅当 canonical economic identity不变，source revision/evidence/stage content发生合法变化，且没有任何历史 revision
exact match时，才在锁内以 `max_revision_no + 1` 和唯一当前尾部物化 row bundle；
`supersedes_observation_version_id` 指向该尾部。数据库 unique/predecessor trigger 与 writer full readback共同防止 fork。

### 10.4 Conflict

- 同 revision/id 不同内容；
- header economic identity变化；
- source set、outbox、lineage 或 child rows任一不闭合；
- 两个 writer竞争产生非唯一 successor；

均映射 typed conflict 并回滚当前 transaction，不尝试覆盖、删除或自动重编号。

## 11. Outbox 与 Recovery

### 11.1 First attempt

scope-aware natural key不存在时写入当前 envelope。natural key包含 selection/package/manifest/date/policy/scope，
不包含 attempt。相同 trace hash或 natural key存在时必须完整 readback；相同则 reuse，不同则 conflict。

### 11.2 Recovery

- predecessor outbox + observation 均存在：完整复用，给当前 batch增加 memberships/delivery；
- predecessor outbox存在但 observation不存在：消费 immutable predecessor envelope，observation 的
  `created_by_capture_batch_id` 使用当前 recovery batch；
- outbox不存在：当前 recovery batch可首次写入；
- natural key相同但内容不同：conflict；
- 已存在 observation不得创建重复 revision。

G3 只执行调用者已经确定的合法 predecessor关系；选择 terminal predecessor、expire/recover batch属于 G4。

### 11.3 No hidden retry

G3 writer一次调用只尝试一次数据库 transaction，不 sleep、不 backoff、不自动重开业务 transaction。
serialization/deadlock/lease/fencing失败显式返回；G4 的下一次正常调用依据 DB exact facts收敛。

## 12. Membership、Delivery 与 Batch Row Version

每个成功 target最终存在且仅存在以下 membership：

```text
SOURCE_REVISION_SET  -> source_revision_set_id/hash
TRACE_OUTBOX         -> trace_outbox_id/content_hash
OBSERVATION_VERSION  -> observation_version_id/content_hash
```

相同 `(batch, role, id)` exact retry复用；hash不同 conflict。新增 membership 后 batch row version逐次递增，writer
始终使用数据库返回值；没有独立的额外 row-version DML。target membership hash只对请求确定的三个明细重算，batch
总 membership count/hash和 COMPLETE receipt由G4在全部targets结束后生成。delivery event
固定 `OBSERVATION_WRITTEN`；按现有 `TraceDeliveryEventType` 和 repository chain contract，该事件是终态，
sequence/predecessor/hash必须完整闭合，exact retry不得追加第二个相同语义终态事件。

若 outbox 已有 exact `OBSERVATION_WRITTEN` 终态事件（包括 recovery），writer完整验证其 observation identity/hash后
复用；不得因当前 capture attempt不同而创建第二个终态事件。当前 batch 对该 target 的提交归属由三个 exact
memberships证明，不依赖改写旧 delivery payload。

若 delivery chain为空，则追加 sequence 1；若尾部为 `OBSERVATION_WRITE_FAILED`，允许按现有 predecessor/hash规则
追加下一 sequence的 `OBSERVATION_WRITTEN`；若链内已有 `OBSERVATION_WRITTEN`，只能完整验证其稳定 payload中的
`trace_outbox_id`、`observation_version_id`、`observation_content_hash` 后复用。成功事件稳定 payload不得包含当前
batch id、fencing token或可变row version；这些attempt事实由capture membership和G4 attempt receipt表达。

单 target writer不自动将 batch转为 COMPLETE。G4 在全部 targets成功后调用 `complete_in_transaction()`，按有序
memberships重算 membership hash和 capture receipt。

## 13. Commit 与 Post-commit Readback

### 13.1 Pre-commit failure

任一步在 `commit()` 返回前明确失败：rollback当前 transaction，抛原始 typed reason；source freeze、outbox、
observation、children、memberships、delivery 和 row-version推进必须全部零残留。

### 13.2 Commit response loss

`commit()` 抛出连接/网络异常时提交状态未知：

1. 不在原连接上声称 rollback成功；
2. 关闭原连接；
3. 使用新只读连接，按 write request中的 deterministic identities读取完整 final bundle；
4. 完全一致则返回正常 `Phase1GTargetCommitProjection`；
5. 判断“当前 transaction 已提交”必须同时包含当前 batch 的三个 exact target memberships和已验证指向同一
   observation 的 `OBSERVATION_WRITTEN` delivery；不能仅因旧 source/outbox/observation/delivery 已存在就判定成功，
   也不能要求可被后续 target合法推进的 batch row version仍等于本次返回值；
6. 明确不存在本次应新增的当前-batch facts，且所有可复用 immutable facts一致时，返回原 commit failure；
7. 部分存在、冲突或无法确认则返回 `ADVISORY_PHASE1G_G3_COMMIT_STATE_UNKNOWN`。

不得把 partial rows当 success，也不得删除可能已提交事实。

### 13.3 Normal post-commit verification

即使 `commit()` 正常返回，也必须用新只读连接完整 readback；不一致返回
`ADVISORY_PHASE1G_G3_POST_COMMIT_VERIFY_FAILED`。已提交事实保留，G4 attempt如实记录，下次正常调用通过 exact
retry重建同一 stable result。

## 14. Concurrency 与 Conflict Matrix

| 场景 | 预期结果 |
|---|---|
| 两 writer同 request/同 batch/同旧row version | 一个成功，一个CAS row-version conflict且零DML；正常重跑读取新版本后exact reuse |
| exact retry使用当前row version | 零重复事实，重建同target commit projection |
| 两 writer同 signal/不同合法 evidence | 串行形成唯一 predecessor chain，无 fork |
| 同 revision identity不同内容 | 一个成功，一个 typed conflict/rollback |
| 同 outbox natural key不同 trace | scope-aware outbox conflict |
| stale row version/fencing | transaction开始即失败，零 DML |
| lease在锁前已过期 | lease expired，零 DML |
| transaction执行期间 lease越过时间 | 以锁定时数据库时间校验结果为本次事实，不在中途二次改变语义 |
| deadlock/serialization failure | 显式失败，无内部重试 |
| commit response loss但完整 rows存在 | 新连接readback成功，返回同 commit projection |
| commit response loss且部分/冲突 rows | commit state unknown，禁止假成功 |

## 15. Capacity 与 Valid No-candidate

- G3 只消费 G2 完整 rows/bytes和 Phase 1E disposition，不重新估算或缩减 workload。
- 超出 target/policy upper bounds时在开事务前失败；不得在 transaction内截断 candidates/stages。
- raw-empty/filtered-empty 均写完整 observation version、lineage和五层 stage；candidate为零或保留 exclusion rows取决于
  G2事实。
- source/data unavailable没有 G2成功 snapshot，不能进入 G3，也不能转换为 valid-no-candidate。
- `DEFERRED/INSUFFICIENT` target不执行 G3 DML；该判断由已冻结 plan事实自动完成，不是人工审批。

## 16. Error 与 Logging Contract

G3 稳定 reason至少包括：

```text
ADVISORY_PHASE1G_G3_INPUT_INVALID
ADVISORY_PHASE1G_G3_SCHEMA_NOT_READY
ADVISORY_PHASE1G_G3_BATCH_NOT_RUNNING
ADVISORY_PHASE1G_G3_BATCH_ROW_VERSION_CONFLICT
ADVISORY_PHASE1G_G3_FENCING_INVALID
ADVISORY_PHASE1G_G3_LEASE_EXPIRED
ADVISORY_PHASE1G_G3_CONTROL_BINDING_CONFLICT
ADVISORY_PHASE1G_G3_CAPTURE_PLAN_CONFLICT
ADVISORY_PHASE1G_G3_SOURCE_REVISION_CONFLICT
ADVISORY_PHASE1G_G3_TRACE_OUTBOX_CONFLICT
ADVISORY_PHASE1G_G3_OBSERVATION_CONFLICT
ADVISORY_PHASE1G_G3_CHILD_ROW_CONFLICT
ADVISORY_PHASE1G_G3_MEMBERSHIP_CONFLICT
ADVISORY_PHASE1G_G3_DELIVERY_CONFLICT
ADVISORY_PHASE1G_G3_CAPACITY_EXCEEDED
ADVISORY_PHASE1G_G3_COMMIT_FAILED
ADVISORY_PHASE1G_G3_COMMIT_STATE_UNKNOWN
ADVISORY_PHASE1G_G3_POST_COMMIT_VERIFY_FAILED
ADVISORY_PHASE1G_G3_UNEXPECTED_ERROR
```

日志仅在 target transaction开始、commit结果、post-commit失败和 unexpected error输出。字段限定为 plan/request/
batch/scope/package/observation hash前缀、transaction stage、reason code和 exception type。禁止输出 DSN/password、
完整 candidates、trace payload、component evidence、模型路径或无价值逐行 INSERT 日志。unexpected exception 保留
一次后台 traceback并 `raise ... from exc`；不得返回 `None` 或空 projection。

## 17. 文件范围与依赖边界

### 17.1 计划代码文件

```text
backend/services/advisory_phase1/phase1g_transactional_writer.py       # new
backend/services/advisory_phase1/observation_capture_postgres.py       # new
backend/services/advisory_phase1/observation_capture.py                # semantic draft + identity materializer
backend/services/advisory_phase1/source_revision_postgres.py           # caller-owned freeze/exact/readonly primitives
backend/services/advisory_phase1/control_binding.py                    # additive tx primitives
backend/services/advisory_phase1/capture_foundation.py                 # additive tx primitives
backend/services/advisory_phase1/trace_outbox.py                       # additive tx primitives
backend/services/advisory_phase1/stage_trace.py                         # multi Alpha decimal parity correction
backend/services/advisory_phase1/phase1g_contract.py                   # additive G3 request/projection DTO
backend/tests/advisory_phase1/test_phase1g_g3_transactional_writer.py
backend/tests/advisory_phase1/test_phase1g_g3_transactional_writer_postgres.py
backend/tests/advisory_phase1/test_phase1g_g3_import_boundary.py
backend/tests/advisory_phase1/test_stage_trace.py
docs/architecture/advisory_phase1g_g3_transactional_postgresql_writer_f2_design_20260715.md
docs/architecture/advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md
```

若实现需要超出该范围，必须先更新设计并说明原因；不得静默扩到 Selection、模拟盘、Paper 或 schema migration。

### 17.2 禁止依赖

G3 production modules禁止 import：

```text
backend.services.selection_center
backend.services.strategy_package inference/validator/asset loader
backend.services.simulation_runtime
backend.services.paper_trading
backend.infra.qmt*
backend.services.quantevolver / rdagent*
backend.qlib_exporter / rl_execution
backend.services.advisory_phase1.release_schema_apply_postgres
backend.db.pg_pool default/global connection
```

数据库连接仅由 caller 注入。G3 不解析 `.env`；G4 CLI 才按用户指定 target和 env file构造 exact connection
factory，不猜测 DEV/production。

## 18. Implementation Plan / 实施方案

### G3A：Contracts、Semantic Draft 与 Identity Materializer

- G3 request/commit projection；
- pure observation semantic draft extraction与锁内参数化 identity materializer；
- single/multi Alpha、raw/filtered-empty parity；
- deep immutable/hash/count tests。

### G3B：Repository Transaction Primitives

- control binding、capture、outbox caller-owned primitives；
- observation exact read/insert双表 writer；
- public wrapper compatibility。

### G3C：Single-target Writer

- fixed lock order；
- source/outbox/observation/membership/delivery transaction；
- retry/successor/recovery；
- commit uncertainty/post-commit readback。

### G3D：Disposable PostgreSQL Acceptance

- PostgreSQL 16 production migrations；
- concurrency、fault injection、rollback、zero residue；
- import/scope/design compliance closure。

每批必须完成自己的全部设计条目，禁止以 fixture-only、in-memory-only、header-only 或无 child readback版本冒充 G3
完成。

## 19. 验证方案

### 19.1 L0 Static

- changed path仅限 §17.1；
- 禁止依赖、global pool、`.env`、DDL/dynamic SQL/`SELECT *`扫描；
- `*_in_transaction` 无 commit/rollback/第二连接；
- 无 role/RBAC/approval/backup/manual bypass；
- F2 validator、Ruff、Black、compileall、`git diff --check`、ownership/guardrail。

### 19.2 L1 Pure

1. G3 request所有跨对象 identity/count/hash正反例。
2. semantic draft/key与现有 in-memory observation oracle golden parity。
3. 给定 revision/predecessor 后 identity materializer 与现有 oracle row bundle parity。
4. identity/payload一一对应、五 stage、candidate hash、decimal boundary。
5. single Alpha N/A、multi Alpha full/degraded、raw/filtered-empty。
6. target commit projection从相同 final facts跨重跑同 hash。
7. 不可变嵌套和 `model_copy(update=...)` 重载漂移失败。
8. reason/context/log redaction与 exception chain。

### 19.3 L2 Disposable PostgreSQL 16

必须应用 production migration链和 registry v3，不允许简化表替代核心关系：

1. 首写完整 source -> outbox -> observation -> lineage identity/payload -> 5 stages -> candidate
   identity/payload -> 3 memberships -> delivery。
2. single Alpha、native multi Alpha、raw-empty、filtered-empty正向贯通。
3. exact retry零新增 observation/version/stage/candidate/outbox；其他target已推进batch时commit projection仍相同。
4. HMM/风险/行情 evidence合法变化创建唯一 successor。
5. identity/payload任一 insert故障使单 target全部零残留。
6. membership、delivery、per-membership batch row-version故障全部回滚，无额外版本推进。
7. recovery四种 predecessor outbox/observation组合。
8. stale row-version、fencing、lease、request/control/plan conflict零DML。
9. 同 request双 writer一成功一CAS冲突，正常重跑收敛；不同 evidence形成串行 successor；冲突不 fork。
10. commit response loss：committed、not committed、partial/unknown三类。
11. commit response loss在旧 immutable facts已存在、当前 batch facts缺失时仍判定not committed。
12. post-commit full readback tamper/missing child fail-closed。
13. compatibility view parity但无 view DML；分区 payload落入正确 decision-date partition。
14. public repository wrapper compatibility。
15. query spy证明一个 target transaction只用一个显式ownership连接，post-commit readback单独只读连接，context
    exit不隐式二次commit。

### 19.4 DEV 与 Production 边界

G3 代码完成只要求 L0-L2。真实 DEV transactional rollback/persistent evidence属于 G5；production DML/激活不在
G3。设计/代码合入、DEV evidence、production DML和runtime activation必须分别报告。

## 20. 正向可达性

| 场景 | 合法事实 | 自动结果 |
|---|---|---|
| 首次单 Alpha | RUNNING batch + G2 single snapshot | 完整原子写入，component N/A |
| 首次多 Alpha | frozen parent/component evidence | 完整原子写入，保留 component capability |
| raw-empty | positive universe + exact empty artifact | 完整零候选 observation |
| filtered-empty | formal stage exclusions to zero | 保存真实 exclusions/component facts |
| exact retry | 当前row version + 所有target DB rows逐字段一致 | 零重复 rows，同 commit projection |
| 合法 evidence变化 | economic identity相同、evidence更新 | 唯一 successor revision |
| recovery outbox+observation存在 | predecessor chain合法 | 复用事实，增加当前 batch membership |
| recovery仅outbox存在 | predecessor chain合法 | 消费旧 envelope，当前 batch创建 observation |
| commit响应丢失且已提交 | 新连接完整readback一致 | 返回同 commit projection |

所有正向路径只依赖有效数据和数据库事实，无人工审批、角色、手工改库、额外备份或 bypass。

## 21. 风险与对策

| 风险 | 后果 | 设计对策 |
|---|---|---|
| repository各自commit | partial observation | caller-owned primitives + one commit |
| 向compatibility view写入 | schema/partition错误 | identity/payload基表显式 SQL |
| child hash全局去重 | 跨observation串行 | identity-scoped PK，禁止恢复v1 UNIQUE |
| HMM/top-k语义重算 | 业务漂移 | 只消费G2 frozen rows |
| recovery绑定当前batch进旧envelope | trace hash改变 | persisted/current binding双轨 |
| 只看最新revision | 重复历史事实 | 遍历完整revision chain |
| commit异常直接rollback/重试 | 双写或假失败 | 新连接full readback，无内部重试 |
| row version缓存 | fencing失效 | 每次membership DML使用RETURNING新版本；并发旧版本显式冲突后正常重跑 |
| target projection包含batch总状态 | 后续target使既有hash漂移 | 只hash该target三个membership；总receipt留给G4 |
| 事务外预分配revision identity | 并发重复/fork | 事务外semantic draft，锁内materialize identity |
| connection context隐式commit | commit uncertainty误判/二次commit | writer专用显式transaction ownership契约 |
| 一个target长事务 | lock放大 | semantic draft在事务前，锁内仅短identity materialization与bounded SQL |
| 额外审批/备份门禁 | 正常流程不可达 | 仅自动事实校验，静态禁止扫描 |

## 22. Rollout、Rollback 与 Production Gates

G3 无 migration、dependency、API、UI、scheduler、startup或runtime activation。代码回滚只需回滚 G3代码；未被
G4调用前没有生产行为。G3代码合入后状态必须保持：

```text
production_ddl_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
dev_dml = not_executed
production_dml = not_executed
runtime_activation = none
```

不得因为 disposable PostgreSQL 通过就宣称 DEV/production已验证或激活。

## 23. Design Acceptance Index

- F-768：G3只实现transaction primitives和单target writer，不实现G4 service/CLI或运行激活。
- F-769：G3只消费G2 frozen snapshot，不调用Selection、推理、策略包validator或asset loader。
- F-770：一个target的source/outbox/observation/children/membership/delivery单transaction原子提交。
- F-771：control binding与RUNNING batch是前置已提交事实，不伪装进入target原子事务。
- F-772：所有in-transaction primitives无连接创建、commit、rollback、global pool或env解析。
- F-773：existing public repository API保持兼容，作为transaction primitive薄包装。
- F-774：writer只写已发布Phase1F.2关系，无migration、runtime DDL或legacy fallback。
- F-775：lineage/candidate写identity+partitioned payload基表，不对compatibility view执行DML。
- F-776：G3 request与G2/Phase1E/batch/binding/plan/envelope/semantic draft逐字段闭合。
- F-777：pure semantic draft及参数化identity materializer与现有observation oracle parity；事务外不预分配revision。
- F-778：五层stage和全部candidate/component rows完整保存，不截断、不抽样。
- F-779：single Alpha、native multi Alpha、raw-empty、filtered-empty语义完整保留。
- F-780：固定lock order和单connection bounded transaction，防止死锁/第二连接。
- F-781：source revision freeze与target DML同transaction，header/member exact readback。
- F-782：scope-aware outbox首写/exact reuse/conflict完整闭合。
- F-783：recovery区分persisted binding与current writer binding，不改写旧envelope。
- F-784：canonical signal header exact reuse；identity conflict fail-fast。
- F-785：exact retry遍历完整revision chain并readback全部children，不能只看latest/PK/count。
- F-786：合法evidence变化形成唯一单前驱successor，同revision冲突不fork。
- F-787：lineage/stage/candidate每次insert后逐字段readback，identity/payload一一对应。
- F-788：三个target membership exact reuse/insert与per-new-membership batch row version逐次闭合；稳定projection不含可变总状态。
- F-789：OBSERVATION_WRITTEN delivery chain exact append/reuse，不生成重复terminal event。
- F-790：pre-commit任一故障使当前target全部零残留。
- F-791：commit response loss用新连接full readback，区分committed/not committed/unknown。
- F-792：normal post-commit也执行新连接full readback，失败不伪造成功或删除事实。
- F-793：同batch首写/retry/commit-response-loss重建同projection hash；recovery复用immutable identities并闭合新batch facts。
- F-794：一次调用无sleep/backoff/自动业务retry，正常重跑依据DB事实收敛。
- F-795：stable reason、脱敏context、exception chain和有价值日志，无静默错误。
- F-796：capacity、DEFERRED/INSUFFICIENT和valid-no-candidate严格消费冻结事实，无新增人工门禁。
- F-797：disposable PostgreSQL使用production migration链，覆盖并发/fault/rollback/partition/view parity。
- F-798：无角色、RBAC、审批、授权、人工确认、备份门禁、manual bypass或人工改库。
- F-799：设计/代码合入、DEV/production DML和runtime activation分开报告；G3后进入G4。

## 24. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-768 | §3、§18 | changed-path/import boundary | local_verified | none |
| F-769 | §4、§17.2 | transitive import denylist | local_verified | none |
| F-770 | §5、§9 | transaction query spy + rollback fault matrix | local_verified | none |
| F-771 | §8.2、§9.1 | binding/batch precondition tests | local_verified | none |
| F-772 | §8.1 | cursor spy/no-commit/no-pool tests | local_verified | none |
| F-773 | §8 | existing wrapper regression | local_verified | none |
| F-774 | §5.3、§22 | SQL/static no-DDL scan | local_verified | none |
| F-775 | §5.1、§8.5 | base-table DML + view parity/query spy | local_verified | none |
| F-776 | §6.1-6.3 | typed cross-identity negative matrix | local_verified | none |
| F-777 | §7、§9.3 | semantic draft/materializer/in-memory golden parity | local_verified | none |
| F-778 | §6.2-6.3、§7 | full stage/candidate no-truncation test | local_verified | none |
| F-779 | §7、§15 | single/multi/raw/filtered positive matrix | local_verified | none |
| F-780 | §9.2 | fixed lock-order/one-connection spy | local_verified | none |
| F-781 | §8.1、§9.3 | source freeze rollback/exact retry | local_verified | none |
| F-782 | §8.4、§11.1 | outbox first/retry/conflict tests | local_verified | none |
| F-783 | §8.4、§11.2 | predecessor/current binding recovery tests | local_verified | none |
| F-784 | §10.1 | header exact/conflict tests | local_verified | none |
| F-785 | §10.2 | non-latest exact historical revision retry | local_verified | none |
| F-786 | §10.3-10.4 | successor/two-writer/fork negatives | local_verified | none |
| F-787 | §8.5、§9.3 | child/identity/payload tamper tests | local_verified | none |
| F-788 | §8.3、§12 | membership/row-version retry tests | local_verified | none |
| F-789 | §8.4、§12 | delivery sequence/predecessor tests | local_verified | none |
| F-790 | §13.1、§19.3 | every-write-node rollback zero-residue | local_verified | none |
| F-791 | §13.2 | commit-response-loss three-way matrix | local_verified | none |
| F-792 | §13.3 | post-commit missing/tampered child matrix | local_verified | none |
| F-793 | §6.4、§11.2 | same-batch hash parity + recovery immutable-identity parity | local_verified | none |
| F-794 | §11.3 | no sleep/backoff + normal rerun | local_verified | none |
| F-795 | §16 | reason/context/caplog/traceback redaction | local_verified | none |
| F-796 | §15 | capacity/disposition/zero-candidate matrix | local_verified | none |
| F-797 | §19.3 | production-migration PostgreSQL 16 matrix | local_verified | none |
| F-798 | §2、§3.2、§20 | source/static forbidden-gate scan | local_verified | none |
| F-799 | §1、§19.4、§22、§25 | separated-state review | local_verified | none |

## 25. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：source/outbox/observation/identity+payload/stage/candidate/membership/delivery和full
  readback全部纳入G3，不接受header-only/in-memory-only/fixture-only。
- [x] `no_silent_error`：pre/post commit、commit unknown、child conflict和store boundary均有稳定失败，不返回假成功。
- [x] `no_business_semantic_drift`：只消费G2 frozen rows，不重排候选、不重算HMM/风险、不改变single/multi Alpha。
- [x] `no_unrequested_gate_or_approval`：只有schema/hash/lease/fencing/transaction自动事实，无角色/审批/备份。
- [x] `positive_path_satisfiable`：首写、retry、successor、recovery和commit-response-loss均有合法自动贯通路径。
- [x] `database_connection_truth`：G3只接收caller注入连接，不读取或猜测`.env`，不回退DEV/production。
- [x] `transaction_boundary_truth`：control binding/batch前置事实、显式connection ownership和单target evidence
  transaction边界明确。
- [x] `partition_schema_truth`：lineage/candidate写identity+payload基表，compatibility view只读。
- [x] `research_isolation`：仅Advisory历史研究证据，不触碰Selection、模拟盘、Paper、交易或回测训练。
- [x] `state_reporting_truth`：设计、代码、DDL/DML、DEV/production和runtime activation分别报告。

## 26. 退出条件与下一阶段

G3 详细设计进入代码阶段前必须满足：

1. F2 validator通过，F-768 至 F-799 共32项无未批准缺口。
2. 父级G3范围、G2 snapshot契约、Phase 1F.2 identity/payload schema前后一致。
3. transaction lock/order、retry/successor/recovery和commit uncertainty均有正反例。
4. 所有保留技术条件在合法数据下可自动通过，不形成不可达门禁。
5. 无migration、global pool、runtime activation、角色/RBAC/approval/backup/manual bypass。
6. 用户已确认并完成G3A-G3D代码实现；PR #2178与merge commit `71d3486d`已经合入，DEV/production
   数据库操作和 runtime activation 均未执行并继续分开报告。

G3已经完整实现并通过L0-L2后合入。下一阶段是G4 Service、CLI And Recovery orchestration。G3合入不代表
G4/G5、DEV evidence、production DML或runtime activation完成；G4必须在独立F2详细设计通过后实施。
