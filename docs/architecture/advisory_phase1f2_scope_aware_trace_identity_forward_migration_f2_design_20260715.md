# AIstock Advisory Phase 1F.2 Scope-aware Trace Identity Forward Migration F2 详细设计

## 1. Background / 文档定位与当前状态

本文是 Phase 1F.1 observation partition/hash schema 与 Phase 1G observation capture DML 之间的独立
实施级详细设计，承接：

```text
Phase 1F v1 release schema
  -> Phase 1F.1 global identity + monthly payload partition layout
  -> Phase 1F.2 scope-aware trace outbox/gap identity
  -> Phase 1G source replay + observation capture DML
```

Phase 1F.1代码已由PR `#2129`合入，并于2026-07-15在DEV和production完成
plan/apply/new-verify/new-exact-reapply；两个目标均为
`COMPATIBLE/COMPATIBLE/downstream_ready=true`，最终catalog fingerprint为
`106af55734c6ec7bb0b0dd4e438bcb780d672be95220aead686ec6f4b6c3e627`。该事实准确证明Phase 1F.1
定义的partition/hash scope已经发布，不证明后续发现的trace identity也已修正。

Phase 1G开工审计确认两个相互关联的问题：

1. `app.advisory_selection_stage_trace_outbox` 已持久化`admission_scope_id/hash`，但唯一键仍是
   `(selection_run_id, package_id, manifest_sha256, decision_as_of_trade_date, capture_policy_hash)`。
   同一Selection证据服务两个独立Program/scope时，第二条合法trace会被拒绝。
2. `ExpectedTraceIdentity`同时被outbox reconciliation和`TraceCaptureGap`使用，但
   `app.advisory_capture_gap`没有scope列。即使只放宽outbox，失败证据仍会跨Program互斥或串线。

任务分级为`T3 / F2`。本阶段修改Advisory Phase 1自有schema、release registry和Advisory-owned
repository identity，不修改Selection、策略包推理、荐股列表、模拟盘、Paper、QMT、QE/RD-Agent、Qlib
或训练路径。

当前状态：

```text
design_status = design_ready
implementation_status = not_started
phase1f1_dev_ddl = applied_and_verified_2026_07_15
phase1f1_production_ddl = applied_and_verified_2026_07_15
phase1f2_code = not_started
phase1f2_dev_ddl = not_executed
phase1f2_production_ddl = not_executed
phase1g_business_code = blocked_pending_phase1f2_dev_ready_receipt
business_dml = none
runtime_activation = none
role_or_approval = none
extra_database_backup = none
```

## 2. Goals / 目标

Phase 1F.2必须同时实现：

1. 同一Selection/package/date/policy在不同权威admission scopes下可以形成独立outbox。
2. 同scope exact retry继续命中同一outbox；capture batch、attempt和fencing不得进入natural key。
3. 新gap identity包含scope id/hash；不同scope的相同reason可独立持久化。
4. 历史gap行保持原NULL scope和原`gap_content_hash`，不猜测、不回填、不重写。
5. 新写API只接受scope-aware v2 identity，不允许optional scope或legacy silent fallback。
6. v3 release registry精确冻结新增/替换对象、migration SHA和v2 predecessor closure。
7. 当前exact v2 catalog在v3 plan中被识别为`PARTIAL_ADDITIVE`且只计划order 90；未知漂移仍拒绝。
8. order 90在单一executor-managed transaction内完成，失败全部回滚，不留下半迁移结构。
9. 合法结构自动通过，无角色、RBAC、审批、授权链、人工确认、manual bypass或额外备份。
10. DEV与production代码/DDL/业务DML/runtime状态独立报告；Phase 1F.2不执行任何业务DML。

## 3. Scope And Non-goals / 范围与非目标

### 3.1 In Scope

- 新增order 90 frozen migration。
- v3 release registry、默认registry指针和v2 predecessor loader泛化。
- outbox scope-aware unique constraint与repository natural key。
- gap nullable scope columns、pair check、legacy/v2 partial unique indexes和typed row parser。
- legacy/v2 trace identity DTO、gap hash兼容与显式selection lookup key。
- release plan/apply/verify/exact-reapply contract测试。
- pure、disposable PostgreSQL、transactional DEV和persistent DEV DDL验证设计。
- 父设计、Phase 1G设计和蓝图状态同步。

### 3.2 Non-goals

- 不实现Phase 1G target request/plan、source replay、historical projection或observation writer。
- 不实现Phase 1G caller-owned transaction primitives、result/attempt store或业务DML。
- 不修改Selection Center、strategy package、inference、模拟盘、Paper、QMT、QE、RD-Agent或Qlib代码。
- 不重新验证策略包资产、模型或入库状态。
- 不新增API、UI、scheduler、startup hook、后台worker或runtime activation。
- 不新增用户、角色、RBAC、审批、授权、人工复核、人工DB修改或DDL前全库备份要求。
- 不读取回测、Paper或模拟盘数据，不生成Parquet，不训练模型。
- 不在本设计阶段执行DEV或production DDL；后续数据库执行必须按用户当次明确指令单独进行。

## 4. Existing Authority And Exact Gaps / 现有权威与精确缺口

### 4.1 当前权威实现

| 能力 | 权威文件 | 当前事实 |
|---|---|---|
| release contract | `release_schema_contract.py` | 默认v2 registry；predecessor loader硬编码v2只能指向v1 |
| release verifier | `release_schema_verify_postgres.py` | predecessor repair当前被`has_cutover_predecessor`的table->view特征限制 |
| release executor | `release_schema_apply_postgres.py` | migration按order、SHA和transaction mode执行 |
| registry | `release_schema_registry/advisory_phase1_dataset_foundation_v2.json` | orders 10/20/30/40/50/55/60/70/80 |
| outbox | `trace_outbox.py` | SQL与in-memory natural key均缺scope hash |
| gap | `capture_foundation.py` | DTO/hash/table insert/readback均无scope |
| scope authority | `advisory_phase0a/handoff.py` | scope identity包含audit target，audit target绑定Program |
| admission binding | `stage_trace.py` | `TraceCaptureBinding`已强制携带scope id/hash |

### 4.2 当前约束

outbox当前唯一约束：

```text
advisory_selection_stage_trac_selection_run_id_package_id_m_key
UNIQUE(selection_run_id, package_id, manifest_sha256,
       decision_as_of_trade_date, capture_policy_hash)
```

gap当前唯一约束：

```text
advisory_capture_gap_selection_run_id_package_id_manifest_s_key
UNIQUE(selection_run_id, package_id, manifest_sha256,
       decision_as_of_trade_date, capture_policy_hash, reason_code)
```

两者均把scope排除在identity之外，与多Program独立性冲突。`trace_content_hash`和`gap_content_hash`的全局
唯一性继续保留；它们用于完整payload冲突检测，不替代业务natural key。

### 4.3 调用面事实

当前`ExpectedTraceIdentity`、`PostgresTraceOutboxRepository`和`PostgresTraceCaptureGapRepository`仅由
`backend.services.advisory_phase1`及其测试消费。Selection Center、策略包推理、模拟盘和Paper不import
这些repository。Phase 1F.2必须用static transitive import/call-site测试继续证明该事实，不得仅按文件名推断。

## 5. Architecture, Authority And Isolation / 架构、权威与隔离

### 5.1 Scope authority

`admission_scope_id/hash`只来自Phase 0A handoff与Phase 1 capture binding，不接受调用方手写或从package/date
重新推导。`HandoffAdmissionScope` canonical identity包含`audit_target_id`，audit target绑定唯一
`program_id`，因此natural key不重复加入`program_id`。Phase 1G后续仍需验证plan program与scope映射。

### 5.2 Data direction

```text
Phase 0A handoff scope identity (read-only authority)
  -> TraceCaptureBinding scope id/hash
  -> ScopeAwareExpectedTraceIdentityV2
  -> advisory_selection_stage_trace_outbox INSERT
  -> advisory_capture_gap INSERT on explicit failure
```

禁止的数据方向：

```text
Phase 1F.2 -> Selection/strategy inference/runtime
Phase 1F.2 -> current Advisory list/review lifecycle
Phase 1F.2 -> Paper/simulation/QMT/order/account/position
Phase 1F.2 -> market source or shared business-table DML
```

### 5.3 No repeated package validation

本阶段只修改trace identity的schema和typed repository contract，不读取package asset、不加载模型、不调用
package validator或Selection。策略包可用性仍由既有入库/admission事实负责。

## 6. PostgreSQL V3 Schema Contracts / 数据库契约

### 6.1 Outbox natural key

删除旧constraint并新增显式命名约束：

```text
uq_advisory_stage_trace_outbox_scope_identity
UNIQUE(selection_run_id, package_id, manifest_sha256,
       decision_as_of_trade_date, capture_policy_hash, admission_scope_hash)
```

`admission_scope_hash`已经是NOT NULL列，迁移不修改历史outbox payload或hash。不同scope可并存；同scope
不同trace payload继续由natural key冲突拒绝。`capture_batch_id`、`capture_fencing_token`、attempt no和
`admission_scope_id`不进入唯一键：前两者属于恢复执行身份，scope id/hash一致性由typed binding/readback保证。

### 6.2 Gap legacy/v2 coexistence

在`app.advisory_capture_gap`新增：

```text
admission_scope_id TEXT NULL
admission_scope_hash TEXT NULL

ck_advisory_capture_gap_scope_pair
CHECK (
  (admission_scope_id IS NULL AND admission_scope_hash IS NULL)
  OR
  (admission_scope_id IS NOT NULL AND admission_scope_hash IS NOT NULL)
)
```

两列无default。历史行自然保持NULL，不执行UPDATE。删除旧全表unique constraint，并建立：

```text
ux_advisory_capture_gap_legacy_identity
UNIQUE(selection_run_id, package_id, manifest_sha256,
       decision_as_of_trade_date, capture_policy_hash, reason_code)
WHERE admission_scope_hash IS NULL

ux_advisory_capture_gap_scope_v2_identity
UNIQUE(selection_run_id, package_id, manifest_sha256,
       decision_as_of_trade_date, capture_policy_hash,
       admission_scope_hash, reason_code)
WHERE admission_scope_hash IS NOT NULL
```

DB layout允许历史NULL行继续存在，但新写API只接受v2 identity。不得提供`allow_legacy_write`、NULL fallback
或按旧键重试。`gap_content_hash UNIQUE`保持不变。

### 6.3 Comments and immutable triggers

- 更新outbox table comment，明确natural key包含scope hash。
- 更新gap table comment，明确NULL scope仅表示迁移前legacy evidence。
- 为gap新增两列写明权威来源和legacy语义的column comments。
- 既有no-update/no-delete triggers保持，不替换、不禁用。
- 不新增角色、GRANT/REVOKE、RLS或人工状态表。

## 7. Typed Identity And Hash Contract

### 7.1 Legacy read-only identity

```text
LegacyExpectedTraceIdentityV1
  selection_run_id
  package_id
  manifest_sha256
  decision_as_of_trade_date
  capture_policy_hash
```

它只由持久化gap row parser在两列scope均为NULL时创建。其canonical payload必须与当前
`ExpectedTraceIdentity.model_dump(mode="json")`逐字节等价，legacy `gap_content_hash`继续按：

```text
sha256({"identity": <legacy canonical payload>, "reason_code": reason})
```

不得增加schema version字段、默认scope或重排字段语义后重算旧hash。

### 7.2 Scope-aware v2 identity

```text
ScopeAwareExpectedTraceIdentityV2
  schema_version = advisory_phase1_trace_identity_v2
  selection_run_id
  package_id
  manifest_sha256
  decision_as_of_trade_date
  capture_policy_hash
  admission_scope_id
  admission_scope_hash
```

v2 outbox natural key固定为：

```text
(selection_run_id, package_id, manifest_sha256,
 decision_as_of_trade_date, capture_policy_hash, admission_scope_hash)
```

v2 gap hash覆盖完整v2 identity（包括schema version、scope id/hash）和reason code。scope hash必须是小写
SHA-256；scope id非空。`from_envelope()`只从Selection identity读取前四项，从`TraceCaptureBinding`读取
policy和scope，不从任意dict猜测scope。

### 7.3 Explicit selection lookup key

当前admission validator使用`identity.natural_key[:4]`查询capture plan。Phase 1F.2新增显式：

```text
selection_lookup_key =
  (selection_run_id, package_id, manifest_sha256, decision_as_of_trade_date)
```

所有capture-plan查询改用该属性，不依赖natural key字段顺序或tuple切片。

### 7.4 Gap read/write types

```text
TraceCaptureGapRecord.identity = LegacyExpectedTraceIdentityV1 | ScopeAwareExpectedTraceIdentityV2
PostgresTraceCaptureGapRepository.record(identity=ScopeAwareExpectedTraceIdentityV2, ...)
TraceCaptureGapHandler.__call__(identity=ScopeAwareExpectedTraceIdentityV2, ...)
TraceCaptureReconciler.expected = Sequence[ScopeAwareExpectedTraceIdentityV2]
```

row parser规则：两列均NULL -> v1；两列均非NULL -> v2；只有一列NULL ->
`ADVISORY_PHASE1F2_GAP_SCOPE_PAIR_INVALID`。v1 readback复算旧hash，v2 readback复算新hash；不匹配时显式失败。

## 8. Release Registry V3 And Predecessor Closure

### 8.1 Registry identity

新增：

```text
backend/services/advisory_phase1/release_schema_registry/
  advisory_phase1_dataset_foundation_v3.json

schema_version = advisory_phase1f_release_schema_contract_v2
release_schema_version = advisory_phase1_dataset_foundation_v3
```

不新增contract/plan/receipt schema v3。现有contract schema v2已经支持typed migration和predecessor；
本阶段只新增release schema version，避免无业务价值的DTO复制。

v3完整catalog inventory冻结为：

```text
required_relations = 35
required_columns = 637
required_constraints = 274
required_indexes = 102
required_functions = 33
required_triggers = 63
required_comments = 151
managed_migration_orders = [10, 20, 30, 40, 50, 55, 60, 70, 80, 90]
```

相对v2仅增加2列、1个partial index净增量和2个column comments；constraint总数保持不变，因为旧gap
unique被pair check替代、outbox unique为一换一。实现测试必须按catalog生成结果核对这些数量，不能手工删减
registry对象来满足断言。

### 8.2 Predecessor closure

v3 `predecessor_contract`精确指向v2 registry filename和content hash，`exact_relations`固定排序为：

```text
app.advisory_capture_gap
app.advisory_selection_stage_trace_outbox
```

`load_predecessor_release_schema_contract()`移除“v2 predecessor必须是v1”的硬编码，改为：

1. predecessor必须由repository registry root内单一JSON filename定位；
2. predecessor hash必须与spec完全一致；
3. predecessor schema version必须属于当前代码支持集合；
4. predecessor release schema version不得与current相同；
5. exact relations必须存在于predecessor required relations；
6. 错误文本不硬编码v1/v2。

`verify_catalog()`同时移除`has_cutover_predecessor`对table->view形态的专用限制：只要current contract声明
predecessor，就在exact relation scope内验证前驱；只有前驱scope完全兼容时才启用
`repairable_unexpected_objects`。验证v3时形成有界链v3 -> v2 -> v1；每层registry content hash锁定下一层，
不拼接、不修改历史registry。v2 -> v1行为保持原测试；v3 -> v2新增正向和
tamper/escape/wrong-hash/wrong-relation/unknown-drift反例。

### 8.3 Default and compatibility classification

`DEFAULT_RELEASE_SCHEMA_REGISTRY`切换到v3。对exact v2 catalog：

```text
managed_schema_status = PARTIAL_ADDITIVE
pending migration orders = [90]
unknown drift = none
```

对exact v3 catalog：

```text
managed_schema_status = COMPATIBLE
pending migration orders = []
exact reapply ddl_executed = false
```

旧outbox/gap constraints及其backing indexes作为v3 `repairable_unexpected_objects`，只允许order 90删除；
新增columns/check/constraints/indexes/comments均声明`repairable_by_orders=[90]`。其他unexpected/drift继续拒绝。

### 8.4 Declared object ownership

v3 registry保留orders 10-80的原SQL file SHA，但最终对象ownership必须与新catalog一致：

- order 10不再声明已被替换的outbox old constraint/backing index和被order 90更新的outbox table comment；
- order 30不再声明已被替换的gap old constraint/backing index和被order 90更新的gap table comment；
- order 90声明2个gap columns、2个column comments、2个更新后的table comments、pair check、新outbox
  constraint/backing index和2个gap partial indexes；
- superseded old constraint/index只存在于`repairable_unexpected_objects`，不得继续出现在required objects；
- 全部migration `declared_object_ids`的集合必须精确等于v3 `contract.object_ids()`。

同一final object id不得同时由旧order和order 90模糊拥有。实现测试必须检查每个受影响object id的唯一
declaring order，而不只检查集合相等。

## 9. Frozen Migration Order 90

### 9.1 File and registry entry

```text
backend/db/migrations/add_advisory_phase1f2_scope_aware_trace_identity_20260715.sql

order = 90
depends_on_orders = [80]
transaction_group = trace_identity_scope
transaction_mode = EXECUTOR_MANAGED
executor_action = SQL_FILE
```

registry冻结文件SHA和完整declared object ids。migration文件不得包含`BEGIN/COMMIT`，事务由release executor
持有；不得使用psql meta-command、动态外部文件或运行时变量。

### 9.2 Exact DDL sequence

order 90在一个transaction中按以下顺序执行：

1. 为gap添加两个nullable、无default的scope列。
2. 添加scope pair check（可先`NOT VALID`再在同次release中`VALIDATE CONSTRAINT`）。
3. 删除gap旧unique constraint及其backing index。
4. 创建legacy与scope-v2两个partial unique indexes。
5. 删除outbox旧unique constraint及其backing index。
6. 添加`uq_advisory_stage_trace_outbox_scope_identity` unique constraint。
7. 更新受影响table/column comments。

migration不使用`IF EXISTS`/`IF NOT EXISTS`掩盖同名漂移；apply前catalog verifier必须证明exact v2
predecessor scope。任一步失败，order 90全部回滚。

### 9.3 No business-row rewrite

- 不UPDATE历史gap或outbox。
- 不回填scope。
- 不改`trace_content_jsonb`、`trace_content_hash`或`gap_content_hash`。
- 不创建shadow/temporary业务表。
- 不复制、删除或重新排序业务行。
- nullable column addition不带default，避免整表payload rewrite。

## 10. Transaction, Concurrency And Rollback Boundary

### 10.1 Apply transaction

release executor继续使用冻结`lock_timeout_ms=10000`、`statement_timeout_ms=900000`和
`automatic_retry=false`。order 90取得PostgreSQL DDL所需table locks；无法及时取得时明确失败并回滚，
不sleep、不隐藏retry、不绕过校验。

### 10.2 Plan staleness

APPLY必须重新验证plan request hash、contract hash、migration SHA、database identity和current catalog。
plan后schema变化返回现有stable stale/conflict reason，零DDL；不得重新plan后静默继续。

### 10.3 Rollback boundary

Phase 1F.2采用forward migration。通用自动rollback不安全：一旦存在两个scope共享旧五字段key的outbox，
恢复旧constraint会拒绝合法数据；删除gap scope列也会丢失v2 identity。

因此：

- order 90 transaction未commit：数据库自动完整回滚到v2。
- commit后尚无scope-v2数据：只可在独立、显式的release修复任务中，经只读确认无跨scope duplicate和
  non-NULL gap scope row后设计反向migration；本阶段不自动执行。
- 已存在scope-v2数据：禁止降回v2，必须forward fix。

这是数据不可逆性的技术事实，不是审批、角色或人工运行门禁。程序不提供`--force-rollback`或删数据路径。

### 10.4 Backup boundary

AIstock每日数据库备份与本migration独立。设计、代码合入、DEV或production DDL授权均不隐含额外全库
dump/export/snapshot；只有用户另行明确要求时才执行额外备份。

## 11. Repository Changes

### 11.1 `trace_outbox.py`

- 新增legacy/v2 typed identity及canonical hash helper。
- in-memory与PostgreSQL `_natural_key()`加入scope hash。
- outbox SELECT/contains/exact-conflict SQL加入`admission_scope_hash`。
- `_outbox_insert_params()`解构显式字段，不依赖tuple位置。
- `ExpectedTraceIdentity.from_envelope()`替换为v2 factory。
- reconciler只接受v2 identity。
- 不在本阶段新增Phase 1G caller-owned transaction API。

### 11.2 `capture_foundation.py`

- gap write只接受v2 identity并写scope id/hash。
- row parser按NULL pair区分legacy/v2并复算对应hash。
- admission plan lookup改用`selection_lookup_key`。
- 不改变capture batch状态机、lease、fencing、membership或receipt语义。

### 11.3 `observation_capture.py`

- observation failure创建gap时必须从exact `TraceCaptureBinding`携带scope id/hash。
- 不从plan、package、日期或默认值重新推导scope。
- 不改变canonical signal/header/version/stage/candidate推导或异常传播语义。

### 11.4 `__init__.py` and import surface

当前lazy public API包含`ExpectedTraceIdentity`。为避免无关import rename，保留该名称作为
`ScopeAwareExpectedTraceIdentityV2`的显式alias；构造时scope仍是必填，不能恢复legacy写入。另导出
`ScopeAwareExpectedTraceIdentityV2`供新代码使用；legacy v1 parser保持module-private read-only。
不得让Selection、Paper、simulation或FastAPI startup import release executor/repository。

### 11.5 Pre-DDL compatibility window

Phase 1F.2代码/registry合入本身不激活consumer。当前没有FastAPI route、scheduler、Selection callback或
Paper/simulation路径调用这些Advisory repositories，因此v2 catalog在DEV apply前不会影响其他模块运行。
当前default release registry loader的调用点也仅为`advisory_phase1_release_schema.py`和Advisory Phase 1
tests，不在backend startup/runtime dependency graph。
新repository write API不提供v2 schema fallback；在v3 ready receipt前也不得被Phase 1G调用。若开发者直接
对v2 schema调用新写API，repository必须把PostgreSQL missing-column/object错误转换为稳定
`ADVISORY_PHASE1F2_SCHEMA_NOT_READY`并保留后台异常链，不得改写成legacy NULL-scope成功。

## 12. CLI And Release Flow

继续复用：

```text
python scripts/advisory_phase1_release_schema.py plan
python scripts/advisory_phase1_release_schema.py apply
python scripts/advisory_phase1_release_schema.py verify
python scripts/advisory_phase1_release_schema.py inspect-receipt
```

不新增Phase 1F.2专用CLI、`--force`、`--skip`、approval或backup参数。数据库连接只使用显式
`--env-file`和`--target-db dev|production`对应的exact env keys，不猜测DSN，不从DEV回退production。

发布顺序：

```text
code/registry merge
  -> DEV plan
  -> DEV apply order 90
  -> new DEV verify
  -> new DEV exact reapply (ddl_executed=false)
  -> Phase 1G DEV code may start
  -> production DDL only after separate explicit user instruction
```

Phase 1G DEV不等待Phase 1F.2 production DDL；Phase 1G production执行必须先有production v3 ready receipt。

## 13. Errors And Logging

新增或冻结以下Phase 1F.2 repository reason：

```text
ADVISORY_PHASE1F2_TRACE_IDENTITY_INVALID
ADVISORY_PHASE1F2_SCHEMA_NOT_READY
ADVISORY_PHASE1F2_GAP_SCOPE_PAIR_INVALID
ADVISORY_PHASE1F2_LEGACY_GAP_HASH_MISMATCH
ADVISORY_PHASE1F2_SCOPE_GAP_HASH_MISMATCH
ADVISORY_PHASE1F2_OUTBOX_SCOPE_CONFLICT
```

release plan/apply/verify继续使用Phase 1F现有contract/hash/catalog/database/DDL reason codes，不复制一套
相同错误。日志只在target开始/完成、catalog difference、migration transaction失败和最终summary输出；
错误包含target、order/object id、reason和exception type，不输出密码、DSN、完整trace/gap payload或无价值逐行日志。

## 14. Code Ownership And Proposed Files

```text
backend/db/migrations/add_advisory_phase1f2_scope_aware_trace_identity_20260715.sql
backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v3.json
backend/services/advisory_phase1/release_schema_contract.py
backend/services/advisory_phase1/release_schema_verify_postgres.py
backend/services/advisory_phase1/trace_outbox.py
backend/services/advisory_phase1/capture_foundation.py
backend/services/advisory_phase1/observation_capture.py
backend/services/advisory_phase1/__init__.py
backend/tests/advisory_phase1/test_release_schema.py
backend/tests/advisory_phase1/test_release_schema_dev_db.py
backend/tests/advisory_phase1/test_release_schema_import_boundary.py
backend/tests/advisory_phase1/test_trace_outbox_dev_db.py
backend/tests/advisory_phase1/test_capture_foundation.py
backend/tests/advisory_phase1/test_capture_foundation_dev_db.py
backend/tests/advisory_phase1/test_stage_trace.py
docs/architecture/advisory_phase1f2_scope_aware_trace_identity_forward_migration_f2_design_20260715.md
docs/architecture/advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md
docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
```

任何Selection、strategy package、simulation、Paper、QMT、QE/RD-Agent/Qlib、frontend或API文件不在允许修改范围。

## 15. Verification Plan

### 15.1 L0 Static

- changed-path allowlist和transitive import boundary。
- migration无`BEGIN/COMMIT`、psql meta-command、business DML、role/RBAC/approval/backup hook。
- v3 registry hash、migration SHA、order 90/dependency/declared objects完整。
- old constraints/indexes只作为order 90 repairable unexpected objects。
- default registry指向v3，v1/v2 registry仍可按显式path解析。
- v3 inventory精确为35 relations/637 columns/274 constraints/102 indexes/33 functions/63 triggers/151 comments。
- Design Acceptance Index、F2 validator和`git diff --check`通过。

### 15.2 L1 Pure

- v2 -> v1和v3 -> v2 predecessor closure正向用例。
- verifier不依赖table->view cutover形态，constraint/index-only predecessor也可精确forward。
- predecessor path escape、wrong hash、same release version、missing exact relation反例。
- legacy identity canonical payload/hash与迁移前golden完全一致。
- v2 identity scope id/hash必填、hash格式、natural key和selection lookup key。
- different scope产生不同v2 gap/outbox identity；same scope exact key一致。
- optional/NULL scope新写、手写scope覆盖和legacy write调用全部拒绝。
- pair mismatch row parser、legacy/v2 hash tamper显式失败。
- public `ExpectedTraceIdentity` alias仍强制v2 scope；无legacy public write alias。

### 15.3 L2 Disposable PostgreSQL 16

1. full order 10/20/30/40/50/55/60/70/80/90 apply通过。
2. exact v2 baseline在v3 plan只产生order 90，unknown drift为空。
3. migration前插入legacy gap，apply后row count、全部旧列和`gap_content_hash`逐值不变。
4. 同Selection/package/date/policy两个scope outbox均成功；同scope不同payload conflict。
5. 两个scope相同reason gap均成功；同scope exact retry返回同一gap。
6. legacy NULL pair与v2 non-NULL pair均readback；半NULL row被check/repository拒绝。
7. order 90任一步fault injection后全部DDL回滚，catalog仍exact v2。
8. apply后new verify为`COMPATIBLE/COMPATIBLE/downstream_ready=true`。
9. exact reapply `ddl_executed=false`且catalog fingerprint一致。
10. old constraint/index残留、wrong predicate、wrong column order、unknown same-name对象均被拒绝。
11. predecessor exact relation出现任一unknown drift时不启用repairable unexpected objects，plan拒绝。
12. query/import spy证明无Selection/Paper/simulation/shared业务表写入。
13. v2 schema直接调用新write API显式失败，不形成NULL-scope新行。
14. disposable database销毁，无DEV/production连接或残留。

### 15.4 L3 Transactional DEV

使用`.env` exact DEV keys验证catalog query、v2 baseline classification、order 90 transaction lock/timeout和
repository SQL；可回滚的测试数据使用专用identity并在结束后零业务残留。该层不冒充persistent DDL apply。

### 15.5 Persistent DEV DDL

用户明确要求执行后，按以下证据闭环：

1. immutable DEV plan receipt；
2. apply receipt且只执行order 90；
3. 新连接catalog readback与row-count/hash preservation；
4. new VERIFY receipt；
5. exact-reapply receipt且`ddl_executed=false`；
6. 最终v3 catalog fingerprint与receipts保存到repo-external store。

不得用disposable数据库、mock receipt或仅unit test冒充persistent DEV完成。

## 16. Positive Reachability / 正向可达性

合法路径无需人工改库：

```text
exact v2 catalog
  + v3 frozen registry/order 90 SHA
  + exact DEV connection
  -> plan PARTIAL_ADDITIVE [90]
  -> atomic order 90
  -> v3 COMPATIBLE
  -> exact reapply zero DDL
  -> scope A outbox/gap write succeeds
  -> scope B same Selection outbox/gap write succeeds
  -> same-scope retry exact reuse
```

所有保留检查必须有正向用例。空表、仅legacy gap行和零Program都可完成schema发布；Phase 1F.2不要求
Phase 1E真实DSE、capacity MEASURED、Parquet或模型状态。

## 17. Impact Matrix

| 模块 | 读 | 写 | 影响 |
|---|---|---|---|
| release registry/verifier | v2/v3 contract/catalog | receipt only | default提升到v3 |
| outbox | exact scope identity | constraint/repository | 跨scope合法并存 |
| capture gap | legacy/v2 identity | additive scope columns/indexes | legacy hash保持，新写scope-aware |
| Phase 1G | future v3 receipt | none in this phase | DEV代码前置解除后才开始 |
| Selection/inference | none | none | 无影响 |
| current荐股列表 | none | none | 无影响 |
| simulation/Paper/QMT | none | none | 无影响 |
| QE/RD-Agent/Qlib/backtest | none | none | 无影响 |
| runtime/startup | none | none | 无激活 |

## 18. Risks And Failure Modes

| 风险 | 后果 | 设计约束 |
|---|---|---|
| 只改outbox不改gap | 失败证据跨Program串线 | success/failure identity同批修正 |
| 回填legacy scope | 伪造历史证据、旧hash失真 | NULL保留、零UPDATE |
| 新写允许NULL scope | 继续生成v1缺陷数据 | write API只接受v2 |
| v3 loader继续硬编码v1 | v3无法消费v2前驱 | generic exact predecessor closure |
| verifier只识别table->view cutover | constraint-only v2 baseline被误判drift | predecessor声明驱动exact-scope验证 |
| old constraint被当unknown drift | v2无法forward plan | repairable unexpected order 90 |
| `IF NOT EXISTS`隐藏漂移 | 错误catalog假成功 | exact predecessor + no silent DDL |
| rollback删除v2 identity | 数据丢失/约束冲突 | post-data forward fix only |
| migration修改共享运行链 | Selection/模拟盘受阻 | Advisory-only paths + import/call spy |
| code merge到DDL apply窗口静默写v1 | 新legacy数据继续产生 | 无activated consumer + no v2 fallback |
| DEV连接回退production | 错库DDL | exact env target，无fallback |
| 增加审批/备份门禁 | 单用户流程不可达 | 零相关实体/参数/static scan |

## 19. Implementation Plan / 实施批次

### F2A：Contract And Migration

- order 90 SQL；
- v3 registry和generic predecessor loader；
- v2 baseline/unknown drift/registry hash pure tests。

### F2B：Typed Identity And Repository

- legacy read-only与scope-aware v2 DTO；
- outbox natural key/query；
- gap parser/write/hash；
- capture plan explicit lookup key。

### F2C：PostgreSQL And Isolation Verification

- disposable PostgreSQL full matrix；
- fault injection/legacy preservation/dual-scope tests；
- import/query scope与DESIGN-COMPLIANCE复核。

### F2D：DEV Release Evidence

- 仅在用户明确要求后执行DEV plan/apply/new-verify/new-exact-reapply；
- 同步父设计和Phase 1G code-start状态；
- production继续保持not executed，除非另行明确要求。

每个批次完整实现自己的设计条目，不得以JSON-only registry、只改constraint、不兼容legacy hash或
in-memory-only测试冒充完成。

## 20. Production Gates (State Reporting Only, No Approval) / 生产状态

```text
code_merge = not_started
phase1f2_dev_ddl = not_executed
phase1f2_production_ddl = not_executed
phase1g_business_dml = not_executed
runtime_activation = none
dependency_install = none
role_or_approval_gate = none
extra_backup_gate = none
```

该节只报告状态以满足AIstock F2文档结构，不创建程序审批。代码合入不自动执行DEV/production DDL；
production DDL只在用户对该次操作明确要求后执行，且不隐含额外数据库备份。

## 21. Design Acceptance Index

- F-650：Phase 1F.2只修正Advisory-owned trace identity，不影响Selection、荐股、模拟盘、Paper或交易。
- F-651：outbox natural key精确加入`admission_scope_hash`，不加入attempt/batch/fencing。
- F-652：不同scope同Selection可并存，同scope不同payload冲突，exact retry复用。
- F-653：gap新增nullable scope pair，历史行零回填、零hash重写。
- F-654：legacy与scope-v2 partial unique indexes前后语义完整。
- F-655：legacy identity只读且canonical payload/hash与当前实现逐字节兼容。
- F-656：新outbox/reconciler/gap写入及公共`ExpectedTraceIdentity` alias只接受scope-aware v2 identity。
- F-657：v2 identity覆盖schema version、scope id/hash并严格校验。
- F-658：capture plan查询使用显式selection lookup key，不切片natural key。
- F-659：v3 registry复用contract schema v2并冻结完整catalog inventory，不复制无意义plan/receipt DTO。
- F-660：v3 predecessor精确指向v2 hash和两个受影响relations。
- F-661：predecessor loader/verifier泛化到constraint-only forward且v2->v1历史行为保持。
- F-662：exact v2 catalog被分类为只需order 90的PARTIAL_ADDITIVE。
- F-663：unknown drift和不受管同名对象继续fail-fast。
- F-664：order 90 SHA、dependency、transaction mode和final object唯一declaring-order完整冻结。
- F-665：order 90单事务、零业务DML、失败全回滚。
- F-666：migration不使用IF EXISTS/NOT EXISTS掩盖漂移。
- F-667：gap新增列无default，历史行和hash逐值保持。
- F-668：既有immutability trigger保留，无角色/RLS/approval对象。
- F-669：post-commit rollback按scope-v2数据存在性区分，已有v2数据只允许forward fix。
- F-670：现有release CLI和exact env target连接被复用，无专用force/skip入口。
- F-671：DEV/production/code/DML/runtime状态独立报告。
- F-672：无额外全库备份、用户、角色、审批、授权或人工DB修改要求。
- F-673：pure tests覆盖legacy/v2 hash、scope natural key和predecessor tamper。
- F-674：disposable PostgreSQL覆盖full apply、v2 forward、failure rollback和exact reapply。
- F-675：双Program/双scope outbox与gap正向路径可达。
- F-676：query/import spy证明不调用或修改Selection/inference/Paper/simulation，pre-DDL窗口无activated consumer或v2 fallback。
- F-677：transactional DEV零业务残留，persistent DEV证据不由mock冒充。
- F-678：Phase 1G DEV只等待Phase 1F.2 DEV ready，不错误等待production DDL。
- F-679：父蓝图、Phase 1父设计、Phase 1F.1和Phase 1G状态前后一致。

## 22. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-650 | §3、§5、§17 | changed-path/import oracle | design_ready | none |
| F-651 | §6.1、§7.2 | constraint/natural-key tests | design_ready | none |
| F-652 | §6.1、§15.3 | dual/same-scope PostgreSQL tests | design_ready | none |
| F-653 | §6.2、§7.1、§9.3 | legacy preservation evidence | design_ready | none |
| F-654 | §6.2、§9 | predicate/index catalog tests | design_ready | none |
| F-655 | §7.1、§15.2 | legacy golden hash | design_ready | none |
| F-656 | §7.2、§7.4、§11 | type/public-alias/write rejection tests | design_ready | none |
| F-657 | §7.2 | DTO/hash validation tests | design_ready | none |
| F-658 | §7.3、§11.2 | explicit lookup-key tests | design_ready | none |
| F-659 | §8.1 | registry/DTO schema/inventory assertions | design_ready | none |
| F-660 | §8.2 | predecessor file/hash/relation tests | design_ready | none |
| F-661 | §8.2、§15.2-15.3 | v2->v1/v3->v2/cutover-independent matrix | design_ready | none |
| F-662 | §8.3、§15.3 | v2 baseline plan receipt | design_ready | none |
| F-663 | §8.3、§15.3 | drift/unexpected-object negatives | design_ready | none |
| F-664 | §8.4、§9.1、§15.1 | SHA/order/unique declared-object checks | design_ready | none |
| F-665 | §9.2-9.3、§10.1 | fault-injection rollback | design_ready | none |
| F-666 | §9.2、§15.1 | migration static scan | design_ready | none |
| F-667 | §6.2、§9.3 | before/after row hash parity | design_ready | none |
| F-668 | §6.3、§15.1 | trigger/role/RLS catalog scan | design_ready | none |
| F-669 | §10.3 | rollback precondition tests/design review | design_ready | none |
| F-670 | §12 | CLI/env/fallback tests | design_ready | none |
| F-671 | §12、§20 | separated-state receipt report | design_ready | none |
| F-672 | §3.2、§10.4、§20 | approval/backup scan | design_ready | none |
| F-673 | §15.2 | pure identity/predecessor suite | design_ready | none |
| F-674 | §15.3 | disposable PostgreSQL full matrix | design_ready | none |
| F-675 | §15.3、§16 | positive dual-scope E2E | design_ready | none |
| F-676 | §4.3、§11.5、§14-15 | transitive import/query/pre-DDL spy | design_ready | none |
| F-677 | §15.4-15.5 | DEV zero-residue/persistent receipts | design_ready | none |
| F-678 | §12、§20、§24 | state transition review | design_ready | none |
| F-679 | §1、§24 | parent/child status reference check | design_ready | none |

## 23. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：outbox与gap成功/失败identity、legacy兼容、registry和DDL完整纳入。
- [x] `no_silent_error`：contract/hash/catalog/pair/hash mismatch均有显式reason和非零失败。
- [x] `no_business_semantic_drift`：只放宽跨scope合法并存，不改Selection、排名、策略包或运行行为。
- [x] `no_unrequested_gate_or_approval`：无角色、审批、授权、人工复核、manual bypass或备份hook。
- [x] `positive_path_satisfiable`：exact v2自动plan order 90，v3后双scope写入和同scope retry可达。
- [x] `legacy_truth`：旧gap NULL scope和hash不改写，不把猜测scope伪装为历史事实。
- [x] `database_connection_truth`：只使用exact `.env` target keys，不猜测、不回退、不输出密码。
- [x] `transaction_truth`：order 90单事务；commit前失败全回滚，commit后不伪造自动rollback。
- [x] `research_boundary`：不读取回测/Paper/模拟盘数据，不生成交易输入或模型。

## 24. Exit Criteria And Next Phase

设计完成条件：

1. F-650至F-679全部`design_ready`，无未批准exception/TODO。
2. v3 registry、order 90、legacy/v2 identity和rollback边界前后一致。
3. Phase 1G、Phase 1父设计和蓝图状态同步。
4. F2 validator、文档引用和`git diff --check`通过。
5. 无额外审批、角色、授权、备份、运行hook或共享模块业务改动设计。

Phase 1F.2代码可请求合入的条件：F2A-F2C完整实现，L0-L2通过，DESIGN-COMPLIANCE-001逐项有
实现/测试证据；不得以只改constraint、只改registry或fixture-only结果冒充完成。

Phase 1F.2 DEV完成条件：用户明确要求后完成DEV plan/apply/new-verify/new-exact-reapply，v3
`COMPATIBLE/COMPATIBLE/downstream_ready=true`，legacy row/hash preservation和immutable receipts完整。

上述DEV条件满足后可开始Phase 1G G1-G4代码；Phase 1G persistent DEV L4仍需Phase 1E真实single/multi
Alpha immutable DSE/receipt。Phase 1F.2 production DDL、Phase 1G business DML和runtime activation继续
独立报告，不由代码合入自动触发。
