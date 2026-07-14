# AIstock Advisory Phase 1F Release Schema Verification F2 详细设计

## 1. Background / 文档定位与当前状态

本文是
`advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`
中 Phase 1F 的唯一实施级详细设计，承接已经合入的 Phase 1A-1E 数据契约与代码。

```text
Phase 1E immutable execution plan
  -> Phase 1F release schema plan
  -> ordered additive migrations and historical month partitions
  -> full PostgreSQL catalog verification
  -> immutable release receipt
  -> Phase 1G / Phase 1H explicit DML requests
```

任务分级为 `T3 / F2`。Phase 1F 只解决数据库结构发布和结构一致性验证，不执行
source ledger、observation、label、build 或 snapshot 业务 DML，不生成 Parquet，不启动 observer，
不训练模型，也不改变 Selection、StrategyPackage inference、模拟盘、Paper、QE、RD-Agent、
Qlib 或 QMT 的任何运行路径。

当前状态：

```text
design_status = design_ready
implementation_status = merged_pr_2114_dev_l3_complete
phase1e_code = merged_as_code_complete_pending_real_dev_input
dev_readonly_preflight = completed_2026-07-14; managed=PARTIAL_ADDITIVE; prerequisite=COMPATIBLE
dev_release_rehearsal = completed_2026-07-14; managed=COMPATIBLE; prerequisite=COMPATIBLE
dev_ddl = migration_order_50_applied_and_verified
dev_v1_receipt_downstream_ready = true
phase1_parent_schema_parity = incomplete_lineage_candidate_partition_and_hash_uniqueness
phase1g_persistent_schema_ready = false_pending_phase1f2_scope_aware_trace_identity
production_ddl = applied_and_verified_via_phase1f1_v2_2026_07_15
production_dml = none
runtime_activation = none
model_training = none
```

Phase 1E persistent dual-track L4 仍等待真实 single/multi Alpha immutable DSE/receipt；该缺口
不阻塞 Phase 1F 设计、schema apply 或 schema verify，但不得在 Phase 1F 中伪造、复制或手写
DSE 予以替代。

Phase 1F.1已完成其partition/hash修正，但2026-07-15 Phase 1G开工复核发现outbox natural key及
capture-gap identity缺少scope。该新缺口由Phase 1G详细设计§5.4/§21 G0的独立Phase 1F.2 contract负责，
不改写Phase 1F/1F.1历史receipt，也不构成审批或人工门禁。
Phase 1F.2唯一实施级设计为
`advisory_phase1f2_scope_aware_trace_identity_forward_migration_f2_design_20260715.md`，当前
`design_ready / implementation_not_started / DEV_DDL_not_executed`。

## 2. 目标

Phase 1F 必须同时达到以下目标：

1. 把分散的 Phase 1 migrations 冻结为一个有顺序、有内容 hash、有依赖关系的 release contract。
2. 在 DEV 或生产目标库上准确区分 `ABSENT`、`PARTIAL_ADDITIVE`、`COMPATIBLE` 和
   `DRIFTED`，禁止
   `IF NOT EXISTS` 把错误的同名对象静默当作成功。
3. 对 Phase 1 repository 真正依赖的表、列、类型、默认值、主外键、唯一约束、check、索引、
   predicate、分区、触发器、函数和权威注释做完整 catalog verification。
4. 从显式 Phase 1D capacity request 的历史日期范围生成完整月分区，不猜测年限，不创建
   default partition，不允许运行进程临时执行 DDL。
5. 合法 schema 在 fresh apply、exact reapply 和 read-only verify 三条正向路径中都必须通过；
   缺对象、漂移或错误分区必须返回明确 reason code 和诊断上下文。
6. 生成不可变、可复算、去敏的 plan/apply/verify receipt，使 DEV 与生产状态可以分别报告。
7. 不增加角色、RBAC、审批表、授权链、人工确认记录、双人复核或 DDL 前全库备份要求。

## 3. 当前事实与缺口

### 3.1 Managed migration 顺序

Phase 1F 管理下列历史 migration 和本阶段新增的 order 70 forward migration。文件内容在 release contract 中按 SHA-256 冻结；若发现
缺陷，只能新增 forward migration，禁止修改已发布 migration 后让同一文件名代表不同内容。

| order | migration | transaction mode | 主要对象 | 依赖 |
|---:|---|---|---|---|
| 10 | `add_advisory_source_availability_ledger_20260712.sql` | `EXECUTOR_MANAGED` | source availability/revision、stage trace、control binding | `app` schema 与既有 Advisory 基础表 |
| 20 | `add_advisory_phase1c2_source_revision_cutoff_20260713.sql` | `EXECUTOR_MANAGED` | source revision set v2 与 enforced cutoff | order 10 |
| 30 | `add_advisory_phase1_capture_foundation_20260713.sql` | `FILE_WRAPPED` | capture batch/plan/gap、observation、stage evidence | order 10、20 |
| 40 | `add_advisory_phase1c3_label_snapshot_foundation_20260713.sql` | `FILE_WRAPPED` | label、blob、build、attempt、snapshot、fixture partitions | order 30 |
| 50 | `add_advisory_phase1_source_observer_20260714.sql` | `EXECUTOR_MANAGED` | observer cursor/receipt | order 10、20 |
| 60 | Phase 1F generated partition DDL | `EXECUTOR_MANAGED` | 完整历史月分区 | order 40 + explicit capacity range |
| 70 | `add_advisory_phase1f_schema_canonicalization_20260714.sql` | `EXECUTOR_MANAGED` | 历史/空库 constraint 与 capture comment 语义收敛 | order 10、30 |

Phase 0A/0A.2 的 Program、dated binding、historical research receipt 和 Selection evidence 是
read-only prerequisite contract，不属于 Phase 1F 管理的 DDL。Phase 1F 只验证后续 1G/1H
实际引用的 prerequisite columns，不重复验证策略包资产，也不执行 Selection 或策略推理。

### 3.2 现有逻辑关系清单

release contract 覆盖 31 个 Phase 1 逻辑关系和所有目标月分区：

| group | relations |
|---|---|
| source/control | `advisory_source_availability_event`、`advisory_source_revision_set`、`advisory_source_revision_member`、`advisory_selection_stage_trace_outbox`、`advisory_selection_stage_trace_delivery_event`、`advisory_phase1_control_binding_event` |
| capture/observation | `advisory_capture_batch`、`advisory_capture_plan`、`advisory_capture_batch_evidence_membership`、`advisory_capture_gap`、`advisory_signal_observation`、`advisory_signal_observation_version`、`advisory_signal_observation_lineage`、`advisory_signal_stage_evidence`、`advisory_signal_stage_candidate` |
| label/build/snapshot | `advisory_dataset_blob`、`advisory_outcome_label`、`advisory_outcome_label_payload`、`advisory_dataset_build`、`advisory_dataset_build_attempt`、`advisory_dataset_attempt_file`、`advisory_dataset_build_event`、`advisory_dataset_build_gap`、`advisory_dataset_snapshot`、`advisory_dataset_snapshot_file`、`advisory_dataset_snapshot_observation`、`advisory_dataset_snapshot_label`、`advisory_dataset_snapshot_invalidation`、`advisory_dataset_snapshot_blob_ref` |
| observer | `advisory_source_observer_cursor`、`advisory_source_observation_receipt` |

`advisory_outcome_label_payload_202606`、`202607` 和 `202608` 只是 Phase 1C-3 fixture/golden
范围，不代表真实历史覆盖已经完成。

### 3.3 必须修复的发布缺口

1. 当前没有统一的 migration manifest、schema contract version 或 catalog fingerprint。
2. 单纯重放 `CREATE ... IF NOT EXISTS` 不能证明同名对象的字段和约束正确。
3. 当前没有完整历史月分区规划，缺分区时 label insert 会直接失败。
4. 当前 DEV tests 分别验证各 migration，尚未验证完整顺序、完整 catalog closure 和跨 migration
   reapply。
5. 当前没有可供后续 Advisory worker 调用的纯只读 schema verifier。
6. 当前没有统一 plan/apply/verify receipt，容易把“代码已合入”“DEV 已应用”“生产已应用”混为
   同一状态。

## 4. Scope / 范围

### 4.1 In Scope

- repo-owned、versioned Phase 1F release schema contract registry。
- migration file SHA、执行顺序、依赖和目标对象 closure。
- DEV/production `.env` 连接目标的显式解析和去敏数据库 identity。
- read-only catalog projection、canonicalization、fingerprint 和差异报告。
- 显式历史日期范围到月分区集合的确定性 planner。
- release-only DDL executor；fresh apply、partial additive repair、exact reapply 和 post-apply verify。
- Advisory-owned CLI：`plan`、`verify`、`apply`、`inspect-receipt`。
- immutable release plan/apply/verify receipt 与 atomic no-replace filesystem persistence。
- pure、static、一次性 PostgreSQL integration 和 persistent DEV release rehearsal 设计。
- 与父蓝图、Phase 1、Phase 1D、Phase 1E 和 Phase 1G/1H/1I 边界的同步。

### 4.2 Non-goals

- 不执行 `INSERT/UPDATE/DELETE` 业务 DML，不回填 source、observation、label 或 snapshot。
- 不创建新的业务 authority table、schema approval table、role、grant/revoke 或 RBAC。
- 不修改 `.env`，不猜测 host、port、database、user 或 password。
- 不要求每次 DDL 前执行数据库导出、全库 dump、DR snapshot 或额外备份；现有每日备份策略保持
  不变，任何额外备份必须由用户另行明确提出。
- 不在 FastAPI startup、Selection、模拟盘、Paper、QMT 或共享 inference path 中执行 DDL。
- 不启动 Phase 1D observer，不创建 scheduler，不注册 ingestion callback。
- 不执行 Phase 1E historical research runner，不制造 DSE/receipt，不把 replay 提升为 formal。
- 不生成训练数据文件，不读取回测/QE/Qlib 数据，不训练模型；未来模型训练仍只允许 WSL/Conda。
- 不创建 Advisory API/UI，也不改变当前荐股列表或任何排名。
- 不自动执行生产 DDL。生产执行属于用户明确授权后的独立操作；该授权不落库，也不实现为应用
  审批功能。

## 5. Architecture And Invariants / 架构、不变量与模块隔离

### 5.1 数据库不变量

1. release apply 只允许执行 contract 中按 hash 冻结的 migration 和 planner 生成的 partition DDL。
2. migration apply 成功后必须在同一命令中完成 post-apply full verify；verify 失败时命令失败，
   不得返回“已应用”。
3. exact reapply 必须得到同一 contract version、同一 expected partition set 和兼容 catalog
   fingerprint；不得创建重复对象或修改业务数据。
4. 同名对象存在但定义不同必须归类 `DRIFTED`，不能因为 `IF NOT EXISTS` 返回成功。
5. runtime verifier 只执行固定 `pg_catalog` / `information_schema` SELECT，不包含 DDL、DML、
   lock table、`FOR UPDATE/SHARE` 或动态用户 SQL。
6. `advisory_outcome_label_payload` 不创建 default partition；缺少目标月必须显式报告。
7. release contract 不检查 row count、候选数量、package validity、DSE 数量、Phase 1E L4、Parquet
   大小、模型状态或用户身份。

### 5.2 共享模块零影响

允许的依赖方向只有：

```text
Phase 1F CLI
  -> Advisory Phase 1 release schema contract/executor/verifier
  -> PostgreSQL pg_catalog and committed migrations
```

禁止 Phase 1F import/call：

- Selection Center router/service/repository；
- StrategyPackage validator、asset loader 或 inference；
- simulation runtime、Paper v2、MiniQMT/QMT；
- QE/QuantEvolver、RD-Agent、Qlib exporter/worker；
- Advisory recommendation consumer、daily scheduler 或页面 API。

实现阶段必须用 import denylist、changed-path scan 和 frozen shared entrypoint diff 验证该边界。
read-only verifier 与 DDL executor 必须位于不同 Python modules；
`backend.services.advisory_phase1.__init__` 不得 re-export executor，未来 Phase 1G/1H worker 的
transitive import graph 不得出现 executor、migration loader 或 SQL apply symbol。

## 6. Release Contract

### 6.1 Registry

新增 repo-owned typed registry，例如：

```text
backend/services/advisory_phase1/release_schema_registry/
  advisory_phase1_dataset_foundation_v1.json
```

registry 顶层字段：

```text
schema_version = advisory_phase1f_release_schema_contract_v1
release_schema_version = advisory_phase1_dataset_foundation_v1
normalizer_version
supported_postgres_major_versions
ddl_session_policy
managed_migrations[]
repairable_drift_variants[] = object_id + exact actual payload hash + repair orders
phase0a_prerequisite_relations[]
external_readonly_prerequisite_relations[]
required_relations[]
required_columns[]
required_constraints[]
required_indexes[]
required_functions[]
required_triggers[]
required_comments[]
partition_contract
contract_content_hash
```

v1 `ddl_session_policy` 固定为：

```text
lock_timeout_ms = 10000
statement_timeout_ms = 900000
automatic_retry = false
```

它只作用于 Phase 1F release connection，不修改 cluster/database/role defaults，不是审批或业务
门禁。修改数值必须形成新的 contract hash；executor 不接受 CLI 任意覆盖。

`contract_content_hash` 对除自身外的 canonical payload 求 SHA-256。每个 migration 保存：

```text
order
relative_path
file_sha256
depends_on_orders
transaction_group
transaction_mode = FILE_WRAPPED | EXECUTOR_MANAGED
declared_object_ids
```

新建的 order 70 migration 通过 repository `.gitattributes` 固定 `eol=lf`，保证 Windows、WSL 和 CI
检出的 raw bytes 与 `file_sha256` 一致；executor 不做换行归一化或 hash fallback。

`FILE_WRAPPED` 表示 migration 自带顶层 `BEGIN/COMMIT`，executor 不嵌套或删除其 transaction
envelope；`EXECUTOR_MANAGED` 表示 executor 为该文件建立独立 transaction。禁止用字符串切割
删除 SQL 中的 `BEGIN/COMMIT`，避免误伤 PL/pgSQL function body。

registry 不从目标数据库自动生成并覆盖。实现 PR 必须在隔离 PostgreSQL transaction 中按顺序
应用 migrations，投影 catalog 后与 registry parity；任何 registry 变更都需要代码审查和直接测试。

### 6.2 完整对象契约

禁止只验证表名。每类对象至少冻结：

| object | required contract |
|---|---|
| relation | schema、name、relkind、persistence、partition strategy/key |
| column | name、type/typmod、nullable、default、identity/generated；ordinal 仅诊断，不进入兼容判定/hash |
| PK/UK/FK/check | name、type、deferrability、validation、normalized definition |
| index | name、unique、valid/ready、access method、columns/expressions、predicate |
| function | schema/name/identity args、return type、language、volatility、security、normalized body hash |
| trigger | name、relation、timing/events、constraint/deferrable、enabled state、function、normalized definition |
| comment | object identity、non-empty expected text hash |
| partition | parent、child name、range lower/upper bound、attached/valid state |

`pg_get_constraintdef`、`pg_get_indexdef`、`pg_get_functiondef` 和 `pg_get_triggerdef` 经版本化
normalizer 仅去除无语义空白、quote 和 PostgreSQL 展示噪音，再计算 hash。normalizer 不删除
predicate、check expression、trigger event、function body或 deferrability 等业务语义。

历史 additive DDL 会导致相同 column semantics 具有不同物理 ordinal，PostgreSQL 也会因建表历史为
未命名 check 生成不同序号。Phase 1F 因此保留 ordinal 诊断值但不以其判定漂移；未命名 check 只在
relation/type/normalized definition 完全相同时映射到 contract identity。除此之外的 name/definition
变化仍为 drift。`repairable_drift_variants` 同时冻结 object id、exact predecessor payload hash 和
repair orders，只覆盖真实 migration 中间态/历史 predecessor；不把任意 index、trigger、function
或 constraint drift 降级为可修复。

### 6.3 Prerequisite Contract

prerequisite 分成两类，均只读 catalog、不由 Phase 1F migration 修改：

1. `phase0a_prerequisite_relations`：Phase 1G/1H 将引用的 Phase 0A/0A.2 精确表列、PK/UK 和
   FK target。
2. `external_readonly_prerequisite_relations`：managed function/trigger 直接引用的外部 schema。
   v1 必须包含 `market.trading_calendar(cal_date,is_trading)` 的精确 relation/column/type/nullability
   contract，因为 `app.verify_advisory_signal_calendar_adjacency()` 在 observation insert 时读取它。

registry 必须从 required function bodies 的 external relation refs 形成 reviewed closure；新增外部
引用却未进入 registry 时，static contract test 失败。`market.dataset_date_refresh_audit` 仍由 Phase 1D
observer activation contract 管理，因为 Phase 1F 不启动 observer，也不把它错误提升为 Phase 1G/1H
managed-schema 条件。

任一 prerequisite 缺失时记录 `PHASE1F_PREREQUISITE_SCHEMA_MISSING` 或
`PHASE1F_PREREQUISITE_SCHEMA_DRIFTED` 以及 exact object differences，但不阻止 managed Phase 1F
schema apply/verify；只有未来 Phase 1G/1H 消费对应 receipt 时才根据 `downstream_ready=false` 拒绝。
Phase 1F 不得修改 `market`、调用策略包校验或 Selection 重新生成数据来消除诊断。

## 7. Target Database Identity And Environment

### 7.1 唯一连接来源

连接只从 `F:\Dev\AIstock\.env` 或 CLI 明确传入的 `--env-file` 读取：

| target | keys |
|---|---|
| DEV | `TDX_DB_DEV_HOST/PORT/NAME/USER/PASSWORD` |
| production | `TDX_DB_HOST/PORT/NAME/USER/PASSWORD` |

禁止 DEV key 缺失时回退 production，禁止使用 localhost、默认端口、默认 database 或当前 shell
中的猜测值。日志和 receipt 永不保存 password 或完整 DSN。

### 7.2 去敏身份

连接成功后从 PostgreSQL 读取并冻结：

```text
target_label
current_database
inet_server_addr
inet_server_port
server_version_num
current_user_hash
environment_contract_hash
```

`environment_contract_hash` 只包含去敏字段。plan 的目标身份与 apply/verify 实际身份不一致时
返回 `PHASE1F_DATABASE_IDENTITY_MISMATCH`；这用于防止误连数据库，不是用户/角色审批。

## 8. Typed Requests And Receipts

### 8.1 ReleaseSchemaPlanRequest

```text
schema_version = advisory_phase1f_release_plan_request_v1
release_schema_version
contract_content_hash
target_label = DEV | PRODUCTION
ddl_session_policy_hash
history_start_trade_date
history_end_trade_date
capacity_request_hash
capacity_receipt_hash nullable
phase1e_plan_hashes[]
requested_operation = PLAN | VERIFY | APPLY
request_content_hash
```

日期范围必须来自显式 Phase 1D `CapacityPlanningRequest`；`phase1e_plan_hashes` 只用于 lineage，
允许为空，不参与 managed schema apply 判定，因此不要求 Phase 1E persistent dual-track L4 已完成。
`capacity_receipt_hash` 可在 `PARTIAL` 时存在，Phase 1F 不要求 `MEASURED`，因为 schema 和分区
范围不依赖 Parquet bytes。
CLI `plan` 必须显式传入 `--requested-operation plan|verify|apply`；plan receipt 同时保存实际
`operation=PLAN` 与目标 `requested_operation`，后续 verify/apply receipt 的两者必须一致，禁止
用 APPLY request 冒充 VERIFY lineage。

### 8.2 ReleaseSchemaPlan

plan 保存：

```text
request_content_hash
database_identity
contract version/hash
ddl_session_policy/hash
ordered migration file identities
current catalog fingerprint + object count + per-kind counts/hashes
managed_schema_status
prerequisite_status
downstream_ready
managed_differences[]
prerequisite_differences[]
expected month partitions[]
pending DDL operations[]
expected final catalog fingerprint + object count + per-kind counts/hashes
plan_content_hash
```

`pending DDL operations` 只能是 registry 中 migration file 或确定性 partition creation。plan 不
保存任意 SQL，不接受用户输入 SQL，也不允许跳过 post-apply verify。

### 8.3 ReleaseSchemaReceipt

```text
schema_version = advisory_phase1f_release_receipt_v1
operation
requested_operation
target identity
request/plan/contract hashes
pre_catalog_fingerprint
pre_catalog_evidence = object_count + per_kind_counts/hashes
executed migration hashes[]
per_migration_results[]
executed partition bounds[]
post_catalog_fingerprint
post_catalog_evidence = object_count + per_kind_counts/hashes
operation_status = SUCCESS | FAILED
managed_schema_status
prerequisite_status
downstream_ready
managed_differences[]
prerequisite_differences[]
diagnostics[]
errors[]
started_at/finished_at from database clock
ddl_executed
dml_executed = false
runtime_activated = false
receipt_content_hash
```

`per_migration_results` 保存 order、transaction mode、pre/post subset fingerprint、
`NOT_NEEDED/COMMITTED/FAILED`、数据库时间与 error。整体执行中途失败时 receipt 为
`APPLY_PARTIAL_FAILED`，准确列出已经提交的 earlier migrations；不得声明全量回滚或成功。

状态推导固定为：

```text
downstream_ready =
  managed_schema_status == COMPATIBLE
  and prerequisite_status == COMPATIBLE

operation_status = SUCCESS
  iff requested PLAN/VERIFY/APPLY operation itself completed without errors
```

prerequisite `MISSING/DRIFTED` 写入 `diagnostics`，Phase 1F operation 可以成功但
`downstream_ready=false`；Phase 1G/1H 必须同时校验 exact receipt hash、
`managed_schema_status=COMPATIBLE`、`prerequisite_status=COMPATIBLE` 和
`downstream_ready=true`。任何 `errors` 非空、DDL/transaction/readback/receipt store 失败时
`operation_status=FAILED` 且非零退出，禁止单一 `verification_status` 掩盖双轴事实。

receipt 使用 atomic temp-write + fsync + no-replace rename 写入显式配置的 repo-external receipt
root。same identity/same content 完整 readback 后幂等返回；same identity/different content 返回
`PHASE1F_RECEIPT_COLLISION`。禁止把 CLI stdout 或日志当作唯一发布证据。

## 9. Catalog Projection And Verification

### 9.1 Read-only Projection

固定查询只读取：

- `pg_namespace`、`pg_class`、`pg_attribute`、`pg_type`；
- `pg_constraint`、`pg_index`、`pg_am`；
- `pg_proc`、`pg_language`、`pg_trigger`；
- `pg_inherits`、`pg_partitioned_table`；
- `pg_description`；
- `current_database()`、`inet_server_addr()`、`inet_server_port()`、`current_setting()`。

verify transaction 固定为 `REPEATABLE READ READ ONLY`，并确认
`transaction_read_only=on`。查询只按 registry object ids 定位，不扫描业务数据表、不计算 row
hash，也不阻塞共享业务写入。

### 9.2 Difference Classification

managed schema 与 downstream prerequisite 分成两个状态轴，禁止 prerequisite 缺口把 Phase 1F
结构发布伪装成失败：

| managed_schema_status | meaning | behavior |
|---|---|---|
| `COMPATIBLE` | 全部 managed objects 与 partitions 完整一致 | verify exit 0；apply 为 exact reapply |
| `ABSENT` | managed schema 全部不存在 | 允许按完整顺序 fresh apply |
| `PARTIAL_ADDITIVE` | 缺少可由冻结 migration/partition DDL补齐的对象，或命中 `repairable_drift_variants` 的 exact predecessor payload | 允许 apply 后 full verify |
| `DRIFTED` | 同名对象定义、migration SHA 或 catalog semantics 冲突 | 拒绝 apply；需要新的 forward migration/contract revision |
| `UNSUPPORTED` | PostgreSQL major/contract normalizer 不支持 | 非零失败，不猜测兼容性 |

`PARTIAL_ADDITIVE` 只允许缺失对象能被 frozen migration 中独立的 `CREATE`、显式
`ALTER ... ADD` 或 typed partition DDL补齐。如果 relation 已存在但缺少只定义在其原始
`CREATE TABLE` 内的 column/constraint，则重放 `CREATE TABLE IF NOT EXISTS` 无法补齐，必须归类
`DRIFTED`，不能先执行再依赖 post-verify 报错。

唯一例外是 contract 中显式列出的 order 70 predecessor constraint。它们的 actual/expected canonical
payload 和 repair order 都被冻结；只有 exact match 才能进入 `PARTIAL_ADDITIVE`。普通 predicate、
disabled trigger、function body、index 或同名 constraint 漂移仍必须为 `DRIFTED`。

| prerequisite_status | meaning | Phase 1F behavior |
|---|---|---|
| `COMPATIBLE` | Phase 0A/0A.2 与 required external read-only schema 完整 | `downstream_ready` 可由 managed 状态共同推导 |
| `MISSING` | 任一 prerequisite object 缺失 | managed apply/verify 继续；记录 downstream diagnostic |
| `DRIFTED` | 任一 prerequisite 同名对象语义冲突 | managed apply/verify 继续；Phase 1G/1H 不得消费 |

这些是结构事实，不是审批状态。合法完整 managed schema 必须确定性得到 `COMPATIBLE`；
prerequisite 状态不改变 Phase 1F managed schema 的成功含义。

### 9.3 Fingerprint

catalog fingerprint 对按 `(object_kind,schema,name,sub_identity)` 排序的完整 canonical object
payload 求 SHA-256。必须同时保存 object count 和 per-kind hashes，禁止只比较一个总 hash 后丢失
诊断。partition range 是 fingerprint 的一部分；业务 row、sequence current value、统计信息和
物理 OID 不进入 fingerprint。

## 10. Historical Partition Planning

### 10.1 Range Authority

唯一日期范围来自 `CapacityPlanningRequest.history_start_trade_date` 与
`history_end_trade_date`。partition key 是 `decision_as_of_trade_date`，因此不把未来 label exit
date 错误加入 partition range。

planner 生成覆盖闭区间内所有月份的半开区间：

```text
child = app.advisory_outcome_label_payload_YYYYMM
FROM = first day of YYYY-MM
TO = first day of next month
```

existing fixture partitions 与目标集合相容时复用。目标范围之外的合法既有 partition 不删除；
同名 partition bound 不同则为 `DRIFTED`。

### 10.2 Partition DDL

partition DDL 仅由 release executor 根据 typed month descriptor 生成：

```sql
CREATE TABLE IF NOT EXISTS app.advisory_outcome_label_payload_YYYYMM
PARTITION OF app.advisory_outcome_label_payload
FOR VALUES FROM ('YYYY-MM-01') TO ('YYYY-MM-01');
```

identifier 由年月 formatter 生成，不接受任意 identifier 或 SQL 字符串。执行后必须从
`pg_inherits` 和 `pg_get_expr(relpartbound, oid)` readback 精确验证 parent 和 bound。

不创建 default partition，不在 Phase 1G/1H insert 失败时自动建分区。未来扩大日期范围时，使用
新的显式 Phase 1F request 扩展分区；不需要新审批实体或 runtime DDL。

## 11. DDL Execution Semantics

### 11.1 Plan Before Apply

`apply` 必须先在同一目标库重新生成 plan，并验证：

1. contract/migration file hash 与请求一致；
2. database identity 与请求一致；
3. 当前 catalog fingerprint 与 plan precondition 一致；
4. 状态为 `ABSENT`、`PARTIAL_ADDITIVE` 或 `COMPATIBLE`；
5. partition range 与 capacity request 一致。

其中没有 package、DSE、row count、Parquet、用户角色或人工批准条件。

### 11.2 Transaction

现有 migration transaction envelope 不统一，因此 Phase 1F 明确采用“单 migration 原子、release
可恢复”，不虚构跨六个文件 migration 加动态 partition DDL 的全局原子性：

- `FILE_WRAPPED` 文件在无 active transaction 的 dedicated connection 上以 driver
  `autocommit=true` 先执行 session-level `SET lock_timeout='10s'`、
  `SET statement_timeout='15min'`，再提交原始完整 bytes，由文件自身 `BEGIN/COMMIT` 保证单
  migration 原子；executor 不嵌套 transaction，也不剥离 envelope。dedicated connection 完成后
  关闭，session settings 不泄漏。
- `EXECUTOR_MANAGED` 文件由 executor 建立独立 transaction；执行后在 commit 前完成该 migration
  `declared_object_ids` 的 subset catalog verify，不一致则回滚该 migration；transaction 开始后先
  `SET LOCAL lock_timeout='10s'`、`SET LOCAL statement_timeout='15min'`。
- dynamic month partitions 在独立 executor-managed transaction 中原子创建并验证全部目标 bound。
- 每个已提交 migration 后都打开新的 `REPEATABLE READ READ ONLY` transaction 做 independent
  subset readback，再进入下一 order。
- migration transaction 一旦返回 committed，executor 必须先记录 migration hash、
  `status=COMMITTED` 和 `ddl_executed=true`，再做 independent readback；readback 失败不得把已经提交
  的 DDL 改写成 `ddl_executed=false` 或从 receipt 删除。
- later migration 失败时，当前 migration 回滚，但 earlier committed migrations 保留；failed receipt
  精确记录已提交 order。下一次 plan 必须把它们识别为兼容 existing objects，并只继续未完成步骤。
- lock/statement timeout 不自动重试；当前 migration 回滚并分别返回
  `PHASE1F_DDL_LOCK_TIMEOUT` 或 `PHASE1F_DDL_STATEMENT_TIMEOUT`，从而避免 release session 长时间
  等待或持有 Advisory 表锁。它不读取、锁定或终止 Selection、Paper、模拟盘、QMT、QE/Qlib
  connection。
- 全部 order 完成后执行 independent full catalog verify；失败返回
  `PHASE1F_POST_COMMIT_VERIFY_FAILED`，不得假成功，也不自动 destructive rollback，由 exact reapply
  或新 forward migration 修复。
- 不执行 `INSERT/UPDATE/DELETE`，不写 schema version row，也不创建 migration approval row。

### 11.3 Exact Reapply

当 preflight 已为 `COMPATIBLE` 时，不重放 migrations；只执行 read-only verify 并生成
`ddl_executed=false` receipt。该路径必须 exit 0，并与首次 apply 的 final catalog fingerprint
一致。

plan 会冻结生成时的 catalog fingerprint 和 `requested_operation`。首次 apply 改变 catalog 后，
原 pre-apply plan 必然失效；verify 必须在新 catalog 上生成新的 `requested_operation=VERIFY` plan，
exact reapply 也必须生成新的 `requested_operation=APPLY` plan。复用旧 plan 必须返回
`PHASE1F_PLAN_STALE` 且零 DDL/DML；这是防止 plan 漂移的正确行为，不是要求人工批准或额外门禁。

### 11.4 Existing Drift

`DRIFTED` 不允许通过 drop/recreate、disable trigger、删除 constraint 或修改已发布 migration
就地修复。实现者必须新增独立 forward migration、更新 contract version，并重新走设计/测试。
Phase 1F CLI 不提供 `--force`、`--skip-object`、`--ignore-drift` 或 arbitrary SQL bypass。

## 12. Runtime Schema Verification

Phase 1F 提供 Advisory-owned `verify_required_schema()`，但本阶段不把它接入 FastAPI startup 或
共享运行链。未来 Phase 1G/1H 独立 worker 在自己的入口开始时调用只读 verifier：

```text
expected release_schema_version
  -> fixed catalog projection
  -> COMPATIBLE / explicit mismatch
```

runtime verifier：

- 不加载 migration SQL；
- 不拥有 DDL executor reference；
- 不尝试修复或创建对象；
- 不因 Phase 1E L4、capacity `PARTIAL`、空表或未启动 observer 而失败；
- mismatch 只阻止对应 Advisory Phase 1 worker，不影响 Selection、荐股消费、模拟盘、Paper 或
  其他服务。

## 13. CLI And Code Ownership

### 13.1 Proposed Files

```text
backend/services/advisory_phase1/release_schema_contract.py
backend/services/advisory_phase1/release_schema_verify_postgres.py
backend/services/advisory_phase1/release_schema_apply_postgres.py
backend/services/advisory_phase1/release_schema_receipt_store.py
backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v1.json
scripts/advisory_phase1_release_schema.py
backend/tests/advisory_phase1/test_release_schema.py
backend/tests/advisory_phase1/test_release_schema_import_boundary.py
backend/tests/advisory_phase1/test_release_schema_dev_db.py
```

`release_schema_contract.py` 只包含 typed contract、canonicalization 和 partition planner；
`release_schema_verify_postgres.py` 只包含 fixed read-only catalog queries；
`release_schema_apply_postgres.py` 才能加载 migration bytes、设置 DDL session policy 并执行 DDL。
CLI 可以显式 import verifier 与 executor；未来 worker 只能 import contract 与 verifier。测试必须遍历
runtime verifier 的 transitive module graph，并拒绝 apply module、migration path 或 DDL verb常量。

若实现发现 frozen migration 有真实缺陷，只能在
`backend/db/migrations/add_advisory_phase1f_*.sql` 新增 forward migration并完成 disposable parity；
不得修改前述已发布 SQL，也不得为无法恢复原始历史 variant 的 canonicalization 伪造 rollback。

### 13.2 Commands

```text
plan
  --db-target dev|production
  --env-file <path>
  --capacity-request <json>
  [--phase1e-plan <json>]...
  --requested-operation plan|verify|apply
  --receipt-root <repo-external-path>

verify
  --db-target dev|production
  --env-file <path>
  --plan <json>
  --receipt-root <repo-external-path>

apply
  --db-target dev|production
  --env-file <path>
  --plan <json>
  --receipt-root <repo-external-path>

inspect-receipt
  --receipt <json>
```

CLI 不交互询问审批、不读取 Windows Credential Manager、不猜测 env。生产 `apply` 只有在用户
对该次操作明确授权后由执行代理调用；程序内部不创建审批流程。
`--phase1e-plan` 必须实现为 `required=False` 的 repeatable option，零次出现时形成规范空 tuple 和
稳定空集合 hash；不得因为 Phase 1E 缺少真实 DSE/receipt 而把它静默改成必填参数。

## 14. Errors And Logging

diagnostic reason 与 error reason 物理分开。diagnostic 不进入 `errors`、不改变 Phase 1F
`operation_status=SUCCESS`，但必须令 `downstream_ready=false`：

```text
PHASE1F_PREREQUISITE_SCHEMA_MISSING
PHASE1F_PREREQUISITE_SCHEMA_DRIFTED
```

至少冻结以下 error reason：

```text
PHASE1F_ENV_CONFIG_MISSING
PHASE1F_DATABASE_CONNECTION_FAILED
PHASE1F_DATABASE_IDENTITY_MISMATCH
PHASE1F_POSTGRES_VERSION_UNSUPPORTED
PHASE1F_CONTRACT_INVALID
PHASE1F_CONTRACT_HASH_MISMATCH
PHASE1F_MIGRATION_FILE_MISSING
PHASE1F_MIGRATION_HASH_MISMATCH
PHASE1F_SCHEMA_DRIFTED
PHASE1F_PARTITION_RANGE_INVALID
PHASE1F_PARTITION_BOUND_MISMATCH
PHASE1F_DDL_LOCK_TIMEOUT
PHASE1F_DDL_STATEMENT_TIMEOUT
PHASE1F_DDL_EXECUTION_FAILED
PHASE1F_TRANSACTION_VERIFY_FAILED
PHASE1F_POST_COMMIT_VERIFY_FAILED
PHASE1F_RECEIPT_COLLISION
PHASE1F_RECEIPT_STORE_FAILED
```

失败必须输出目标 label、去敏 database identity、contract/plan hash、migration order、object id、
expected/actual canonical fragment、transaction stage 和原始异常类型；password、DSN、完整 env
内容不得进入日志。每个 operation 只记录 start、meaningful stage transition、failure 或 summary，
不逐列输出成功日志。

退出码：

```text
0 operation_success_or_exact_reapply; prerequisite diagnostics may exist but downstream_ready=false
2 request_or_contract_invalid
3 environment_or_database_identity
4 schema_or_migration_drift
5 ddl_execution_or_transaction_verify
6 post_commit_verify_or_receipt_store
7 internal_error
```

失败不得返回 0，不得只写日志，不得生成 `operation_status=SUCCESS` 或
`managed_schema_status=COMPATIBLE` 的空 receipt。Phase 1G/1H 不得只检查退出码或
`operation_status`，必须检查 §8.3 的四项 downstream 条件。

## 15. 正向可达性

正常合法流程必须无需人工修改数据库即可通过：

```text
valid .env target
  + valid frozen contract
  + explicit capacity date range
  + prerequisite schema status independently reported
  -> plan ABSENT or PARTIAL_ADDITIVE
  -> ordered per-migration atomic apply and subset readback
  -> partition transaction
  -> independent full verify
  -> independent read-only verify COMPATIBLE
  -> immutable receipt
  -> exact reapply COMPATIBLE with ddl_executed=false
```

已有完整 schema 的库直接走 read-only verify；已有部分合法对象的库仅在 frozen migration 确实
能够补齐时归类 additive；只有真实定义冲突才要求 forward migration。Phase 0A prerequisite 缺口
单独报告但不阻止 managed apply。空表、零 DSE、零 receipt、空 `phase1e_plan_hashes`、capacity
`PARTIAL`、未启动 observer 和未生成 Parquet 都不会阻碍这一正向路径。

## 16. Verification Plan

### 16.1 L0 Static

- registry JSON schema、contract hash、migration path/order/dependency/hash 完整。
- migration files UTF-8、无 runtime DDL helper、无 arbitrary SQL input。
- required function body external relation refs 与 external prerequisite registry 完整闭合，至少覆盖
  `market.trading_calendar(cal_date,is_trading)`。
- verifier transitive import graph 不含 apply module、migration loader 或 DDL symbols；package
  `__init__` 不 re-export executor。
- changed paths 限于 Advisory Phase 1、migration、tests、script 和对应 design docs。
- role/RBAC/approval/authorization/backup hook、Selection/Paper/simulation/QE/Qlib imports 为零。

### 16.2 L1 Pure

- contract canonicalization、hash tamper、missing/duplicate order、dependency cycle。
- DEV/production env key selection，缺 key 不回退，receipt 去敏。
- capacity range 到 month descriptors 的跨年、单月、月末、非法范围测试。
- catalog payload canonicalization 与 per-kind/total fingerprint。
- managed `COMPATIBLE/ABSENT/PARTIAL_ADDITIVE/DRIFTED/UNSUPPORTED` 与 prerequisite
  `COMPATIBLE/MISSING/DRIFTED` 双轴分类正反例。
- receipt 的 operation/managed/prerequisite/downstream 四项推导、diagnostic/error 分离，以及
  Phase 1G/1H 不接受 `downstream_ready=false`。
- `--phase1e-plan` 零次/一次/多次输入、规范空 tuple/hash 和 argparse `required=False`。
- frozen DDL session policy/hash、CLI 无 override、timeout reason 与 no-retry。
- plan/request/receipt hash、no-replace、exact retry/collision。

### 16.3 L2 Disposable PostgreSQL Integration

使用测试代码自行启动的 pinned PostgreSQL 16 Docker image 创建一次性 cluster/database。测试不接收
DEV/production DSN、不读取 `.env`；每个测试销毁 database，module teardown 强制删除 container 并
验证不存在。`AISTOCK_PHASE1F_REQUIRE_L2=1` 只让缺少 Docker 时测试明确失败，不是运行期门禁，也
不是审批、备份或数据库连接配置。测试不依赖无法包住 file-level `COMMIT` 的外层 transaction：

1. 按完整顺序 apply 所有 migrations 和跨年分区。
2. full catalog verify `COMPATIBLE`。
3. second apply exact reapply，fingerprint 不变。
4. 缺列、错误 type/default、FK/check/predicate、disabled trigger、function body drift、错误 partition
   bound 分别得到精确 mismatch。
5. 缺失/漂移 `market.trading_calendar` 或 Phase 0A prerequisite 时仍允许 managed apply/verify，
   receipt 为 `operation_status=SUCCESS`、`downstream_ready=false`；Phase 1G consumer拒绝。
6. migration 中途失败时当前 migration 原子回滚、earlier committed orders 精确留证；再次 plan
   识别 `PARTIAL_ADDITIVE` 并只续跑未完成步骤。
7. verify transaction 确认 read-only；query spy 无业务表读取、DDL/DML 或 row lock。
8. 竞争 Advisory table lock 时实际触发 10 秒 lock timeout 并证明零自动重试；从 session readback
   验证 statement timeout 固定为 15 分钟并验证 timeout reason 分类。
9. 完成后销毁每个 disposable database 和整个 container，证明 DEV/production 零连接、零残留。

### 16.4 L3 Persistent DEV Release Rehearsal

代码审核通过后，在 Phase 1F DEV release rehearsal 中：

- 使用真实 `.env` DEV 目标执行 plan/apply/verify；
- 应用完整历史月分区并 commit；
- 独立新连接 readback full catalog fingerprint；
- 再执行 exact reapply，确认 `ddl_executed=false`；
- readback receipt 显式保存双轴状态、`downstream_ready`、diagnostics/errors 和 session policy hash；
- 保存 immutable receipt，核对数据库零业务 DML；
- 不运行 Phase 1G/1H，不启动 observer。

L3 是 Phase 1F DEV schema 状态证据，不是 Phase 1E dual-track L4、Phase 1I SEALED snapshot 或
生产激活证据。

### 16.5 L4 Production Apply And Readback

仅在用户明确授权该次生产 DDL 后执行：

- production plan/read-only preflight；
- apply exact frozen plan；
- 按 transaction mode 执行逐 migration subset readback，完成后新连接 full readback；
- immutable production receipt；
- exact reapply verify；
- `production_dml=false`、`runtime_activation=false`。

不执行额外全库备份；日常备份策略与本任务分离。没有授权或连接条件时只报告
`production_ddl_pending`，不影响代码/设计合入真实性。

## 17. Production Gates, Rollout And Rollback / 生产状态、发布与回滚

当前生产状态为：

```text
production_ddl_gate = applied_and_verified
production_dml_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
runtime_activation = noop
observer_activation = noop
model_training = noop
```

`production_ddl_gate=applied_and_verified`来自2026-07-15用户明确授权后的独立执行，不是应用内审批。
代码/设计合入本身没有自动改变该状态。

### 17.1 Rollout States

必须分别报告：

```text
design_state
code_merge_state
dev_schema_state
production_schema_state
phase1e_real_input_state
observer_activation_state
phase1g_dml_state
phase1h_dml_state
phase1i_snapshot_state
```

任何一项不得由另一项推断。

### 17.2 Rollback

- 单 migration transaction commit 前失败：由 PostgreSQL 自动回滚当前 migration；earlier
  committed migrations 按 receipt 保留并由后续 exact plan续跑。
- existing rollback SQL 只在 disposable database 中分别验证；由于 source ledger 没有完整 rollback
  对，rollback scripts 不作为全量 release 零残留或生产回退依据。
- committed production migration：不自动执行 destructive rollback；保留 additive schema，使用新
  forward migration 修复。已有 rollback 文件不是生产自动回退入口。
- Phase 1F 无 runtime activation，因此代码回滚不需要停 Selection、模拟盘、Paper 或荐股页面。

## 18. Impact Matrix

| subsystem | read | write | behavior change |
|---|---:|---:|---|
| Advisory Phase 1 PostgreSQL schema | catalog + migration DDL | DDL only in explicit release command | 完整 schema/partition 可验证 |
| Advisory Phase 0A/0A.2 | prerequisite catalog only | none | none |
| Selection Center | none | none | none |
| StrategyPackage inference | none | none | none |
| Advisory current list consumer/API/UI | none | none | none |
| Simulation/Paper/QMT | none | none | none |
| QE/RD-Agent/Qlib/backtest | none | none | none |
| market/source business rows | none | none | none |
| filesystem | explicit repo-external receipts | atomic no-replace | release evidence only |

## 19. Risks And Failure Modes

| risk | consequence | treatment |
|---|---|---|
| `IF NOT EXISTS` 掩盖错误同名对象 | 新代码在运行期失败 | full catalog contract + post-apply verify |
| migration 文件被原地修改 | 同名 release 不可复算 | file SHA freeze；新缺陷只加 forward migration |
| DEV key 缺失回退 production | 误操作生产库 | exact env key mapping；no fallback |
| 只看表名 | constraint/trigger/function 漂移 | 全对象 normalized contract |
| current OID/统计进入 hash | reapply 或跨库误报 | OID/row/statistics excluded |
| function normalizer 吞掉语义 | 漂移被接受 | 只移除展示噪音；body/predicate/event保留 |
| 分区范围猜测 | 历史 label 中途失败 | explicit capacity range -> deterministic months |
| default partition 吞掉缺口 | 范围失控 | no default partition |
| post-commit verify 失败却返回成功 | 假发布 | non-zero + immutable failed receipt + forward fix |
| 单一 verification status 掩盖 prerequisite 缺口 | Phase 1G 消费未就绪 schema | operation/managed/prerequisite/downstream 四项显式字段 |
| trigger 外部 relation 未进入 contract | 首次 observation insert 才失败 | function external-ref closure + market calendar prerequisite |
| verifier 与 executor 同模块 | worker 误获得 DDL 能力 | module-level split + transitive import denylist |
| DDL 无限等待表锁 | Advisory writer 或发布任务长时间等待 | 10s lock/15min statement timeout + no retry |
| optional Phase 1E plan 被实现成必填 | 无真实 DSE 时错误阻塞 Phase 1F | optional repeatable CLI + empty tuple/hash tests |
| runtime 自动补 DDL | 共享服务权限和稳定性受影响 | executor 与 verifier 模块物理分离 |
| 空 DSE/Parquet 被设为 schema 阻塞 | 正常流程无法启动 | schema verify 明确不读取业务 rows |
| 新增审批/角色 | 单用户流程被人为阻塞 | code/schema scan + zero approval entities |
| 每次 DDL 全库备份 | 时间与存储不可接受 | 不实现备份 hook；沿用每日备份 |

## 20. Implementation Plan / 实施方案

### F1：Contract And Pure Planner

- typed registry、双轴 request/plan/receipt、external prerequisite closure、hash normalizer、env target、
  optional Phase 1E lineage 和 partition planner；
- L0/L1；
- 不连接数据库、不执行 DDL。

### F2：Read-only Catalog Verifier

- fixed catalog SQL、canonical object projection、difference classification；
- 独立 runtime-safe verifier module 与 executor import denylist；
- disposable PostgreSQL catalog tests 与真实 DEV read-only verify。

### F3：Release Executor And Receipt Store

- 独立 release-only executor、frozen migration loader/session timeout policy、ordered transaction、
  partition DDL、pre/post verify、atomic receipt；
- no arbitrary SQL/no bypass/no DML；
- disposable PostgreSQL apply/reapply/partial-resume/database-destroy tests。

### F4：DEV Release Rehearsal

- persistent DEV plan/apply/verify/exact reapply；
- full receipt、zero business DML evidence；
- 更新 acceptance matrix 的真实 implementation/evidence。

### F5：Production Apply

- 不是代码开发自动步骤；用户明确授权后单独执行 production plan/apply/readback；
- 不启动 observer，不进入 Phase 1G/1H DML。

## 21. Design Acceptance Index

- F-601：Phase 1F 只管理 release schema，不执行业务 DML、文件构建、模型训练或 runtime activation。
- F-602：六个文件 migration 与动态 partition DDL 的 path/order/dependency/SHA/transaction mode 全部进入 typed contract。
- F-603：31 个逻辑关系及目标 partitions 的完整 catalog closure 被验证，不是表名子集。
- F-604：Phase 0A prerequisite 只读核对并独立报告；缺口不阻止 managed schema apply，不重复
  package/Selection 验证或修改其对象。
- F-605：DEV/production 连接只读取 `.env` exact keys，无默认值、猜测或 DEV->production fallback。
- F-606：database identity 去敏冻结，plan/apply/verify 目标一致。
- F-607：capacity 日期范围确定性生成完整月分区，跨年/边界闭合，无 default partition。
- F-608：migration/partition DDL 只能来自 frozen contract/typed planner，无 arbitrary SQL。
- F-609：catalog normalizer 保留 column/constraint/index/function/trigger/partition 业务语义。
- F-610：managed schema 与 downstream prerequisite 双轴自动分类明确，且 managed 正向路径不被
  prerequisite、空 DSE 或空 Phase 1E plan hashes 阻断。
- F-611：fresh apply、partial additive apply、exact reapply 和 read-only verify 均有正向测试。
- F-612：同名错误对象、hash drift、错误分区和 disabled trigger 明确失败，不静默成功。
- F-613：每个 migration 自身原子；release 中途失败精确留证 earlier commits 并可续跑，不虚构
  全局回滚；post-commit failure 不假成功。
- F-614：receipt atomic/no-replace、可复算、去敏，same content 幂等、different content 冲突。
- F-615：runtime verifier 与 DDL executor 物理分离，runtime 无 DDL 入口或自动修复。
- F-616：schema verify 不依赖 row count、DSE、Phase 1E L4、capacity MEASURED、Parquet 或模型状态。
- F-617：Phase 1F 不 import/call Selection、StrategyPackage inference、Paper、模拟盘、QMT、QE、
  RD-Agent 或 Qlib。
- F-618：无角色、RBAC、审批、授权表、人工确认、双人复核或 manual override。
- F-619：不新增每次 DDL 前全库备份要求或备份 hook。
- F-620：DEV persistent apply 与生产 apply 分开报告，代码合入不冒充 production schema ready。
- F-621：生产 apply 只在用户明确授权的独立操作执行，不编码审批系统。
- F-622：生产 committed schema 使用 forward fix，不自动 destructive rollback。
- F-623：设计与父蓝图、Phase 1、Phase 1D/1E 和后续 1G/1H/1I 边界一致。
- F-624：错误日志包含可诊断上下文且去敏，失败非零退出，无空成功 receipt。
- F-625：plan/receipt 显式保存 operation、managed、prerequisite、downstream 四项状态；diagnostic
  不冒充 error，Phase 1G/1H 不接受 `downstream_ready=false`。
- F-626：managed function/trigger 的 external relation refs 完整进入只读 prerequisite registry；
  v1 精确覆盖 `market.trading_calendar(cal_date,is_trading)`，不修改 market 数据。
- F-627：read-only verifier 与 DDL executor 文件级隔离，runtime transitive import graph 无 executor、
  migration loader 或 DDL symbol。
- F-628：DDL session 使用 contract-frozen 10 秒 lock timeout、15 分钟 statement timeout、零自动
  重试；超时准确回滚当前 migration 并留证。
- F-629：`--phase1e-plan` 明确为 optional repeatable option；零输入形成稳定空 tuple/hash，不因
  Phase 1E 缺少真实输入阻塞 Phase 1F。

## 22. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-601 | §1、§4、§18 | scope/import/DML scan | design_ready | none |
| F-602 | §3.1、§6.1 | migration manifest/hash/order tests | design_ready | none |
| F-603 | §3.2、§6.2、§9 | fresh catalog parity and drift matrix | design_ready | none |
| F-604 | §3.1、§6.3、§9.2 | prerequisite read-only/missing but managed-apply-positive tests | design_ready | none |
| F-605 | §7.1 | env exact-key/no-fallback tests | design_ready | none |
| F-606 | §7.2、§8 | identity mismatch/redaction tests | design_ready | none |
| F-607 | §10 | single/cross-year/month-bound partition tests | design_ready | none |
| F-608 | §8.2、§10.2、§11 | SQL provenance and injection negative tests | design_ready | none |
| F-609 | §6.2、§9 | per-object normalizer parity/tamper tests | design_ready | none |
| F-610 | §9.2、§15 | managed/prerequisite dual-axis classification tests | design_ready | none |
| F-611 | §11、§15、§16 | disposable apply/reapply/partial-resume plus DEV verify | design_ready | none |
| F-612 | §9、§14、§16 | schema drift direct negative matrix | design_ready | none |
| F-613 | §11.2、§16.3 | per-migration rollback, partial-resume and post-commit failure tests | design_ready | none |
| F-614 | §8.3 | receipt no-replace/retry/collision tests | design_ready | none |
| F-615 | §5.2、§12、§13 | import graph and query-spy tests | design_ready | none |
| F-616 | §5.1、§12、§15 | empty DB rows/PARTIAL capacity positive tests | design_ready | none |
| F-617 | §5.2、§18 | frozen shared entrypoint zero-diff scan | design_ready | none |
| F-618 | §4.2、§21 | approval/RBAC/override scan | design_ready | none |
| F-619 | §4.2、§16.5、§19 | backup hook/full-dump scan | design_ready | none |
| F-620 | §16、§17 | separated state receipt/reporting tests | design_ready | none |
| F-621 | §4.2、§13.2、§16.5 | production command workflow review | design_ready | none |
| F-622 | §11.4、§17.2 | forward-fix/no-auto-rollback tests | design_ready | none |
| F-623 | §1、§18、§23 | parent-reference consistency review | design_ready | none |
| F-624 | §14 | reason/redaction/exit-code tests | design_ready | none |
| F-625 | §8.2-8.3、§9.2、§14 | dual-axis receipt/consumer/diagnostic-error tests | design_ready | none |
| F-626 | §6.1-6.3、§16 | function external-ref and market calendar catalog tests | design_ready | none |
| F-627 | §5.2、§12-13、§16 | verifier/executor transitive import denylist | design_ready | none |
| F-628 | §6.1、§8、§11.2、§14、§16 | session policy/hash/lock/statement timeout tests | design_ready | none |
| F-629 | §8.1、§13.2、§15-16 | optional zero/one/many Phase 1E plan tests | design_ready | none |

### 22.1 Implementation Evidence Status / 实现验收状态（2026-07-14）

本轮实现已建立以下 repo-owned 交付物：

```text
backend/services/advisory_phase1/release_schema_contract.py
backend/services/advisory_phase1/release_schema_verify_postgres.py
backend/services/advisory_phase1/release_schema_apply_postgres.py
backend/services/advisory_phase1/release_schema_receipt_store.py
backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v1.json
backend/db/migrations/add_advisory_phase1f_schema_canonicalization_20260714.sql
scripts/advisory_phase1_release_schema.py
backend/tests/advisory_phase1/test_release_schema*.py
.gitattributes
.gitignore
```

| design items | implementation evidence | current status | remaining evidence |
|---|---|---|---|
| F-601 to F-610 | typed frozen contract, full catalog registry, readonly verifier, exact env resolver, dual-axis plan, typed month planner | local implementation verified | none for code scope |
| F-611 to F-613 | release executor, committed-before-readback receipt semantics, per-migration atomic rollback, partial-resume and post-commit failure tests | L2 + persistent DEV verified | none for DEV schema scope |
| F-614 to F-619 | durable atomic no-replace receipt store, transitive import denylist, no approval/RBAC/backup/runtime-DML paths | local and L2 verified | none for code-merge scope |
| F-620 to F-626 | separate DEV/production labels, external production execution boundary, forward-fix policy, structured receipt/diagnostic fields, market calendar catalog prerequisite | local + persistent DEV verified | later production execution remains separate |
| F-627 to F-629 | verifier/executor physical split, frozen session timeout policy, optional repeatable `--phase1e-plan`, explicit requested operation | local and L2 verified | none for code-merge scope |

2026-07-14 已使用 `.env` 的 DEV exact keys 完成 persistent L3 release rehearsal。输入 request hash 为
`8ecc1520ee07e24fadbcadad09cd5642bc21e55e4679111a19dc448a737ec4a0`；首次 apply plan 为
`39cc17ce83b96982b0d34f7c6be439987715df8e1436fd937131a552919447d6`，apply receipt 为
`db0e76df4753830efa87b11e317f2a8a8b88fe194c9a9088d743ce79d17935b2`。它只原子提交唯一 pending
order 50 migration，结果为 `SUCCESS/COMPATIBLE/COMPATIBLE/downstream_ready=true`。

在 apply 后的新 catalog 上，新的 VERIFY plan/receipt 分别为
`6075ca6ee46728388ddfa33524d16bbb87023689a5265ee458b2b3810b354026` /
`6bfee9a73550b66953d8c0ba45fe79d1fc0bb960e71d3d096127c368b3c21be6`；新的 APPLY exact-reapply
plan/receipt 分别为 `96684eb234712027c2a5845001544d1015dffdbb8835e8ec0cd73a6a3091885e` /
`7943500ee06ae8081509a886af61708a6cb01ba23c09f447e20bb58c5f357925`。两次均为
`ddl_executed=false`、`dml_executed=false`、`runtime_activation=false`，最终 catalog fingerprint
一致为 `b948c1c9329f7c7932f08ded03ec7c7007562b6e0de8ea75edd6820c0af85272`。

该 request 中 DEV Advisory Program count 为 0，因此上述结果只证明 schema release/reapply 完整，
不冒充 Phase 1E 真实 dual-track L4、Phase 1G persistent observation DML 或 capacity readiness。
本次历史v1 DEV rehearsal未执行production操作；后续Phase 1F.1独立发布已于2026-07-15完成production
DDL。至今仍未执行production DML、observer startup、Phase 1G/1H DML、Parquet构建或模型训练。

Phase 1G详细设计复核进一步发现：v1 registry把stage/candidate局部content hash冻结为全局UNIQUE，且
lineage/candidate仍为普通表，与Phase 1父设计的作用域唯一和月分区要求不一致。因此上述
`downstream_ready=true`只在v1 contract内成立，不能被解释为Phase 1G persistent DML已具备最终schema。
forward修正的唯一详细设计为
`docs/architecture/advisory_phase1f1_observation_partition_schema_forward_migration_f2_design_20260714.md`；
其代码已由PR `#2129`合入，DEV与production DDL已于2026-07-15应用并完整验证。

实现完成后的合并前证据：pure/static + pinned disposable PostgreSQL consolidated matrix 为
`38 passed`，其中 L2 为 `18 passed`；fresh apply、exact reapply、九类 drift、partial resume、
executor/file-wrapped rollback、committed-readback failure、lock timeout、session policy、query spy、
database/container destroy 全部通过。该证据本身不表示persistent DDL已执行；实际DEV/production执行
事实由Phase 1F.1独立immutable receipts证明。

## 23. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：完整验证 relation/column/constraint/index/function/trigger/comment/
  partition，不以表存在或 migration exit 0 冒充 schema closure。
- [x] `no_silent_error`：migration/hash/catalog/partition/transaction/receipt 错误均有稳定 reason、
  去敏 context 和非零退出；post-apply verify 是同一 operation 的必要组成。operation、managed、
  prerequisite、downstream 四项状态不能由单一 success 字段覆盖。
- [x] `no_business_semantic_drift`：只管理 Advisory Phase 1 schema，业务 DML、当前荐股、Selection、
  StrategyPackage inference、模拟盘、Paper、QE/RD-Agent/Qlib/QMT 零行为变化；对
  `market.trading_calendar` 只做 catalog read，不读取或修改 market rows。
- [x] `no_unrequested_gate_or_approval`：没有角色、RBAC、审批、授权链、人工确认表、双人复核、
  manual override 或额外备份。结构结果是自动事实分类，不是审批。
- [x] `positive_path_satisfiable`：frozen contract + explicit date range 可自动 fresh apply/partial
  apply/verify/exact reapply；prerequisite 独立报告，空业务表、空 Phase 1E plan hashes 和 capacity
  `PARTIAL` 不阻塞 managed schema。
- [x] `database_connection_truth`：只使用 `.env` exact DEV/production keys，不猜测连接，不输出密码。
- [x] `release_runtime_separation`：代码合入、DEV apply、production apply、observer activation、
  Phase 1G/1H DML 分开报告。
- [x] `training_boundary`：不读取回测数据、不生成训练集、不训练模型；未来训练只在 WSL/Conda。

## 24. Exit Criteria And Next Phase

设计可标记 `design_ready` 的条件：

1. F-601 至 F-629 全部 `design_ready`，无未批准 exception/TODO。
2. 父蓝图、Phase 1 父设计和 Phase 1E 状态同步。
3. F2 validator、`git diff --check` 和文档引用检查通过。
4. 没有额外角色、审批、授权、备份、shared runtime 或业务 DML设计。

代码请求合入条件已由 PR `#2114` 满足：

1. F1-F3 实现完整，L0-L2 全部通过。
2. disposable PostgreSQL full-order apply/reapply/drift/partial-resume matrix 通过且测试库销毁。
3. DESIGN-COMPLIANCE-001 按实现逐项复核，无简化版、静默错误或业务偏移。
4. production gates 准确报告；2026-07-15授权执行前保持`production_ddl_pending`，授权后的实际状态为
   `applied_and_verified`，不得混淆时间点。

Phase 1F v1 DEV已完成F4 persistent release rehearsal、独立readback、exact reapply和immutable
receipt；Phase 1F.1 forward contract也已在DEV/production应用并验证。Phase 1G的唯一实施级设计为
`docs/architecture/advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md`，当前为
`design_ready_after_consistency_review / implementation_not_started / blocked_pending_phase1f2`；它只消费
exact Phase 1F.2 scope-aware receipt和`managed_schema_status=COMPATIBLE`、
`prerequisite_status=COMPATIBLE`、`downstream_ready=true` 的target schema。该前置条件尚未满足。

Phase 1F/1F.1 production schema完成条件已满足：用户明确授权后完成production apply/readback/new verify/
exact reapply receipts。这没有自动启动observer、Phase 1G/1H DML、Phase 1I snapshot或任何模型能力。
