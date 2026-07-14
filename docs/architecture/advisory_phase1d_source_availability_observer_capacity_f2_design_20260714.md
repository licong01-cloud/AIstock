# AIstock Advisory Phase 1D Source Availability Observer And Capacity F2 Design

## 1. Background / 文档定位

本文是 `advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`
中 Phase 1D 的唯一实施级详细设计，承接已经合入的 Phase 1C-3 Batch D：

```text
production market ingestion
  -> mutable market.dataset_date_refresh_audit completion signal
  -> default-off standalone Advisory observer
  -> registered read-only source partition query
  -> canonical schema/row/content verification
  -> immutable app.advisory_source_availability_event
  -> Phase 1E source/readiness planning

read-only database inventory
  -> deterministic capacity probe
  -> content-addressed capacity receipt
  -> Phase 1E/1I sizing input
```

Phase 1C-3 Batch D 已证明 fixture/development 数据能够经过真实 PostgreSQL、确定性
Parquet、CAS、promotion 和 seal 到达 `SEALED`。Phase 1D 不重复实现 snapshot builder，
而是补齐“生产数据何时被本系统真实观察到”和“后续历史 observation/label/snapshot 需要
多少数据库、内存、临时空间和持久存储”的能力。

本阶段为 `F2`，因为未来实现会跨越可变 `market` ingestion 辅证、append-only `app`
Advisory evidence、独立 worker 和离线容量探针。但它不接入 StrategyPackage、Selection、
Paper v2、模拟盘、QMT、荐股 consumer、模型训练或交易执行。

本文设计对应代码已由 PR `#2067` 合入并完成 DEV migration、真实 PostgreSQL E2E、容量探针、
rollback-no-residue 与 F2 验收。生产 DDL 仍为 `pending`，observer 未激活，未读取或写入生产
数据库，也未启动 scheduler。下一阶段实施级设计为
`docs/architecture/advisory_phase1e_dual_track_readiness_execution_plan_f2_design_20260714.md`。

## 2. Parent Baseline / 父级基线与当前事实

### 2.1 已存在并必须复用

1. `SourceAvailabilityEventRequest`、`SourceAvailabilityEvent` 和
   `PostgresSourceAvailabilityLedger` 已实现 repository-controlled
   `first_observed_at`、append-only event chain、单后继和 exact retry。
2. `app.advisory_source_availability_event` 已具有自然 partition/revision 唯一约束、
   database clock 检查、immutable trigger 和 as-of index。
3. `SourceRequirement`、`SourceResolutionReceipt` 和 source revision set 已消费
   availability event；Phase 1D 不创建第二套 source authority。
4. `market.dataset_date_refresh_audit` 是全系统当前 dataset/date readiness 辅证，采用
   `(dataset, trade_date, data_source)` upsert；它是可变当前状态，不是不可变历史。
5. Batch D 已有真实 deterministic Parquet/CAS/SEALED golden，但它是 fixture/development
   数据集，不代表生产 observer、生产历史数据积累或模型训练已经启用。

### 2.2 必须修正的语义

- `market.dataset_date_refresh_audit` 只能作为 observer 发现 ingestion completion 的输入，
  不能直接成为 `formal_available_at`、revision identity 或训练数据 authority。
- `refreshed_at` 是本地 audit row 更新时间，不能冒充 provider 发布时间。
- observer 启用前存在的历史数据不能回填或猜测当时的 availability。若在启用后观察旧
  partition，其 `first_observed_at` 仍只能是当前 database time，并且 v1 默认不扫描配置
  `effective_from_observed_at` 以前的 audit revision。
- ingestion success 只说明刷新过程成功；必须通过注册查询重新核对 source partition 的
  schema、行数、质量和完整 content hash，才能追加 PASS availability event。
- ingestion failure 不等于旧 source revision 已失效。v1 不依据 failed audit row 自动追加
  `INVALIDATED`，避免错误降级已经合法的历史 evidence。

## 3. Scope / 范围

### 3.1 In Scope

1. versioned `SourceObserverConfigBundle` 与编译期 query/partition registry。
2. 默认关闭、独立进程运行的 `SourceAvailabilityObserver.run_once()`。
3. restart-safe observer cursor 和 immutable observation receipt。
4. 对 audit completion row 与真实 source partition 的一致读取、全量 canonical hash 和
   availability event type 推导。
5. `INGESTED`、`CORRECTED`、合法 `REVALIDATED`、`UNCHANGED` 和 `NOT_ELIGIBLE`
   的确定性行为。
6. per dataset/source/role failure isolation、exact retry、并发串行化和有价值日志。
7. 只读 `AdvisoryPhase1CapacityProbe`、容量公式和 deterministic capacity receipt。
8. standalone offline CLI：`observe-once`、`capacity-plan`、`verify-receipt`。
9. DEV migration、repository、pure/DB/CLI 验证设计和生产发布状态定义。

### 3.2 Non-goals

- 不修改 `market` 源表、`market.dataset_date_refresh_audit` 或 ingestion job 状态。
- 不在 `DataRefreshAuditRepository.record_success()`、Tushare sync、TDX scheduler 或 Go
  ingestion 中注入同步 callback、trigger 或 Advisory 写入。
- 不接入 FastAPI startup，不新增 API/UI，不接入现有 Selection、Paper、模拟盘 scheduler。
- 不执行历史回填，不猜测 provider published time，不把当前 audit row 解释为历史 OOS
  availability。
- 不创建 capture batch、observation、label、dataset build 或 SEALED snapshot。
- 不读取回测结果、Qlib bin、回测 Parquet 或策略包历史回测制品。
- 不训练模型。未来模型训练仍只能在 WSL/Conda；Phase 1D 的 Windows 进程只执行数据库
  观察、hash、容量分析和 receipt 验证。
- 不创建用户、角色、RBAC、授权、审批、人工确认、manual override 或双人复核。
- 不要求 DDL 前执行全库导出或额外备份。生产日常备份与未来 migration 授权是独立事实。

## 4. Architecture And Authority / 架构、权威与隔离

| 对象 | 权威来源 | Phase 1D 用法 | 明确禁止 |
|---|---|---|---|
| ingestion completion | `market.dataset_date_refresh_audit` 当前 row | 发现候选 audit revision | 当作不可变 availability authority |
| source data | registry 绑定的 `market` table/query | read-only partition stream | UPDATE/DELETE/修复源表 |
| provider published time | allowlisted typed metadata 且有 provider 语义 | 可选写入 event | 使用 `refreshed_at` 或 job finish time 猜测 |
| first observed time | PostgreSQL `clock_timestamp()` | repository 生成 | CLI/config/caller 传入或回填 |
| availability evidence | `app.advisory_source_availability_event` | 唯一 authority | audit row、日志或 cursor 替代 |
| observer processing evidence | `app.advisory_source_observation_receipt` | 证明 audit row 如何处理 | 作为 source revision member |
| capacity evidence | content-addressed JSON receipt | Phase 1E/1I sizing 输入 | 作为模型训练或交易输入 |

observer 只具有以下数据方向：

```text
SELECT market.dataset_date_refresh_audit
SELECT registered market source partition
INSERT app.advisory_source_availability_event
INSERT app.advisory_source_observation_receipt
INSERT/UPDATE app.advisory_source_observer_cursor
```

不得授予或调用 StrategyPackage、Selection、Paper、simulation、QMT、broker、order、position
相关 repository。cursor 的 UPDATE 只是 worker checkpoint，不是业务数据修改、审批状态或
availability authority。

## 5. Versioned Configuration / 版本化配置

### 5.1 `SourceObserverConfigBundle`

```text
schema_version = advisory_phase1_source_observer_config_v1
observer_config_id
observer_config_version
effective_from_observed_at
query_registry_hash
poll_interval_seconds
audit_scan_batch_size
source_fetch_rows
statement_timeout_ms
lock_timeout_ms
serialization_retry_limit
max_partition_rows
max_partition_bytes
created_by_service_principal
dataset_specs[]
config_hash
```

规则：

- config 为代码仓库中的 versioned typed registry；环境变量只能选择已注册 config id/version
  和是否启动独立 worker，不能注入任意 SQL、table、column 或 hash 算法。
- `effective_from_observed_at` 是 observer 首次允许发现 audit revision 的 database-time 边界。
  早于该边界的 row 不处理，不用当前时间补造历史 evidence。
- 数值 resource bounds 必须来自 Phase 1D capacity receipt；实现初始 DEV fixture 值只用于
  测试，生产 config 在 receipt 形成后冻结。不得在超限时缩小数据或抽样后假成功。
- `serialization_retry_limit` 是 PostgreSQL `40001`/deadlock 的有限自动重试次数；耗尽后输出
  `CURSOR_CONFLICT`，不是人工确认、审批或无限重试。
- config hash 覆盖全部字段和有序 dataset specs；同 id/version 不同 hash 是 conflict。
- config 不含 `approved_by`、role、authorization、approval chain、manual gate 或人工状态。

### 5.2 `ObservedDatasetSpec`

```text
dataset_name
allowed_data_sources[]
source_roles[]
audit_partition_mapper_id/version/hash
source_query_template_id/version/hash
source_schema_id/version/fingerprint
canonical_columns[]
canonical_sort_columns[]
provider_published_at_extractor_id nullable
eligible_audit_statuses = [success]
eligible_quality_statuses[]
empty_partition_policy
row_count_parity_policy
content_hash_algorithm = canonical_stream_sha256_v1
```

- SQL identifier 和 query text 只能来自编译期 registry；audit metadata 不参与 SQL 拼接。
- 一个物理 dataset 可按多个 `source_role` 追加独立 event chain；每个 role 复用同一 source
  partition descriptor/hash，但具有独立 partition chain key。
- `empty_valid` 只有 dataset spec 明确声明合法且 audit/source 都为零行时才可映射 PASS。
- schema fingerprint 来自显式列名、类型、nullable、单位和时区，不使用 table modification
  timestamp 代替。

## 6. Persistence Plan / 持久化计划

Phase 1D 未来代码实现需要一份 additive migration，新增两个 Advisory operational/evidence
对象，不修改现有 source ledger 和 market 表。

### 6.1 `app.advisory_source_observer_cursor`

```text
observer_config_hash
dataset_name
data_source
source_role
last_audit_refreshed_at
last_trade_date
last_audit_row_hash
row_version
updated_at

PK(observer_config_hash,dataset_name,data_source,source_role)
```

- 每个 dataset/source/role 独立 cursor；一个 scope 出错不阻断其他 scope。
- cursor natural ordering 固定为
  `(refreshed_at, trade_date, dataset, data_source)`。discovery 每次从
  `last_audit_refreshed_at` 的等值边界开始重放全部 rows，再以 audit row hash 和 immutable
  receipt 去重；不能只使用严格 `refreshed_at > cursor`。因此相同 timestamp 的多 rows、同一
  natural row 在相同 timestamp 下发生 content change，均不会被漏掉。
- cursor row 在单个 input transaction 中 `FOR UPDATE`，只有 terminal receipt 与必要 source
  event 同事务成功后才前进。
- `row_version` 用于进程并发和诊断，不是人工 expected-version 审批。

### 6.2 `app.advisory_source_observation_receipt`

```text
observation_receipt_id
observation_receipt_hash UNIQUE
observer_config_id/version/hash
dataset_name/data_source/source_role
trade_date/partition_key/partition_key_hash
audit_refreshed_at/audit_row_hash
outcome = EVENT_APPENDED | UNCHANGED | NOT_ELIGIBLE
availability_event_id/hash nullable
observed_schema_fingerprint nullable
observed_row_count nullable
observed_partition_content_hash nullable
reason_codes
observed_at
created_at

UNIQUE(observer_config_hash,audit_row_hash,source_role)
```

- receipt 为 append-only；UPDATE/DELETE trigger 拒绝修改。
- `EVENT_APPENDED` 必须具有 event id/hash 和完整 source descriptor。
- `UNCHANGED` 必须引用当前 terminal event，且 descriptor 与 terminal 逐字段相等。
- `NOT_ELIGIBLE` 不得引用 event，必须有 reason code；它表示该 audit revision 不满足追加
  条件，不表示旧 event 被 invalidated。
- repository 使用 insert-do-nothing 后完整 readback 比较，禁止 `ON CONFLICT DO UPDATE`。
- unexpected infrastructure/transaction error 不写 terminal receipt、不移动 cursor，进程输出明确
  error；修复后相同 input 自动重试。正确配置和数据具有完整正向通过用例。

### 6.3 Migration Boundary

- migration 只在开发/发布阶段显式执行，runtime 无 DDL 权限和入口。
- migration 必须可重复 apply，并具备空库、升级库、trigger/index/comment 和 rollback-no-residue
  DEV 验证。
- 设计文档合入不需要 DDL；未来代码 PR 在 migration 未获生产执行授权前只报告
  `production_ddl_gate=pending`，不虚构生产 ready。
- 不因 migration 创建数据库 role、GRANT/REVOKE、审批表或人工操作链。

## 7. Observer Algorithm / Observer 算法

### 7.1 Discovery

每个 scope 的 `run_once()`：

1. 加载并 canonical validate frozen config/registry。
2. 在短事务中锁定对应 cursor row；不存在时以
   `effective_from_observed_at` 初始化，不扫描更早 row。
3. 从 `market.dataset_date_refresh_audit` 按固定 composite order 读取下一批发生变化的 rows；
   查询包含 cursor timestamp 的等值边界，已经存在 exact terminal receipt 的 input 只做
   readback，不重复 source hash 或 event append。
4. 对每个 audit row 计算 `audit_row_hash`，覆盖 dataset/date/source/job/status、row count、
   refreshed/data-max time、coverage、quality、failure category 和 canonical metadata。
5. 每个 input 使用独立 bounded transaction；一个失败不回滚其他 scope 已完成事务。

不得使用 `ingestion_jobs.finished_at`、文件 mtime 或当前 source max date 代替 audit discovery。
没有 audit row 的 source 不产生 availability event。

### 7.2 Eligibility

按顺序判断：

1. audit status 是否在 spec allowlist；v1 只有 `success`。
2. quality status 是否被 spec 接受。
3. empty partition 是否满足显式 empty policy。
4. provider published timestamp 是否来自 allowlisted extractor；无法证明时保持 NULL。
5. partition mapper 是否生成 canonical partition key。

不满足时写 `NOT_ELIGIBLE` receipt 并推进该 input cursor；reason code 必须具体。failed audit
row 不自动产生 `INVALIDATED`。

### 7.3 Source Read And Hash

eligible input 在一个 `REPEATABLE READ` transaction 中：

1. 通过 registry query template 绑定 partition 参数。
2. 设置 local statement/lock timeout，使用 server-side cursor/`fetchmany` 有序流式读取。
3. 验证实际 schema fingerprint。
4. 对全部 canonical rows 计算 row count 和 `canonical_stream_sha256_v1`，禁止 sampling。
5. 验证 audit row count/coverage 与实际 source descriptor 的 policy parity。
6. 超过 row/byte/time bound 明确失败并保持 cursor；不得截断后追加 event。

source query 只 SELECT。observer 不修复、填充、更新或锁写 market source rows。

### 7.4 Event Type

```text
no prior event
  -> INGESTED revision 1

prior terminal PASS + same schema/count/content
  -> UNCHANGED receipt, no new event

prior terminal PASS + new valid content descriptor
  -> CORRECTED next revision with exact predecessor

prior terminal INVALIDATED + new valid different content descriptor
  -> REVALIDATED next revision with exact predecessor
```

Phase 1D v1 不从普通 failed/low-quality audit 自动生成 `INVALIDATED`。未来若需要自动
invalidation，必须先设计独立、可证明的 invalidation signal 契约；不得通过本阶段 metadata
布尔值或错误消息猜测。

`revision_id` 由 dataset/source role/partition/query/schema/content/provider job descriptor 的
canonical hash 派生，不能只使用 mutable audit key 或 job id。
当 source content 未变化但 audit job/data source 变化时，existing ledger 的 content-identity
约束要求复用原 event，并由新 observation receipt 绑定本次 audit/data source；不得为同内容
伪造 `CORRECTED` event。只有新 valid content 形成的新 event 才使用新的 revision identity。

### 7.5 Atomic Commit And Retry

每个 input 的 commit 顺序：

```text
lock/revalidate cursor
  -> re-read audit input in transaction snapshot
  -> read/hash source partition
  -> append or exact-read source event
  -> append exact observation receipt
  -> advance cursor row_version
  -> commit
  -> complete readback
```

- observer 需要为 `PostgresSourceAvailabilityLedger` 提取 transaction-bound internal append
  primitive；公共 `append()` 行为和测试保持不变。
- 任一步失败则整个 input transaction 回滚；event、receipt、cursor 不允许半成功。
- commit response 丢失时重新运行，通过 audit hash、event request hash 和 receipt hash exact
  readback 收敛到同一结果。
- 多 worker 通过 per-scope cursor row lock 和现有 per-chain advisory lock 串行化；不产生 fork。

## 8. Runtime Placement / 运行位置

### 8.1 Standalone Only

未来实现入口：

```text
scripts/advisory_phase1_source_observer.py observe-once
scripts/advisory_phase1_source_observer.py capacity-plan
scripts/advisory_phase1_source_observer.py verify-receipt
```

`observe-once` 是可重复调度的单次执行单元。Phase 1D 不把它注册到 FastAPI startup、现有
Paper/模拟盘 scheduler 或 ingestion transaction。未来生产周期调度属于独立部署激活事实，
不在本设计提交中启用。

### 8.2 Configuration State

`AISTOCK_ADVISORY_PHASE1_SOURCE_OBSERVER_ENABLED` 默认 false。这个开关只是独立 worker 的
运行配置，不是审批、角色或人工门禁：

- backend/Selection/Paper/模拟盘从不读取该开关；
- 未启动 worker 时现有业务完全不变；
- 显式调用 `observe-once` 但开关为 false 时返回非零退出码和
  `ADVISORY_PHASE1_SOURCE_OBSERVER_DISABLED`，不得返回假成功；
- 开启后合法输入无需人工确认即可自动写 event/receipt/cursor。

## 9. Capacity Probe / 容量探针

### 9.1 Read-only Inputs

容量探针仅从配置的数据库连接环境读取：

- `pg_class`/`pg_stat_all_tables`/`pg_indexes_size`/`pg_total_relation_size`；
- `market.dataset_date_refresh_audit` 的日期覆盖、row_count、expected_rows、coverage 分布；
- registry allowlist source table 的 min/max date、bounded date sample 和实际 row width；
- 当前 Advisory ledger/capture/label/build/snapshot 表的 relation size 和 row count；
- `SEALED` 的 `app.advisory_dataset_snapshot` / `advisory_dataset_snapshot_file` 及其本地可读
  Parquet metadata，用于真实 logical row width、compressed bytes/row、changed-partition ratio
  和 snapshot/manifest provenance；
- versioned workload assumptions。

数据库连接必须由仓库现有 env/config 解析，禁止猜测 host、port、database、username 或
password。容量探针 transaction 为 `REPEATABLE READ READ ONLY`，不写数据库。

### 9.2 `CapacityPlanningRequest`

```text
schema_version = advisory_phase1_capacity_request_v1
observer_config_hash
query_registry_hash
as_of_ts
history_start_trade_date
history_end_trade_date
program_count_by_style
candidate_depth_by_program
universe_size_p50/p95/max
horizons[]
projection_count
stage_projection_factor
revision_multiplier_p50/p95/max
retained_snapshot_count
concurrent_build_count
staging_copy_count
parquet_target_file_bytes
memory_budget_bytes
worker_memory_overheads
store_available_bytes
orphan_reserve_bytes
concurrent_build_bytes
manifest_overhead_bytes_per_snapshot
parquet_measurement_snapshot_limit
parquet_measurement_file_limit
request_hash
```

workload assumptions 和 probe bounds 必须显式进入 request/hash，不得隐藏在代码默认值中。
request 禁止接收任何命名为 measured 的 Parquet/row-width/changed-ratio 字段；这些数值只能由
read-only probe 从 `SEALED` evidence 形成。未知值输出 `PARTIAL` 和 missing field，不以零、
经验值或其它 role 代理值假装已测量。

### 9.3 Deterministic Formulas

```text
trading_days = count(calendar days in requested range)
signal_rows = sum(program_days * candidate_depth)
stage_rows = signal_rows * configured_stage_projection_factor
candidate_label_rows = signal_rows * horizon_count * projection_count
universe_outcome_rows = program_days * universe_size * horizon_count * projection_count
source_event_rows = sum(observed_partitions_by_role) * revision_multiplier
logical_uncompressed_bytes = sum(role_rows * measured_role_row_width)
projected_parquet_bytes = sum(role_rows * measured_role_parquet_bytes_per_row_p95)
staging_peak_bytes = projected_parquet_bytes * staging_copy_count
retained_store_bytes = initial_full_snapshot_bytes
                     + changed_partition_bytes * (retained_snapshot_count - 1)
                     + manifest_overhead_bytes_per_snapshot * retained_snapshot_count
required_free_bytes = staging_peak_bytes + concurrent_build_bytes * concurrent_build_count + orphan_reserve_bytes
peak_worker_memory = fetch_batch_bytes + arrow_builder_bytes + hash_buffer_bytes + verifier_bytes
concurrent_peak_memory = peak_worker_memory * concurrent_build_count
```

- row width、compression 和 changed-partition ratio 只能来自真实 bounded sample/Batch D
  `SEALED` golden，并在 receipt 绑定 snapshot set hash、manifest hash、writer version 和 file
  count；某个 required role 没有非空 Parquet measurement 时必须 `PARTIAL`，不得跨 role 代理。
- 使用 p50/p95/max 三组结果，不只给平均值。
- capacity probe 不全表加载 DataFrame；SQL aggregate、server-side cursor 和 bounded sample
  都必须有 query timeout/row budget。
- capacity 结果只规划 Phase 1E/1I 的配置和存储，不阻断 Selection、Paper、模拟盘或当前荐股。

### 9.4 `CapacityPlanningReceipt`

```text
schema_version = advisory_phase1_capacity_receipt_v1
request_hash/config_hash/query_registry_hash
database_observed_at/database_version
source_coverage_summary
relation_size_summary
row_distribution_summary
role_projection_summary
parquet_measurement_summary
db_transaction_budget_summary
memory_budget_summary
staging_store_summary
durable_store_summary
status = MEASURED | PARTIAL | INSUFFICIENT
reason_codes/missing_measurements
receipt_hash
```

- receipt 使用 canonical JSON 和 SHA-256，默认输出到 repo 外配置目录；相同 request 和同一
  observation snapshot 的 canonical receipt hash 必须相同。
- `PARTIAL`/`INSUFFICIENT` 是测量结果，不是审批状态；它们不能被手工改成 `MEASURED`。
- Phase 1E 只冻结 `MEASURED` receipt 中确有证据的数值；缺口由后续程序修复/复测，不引入
  approval role。

## 10. Error And Logging Contract / 错误与日志

稳定 reason codes 至少包括：

```text
ADVISORY_PHASE1_SOURCE_OBSERVER_DISABLED
ADVISORY_PHASE1_SOURCE_OBSERVER_CONFIG_INVALID
ADVISORY_PHASE1_SOURCE_OBSERVER_REGISTRY_MISMATCH
ADVISORY_PHASE1_SOURCE_OBSERVER_AUDIT_SCHEMA_MISSING
ADVISORY_PHASE1_SOURCE_OBSERVER_AUDIT_ROW_CHANGED
ADVISORY_PHASE1_SOURCE_OBSERVER_CURSOR_CONFLICT
ADVISORY_PHASE1_SOURCE_OBSERVER_QUERY_UNREGISTERED
ADVISORY_PHASE1_SOURCE_OBSERVER_SCHEMA_MISMATCH
ADVISORY_PHASE1_SOURCE_OBSERVER_ROW_COUNT_MISMATCH
ADVISORY_PHASE1_SOURCE_OBSERVER_QUALITY_NOT_ELIGIBLE
ADVISORY_PHASE1_SOURCE_OBSERVER_RESOURCE_LIMIT
ADVISORY_PHASE1_SOURCE_OBSERVER_EVENT_CONFLICT
ADVISORY_PHASE1_SOURCE_OBSERVER_RECEIPT_CONFLICT
ADVISORY_PHASE1_SOURCE_OBSERVER_UNEXPECTED
ADVISORY_PHASE1_CAPACITY_REQUEST_INVALID
ADVISORY_PHASE1_CAPACITY_STATS_UNAVAILABLE
ADVISORY_PHASE1_CAPACITY_BUDGET_INSUFFICIENT
ADVISORY_PHASE1_CAPACITY_RECEIPT_CONFLICT
```

日志只保留有诊断价值的边界：

- worker start/end：config hash、scopes、processed/appended/unchanged/not-eligible/failed counts、耗时；
- input failure：dataset/source role/partition/audit hash、reason code、error type、transaction stage；
- cursor conflict/concurrency：scope、row version、等待/失败时长；
- capacity result：request/receipt hash、status、缺失测量和主要容量数字。

不得输出数据库密码、完整 env、证券逐行数据、完整 source payload 或无价值 per-row success。
所有异常必须保持原始 traceback 在后台日志，并向 CLI 输出稳定 reason/context；禁止裸
`Failed to fetch`、空 success、捕获后继续伪造 receipt 或降级到 sampling。

## 11. Implementation Scope / 未来代码允许范围

```text
docs/architecture/advisory_phase1d_source_availability_observer_capacity_f2_design_20260714.md
docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
backend/services/advisory_phase1/source_ledger_postgres.py          # transaction-bound append refactor only
backend/services/advisory_phase1/source_observer.py                  # new
backend/services/advisory_phase1/source_observer_postgres.py         # new
backend/services/advisory_phase1/source_capacity.py                  # new
backend/db/migrations/add_advisory_phase1_source_observer_20260714.sql # new
scripts/advisory_phase1_source_observer.py                            # new, standalone only
backend/tests/advisory_phase1/test_source_observer*.py                # new
backend/tests/advisory_phase1/test_source_capacity*.py                # new
backend/tests/advisory_phase1/test_source_ledger.py                   # transaction refactor regression
```

以下路径必须 zero-diff：

```text
backend/services/data_refresh_audit.py
backend/services/tushare_sync_engine.py
backend/ingestion/
backend/services/strategy_package/
backend/services/selection_center/
backend/services/simulation_runtime/
backend/services/paper_trading/
backend/routers/
backend/main.py
frontend/
```

若实现发现必须修改 frozen 路径、接入 runtime scheduler 或新增第三个 authority，必须停止
编码并前后一致修订父设计和本文，不能静默扩 scope。

## 12. Implementation Plan / 实施顺序

### D1：Typed contracts and registry

- 实现 config/spec/audit snapshot/receipt/capacity request/receipt canonical models。
- 冻结首批 dataset/query/partition/schema registry。
- 完成 config/hash/SQL allowlist pure tests。

### D2：Additive schema and repositories

- 实现 cursor/receipt migration、immutable trigger、indexes/comments。
- 提取 transaction-bound source ledger append primitive并保持公共行为一致。
- 完成 exact retry、conflict、concurrency、rollback tests。

### D3：Observer service

- 实现 discovery、eligibility、source stream/hash、event mapping、atomic commit/readback。
- 实现 scope isolation、resource bound、错误/日志契约。

### D4：Capacity probe

- 实现 read-only inventory、bounded sample、公式、canonical receipt 和 verifier。
- 通过 DEV 数据得到首份真实 capacity receipt；不得用静态 fixture receipt 冒充。

### D5：Standalone CLI and DEV E2E

- 实现 `observe-once`、`capacity-plan`、`verify-receipt`。
- 在 DEV PostgreSQL 运行合法数据正向链、correction、restart、concurrency、bad input 和
  capacity receipt E2E。

### D6：Design compliance review

- 逐项更新 Design Acceptance Matrix 为真实 refs/evidence。
- 检查无简化版、静默错误、业务偏移、额外审批/角色/人工门禁。
- 证明 frozen runtime paths zero-diff 后才可报告代码阶段完成。

## 13. Verification Plan / 验证方案

### 13.1 L0 Static

- import graph 证明 observer/capacity 不 import Selection/Paper/simulation/QMT/broker/order。
- source registry 不允许动态 SQL identifier/query text。
- frozen path diff 为零。
- DDL/source scan 不含 role/RBAC/approval/manual override/runtime DDL。

### 13.2 L1 Pure

- config canonical hash、same id/different hash conflict。
- audit row/partition/schema/content/revision/receipt deterministic hash。
- eligibility 和 event mapping 全矩阵。
- failed audit 不 invalidates prior event。
- capacity p50/p95/max 公式、unknown measurement 不产生 FIT/MEASURED。

### 13.3 L2 PostgreSQL Rollback-only

- migration apply/reapply/schema/trigger/index/comment/rollback-no-residue。
- database-controlled `first_observed_at` 和 caller backdate rejection。
- event + receipt + cursor 同事务 commit/rollback。
- exact retry、commit response lost、two-worker serialization、chain no-fork。
- bad scope 不阻断另一个 dataset/source/role scope。

### 13.4 L3 DEV Positive E2E

至少验证：

1. 合法 success audit + 完整 source partition 自动追加 `INGESTED`。
2. 相同 input 重跑只产生 `UNCHANGED` receipt，不重复 event。
3. source correction + 新 audit revision 自动追加 `CORRECTED`。
4. 进程在 event/receipt/cursor 各边界中断后可 exact retry。
5. 不合格 audit 产生 `NOT_ELIGIBLE` 且不影响其他 scope。
6. 修复 schema/row/content 错误后原 input 自动通过，不需要人工 override。
7. capacity-plan 从真实 DEV 数据产生可验证 canonical receipt。
8. observer 开关关闭时 backend、Selection、Paper、模拟盘行为完全不变。

### 13.5 Deferred Validation

生产数据量下的长周期运行、实际 scheduler 部署和 Phase 1E 多 Program source coverage 属于
后续发布/Phase 1E 验证。它们不能被 DEV fixture 宣称为已完成，也不应在本设计或代码 PR
中触碰模拟盘运行。

## 14. Automatic Invariants / 自动不变量与可满足性

本阶段没有审批层。只保留五组程序自动不变量：

| invariant | 合法数据如何通过 | 失败后如何恢复 | 不影响范围 |
|---|---|---|---|
| config/registry identity | 已注册 config/query/schema hash 精确匹配 | 修复版本化配置后重跑 | 所有 runtime consumer |
| audit/source parity | success/quality/schema/row/content 全部一致 | 修复 ingestion/source 后新 audit revision 自动通过 | prior source event 不被误删 |
| event chain | next revision + exact predecessor + DB time | 同 input exact retry 或合法 correction | 其他 partition chain |
| receipt/cursor atomicity | event/receipt/cursor 同事务完成 | rollback 后自动重跑 | 其他 scope cursor |
| resource bounds | capacity receipt 冻结的正确 batch/row/byte/time 范围 | 调整有证据的下一 config version | Selection/Paper/模拟盘 |

每个不变量必须同时有正向通过和反向拒绝测试。合法配置、完整数据和正常数据库资源必须
自动通过，不存在需要角色批准、人工修改数据库、manual bypass 或永远无法满足的条件。

## 15. Production Gates And Rollout / 生产状态、发布与回滚

当前代码交付：

```text
feature_tier = F2
implementation_status = verified
production_ddl_gate = pending
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
runtime_activation = noop
database_read_or_write = DEV apply/reapply/E2E/rollback-no-residue verified; production untouched
model_training = none
selection_paper_simulation_qmt_impact = none
```

未来代码提交：

- DEV migration 与真实 DEV E2E 必须完成后才可请求代码合入。
- 若包含 additive migration，合入时分别报告 source merge 和
  `production_ddl_gate=pending/applied_and_verified`；不要求每次 DDL 前全库备份。
- observer production activation 与代码/DDL 合入分离；没有用户明确要求时保持未激活。
- rollback 为停止独立 worker并 revert observer code；append-only evidence 不 DELETE，错误
  event 必须按既有 chain 语义追加 correction/invalidation，不能手工改表。

## 16. Risks And Failure Modes / 风险与失败模式

| 风险 | 错误结果 | 设计处置与恢复 |
|---|---|---|
| mutable audit row 覆盖旧状态 | 把当前 row 当成历史 availability | audit 只作 discovery；event 使用 DB observation time，旧 revision 未观察到就保持 gap |
| source query 与 audit row 发生竞态 | row count/hash 对不上或记录 split view | 单 input repeatable-read、事务内 audit re-read；不一致回滚，下一次新 audit revision重试 |
| provider time 缺失 | 猜测更早 formal availability | 保持 `provider_published_at=NULL`，以 DB first observed time 为准 |
| 大 partition 全量 hash 耗时 | 数据库压力或 worker OOM | capacity receipt 冻结 fetch/row/byte/time bounds；超限明确失败，不截断、不抽样成功 |
| 一个 source scope 数据持续错误 | cursor 无法前进 | per dataset/source/role 隔离；错误日志包含 exact key，修复数据/config 后同 input 自动重试 |
| observer 并发或 commit response 丢失 | event fork、重复 receipt 或 cursor 跳跃 | cursor row lock、existing chain lock、request/receipt hash exact readback |
| capacity 使用未知经验值 | 低估磁盘/内存并错误进入 Phase 1E | 未实测字段得到 `PARTIAL`，不能输出 `MEASURED`；补测后生成新 receipt |
| migration 与运行代码不同步 | 新代码启动后缺表 | 发布阶段显式 schema verification；runtime 不执行 DDL，未同步时 worker 明确失败且不影响其他模块 |
| standalone worker 被误接入共享 runtime | Selection/Paper/模拟盘受到异常或延迟影响 | frozen path zero-diff、import graph 和启动回归；activation 与代码/DDL 合入分离 |
| receipt 被误当训练或 source authority | 下游 lineage 语义漂移 | source revision resolver 只接受 existing availability event；receipt 类型不可作为 revision member |

以上风险均由程序状态、事务、资源边界和自动测试处理，不引入人工审批或角色。任何未知风险
若要求改变 authority、接入共享 runtime 或扩大 migration 范围，必须先修订完整设计，而不是
在实现中增加 fallback。

## 17. Design Acceptance Index

- F-401：Phase 1D 唯一 authority 仍是现有 append-only source availability ledger。
- F-402：mutable refresh audit 只用于 completion discovery，不提供历史 availability。
- F-403：observer 使用 versioned typed registry，禁止动态 SQL 和猜测 provider time。
- F-404：first observed time 由 PostgreSQL repository 控制，历史 row 不被回填为历史可用。
- F-405：source partition 全量 schema/row/content 验证，无 sampling/truncation success。
- F-406：event type 与 predecessor/revision 推导确定，failed audit 不误 invalidates prior event。
- F-407：event/receipt/cursor 单事务原子，commit-loss exact retry。
- F-408：per scope isolation 和并发串行化不产生 fork或跨 scope 阻塞。
- F-409：observer receipt append-only、content-addressed且不能替代 source authority。
- F-410：standalone observer 默认关闭，不接入 backend/ingestion/Selection/Paper/模拟盘。
- F-411：容量探针只读、bounded、使用真实测量和显式 workload assumptions。
- F-412：capacity receipt deterministic，未知测量不能伪装 MEASURED/FIT。
- F-413：错误具有稳定 reason/context/traceback，无静默 fallback 或假成功。
- F-414：未来实现范围、migration、CLI、测试和 frozen paths 已明确。
- F-415：所有自动不变量均有合法数据正向通过和失败修复后自动恢复测试。
- F-416：不读取回测数据，不训练模型；未来训练仍只在 WSL/Conda。
- F-417：无用户、角色、授权、审批、人工门禁、manual override 或额外备份要求。
- F-418：完整交付 authority、observer、capacity、错误和隔离契约，业务语义与跨模块 runtime 保持不变。

## 18. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-401 | `source_ledger_postgres.py`; `source_observer_postgres.py` | existing ledger regression + DEV event readback | verified | none |
| F-402 | `AuditRowSnapshot`; observer discovery | eligibility/correction/no-backfill unit and DEV matrix | verified | none |
| F-403 | `SOURCE_QUERY_TEMPLATES`; typed config/spec | registry mismatch, schema and isolation static tests | verified | none |
| F-404 | transaction-bound ledger append | DEV DB-clock event/receipt readback | verified | none |
| F-405 | `canonical_source_partition_descriptor`; named-cursor stream | full-row/resource unit + real DEV source hash | verified | none |
| F-406 | `decide_observation` | INGESTED/UNCHANGED/CORRECTED/REVALIDATED/failed tests | verified | none |
| F-407 | `PostgresSourceObserverRepository` | DEV event/receipt/cursor rollback, recovery and exact retry | verified | none |
| F-408 | per-scope cursor lock + bounded serialization retry | real two-worker DEV convergence and isolation static test | verified | none |
| F-409 | observer migration + receipt repository | apply/reapply, immutable/hash/conflict and rollback-no-residue | verified | none |
| F-410 | standalone CLI | disabled nonzero, import graph and frozen-path scans | verified | none |
| F-411 | `AdvisoryPhase1CapacityProbe` | real DEV read-only calendar/source/relation/SEALED evidence probe | verified | none |
| F-412 | capacity receipt/verifier | three-tier deterministic receipt and missing-role non-MEASURED evidence | verified | none |
| F-413 | stable observer errors + structured scope context | traceback/reason/context/transaction-stage DEV evidence | verified | none |
| F-414 | implementation paths in §11 | changed-path scope and `git diff --check` | verified | none |
| F-415 | automatic invariants in §14 | positive, negative, rollback, recovery and concurrency matrix | verified | none |
| F-416 | standalone observer/capacity modules | no backtest/training import scan | verified | none |
| F-417 | typed config + migration + CLI | no approval/RBAC/backup/manual-gate scan | verified | none |
| F-418 | full implementation and DESIGN-COMPLIANCE-001 | item-by-item review + F2 validator | verified | none |

## 19. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：设计包含真实 ingestion audit discovery、真实 source 全量 hash、
  PostgreSQL event/receipt/cursor、真实 DEV E2E 和 capacity receipt；static 或 mock-only artifacts
  不能替代这些交付物。
- [x] `no_silent_error`：所有配置、audit、schema、row、hash、resource、transaction、receipt
  错误均有稳定 reason；失败不移动 cursor或伪造 event。
- [x] `no_business_semantic_drift`：audit 不升级为 authority；observer 不修改 source、策略包、
  Selection、Paper、模拟盘或荐股 consumer。
- [x] `no_unrequested_gate_or_approval`：没有用户、角色、授权、审批、人工确认、manual override；
  五组自动数据不变量对合法数据均可通过。
- [x] `runtime_isolation`：仅 standalone 默认关闭 observer与只读 capacity CLI；backend/main、
  ingestion transaction 和交易相关 scheduler 不接线。
- [x] `training_boundary`：不读回测数据、不训练模型；未来训练只允许 WSL/Conda。
- [x] `production_truth`：设计合入、代码合入、DDL、worker activation、Phase 1E readiness 分开报告。

## 20. Exit Criteria / 代码合入条件

只有以下条件全部满足，Phase 1D 代码才可请求合入：

1. F-401 至 F-418 全部为 `verified` 且无未批准 gap、TODO 或 exception。
2. F2 feature workflow validation 通过。
3. 父蓝图、Phase 1 父设计、现有 source ledger、mutable refresh audit 和 Batch D 当前代码一致。
4. 允许修改范围和 frozen runtime paths 明确，无 ingestion/Selection/Paper/模拟盘接线。
5. 合法 DEV input 存在无需人工干预的正向路径：
   `audit success -> source full verify -> event -> receipt -> cursor`。
6. 每个错误在修复配置/数据/基础设施后可自动重跑通过，不需要数据库人工改值或审批 override。
7. DEV migration apply/reapply/E2E/rollback-no-residue 已验证；production DDL 与 worker activation 仍
   分开报告为 pending/noop，不由 runtime 自动执行。

Phase 1D 实现允许真实 DEV capacity receipt 因缺少某个 required role 的非空 `SEALED` sample 而
明确返回 `PARTIAL`；这证明 unknown-measurement 契约生效，不是代码缺项。Phase 1E 仍只能冻结
后续补齐数据后得到的 `MEASURED` receipt，当前不得注册 production observer config 或激活 worker。
