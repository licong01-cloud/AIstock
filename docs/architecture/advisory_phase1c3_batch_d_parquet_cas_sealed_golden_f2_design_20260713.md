# AIstock Advisory Phase 1C-3 Batch D Deterministic Parquet, CAS And SEALED Golden F2 Design

## 1. Background / 文档定位

本文是 `advisory_phase1c3_fixture_label_snapshot_f2_design_20260713.md` 中 Batch D 的唯一实施级详细设计，
承接已经合入并完成生产 schema 同步的 Batch A、Batch B 与 Batch C。Batch D 的目标不是再建立一套数据
链路，而是把 Batch C 已冻结的 build/attempt/snapshot persistence foundation 接到真实的、可重放的离线文件链：

```text
frozen FixtureDatasetBuildRequest
  -> one bounded PostgreSQL REPEATABLE READ READ ONLY materialization view
  -> deterministic PyArrow 21.0.0 Parquet files
  -> full-file/full-row verifier
  -> repo-external local content-addressed store
  -> deterministic manifest and promotion receipt
  -> PostgreSQL PROMOTED checkpoint
  -> one short PostgreSQL seal transaction
  -> first real Phase 1C-3 SEALED golden snapshot
```

Batch D 完成只表示一个真实 fixture/development snapshot 能从权威历史研究数据自动到达 `SEALED`，并且
同一冻结输入两次生成的 Parquet bytes、file SHA、manifest 和 snapshot id 完全一致。它不表示模型训练、
训练数据消费、生产 observer、实时荐股、模拟盘、实盘或生产级对象存储已经启用。

本批为 `F2`。禁止以静态 JSON、手工改状态、mock-only CAS、只校验样本行、直接构造 `SealedDatasetSnapshot`
或跳过 promotion receipt 的方式冒充完成。

## 2. Parent Baseline / 父级基线与当前事实

### 2.1 已完成基础

- Batch A：label policy、统一 `OutcomeEngine`、calculation evidence canonical bytes/hash 与真实本地
  create-if-absent evidence store。
- Batch B：label capture request、candidate/universe enumeration、append/revision/selector 与 label builder。
- Batch C：完整 PostgreSQL label authority、build/attempt/file/event/gap/blob/snapshot/invalidation schema，
  以及 REQUESTED/MATERIALIZED/Batch-C-VERIFIED foundation。
- `DatasetBuild` 已冻结 `REQUESTED -> MATERIALIZED -> VERIFIED -> PROMOTED -> SEALED` checkpoint 和
  lease/fencing/row-version 语义。
- `PostgresDatasetBuildRepository.save_sealed_snapshot()` 已要求
  `PHASE1C3_BATCH_D_FULL_PARQUET_V1`、PROMOTED build、matching manifest/promotion receipt 和 active SEAL
  attempt；不存在未验证文件直接 seal 的合法入口。
- `requirements.txt` 已固定 `pyarrow==21.0.0`，Batch D 不需要新增 Python 依赖。

### 2.2 当前缺口

- 尚无 `snapshot_writer.py`、全量 Parquet verifier 或 dataset local CAS。
- `complete_verify()` 明确只签发 `PHASE1C3_BATCH_C_FILESET_FOUNDATION_V1`，不能签发 Batch D full receipt。
- 尚无 repository `complete_promote()`，所以当前不存在真实 `VERIFIED -> PROMOTED` 正向链。
- 尚无 manifest/promotion receipt 的 canonical model、durable local publication 或完整 crash-retry oracle。
- 尚无真实 SEALED golden；Phase 1C-3 不能标记完成。

### 2.3 冻结的运行边界

Batch D 是离线 fixture builder，不接入 FastAPI startup、API、UI、scheduler、Selection Center、Paper v2、
模拟盘、QMT、策略包推理或交易执行。以下目录保持 zero-diff：

```text
backend/services/advisory_phase1/stage_trace.py
backend/services/simulation_runtime/
backend/services/strategy_package/
backend/services/selection_center/
backend/services/paper_trading/
backend/routers/
frontend/
```

## 3. Scope / 本批范围

### 3.1 必须完整交付

1. 强类型 materialization plan、logical row stream、schema registry、file descriptor 和 receipt models。
2. 单一 `REPEATABLE READ READ ONLY` 数据库视图下的 bounded/batched materialization。
3. PyArrow 21.0.0 确定性 writer，以及对全部文件和全部行执行的 verifier。
4. repo 外、同卷 staging/blob publication 的真实 local CAS，包含容量、原子性和持久化语义。
5. canonical manifest、promotion receipt、full verify receipt 和 seal aggregate 的完整 hash closure。
6. `MATERIALIZED -> VERIFIED -> PROMOTED -> SEALED` 的 repository 正向链和 exact retry。
7. 每个持久化边界的 crash/retry/fencing 验证。
8. 同一冻结输入执行两次的 byte-for-byte golden，以及真实 DEV PostgreSQL `SEALED` readback。

### 3.2 允许修改范围

```text
docs/architecture/advisory_phase1c3_fixture_label_snapshot_f2_design_20260713.md  # 仅状态/索引一致性
docs/architecture/advisory_phase1c3_batch_d_parquet_cas_sealed_golden_f2_design_20260713.md
backend/services/advisory_phase1/dataset_build.py
backend/services/advisory_phase1/dataset_build_postgres.py
backend/services/advisory_phase1/calculation_evidence.py                  # 仅提取共享 CAS primitive，行为零漂移
backend/services/advisory_phase1/snapshot_writer.py                       # new
backend/services/advisory_phase1/dataset_store.py                         # new
scripts/advisory_phase1c3_fixture_snapshot.py                              # new, offline only
backend/tests/advisory_phase1/test_phase1c3_batch_d_*.py                   # new
backend/tests/advisory_phase1/test_dataset_build.py                        # Batch C regression
backend/tests/advisory_phase1/test_calculation_evidence.py                 # shared CAS regression
```

本批预期不新增或修改 migration。若实现中发现 Batch C schema 无法表达本文已冻结的正向链，必须停止代码
开发，先前后一致地修订父级与本文并重新执行 F2 design validation；不得在运行时执行 DDL，也不得以 JSONB
旁路、手工 SQL 或内存状态绕过 schema 缺口。

## 4. Non-goals / 非目标

- 不训练模型；未来模型训练仍只允许 WSL/Conda，Batch D 的 Windows 进程仅做离线 fixture 文件构建。
- 不读取实时行情，不运行实时荐股，不产生买入、卖出、止盈、止损或下单建议。
- 不启用 production observer、历史回填 scheduler、训练 scheduler 或 runtime consumer。
- 不创建生产级 remote object store、reservation/hold/GC/consumer epoch；这些属于 Phase 1I。
- 不增加用户、角色、RBAC、授权、审批、人工 acknowledge、人工 override、confirm-run 或双人复核。
- 不改变策略包、Selection、Paper v2、模拟盘、QMT 或共享推理链的业务逻辑。
- 不把 Batch C foundation verification 升格为 full Parquet verification。
- 不要求每次 DDL 前全库备份；本批没有 DDL。

`actor`、`lease_owner_id` 和 `fencing_token` 是现有并发诊断字段，不代表用户角色或审批权。离线 CLI 自动生成
稳定的进程实例标识并按现有状态机写入，不需要人工批准。

## 5. Architecture / 单一权威数据流架构

```text
FixtureDatasetBuildRequest + Batch C persisted build
  -> validate frozen capture/source/selector identities from PostgreSQL
  -> begin REPEATABLE READ READ ONLY transaction
  -> resolve COMPLETE capture memberships and exact selected mappings
  -> stream immutable observation/stage/label/source-revision rows
  -> read calculation evidence by DB-persisted CAS descriptor and verify bytes/hash/size
  -> write attempt-scoped Parquet staging files
  -> close source transaction
  -> persist DatasetAttemptFile descriptors and MATERIALIZED checkpoint on the control connection
  -> reopen every materialized file and execute full verifier
  -> persist typed full-verification receipt and VERIFIED checkpoint
  -> publish every verified byte to local CAS
  -> publish canonical manifest and promotion receipt
  -> reopen CAS objects and persist PROMOTED checkpoint
  -> start SEAL attempt and construct SealedDatasetSnapshot only from receipts
  -> save_sealed_snapshot() in one PostgreSQL transaction
```

不存在“DB 失败则读取缓存”“evidence 文件缺失则只输出 label header”“校验失败仍 promotion”“seal 失败仍返回
snapshot id”或“先用 Batch C receipt 继续”的替代路径。

## 6. Offline Entry Point / 离线入口

唯一入口为：

```text
python scripts/advisory_phase1c3_fixture_snapshot.py build --request <canonical-request.json>
python scripts/advisory_phase1c3_fixture_snapshot.py verify --build-id <build-id>
python scripts/advisory_phase1c3_fixture_snapshot.py resume --build-id <build-id>
```

- 数据库连接只通过项目现有 `.env`/settings/`pg_pool` 配置加载，不猜测 host、port、database、user 或
  password，不接受源码硬编码连接信息。
- dataset store root 只从 `AISTOCK_ADVISORY_DATASET_STORE_ROOT` 读取；必须是绝对路径且在 repo 外。
- CLI 只在命令执行时 lazy-import `pyarrow`、writer 和 store；FastAPI import/startup 不得引入 PyArrow 或
  初始化 store。
- `build` 驱动合法状态的完整正向链；`verify` 只验证已有 frozen files，不改变 checkpoint；`resume` 根据 DB
  checkpoint 和 receipt 继续唯一合法的下一步，不回退已完成 checkpoint。
- 同一 request 已存在 SEALED build 时，`build` 完整 readback 现有 snapshot/CAS receipts 后幂等返回，不创建
  第二个 generation，也不重写文件。
- 所有命令成功时输出一个 compact structured receipt；失败时返回非零 exit code、稳定 reason code 和诊断
  context，禁止只打印 traceback 后返回 0。

## 7. Contracts / 强类型契约

`snapshot_writer.py` 新增以下 frozen Pydantic models，均为 `extra="forbid"`：

| Model | 关键字段 | identity |
|---|---|---|
| `DatasetMaterializationPlan` | build/request hashes、attempt/fencing、source tx identity、logical partitions、writer config | canonical JSON SHA256 |
| `LogicalDatasetRow` | logical role、partition key、sort key、typed values | role schema + canonical row hash |
| `MaterializationReceipt` | build/attempt、file set hash、file descriptors、source revision/capture hashes | canonical JSON SHA256 |
| `FullParquetVerificationReceipt` | contract version、file set hash、per-file verify result、relational closure summary；不含 attempt/time | canonical JSON SHA256 |
| `DatasetManifestCore` | complete file list、selected observation/label mappings、source/base/capability/schema identities | `manifest_core_sha256` |
| `DatasetManifest` | core、manifest schema version、store backend identity | canonical bytes SHA256 |
| `PromotionReceipt` | build id、verified receipt hash、manifest hashes、complete blob set、store backend hash | canonical bytes SHA256 |
| `DatasetCapabilityManifest` | exact component/capability/status/reasons rows | canonical JSON SHA256 |
| `SealAssemblyReceipt` | snapshot aggregate inputs and derived snapshot id | canonical JSON SHA256 |

不得使用未校验 `dict[str, Any]` 直接驱动 checkpoint。JSON 仅用于已经被对应 Pydantic/Arrow schema 验证的
canonical payload；repository 只接受上述 typed receipt。

## 8. Logical Files And Arrow Schemas / 逻辑文件与 Arrow schema

### 8.1 完整文件集

```text
canonical_signals/year=YYYY/month=MM/part-00000.parquet
observation_versions/year=YYYY/month=MM/part-00000.parquet
selected_observations/part-00000.parquet
lineage/year=YYYY/month=MM/part-00000.parquet
stage_summaries/year=YYYY/month=MM/part-00000.parquet
stage_candidates/year=YYYY/month=MM/part-00000.parquet
outcome_labels/horizon=H/year=YYYY/month=MM/part-00000.parquet
selected_labels/horizon=H/part-00000.parquet
outcome_source_evidence/owner_type=CANDIDATE|UNIVERSE/horizon=H/year=YYYY/month=MM/part-00000.parquet
universe_outcomes/horizon=H/year=YYYY/month=MM/part-00000.parquet
gaps/year=YYYY/month=MM/part-00000.parquet
source_revisions/source_revision_set.parquet
schemas/<logical-role>.schema.json
```

所有请求中声明的 role 均必须有 schema descriptor。允许合法 0-row role 生成一个固定 schema 的 0-row
Parquet 文件；不得因没有 gap 或没有 universe label 而静默省略逻辑 role。manifest 必须列出完整文件集。

### 8.2 Schema authority

Arrow schema 由 `snapshot_writer.py` 中不可变 `SNAPSHOT_ARROW_SCHEMAS_V1` 显式定义，禁止从首批数据推断。
字段映射规则如下：

- `canonical_signals` 完整映射 `app.advisory_signal_observation` 的业务列，排除 `created_at`。
- `observation_versions` 完整映射 `app.advisory_signal_observation_version` 的业务列，`reason_codes` 为
  `list<utf8>`，排除 `created_at`。
- `selected_observations` 完整映射 label-capture `request_payload_jsonb` 中已经持久化的
  `SelectedObservationMappingReference` 六个字段，并与 build request 的 mapping id/hash set 对齐；它不伪造未持久化
  的 selector request 字段，也不执行 latest lookup。
- `lineage` 完整映射 `app.advisory_signal_observation_lineage` 的业务列，排除 `created_at`。
- `stage_summaries` 完整映射 `app.advisory_signal_stage_evidence` 的业务列；reason codes 为排序去重 list。
- `stage_candidates` 完整映射 `app.advisory_signal_stage_candidate` 的业务列；NUMERIC 为
  `decimal128(38,12)`；component JSON 为 canonical UTF-8 JSON，并由 hash 验证，不丢弃多 Alpha component
  evidence。
- `outcome_labels` 为 `app.advisory_outcome_label` 与对应月分区 payload 的完整业务列一对一 join，排除
  DB operational timestamp；`computed_at` 是 label provenance，保留为 UTC microsecond timestamp。
- `selected_labels` 使用 build request 的 label as-of/policy/selected mapping id+hash、对应 frozen label revision chain
  和 Batch B terminal-first no-fallback selector 确定性重建完整 `SelectedLabelMapping`；重建 id/hash 必须与 request
  identity 相等，不从 current/latest 补选。
- `outcome_source_evidence` 从每个 label 的 DB-persisted evidence descriptor 读取 canonical evidence bytes，
  验证 store/hash/size/owner/policy 后完整映射 typed `CalculationEvidenceBundle`；禁止只写 descriptor。
- `universe_outcomes` 是 `owner_type=UNIVERSE` 的完整 label+payload projection，不从 candidate 行推断。
- `gaps` 合并 frozen capture set/date/scope 可达的 observation capture、label capture 与既有 typed dataset gap
  records，包含 source kind 与 canonical gap hash，禁止用空字符串代替缺失值。当前 attempt 的 operational
  failure/timeout 不写入数据文件，避免 build 自身失败形成循环输入；它们只进入 build attempt/event authority。
- `source_revisions` 完整映射冻结 `snapshot_source_revision_set_id` 的 set header 和全部 member，包含
  enforced cutoff predicate hash；`partition_key` 为 canonical UTF-8 JSON。

实现必须维护显式 authority-column allowlist。测试从 `information_schema.columns` 读取相关表的业务列并与
allowlist 对比；新增业务列而 writer 未映射时直接失败，防止 schema 漂移造成静默字段丢失。

### 8.3 Physical types

- text/id/hash/status/policy/version：`utf8`，non-null 规则与 authority model 一致。
- date：`date32`。
- aware datetime：`timestamp[us, tz=UTC]`；输入先标准化 UTC，禁止 local/naive timestamp。
- money/score/return/quantity：`decimal128(38,12)`，禁止 float round-trip。
- count/rank/horizon：按 schema 固定 `int32` 或 `int64`，不按数据范围推断。
- bool：Arrow bool。
- reason code collections：排序、去重后的 `list<utf8>`，null 与 empty list 语义分离。
- typed nested payload：canonical JSON UTF-8，key 排序、无空白、UTF-8、禁止 NaN/Infinity，并保留对应 hash。

每个 `schemas/*.schema.json` 由同一个 Arrow schema canonicalizer 生成，包含字段名、顺序、类型、nullable、
role/schema version，不包含生成时间、主机名或绝对路径。它以 `logical_role=SCHEMA_DESCRIPTOR`、
`compression=none`、`row_count=0` 进入 attempt/file manifest；descriptor 自身的 logical path、SHA、size 和
canonical content hash 仍参与 file-set identity。

## 9. Partition And Sort Contract / 分区与排序

- 数据 role 按 `decision_as_of_trade_date` 的 year/month 分区；horizon role 先按 horizon，再按 year/month。
- global mapping/source descriptor role 不按日期拆分。
- 每个 logical partition 当前固定一个 `part-00000.parquet`；未来改变 file sizing 必须升级
  partition policy/writer version，不能在同一 identity 下自适应切片。
- logical path 使用 `/`，不使用平台 path separator。
- role 内排序键冻结为业务主键的全序：日期、canonical signal、observation、stage ordinal、symbol、horizon、
  projection、revision；相同业务键不允许存在两行。
- A 股 symbol 只接受已经通过冻结 normalization policy 的 canonical symbol；writer 不做第二次纠正。
- `partition_key_hash`、`partition_content_hash` 和 file-set hash 均基于 canonical logical rows，不基于查询返回顺序。

## 10. Deterministic PyArrow Writer / 确定性 writer

`writer_version=ADVISORY_PHASE1C3_PYARROW21_PARQUET_V1`，固定：

```text
pyarrow_version = 21.0.0
format_version = 2.6
compression = zstd
compression_level = 3
use_dictionary = false
write_statistics = true
data_page_version = 2.0
data_page_size = 1048576
write_batch_size = 1024
write_page_index = false
write_page_checksum = true
use_compliant_nested_type = true
store_decimal_as_integer = false
use_content_defined_chunking = false
use_deprecated_int96_timestamps = false
coerce_timestamps = us
allow_truncated_timestamps = false
row_group_size = 65536
store_schema = true
```

- V1 只接受冻结 `compression_config={"codec":"zstd","level":3}`；其 canonical hash 已在 build request 中。
  其它配置必须升级 writer/config identity，不能在 V1 下隐式接受。
- schema/column order、row order、row group boundary、compression 和 writer options 全部受冻结 identity 约束；
  request 冻结 compression identity，row-group/page/batch 参数由上述 writer version 常量冻结。
- 写入前清除输入 Table/field 的任意 metadata，只写固定 `aistock_snapshot_schema_version`、logical role、
  schema fingerprint、writer version；禁止写 created time、host、PID、attempt id 或临时路径。
- staging 文件名允许含 attempt id 以隔离并发，但文件内部 bytes 和最终 logical path 不得依赖 attempt id。
- 每个文件 close/flush 后重新打开计算 size/SHA，并从 Parquet footer 读取 schema、row groups、row count；不得信任
  writer 内存计数。
- 相同冻结输入和 writer version 必须 byte-for-byte 相同；只做到 logical-row hash 相同不算通过。

## 11. Source Transaction / 数据库一致性读取

### 11.1 Transaction boundary

MATERIALIZE 使用一个 bounded coordinator transaction：

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
```

transaction 内先锁定并验证 build request 引用的 capture/source/selector/hash closure，然后以 server-side cursor
和 `fetchmany(10000)` 流式读取。禁止按 symbol/date 执行 N x M 查询，禁止在不同连接拼接同一 snapshot。

### 11.2 Frozen selection

- capture 必须是 request 中列出的 COMPLETE observation/label capture，purpose、receipt、membership、handoff、
  admission 与 source revision hash 全部相符。
- label-capture `request_payload_jsonb` 必须先经现有 tagged request parser 重建 `LabelCaptureBatchRequestV2`，并重新
  验证 request/binding/mapping-set hashes；禁止直接信任 raw JSONB。
- selected observation 必须使用 typed request 中的 mapping reference；selected label 必须由冻结 as-of/policy 和
  revision chain 重建；两者都必须匹配 build request 已冻结的 mapping id/hash，不查询“当前 latest”。
- source revision set 必须等于 request identity，所有 member 的 cutoff predicate 仍闭合。
- label 必须是请求 key 的唯一 terminal legal revision；PENDING/UNAVAILABLE 是否允许由冻结 label policy 决定，
  writer 不改变 maturity 语义。
- transaction 结束前重新读取 authority header/count/hash，确保 materialization plan 与最终 stream counts 一致。

### 11.3 External calculation evidence

calculation evidence bytes 不是可替代来源。writer 只按 label authority 中持久化的 exact
`store_backend_hash + sha256 + size + uri` 读取现有 local evidence store，并执行完整 bytes/hash/typed payload/
owner/policy closure。任一 blob 缺失或冲突，整个 MATERIALIZE attempt 显式失败；不输出残缺 evidence role。

### 11.4 Resource bounds

- `ADVISORY_PHASE1C3_BUILDER_V1` 冻结：cursor fetch 10,000 rows、Arrow record batch 最多 65,536 rows 或
  128 MiB（先达到者为边界）、process RSS 上限 2 GiB、attempt lease 900 秒、heartbeat 周期 60 秒。
- DB rows 以 cursor batch 读取；构建每个 Arrow batch 前后使用已固定依赖 `psutil==7.2.1` 检查 RSS。超过
  2 GiB 立即失败当前 attempt，不降低 batch 后继续，也不切换到无界内存模式。
- writer 逐 partition/row group 落盘，不把全量数据加载到内存。
- 达到 transaction timeout、memory bound、disk bound 或 row bound 时返回稳定错误并失败 attempt。
- 失败不会留下 MATERIALIZED checkpoint；attempt staging 可由恢复流程清理，但不能被下游读取。

## 12. Materialization Completion / MATERIALIZED

每个 close 后的真实文件生成 `DatasetAttemptFile` 并经 repository exact append。全部 role 完成后：

1. 重新枚举计划文件和 DB attempt files，集合必须完全相等。
2. 运行现有 `verify_attempt_file_set()` 的 bytes/size/SHA 基础校验。
3. 生成 `MaterializationReceipt`，包含 source tx identity、capture/source hashes、完整 descriptor 和 file-set hash。
4. `complete_materialize()` 在现有事务中完成 attempt 并推进 checkpoint。

base snapshot 复用文件可以让 `staging_uri` 指向已验证 base CAS blob；这仍是 immutable materialized file，
不是绕过。只有 schema fingerprint、writer/compression/partition policy、logical path、partition content hash 和 bytes
SHA 全部相同才允许复用，否则必须重新 materialize。

## 13. Full Parquet Verifier / 全量校验器

### 13.1 Input boundary

verifier 只读取：frozen request、MATERIALIZED build、attempt file descriptors、schema descriptors 和这些 descriptor
指向的 immutable bytes。它不重新查询行情/标签数据，不修复文件，不做 fallback。

### 13.2 Per-file full checks

- 路径、role、partition、ordinal、file count 集合完全一致。
- 所有文件先做全字节 size/SHA。对 `.parquet` 再验证 Parquet footer、Arrow schema/fingerprint、writer metadata、
  compression、row groups，并对全部 row groups/rows 验证 typed values、nullability、sort order、unique key、
  row count、min/max key、partition predicate 和 partition content hash；禁止 sampling。
- 对 `schemas/*.schema.json` 验证 UTF-8 canonical JSON bytes、descriptor schema、role/fingerprint closure，并与
  对应 Parquet 的实际 Arrow schema 双向相等；禁止把 JSON descriptor 当 Parquet 或跳过它。
- canonical JSON payload 必须可解析、重编码后 bytes/hash 相同。
- 不接受 schema widening、column reorder、unknown column 或 silent cast。

### 13.3 Cross-file relational checks

- 每个 canonical signal 恰有 request 选择的一个 terminal observation mapping。
- selected observation 指向存在且 COMPLETE/eligible 的 observation version，并与 lineage/stage closure 相符。
- 每个 requested owner/symbol/horizon/projection 恰有一个 terminal selected label mapping。
- candidate label 的 observation/stage/symbol 必须交叉一致；universe label 不得伪造 candidate stage。
- 每个 label 的 calculation evidence 必须有 exactly-one evidence row，owner/policy/source/path hashes 一致。
- source revision member set、capture membership、query registry、calendar、universe/benchmark/cost/symbol policy 全闭合。
- capability rows按 component/capability/status/reasons 全维度对齐，禁止聚合为一个 bool。
- schema/file/row/maturity/event/count summary 必须由实际全量 rows 重新计算，不接受 caller 提供的 summary。

验证成功才生成 `FullParquetVerificationReceipt`，contract 固定为
`PHASE1C3_BATCH_D_FULL_PARQUET_V1`。repository 新增
`complete_full_verify(receipt, attempt_id, expected_fencing_token, ...)`：attempt/fencing 是并发调用上下文，不进入
receipt content identity；repository 校验 receipt file-set 与实际 DB state 后原子推进 VERIFIED。现有
`complete_verify()` 保持 Batch C 语义，不扩展为 string discriminator 通用入口。

DB 持久化 full receipt hash、contract 和 verified file-set；canonical receipt bytes 可由 immutable files 全量重验
确定性重建。若进程在 VERIFIED 后退出，PROMOTE/resume 必须先重建 receipt 并验证其 hash 等于 DB
`verify_receipt_hash`，不得只信任 DB 中的 contract 字符串。

## 14. Local Dataset CAS / 本地内容寻址存储

### 14.1 Store identity and layout

```text
<root>/
  staging/<build_id>/<attempt_id>/...
  blobs/sha256/<first-2>/<sha256>
  manifests/sha256/<first-2>/<manifest_sha256>.json
  promotion_receipts/sha256/<first-2>/<receipt_sha256>.json
```

store identity 包含 schema version、resolved root、filesystem/volume identity、durability mode、atomic publish mode 和
writer compatibility。其 canonical hash 为 `store_backend_hash`。

### 14.2 Shared primitive

`calculation_evidence.py` 现有 hardlink-create-if-absent、exact compare、file/directory flush 逻辑应提取为内部共享
local CAS primitive；`LocalCalculationEvidenceStore` 的 public API、URI、hash 和现有测试必须 zero-diff。
`LocalFilesystemDatasetStore` 复用该 primitive，禁止复制一套行为略有不同的原子写入代码。

### 14.3 Automatic storage invariants

这些是父级已批准的自动数据正确性条件，不是审批或人工门禁：

- root 必须是 repo 外的绝对路径；staging 和 blob 目录必须在同一 filesystem/volume。
- store identity 必须显式匹配当前平台 durability/atomic mode。
- 写前以 DB authority `sum(pg_column_size(row))`、calculation evidence descriptor sizes 和 schema-file 固定开销
  计算 `logical_source_bytes`；`projected_bytes=max(512 MiB, 2 * logical_source_bytes)`，
  `reserved_bytes=1 GiB`，`min_free_after_write=max(2 GiB, volume_capacity/10)`。必须满足
  `available_bytes >= projected_bytes + reserved_bytes + min_free_after_write`；无法取得容量信息时明确失败。
- 每个 staging file 先 flush，hardlink create-if-absent；目标存在时完整比较 size/hash/bytes，绝不覆盖。
- publish 后 flush parent directory，再从最终 CAS URI 重新打开校验。

合法配置且空间充足时全链自动通过，不需要任何人确认或解除。失败修复配置/空间/数据后重新执行即可，不存在
manual override。

## 15. Manifest And Promotion / manifest 与 promotion

### 15.1 Canonical manifest

`DatasetManifestCore` 包含：

- sorted complete logical file descriptors and blob identities；
- sorted selected observation and selected label mappings；
- snapshot source revision set、capture set、handoff、admission、query registry identities；
- base snapshot identity（如有）；
- exact capability manifest；
- schema fingerprint、writer/builder/code/partition/policy identities；
- file/row/bytes/maturity/event summary。

`manifest_core_sha256` 只由上述业务内容产生；`snapshot_content_hash=manifest_core_sha256`，
`snapshot_id=advsnap_<first24>`。`DatasetManifest` canonical bytes 再产生 `manifest_sha256`。二者都不包含
attempt id、fencing token、生成时间、PID、host 或 staging path。

### 15.2 Promotion order

PROMOTE attempt 的顺序固定：

1. 校验 build 为 Batch-D VERIFIED 且 full receipt/file set 匹配。
2. 将每个 verified file publish 到 `blobs/`，并从 final URI 全量 reopen 校验。
3. canonicalize/publish manifest，再 reopen 校验。
4. canonicalize/publish promotion receipt，再 reopen 校验。
5. repository `complete_promote()` 在短事务内校验 attempt/fencing/row version、full receipt、manifest hash 和
   promotion receipt，完成 attempt 并推进 PROMOTED。

promotion receipt 不含 attempt/fencing/time，所以同一 build 的 crash retry 产生相同 bytes/hash。attempt/fencing
仅写入 DB event，避免恢复后 receipt identity 漂移。

manifest/receipt 已存在但 DB 尚未 PROMOTED 时属于不可消费 orphan candidate；`resume` 完整验证后复用 exact bytes。
Batch D 不提供 consumer API，因此只有 DB `SEALED` snapshot 才是可消费 authority。

## 16. Seal Transaction / SEALED

SEAL attempt 只接收 `SealAssemblyReceipt`，由 repository/service 根据 PROMOTED build、full verification receipt、
manifest、promotion receipt 和 CAS reopen results 组装；CLI 不能传入任意 snapshot fields。

调用现有 `save_sealed_snapshot()` 时，在一个短 PostgreSQL transaction 中：

1. 锁定 snapshot identity 和 build；验证 active PROMOTED build/SEAL attempt/fencing。
2. upsert blob headers，并验证 store/hash/size exact identity。
3. 插入 snapshot header、全部 file/observation/label/blob-ref membership。
4. 完成 SEAL attempt，原子推进 build lifecycle/checkpoint 为 SEALED，并写 event。
5. transaction commit 后重新 readback 完整 aggregate，与 assembly receipt 逐字段相等。

`seal_receipt_hash` 从 build/snapshot/manifest/promotion/full-verify identities 派生，不含 attempt/time。transaction
失败时无 snapshot 半成品；客户端超时后按 snapshot/build identity readback，完全相同则返回成功，不同则冲突，
不存在 last-writer-wins。

## 17. Base Snapshot Reuse / 基础快照复用

- base 必须存在、SEALED、未 invalidated，且 request 中 id/content/manifest/source/capture/policy compatibility 全相等。
- child 始终生成完整 manifest 和完整 selected mapping，不生成 delta-only snapshot。
- 每个 partition 先从当前冻结 source rows 重新计算 partition content hash；只有与 base descriptor 完全一致时才引用
  base blob。
- source revision、schema、writer、compression、partition policy 或任一 row 变化时，该 partition 必须重写。
- reused blob 仍执行 full reopen/full verifier/CAS identity checks，并写 child blob ref。
- invalidated base、缺 blob、hash conflict 或 capability mismatch 明确失败，禁止退回全量 current/latest 读取。

## 18. Capability Contract / 能力声明

首个 SEALED golden 必须显式包含逐维度 capability rows，并固定：

```text
MODEL_TRAINING_READY = false
RUNTIME_ADVISORY_READY = false
TRADING_EXECUTION_READY = false
```

这三个值是能力事实，不是审批。Batch D 只证明 fixture dataset 的数据与持久化闭环；未来训练 readiness 必须由
后续阶段依据训练 schema/coverage 单独产生新 capability manifest，不能手工改为 true。

## 19. Recovery Matrix / 崩溃与重试

| crash/failure point | persisted authority | resume behavior | forbidden behavior |
|---|---|---|---|
| source tx before first file | REQUESTED + active/expired MATERIALIZE attempt | fail/expire/recover 后重新读取一个完整 tx | 使用已读内存残片 |
| partial staging file | REQUESTED | 删除该 attempt staging，重新写完整文件 | append partial bytes |
| files complete before `complete_materialize` | REQUESTED + immutable files | exact reopen，重建 descriptors 后完成或新 attempt 重写相同 bytes | 手工推进 checkpoint |
| after MATERIALIZED commit | MATERIALIZED | 只进入 VERIFY | 重新 latest materialize |
| verifier mid-file | MATERIALIZED | 新 VERIFY attempt 从第一个文件全量重验 | 跳过已扫文件 |
| after full verify before DB commit | MATERIALIZED | 重验并生成同内容 receipt | 接受内存 success |
| after VERIFIED commit | VERIFIED | 只进入 PROMOTE | 使用 Batch C contract |
| blob publish midway | VERIFIED + CAS subset | exact compare 已有 blob，继续缺失 blob | 覆盖冲突 blob |
| manifest published before receipt | VERIFIED + orphan manifest | 验证并继续 canonical receipt | 暴露 snapshot |
| receipt published before PROMOTED commit | VERIFIED + complete CAS candidate | exact reopen 后 `complete_promote` | 将文件存在视为 promoted |
| after PROMOTED commit | PROMOTED | 只进入 SEAL | 回写 manifest |
| seal transaction rollback | PROMOTED, no snapshot rows | 新 SEAL attempt 使用相同 aggregate | 保留半 snapshot |
| seal commit/client timeout | SEALED or PROMOTED | readback 判定 exact success 或安全重试 | 盲目重复插入/返回失败 |
| stale fencing token | newer attempt authority | 返回 stable stale reason | 旧 worker 继续写 checkpoint |

恢复不需要审批。只有现有 lease 仍 active 时另一个进程会收到 already-running；lease 到期后使用既有
expire/recover 语义自动恢复。

## 20. Error And Logging Contract / 错误与日志

在保留 Batch C reason codes 的基础上新增：

```text
ADVISORY_PHASE1C3_SOURCE_SNAPSHOT_CONFLICT
ADVISORY_PHASE1C3_ARROW_SCHEMA_CONFLICT
ADVISORY_PHASE1C3_PARQUET_WRITE_FAILED
ADVISORY_PHASE1C3_PARQUET_BYTES_CONFLICT
ADVISORY_PHASE1C3_PARQUET_FULL_VERIFY_FAILED
ADVISORY_PHASE1C3_RELATIONAL_CLOSURE_FAILED
ADVISORY_PHASE1C3_EVIDENCE_BLOB_INVALID
ADVISORY_PHASE1C3_DATASET_STORE_INVALID
ADVISORY_PHASE1C3_DATASET_STORE_CAPACITY_INSUFFICIENT
ADVISORY_PHASE1C3_CAS_CONTENT_CONFLICT
ADVISORY_PHASE1C3_MANIFEST_CONFLICT
ADVISORY_PHASE1C3_PROMOTION_RECEIPT_CONFLICT
ADVISORY_PHASE1C3_SEAL_READBACK_CONFLICT
```

- 所有 failure 显式传播到 CLI nonzero result，并通过 `fail_attempt()` 记录稳定 reason code。
- 不使用 `except: pass`、catch-all 后继续、空文件/空 summary 兜底、unknown column 忽略或默认业务值。
- 日志只在 operation start/success/failure、checkpoint、file summary、CAS conflict 和 recovery 边界输出；包含
  build/attempt/operation/logical path/hash/row count/elapsed/reason，不打印 DB password、完整 evidence payload、
  每行日志或无诊断价值 heartbeat 噪声。
- PostgreSQL error 保留 SQLSTATE、operation 和 relation/category，不输出连接秘密。

## 21. Concurrency And Exact Retry / 并发与幂等

- 一个 build 同时只有一个 active attempt；现有 lease/fencing/row-version 是唯一并发 authority。
- filesystem staging 按 build/attempt 隔离；CAS publish 只允许 create-if-absent。
- file、manifest、promotion receipt、snapshot 均采用 content identity；同 identity 同 bytes 为 exact retry，
  同 identity 不同 bytes 为 conflict。
- DB checkpoint transaction 不包围大文件复制；大文件写入完成并 durable 后才执行短 checkpoint transaction。
- 旧 fencing token 无法完成任何 checkpoint，即使其 staging/CAS 文件仍存在。
- 清理只删除明确属于 failed/expired attempt 的 staging；不删除 CAS blob、manifest、receipt 或 SEALED membership。

## 22. Implementation Plan / 实施顺序

### D1：schema registry 与 typed receipts

- 实现全部 Arrow schemas、canonical schema descriptors、logical role/partition/sort registry。
- 实现 materialization/full verify/manifest/promotion/seal typed models 和 hash derivation。
- 扩展 in-memory oracle，保持 Batch C `complete_verify()` 行为不变。

### D2：database streaming writer

- 实现单 transaction source resolver、server cursor/record batch、evidence exact read 和 deterministic writer。
- 完成完整 logical fileset、attempt descriptors 和 MATERIALIZED receipt。

### D3：full verifier

- 实现 per-file 全量 readback 和 cross-file relational closure。
- 新增 `complete_full_verify()`，只能接受 typed full receipt。

### D4：shared local CAS 与 promotion

- 提取 shared local CAS primitive，保持 calculation evidence store 回归零差异。
- 实现 dataset store、capacity contract、manifest/receipt publication、reopen 和 `complete_promote()`。

### D5：seal、resume 与 golden

- 从 receipts 组装 snapshot，调用现有 seal transaction，完成 readback。
- 实现 offline CLI build/verify/resume 和全部 crash injection。
- 在真实 DEV PostgreSQL 与真实 local filesystem 上生成首个 SEALED golden，并重复构建证明确定性。

### D6：设计一致性审核

- 逐项执行 Design Acceptance Matrix 和 DESIGN-COMPLIANCE-001。
- 检查 shared runtime zero-diff、无 DDL、无审批/RBAC、无训练/runtime activation。
- 更新父级 Phase 1C-3 状态；只有 F-301 至 F-318 全部 verified 才标记 Batch D/Phase 1C-3 complete。

## 23. Verification Plan / 验证方案

### 23.1 L0 static

- `ruff`/compile/import 测试；FastAPI import 不加载 PyArrow/store。
- changed-file scan：Selection/Paper/模拟盘/QMT/策略包/routers/frontend zero-diff。
- source scan：无 approval/RBAC/role/grant/revoke/confirm-run/manual override/runtime DDL。
- schema registry 与 authority column allowlist 对齐。
- `python scripts/aistock_feature_workflow.py validate --design <this-doc> --tier F2`。
- `git diff --check`。

### 23.2 L1 pure and filesystem

- 所有 role schema、null/type/decimal/timezone/canonical JSON、partition/sort/duplicate negative tests。
- 同 logical rows 不同输入顺序仍产生同 bytes；同输入写两次每个文件 bytes/SHA 完全相同。
- 0-row role 仍生成 fixed schema file。
- full verifier 对 byte corruption、footer/schema/metadata/row order/duplicate/cross-ref/count/hash/capability drift 全部拒绝。
- CAS put/get/exact retry/hash-path conflict/capacity/root/same-volume/durability tests。
- calculation evidence store 全部既有回归通过。
- critical branches 100%，新增/修改模块 line coverage >= 85%，branch coverage >= 80%。

### 23.3 L2 DEV PostgreSQL

- 使用现有 DEV DB env 连接，不猜测连接参数；不运行 migration。
- 从真实 COMPLETE fixture captures/labels 创建 frozen request，执行一条完整 build 到 SEALED。
- 验证 source transaction split-view 防护、attempt fencing、full verify receipt、promotion CAS、seal membership/readback。
- 对每个 checkpoint crash 注入并 resume；无半成品被视为成功。
- base snapshot unchanged partition reuse 与 changed partition rewrite。
- invalidated base、missing evidence、stale mapping、wrong source set、schema drift、CAS conflict negative tests。

### 23.4 Golden determinism oracle

在同一个 bounded frozen source view 中，把完全相同的 typed record batches 送入两个独立 staging writer，执行：

```text
logical paths equal
all Parquet bytes equal
all file SHA/size/row/schema/partition hashes equal
manifest core bytes/hash equal
manifest bytes/hash equal
promotion receipt bytes/hash equal
snapshot_content_hash equal
snapshot_id equal
```

验证必须比较实际 bytes，不能只比较 Pydantic objects 或 row hashes。两个输出都必须通过 full verifier；其中
一个继续执行真实 CAS/PROMOTED/SEALED，另一个仅作为 DEV golden comparison artifact 并在验收后清理。不能为
同一已经 SEALED 的 logical key 伪造第二个 build generation。

### 23.5 Regression boundary

- Batch C foundation fileset tests 全部通过，仍不能 seal。
- capture/label/calculation evidence/dataset build PostgreSQL 既有测试通过。
- `stage_trace.py` 与 simulation selection 的 direct behavior regression zero-diff。
- 不启动服务、不访问生产 DB、不执行生产 DML、不训练模型。

## 24. Risks And Failure Modes / 风险与失败模式

| 风险 | 后果 | 设计控制 |
|---|---|---|
| PyArrow 配置漂移 | 相同 rows 不同 bytes | pin 21.0.0 + writer identity + exact golden |
| DB 多连接读取 | split-view snapshot | one repeatable-read read-only transaction |
| JSON/decimal/timestamp 隐式转换 | hash/数值漂移 | explicit Arrow types + canonical JSON + no float |
| authority 新列未输出 | 静默字段丢失 | information_schema allowlist parity failure |
| evidence blob 缺失 | label 无来源闭环 | exact descriptor/bytes typed read, whole-attempt failure |
| sample verifier 漏错 | 错误文件 promotion | full row/file verifier only |
| CAS overwrite | immutable snapshot 被改写 | hardlink create-if-absent + byte compare |
| receipt 包含 attempt/time | crash retry identity 漂移 | content-only manifest/promotion/seal receipts |
| manifest 已存在即被消费 | 未 seal 数据泄漏 | DB SEALED is sole consumer authority |
| 直接构造 snapshot | 绕过 verify/promotion | typed receipt assembly + repository state validation |
| base partition 误复用 | child 内容错误 | recomputed content hash + full verifier |
| 增加审批/人工确认 | 单用户流程阻塞 | explicit no-approval scope and code scan |
| 影响共享选股链 | Selection/Paper/模拟盘回归 | frozen zero-diff paths and direct regression |

## 25. Rollout, Rollback And Production Gates / 发布、回滚与生产状态

本设计文档任务及后续 Batch D 代码开发均不激活 runtime。Batch D implementation 预期无 DDL 与新依赖；真实
golden 只在隔离 DEV DB 和 repo-external DEV dataset store 中生成。

```text
production_ddl_gate = noop
production_dml_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop (pyarrow 21.0.0 already pinned)
runtime_activation = noop
model_training = noop
windows_model_training = prohibited
selection_paper_simulation_qmt_impact = none
```

以上是交付状态说明，不是新增运行门禁。代码回滚为 revert Batch D code/CLI；CAS 中未被 SEALED snapshot 引用的
orphan bytes 保留待 Phase 1I GC，不做危险自动删除。Batch C schema 和既有 snapshots 不回滚。

## 26. Design Acceptance Index

- F-301：完整 logical fileset 和每个 role 的显式 Arrow schema/authority mapping 已冻结。
- F-302：PyArrow 21.0.0 writer 对同一输入产生 byte-for-byte 相同 Parquet。
- F-303：一个 bounded repeatable-read read-only transaction 提供无 split-view 的完整 materialization。
- F-304：所有 calculation evidence 按 DB descriptor 完整读取和验证，无残缺 label fallback。
- F-305：MATERIALIZED descriptor、file-set 和 receipt 来自真实 close/reopen 文件。
- F-306：full verifier 对每个文件、row group、row 和 cross-file relation 全量校验，无 sampling。
- F-307：Batch C foundation verify 与 Batch D full verify API/receipt 不可混用。
- F-308：local dataset CAS 为 repo-external、create-if-absent、durable、exact retry 且不覆盖冲突内容。
- F-309：manifest、promotion receipt、seal receipt 为 content-only deterministic identities。
- F-310：`VERIFIED -> PROMOTED` 由真实 CAS reopen 和 repository 原子 checkpoint 完成。
- F-311：`PROMOTED -> SEALED` 使用现有单事务 snapshot aggregate，并执行 commit 后完整 readback。
- F-312：每个 crash point 均可恢复，不把 staging/orphan/timeout 当成静默成功。
- F-313：base snapshot 只复用完全不变的 partition，child manifest 始终完整。
- F-314：capability manifest 保留全维度并明确三个 readiness 均为 false。
- F-315：真实 DEV PostgreSQL + 真实 filesystem 产生两套 byte-identical materialization，其中一套完成真实
  SEALED golden。
- F-316：无简化版、mock-only success、静默错误或业务语义偏移。
- F-317：无用户、角色、授权、审批、人工门禁、manual override 或未经确认的运行条件。
- F-318：Selection/Paper/模拟盘/QMT/策略包/routers/frontend 零修改、零接线、行为零漂移；无训练。

## 27. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-301 | `snapshot_writer.py` schema/role registries | schema descriptor golden + authority-column parity | design_ready | none |
| F-302 | deterministic writer config and row batching | two-write byte equality for every role | design_ready | none |
| F-303 | PostgreSQL source reader/materialization coordinator | split-view and bounded cursor DEV tests | design_ready | none |
| F-304 | existing evidence store reader + outcome evidence mapper | missing/conflict/owner/policy closure tests | design_ready | none |
| F-305 | `MaterializationReceipt`; existing attempt file repository | real close/reopen/file-set exact tests | design_ready | none |
| F-306 | `FullParquetVerifier` | corruption/schema/sort/relational full-read matrix | design_ready | none |
| F-307 | `complete_full_verify()` separate from Batch C `complete_verify()` | cross-contract rejection tests | design_ready | none |
| F-308 | shared local CAS primitive; `LocalFilesystemDatasetStore` | real filesystem exact retry/conflict/durability tests | design_ready | none |
| F-309 | manifest/promotion/seal canonical models | canonical bytes and crash-retry identity golden | design_ready | none |
| F-310 | `complete_promote()` repository transaction | CAS reopen + fencing/checkpoint DEV tests | design_ready | none |
| F-311 | receipt-driven assembly + existing `save_sealed_snapshot()` | full membership/readback/rollback DEV tests | design_ready | none |
| F-312 | CLI resume and attempt recovery | checkpoint-by-checkpoint crash injection | design_ready | none |
| F-313 | base validator and partition reuse planner | unchanged reuse/changed rewrite/invalidation tests | design_ready | none |
| F-314 | exact capability manifest builder | multidimensional mismatch and readiness tests | design_ready | none |
| F-315 | offline CLI on DEV DB/store | two real materialization outputs byte-equal; one real SEALED golden/readback | design_ready | none |
| F-316 | error propagation, reason codes, complete implementation | no fallback/fake-success scan + negative tests | design_ready | none |
| F-317 | changed-file/source scan | no approval/RBAC/role/grant/revoke/manual gate | design_ready | none |
| F-318 | frozen path diff + direct regressions | shared runtime zero-diff and no-training evidence | design_ready | none |

## 28. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：设计包含真实 DB、真实 Parquet、真实 CAS、真实 promotion/seal 和真实 golden，
  不接受 mock/静态/子集交付。
- [x] `no_silent_error`：所有缺失、冲突、timeout、schema/hash/capability/receipt 错误均有稳定失败结果；无
  fallback/fake success。
- [x] `no_business_semantic_drift`：只消费已冻结 capture/selector/source/label authority，不执行 current/latest
  重新选择，不修改 Selection/Paper/模拟盘/QMT/策略包。
- [x] `no_unrequested_gate_or_approval`：未设计用户、角色、授权、审批、人工确认或 manual override；仅保留父级
  已批准且合法数据自动可满足的五类 identity/label/build/file-CAS/seal 数据不变量。
- [x] Batch C/full verification discriminator 前后一致，不能用 foundation receipt seal。
- [x] schema、transaction、filesystem durability、crash recovery、error、test 和 acceptance 均有实施位置。
- [x] 不新增 DDL、依赖、runtime activation 或训练路径；Windows 不训练模型。
- [x] 第一个真实 SEALED golden 是 Batch D/Phase 1C-3 完成的必要条件，不提前宣称完成。

## 29. Exit Criteria / 进入代码阶段条件

只有以下条件全部满足，Batch D 详细设计才可进入代码开发：

- F-301 至 F-318 全部为 `design_ready`，无未批准 gap、partial、todo 或 exception。
- F2 feature workflow validation 通过。
- 父级 Batch D 范围、Batch C 当前代码、PyArrow pin 和 production schema 事实一致。
- 合法 fixture 输入存在无需人工干预的完整正向链：
  `REQUESTED -> MATERIALIZED -> VERIFIED(full) -> PROMOTED -> SEALED`。
- 每个保留数据不变量均有合法输入通过用例和失败修复后的自动恢复用例，不存在永远无法通过的条件。
- 实现允许范围、zero-diff runtime 边界、无 DDL/依赖/训练/审批状态明确。

代码阶段完成时，必须把本矩阵逐项更新为 `verified` 并填写真实 implementation/test refs；任一条未验证时，
不得报告 Batch D 完成、不得请求代码合入、不得把 Phase 1C-3 标记 complete。
