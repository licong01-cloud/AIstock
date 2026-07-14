# AIstock Advisory Phase 1F.1 Observation Partition Schema Forward Migration F2 详细设计

## 1. Background / 文档定位与当前状态

本文是 Phase 1F release schema verification 与 Phase 1G observation capture DML 之间的必要
forward-schema 设计。Phase 1F v1 代码已由 PR `#2114` 合入，2026-07-14 DEV rehearsal 对 v1
contract 得到 `COMPATIBLE/COMPATIBLE/downstream_ready=true`；该结果证明 v1 contract 被准确应用，
不证明 v1 contract 已覆盖父级 Phase 1 的全部持久化语义。

Phase 1G 设计复核发现两个父级一致性缺口：

1. `app.advisory_signal_stage_evidence.content_hash` 与
   `app.advisory_signal_stage_candidate.candidate_content_hash` 被实现成全局唯一；父设计只要求
   `(observation_version_id, stage)` 与 `(stage_evidence_id, symbol)` 作用域内唯一。不同日期、版本或
   stage 合法出现相同局部内容时，当前约束会拒绝正常数据。
2. `app.advisory_signal_observation_lineage` 与 `app.advisory_signal_stage_candidate` 仍为普通表且没有
   冗余 `decision_as_of_trade_date`，但父设计要求按决策月 RANGE 分区。Phase 1G 开始累计历史数据后，
   继续使用 fixture-era 普通表属于未批准的简化实现。

当前状态：

```text
design_status = design_ready
implementation_status = code_complete
phase1f_v1_dev_receipt = compatible_but_contract_incomplete_for_phase1g
disposable_postgresql_l2 = passed
dev_ddl = not_executed
production_ddl = not_executed
business_dml = none
runtime_activation = none
```

任务分级为 `T3 / F2`。本设计只修正 Advisory Phase 1 自有 schema/release contract，不增加用户、
角色、RBAC、审批、授权链、人工确认表、双人复核或 DDL 前数据库导出/全库备份要求。数据库日常备份
与本 migration 相互独立；本设计不触发、检查或要求额外备份。

## 2. 目标

1. 让真实多日期、multi-stage、multi-revision observation 能在合法内容重复时自动写入。
2. 把高增长 lineage 与 stage-candidate payload 按 `decision_as_of_trade_date` 月分区。
3. 用非分区 identity 表保留跨月全局 ID/natural-key 唯一性，避免 PostgreSQL 分区唯一约束必须包含
   partition key 导致的语义弱化。
4. 通过只读兼容 view 保持当前 `snapshot_writer.py` 的 relation name、旧列集合和查询语义不变。
5. 用 `prepare -> typed partitions -> atomic cutover` forward sequence搬迁已有fixture/DEV rows，并在
   删除旧物理表前逐行对账。
6. 发布后由 Phase 1F v2 catalog verifier完整核对 identity/payload/view/partition/trigger/index。
7. 合法 schema 自动通过，不增加运行期 DDL、人工审批、manual bypass 或静默 fallback。

## 3. Scope 与 Non-goals

### 3.1 In Scope

- 两个全局 identity 表、两个 RANGE partitioned payload parent 和历史月 partitions。
- 两个保持旧读契约的只读 compatibility views。
- stage evidence/candidate content hash 唯一性修正。
- 原 relation 到 v2 physical layout 的事务型 exact copy/readback/swap。
- Phase 1F release registry/version、catalog contract、partition planner 和 receipts 更新。
- Phase 1G writer 对 v2 physical tables 的明确写入契约。
- disposable PostgreSQL、DEV plan/apply/verify/exact-reapply 设计。

### 3.2 Non-goals

- 不写 source、observation、label、snapshot 等业务 DML。
- 不执行 Phase 1G capture，不启动 observer/scheduler/FastAPI hook。
- 不改 Selection、StrategyPackage inference、模拟盘、Paper、QMT、QE/RD-Agent/Qlib/backtest。
- 不新增 API、UI、依赖、模型训练或 Windows/WSL job。
- 不修改既有历史 migration；只能新增 forward migration 和新 release contract version。
- 不提供 arbitrary SQL、`--force`、skip、ignore drift 或 destructive runtime repair。
- 不在本设计任务执行 DEV/production DDL。

## 4. Architecture / 权威关系与兼容边界

v2 写入权威：

```text
app.advisory_signal_observation_lineage_identity        global identity/natural key
app.advisory_signal_observation_lineage_payload         RANGE month payload parent
app.advisory_signal_stage_candidate_identity            global (stage_evidence_id, symbol)
app.advisory_signal_stage_candidate_payload             RANGE month payload parent
```

兼容只读面：

```text
app.advisory_signal_observation_lineage                 VIEW with exact v1 columns
app.advisory_signal_stage_candidate                     VIEW with exact v1 columns
```

Phase 1G writer只写 identity/payload physical tables。既有 `snapshot_writer.py` 继续按旧 relation name
读取 compatibility view；它不获得写能力，也不需要 import Phase 1G service。static test必须证明没有
其他 runtime module对两个 view执行 INSERT/UPDATE/DELETE。

## 5. v2 Schema Contracts / 数据库契约

### 5.1 Lineage identity

`app.advisory_signal_observation_lineage_identity` 至少保存：

```text
lineage_id PRIMARY KEY
decision_as_of_trade_date
observation_version_id
phase0a_audit_id
admission_scope_id
program_id
binding_version_id
lineage_source_type
source_run_id
lineage_content_hash UNIQUE
created_at preserved from v1
UNIQUE(observation_version_id, phase0a_audit_id, admission_scope_id,
       program_id, binding_version_id, lineage_source_type, source_run_id)
UNIQUE(lineage_id, decision_as_of_trade_date)
```

`decision_as_of_trade_date` 必须通过 trigger 与
`observation_version -> canonical_signal.decision_as_of_trade_date` 完全一致。

### 5.2 Lineage payload

`app.advisory_signal_observation_lineage_payload` 保存identity表未保存的其余lineage业务字段，按
`decision_as_of_trade_date` RANGE分区；同一v1字段只在identity或payload一处具有权威值：

```text
PRIMARY KEY(decision_as_of_trade_date, lineage_id)
FOREIGN KEY(lineage_id, decision_as_of_trade_date)
  -> advisory_signal_observation_lineage_identity(lineage_id, decision_as_of_trade_date)
PARTITION BY RANGE(decision_as_of_trade_date)
```

每月 partition 名称、bound 与 Phase 1F typed month descriptor一致；无 default partition。

### 5.3 Stage-candidate identity

`app.advisory_signal_stage_candidate_identity` 保存：

```text
stage_evidence_id
symbol
decision_as_of_trade_date
registered_at
PRIMARY KEY(stage_evidence_id, symbol)
UNIQUE(stage_evidence_id, symbol, decision_as_of_trade_date)
```

date trigger 通过 `stage_evidence -> observation_version -> canonical_signal` 逐层核对。该表不保存 score、
component 或 membership payload，不形成第二份业务权威。

### 5.4 Stage-candidate payload

`app.advisory_signal_stage_candidate_payload` 保存除identity key外的v1 candidate业务列并新增
partition key；v1 `created_at`只保存在payload：

```text
decision_as_of_trade_date
stage_evidence_id
symbol
membership/rank/score/exclusion fields
component capability/evidence/reason fields
candidate_content_hash
created_at
PRIMARY KEY(decision_as_of_trade_date, stage_evidence_id, symbol)
FOREIGN KEY(stage_evidence_id, symbol, decision_as_of_trade_date)
  -> advisory_signal_stage_candidate_identity(...)
PARTITION BY RANGE(decision_as_of_trade_date)
```

`candidate_content_hash` 是候选局部内容摘要，可在不同 identity 中相同，因此只建普通 btree index，
不建 UNIQUE。全局 row identity 由 identity table的 `(stage_evidence_id, symbol)` 保证。

### 5.5 Stage evidence hash

`app.advisory_signal_stage_evidence` 保持普通 immutable table以及
`UNIQUE(observation_version_id, stage)`。删除全局 `UNIQUE(content_hash)`，替换为普通 btree index。
不同 observation version的相同 stage内容可以共享相同 content hash，但仍是两个父子关系不同的 row。

### 5.6 Compatibility views

两个 view必须按 v1 relation 的列名和类型精确输出，不暴露新增 partition key：

```text
lineage view = identity JOIN lineage_payload USING(lineage_id, decision_as_of_trade_date)
candidate view = candidate_identity JOIN candidate_payload
                 USING(stage_evidence_id, symbol, decision_as_of_trade_date)
```

catalog contract冻结 view definition hash。snapshot writer 的 `SELECT * ... WHERE observation_version_id`
和 `... WHERE stage_evidence_id` 结果列集合、row content与迁移前完全一致；Phase 1G不得写 view。

## 6. Forward Migration 算法

新增两个frozen migrations，中间由executor-managed typed partition operation连接。既有 Phase 1F v1 的
`add_advisory_phase1f_schema_canonicalization_20260714.sql`原登记为 order 70；v2 release registry 保留其
字节和 SHA-256 不变，但将其 contract 排序号规范为 order 55，以避免与本阶段的 typed partition order 70
冲突，并确保新库仍执行既有 canonicalization：

```text
backend/db/migrations/add_advisory_phase1f1_observation_partition_prepare_20260714.sql
Phase 1F executor-managed lineage/candidate/label month partitions
backend/db/migrations/add_advisory_phase1f1_observation_partition_cutover_20260714.sql
```

plan先用只读query从旧lineage/candidate父链计算`Phase1F1LegacyMonthInventory`：old row counts、resolved
decision month set和inventory hash。它不读取候选payload内容、不写数据库，也不成为业务数据authority。
目标month set是显式capacity range与legacy month set的并集，保证任何既有row都可搬迁且不创建default
partition。无法唯一解析date时plan直接失败。

v2 contract同时冻结v1 registry文件名、v1 `contract_content_hash`以及必须精确匹配的lineage、stage-candidate、
stage-evidence relation scope。verifier仅在三个scope按v1 contract完整核对relation/column/constraint/index/
trigger/comment且零差异时，才把普通表到compatibility view的`r -> v`转换和旧content-hash UNIQUE识别为
order 80可修复状态。仅同名、仅relkind相同、同名但定义不同或额外子对象均为unknown drift，不得进入cutover。

order 60 `prepare`在单个file-wrapped transaction中：

1. 创建identity tables与空partitioned payload parents、FK/check/index/comments/immutable triggers。
2. 不复制旧row、不改变旧authority relation，不创建compatibility view。
3. exact reapply完整readback；同名对象不同定义为drift。

order 70由typed planner为lineage/candidate/label payload parents创建完整目标月partitions并readback bounds。

order 80 `cutover`使用SHA冻结的SQL template与executor-managed单事务。executor只允许绑定plan中冻结的
`legacy_inventory_hash`、row counts和month set；通过transaction-local typed parameters传入，禁止字符串
拼接、任意SQL或运行时重算后覆盖plan值：

1. 锁定旧 lineage、stage candidate、stage evidence及必要父表，拒绝并发 writer。
2. executor在同一锁内重算canonical legacy inventory并与plan hash一致；template再次核对冻结row counts和
   legacy/target month set，防止prepare与cutover之间出现未计划writer；任一变化回滚。
3. 从旧表按稳定PK顺序插入identity与payload，保留全部timestamp/decimal/JSON/hash。
4. 用双向`EXCEPT ALL`进行NULL-safe逐列对账；old-only、new-only或内容差异任一非零即抛错。
5. 验证每月row count总和、全局identity count与旧表一致。
6. 删除旧物理表并创建同名compatibility views。
7. 删除stage evidence全局`UNIQUE(content_hash)`并建立普通索引；candidate content-hash UNIQUE随旧
   物理表消失，在payload上建立普通索引。
8. 完整catalog verify；transaction成功后commit。

迁移不得使用UPDATE旧业务行、truncate后重填、忽略冲突或保留两个可写authority。prepare/partition
已提交而cutover失败时，旧v1 relation仍是唯一可写authority，v2 prepared objects保持空且不可消费；
release plan将其分类为可续跑的exact partial state。cutover自身失败整事务回滚，不会留下半复制/半切换
数据。commit后verify失败按Phase 1F既有规则返回非零并通过新的forward migration修复，不在运行时
反向改库。

## 7. Release Contract v2

Phase 1F registry升级为新 schema version：

```text
order 10..50 = existing frozen migrations
order 55 = existing frozen schema-canonicalization migration (v1 registry order 70, source/SHA unchanged)
order 60 = FILE_WRAPPED prepare migration
order 70 = EXECUTOR_MANAGED typed month partitions for lineage/candidate/label payload parents
order 80 = EXECUTOR_MANAGED frozen cutover SQL template + typed legacy inventory parameters
```

release plan/apply receipt新增`legacy_inventory_hash`、旧两表row counts、legacy/target month set hashes以及
order 60/70/80逐项状态。executor在order 80 transaction开始并持锁后重算canonical inventory hash，匹配
后设置transaction-local typed parameters；template只重算并比较数据库row counts/month set，不能从
current rows自行生成“新的期望值”。

v2 registry的`predecessor_contract`必须引用repository内冻结v1 registry并固定其content hash与exact
relation scope；v1 contract不得声明该字段。predecessor文件、hash或scope任一不一致均为contract error。

v2 catalog closure必须包含：

- identity/payload relations的columns、PK/UK/FK/check/index/comments/triggers；
- compatibility views的relkind、列、definition hash；
- stage evidence普通 content-hash index且旧 UNIQUE不存在；
- lineage/candidate/label三个 payload parent的 RANGE key与完整月 partition bounds；
- snapshot writer只读兼容列集合；
- 旧两个物理普通表不再存在。

当前 v1 receipt不能被 Phase 1G persistent DML消费。Phase 1G必须读取 exact v2 apply/verify receipt，
并要求 managed/prerequisite均 `COMPATIBLE`、`downstream_ready=true`。这是自动 schema事实，不是角色、
审批或人工授权。

## 8. Database Connection 与执行边界

- 连接只来自 `F:\Dev\AIstock\.env` 或 CLI显式 `--env-file` 的 exact DEV/production keys。
- DEV缺 key不回退 production；不猜测 host/port/database/user，不输出 password/DSN。
- 代码合入、DEV DDL、production DDL、Phase 1G业务 DML分别报告。
- DEV/production apply只通过既有 Phase 1F release CLI的显式 plan/apply/verify调用。
- 不设计 backup precondition、approval row、role check或人工确认 token。
- production DDL不在代码合入或 DEV验证时自动执行。

### 8.1 Production Gates (All Noop) / 生产状态

本标题用于F2交付状态分离，不表示新增应用门禁：

```text
production_dependency_gate = noop
production_ddl = not_executed
production_dml = none
runtime_activation = none
role_or_approval_gate = none
backup_gate = none
```

代码或文档合入不执行production操作。后续只有用户明确要求执行某次production DDL时，发布工具才按
exact `.env` production keys运行既有plan/apply/verify流程；程序内部没有审批实体或等待状态。

## 9. Rollout And Rollback / 发布、事务与可恢复性

- migration使用固定 lock/statement timeout，零自动 retry/backoff。
- plan冻结 migrations/template SHA、pre-catalog fingerprint、legacy inventory、目标month set和
  requested operation。
- apply后必须基于新 catalog生成新的 VERIFY plan；exact reapply也生成新的 APPLY plan。
- stale plan返回 `PHASE1F_PLAN_STALE` 且零 DDL/DML。
- 只接受exact v1 predecessor或本设计定义的prepared/partitioned partial predecessor；未知drift不做
  猜测转换。
- exact reapply在v2 compatible时 `ddl_executed=false`、fingerprint一致。

## 10. Isolation 与影响矩阵

| 模块 | 允许变化 | 禁止变化 |
|---|---|---|
| Advisory Phase 1 release | v2 registry/migration/verifier/tests | runtime DDL/startup hook |
| Phase 1G writer | 写v2 identity/payload | 写compatibility view/shared表 |
| snapshot writer | 零代码行为变化，继续读view | 改读取结果/文件语义 |
| Selection/策略推理 | none | import/call/schema dependency |
| 模拟盘/Paper/QMT | none | runtime或数据变化 |
| QE/RD-Agent/Qlib/backtest | none | 读取、写入或训练 |
| API/UI | none | 新入口或展示 |

`snapshot_writer.py` frozen query与Arrow schema必须用 migration前后同一 fixture做 byte/hash parity；若 view
不能保持完全一致，migration不得合入，不能通过修改 snapshot输出掩盖差异。

## 11. Risks And Failure Modes / 风险、错误与日志

| risk / failure mode | consequence | treatment |
|---|---|---|
| content hash保持全局UNIQUE | 合法跨日期/版本写入失败 | v2普通索引 + scoped identity |
| 分区表直接替换无全局identity | 跨月fork/重复 | 非分区identity table |
| prepare后出现新legacy row | cutover漏数 | inventory hash在锁内重算并回滚 |
| view列漂移 | snapshot文件语义改变 | migration前后byte/hash parity |
| 缺月partition | cutover/capture失败 | capacity+legacy月份并集typed planner |
| partial release被假称ready | Phase 1G写入未完成layout | v2 full catalog receipt only |
| cutover中途失败 | 半复制/半切换 | 单事务copy/parity/swap |
| shared module被改写 | Selection/Paper/模拟盘回归 | changed-path/import/query sentinels |

至少冻结：

```text
ADVISORY_PHASE1F1_PREDECESSOR_SCHEMA_INVALID
ADVISORY_PHASE1F1_PARENT_DATE_UNRESOLVED
ADVISORY_PHASE1F1_COPY_MISMATCH
ADVISORY_PHASE1F1_VIEW_CONTRACT_MISMATCH
ADVISORY_PHASE1F1_PARTITION_MISSING
ADVISORY_PHASE1F1_CATALOG_DRIFTED
ADVISORY_PHASE1F1_POST_COMMIT_VERIFY_FAILED
ADVISORY_PHASE1F1_POST_FAILURE_VERIFY_FAILED
```

日志只记录 target label、去敏 DB identity、plan/migration hash、row counts、partition、operation stage和
稳定 reason；unexpected exception及失败后的catalog readback exception保留后台 traceback。post-failure
readback失败必须作为第二条结构化error进入FAILED receipt，不得回退成未说明的旧catalog事实。不得输出
完整 row payload、候选列表、密码或无价值逐行成功日志。失败非零退出，不生成成功 receipt。

## 12. Implementation Plan / 实施方案与文件

```text
backend/db/migrations/add_advisory_phase1f1_observation_partition_prepare_20260714.sql
backend/db/migrations/add_advisory_phase1f1_observation_partition_cutover_20260714.sql
backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v2.json
backend/services/advisory_phase1/release_schema_contract.py
backend/services/advisory_phase1/release_schema_verify_postgres.py
backend/services/advisory_phase1/release_schema_apply_postgres.py
scripts/advisory_phase1_release_schema.py
backend/tests/advisory_phase1/test_release_schema*.py
backend/tests/advisory_phase1/test_phase1f1_*.py
docs/architecture/advisory_phase1f1_observation_partition_schema_forward_migration_f2_design_20260714.md
```

Phase 1F.1不得修改 snapshot writer、Selection、inference、模拟盘、Paper或训练代码。Phase 1G后续 writer
文件不属于本 migration实现批次。

## 13. Verification Plan / 验证方案

### 13.1 L0/L1

- migrations/registry SHA/order/dependency/contract canonicalization与legacy inventory hash。
- no shared imports、no DML/runtime/role/approval/backup/static scan。
- frozen v1 predecessor hash/scope、exact predecessor与unknown child/relation drift分类。
- content hash重复的合法 stage/candidate fixtures。
- compatibility view列名/type/order contract。

### 13.2 L2 Disposable PostgreSQL

1. fresh v2 full-order apply，prepare/partitions/cutover与三个历史月parents/partitions完整。
2. 从含重复 stage/candidate content hash的v1 fixture迁移成功。
3. lineage/candidate migration前后双向逐行一致。
4. compatibility view与旧 relation dump/hash一致；同一fixture通过真实snapshot reader、冻结Arrow schema
   与deterministic Parquet writer得到逐文件bytes/hash一致。
5. orphan date、prepare后legacy inventory变化、row mismatch、wrong partition、view drift分别失败；
   cutover事务完整回滚且旧authority仍可读。
6. v2 verify/exact reapply fingerprint一致、第二次DDL为false。
7. physical identity/payload parent/partition immutable triggers拒绝非法mutation，compatibility view拒绝DML。
8. database/container销毁，零 DEV/production连接。

### 13.3 L3 Persistent DEV

在用户后续明确要求执行该 DEV DDL时：

1. 使用 `.env` exact DEV keys生成plan；
2. apply v1->v2 forward migration并readback；
3. 新 VERIFY plan/receipt；
4. 新 APPLY exact-reapply plan/receipt，`ddl_executed=false`；
5. snapshot compatibility read-only smoke；
6. 保存repo-external immutable receipts。

不执行 Phase 1G业务 DML、observer、模型训练或production DDL。每日数据库备份事实不转换为每次 DDL
前备份要求。

## 14. 正向可达性

```text
exact v1 schema + complete existing rows + explicit month range
  -> deterministic v2 plan
  -> atomic identity/payload copy
  -> full bidirectional parity
  -> compatibility views
  -> v2 catalog COMPATIBLE
  -> new verify/exact-reapply receipts
  -> Phase 1G can consume v2 schema automatically
```

空表、存在合法fixture rows、跨月rows、相同局部content hash都必须通过。只有真实schema/data conflict
失败；没有角色、审批、授权或人工改库步骤。

## 15. Design Acceptance Index

- F-630：明确v1 receipt只证明v1 contract，不冒充父设计完整。
- F-631：stage/candidate content hash允许跨identity合法重复。
- F-632：lineage与candidate payload按decision month RANGE分区。
- F-633：identity tables保留跨月全局PK/natural-key唯一。
- F-634：compatibility views保持snapshot旧读契约完全一致。
- F-635：prepare/typed partitions/atomic cutover顺序冻结，删除旧表前完成双向逐行对账。
- F-636：v1 unknown drift与orphan明确失败，零静默修复。
- F-637：v2 registry覆盖table/view/partition/function/trigger/index/comment闭包。
- F-638：Phase 1G只消费exact v2 ready receipt。
- F-639：运行期无DDL executor、auto migration或fallback。
- F-640：Selection/inference/模拟盘/Paper/QMT/QE/Qlib零影响。
- F-641：无API/UI/scheduler/startup/dependency/model training变化。
- F-642：无角色、RBAC、审批、授权、manual bypass或额外备份。
- F-643：连接只用exact `.env` target keys，不猜测、不回退。
- F-644：apply/verify/exact-reapply使用新plan，stale plan fail-closed。
- F-645：重复hash、跨月、空表与fixture rows均有正向用例。
- F-646：L2 migration failure整事务回滚，database/container销毁。
- F-647：DEV与production执行状态分开，设计/合入不自动执行DDL。
- F-648：父蓝图、Phase 1、Phase 1F和Phase 1G状态前后一致。
- F-649：不得以不分区普通表或只改hash的简化版冒充完成。

## 16. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-630 | `release_schema_contract.py`, `release_schema_apply_postgres.py` | historical v1 parser + v2 plan/receipt tests | verified | none |
| F-631 | v2 migration prepare/cutover, v2 registry | L2 scoped duplicate stage/candidate content-hash test | verified | none |
| F-632 | v2 payload parents, typed partition planner | L0 planner + L2 cross-month physical partition assertions | verified | none |
| F-633 | identity PK/UK/FK contract | L2 duplicate lineage natural key and candidate identity rejection | verified | none |
| F-634 | cutover compatibility views, view definition hash | L2 pre/post `SELECT *` parity plus real snapshot reader/Arrow/Parquet bytes and hash parity; compatibility view DML rejection | verified | none |
| F-635 | orders 55/60/70/80, typed executor actions | L2 v1 forward apply, atomic cutover failure and exact resume | verified | none |
| F-636 | frozen v1 predecessor contract/scope, typed legacy inventory and stable reasons | L2 unknown v1 child drift, isolated legacy remnant, compatibility-view definition drift, orphan and stale inventory failures | verified | none |
| F-637 | v2 registry, catalog projection/verifier | v2 registry hash/load and full L2 catalog verify/reapply | verified | none |
| F-638 | v2 plan/receipt schema records inventory | receipt contract explicitly distinguishes v1/v2 and carries frozen inventory for the later Phase 1G consumer | verified | none |
| F-639 | release-only executor modules | import-boundary and runtime-DDL static tests | verified | none |
| F-640 | scoped Advisory Phase 1 paths only | changed-path/import denylist checks prove no shared runtime file changed | verified | none |
| F-641 | no API/UI/scheduler/dependency edits | changed-path static review | verified | none |
| F-642 | no role/approval/backup implementation | existing no-approval/no-backup static test | verified | none |
| F-643 | existing exact env resolver and release CLI | resolver unit test; no DEV/production connection in this batch | verified | none |
| F-644 | frozen plan revalidation and v2 receipt | L2 verify/exact-reapply and legacy-inventory stale-plan test | verified | none |
| F-645 | full v1-to-v2 migration path | L2 empty, cross-month, duplicate-hash and identity cases | verified | none |
| F-646 | single-transaction cutover executor and structured failure readback | L2 injected copy mismatch/CREATE VIEW failure, rollback/resume and post-failure readback traceback/receipt evidence | verified | none |
| F-647 | release CLI/status separation | explicit code/L2/DEV/production state documented | verified | none |
| F-648 | this design plus parent status updates | parent-reference audit | verified | none |
| F-649 | full v2 migration/registry/verifier/test scope | DESIGN-COMPLIANCE-001 review | verified | none |

## 17. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：同时解决重复hash、全局identity、月分区、迁移和兼容读，不只删约束。
- [x] `no_silent_error`：orphan/copy/view/catalog/post-commit均有稳定reason与非零退出。
- [x] `no_business_semantic_drift`：只改变Advisory Phase 1物理存储，旧只读projection byte/hash parity。
- [x] `no_unrequested_gate_or_approval`：没有角色、审批、授权、人工确认或backup precondition。
- [x] `positive_path_satisfiable`：合法v1数据自动迁移、验证、重放，无人工UPDATE。
- [x] `database_connection_truth`：只读exact `.env` target keys。
- [x] `production_truth`：设计、代码、DEV DDL、production DDL、Phase 1G DML分别报告。

## 18. Exit Criteria

设计完成条件：F-630至F-649全部 `design_ready`，父级文档同步，F2 validator与`git diff --check`
通过。

Phase 1F.1代码可请求合入条件：migration/registry/verifier/executor完整实现，L0-L2和
DESIGN-COMPLIANCE-001通过，snapshot compatibility parity无差异；不得以fixture-only migration或
只删除UNIQUE约束冒充完成。

Phase 1F.1 DEV完成条件：后续显式执行DEV plan/apply/new-verify/new-exact-reapply，v2 catalog
`COMPATIBLE/COMPATIBLE/downstream_ready=true`且immutable receipts完整。此后Phase 1G才可执行persistent
DEV observation DML；不要求production DDL先完成。
