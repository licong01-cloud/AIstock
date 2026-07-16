# AIstock Advisory Real DEV Dual-Track Input Onboarding F2 Design

## 1. Background / 文档定位

本文解决真实数据库盘点确认的 Phase 1E / Phase 1G G5 前置缺口：production 存在单 Alpha/原生多 Alpha
package及其Program，但现有binding为legacy null interval，目标包DSE全部是不能被historical runner消费的v1；DEV只有
新schema，没有对应package、Program、dated binding、DSE v2或历史研究receipt。现有
`scripts/dev_db/batch_a_import_real_data.py` 会
`TRUNCATE` 并全量改写 StrategyPackage、Paper v2 等无关 DEV 关系，而且不覆盖 Advisory/Selection 精确证据，
不能用于本任务。

本文是以下父设计之间唯一的真实 DEV 输入 onboarding 子设计：

- `advisory_phase0a2_evidence_readiness_bootstrap_f2_design_20260711.md`；
- `advisory_phase1e_dual_track_readiness_execution_plan_f2_design_20260714.md`；
- `advisory_phase1g_g5_dev_evidence_f2_design_20260716.md`；
- `advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md`。

任务分级为 `T3 / F2`。模块为 `advisory_dev_input_onboarding`，风险为跨数据库身份、不可变证据、PIT
时间和共享表 DEV 数据隔离。它是一次性研究证据准备工具，不是荐股运行时、Selection 运行时、数据同步服务、
scheduler 或审批系统。

当前状态：

```text
design_status = accepted
o1_implementation_status = merged_pr_2231
o2_implementation_status = code_validated_pending_review_and_merge
o3_to_o5_implementation_status = not_started
dev_database = schema_ready_but_real_dual_track_input_absent
production_database = read_only_source_only
production_ddl = none_for_this_design
production_dml = prohibited
production_readonly_export_execution = not_run
dev_ddl = none
dev_import_plan_execution = not_run
dev_import_rollback_validation = not_run
dev_import_persistent_execution = not_run
runtime_activation = none
role_or_approval_gate = none
model_training = none
```

设计完成不代表 onboarding、Phase 1E E6、G5 L3/L4 或 Phase 1G 已完成。

## 2. Verified Current Facts / 已核验现状

2026-07-16 使用 `F:\Dev\AIstock\.env` 中的精确连接信息执行只读盘点，未输出凭据，得到：

| 事实 | DEV | production |
|---|---:|---:|
| `app.advisory_program` | 0 | 6 |
| `app.advisory_strategy_binding_version` | 0 | 6 |
| `app.advisory_research_batch_receipt` | 0 | relation absent |
| `app.advisory_research_program_run` | 0 | relation absent |
| `selection.daily_selection_evidence` | 0 | 239 |
| `selection.run` | 21 | 1735 |
| `strategy_pkg.selection_score_artifact` | 43 | 220 |
| `app.advisory_source_availability_event` | 1 | 0 |

DEV 的 4 个 package 均为 `single_alpha`。production 中当前启用的目标双轨为：

- 单 Alpha：`pkg_378eb9c91e104c64935404e257e932ee`，73份DSE v1，日期覆盖2026-04-28至
  2026-07-16；
- 原生多 Alpha：`pkg_ma_8ec5e389fa2c5e484a1ac7e9`，26份DSE v1，日期覆盖2026-06-01至
  2026-07-16；
- 99份目标DSE的`schema_version`均为`daily_selection_evidence_v1`，缺少historical runner要求的
  `research_scope`、`market_data_scope`和`execution_prohibited`，不可导入、升级或冒充v2；
- single package具有59条`package_asset`、1个declared component和完整runtime/source evidence；native multi
  parent具有37条`package_asset`、2个declared components和完整runtime/source evidence，package闭包正向输入存在；
- 两者各有一个`ENABLED` Program和一个`ACTIVE` binding，但`effective_from_trade_date`均为NULL，不能作为
  historical dated binding；
- production 的 historical-research runner relation 尚未部署，因此 production 不能作为本设计的写入或
  runner 目标；
- frozen policy `advisory_phase0a/v1` 已验证，registry content hash 为
  `68538d81784294f9b6a6d09c46df274438fb1f34ce0ff5d6da68cb3dbdf86d64`。

这些值是盘点事实，不是硬编码默认值。实现必须重新 inventory 并由显式 request 固定 exact
Program/package/date/hash；不得按名称、状态、mtime 或“最新”隐式选择。

## 3. Scope / 范围与目标

1. 从production只读导出显式选择的单Alpha、原生多Alpha package及其资产/组件最小不可变闭包；现有DSE v1和
   legacy binding只作为不合格诊断，不进入bundle。
2. 把证据闭包保存为仓库外、content-addressed、可离线验证的 portable bundle。
3. 在 DEV 只执行固定关系 allowlist 的 `INSERT-or-compare`，不覆盖、不删除、不截断既有数据。
4. 通过现有AdvisoryProgram服务在DEV创建新的真实Program和future-effective dated binding，不复制或回填legacy
   null binding。
5. 使用正常Selection producer和显式`ADVISORY_RUN` prospective context在DEV原生生成DSE v2，不转换v1。
6. 使用现有DEV historical-research runner正常生成双轨batch/program receipts，不手写receipt。
7. 使用Advisory-owned read-only audit/handoff、Phase 1D source/capacity和Phase 1E compiler生成真实输入。
8. 区分identity-complete、source-pending和source-ready；不得把PARTIAL plan冒充G5 L3/L4 ready。
9. 在source-ready输入形成后恢复G5 inventory、L3 rollback zero-residue和L4 persistent dual-track。
10. 多个Program、多个package独立处理，不融合双轨，也不限制系统只能有两个package。
11. 全流程研究专用，保持`DB_HISTORICAL`、`MANUAL_HISTORICAL_RESEARCH`、
   `HISTORICAL_RESEARCH_ONLY` 和 `execution_prohibited=true`。
12. 不修改Selection、StrategyPackage、Paper、模拟盘、QE、QMT或交易运行时业务逻辑。

## 4. Non-goals / 非目标

- 不刷新、克隆或恢复整库，不使用 `pg_dump`/`pg_restore` 代替精确证据闭包。
- 不运行 `scripts/dev_db/batch_a_import_real_data.py` 或任何 `TRUNCATE` 型导入。
- 不写 production，不在 production 应用 historical runner DDL，不启动 production runtime。
- 不在 onboarding 内重新执行策略推理、Selection 排名、HMM、模拟盘、Paper 或交易。
- 不复制 Paper、simulation、QE、QMT、market 全表或用户账户数据。
- 不导入手工多包 fusion Program；只接受一个单 Alpha package 或一个原生多 Alpha 父包的 Program。
- 不从归档包、名称相似包、旧 manifest 或 union run 推断目标 package。
- 不导入production DSE v1、Selection run/result/artifact或legacy null binding，也不把v1 payload包装成v2。
- 不伪造 source availability、provider publish time、first-observed time、DSE、artifact、receipt 或 candidate。
- 不添加用户、角色、RBAC、审批、授权、双人复核、备份门禁、`--force`、`--skip` 或人工 SQL 流程。
- 不新增 API、UI、scheduler、startup hook、常驻 worker、自动重试循环或运行时 DDL。
- 不训练模型，不读取回测/Qlib/Paper 数据文件。

## 5. Architecture And Authority Boundaries / 架构、权威与隔离边界

### 5.1 Source authority

production 只作为既有不可变业务事实的 read-only source。连接必须从用户显式 `--env-file` 的
`TDX_DB_*` 读取，连接建立后由 PostgreSQL 强制 `default_transaction_read_only=on`，并验证
`transaction_read_only=on`。任何 production write statement 都必须在 SQL recorder 层拒绝。

### 5.2 Target authority

DEV 是唯一写入目标，连接只从同一显式 env 文件的 `TDX_DB_DEV_*` 读取。不得按 localhost、固定端口或
dbname 文本猜测 DEV；必须读取数据库 identity 并证明 source/target identity 不同、target 与已核验 Phase 1F.2
DEV receipt 的 environment contract 和 catalog fingerprint 一致。

### 5.3 Runtime isolation

新增模块只能依赖 Advisory-owned DTO/projection、固定 SQL repository 和 immutable contract。禁止 import：

```text
backend.services.selection_center
backend.services.strategy_package multi/live/runtime loaders
backend.services.simulation_runtime
backend.services.paper_trading*
backend.services.quantevolver
backend.services.rdagent*
backend.qlib_exporter
backend.infra.qmt*
```

portable bundle只读取StrategyPackage数据库身份和资产闭包。DEV证据生产编排脚本可在独立进程中组合现有
AdvisoryProgram与Selection producer，并显式注入DEV repositories；Advisory service模块、Phase1E/G5和现有运行时
均不得import该编排脚本或形成反向依赖。编排不得修改Selection算法、validator、排名、过滤或target构造。

### 5.4 Data boundary

import 只允许以下固定关系，并且只允许 request 闭包中的 exact rows：

```text
strategy_pkg.package
strategy_pkg.package_asset
```

`app.advisory_program`/binding由现有AdvisoryProgram service写入；Selection run/result/artifact/DSE由现有Selection
producer写入；`app.advisory_research_*`由现有historical runner写入；source event由现有Phase1D observer写入。
package importer不写这些关系。

### 5.5 Package asset store boundary

source package blob root和target DEV blob root都必须由CLI显式参数提供并解析到不同的仓库外目录。source root只读；
target root使用content-addressed no-replace写入。DEV prospective Selection必须显式注入target
`PackageAssetStore`，不得读取production asset root、修改进程全局环境或依赖当前backend的默认CAS配置。

## 6. End-to-End Data Flow / 端到端数据流

```text
explicit source package ids + target DEV Program specs
  -> production read-only package/asset/component inventory
  -> exact single/native-multi package portable closure
  -> PortableAdvisoryEvidenceBundle in external CAS
  -> offline full verification
  -> DEV read-only conflict plan
  -> one DEV transaction: package closure INSERT-or-compare only
  -> fresh DEV readback receipt
  -> normal DEV AdvisoryProgram service creates Program + dated binding
  -> normal DEV Selection producer creates DSE v2 with ADVISORY_RUN context
  -> existing historical research runner on exact DEV connection
  -> Phase 0A read-only audit + Phase 0A.1 handoff
  -> Phase 1D observer/capacity with exact DEV connection
  -> Phase 1E explicit input bundle + compile-batch
  -> G5 inventory
  -> source-ready: L3 then L4
  -> source-pending: truthful pending evidence, no G5 DML
```

## 7. Typed Contracts / 强类型契约

所有契约使用 Pydantic `extra=forbid`、canonical JSON、lowercase SHA-256、UTC aware timestamps 和稳定排序。

### 7.1 `RealDevOnboardingRequest`

```text
schema_version = advisory_real_dev_onboarding_request_v1
source_target = PRODUCTION_READ_ONLY
target_target = DEV
source_program_refs[]                 # diagnostic provenance only
source_package_ids[]
target_dev_program_specs[]
binding_effective_from_trade_date
decision_trade_date                   # must be completed and inside new binding interval
expected_program_packages{}
expected_package_manifest_sha256s{}
required_alpha_modes = [single_alpha, multi_alpha]
policy_registry_id/version/hash
release_receipt_ref
research_scope = HISTORICAL_RESEARCH_ONLY
execution_prohibited = true
request_hash
```

request是显式数据输入，不是审批或授权记录。target DEV Program identity由正常service创建；不能复用production
legacy binding。一次request可包含更多独立Program，但必须至少含一个single和一个native multi。

### 7.2 `RealDevOnboardingInventoryReceipt`

```text
source_database_identity
target_database_identity
release_catalog_fingerprint
program_candidates[]
common_completed_trade_dates[]
selected_request_hash|null
relation_row_counts{}
dependency_closure_hash|null
classification = DUAL_TRACK_AVAILABLE | INPUT_INCOMPLETE | TARGET_CONFLICT
reason_codes[]
observed_at
inventory_hash
```

inventory 可列出候选，但执行只能消费显式 request，不得自动选择最大日期或“当前包”。

### 7.3 `PortableRelationRowSet`

```text
relation_name
primary_or_natural_key_fields[]
semantic_column_names[]
source_provenance_column_names[]
column_contract_hash
sorted_rows[]
row_content_hashes[]
row_set_hash
```

JSON/array/timestamp/date/numeric 使用 PostgreSQL-aware typed serializer；不得用字符串拼接或数据库文本 dump
作为 canonical authority。

`strategy_pkg.package_asset.asset_id`是数据库代理键，只属于source provenance，不进入跨库semantic row hash；target
按`(package_id, asset_type, asset_ref)`自然键分配自己的asset id，并对asset SHA、metadata、protected flag等全部
semantic字段full compare。其他relation若存在代理键，必须在冻结column contract中逐一显式分类，禁止临时忽略。

### 7.4 `PortableAdvisoryEvidenceBundle`

```text
schema_version = advisory_real_dev_portable_bundle_v1
request
source_database_identity_hash
export_snapshot_identity
package_refs[]
native_multi_component_refs[]
relation_row_sets[]
artifact_blob_refs[]
dependency_edges[]
dependency_closure_hash
bundle_content_hash
```

bundle不保存密码、DSN、绝对本机路径、模型明文、DSE、Selection候选或非package闭包行。它不搬运或转换
production DSE v1；DEV DSE v2必须由正常producer新生成。

### 7.5 `RealDevImportPlan`

```text
bundle_ref
target_database_identity
release_receipt_ref
insert_rows_by_relation{}
exact_match_rows_by_relation{}
conflict_rows_by_relation{}
ordered_write_operations[]
planned_write_relation_set
plan_hash
status = EXECUTABLE | ALREADY_PRESENT | CONFLICT
reason_codes[]
```

只有 `EXECUTABLE` 或 `ALREADY_PRESENT` 可进入 import。`CONFLICT` 返回 exact identity/hash 差异且零 DML；
不提供覆盖或忽略冲突选项。

### 7.6 `RealDevImportReceipt`

```text
request_hash/bundle_hash/plan_hash
source/target database identity hashes
transaction_id
inserted_row_counts{}
matched_row_counts{}
write_relation_set
post_readback_row_hashes{}
post_dependency_closure_hash
physical_commit_count
commit_outcome = COMMITTED | ALREADY_PRESENT | STATE_UNKNOWN
started_at/finished_at
reason_codes[]
receipt_hash
```

`COMMITTED` 必须完整 readback；commit response 丢失时用新连接按 exact keys 判定 committed/not-committed/
state-unknown，不静默重试写入。

### 7.7 `Phase1ERealInputBundle`

```text
historical_batch_receipt_ref
phase0a_audit_refs[]
handoff_bundle_refs[]
source_requirement_registry_ref
capacity_request_ref
capacity_receipt_ref
phase1e_revalidation_batch_request_ref
input_bundle_hash
readiness = IDENTITY_COMPLETE_SOURCE_PENDING | SOURCE_READY | BLOCKED
reason_codes[]
```

该bundle只引用真实输出；未形成的ref保持缺失并返回稳定reason。`IDENTITY_COMPLETE_SOURCE_PENDING`可以编译
PARTIAL/diagnostic plan，但不得被G5当作L3/L4 ready。

## 8. Production Export Algorithm / 生产只读导出算法

1. 从显式 env 文件分别解析 production 和 DEV 配置，不使用 global pool。
2. production 连接设置 server-enforced read-only、`REPEATABLE READ` 和有界 statement timeout。
3. 验证source Program仅作为provenance存在；其legacy null binding和DSE v1明确标记`INELIGIBLE_SOURCE_FACT`，
   不进入portable bundle。
4. 验证single/native-multi package identity，并按StrategyPackage权威canonical算法从`manifest_json`重算current
   manifest hash；允许状态固定为当前Selection可见的非`RETIRED`已知生命周期，未知状态和`RETIRED`均不导出。
5. 对 native multi parent 从 manifest 恢复全部 component package identities 和 component evidence；不同合法
   lookback/window 保持独立，不要求腿级窗口相同。
6. 读取parent/component package和package_asset的exact闭包；runtime blob只从`factor_set`、`model_asset`、
   `runtime_assets`及`source_evidence.multi_alpha.legs[].runtime_assets/seed_runtime_assets`投影，从显式只读source
   asset root加载、逐blob SHA验证并进入bundle CAS。不得递归收集其他`source_evidence`，与Selection运行无关的
   历史输出文件不加载。
7. 重新计算package/manifest/asset/component/policy canonical hashes，不继承source Program/binding hash作为DEV身份。
8. 构造dependency graph；任一必需child缺失、重复、hash不同或越界时，整个bundle不发布。
9. bundle以`<root>/advisory/dev-onboarding/bundles/<prefix>/<bundle_hash>.json` atomic no-replace发布并
    full readback。
10. 关闭production transaction并记录rollback；全流程production write query count必须为0。

## 9. DEV Import Algorithm / DEV 导入算法

### 9.1 Pre-DML plan

1. 离线验证 bundle raw SHA、semantic hash、dependency closure和所有 child refs。
2. 使用 current Phase 1F.2 DEV receipt 对 target catalog 做 fresh read-only verification。
3. 对 allowlist 每个 exact key读取 DEV 当前行并分类 `INSERT`、`EXACT_MATCH`、`CONFLICT`。
4. 任一conflict时输出`CONFLICT` plan并保持整个DEV零DML；不得部分导入非冲突package closure。
5. 检查计划写关系集合严格等于固定 allowlist子集；出现未知关系或 DDL/UPDATE/DELETE/TRUNCATE 即拒绝。

### 9.2 One transaction import

一个 owner connection、一个 physical transaction，按依赖顺序执行固定参数化 SQL：

```text
strategy_pkg.package (components before parent)
strategy_pkg.package_asset
```

每行使用 `INSERT ... ON CONFLICT DO NOTHING` 后立即按 exact identity full readback compare。不得只比较 PK、row
count 或部分列。任何 mismatch 整个 transaction rollback。禁止 `session_replication_role=replica`、constraint
disable、deferred manual repair 或 sequence reset。

数据库transaction前先把bundle blob原子no-replace物化到独立target DEV asset root并full readback；若随后DB
transaction失败，已存在blob作为未引用content-addressed对象保留，不删除、不覆盖。DB提交前再次验证每个package_asset
row都能在target store解析到exact SHA。source/target blob root不得相同。

### 9.3 Commit and uncertainty

全部 relation full readback 与 bundle closure一致后才物理 commit。commit response loss 使用 fresh connection
读取全部 exact keys：全匹配为 committed，全部不存在为 not committed，混合状态为 `STATE_UNKNOWN` 并停止；不得自动补写。

### 9.4 Persistence semantics

成功导入的 DEV rows 是真实研究证据，不在验收后 DELETE。代码回滚只停止新导入；相同 bundle 重跑必须
`ALREADY_PRESENT` 且零 DML。需要修正时使用新的 production authority/bundle identity，不修改旧 immutable 行。

## 10. Historical Research And Audit / 历史研究与审计

新增standalone DEV编排CLI，但不新增Selection或Advisory业务实现。它先通过现有AdvisoryProgram service创建DEV
Program和合法dated binding，再通过现有SelectionCenterService/StrategyPackageSelectionService的公开调用面，传入
`ProspectiveSelectionContext(capture_mode=PROSPECTIVE, execution_origin=ADVISORY_RUN)`原生生成DSE v2，最后把exact
`HistoricalResearchBatchRequest`交给现有`HistoricalAdvisoryResearchRunner`。所有repository/resolver显式注入
`TDX_DB_DEV_*` connection factory，不得通过production backend API、`backend.main`或global pool运行。
StrategyPackage runtime和asset resolver同样显式注入O2生成的target DEV package asset store，禁止使用production或
当前常驻backend的默认asset root。

固定流程：

1. Program create request显式固定target package、review policy、runtime config和future-effective binding date；不
   backdate、不修复或复制production legacy binding。
2. decision date必须是binding生效后已经完成的交易日；当前没有合法日期时输出`INPUT_PENDING`，不得改日期语义。
3. prospective Selection对每个Program/package独立运行，必须产生唯一DSE v2、artifact v2和完整stage/source receipts；
   v1、capture failed或incomplete evidence不能继续。
4. read-only preflight验证exact dated binding和唯一DSE v2；正常runner对各Program独立事务执行并产生正式receipt。
5. 同request exact rerun返回相同business identities，不产生第二份published事实。
6. 单Program `WAITING_INPUT/FAILED`不阻断其他Program，但batch receipt准确保留逐Program状态。
7. 只有双轨均`COMPLETE`才形成Phase 1E dual-track input。
8. 使用Phase0A audit CLI的exact-target connection resolver对DEV做只读audit并写仓库外receipt。
9. 移除Phase0A CLI的localhost/port文本猜测，改为database identity与explicit target contract；该修改
   只影响 standalone audit CLI，不改变任何运行时连接逻辑。

## 11. Source And Capacity Formation / Source 与容量输入形成

### 11.1 Source availability truth

不从 production 复制 source availability event，也不从 DSE timestamp猜测事件。Phase 1D observer 必须使用
显式 DEV connection factory正常观察 DEV ingestion事实并append event。历史事实无法证明decision cutoff前可用时，
scope保持 `SOURCE_PENDING/PARTIAL`。

### 11.2 Source-ready positive path

完整 G5 正向路径需要至少一个 Program/date 的 required source events满足
`formal_available_at <= decision_cutoff`。实现必须支持：

1. observer先记录真实 DEV ingestion完成事实；
2. 后续由现有 Selection Center正常产生一个完成历史日的 DSE；
3. DSE、artifact和source event共享 exact PIT identity；
4. Phase 1E source replay得到 complete receipt；
5. G5 inventory产生 eligible L3 source和可执行 L4 target。

只有顶层standalone编排CLI调用现有Selection producer；Advisory onboarding service模块、Phase1E和G5均不import
Selection runtime。若新DSE v2的source availability仍不能证明在decision cutoff前可用，则只能形成
`IDENTITY_COMPLETE_SOURCE_PENDING`，不得伪装为source-ready。编排是显式一次性命令，不是scheduler。

### 11.3 Source requirement registry

registry builder只从 typed DSE input context、parent/component manifest、frozen query registry和已有source policy
派生。每个native multi component必须有独立template和真实lookback/window。禁止generic template、默认window、
跨腿取交集或按package名称推断。

### 11.4 Capacity request and receipt

`CapacityPlanningRequest` 的Program count、style、candidate depth、horizons和projection workload从exact request、
Program policy和DSE contract派生；memory/store/Parquet测量由Phase 1D capacity probe产生。不得从测试fixture或代码
常量填充业务值。CLI必须显式使用DEV connection和仓库外输出路径。

## 12. Phase 1E Compilation / Phase 1E 编译

1. `Phase1ERevalidationBatchRequest` 中每个Program/date固定exact receipt、package、manifest、alpha mode和style。
2. policy、source registry、query registry、calendar、label、partition、store和capacity refs全部指向immutable
   artifact，不接受路径alias或latest。
3. `AISTOCK_ADVISORY_PHASE1E_ARTIFACT_ROOT` 不存在时，由CLI显式 `--artifact-root` 指定仓库外受约束root；
   不写repo，也不猜测默认目录。
4. compile-batch只打开DEV read-only snapshot；输出plan/batch receipt后逐个verify-plan/full readback。
5. identity-complete/source-pending必须输出准确operation disposition和missing slots；不能broad exception返回空计划。
6. source-ready双轨计划必须分别保留single/native multi parent/component证据，多个Program不合并。

## 13. CLI Contract / CLI 契约

建议单一入口：

```text
scripts/advisory_real_dev_onboarding.py inventory
scripts/advisory_real_dev_onboarding.py export-bundle
scripts/advisory_real_dev_onboarding.py verify-bundle
scripts/advisory_real_dev_onboarding.py plan-import
scripts/advisory_real_dev_onboarding.py import-dev
scripts/advisory_real_dev_onboarding.py verify-import
scripts/advisory_real_dev_onboarding.py run-historical
scripts/advisory_real_dev_onboarding.py build-phase1e-inputs
scripts/advisory_real_dev_onboarding.py verify-evidence
```

`verify-import`必须同时接收生成receipt的exact `--plan`，并核对request、bundle、plan、source/target database
identity、relation counts、全部post-readback hashes和dependency closure；不能只验证部分receipt字段。

每个有副作用的command语义由命令名和exact request决定，不增加 `--confirm`、审批token、role、backup或
manual bypass。`inventory/export/verify/plan` 永远零DB写入；`import-dev`只写固定import allowlist；
`run-historical`只通过现有runner写Advisory historical relations；`build-phase1e-inputs`只写external CAS。

稳定退出码：

```text
0 requested operation complete or exact idempotent readback
2 invalid request/contract/conflict before DML
3 authoritative input pending
4 DEV import/runner verification failed
5 commit outcome partial or state unknown
70 unexpected internal error
```

## 13A. Implementation Scope / 未来代码允许范围

允许新增：

```text
backend/services/advisory_dev_input_onboarding/contracts.py
backend/services/advisory_dev_input_onboarding/store.py
backend/services/advisory_dev_input_onboarding/production_projection.py
backend/services/advisory_dev_input_onboarding/dev_importer.py
backend/services/advisory_dev_input_onboarding/phase1e_inputs.py
scripts/advisory_real_dev_onboarding.py
backend/tests/advisory_dev_input_onboarding/
```

允许最小修改：

```text
scripts/advisory_phase0a_audit.py
scripts/advisory_phase1_source_observer.py
scripts/advisory_phase1e_readiness_plan.py
backend/services/advisory_phase1/readiness_plan_store.py
backend/services/advisory_phase1/source_observer_postgres.py
```

上述修改只允许增加exact connection/root injection和typed output wiring，不改变既有业务判定。standalone脚本可组合
现有`AdvisoryProgramService`、`SelectionCenterService`、`StrategyPackageSelectionService`和historical runner的公开
接口，但不得修改它们的源码语义。以下路径冻结：

```text
backend/main.py
backend/routers/
backend/services/selection_center/
backend/services/strategy_package/
backend/services/simulation_runtime/
backend/services/paper_trading*/
backend/services/quantevolver/
backend/services/rdagent*/
backend/qlib_exporter/
frontend/
rl_execution/
```

若实现证明现有constructor/protocol无法完成exact DEV injection，必须停止并先修订本文及父设计；不得静默扩大
允许范围、改用global pool、直接SQL重写Selection结果或添加fallback。

## 14. Failure And Logging Semantics / 失败与日志

稳定reason codes至少包括：

```text
ADVISORY_DEV_ONBOARDING_INPUT_PENDING
ADVISORY_DEV_ONBOARDING_DUAL_TRACK_MISSING
ADVISORY_DEV_ONBOARDING_PROGRAM_BINDING_INVALID
ADVISORY_DEV_ONBOARDING_DSE_INVALID
ADVISORY_DEV_ONBOARDING_NATIVE_MULTI_CLOSURE_INVALID
ADVISORY_DEV_ONBOARDING_SOURCE_TARGET_IDENTITY_CONFLICT
ADVISORY_DEV_ONBOARDING_SCHEMA_INCOMPATIBLE
ADVISORY_DEV_ONBOARDING_BUNDLE_CONFLICT
ADVISORY_DEV_ONBOARDING_TARGET_ROW_CONFLICT
ADVISORY_DEV_ONBOARDING_WRITE_SCOPE_VIOLATION
ADVISORY_DEV_ONBOARDING_IMPORT_FAILED
ADVISORY_DEV_ONBOARDING_COMMIT_STATE_UNKNOWN
ADVISORY_DEV_ONBOARDING_REFERENCED_READBACK_FAILED
ADVISORY_DEV_ONBOARDING_SOURCE_EVIDENCE_PENDING
ADVISORY_DEV_ONBOARDING_UNEXPECTED_ERROR
```

expected business gap记录一次摘要；unexpected error保留后台traceback。Pydantic/contract校验错误只输出字段路径、
错误数量和稳定reason，不得输出`input_value`或原始payload。日志只包含command、request/bundle/plan/
receipt hash、target label、relation counts和reason/context，不输出密码、DSN、完整manifest、模型payload、候选全量
或逐行无价值日志。禁止catch-all转success、空列表或 `ALREADY_PRESENT`。

## 15. Concurrency And Idempotency / 并发与幂等

- bundle CAS使用atomic no-replace和full readback；同内容并发导出得到同hash。
- import transaction按bundle hash取得transaction-scoped advisory lock，只序列化相同bundle，不形成全局锁。
- 不同package bundle可并发，但数据库unique/FK与exact compare仍是最终authority。
- exact bundle重复执行必须零DML并返回同post-closure hash、新invocation receipt。
- 相同identity不同payload必须冲突；不采用last-write-wins、upsert update或静默跳过。
- historical runner继续使用既有business key和逐Program独立事务，不改变其重试/恢复语义。

## 16. Security And Data Minimization / 安全与数据最小化

- env文件只读取所需key；receipt中只存database identity hash，不存credential。
- export request只能指定数据库中已存在的source package和显式DEV Program spec，不接受任意SQL、schema、table或path。
- relation和column list由代码冻结，标识符不来自用户输入。
- bundle root、blob root、Phase 1E root和evidence root都必须仓库外、canonical resolve、containment检查、拒绝
  symlink/reparse/latest pointer。
- 不复制账户、资金、订单、持仓、Paper、simulation、QE实验或其他无关数据。

## 17. Implementation Plan / 分批实施方案

### O1：Contracts, inventory and external CAS

实现F-879至F-887：typed request/inventory/bundle/ref/store、source/target exact connection、只读inventory和离线
verification。零数据库写入。

### O2：Production package exporter and DEV package importer

实现F-888至F-899：固定package projection、asset/component closure、import plan、单事务insert-or-compare、
commit uncertainty和fresh readback。只写DEV StrategyPackage allowlist，不导入Selection/DSE/Program/binding。

### O3：DEV Program, prospective DSE v2, historical runner and audit

实现F-900至F-905：DEV standalone编排、正常Program/binding service、正常prospective Selection producer、
historical runner、audit target resolver、双轨exact retry和formal receipts。不改运行时API或Selection业务逻辑。

### O4：Phase 1D / Phase 1E input builder

实现F-906至F-911：target-aware observer/capacity、source registry builder、Phase1E input bundle和compile/verify。
source不成熟时准确pending。

### O5：G5 continuation and design closeout

实现F-912至F-916：重跑G5 inventory；source-ready后按既有设计执行L3/L4；同步父文档真实状态。O5不修改G5
业务契约。

每批必须完整实现本批设计条目，不得以fixture、mock、手写JSON、部分relation复制或静默source fallback冒充完成。

## 18. Verification Plan / 验证方案

### 18.1 L0 static

- forbidden import AST scan；运行时反向import scan；
- SQL AST/registry证明production只含SELECT，DEV仅固定INSERT/SELECT；
- 扫描禁止DDL、UPDATE、DELETE、TRUNCATE、COPY FROM arbitrary relation、session replication bypass；
- 扫描角色、审批、授权、backup、force、skip、hardcoded credential/host/port；
- `python scripts/aistock_feature_workflow.py validate --design <path> --tier F2`。

### 18.2 L1 pure

- request/bundle/row-set/plan/receipt canonical hash、排序、篡改和redaction；
- single/native-multi闭包、component窗口差异、union/manual multi拒绝；
- relation/column allowlist、dependency graph完整性、unknown relation拒绝；
- insert/exact/conflict分类、same key different payload拒绝；
- identity-complete/source-pending/source-ready严格分型；
- exit/reason/log contract，无silent fallback。

### 18.3 L2 disposable PostgreSQL 16

使用与O2关系有关的完整production StrategyPackage migration chain构造source/target disposable database，禁止在
测试中手写`strategy_pkg.package/package_asset`替代生产结构：

1. source只读强制和write injection拒绝；
2. single/native multi package/asset/component exact export、bundle full readback，DSE v1和legacy binding拒绝进入bundle；
3. target空库首次package import、exact rerun零DML、部分预存exact rows；
4. 任一relation conflict整个transaction零写入；
5. FK/unique/check/trigger正常启用，不使用replica role；
6. 每个写节点异常rollback、commit response loss三态；
7. concurrent same bundle与different bundle；
8. 正常DEV Program/binding创建、prospective DSE v2生产、historical runner双轨、一个Program失败隔离、exact retry；
9. Phase1E input source-pending与source-ready两条完整路径；
10. container/database销毁，不连接真实DEV/production。

### 18.4 L3 real DEV rollback validation

先在真实DEV对exact bundle执行只读plan；使用owner transaction运行完整importer后物理rollback，用fresh connection
逐exact key证明零残留。不得用DELETE/TRUNCATE清理。随后才允许persistent import。

### 18.5 L4 real DEV persistent validation

1. production只读package export和offline verify；
2. DEV persistent package import与fresh full readback；
3. exact rerun零DML；
4. 正常DEV service产生Program/dated binding、DSE v2和single/native-multi formal receipt；
5. audit/handoff/Phase1E计划完整且Program独立；
6. source-pending准确保留，source-ready后G5 L3/L4按父设计执行；
7. Selection/Paper/模拟盘/QE/QMT关系row count/hash在任务前后不因本任务allowlist之外发生变化。

## 19. Risks And Failure Modes / 风险与失败模式

| 风险 | 后果 | 设计处置 |
|---|---|---|
| production DSE v1被误当v2 | 历史研究身份伪造 | inventory标记不合格；bundle schema禁止DSE |
| legacy null binding被复制 | historical as-of不可证明 | DEV正常创建future-effective binding |
| package闭包遗漏blob/component | Selection产生确定性失败 | export closure和offline full readback整体失败 |
| importer范围扩大 | 污染Selection/Paper/模拟盘 | 固定relation/column/SQL allowlist和AST检查 |
| insert冲突被忽略 | DEV身份混杂 | full row compare；任一冲突整个transaction零DML |
| commit响应丢失后重写 | 重复或部分事实 | fresh readback三态；禁止hidden retry |
| prospective Selection改变排名 | 业务语义漂移 | 复用现有producer；候选parity hash作为验收oracle |
| source时间被回填猜测 | PIT泄漏 | observer只追加真实observed事实；不成熟保持pending |
| future-effective日期被当审批 | 人为阻塞误解 | 明确为确定性时间事实，到期后程序自动可执行 |
| standalone工具下沉runtime | 影响常驻模块 | one-way script composition和反向import denylist |

## 20. Rollout And Rollback / 发布与回滚

### 19.1 Rollout

1. 按O1-O4独立PR合入代码；代码合入不自动连接数据库。
2. O1/O2 L0-L2通过后执行production read-only inventory/export。
3. DEV先做rollback-only importer验证，再执行persistent exact import。
4. 正常运行DEV historical runner、audit、Phase1D和Phase1E CLI。
5. G5 source pending保持pending；source-ready后执行L3再执行L4。
6. O5只同步已发生事实，不把设计或低层测试写成DEV完成。

### 19.2 Rollback

- code rollback：停止调用新CLI，删除代码版本不删除已导入真实DEV evidence。
- bundle：CAS不覆盖；错误bundle追加新identity，不修改旧文件。
- imported rows：不DELETE/UPDATE；冲突在import前阻断，已完整提交的真实行保留。
- historical/source rows：遵循既有append-only/retry/recovery，不人工清理。
- production：始终只读，无数据库rollback动作。

## 21. Production Gates / 生产影响状态（无新增运行门禁）

本节仅满足交付状态报告，不引入approval、role、backup或运行时阻断逻辑：

```text
production_ddl_gate = noop
production_dml_gate = prohibited
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
production_runtime_activation = noop
production_read_access = explicit read-only export only
dev_ddl_gate = noop
dev_dml_state = future O2/O3 execution only after code merge and exact request
```

代码合入、production只读export、DEV package import、DEV Program/DSE v2、Phase1E和G5状态必须分别报告。

## 22. Positive Reachability / 正向可达性

```text
existing production single + native multi package/asset closure
  -> exact package export bundle
  -> empty/non-conflicting DEV target
  -> insert-or-compare commit
  -> normal DEV Program + future-effective dated binding
  -> normal prospective Selection DSE v2 after binding becomes effective
  -> existing historical runner COMPLETE x 2
  -> audit/handoff
  -> Phase1E identity-complete plans
```

source-ready完整路径：

```text
DEV source observer records real ingestion before cutoff
  -> normal upstream Selection produces exact DSE
  -> source replay COMPLETE
  -> Phase1E executable plans
  -> G5 inventory L3_READY/L4_DUAL_TRACK_READY
  -> L3 COMPLETE_ZERO_RESIDUE
  -> L4 COMPLETE_DUAL_TRACK
```

所有条件均由合法数据和正常程序自动通过；不存在角色、审批、授权、备份、人工改库或不可达门禁。当前无合法
dated binding和DSE v2时，必须等待新binding生效后的第一个已完成且source可证明交易日；这是PIT数据事实，不是批准
等待，也不得通过backdate、v1升级或猜测available-at规避。

## 23. Design Acceptance Index / 设计验收索引

- F-879：显式双轨Program/date request和稳定hash，无latest/name推断。
- F-880：production connection server-enforced只读且write query count为0。
- F-881：DEV exact target identity来自env/database identity，不硬编码host/port/dbname。
- F-882：source/target identity不同且current DEV catalog receipt一致。
- F-883：仓库外content-addressed bundle/ref/store原子no-replace/full readback。
- F-884：typed PostgreSQL serializer和relation row-set hash完整；跨库semantic/provenance列分离，
  package_asset代理键不污染target identity。
- F-885：single/native-multi parent/component闭包完整，腿级window差异合法。
- F-886：package/asset blob/native-component dependency graph精确，不复制model-state或execution-policy运行事实。
- F-887：无凭据、绝对路径、模型明文或无关数据泄漏；source/target package asset roots显式、不同且分别
  read-only/no-replace。
- F-888：只导出固定relation/column和exact keys，无任意SQL/全表复制。
- F-889：import plan逐行INSERT/EXACT/CONFLICT分类且冲突零DML。
- F-890：DEV只允许固定INSERT/SELECT，无DDL/UPDATE/DELETE/TRUNCATE/COPY bypass。
- F-891：一个owner transaction和正常constraint/trigger，任一错误全rollback。
- F-892：每行insert-or-compare full payload，不用PK/row-count假幂等。
- F-893：commit response loss fresh readback三态准确，无hidden retry。
- F-894：exact rerun零DML、same closure、新invocation receipt。
- F-895：并发same/different bundle正确收敛，无global lock。
- F-896：已导入真实DEV行不在验收后清理或改写。
- F-897：package importer与Advisory模块不得调用共享runtime；仅standalone编排可调用现有Selection producer且不得
  修改其业务逻辑。
- F-898：不得使用全库refresh、batch_a import、replica role或sequence reset。
- F-899：多Program独立，双轨不融合，不限制未来多个package。
- F-900：standalone编排只使用exact DEV connection injection，不调用backend.main/global pool。
- F-901：DEV Program/binding由正常service创建且future-effective，不复制/回填legacy null interval。
- F-902：正常prospective Selection原生生成single/native-multi DSE v2，v1不可升级或导入。
- F-903：runner只接受manual historical/research-only/execution-prohibited request，逐Program失败隔离且receipt准确。
- F-904：Phase0A audit CLI移除localhost猜测，使用explicit target identity。
- F-905：audit/handoff只读且artifact仓库外content-addressed。
- F-906：source event只由DEV observer从真实ingestion事实追加，不复制/猜测/backdate。
- F-907：source-pending与source-ready分型准确，不把PARTIAL冒充ready。
- F-908：source registry从typed DSE/manifest/query policy派生，无generic/default window。
- F-909：capacity request业务值来自exact Program/policy，测量来自capacity probe。
- F-910：Phase1E全部input refs immutable、explicit、hash closed。
- F-911：Phase1E计划single/native-multi/component和多Program独立。
- F-912：G5只消费source-ready计划，pending保持零DML。
- F-913：G5 L3/L4继续使用既有契约，不在onboarding重定义门禁。
- F-914：无角色、RBAC、审批、授权、备份、force、skip或人工数据库修改。
- F-915：无API/UI/scheduler/startup/runtime activation/production DDL/DML。
- F-916：状态分别报告code、bundle、DEV import、historical receipt、Phase1E、G5 L3/L4和production。

## 24. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-879 | §7.1、§8 | `contracts.py`; request canonical/hash/date/package tests；PR #2231 | verified_merged | none |
| F-880 | §5.1、§8 | `production_projection.py`; server read-only/write-query spy；PR #2231 | verified_merged | none |
| F-881 | §5.2、§10 | exact env/database identity tests；PR #2231 | verified_merged | none |
| F-882 | §5.2、§9.1 | source-target identity/catalog receipt tests；PR #2231 | verified_merged | none |
| F-883 | §7.4、§8 | `store.py`; CAS no-replace/collision/tamper/readback tests；PR #2231 | verified_merged | none |
| F-884 | §7.3 | typed PostgreSQL value serialization golden tests；PR #2231 | verified_merged | none |
| F-885 | §7.4、§8 | single/native-multi/component-window closure tests；PR #2231 | verified_merged | none |
| F-886 | §5.4、§8 | package/blob/component graph and mutable-row exclusion tests；PR #2231 | verified_merged | none |
| F-887 | §7.4、§16 | credential/path/payload redaction scan；PR #2231 | verified_merged | none |
| F-888 | §5.4、§8 | canonical manifest/lifecycle/runtime projection tests；migration-chain PostgreSQL actual readonly exporter | verified_l0_l2 | none |
| F-889 | §7.5、§9.1 | `dev_importer.py`; INSERT/EXACT/CONFLICT classification tests | verified_l0_l2 | none |
| F-890 | §9、§18.1 | fixed SQL registry/AST/forbidden statement scan | verified_l0_l2 | none |
| F-891 | §9.2、§18.3 | production StrategyPackage migration-chain PostgreSQL 16 owner transaction/trigger rollback | verified_l0_l2 | none |
| F-892 | §9.2 | disposable PostgreSQL full-row insert-or-compare/readback | verified_l0_l2 | none |
| F-893 | §9.3 | all-key committed/not-observed/state-unknown tests including preexisting-row conflict | verified_l0_l2 | none |
| F-894 | §9.4、§15 | exact rerun zero-DML/new receipt；forged request/source/plan/count rejection | verified_l0_l2 | none |
| F-895 | §15 | disposable PostgreSQL same/different bundle concurrency tests | verified_l0_l2 | none |
| F-896 | §9.4、§20.2 | no-cleanup SQL/source scan | verified_l0_l2 | none |
| F-897 | §5.3、§10 | one-way onboarding import graph/runtime import AST test | verified_l0_l2 | none |
| F-898 | §4、§9.2 | batch-a/replica-role/sequence-reset denylist | verified_l0_l2 | none |
| F-899 | §3、§6 | multi-package request and independent package identity tests | verified_l0_l2 | none |
| F-900 | §10、§13 | exact DEV injected repositories/no-global-pool tests | design_ready | none |
| F-901 | §10 | normal service future-effective binding tests | design_ready | none |
| F-902 | §10 | prospective DSE v2 golden and v1 rejection tests | design_ready | none |
| F-903 | §10 | historical request/Program isolation/retry tests | design_ready | none |
| F-904 | §10 | audit target identity and non-local DEV host tests | design_ready | none |
| F-905 | §10、§16 | audit/handoff read-only CAS tests | design_ready | none |
| F-906 | §11.1 | observer real-ingestion/no-copy/no-backdate tests | design_ready | none |
| F-907 | §7.7、§11.2 | pending/ready classification and no-fake-ready tests | design_ready | none |
| F-908 | §11.3 | typed registry/per-component-window tests | design_ready | none |
| F-909 | §11.4 | exact workload and capacity measurement tests | design_ready | none |
| F-910 | §7.7、§12 | immutable input ref/hash closure tests | design_ready | none |
| F-911 | §12 | single/native-multi/multi-Program plan parity | design_ready | none |
| F-912 | §11.2、§12 | G5 pending zero-DML and source-ready inventory | design_ready | none |
| F-913 | §17 O5、§22 | existing G5 contract parity and L3/L4 evidence | design_ready | none |
| F-914 | §4、§21 | role/approval/backup/force/skip scan | design_ready | none |
| F-915 | §4、§21 | API/UI/scheduler/startup/production-impact scan | design_ready | none |
| F-916 | §1、§21、§26 | separated state reporting assertions | design_ready | none |

本矩阵的 `gap_or_exception` 只记录设计偏差或验收例外，不记录尚未执行的环境层验证。代码、数据库和运行证据的
完成状态以本文开头的独立状态字段为准；`verified_l0_l2` 不代表真实 production/DEV 执行已完成。

## 25. DESIGN-COMPLIANCE-001 Review

- `no_simplified_delivery`：覆盖export、DEV import、historical runner、source/capacity、Phase1E和G5完整链路；
  identity bootstrap不能冒充source-ready完成。
- `no_silent_error`：pending、conflict、rollback、commit uncertainty、partial和unexpected均有稳定reason/exit/log。
- `no_business_semantic_drift`：DSE v2由正常Selection生产，不转换v1、不修改排名，不融合Program，不改策略包、
  荐股、模拟盘或交易语义。
- `no_unrequested_gate_or_approval`：无角色、审批、授权、备份、force、skip；显式request/command是数据与动作输入，
  不是人工审批记录。
- `positive_path_satisfiable`：现有production双轨package可导入DEV；正常future-effective binding、真实source event和
  prospective DSE v2可到Phase1E及G5 L3/L4。
- `exact_database_truth`：所有连接来自显式env并核验database identity；production永远只读，DEV写关系固定。
- `research_isolation`：全部输出research-only、execution-prohibited，不产生交易输入。
- `state_reporting_truth`：design、implementation、DEV DML、production、Phase1E和G5状态分开报告。

## 26. Exit Criteria And Next Stage / 退出条件与下一阶段

本文可标记 `design_ready` 的条件：

1. F-879至F-916全部有前后一致的设计与验证映射。
2. 父级Phase 0A.2、Phase 1E、G5的输入/状态/退出条件引用本文且不互相矛盾。
3. 没有全库refresh、fixture、手写receipt、共享runtime import或source猜测路径。
4. 所有失败均显式，所有保留技术条件有正向可达路径。
5. 无额外角色、审批、授权、备份或人工数据库修改设计。
6. F2 validator、文档引用、`git diff --check`通过。

设计合入后，代码阶段固定从O1开始，按O1至O5顺序实施。不得直接跳到persistent DEV import，也不得在
O1/O2完成后宣称Phase 1E或G5完成。
\n
