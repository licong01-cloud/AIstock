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
design_status = design_ready
o4_design_revision_status = revised_20260718_admitted_input_projection_no_secondary_package_validation
o1_implementation_status = merged_pr_2231
o2_implementation_status = merged_pr_2261_runtime_validated
o3_implementation_status = merged_pr_2351_l0_l2_and_design_compliance_verified
o3_code_merge = merged_commit_d4af94833a5b21cdb5490822a15ab3e7180fdee0
o3_focused_regression = 50_related_tests_passed
o3_module_regression = validation_center_backend_and_paper_v2_backend_passed
o3_postgresql16_l2 = real_single_and_native_multi_producer_passed_zero_residual_test_database
o3_real_dev_readonly_plan = already_present_98_exact_match_zero_dml
o3_real_dev_persistent_execution = not_executed
o3_statement_coverage = 84_percent
o3_branch_coverage = 74_percent
o3_feature_workflow_validation = f2_pass_38_of_38_zero_warning
o4_design_feature_workflow_validation = f2_pass_39_of_39_zero_warning
o4_implementation_status = development_started_not_merged
o5_implementation_status = not_started
dev_database = real_dual_track_package_closure_imported
production_database = read_only_source_only
production_ddl = none_for_this_design
production_dml = prohibited
production_readonly_export_execution = completed_bundle_75806f83b2a5
dev_ddl = none
dev_import_initial_plan_execution = executable_plan_8da94d2f87b9
dev_import_rollback_validation = completed_zero_residue_receipt_21f01f6aeab4
dev_import_persistent_execution = committed_receipt_62dd9e07ca0c
dev_import_fresh_verification = passed_receipt_62dd9e07ca0c
dev_import_idempotent_rerun = already_present_zero_dml_receipt_3e9977fc8355
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

O1/O2 对 portable bundle 的 row/blob SHA readback只证明跨库复制内容与已经准入的source package一致，属于一次性传输
完整性校验，不是重新执行package admission、资产健康检查或模型可用性判定。DEV package closure导入并完成fresh readback后，
O3/O4以及今后每日荐股不得再次执行这组asset closure/blob validation；它们只消费dated binding、frozen manifest identity和
§7.9.1 的纯input projection。

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
  -> project exact source inputs from admitted manifest metadata only; no package revalidation or asset loading
  -> before completed decision cutoff: build pre-observation scope from historical request/package/binding/input projection
  -> after real DEV ingestion and before cutoff: Phase 1D observer appends physical source facts
  -> normal DEV Selection producer creates DSE v2 with ADVISORY_RUN context
  -> existing historical research runner on exact DEV connection
  -> Phase 0A read-only audit + Phase 0A.1 handoff
  -> reconcile actual DSE receipts with pre-observation scope
  -> Phase 1D capacity with exact DEV connection
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
schema_version = advisory_real_dev_portable_bundle_v2
request
source_database_identity_hash
export_snapshot_identity
package_refs[]                       # source/portable manifest hashes + projection evidence
native_multi_component_refs[]
relation_row_sets[]
artifact_blob_refs[]
dependency_edges[]
dependency_closure_hash
bundle_content_hash
```

source package保持只读且不修改。若source manifest含有非运行的`backtest_context`、执行模型工作站路径或历史预测
URI，exporter按冻结projection policy精确移除这些字段，并把manifest内`source.source_type`及target package行
`source_type`投影为DEV已支持的`candidate_strategy_package`。package ID、alpha mode、alpha components和runtime asset
refs必须前后完全一致；bundle同时冻结source manifest hash、portable manifest hash、被移除manifest内容hash、被排除
source行字段hash、component hash和runtime asset closure hash。任一parity不成立则整体失败，不允许放宽路径扫描。

`prediction_ref_uri/prediction_ref_sha256/model_artifact_uri/model_artifact_sha256`是历史输出或旧locator，不进入DEV
relation semantic row；模型运行身份只来自portable manifest、`strategy_pkg.package_asset`和CAS blob闭包。bundle不保存
密码、DSN、绝对本机路径、模型明文、DSE、Selection候选或非package闭包行。它不搬运或转换production DSE v1；
DEV DSE v2必须由正常producer新生成。

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
commit_outcome = COMMITTED | ROLLED_BACK | ALREADY_PRESENT | STATE_UNKNOWN
started_at/finished_at
reason_codes[]
receipt_hash
```

`ROLLED_BACK`必须真实执行全部计划INSERT、事务内完整readback、物理rollback并用新连接证明所有exact key回到执行前
计划且数据库零残留；它要求`physical_commit_count=0`，不能冒充persistent import。`COMMITTED`必须完整readback；
commit response丢失时用新连接按exact keys判定committed/not-committed/state-unknown，不静默重试写入。

### 7.7 `RealDevHistoricalRunRequest` 与 `RealDevHistoricalRunReceipt`

`RealDevHistoricalRunRequest`单独冻结O3才需要、且不能加入既有O1/O2 request而破坏其已发布hash的字段：

```text
onboarding_request_ref/request_hash
target_database_identity_hash
target_package_asset_root_hash
program_specs[]                      # exact program_id/name/package/alpha mode/policy/runtime config
binding_effective_from_trade_date
decision_trade_date
policy_registry_id/version/hash
code_release_id/hash
research_scope = HISTORICAL_RESEARCH_ONLY
execution_prohibited = true
historical_request_hash
```

`RealDevHistoricalRunReceipt`只是standalone编排回执；历史研究权威事实仍由既有runner写入
`app.advisory_research_*`并产生formal batch/program receipt。编排回执固定记录request/DEV identity/asset root hash、
逐Program的Program/binding、DSE v2和runner identity/status，以及formal batch receipt identity。相同request重跑时，
已有exact Program/binding和唯一DSE v2必须readback复用，不新增第二份published事实；不同payload占用相同identity时失败。

### 7.8 `Phase1ERealInputBuildRequest`

O4 不接受一组松散路径或由目录扫描推断 `latest`。构建入口必须先冻结一个 hash-closed request：

```text
schema_version = advisory_phase1e_real_input_build_request_v1
historical_run_request_ref/hash
historical_run_receipt_ref/hash
target_database_identity_hash
target_package_asset_root_hash
program_dates[] {
  program_id
  decision_trade_date
  package_id
  manifest_sha256
  alpha_mode
  style_family
  historical_status = COMPLETE | WAITING_INPUT | FAILED
  historical_program_run_id nullable
  historical_reason_codes[]
  historical_batch_receipt_ref/hash
}
phase0a_policy_registry_ref/hash
source_mapping_registry_ref/hash
source_query_registry_ref/hash
calendar_registry_ref/hash
label_policy_bundle_ref/hash
partition_policy_ref/hash
store_backend_policy_ref/hash
capacity_policy_ref/hash
phase1e_artifact_store_policy_ref/hash
code_release_id/hash
research_scope = HISTORICAL_RESEARCH_ONLY
execution_prohibited = true
build_request_hash
```

`program_dates[]` 从 O3 receipt 的逐 Program 结果逐项生成；不能只依据顶层 batch status，也不能按 package 名称、
当前 binding、当前 enabled 状态或目录内容补全。`COMPLETE` 条目必须具有完整historical Program identity；非 COMPLETE
条目保留原始status/reasons并进入最终bundle的`IDENTITY_PENDING/BLOCKED` Program unit，但不得生成伪造的Phase1E request。
`Phase1ERealInputBuildRequest` 本身必须以 `real_input_build_request` kind 写入 O4 CAS；其 ref/hash 是后续
`Phase1ERealInputBundle` 的直接依赖，不得用 `phase1e_batch_request`、手写路径或仅有 hash 的占位对象替代。

### 7.9 `AdvisorySourceMappingRegistry`

`source policy` 在本文中固定为一个独立、版本化、content-addressed 的 typed registry，不再使用未定义的文字概念：

```text
schema_version = advisory_source_mapping_registry_v1
registry_id/version
entries[] {
  dse_source_role
  dse_dataset_id
  dse_query_template_id/version
  physical_requirements[] {
    source_role
    dataset_name
    observer_query_template_id/version/hash
    audit_evidence_policy_id/version/hash
    partition_mapper_id/version/hash
    partition_granularity = DAILY | AS_OF_SNAPSHOT
    bind_parameter_schema
    canonical_sort_columns[]
    capacity_date_column nullable
    business_window_derivation
    availability_requirement
    cutoff_predicate_id/version/hash
  }
}
registry_hash
```

`source_query_registry_ref/hash`指向由代码中固定`SourceQueryTemplateV1/V2` canonical dump发布的immutable artifact；
artifact必须包含template id/version/hash、schema/table identity、typed bind schema、canonical columns/sort和SELECT text hash。
CLI不能从JSON接收或修改SQL正文，mapping registry只能引用已编译template。

当前真实推理 receipt 至少包含以下逻辑输入，O4 registry 必须逐项提供 concrete mapping，不允许 generic/default
template：

| DSE logical input | 必须形成的物理证据闭包 |
|---|---|
| `pit_universe / market.stock_universe_pit` | `market.stock_universe_pit_spans` 的 exact universe key 与 decision-date interval；同时绑定生成它的`stock_basic/stock_st_events`成功audit closure和`market.stock_universe_pit_state`已完成build identity，不调用共享 universe runtime |
| `market_history / market.kline_daily_raw` | 每个 leg 的真实 `window_start_date..effective_trade_date` 内 `market.kline_daily_raw`；若 DSE query 声明前复权，同时包含 `market.adj_factor` |
| `fundamental_moneyflow / timescaledb.fundamental_moneyflow` | 由 frozen query version 明确展开为 `market.daily_basic`、`market.moneyflow_ts`、`market.bak_basic`、`market.stock_basic`、`market.cyq_perf`、`market.sector_data` 中该 query 实际读取的成员，不允许把虚拟 dataset 当成单表 |
| `trading_calendar / market.trading_calendar` | exact window 与 target-date resolution 使用的 `market.trading_calendar` 行 |
| `reference_price / market.kline_daily_raw` | reference trade date 的 exact `market.kline_daily_raw` partition |

某个 package/leg 的 DSE receipt 使用 registry 未覆盖的 role、dataset、query id/version 时，该 Program 必须返回
`ADVISORY_DEV_ONBOARDING_SOURCE_MAPPING_MISSING`；不得省略该输入、复用其它 role、按名称猜测或退化为父包 generic
requirement。registry 只描述 Advisory source evidence，不调用或修改 Selection、StrategyPackage、模拟盘或 QE。

现有observer v1仅支持`trade_date_v1`和单日`market`表。O4允许additive observer-template v2表达daily partition与
as-of snapshot，但每个template仍必须是固定SELECT、固定typed bind slots和固定canonical sort，不能接收任意SQL。
multi-day window必须展开为有序daily requirements，不使用一个聚合window hash掩盖缺失日期。derived dataset（当前为
`stock_universe_pit_spans`）必须由registry声明完整upstream audit + build-state evidence policy；缺少任一成员时pending，
state/audit/hash冲突时blocked。capacity probe遇到无`trade_date`列的as-of source时使用registry的nullable
`capacity_date_column`和bounded sample policy，不得硬编码查询`trade_date`或把该source静默跳过。

### 7.9.1 `StrategyPackageAdvisoryInputProjectionV1`

O4 在 prospective Selection 前需要 exact single/native-multi leg factor order 与 required window，但该需求不得演变为
策略包二次准入、资产复验或新的运行门禁。实现固定增加一个纯只读、无 I/O 的
`StrategyPackageAdvisoryInputProjectionV1`：

```text
schema_version = strategy_package_advisory_input_projection_v1
projection_source = ADMITTED_MANIFEST_ONLY
package_id/manifest_sha256/alpha_mode
selection_query_contract_id/version/hash
legs[] {
  alpha_component_id
  factor_order[]
  factor_order_hash
  required_window
  window_resolution = trading_day
  alpha158_alias_set_hash
  dynamic_factor_ref_set_hash
}
projection_hash
```

唯一输入是 dated binding 已解析到的、数据库中已经入库并完成 StrategyPackage 准入的 typed frozen manifest。投影只读取：

- single Alpha：`manifest.runtime_assets.alpha158.aliases`、`manifest.factor_set[].factor_name/factor_id` 与唯一
  `alpha_component` 的 factor refs；
- native multi Alpha：parent manifest 的 `alpha_components[].lineage.factor_artifact_refs`、parent `factor_set`，以及
  已持久化 multi leg `runtime_assets.alpha158.aliases`；
- frozen provider/query contract 与纯函数 `backend.data_service.preprocessor.get_required_data_window`。

factor order的确定性规则固定如下，不允许排序、取集合、按名称猜测或跨腿合并：

1. single Alpha：按manifest保存顺序连接`runtime_assets.alpha158.aliases`和`factor_set[].factor_name`；
2. native multi：每腿按该leg持久化runtime metadata中的Alpha158 alias顺序，再按component
   `lineage.factor_artifact_refs`声明顺序解析parent `factor_set`中的exact factor；
3. 任一ref缺失、ambiguous或产生duplicate factor时形成当前Program/date的projection错误，不改package状态；
4. 每腿独立调用`get_required_data_window(factor_order)`，禁止用父包最大值、最小值、平均值或其它腿结果代替。

投影内部可以使用本文件定义的strict typed DTO解析上述已持久化字段；该字段级解析只防止静默缺失或歧义，不是
StrategyPackage validator、asset validation或package health判定。

现有 package freeze 已把 Alpha158 alias 列表、factor identity 和 native multi leg runtime metadata写入 frozen manifest，
因此当前已入库的单 Alpha 和原生多 Alpha父包可以直接产生投影，不需要 DDL、资产回读、模型文件解析、workspace
materialization或重新执行包验证。投影实现不得调用：

```text
package validator / package health / runtime_asset_admission_status
asset store get/readback / blob sha recompute / package closure recompute
model loader / frozen runtime self-check / live inference preflight / live inference
multi_alpha_live execution / Selection producer / HMM / Paper / simulation
```

该投影只是把已准入 manifest 中已经存在的输入结构转换为 Advisory-owned typed DTO，不重新判断 package 是否可用，
不改变 package status、enabled 状态、binding、asset 或 admission receipt，也不能阻断 Selection、模拟盘、Paper 或其它
Program。若 frozen manifest 本身缺少形成 exact input projection 所需的已持久化字段，只对当前 Advisory Program/date
返回 `ADVISORY_INPUT_PROJECTION_UNAVAILABLE`；若同一字段存在互相矛盾的已持久化 identity，则返回
`ADVISORY_INPUT_PROJECTION_CONFLICT`。两者都是显式输入数据错误，不是策略包准入结果、审批或人工门禁；其它 Program继续执行。

该纯投影允许新增在 `backend/services/strategy_package/advisory_input_projection.py`，但文件只能 import typed manifest model、
canonical hash utility与`get_required_data_window`，不能 import repository、asset store、validator、health、inference或 Selection。除该文件及其直接
单元测试外，StrategyPackage其它路径继续冻结。Advisory Phase 1/Phase 1E只消费序列化后的 projection DTO，不 import
StrategyPackage runtime。

### 7.10 `AdvisorySourceObservationScopeRequest`

source event必须在decision cutoff前真实形成，因此observer不能等待DSE生成后才决定观察哪些数据。O4在运行
prospective Selection之前，使用O3 historical request中的Program spec、dated binding、
`StrategyPackageAdvisoryInputProjectionV1`、Selection纯 runtime-config normalization结果和source mapping registry构造
预观察request。该过程不得调用 StrategyPackage preflight、validator、asset loader或模型：

```text
schema_version = advisory_source_observation_scope_request_v1
target_database_identity_hash
program_id/decision_trade_date
package_id/manifest_sha256/alpha_mode/style_family
binding_version_id/binding_payload_hash
selection_normalized_config_hash
strategy_package_input_projection_ref/hash
source_mapping_registry_ref/hash
source_query_registry_ref/hash
window_policy_ref/hash
decision_cutoff_ts
expected_logical_inputs[] {
  alpha_component_id nullable
  source_role/dataset_id/query_template_id/version
  expected_window_start_date/effective_trade_date/required_window/window_resolution
  expected_window_lineage_hash
  physical_requirement_templates[]
}
observation_scope_hash
```

每个leg的`required_window`必须由exact factor order调用当前推理共用的canonical纯函数
`backend.data_service.preprocessor.get_required_data_window`计算，并把helper版本/source hash冻结为`window_policy_ref/hash`；
input projection可以调用该纯函数，Advisory service和Phase1E不得import`inference_engine`或StrategyPackage runtime。
query role集合来自frozen provider/query contract，不按模型类型或package名称推断。

该request只使用运行Selection所需且在执行前已经确定的配置，不执行模型、不产生候选，也不读取未来DSE。observer按其
physical templates观察真实ingestion并append event。Selection完成后，registry builder必须把实际DSE
`source_read_receipts`和`artifact_input_context/per_leg_window_lineage`与该request逐字段reconcile：完全一致才允许消费
cutoff前event；任何role、query、window或leg差异均为`SOURCE_MAPPING_CONFLICT/BLOCKED`，不得在DSE生成后补写或回填一个
cutoff前event。这样正向顺序是可达的，同时不复制Selection算法。
`strategy_package_input_projection_ref/hash`必须指向与当前Program/package/manifest完全一致的
`strategy_package_input_projection` artifact；observation request不得只复制projection字段后丢失其可定位ref。

### 7.11 `Phase1ECapacityPlanningRequestV2`

容量运行参数先冻结为不含测量结果的typed policy artifact：

```text
schema_version = advisory_phase1_capacity_policy_v1
policy_id/version
retained_snapshot_count
concurrent_build_count
staging_copy_count
parquet_target_file_bytes
memory_budget_bytes
worker_memory_overheads
orphan_reserve_bytes
manifest_overhead_bytes_per_snapshot
parquet_measurement_snapshot_limit
parquet_measurement_file_limit
policy_hash
```

policy是版本化程序配置，不含`approved_by`、role、人工状态、store free bytes、row width或其它测量值。相同id/version不同
hash为conflict；修改参数发布新version，不覆盖旧artifact。

现有 v1 `program_count_by_style + candidate_depth_by_program` 无法无损表达同 style、不同 candidate depth 的多个
Program。O4 必须使用 additive v2 contract；不得把 Program id 冒充 style，也不得取平均值、最大值或固定常量：

```text
schema_version = advisory_phase1_capacity_request_v2
observer_config_ref/hash
query_registry_ref/hash
capacity_policy_ref/hash
as_of_ts
history_start_trade_date/history_end_trade_date
program_workloads[] {
  program_id
  decision_trade_date
  style_family
  package_id/manifest_sha256/alpha_mode
  candidate_depth
  input_universe_count
  horizons[]
  projection_count
  stage_projection_factor
  source_requirement_set_hash
}
universe_size_p50/p95/max
retained_snapshot_count/concurrent_build_count/staging_copy_count
parquet_target_file_bytes/memory_budget_bytes/worker_memory_overheads
store_root_ref/hash
orphan_reserve_bytes
manifest_overhead_bytes_per_snapshot
parquet_measurement_snapshot_limit/parquet_measurement_file_limit
request_hash
```

字段权威来源固定如下：Program/package/style/candidate depth 来自 O3 receipt、dated binding 与 Program policy exact
readback；每个`input_universe_count`来自actual DSE/artifact input context，顶层universe p50/p95/max由本次exact
`program_workloads[]`确定性计算；horizon/projection/stage参数来自冻结label/partition policy；
memory/concurrency/retention/reserve参数来自`capacity_policy_ref`；store root来自不可变store policy。revision multiplier、
store available bytes、Parquet/row-width/change-ratio、source partition分布只能出现在Phase1D read-only measurement/receipt，
不能由request携带或由调用方填写。这样probe不存在“先填测量值才能测量”的循环。v1继续兼容既有调用，但O4 builder和
O4生成的Phase1E batch只接受v2。

对应receipt同样使用additive v2并绑定逐Program workload，不得把v2 request压回v1 style summary：

```text
schema_version = advisory_phase1_capacity_receipt_v2
request_ref/hash
program_workload_set_hash
observer_config_hash/query_registry_hash/capacity_policy_hash
target_database_identity_hash
database_observed_at/database_version
source_coverage_summary/relation_size_summary/row_distribution_summary
observed_revision_multiplier_p50/p95/max
role_projection_summary/parquet_measurement_summary
db_transaction_budget_summary/memory_budget_summary
staging_store_summary/durable_store_summary/store_available_bytes
status = MEASURED | PARTIAL | INSUFFICIENT
reason_codes[]/missing_measurements[]
receipt_hash
```

receipt verifier必须重算`program_workload_set_hash`并证明所有Program workload都被测量范围覆盖；不得只比较顶层
request hash或style汇总。

### 7.12 `Phase1EProgramInputUnit` 与 `Phase1ERealInputBundle`

```text
Phase1EProgramInputUnit {
  program_id/decision_trade_date
  package_id/manifest_sha256/alpha_mode/style_family
  historical_program_run_ref/hash
  phase0a_audit_ref/hash nullable
  handoff_readiness_ref/hash nullable
  handoff_bundle_ref/hash nullable
  source_requirement_set_ref/hash nullable
  source_resolution_receipt_ref/hash nullable
  capacity_program_workload_ref/hash nullable
  capacity_coverage_ref/hash nullable
  phase1e_program_date_request_ref/hash nullable
  identity_readiness = PENDING | COMPLETE | BLOCKED
  source_readiness = NOT_EVALUATED | PENDING | READY | BLOCKED
  capacity_status = NOT_MEASURED | PARTIAL | MEASURED | INSUFFICIENT
  plan_readiness = IDENTITY_PENDING | IDENTITY_COMPLETE_SOURCE_PENDING |
                   SOURCE_READY_CAPACITY_PARTIAL | FULL_READY | BLOCKED
  missing_slots[]
  reason_codes[]
  program_input_hash
}

Phase1ERealInputBundle {
  build_request_ref/hash
  target_database_identity_hash
  policy/query/calendar/label/partition/store/capacity/artifact_store refs+hashes
  source_mapping_registry_ref/hash
  source_requirement_registry_ref/hash nullable
  capacity_request_ref/hash nullable
  capacity_receipt_ref/hash nullable
  phase1e_revalidation_batch_request_ref/hash nullable
  program_inputs[]
  counts_by_identity/source/capacity/plan_readiness
  aggregate_readiness = ALL_FULL_READY | MIXED | ALL_PENDING | BLOCKED
  dependency_closure_hash
  input_bundle_hash
}
```

逐 Program unit 是 readiness authority；顶层 aggregate 只做统计，不得阻断其它合法 Program。一个 Program pending
时，其他 `FULL_READY` Program 仍生成独立 complete plan；一个 Program blocked 时，其他 Program 不被降级。所有 ref
必须使用 typed artifact ref、semantic hash 与 full readback；未形成的 ref 保持 NULL 并列出 exact `missing_slots`，禁止
空字符串、占位 hash、手写 JSON 或 latest path。`IDENTITY_COMPLETE_SOURCE_PENDING` 只允许 diagnostic/template
operation，不得被 G5 当作 L3/L4 ready。

Program unit中的O4-owned ref与artifact kind固定一一对应：

| Program unit ref | exact O4 artifact kind |
|---|---|
| `source_requirement_set_ref` | `source_requirement_set` |
| `capacity_program_workload_ref` | `capacity_program_workload` |
| `capacity_coverage_ref` | `capacity_program_coverage` |
| `phase1e_program_date_request_ref` | `phase1e_program_date_request` |

不得使用registry、capacity request/receipt或batch request的ref冒充Program级artifact。每个Program级artifact都包含parent
artifact ref/hash、Program/date identity和自身semantic hash；batch membership不进入其identity。

Program readiness固定按下表派生，调用方不能自行提升或覆盖：

| plan_readiness | identity/source/capacity必要条件 | 允许输出 |
|---|---|---|
| `IDENTITY_PENDING` | identity=PENDING；source=NOT_EVALUATED；capacity不提升状态 | 仅Program unit、missing slots和pending reason |
| `IDENTITY_COMPLETE_SOURCE_PENDING` | identity=COMPLETE；source=PENDING；capacity不改变source事实 | diagnostic/template，不生成source-dependent complete request |
| `SOURCE_READY_CAPACITY_PARTIAL` | identity=COMPLETE；source=READY；capacity=PARTIAL且missing仅为允许的bootstrap measurement | 仅bounded staging操作；不得标记MEASURED或完整计划ready |
| `FULL_READY` | identity=COMPLETE；source=READY；capacity=MEASURED且覆盖exact workload | 独立Phase1E complete plan |
| `BLOCKED` | 任一identity/source为BLOCKED，O3 Program为FAILED，capacity=INSUFFICIENT，或workload/ref无法无损表达 | failed scope和blocked reason；不得生成complete request |

aggregate派生规则：全部unit为`FULL_READY`才是`ALL_FULL_READY`；没有full/staging/blocked且至少一个pending时为
`ALL_PENDING`；full、staging、pending、blocked任意混合为`MIXED`；只有build-level request/closure不可解析、没有可构造unit，或
全部unit均`BLOCKED`时才为`BLOCKED`。aggregate不参与Program plan hash，也不是审批状态。

## 8. Production Export Algorithm / 生产只读导出算法

1. 从显式 env 文件分别解析 production 和 DEV 配置，不使用 global pool。
2. production 连接设置 server-enforced read-only、`REPEATABLE READ` 和有界 statement timeout。
3. 验证source Program仅作为provenance存在；其legacy null binding和DSE v1明确标记`INELIGIBLE_SOURCE_FACT`，
   不进入portable bundle。
4. 验证single/native-multi package identity，并按StrategyPackage权威canonical算法从`manifest_json`重算current
   manifest hash；允许状态固定为当前Selection可见的非`RETIRED`已知生命周期，未知状态和`RETIRED`均不导出。
   source manifest不改写；target使用冻结portable projection生成独立manifest hash，并保存source-to-portable lineage。
5. 对 native multi parent 从 manifest 恢复全部 component package identities 和 component evidence；不同合法
   lookback/window 保持独立，不要求腿级窗口相同。
6. 读取parent/component package和package_asset的exact闭包；runtime blob只从`factor_set`、`model_asset`、
   `runtime_assets`及`source_evidence.multi_alpha.legs[].runtime_assets/seed_runtime_assets`投影，从显式只读source
   asset root加载、逐blob SHA验证并进入bundle CAS。不得递归收集其他`source_evidence`，与Selection运行无关的
   历史输出文件不加载。
7. target package行固定使用`candidate_strategy_package` source type；历史prediction/model locator列不复制。重新计算
   package/manifest/asset/component/policy canonical hashes，不继承source Program/binding hash作为DEV身份。
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
该调用是策略包已经准入后的正常推理执行，不在执行前增加package validator、health、runtime self-check、asset closure
revalidation或其它package gate；推理自身读取模型/因子资产属于既有执行语义，不得包装为新的荐股前置验证阶段。

固定流程：

1. Program create request显式固定target package、review policy、runtime config和future-effective binding date；不
   backdate、不修复或复制production legacy binding。
2. 首次`run-historical`允许在decision date尚未完成时先通过正常Program语义持久化future-effective binding，然后
   输出`INPUT_PENDING`；同一request在日期完成后重跑必须readback复用该Program/binding。不得先阻断Program创建再等
   生效日过去，否则正向流程不可达。decision date最终必须是binding生效后已经完成的交易日，不得改日期语义。
3. prospective Selection对每个Program/package独立运行，必须产生唯一DSE v2、artifact v2和完整stage/source receipts；
   v1、capture failed或incomplete evidence不能继续。
4. read-only evidence lookup核对exact dated binding和唯一DSE v2；该步骤不重新验证package资产；正常runner对各Program
   独立事务执行并产生正式receipt。
5. 同request exact rerun返回相同business identities，不产生第二份published事实。
6. 单Program `WAITING_INPUT/FAILED`不阻断其他Program，但batch receipt准确保留逐Program状态。
7. 每个`COMPLETE` Program独立形成Phase 1E Program input unit；双轨 cohort 状态只有在所选single/native-multi均
   complete时才标记dual-track complete，但一个Program的WAITING/FAILED不得删除或降级其它complete Program事实。
8. 使用Phase0A audit CLI的exact-target connection resolver对DEV做只读audit并写仓库外receipt。
9. 移除Phase0A CLI的localhost/port文本猜测，改为database identity与explicit target contract；该修改
   只影响 standalone audit CLI，不改变任何运行时连接逻辑。
10. standalone编排开始时获得的DEV database identity必须传入所有repository使用的connection factory；每一个新建
    writable connection都重新读取并核对完整database identity，不能只校验首个只读连接。
11. `code_release_id/hash`只接受clean Git worktree的exact HEAD。tracked、staged或untracked变更均使执行失败；同Program/date
    已存在DSE v2时，还必须核对DSE contract与effective config chain中的producer code release，跨release不可复用。
12. O3不复制Selection的universe业务。由package inference或后续权威runtime负责、且未形成独立layer materialization的
    upstream layer明确记录`NOT_APPLICABLE`；package layer使用真实artifact source receipts记录`RESEARCH_ONLY`，risk和
    tradability layer使用真实stage receipt记录`FORMAL_READY`。禁止合成`universe:<name>`数据集、占位hash或伪造`PARTIAL`。
13. O3不得向`SelectionCenterService`注入`object()`、mock或无类型placeholder。Paper portfolio依赖使用typed
    execution-prohibited boundary；若共享Selection路径意外进入Paper创建，必须fail loud且不得产生执行事实。
14. 编排阶段记录的Program failure不能被formal runner后续的`WAITING_INPUT`覆盖。逐Program最终状态按
    `FAILED > WAITING_INPUT > COMPLETE`聚合，batch status必须等于逐Program状态的同一聚合结果，并由receipt contract校验。
15. Phase0A audit执行必须显式提供存在的env文件和仓库外output root；不读取缺失env后的process fallback，不设置
    `--execute-readonly`、acknowledgement或其他确认门槛，也不返回可能被误解为已完成audit的validation-only success。

## 11. Source And Capacity Formation / Source 与容量输入形成

### 11.1 Source availability truth

不从 production 复制 source availability event，也不从 DSE timestamp猜测事件。Phase 1D observer 必须使用
显式存在的 env 文件与 `TDX_DB_DEV_*` connection factory正常观察 DEV ingestion事实并append event。env 文件缺失、
DEV key缺失或任一 connection 的database identity与build request不一致时明确失败；不得回退process env、global pool、
localhost、production或当前常驻backend连接。历史事实无法证明decision cutoff前可用时，对应 Program保持
`IDENTITY_COMPLETE_SOURCE_PENDING`，其他 Program继续独立处理。

observer command本身就是一次显式执行请求，不再要求
`AISTOCK_ADVISORY_PHASE1_SOURCE_OBSERVER_ENABLED`、`--confirm`、acknowledgement、role或其它人工开关。该变更只作用于
standalone DEV observer CLI；不新增scheduler、worker或startup hook。observer repository中的默认global-pool构造仍可
供历史调用兼容，但O4路径必须显式注入DEV factory，且测试证明没有触发默认factory。

### 11.2 Source-ready positive path

完整 G5 正向路径需要至少一个 Program/date 的 required source events满足
`formal_available_at <= decision_cutoff`。实现必须支持：

1. 在prospective Selection前，由dated binding、admitted-manifest-only input projection、Selection config normalization和
   `AdvisorySourceMappingRegistry`构造`AdvisorySourceObservationScopeRequest`；该步骤不调用任何 package validator、
   health、asset loader、model loader、preflight或inference；
2. observer按该request先记录physical inputs的真实DEV ingestion完成事实；
3. 后续由现有Selection Center正常产生一个完成历史日的DSE；
4. actual DSE receipts/分腿window与pre-observation request逐字段reconcile；
5. DSE、artifact、mapping registry、requirement set和source event共享 exact PIT identity；
6. Phase 1E source replay得到 complete receipt；
7. G5 inventory只对`source_readiness=READY`且`plan_readiness=FULL_READY`的Program产生eligible L3 source和可执行L4 target。

只有顶层standalone编排CLI调用现有Selection producer；Advisory onboarding service模块、Phase1E和G5均不import
Selection runtime。若新DSE v2的source availability仍不能证明在decision cutoff前可用，则只能形成
`IDENTITY_COMPLETE_SOURCE_PENDING`，不得伪装为source-ready。编排是显式一次性命令，不是scheduler。

observer在decision cutoff之后首次观察到的真实event仍可append并保留，但只证明后续时间可用；它不能通过audit
`refreshed_at`、provider job结束时间或DSE生成时间回填成cutoff前可用。首次部署observer时，过去日期通常保持pending是
正确结果；正向验证使用observer已运行并在cutoff前记录完成事实的后续交易日，不通过backdate构造历史ready。

### 11.3 Source requirement registry

registry builder只从 typed pre-observation request、actual DSE input context/source receipts、
`StrategyPackageAdvisoryInputProjectionV1`、frozen query registry和`AdvisorySourceMappingRegistry`派生。实现按以下顺序工作：

1. 核对DSE、artifact与input projection中已持久化的package/manifest identity一致；该核对只证明当前Advisory证据引用同一
   已准入package，不重新验证package资产、不改变package可用状态，也不执行模型；
2. single Alpha读取DSE顶层window lineage；native multi从`per_leg_window_lineage`读取每腿独立
   `window_start_date/required_window/window_resolution/window_lineage_hash`；
3. 每个source receipt必须匹配pre-observation request和mapping registry中的exact role/dataset/query id/version/window；
4. 将logical receipt展开为全部physical requirements，并按交易日历形成inclusive business window和partition keys；
5. `pit_universe`、calendar、reference price等单日或interval输入按registry声明形成独立requirement；
6. 生成typed `SourceRequirementRegistry`和逐Program `SourceRequirementSet`，并full readback全部hash。

每个native multi component必须有独立template和真实lookback/window。禁止generic template、默认window、跨腿取交集、
使用父腿最大/最小window代替分腿window、从package名称推断，或把DSE虚拟dataset直接当作物理单表。任何receipt未覆盖时
只使该Program pending/blocked并输出稳定reason，不允许省略该receipt后继续。

### 11.4 Capacity request and receipt

O4使用`Phase1ECapacityPlanningRequestV2`。每个Program独立workload必须从exact build request、Program policy、DSE和
source requirement set派生；不得通过style级平均、最大值、重复计数或固定candidate depth近似。memory/store/Parquet
测量由Phase 1D capacity probe产生，operational bounds来自不可变capacity policy；不得从测试fixture或代码常量填充
业务值。CLI必须显式使用DEV connection和仓库外CAS root。

Parquet probe只允许读取DEV `app.advisory_dataset_snapshot` / `app.advisory_dataset_snapshot_file`引用的Advisory
`SEALED`研究数据文件及其metadata；禁止读取QE、Qlib、回测、Paper或任意用户路径。无SEALED文件时返回准确
`PARTIAL`，不要求为O4预先生成回测数据，也不阻断Selection、模拟盘或source event append。

首次无Parquet测量的bootstrap固定为两步：

1. source、identity和结构容量均完整，且missing仅属于允许的Parquet/changed-ratio measurement slots时，Phase1E只能
   生成既有bounded staging capture操作，不能标记capacity measured或完整后续操作ready；
2. staging产生首个合法Advisory SEALED snapshot后重新probe，形成新的capacity receipt，再重新compile；只有新的
   receipt覆盖exact workload时才冻结完整resource values。

其它missing、`INSUFFICIENT`、request/receipt/hash mismatch均不进入bounded staging。该过程是自动证据状态，不是审批或
人工门禁。

## 12. Phase 1E Compilation / Phase 1E 编译

1. `Phase1ERevalidationBatchRequest` 中每个Program/date必须填满`expected_package_id`、
   `expected_manifest_sha256`、`expected_alpha_mode`、`expected_style_family`和exact historical receipt；O4 verifier拒绝
   这些字段为NULL，不能依赖compiler从当前数据库状态推断。
2. policy、source mapping、source requirement、query registry、calendar、label、partition、store、capacity和artifact
   store refs全部指向immutable artifact，不接受路径alias、latest、目录mtime或只有hash没有可定位ref的输入。
3. `AISTOCK_ADVISORY_PHASE1E_ARTIFACT_ROOT` 不存在时，由CLI显式 `--artifact-root` 指定仓库外受约束root；
   不写repo，也不猜测默认目录；env和CLI同时提供root但resolved path不一致时明确冲突。
4. compile-batch只接受DEV，删除`--target-db prod`；必须使用显式存在的`--env-file`，禁止process env/global pool
   fallback。每个Program snapshot读取并核对database identity；输出plan/batch receipt后逐个verify-plan/full readback。
5. identity-complete/source-pending必须输出准确operation disposition和missing slots。compiler允许把单个scope的预期
   `Phase1EError`记录到`failed_input_scopes`并继续其它Program；unexpected exception必须保留traceback并形成该scope的
   stable failure。全部scope失败或plan count为0时，batch必须非成功退出，不能broad exception、空列表或空batch冒充完成。
6. `FULL_READY`计划必须分别保留single/native multi parent/component证据，多个Program不合并；mixed batch中
   FULL_READY Program正常产生complete plan，pending Program只产生diagnostic/template或failed scope，
   SOURCE_READY_CAPACITY_PARTIAL Program只产生bounded staging。
7. compile前后均校验build request、input bundle、registry、capacity和batch request dependency closure；任一ref指向
   不同semantic hash时整个compile invocation失败，但不修改任何数据库事实。

## 13. CLI Contract / CLI 契约

O4固定复用以下单一入口，不新增第二套业务CLI：

```text
scripts/advisory_real_dev_onboarding.py inventory
scripts/advisory_real_dev_onboarding.py export-bundle
scripts/advisory_real_dev_onboarding.py verify-bundle
scripts/advisory_real_dev_onboarding.py plan-import
scripts/advisory_real_dev_onboarding.py import-dev
scripts/advisory_real_dev_onboarding.py verify-import
scripts/advisory_real_dev_onboarding.py run-historical
scripts/advisory_real_dev_onboarding.py observe-source
scripts/advisory_real_dev_onboarding.py plan-capacity
scripts/advisory_real_dev_onboarding.py build-phase1e-inputs
scripts/advisory_real_dev_onboarding.py compile-phase1e
scripts/advisory_real_dev_onboarding.py verify-evidence
```

`verify-import`必须同时接收生成receipt的exact `--plan`，并核对request、bundle、plan、source/target database
identity、relation counts、全部post-readback hashes和dependency closure；不能只验证部分receipt字段。

每个有副作用的command语义由命令名和exact request决定，不增加 `--confirm`、审批token、role、backup或
manual bypass。`inventory/export/verify/plan` 永远零DB写入；`import-dev`只写固定import allowlist；
`run-historical`只通过现有runner写Advisory historical relations；`observe-source`只通过现有Phase1D repository向DEV
source ledger/observer relations追加真实事实；`plan-capacity`只读DEV并写external CAS；`build-phase1e-inputs`和
`compile-phase1e`只读DEV并写external CAS。
Phase0A audit同样由request直接决定只读执行，不增加`--execute-readonly`或等价acknowledgement；audit必须显式接收
`--env-file`和仓库外`--output-root`。

所有O4 command必须显式接收存在的`--env-file`、`--artifact-root`和exact request/ref；不提供`--target-db`，目标固定为
DEV。兼容保留的`scripts/advisory_phase1_source_observer.py`与`scripts/advisory_phase1e_readiness_plan.py`必须复用同一
resolver/contract，不得保留enable flag、production选项或process-env fallback形成第二套语义。

同一历史request的source-ready操作顺序固定为：首次`run-historical`在日期未完成时正常保留Program/binding并返回pending；
日期当日真实ingestion完成后显式执行`observe-source`；随后重跑`run-historical`生成DSE/receipt；最后执行
`plan-capacity -> build-phase1e-inputs -> compile-phase1e`。这只是时间顺序和数据事实，不是审批门禁。若用户在observer
记录前先生成DSE，该日期仍可研究但source保持pending，程序不得删除DSE、回填event或自动改用其它日期。

稳定退出码：

```text
0 all requested Program units complete for this operation or exact idempotent readback
2 invalid request/contract/conflict before DML
3 operation persisted truthful artifacts but one or more Program units remain authoritative input pending
4 DEV import/runner/O4 verification failed or one or more Program units are blocked
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
backend/services/advisory_dev_input_onboarding/historical_onboarding.py
backend/services/advisory_dev_input_onboarding/phase1e_inputs.py
scripts/advisory_real_dev_onboarding.py
backend/tests/advisory_dev_input_onboarding/
```

允许最小修改：

```text
scripts/advisory_phase0a_audit.py
scripts/advisory_phase1_source_observer.py
scripts/advisory_phase1e_readiness_plan.py
backend/services/advisory_phase1/source_observer.py
backend/services/advisory_phase1/readiness_plan_store.py
backend/services/advisory_phase1/source_observer_postgres.py
backend/services/advisory_phase1/source_capacity.py
backend/services/advisory_phase1/readiness_plan.py
backend/services/advisory_phase1/readiness_plan_postgres.py
backend/tests/advisory_phase1/
backend/services/strategy_package/advisory_input_projection.py
backend/tests/strategy_package/test_advisory_input_projection.py
```

`source_observer.py`只允许增加O4所需的versioned physical query templates/mapping-compatible typed contracts；
`source_capacity.py`只允许additive v2 request/receipt与v1兼容读取；`readiness_plan.py`只允许要求O4生成的exact identity、
消费v2 workload和保持逐Program独立；`readiness_plan_postgres.py`只允许database identity校验和既有read-only projection
wiring。既有v1调用、Selection、Paper、模拟盘和其它Phase1调用的业务结果必须保持不变，并有直接回归测试。
`advisory_input_projection.py`是唯一允许新增的StrategyPackage共享文件，只能从已经准入的typed frozen manifest生成
`StrategyPackageAdvisoryInputProjectionV1`；不得调用或包装现有 preflight/validator/health/asset store/model loader/
inference，也不得重新计算package资产闭包。其新增不改变StrategyPackage admission、Selection、Paper或模拟盘行为。
不得解析模型文件猜测window、运行一次模型探路、修改其它共享StrategyPackage/Selection模块，或先生成DSE再回填
pre-observation request。

上述其它修改只允许增加exact connection/root injection和typed output wiring，不改变既有业务判定。standalone脚本通过
`historical_onboarding.py`组合现有`AdvisoryProgramService`、`SelectionCenterService`、
`StrategyPackageSelectionService`和historical runner。由于现有Program公开创建接口不能接收request中已经冻结的
`program_id`、指定的future-effective binding date和binding runtime config，O3适配层允许调用
`AdvisoryProgramService`既有配置校验、日期校验和binding构造语义，并通过既有repository原子写入同一Program/binding
模型；不得复制或放宽这些校验。由于prospective DSE v2的context必须等于Selection最终规范化配置，O3适配层允许调用
现有Selection service的纯规范化/预检方法形成context，但最终候选、artifact和DSE仍必须由公开
`SelectionCenterService.run_single_package`路径生成。WSL推理环境只允许由O3适配层从显式`TDX_DB_DEV_*`配置构造
子进程env，不得修改共享推理器或进程全局数据库配置。以下路径冻结：

```text
backend/main.py
backend/routers/
backend/services/selection_center/
backend/services/strategy_package/  # except additive pure advisory_input_projection.py
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
对共享constructor中O3不会调用的执行依赖，必须注入typed fail-loud boundary，禁止使用`object()`、fixture或mock冒充
生产依赖；该boundary只表达既有`execution_prohibited=true`研究范围，不增加审批、授权或运行门禁。

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
ADVISORY_DEV_ONBOARDING_SOURCE_MAPPING_MISSING
ADVISORY_DEV_ONBOARDING_SOURCE_MAPPING_CONFLICT
ADVISORY_DEV_ONBOARDING_PROGRAM_INPUT_INCOMPLETE
ADVISORY_DEV_ONBOARDING_PROGRAM_READINESS_CONFLICT
ADVISORY_DEV_ONBOARDING_CAPACITY_POLICY_MISSING
ADVISORY_DEV_ONBOARDING_CAPACITY_WORKLOAD_MISMATCH
ADVISORY_DEV_ONBOARDING_ARTIFACT_REF_MISSING
ADVISORY_DEV_ONBOARDING_ARTIFACT_CLOSURE_CONFLICT
ADVISORY_DEV_ONBOARDING_ENV_FILE_REQUIRED
ADVISORY_DEV_ONBOARDING_DEV_IDENTITY_DRIFT
ADVISORY_DEV_ONBOARDING_UNEXPECTED_ERROR
```

expected business gap记录一次摘要；unexpected error保留后台traceback。Pydantic/contract校验错误只输出字段路径、
错误数量和稳定reason，不得输出`input_value`或原始payload。日志只包含command、request/bundle/plan/
receipt hash、target label、relation counts和reason/context，不输出密码、DSN、完整manifest、模型payload、候选全量
或逐行无价值日志。禁止catch-all转success、空列表或 `ALREADY_PRESENT`。
Program provisioning/Selection阶段的失败状态必须进入最终receipt；formal runner只补充正式run identity和其自身状态，不能
把已记录的失败降级为input pending。receipt model必须拒绝batch status与逐Program聚合状态不一致的payload。

O4 readiness reason必须逐Program记录。source event尚未在cutoff前形成和允许的首次Parquet measurement gap属于可恢复
pending；`SOURCE_MAPPING_MISSING`、pre-observation与actual DSE差异、identity/hash冲突、未知query version、database
identity drift、同ref不同内容、capacity workload不能无损表达属于blocked。不得把blocked降级为pending，也不得因一个
Program blocked把其它Program改成blocked。日志禁止输出env值、DSN、完整DSE/manifest、候选列表或完整异常payload；
unexpected traceback只写后台日志。

## 15. Concurrency And Idempotency / 并发与幂等

- bundle CAS使用atomic no-replace和full readback；同内容并发导出得到同hash。
- import transaction按bundle hash取得transaction-scoped advisory lock，只序列化相同bundle，不形成全局锁。
- 不同package bundle可并发，但数据库unique/FK与exact compare仍是最终authority。
- exact bundle重复执行必须零DML并返回同post-closure hash、新invocation receipt。
- 相同identity不同payload必须冲突；不采用last-write-wins、upsert update或静默跳过。
- historical runner继续使用既有business key和逐Program独立事务，不改变其重试/恢复语义。
- historical request/receipt CAS和Phase0A audit directory CAS都必须使用atomic no-replace、文件集合闭包校验和逐文件
  exact readback；并发发布不能使用`Path.replace()`覆盖已存在identity，额外文件或目录同样视为冲突。
- O4 CAS固定增加以下15个kind，禁止复用其它kind冒充缺失层级：

  ```text
  real_input_build_request
  strategy_package_input_projection
  source_mapping_registry
  source_observation_scope_request
  source_requirement_registry
  source_requirement_set
  capacity_policy
  capacity_request
  capacity_program_workload
  capacity_receipt
  capacity_program_coverage
  program_input
  input_bundle
  phase1e_program_date_request
  phase1e_batch_request
  ```

  每个kind具有固定namespace、typed envelope、semantic hash、file hash和dependency refs；禁止把这些对象作为无类型JSON
  写到任意路径。
- 同一Program/date相同build request并发必须收敛到相同`program_input_hash`；不同Program从不共享可变临时状态。batch
  membership不进入Program input identity，增加或移除其它Program不得改变已有Program unit hash。

## 16. Security And Data Minimization / 安全与数据最小化

- env文件只读取所需key；receipt中只存database identity hash，不存credential。
- export request只能指定数据库中已存在的source package和显式DEV Program spec，不接受任意SQL、schema、table或path。
- relation和column list由代码冻结，标识符不来自用户输入。
- bundle root、blob root、Phase 1E root和evidence root都必须仓库外、canonical resolve、containment检查、拒绝
  symlink/reparse/latest pointer。
- 不复制账户、资金、订单、持仓、Paper、simulation、QE实验或其他无关数据。
- O4 query registry只含固定SELECT template和typed bind slots；不得接收用户SQL、表名、列名、任意URI或runtime loader。
- StrategyPackage input projection只读取调用方已提供的typed frozen manifest对象，不打开asset URI/path、不读取blob、
  不访问package repository、不运行asset/admission validation，也不把投影结果写回package或binding。
- capacity probe读取的Parquet路径必须来自DEV Advisory snapshot rows并通过root containment、记录size和footer metadata
  readback；不得遍历目录、读取QE/Qlib/backtest/Paper文件，缺失时返回PARTIAL而不是改读其它数据源。

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

实现F-906至F-911，内部顺序固定如下：

1. O4A contracts/CAS：build request、`real_input_build_request` artifact kind、source mapping registry、capacity v2、Program
   input unit、bundle和其余artifact kinds；
2. O4B admitted-input projection + target-aware observer：从typed frozen manifest纯投影single/native-multi分腿factor order/window，
   不复验package资产；随后完成真实DSE role到physical requirement映射、显式DEV factory和去除enable/prod/process-env路径；
3. O4C registry/capacity builder：分腿window、逐Program workload、capacity policy和DEV read-only probe；
4. O4D Phase1E input/compile：逐Program audit/handoff/source resolution、immutable refs、mixed batch和full readback；
5. O4E direct regression：v1 compatibility、frozen module isolation、真实DEV read-only/pending/ready验证。

每个子步骤必须完成其完整契约和测试后才能进入下一步；不得以手写registry、fixture workload、单Program特例或全局
readiness替代。source不成熟时逐Program准确pending，mapping/hash/identity冲突准确blocked。

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
- 扫描并直接断言O4 CLI不存在observer enable flag、`--target-db prod`、缺失env fallback或global-pool调用；
- AST直接断言`advisory_input_projection.py`除typed manifest model、canonical hash utility和window helper外，不import
  repository、asset store、validator、health、inference、Selection、Paper或simulation，且不包含文件/网络/数据库I/O；
  除该文件外StrategyPackage frozen path零修改；
- source mapping registry中的每个physical template必须属于固定SELECT allowlist，且当前DSE五类logical input全覆盖；
- v1 capacity/readiness public contract import和既有调用保持兼容，O4只使用v2；
- `python scripts/aistock_feature_workflow.py validate --design <path> --tier F2`。

### 18.2 L1 pure

- request/bundle/row-set/plan/receipt canonical hash、排序、篡改和redaction；
- single/native-multi闭包、component窗口差异、union/manual multi拒绝；
- relation/column allowlist、dependency graph完整性、unknown relation拒绝；
- insert/exact/conflict分类、same key different payload拒绝；
- identity-complete/source-pending/source-ready严格分型；
- 逐Program identity/source/capacity状态与
  `IDENTITY_PENDING/IDENTITY_COMPLETE_SOURCE_PENDING/SOURCE_READY_CAPACITY_PARTIAL/FULL_READY/BLOCKED`计划状态表、mixed batch和
  batch-independent Program hash；
- 当前真实DSE role/dataset/query version到physical requirements的golden mapping；unknown role/version明确失败；
- admitted-manifest-only projection对single/native-multi分别输出exact factor order与独立required window；测试通过monkeypatch
  证明package validator、health、asset store、model loader、preflight和inference即使被设置为调用即失败也不会触发；
- pre-observation request由package/binding/input projection确定且不依赖未来DSE；actual DSE role/window/leg reconciliation
  一致通过，任一差异blocked且不补写历史event；
- pre-observation required window与canonical factor-order helper、code release和actual DSE window lineage parity；
- daily window逐交易日展开、as-of snapshot与derived PIT universe upstream audit/build-state closure正反例；
- native multi每腿不同window、同style不同candidate depth、多个single/native-multi Program workload无损表达；
- build request、Program input、bundle、所有registry/policy/ref/hash closure篡改和missing-slot tests；
- no-SEALED bootstrap只产生bounded staging，非Parquet missing和INSUFFICIENT不能进入bootstrap；
- exit/reason/log contract，无silent fallback。

### 18.3 L2 disposable PostgreSQL 16

O3 producer integration使用`ADVISORY_O3_TEST_DSN`仅作为PostgreSQL 16管理连接，测试自行创建唯一
`aistock_o3_l2_<uuid>`数据库并在finally中终止残留连接后删除；禁止在配置指向的数据库内执行schema级清理。
Validation Catalog必须声明`writes_database=true`，但该声明不增加审批、备份或人工确认流程。
该L2固定`runner_enabled=false`，资源策略为`isolated_write`、`allowed_db_targets=[dev_db]`、
`forbidden_db_targets=[prod_db]`并由pytest finalizer自动删除临时数据库；普通backend CI仍运行
同一session中的非数据库回归，未提供DSN时PostgreSQL用例明确skip而不伪造L2成功。

模型执行边界可注入确定性`LiveInferenceResult`，但artifact必须由真实
`StrategyPackageSelectionArtifactService`生成，随后由真实`SelectionCenterService`、DSE assembler和PostgreSQL
repository消费。禁止复制fixture artifact、手工构造DSE或把package health固定为READY。

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
9. Phase1E input至少覆盖all-ready、all-pending、ready+pending、ready+blocked、全部scope失败和plan-count-zero；
10. 真实O3 single/native-multi先从已准入frozen manifest形成纯input projection，再在Selection前形成pre-observation
    request；整个projection过程零资产读取、零package validation、零模型执行；Selection后DSE source receipts经mapping
    registry展开并逐字段reconcile，分腿window与query closure完整readback；
11. observer template v2 daily/as-of/derived policy、无trade_date source capacity sample和PIT universe upstream closure；
12. capacity v2覆盖同style不同depth、多style、多日期，v1 existing tests保持不变；
13. env文件缺失、DEV identity drift、production target尝试、默认factory触发均明确失败且零写入；
14. 无SEALED snapshot时PARTIAL/bounded staging，加入合法Advisory SEALED snapshot后重新probe为MEASURED；测试文件只在
    临时Advisory CAS，禁止引用QE/Qlib/backtest/Paper路径；
15. 当前已导入的single/native-multi manifest fixture直接生成projection与observation request，不需要migration、package
    重新入库或asset backfill；
16. container/database销毁，不连接真实DEV/production。

### 18.4 L3 real DEV rollback validation

先在真实DEV对exact bundle执行只读plan；使用owner transaction运行完整importer后物理rollback，用fresh connection
逐exact key证明零残留。不得用DELETE/TRUNCATE清理。随后才允许persistent import。

O4本身不新增import DML。O4 L3先对真实DEV执行build request inventory、source mapping、capacity probe和Phase1E compile
read-only smoke；observer append验证如需新增event，必须使用真实已完成ingestion且在transaction rollback可证明的专用
测试partition，不能回填时间或清理既有event。无法形成cutoff前真实event时，L3预期结果就是逐Programpending，不伪造ready。
L3必须直接读取当前DEV已导入的exact single/native-multi frozen manifest并生成input projection，记录package row/status/binding
前后hash完全不变，同时用sentinel证明validator、health、asset store、model loader、preflight和inference零调用。

### 18.5 L4 real DEV persistent validation

1. production只读package export和offline verify；
2. DEV persistent package import与fresh full readback；
3. exact rerun零DML；
4. 当前已准入single/native-multi manifest无需资产复验直接产生input projection；正常DEV service随后产生Program/dated
   binding、DSE v2和single/native-multi formal receipt；
5. audit/handoff/source mapping/source registry/capacity v2/input bundle/Phase1E计划完整且Program独立；
6. 至少验证一个真实pending Program不会阻断其它FULL_READY Program；若当前没有cutoff前source event，则保留pending证据，
   不把环境限制写成代码完成；
7. source-ready后G5 L3/L4按父设计执行；无source-ready时O4仍可完成代码与pending验证，但不得声明G5 ready；
8. Selection/Paper/模拟盘/QE/QMT关系row count/hash在任务前后不因本任务allowlist之外发生变化。

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
| DSE逻辑dataset直接当物理表 | source闭包缺失或查询错误 | versioned source mapping registry逐项展开virtual query依赖；unknown mapping明确pending/blocked |
| O4为取得factor order再次运行包preflight或读取资产 | 已入库可用包被荐股二次阻断，且影响共享运行链 | admitted-manifest-only纯投影；禁止validator/health/asset/model/inference调用，结果不写回package状态 |
| 一个Program pending污染整个batch | 多包独立荐股被错误阻断 | ProgramInputUnit是readiness authority；batch只聚合统计 |
| 同style不同candidate depth被平均/取最大 | 容量低估、高估或hash漂移 | capacity v2逐Program workload，无lossy aggregation |
| env缺失退回process/global pool | 误连production或错误DEV | O4 CLI强制存在env文件、固定DEV、逐connection identity核验 |
| 无SEALED Parquet形成容量循环依赖 | 永久无法产生首个计划 | 只对允许measurement gap开放bounded staging，之后重新probe |
| optional expected identity被省略 | compiler读取当前状态导致身份漂移 | O4生成的ProgramDateRequest四项expected字段强制非空并full readback |
| future-effective日期被当审批 | 人为阻塞误解 | 明确为确定性时间事实，到期后程序自动可执行 |
| standalone工具下沉runtime | 影响常驻模块 | one-way script composition和反向import denylist |

## 20. Rollout And Rollback / 发布与回滚

### 20.1 Rollout

1. 按O1-O4独立PR合入代码；代码合入不自动连接数据库。
2. O1/O2 L0-L2通过后执行production read-only inventory/export。
3. DEV可在待合入代码上做rollback-only importer验证；persistent exact import必须等待对应代码合入后再执行。
4. 按O4A-O4E顺序合入contracts、target-aware observer、capacity v2、input builder和compile/verify；O4不需要DDL。
5. 正常运行DEV historical runner、audit、Phase1D和Phase1E CLI；逐Program状态独立记录。
6. G5 source pending保持pending；source-ready后执行L3再执行L4。
7. O5只同步已发生事实，不把设计或低层测试写成DEV完成。

### 20.2 Rollback

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
dev_dml_state = o2_committed_and_verified_o3_not_executed
```

代码合入、production只读export、DEV package import、DEV Program/DSE v2、Phase1E和G5状态必须分别报告。

## 22. Positive Reachability / 正向可达性

```text
existing production single + native multi package/asset closure
  -> exact package export bundle
  -> empty/non-conflicting DEV target
  -> insert-or-compare commit
  -> normal DEV Program + future-effective dated binding
  -> before completed decision date: INPUT_PENDING with Program/binding retained
  -> real ingestion completes; observe-source records pre-observation physical facts before cutoff when available
  -> same exact request rerun after the decision date completes
  -> normal prospective Selection DSE v2 after binding becomes effective
  -> existing historical runner COMPLETE x 2
  -> audit/handoff
  -> Phase1E identity-complete plans
```

source-ready完整路径：

```text
DEV source observer records real ingestion before cutoff
  -> source mapping registry covers every DSE logical input and every native-multi leg window
  -> normal upstream Selection produces exact DSE
  -> source replay COMPLETE
  -> per-Program Phase1E executable plan
  -> G5 inventory L3_READY/L4_DUAL_TRACK_READY
  -> L3 COMPLETE_ZERO_RESIDUE
  -> L4 COMPLETE_DUAL_TRACK
```

mixed Program路径：

```text
Program A exact source events before cutoff + capacity covered -> FULL_READY -> complete Phase1E plan
Program B event first observed after cutoff  -> IDENTITY_COMPLETE_SOURCE_PENDING -> diagnostic/template only
Program C unknown mapping/hash conflict      -> BLOCKED -> failed_input_scope
batch aggregate = MIXED; A remains executable; B/C do not alter A identity or plan hash
```

首次capacity测量路径：

```text
identity/source structurally complete + only Advisory SEALED Parquet measurements missing
  -> capacity PARTIAL
  -> bounded staging capture only
  -> first valid Advisory SEALED snapshot
  -> new read-only capacity probe and immutable receipt
  -> workload covered MEASURED
  -> full Phase1E operations compiled
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
- F-900：standalone编排只使用exact DEV connection injection，不调用backend.main/global pool；首个身份与每个后续writable
  connection的database identity必须一致。
- F-901：DEV Program/binding由正常service创建且future-effective，不复制/回填legacy null interval。
- F-902：正常prospective Selection原生生成single/native-multi DSE v2，v1不可升级或导入；DSE必须与clean code release一致，
  universe provenance不得使用合成dataset、占位hash或伪造PARTIAL。
- F-903：runner只接受manual historical/research-only/execution-prohibited request，逐Program失败隔离且receipt准确；状态按
  `FAILED > WAITING_INPUT > COMPLETE`聚合，禁止失败降级。
- F-904：Phase0A audit CLI移除localhost猜测，使用explicit target identity、显式env文件且无acknowledgement门槛。
- F-905：audit/handoff只读且artifact仓库外content-addressed，atomic no-replace并校验完整文件闭包。
- F-906：source event只由显式DEV observer从真实ingestion事实追加，不复制/猜测/backdate；O4不存在enable、prod target或
  process/global-pool fallback。
- F-907：逐Program identity/source/capacity/plan状态准确，mixed batch不把PARTIAL冒充FULL_READY，也不因一个Program失败
  阻断其它FULL_READY Program。
- F-908：pre-observation request在Selection前由dated binding、admitted-manifest-only typed input projection、Selection config
  normalization和versioned mapping形成；Selection后source registry从actual DSE receipt/input context、projection、query
  registry逐字段reconcile派生；真实五类logical input全覆盖，多Alpha每腿window独立，无generic/default window或事后回填。
- F-909：capacity v2逐Program表达style/depth/horizon/source workload，业务值来自exact Program/policy，测量来自DEV
  capacity probe；无SEALED时仅允许规定的bounded staging bootstrap。
- F-910：Phase1E build request、input projection、Program级requirement/workload/coverage/request和batch级artifact均使用独立
  exact kind；全部refs immutable、explicit、可定位且hash closed，无latest、跨层kind冒充或只有hash没有artifact ref。
- F-911：Phase1E计划single/native-multi parent/component和多Program独立；expected identity字段强制完整，mixed/all-failed/
  zero-plan状态显式且非成功不静默。
- F-912：G5只消费source-ready计划，pending保持零DML。
- F-913：G5 L3/L4继续使用既有契约，不在onboarding重定义门禁。
- F-914：无角色、RBAC、审批、授权、备份、force、skip、enable flag、production target selector或人工数据库修改。
- F-915：无API/UI/scheduler/startup/runtime activation/production DDL/DML。
- F-916：状态分别报告code、bundle、DEV import、historical receipt、Phase1E、G5 L3/L4和production。
- F-917：荐股O4只从已准入frozen manifest做纯输入投影，不调用package validator/health/asset/model/preflight/inference，
  不改变package/binding状态，不影响Selection、模拟盘、Paper或其它Program；合法现有single/native-multi包自动通过正向路径。

## 24. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-879 | §7.1、§8 | `contracts.py`; request canonical/hash/date/package tests；PR #2231；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_merged | none |
| F-880 | §5.1、§8 | `production_projection.py`; server read-only/write-query spy；PR #2231；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_merged | none |
| F-881 | §5.2、§10 | exact env/database identity tests；PR #2231；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_merged | none |
| F-882 | §5.2、§9.1 | source-target identity/catalog receipt tests；PR #2231；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_merged | none |
| F-883 | §7.4、§8 | `store.py`; CAS no-replace/collision/tamper/readback tests；PR #2231；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_merged | none |
| F-884 | §7.3 | typed PostgreSQL value serialization golden tests；PR #2231；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_merged | none |
| F-885 | §7.4、§8 | single/native-multi/component-window closure tests；PR #2231；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_merged | none |
| F-886 | §5.4、§8 | package/blob/component graph and mutable-row exclusion tests；PR #2231；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_merged | none |
| F-887 | §7.4、§16 | credential/path/payload redaction scan；真实production只读bundle `75806f83b2a5`绝对历史locator移除且runtime closure完整；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-888 | §5.4、§8 | canonical manifest/lifecycle/runtime projection tests；source→portable lineage与`candidate_strategy_package`投影；真实DEV plan `8da94d2f87b9`；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-889 | §7.5、§9.1 | `dev_importer.py`; INSERT/EXACT/CONFLICT classification tests；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-890 | §9、§18.1 | fixed SQL registry/AST/forbidden statement scan；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-891 | §9.2、§18.3 | PostgreSQL 16 owner transaction/trigger rollback；真实DEV receipt `21f01f6aeab4`执行98次INSERT尝试、physical commit 0、fresh plan相同；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-892 | §9.2 | disposable PostgreSQL full-row insert-or-compare/readback；真实DEV receipt `62dd9e07ca0c`提交2个package和96个package_asset并通过fresh verify；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_real_dev | none |
| F-893 | §9.3 | all-key committed/not-observed/state-unknown tests including preexisting-row conflict；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-894 | §9.4、§15 | exact rerun plan `770daea7ba2e`为`ALREADY_PRESENT`；receipt `3e9977fc8355`为0 insert、0 physical commit、98 exact match并通过fresh verify；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_real_dev | none |
| F-895 | §15 | disposable PostgreSQL same/different bundle concurrency tests；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-896 | §9.4、§20.2 | no-cleanup SQL/source scan；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-897 | §5.3、§10 | one-way onboarding import graph/runtime import AST test；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-898 | §4、§9.2 | batch-a/replica-role/sequence-reset denylist；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-899 | §3、§6 | multi-package request and independent package identity tests；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l2 | none |
| F-900 | §10、§13 | `historical_onboarding.py`; exact DEV repository/runtime/WSL env injection、逐connection identity drift拒绝与AST no-global-pool测试；`python -m pytest backend/tests/advisory_dev_input_onboarding -q` | verified_l0_l1 | none |
| F-901 | §10、§18.3 | exact Program id、normal config/date validation、future binding、pre-date `INPUT_PENDING`和exact rerun测试；`backend/tests/advisory_dev_input_onboarding/test_o3_postgres_integration.py` | verified_l0_l2 | none |
| F-902 | §10、§18.3 | clean worktree release、existing DSE release一致性、typed prospective context、无合成universe provenance、DSE v2/v1 rejection与capture-status测试；`backend/tests/advisory_dev_input_onboarding/test_o3_postgres_integration.py` | verified_l0_l2 | none |
| F-903 | §10、§14、§18.3 | manual historical request、Selection失败不降级、逐Program状态隔离、batch aggregate contract与exact retry；`backend/tests/advisory_dev_input_onboarding/test_o3_historical_onboarding.py` | verified_l0_l2 | none |
| F-904 | §10、§13 | Phase0A audit explicit database identity、显式env、non-local DEV target、acknowledgement移除；`backend/tests/advisory_dev_input_onboarding/test_cli_and_isolation.py` | verified_l0_l1 | none |
| F-905 | §10、§15 | Phase0A audit/handoff read-only projection、CAS闭包与full-readback；`backend/tests/advisory_dev_input_onboarding/test_o3_historical_onboarding.py` | verified_l0_l1 | none |
| F-906 | §11.1、§13 | observer real-ingestion/no-copy/no-backdate、explicit DEV identity、no-enable/no-prod/no-fallback；`backend/tests/advisory_dev_input_onboarding/test_o4_source_observer.py` | design_ready | none |
| F-907 | §7.12、§11.2、§12 | per-Program state table、mixed batch、pending/blocked/no-fake-ready；`backend/tests/advisory_dev_input_onboarding/test_o4_program_readiness.py` | design_ready | none |
| F-908 | §7.9-7.10、§11.2-11.3 | admitted-manifest input projection、pre-observation/actual-DSE reconciliation、logical-to-physical closure和分腿window；`backend/tests/strategy_package/test_advisory_input_projection.py`、`backend/tests/advisory_dev_input_onboarding/test_o4_source_mapping.py` | design_ready | none |
| F-909 | §7.11、§11.4 | heterogeneous Program workload、capacity policy/probe、bounded bootstrap和v1兼容；`backend/tests/advisory_dev_input_onboarding/test_o4_capacity_v2.py` | design_ready | none |
| F-910 | §7.8、§7.10、§7.12、§12、§15 | 15-kind artifact closure、Program/batch层级ref不可互换、dependency hash/full-readback；`backend/tests/advisory_dev_input_onboarding/test_o4_input_bundle.py`、`backend/tests/advisory_dev_input_onboarding/test_o4_source_mapping.py` | design_ready | none |
| F-911 | §7.12、§12 | expected identities、single/native-multi/multi-Program parity、all-failed/zero-plan；`backend/tests/advisory_dev_input_onboarding/test_o4_phase1e_compile.py` | design_ready | none |
| F-912 | §11.2、§12 | G5 pending zero-DML and source-ready inventory；`backend/tests/advisory_phase1/test_phase1g_dev_inventory.py` | design_ready | none |
| F-913 | §17 O5、§22 | existing G5 contract parity and L3/L4 evidence；`backend/tests/advisory_phase1/test_phase1g_service.py` | design_ready | none |
| F-914 | §4、§11.1、§13、§21 | role/approval/backup/force/skip/enable/prod-selector scan；`backend/tests/advisory_dev_input_onboarding/test_o4_static_and_cli.py` | design_ready | none |
| F-915 | §4、§21 | API/UI/scheduler/startup/production-impact scan；`backend/tests/advisory_dev_input_onboarding/test_o4_static_and_cli.py` | design_ready | none |
| F-916 | §1、§21、§26 | separated state reporting assertions；`backend/tests/advisory_dev_input_onboarding/test_o4_program_readiness.py` | design_ready | none |
| F-917 | §7.9.1、§11.2、§13、§18 | no-secondary-validation import/call denylist、existing single/native-multi positive projection、package/Selection/simulation zero-diff；`backend/tests/strategy_package/test_advisory_input_projection.py`、`backend/tests/advisory_dev_input_onboarding/test_o4_static_and_cli.py` | design_ready | none |

本矩阵的 `gap_or_exception` 只记录设计偏差或验收例外，不记录尚未执行的环境层验证。代码、数据库和运行证据的
完成状态以本文开头的独立状态字段为准；`verified_l0_l2` 不代表真实 production/DEV 执行已完成。

## 25. DESIGN-COMPLIANCE-001 Review

- `no_simplified_delivery`：覆盖export、DEV import、historical runner、真实DSE logical-to-physical source mapping、逐Program
  capacity v2、input bundle、Phase1E和G5完整链路；禁止generic registry、style聚合近似、单Program特例、手写JSON或把
  identity/bootstrap冒充source-ready完成。
- `no_silent_error`：逐Program pending/blocked、mapping missing、identity conflict、rollback、commit uncertainty、capacity
  partial/insufficient、all-failed、zero-plan和unexpected均有稳定reason/exit/log；mixed batch不得隐藏失败scope。
- `no_business_semantic_drift`：DSE v2由正常Selection生产，不转换v1、不修改排名；Program、package、multi-alpha leg、
  lookback/window、candidate depth和plan hash均独立，不融合Program，不改策略包、荐股、模拟盘或交易语义。
- `no_duplicate_package_gate`：O4只把已准入frozen manifest投影为输入DTO，不调用validator/health/asset/model/preflight/
  inference，不重新判断package可用性；projection或Advisory evidence错误不写回package/binding状态，也不影响其它模块。
- `no_unrequested_gate_or_approval`：无角色、审批、授权、备份、force、skip、observer enable flag、production target选择或
  acknowledgement；显式request/command是数据与动作输入，不是人工审批记录。
- `positive_path_satisfiable`：现有production双轨package可导入DEV；已准入manifest内现有Alpha158 aliases、factor refs和
  multi-leg runtime metadata足以零资产回读形成input projection；正常future-effective binding、真实source event和
  prospective DSE v2可到Phase1E及G5 L3/L4。
- `exact_database_truth`：所有连接来自显式env并核验database identity；production永远只读，DEV写关系固定。
- `research_isolation`：全部输出research-only、execution-prohibited，不产生交易输入。
- `state_reporting_truth`：design、implementation、DEV DML、production、Phase1E和G5状态分开报告。

## 26. Exit Criteria And Next Stage / 退出条件与下一阶段

本文可标记 `design_ready` 的条件：

1. F-879至F-917全部有前后一致的设计与验证映射。
2. 父级Phase 0A.2、Phase 1E、G5的输入/状态/退出条件引用本文且不互相矛盾。
3. 没有全库refresh、fixture、手写receipt、共享runtime执行或source猜测路径；唯一StrategyPackage改动是无I/O纯input
   projection，不形成资产复验或包准入门禁。
4. O4 build request、source mapping、capacity v2、Program input和CAS dependency closure全部有typed contract与逐字段权威来源。
5. 所有失败均显式，mixed Program、首次capacity bootstrap和source-ready均有正向可达路径。
6. 无额外角色、审批、授权、备份、enable flag、production selector或人工数据库修改设计。
7. F2 validator、文档引用、`git diff --check`通过。

本次设计修订合入后，代码阶段继续完成O4A的dedicated build-request kind，再按修订后的O4B admitted-input projection与
target-aware observer、O4C-O4E执行；O1-O3保持已合入状态，O5不得提前开始。
不得把O4 contracts、pending验证或bounded staging描述为Phase 1E/G5完整完成。
\n
