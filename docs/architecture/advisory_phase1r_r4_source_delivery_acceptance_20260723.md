# Advisory Phase 1R R4 源码、DEV 与历史业务验收记录

> 更新日期：2026-07-24  
> 唯一实施权威：`docs/architecture/advisory_phase1r_r4_outcome_summary_phase1_bridge_f2_design_20260723.md`  
> 当前状态：`source_dev_schema_and_rollback_bridge_verified_historical_e2e_pending_authorization`  
> 本记录不代表 commit、PR、merge、生产 DDL/DML、服务重启或 runtime activation 已完成。

## 1. 工作区与边界

```text
worktree = F:\Dev\AIstock_worktrees\advisory-phase1r-r4-outcome-bridge-20260723
branch = feature/advisory-phase1r-r4-outcome-bridge-20260723
head = 001ab1520eaf41850d390d0b10dcf472650e5f0a
origin_main_relation = behind_19
lane = verify-aistock-feature
feature_tier = F2
task_tier = T3
```

本轮保留现有未提交现场，不 rebase、merge、commit、discard 或 cleanup。R4 只实现 Outcome、Summary 与 Phase 1 retrospective bridge；不实现 R5 API/UI、模型训练或用户可见 capability，不读取 QE/backtest、Paper/模拟盘或人工交易结果，不修改 Selection、Paper、Simulation、QE、Qlib、QMT、订单、持仓、账户或交易业务逻辑。

R3 权威前置事实仍来自 `docs/architecture/advisory_phase1r_r3_source_delivery_acceptance_20260722.md`：权威 15 日 batch 为 `ahrb_dccde5770463663ecbde96fbe304cd26`，范围 `2026-07-01..2026-07-21`，单 Alpha 与原生多 Alpha 两个 Program 均为 15/15 成功。R4 没有重跑或改写 R3。

## 2. 实现范围

### 2.1 Outcome

- 在 `backend/services/advisory_phase1/outcome_engine.py` 提取并复用 `PositionPathValuationCore`；formal `OutcomeEngine` 保持字段与 hash parity。
- 在 `backend/services/advisory_historical_range/outcome_planner.py`、`outcome_evaluator.py`、`outcome_projection.py`、`outcome_source.py`、`outcome_service.py` 和 `outcome_policy_provider.py` 实现四类 subject、`FIXED_HORIZON`/`EPISODE_LIFECYCLE`、T/E/S/X 时间线、range-native policy、真实 source revision、maturity、correction、typed artifact、exact retry 与 durable refresh operation。
- recommendation 使用 R3 T 日 frozen decision mark；executable 使用真实 next-open/exit/terminal evidence；open episode 只允许 `RIGHT_CENSORED`，episode horizon 使用 0 sentinel。
- list/range 聚合只消费 child outcome artifact，不重新读取价格。

### 2.2 Summary

- 在 `backend/services/advisory_historical_range/summary_service.py` 实现 candidate、episode、list-version、range 四层聚合与 append-only exact retry。
- 胜率、赔率、等权 cohort、turnover、drawdown、holding period、industry HHI、strategy recall 与 conditional recall 分开计算；PIT denominator 缺失时产生 typed unavailable，不阻断其他 summary。
- 行业与 regime 只使用 R3 decision-date T 的冻结证据，不读取 outcome-date 或 current/latest 行业/HMM。

### 2.3 Phase 1 retrospective bridge

- 在 `backend/services/advisory_phase1/retrospective_contracts.py`、`retrospective_selector.py`、`capture_foundation.py`、`observation_capture.py`、`label_capture.py`、`dataset_build.py` 与 `snapshot_writer.py` 实现 formal/range tagged union；range 路径不合成 Phase 0A audit、handoff 或 admission identity。
- 在 `backend/services/advisory_historical_range/dataset_bridge.py` 实现 exact-ref input closure、canonical signal 去重、candidate fixed-horizon executable label 投影、`VALID_EMPTY` 与 non-empty 两条路径，以及 durable `BUILD_DATASET_BRIDGE` lease/fencing/recovery/exact retry。
- 在 `backend/services/advisory_historical_range/dataset_bridge_postgres.py` 接入真实 PostgreSQL observation/label capture、calculation evidence CAS、retrospective dataset build 与 Batch-D SEALED snapshot pipeline；capture recovery 保持 request hash、range policy 与 selector hash。
- snapshot 使用独立 retrospective Arrow schema，透传 retrospective selector hash；formal schema/canonical bytes 不因 R4 漂移。只有 non-empty bridge 创建 snapshot，`VALID_EMPTY` 不创建零行或伪 snapshot。

### 2.4 Migration 与 release schema

- 新增 `backend/db/migrations/add_advisory_historical_range_r4_outcome_bridge_20260723.sql`，以 additive tagged-union 方式扩展 R4 outcome/summary/operation 与 Phase 1 capture/label/dataset/snapshot contract。
- 新增 release schema v4：`backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v4.json`；默认 registry 已切到 v4，并保持 v4 → v3 → v2 → v1 predecessor chain。
- migration SHA-256：`a0f99cd4bca07dcf74ac5f74da0900e589d8cf6dc9d4a114135f9b9ab4e67142`。
- v4 contract hash：`eb44a5917e90f3a53800ff0b7ca9c03f20654a92d79a91a040584535dba5ad97`。

## 3. F-700 至 F-739 验收矩阵

状态语义：`PASS_SOURCE` 表示源码和直接合同验证通过；`PASS_SOURCE_DEV_SCHEMA` 额外表示现有 DEV schema 已验证；`PARTIAL` 或 `PENDING_AUTHORIZATION` 不得解释为完整验收或可合入。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-700 | `models.py`; `composition.py`; `dataset_bridge.py` | changed-file/protected-module audit | PASS_SOURCE | R5、训练、API/UI 与 capability 均未进入范围 |
| F-701 | R3 acceptance ledger；父设计与蓝图 | R3 batch `ahrb_dccde5770463663ecbde96fbe304cd26` 既有记录 | PASS_PRECONDITION | R4 未重跑 R3 |
| F-702 | `advisory_phase1/outcome_engine.py`; `outcome_projection.py` | `test_formal_outcome_engine_delegates_with_field_and_hash_parity`；formal Phase 1 direct matrix | PASS_SOURCE | none |
| F-703 | `artifact_store.py`; `repository.py`; R4 migration | outcome artifact/repository/full-readback contracts | PASS_SOURCE_DEV_SCHEMA | 真实 15 日 append 尚未执行 |
| F-704 | `outcome_projection.py`; `outcome_evaluator.py` | recommendation/executable separation 与 work-item identity tests | PASS_SOURCE | none |
| F-705 | `outcome_planner.py`; `models.py`; R4 migration | planner 四 subject、cursor、episode 0 sentinel；migration test | PASS_SOURCE_DEV_SCHEMA | none |
| F-706 | `outcome_evaluator.py`; `outcome_policy_provider.py` | exact policy artifact 与 component drift tests | PASS_SOURCE | 无 synthetic admission |
| F-707 | `outcome_evaluator.py`; `outcome_source.py`; `composition.py` | projection/evaluator calendar contract tests | PASS_SOURCE | none |
| F-708 | `models.py`; `outcome_projection.py`; `outcome_service.py` | maturity、open/closed episode、source-not-arrived tests | PASS_SOURCE | known unavailable 不写 0 |
| F-709 | `outcome_service.py` | `test_source_not_arrived_waits_without_writing_failed_outcome` | PASS_SOURCE | none |
| F-710 | `models.py`; `repository.py`; R4 migration | revision reason/transition/migration contracts | PASS_SOURCE_DEV_SCHEMA | 历史 source correction E2E 未执行，见 F-738 |
| F-711 | `models.py`; `outcome_projection.py`; `repository.py` | work-item identity、unique input 与 exact retry tests | PASS_SOURCE_DEV_SCHEMA | none |
| F-712 | `artifact_store.py`; `repository.py`; `outcome_evaluator.py` | outcome set loader/full readback/upstream closure tests | PASS_SOURCE | none |
| F-713 | `repository.py`; `outcome_evaluator.py`; `outcome_projection.py` | T mark、episode owner、open censor/closed rejection tests | PASS_SOURCE | none |
| F-714 | `outcome_source.py`; `outcome_engine.py`; `outcome_projection.py` | executable calculation evidence contracts | PASS_SOURCE | none |
| F-715 | `outcome_evaluator.py`; `summary_service.py` | child-only list aggregate test；summary formula matrix | PASS_SOURCE | none |
| F-716 | `summary_service.py` | coverage、win/loss、cohort、drawdown、turnover、holding、HHI tests | PASS_SOURCE | none |
| F-717 | `summary_service.py`; `outcome_source.py` | exact-positive denominator 与 missing-recall-visible tests | PASS_SOURCE | denominator unavailable 为 typed unavailable |
| F-718 | `summary_service.py`; `models.py`; `repository.py`; R4 migration | summary coordinator append/exact retry 与 outcome-set loader tests | PASS_SOURCE_DEV_SCHEMA | none |
| F-719 | `outcome_service.py`; `dataset_bridge.py`; `repository.py`; R4 migration | refresh/bridge durable operation、capacity retry、expired takeover tests | PASS_SOURCE_DEV_SCHEMA | none |
| F-720 | `outcome_service.py`; `composition.py` | bounded top-level slice 与独立 range-run composition contracts | PASS_SOURCE | 单/多 Program 真实 15 日 R4 E2E 并未执行 |
| F-721 | `outcome_source.py`; `composition.py` | read-only source provider/source binding contracts；DEV 连接仅由 `.env` 解析 | PASS_SOURCE | 未记录 credential 值 |
| F-722 | composition/import/scope audit | forbidden-source search 与 changed-file audit | PASS_SOURCE | 未读取 QE/backtest/Paper/模拟盘/人工交易结果 |
| F-723 | `retrospective_contracts.py`; capture/label/dataset/snapshot modules；R4 migration | tagged-union repository、manifest 与 migration tests | PASS_SOURCE_DEV_SCHEMA | 未迁移 legacy replay |
| F-724 | `observation_selector.py`; `retrospective_selector.py` | `test_formal_selector_rejects_range_lineage_without_fallback` | PASS_SOURCE | none |
| F-725 | `retrospective_selector.py`; `dataset_bridge.py` | exact range selector、genuine empty input、valid-empty/no-snapshot tests | PASS_SOURCE | none |
| F-726 | `dataset_bridge.py`; `observation_capture.py` | canonical-signal non-empty bridge 与 lineage preservation tests | PASS_SOURCE | economic sample 去重但保留全部 range lineage |
| F-727 | `retrospective_projection.py`; `observation_capture.py`; `dataset_bridge.py` | non-empty projection 与 observation PostgreSQL adapter contracts | PASS_SOURCE | 不调用 Selection/Inference |
| F-728 | `dataset_bridge.py`; `dataset_bridge_postgres.py` | 仅 candidate/fixed/executable 投影以及 persisted label readback tests | PASS_SOURCE | label 只消费 outcome calculation evidence |
| F-729 | `dataset_build.py`; `dataset_build_postgres.py`; `snapshot_writer.py`; `dataset_bridge_postgres.py` | retrospective build readback、selector-hash manifest、rollback-only non-empty SEALED snapshot | PASS_SOURCE_DEV_SCHEMA | 未执行真实 15 日 non-empty snapshot |
| F-730 | `dataset_bridge.py`; retrospective snapshot manifest | capability isolation人工复审 | PASS_SOURCE | 未发布任何用户可见 READY capability |
| F-731 | `dataset_bridge.py`; `dataset_bridge_postgres.py`; `repository.py` | valid-empty、terminal exact retry、retryable failure、expired lease、capture recovery、rollback-only full bridge | PASS_SOURCE_DEV_SCHEMA | none |
| F-732 | R4 migration；release schema v4 | 最新 migration 两轮 DEV apply/readback；managed/prerequisite verifier 零差异 | PASS_SOURCE_DEV_SCHEMA | 生产 migration 未授权、未执行 |
| F-733 | Phase 1R/Phase 1 retrospective/CAS changed files | ownership 与 protected-module zero-hit audit | PASS_SOURCE | 未执行生产 15 日 protected-count audit |
| F-734 | `composition.py`; bridge/outcome services | dependency/import/manual call-site audit | PASS_SOURCE | 无 package validator/health/asset/model 二次 gate |
| F-735 | planner/service/bridge positive path | no-extra-gate tests 与人工复审 | PASS_SOURCE | 无 latest-day、min-candidate、all-horizon-mature 或 copy-prod-to-DEV gate |
| F-736 | typed errors/reason codes in outcome/summary/bridge services | source-not-arrived、capacity、lineage conflict 与 rollback tests | PASS_SOURCE | none |
| F-737 | production composition 与 exact-ref loader 已接线 | 生产只读 readiness 确认 R3 batch 与两个 15/15 Program 完整 | PENDING_AUTHORIZATION | 生产缺少最新 R4 Recall 字段和 artifact-column functions；未获生产 DDL/DML 授权，outcome/summary/non-empty bridge E2E 未执行 |
| F-738 | revision transitions；operation/capture exact retry | unit/contract exact retry 与 source-correction identity tests | PARTIAL | exact retry 已验证；真实历史 source correction E2E 未执行，二者不得互相冒充 |
| F-739 | 本验收记录 | 分层 delivery state | PASS_REPORT | 报告本身不提升 F-737/F-738 状态 |

汇总：当前 source 独立复审、frozen contract 与最新 DEV schema 验证已通过；`F-737=PENDING_AUTHORIZATION`、`F-738=PARTIAL`。因此 R4 仍不能报告为完整历史业务验收通过，也不能在未获用户确认时提交或合入。

## 4. 验证证据

### 4.1 本地与直接依赖

```text
nox -s advisory_historical_range_backend
result: 172 passed, 3 skipped

Phase 1 formal parity/direct dependency matrix
result: 219 passed

release schema registry tests
result: 26 passed

backend/tests/advisory_historical_range/test_r4_dataset_bridge_postgres.py
result: 6 passed

python scripts/aistock_feature_workflow.py validate \
  --design docs/architecture/advisory_phase1r_r4_outcome_summary_phase1_bridge_f2_design_20260723.md \
  --tier F2
result: PASS, 40/40 design items, 0 warnings

changed Python compile: PASS, 49 files
changed-file Ruff: PASS
git diff --check: PASS
strict L0: PASS, 52 delivery files, 0 blocking findings
strict ownership: PASS, 52 mapped, 0 unmapped, 0 ambiguous
```

3 个 skip 是未注入显式 PostgreSQL/真实历史 batch root 的外部集成入口，不是捕获异常后转成成功。PostgreSQL adapter 的直接合同、frozen registry verifier 与最新 migration 的 DEV reapply/readback 均已显式执行；它们仍不等于 F-737 的真实 15 日业务 E2E。

### 4.2 DEV schema

> 当前状态：最新 migration SHA `a0f99cd4bca07dcf74ac5f74da0900e589d8cf6dc9d4a114135f9b9ab4e67142` 已在 DEV 连续执行两轮 apply/readback。两个 artifact-column function、两个 trigger 均精确存在，Outcome/Summary 缺失 artifact/JSON 的遗留行均为 0；release registry managed/prerequisite 均为 `COMPATIBLE` 且差异为 0。

DEV 目标只由 `F:\Dev\AIstock\.env` 解析，未在代码、日志或本记录保存 credential 值。已确认目标为现有 `aistock_dev`、PostgreSQL 16.10，environment contract hash 为 `336bd3ce579d6f9a313fb5c52e28196223c67f8b212b1d07fb9502442cbc6d4c`。

- migration 首次 apply 因 compatibility view 在新增 `range_signal_context_hash` 前引用该列而事务性失败；调整列创建顺序后重新执行成功，没有留下部分 schema。
- 临时 verifier 最初期待了错误 trigger 名，修正为实际两个 deferred closure trigger 后通过。
- 最终 catalog readback：9 个关键 columns、8 个 constraints、4 个 triggers、47 个 comments。
- exact migration reapply：通过；reapply 后只读 readback：通过。
- release schema v4：managed schema `COMPATIBLE`，prerequisite `COMPATIBLE`，managed differences `0`，分区月份 `2026-06`、`2026-07`、`2026-08`。

以上只证明现有 DEV schema 与 release registry 合同，不代表生产 DDL/DML 或历史业务数据流已执行。

### 4.3 DEV rollback-only non-empty bridge

使用同一 `.env` 解析的 DEV 连接，在单一事务中执行真实 PostgreSQL observation capture、label capture、retrospective dataset build、Batch-D materialize/full verify/promote/seal，并在 rollback 前执行 `SET CONSTRAINTS ALL IMMEDIATE`。结果为：

```text
status = passed_rollback_only
before_counts = (0, 0, 0, 0, 0)
during_counts = (2, 1, 2, 1, 1)
after_counts = (0, 0, 0, 0, 0)
observation_count = 1
label_count = 2
sealed_snapshot = true
```

`during_counts` 证明非空链实际写入并形成 SEALED snapshot；`after_counts` 由 rollback 后新连接独立回读，证明五类目标对象均零残留。该证据验证真实 DEV persistence/constraint/rollback 合同，但不使用 R3 权威 15 日业务事实，因此不能替代 F-737/F-738 历史业务 E2E。

### 4.4 Scope 与 ownership

最终 ownership scan 覆盖 55 个 changed/untracked 文件：52 个交付文件 mapped、0 ambiguous；根目录 `task_plan.md`、`findings.md`、`progress.md` 三个作为 planning 过程记录单独保留。对 52 个交付文件启用 `--fail-on-unmapped --fail-on-ambiguous` 后通过。Selection、Paper、Simulation、QE、Qlib、QMT 与前端均无 changed-file hit。

Catalog routing 将 `advisory.historical_range` 映射到 `l0 + advisory_historical_range_backend`，两者均已执行。旧 ownership 规则把 `dataset_build.py` 与 `dataset_build_postgres.py` 泛化映射到 `local_data/data_sync_autonomy_backend`，但该 plan 实际只编译和测试 Tushare/TDX ingestion，与 R4 dataset contract 无共享行为；因此未运行该无关 suite，而是运行 219 项 Phase 1 capture/label/dataset/snapshot/release-schema 精确直接依赖矩阵。

### 4.5 生产只读 readiness（2026-07-25）

- 连接只由 `.env` 的 `TDX_DB_*` 解析；目标 identity 为 production `aistock`、PostgreSQL 16.10，environment contract hash 为 `1db7f2f57f2b0c130e660d56835e963b919e9735fbe60fdd6ff7198bbb6ca767`。
- 权威 R3 batch `ahrb_dccde5770463663ecbde96fbe304cd26` 仍为 `COMPLETED`，`2026-07-01..2026-07-21` 共 15 个交易日；单 Alpha 和原生多 Alpha两个 range run 均为 `15/15 COMPLETE`。
- 生产 schema 尚无 `recall_denominator_set_hash`、`recall_denominator_evidence_json`，也尚无两个 outcome/summary artifact-column verification functions。真实 R4 E2E 必须先执行独立授权的生产 DDL，再执行独立授权的 R4 DML。
- 本次只读检查首次使用旧字段名查询时收到 `UndefinedColumn`，事务完整回滚；随后改为读取实际 JSON row contract。未执行生产 DDL/DML、服务控制或 runtime activation。

## 5. DESIGN-COMPLIANCE-001

| 检查项 | 源码结论 | 说明 |
|---|---|---|
| 禁止简化交付 | PASS_SOURCE | outcome、summary、真实 PostgreSQL bridge adapter、migration、registry 与恢复链均存在；没有 placeholder/mock-only production path。F-737 明确保持未完成，未把单测冒充 E2E |
| 禁止静默错误 | PASS_SOURCE | unknown/transient/terminal、capacity、lease、source drift、lineage conflict 均使用 typed status/reason；known unavailable 为 null/typed unavailable，不写伪 0 |
| 禁止业务语义漂移 | PASS_SOURCE | recommendation/executable、candidate/episode/list/range、T 日 context、PIT denominator、formal/retrospective selector 与 R3 immutable facts 分离 |
| 禁止私增门禁审批 | PASS_SOURCE | 未新增角色、RBAC、备份、canary、人工确认、latest-day、candidate-count、all-horizon、package health/admission 或生产数据复制门禁 |
| 禁止 synthetic Phase 0A | PASS_SOURCE_DEV_SCHEMA | range path 使用 `HISTORICAL_RANGE` tagged union；formal/range cross-pair、mixed selector/base snapshot 显式失败 |
| formal Phase 1 parity | PASS_SOURCE | formal OutcomeEngine、capture/build/snapshot schema/canonical bytes 与 selector path 的直接矩阵通过 |
| protected-module isolation | PASS_SOURCE | 受保护模块无 changed-file hit；composition 不导入其运行时验证/推理入口 |

上一轮人工复审结论已由 2026-07-24 formal source audit 取代。本轮审核发现并修复了 lineage identity、observation version、dataset selector、lineage payload 跨表 union 与 industry-at-T overlap 五类问题；完整独立复审和 routed matrix 未结束前，不声明无 blocking finding。

### 5.1 Formal source audit remediation checkpoint（2026-07-24）

- lineage identity 现在要求 formal 完整 program/binding/source identity；range 分支清空 Phase 0A/program binding 并要求 `source_run_id = range_day_run_id`。
- observation version 现在对 formal Selection evidence 与 range Selection-null 分支完整互斥，同时保留 range 合法的 runtime/HMM 研究上下文。
- formal dataset build 现在强制 `selector_policy_hash IS NULL`。
- lineage payload 新增 identity-aware PostgreSQL trigger，阻止 Phase 0A/range cross-tag payload。
- industry-at-T 查询不再 `LIMIT 1`；冲突 membership typed fail closed，等价重复按显式排序确定性读取。
- DEV preflight、apply/readback、exact reapply/readback 通过；release schema v4 managed/prerequisite 均 `COMPATIBLE` 且 differences 为 0。
- 本节是 remediation checkpoint，不提升 F-737/F-738，也不授权 commit、PR、生产 DDL/DML、runtime activation 或 merge。

## 6. 独立交付状态

```text
design_document = merged_pr_2665_commit_49a37e8fae0e4cc493804d3abb5598d883dbc0d5
source_code = implemented_uncommitted_locally_validated
dev_ddl = applied_verified_exact_reapplied
dev_dml = rollback_only_non_empty_bridge_verified_zero_residue; no_full_r4_business_e2e
production_ddl = not_authorized_not_applied
production_dml = not_authorized_not_applied
historical_single_alpha_15_day_r4_e2e = not_executed_authorization_required
historical_native_multi_alpha_15_day_r4_e2e = not_executed_authorization_required
historical_source_correction_e2e = not_executed
dependencies = noop_no_dependency_files_changed
service_restart = not_requested_not_performed
runtime_activation = not_requested_not_performed
commit = not_created
pull_request = not_created
merge = not_performed
close_sync = not_applicable
root_main_sync = not_performed
worktree_cleanup = not_performed
```

## 7. 当前结论与下一授权点

R4 源码、formal Phase 1 parity、DEV migration、release schema v4 和直接合同验证已形成完整证据；没有发现受保护模块业务写入、synthetic Phase 0A、silent fallback 或额外 gate。仍缺少两项不能由源码单测或 DEV schema 冒充的业务证据：

1. 使用持有 R3 权威 15 日事实的历史数据库，追加 R4 outcome、summary 与 non-empty retrospective snapshot，覆盖单 Alpha 和原生多 Alpha；
2. 在同一真实历史链上分别执行 terminal exact retry 与 source correction E2E，并证明两者身份和 revision 语义不同。

这些步骤会先对持有 R3 事实的生产数据库执行最新 R4 DDL，再执行 R4 DML；当前没有针对生产 DDL/DML 的明确授权，因此停在 `awaiting_production_ddl_dml_authorization`。提交、建 PR 和合入仍需用户另行确认；不得控制服务或执行 runtime activation。
