# Advisory Phase 1R R1 实施验收记录

## 1. 范围

本记录只验收父级 F2 设计中的 R1：contracts、状态/identity/hash、独立 CAS、additive migration 与 repositories。R2-R5 的 Selection 计算提取、候选生产、列表算法、executor、outcome 计算、Phase 1 bridge、API 和 UI 未在本批次实现，也不作为 R1 完成项申报。

父级设计：`docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md`。

## 2. 实施引用

| 范围 | implementation_refs | test_or_evidence | status |
|---|---|---|---|
| client/resolved request、Program、date-plan contracts | `backend/services/advisory_historical_range/models.py` | typed admitted component/weight/factor order/runtime input/lookback、per-component warmup adversarial tests | implemented_r1 |
| batch/Program/day/outcome/operation 状态机 | `models.py`、migration triggers | `test_state_machine.py`、disposable PostgreSQL positive/negative smoke | implemented_r1 |
| 多 Program 独立 run identity | `ResolvedHistoricalRangeRequestV1.range_run_id`、`repository.create_batch` | repository multi-Program idempotency test | implemented_r1 |
| caller key 与 resolved semantic 幂等 | `advisory_historical_range_request_key`、`repository._bind_request_key` | same semantic/different key convergence、same key/different semantic rejection | implemented_r1 |
| 500-row frozen date-plan materialization | `repository.materialize_day_plan_chunk` | 1,200-date 500/500/200 test、no calendar query assertion | implemented_r1 |
| attempt/lease/fencing persistence | day/operation tables、models、repository atomic transition/commit methods | final attempt 与状态同事务、expired attempt receipt 后 takeover、缺失 attempt DB 拒绝 | implemented_r1 |
| candidate/list/episode/outcome/summary append-only schema | migration、typed fact contracts、repository append/commit methods | content-hash closure、全量成功日 readback、WATCH/ENTER 必须来自 INCLUDED candidate | implemented_r1 |
| explicit repo-external CAS | `artifact_store.py`、repository mandatory store dependency | missing/relative/repo root rejection、full ref JSONB、递归 upstream readback、collision/tamper/path tests | implemented_r1 |
| legacy/shared module read-only boundary | Phase 1R-owned canonical serializer、service imports and migration mutation scope | AST import scan（含禁止 `advisory_phase0a`）、SQL mutation target scan | implemented_r1 |
| migration apply/exact reapply | `add_advisory_historical_range_phase1r_20260719.sql` | disposable PostgreSQL 16 apply and exact reapply | implemented_r1 |
| legal positive path | migration triggers and facts | disposable PostgreSQL repository `QUEUED -> RUNNING -> VALID_NO_CANDIDATE -> COMPLETED`，day/operation takeover | implemented_r1 |
| invalid negative paths | migration triggers | fake aggregate、missing attempt、无 candidate 的 WATCH、premature canonical result、terminal closure rejected | implemented_r1 |

## 3. 父级验收映射

| Design ID | R1 状态 | 说明 |
|---|---|---|
| F-920-F-922 | implemented_r1 | 多 Program、两种 Program 来源、单/原生多 Alpha typed identity 已闭合；真实 package resolver 属于 R2。 |
| F-923 | boundary_protected_r1 | R1 不 import 或调用 package health、asset/model/factor revalidation；admitted projection resolver 属于 R2。 |
| F-924-F-925 | implemented_r1 | 显式历史 date-plan、watermark、逐 Program/leg typed warmup contract 和无 latest 物化已闭合。 |
| F-926-F-927 | implemented_r1 | 五类状态机、自然键、resolved hash、request key alias 和 payload conflict 已实现。 |
| F-928 | foundation_implemented_r1 | attempt/lease/fencing schema、原子 final receipt、heartbeat 与 expired takeover 已实现；调度 executor 属于 R3。 |
| F-929 | foundation_implemented_r1 | 每 Program 独立 run/day/FK/事务边界已建立；并行 executor 属于 R3。 |
| F-930-F-931 | boundary_protected_r1 | R1 未接 Selection；共享 computation 提取属于 R2。 |
| F-932 | cas_foundation_implemented_r1 | range-owned CAS 与 candidate fact refs 已实现；真实 inference artifact producer 属于 R2。 |
| F-933-F-935 | deferred_by_approved_plan | PIT/HMM provider 与共享 list transition 分别属于 R2/R3。 |
| F-936 | foundation_implemented_r1 | first-day empty predecessor contract、previous day/list hash schema 和 DB trigger 已建立。 |
| F-937-F-939 | persistence_invariants_r1 | bounded list counts、WATCH/EXIT/episode 不变量已建立；实际 transition 算法属于 R3。 |
| F-940-F-942 | persistence_contracts_r1 | outcome/summary 版本与 maturity contracts 已建立；计算和模型能力展示属于 R4/R5。 |
| F-943-F-945 | implemented_r1 | 独立 additive schema、append-only facts、全字段 CAS ref JSONB、kind/range/day/source/upstream closure 和 exact readback 已实现。 |
| F-946 | operation_foundation_r1 | finite operation persistence 已实现；executor/resume/cancel 行为属于 R3。 |
| F-947-F-950 | deferred_by_approved_plan | API/UI/Phase 1 bridge 属于 R4/R5。 |
| F-951 | boundary_protected_r1 | 无 Selection、当前 Advisory、Paper、模拟盘、QE/Qlib/QMT import 或写入。 |
| F-952 | foundation_implemented_r1 | stable contract reason codes 已实现；业务 executor 日志属于 R3。 |
| F-953 | satisfied_r1 | R1 范围没有 placeholder、mock-only production path 或同步 range 冒充。 |
| F-954 | satisfied_r1 | migration 未连接 DEV/production；数据库连接只允许显式注入，DEV catalog test 只接受显式 DSN。 |
| F-955 | not_claimed_r1 | 真实 2-3 周单/多 Alpha E2E 属于 R2-R5，R1 不申报。 |
| F-956 | satisfied_r1 | code、disposable DB、DEV/production DDL、runtime 状态分开记录。 |

## 4. 验证结果

- `python -m pytest backend/tests/advisory_historical_range -q`：41 passed，2 skipped；skip 分别为未提供显式 DEV catalog DSN 和 disposable PostgreSQL DSN。
- `python -m ruff check backend/services/advisory_historical_range backend/tests/advisory_historical_range`：PASS。
- `python -m compileall -q backend/services/advisory_historical_range backend/tests/advisory_historical_range`：PASS。
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md --tier F2`：PASS，39/39，0 warnings。
- Disposable PostgreSQL 16：首次 migration PASS；携带真实数据 exact reapply PASS；13 张独立 Phase 1R 表可见。
- `AISTOCK_PHASE1R_TEST_DSN=<disposable>` 显式执行：1 passed；真实 repository batch/run/day 合法链闭合为 `COMPLETED/COMPLETED/VALID_NO_CANDIDATE`，exact retry 完成 candidate/list/episode/attempt 全量 readback。
- Disposable negative contracts：caller 伪造 completed aggregate、终态缺失 attempt、无 INCLUDED candidate 的 WATCH、终态 closure 缺失均被 DB/repository 明确拒绝。
- Disposable lease contracts：day 与 operation 的 expired attempt receipt、递增 fencing takeover 可达；heartbeat/未过期 takeover 仍按独立契约处理。

## 5. DESIGN-COMPLIANCE-001 逐项结论

| 检查项 | 实现/证据 | 结论 |
|---|---|---|
| 禁止简化交付 | R1 仅申报父设计允许的 contracts/DDL/repository/CAS；R2-R5 继续明确 deferred，没有用静态、mock-only 或子集冒充后续能力 | PASS |
| 禁止静默错误 | missing/tampered artifact、hash/identity mismatch、fake aggregate、missing attempt、invalid WATCH、row-version/fencing conflict 均显式失败 | PASS |
| 禁止改变业务逻辑 | 多 Program 独立、单 Alpha/原生多 Alpha、历史 DB/CAS、无 latest 依赖、bounded list、research-only/isolation 语义保持父设计一致 | PASS |
| 禁止私增门禁审批 | 没有 role/approval/authorization/backup/package re-admission/health preflight；保留项全部是数据一致性与状态事实约束 | PASS |

## 6. 生产与运行状态

```text
code_merge = pending_pr_merge
dev_ddl = not_executed
production_ddl = not_executed
service_restart = not_required
runtime_activation = none
selection_paper_simulation_qe_side_effect = none
```
