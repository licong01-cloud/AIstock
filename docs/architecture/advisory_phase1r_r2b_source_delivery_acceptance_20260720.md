# Advisory Phase 1R R2-B Source Delivery 验收记录

> 更新日期：2026-07-21  
> 当前状态：`real_dev_e2e_passed_source_merged`
> 详细设计：`docs/architecture/advisory_phase1r_r2b_historical_candidate_adapter_f2_design_20260720.md`

## 1. 验收边界

本记录只验收 R2-B 的 catalog planning、历史只读 StrategyPackage 推理适配、候选事实投影和外部 CAS 发布。它不申报 R3 日执行器、列表淘汰算法、episode、收益标签、价格区间模型、API、UI 或完整 Phase 1R 已完成。

R2-B 没有激活 scheduler 或运行入口，不写普通 Selection、Paper、模拟盘、QE/Qlib、QMT 或当前 Advisory 业务表。R1 migration 与 R2-B corrective migration 已在现有 DEV 数据库完成 apply/verify/exact-reapply；production DDL 未执行，服务未重启，runtime 未激活。

## 2. 已实现范围

| 范围 | 实现引用 | 状态 |
|---|---|---|
| 可恢复 catalog planning | `planning_service.py`、`catalog_planner.py`、`catalog_postgres.py`、`repository.py` | validated_dev |
| 逐 Program/Alpha/T 精确历史窗口 | `calendar_resolver.py`、`requirement_planner.py`、`models.py` | validated_dev |
| 单 Alpha/原生多 Alpha 历史 WSL 推理 | `selection_signal_preparation.py`、`multi_alpha_live.py`、`live_inference.py` | validated_dev |
| current Selection artifact parity | `selection_artifact.py`、`selection_computation.py` | validated_dev |
| ST/suspend/industry 历史只读 provider | `historical_selection_providers.py` | validated_dev |
| frozen HMM 正常/空候选证据闭合 | `hmm_runtime.py`、`selection_signal_preparation.py` | implemented_local |
| config-only HMM WAITING/resume 与 resolved binding-set | `requirement_planner.py`、`catalog_postgres.py`、`planning_service.py`、`candidate_producer.py` | implemented_local |
| INCLUDED/EXCLUDED 四阶段事实和 deterministic CAS | `candidate_projector.py`、`candidate_producer.py` | validated_dev |
| 普通 Selection/Paper/模拟盘隔离 | composition root、forbidden repository、AST isolation tests、DEV protected-relation count receipt | validated_dev |

## 3. 本轮缺陷修复

1. Phase 1R 阶段回执身份排除 observation time、本地模型路径和临时 workspace，并按规范化内容重算 stage hash。
2. 删除未被历史推理消费的 `reference_price` requirement，并同步修正 historical query contract。
3. 每个 Program、Alpha 腿和 T 冻结独立 window start；不再把区间首日 warmup start 复用于后续日期。
4. trading calendar revision 覆盖真实 warmup 范围；历史请求只使用 `<= end` 的完成水位，不吸收无关的全库最新交易日。
5. 历史 ST/suspend/industry provider 强制使用 `REPEATABLE READ, READ ONLY` session；current consumer 默认连接行为不变。
6. `st_risk` 和 `suspend` 的真实空集合允许形成零行 revision；其他必要模型输入仍禁止零行伪成功。
7. 实际 universe、market、fundamental、calendar read receipts 校验 dataset、partition、行数、content/window lineage 和 catalog membership。
8. HMM raw-empty 仍执行 exact frozen snapshot/model/coefficient preflight，不生成系数、不训练、不回退 latest。
9. 最终 VERIFY operation 已完成但 request seal 中断时，可通过幂等 `seal_completed_catalog` 恢复，不形成永久半完成 batch。
10. code release requirement 保存 git commit 和实际执行文件 SHA closure；dirty worktree 不构成运行门禁。
11. config-only HMM requirement 通过现有 append-only source availability ledger 消费显式 evidence bundle；缺失时等待，补齐后可在同一 requirement plan/batch 继续 DISCOVER/VERIFY。
12. seal 生成逐 Program、逐 T 的 `HMM_BINDING_SET` planning CAS，并把 ref/hash 写入 resolved frozen Program；base runtime config/hash 不改写。
13. candidate 执行按 T readback binding-set、catalog member 和 source revision ref 后生成 day-local snapshot profile；不调用 latest/trained-at 推测，也不写共享 Program/Selection 配置。
14. sealed HMM binding-set 文件暂时缺失显式映射为 `ADVISORY_HR_HMM_INPUT_UNAVAILABLE`；artifact tamper/hash conflict 不降级、不回退。
15. BUG-799 / PR `#2545` 修正合法 `unresolved=0` 被转换为 `-1`，并允许 `QUEUED -> QUEUED` 子运行聚合刷新；日期、日历、股票池、来源和 sealed catalog 校验保持不变。
16. BUG-802 / PR `#2549` 移除 StrategyPackage 投影对 Advisory runtime 模块的反向 import，使 WSL Python 3.10 在模型加载前不再因 `datetime.UTC` 导入失败；Selection 与 historical query contract hash 不变。
17. BUG-803 / PR `#2557` 将 Phase 1R 显式 task runtime root 传入 WSL allowlist；默认 Selection/Paper composition 不新增 root，也不改变现有消费者行为。
18. BUG-805 / PR `#2558` 保留单 Alpha window lineage 与多 Alpha per-leg window lineage；完整缺失仍兼容当前 Selection，部分 lineage 继续显式失败，不以 fallback 补造证据。

## 4. 验证回执

本轮已执行的变更模块与真实共享依赖矩阵：

```text
python -m pytest \
  backend/tests/advisory_historical_range \
  backend/tests/test_inference_engine_historical_readonly.py \
  backend/tests/test_inference_strict_scoring_alignment.py \
  backend/tests/scripts/test_strategy_package_live_inference.py \
  backend/tests/test_strategy_package_live_inference_window_patch.py \
  backend/tests/strategy_package/test_selection_signal_preparation.py \
  backend/tests/strategy_package/test_multi_alpha_signal_preparation.py \
  backend/tests/strategy_package/test_multi_alpha_live_selection.py \
  backend/tests/strategy_package/test_selection_computation.py \
  backend/tests/selection_center/test_runtime_selection.py \
  backend/tests/selection_center/test_hmm_runtime.py \
  backend/tests/simulation_runtime/test_strategy_package_selection_service.py \
  backend/tests/strategy_package/test_runtime_package_assets_batch2.py \
  backend/tests/paper_trading_v2/test_session.py \
  backend/tests/paper_trading_v2/test_day_runner.py -q

result: 307 passed, 2 skipped
```

该最终矩阵已包含逐日窗口、calendar closure、HMM config-only WAITING/resume/binding-set、HMM raw-empty、直接 projector，以及 Paper v2 `test_day_runner.py`/`test_session.py` 的正向依赖回归。

`git diff --check` 与变更 Python 模块 `compileall` 已通过。两个 skip 是显式 PostgreSQL/DEV DSN 未提供，不是被吞掉的失败。

- `python -m ruff check <R2-B changed modules and tests>`：PASS。
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_phase1r_r2b_historical_candidate_adapter_f2_design_20260720.md --tier F2`：PASS，`31/31`，`warnings=0`。

### 4.1 真实 DEV v4 回执

2026-07-21 使用仓库根 `.env` 的既有 source/DEV 连接、显式外部 CAS/runtime root 和历史交易日 `2026-07-03` 执行完整正向链路。source transaction 固定只读，DEV 写入仅限 `app.advisory_historical_range_*` planning 命名空间；没有创建新测试数据库，也没有执行 production DDL/DML。

```text
receipt = F:\Dev\AIstock_artifacts\advisory_phase1r_dev_validation_20260721\integration-v4\r2b_dev_validation_receipt.json
receipt_sha256 = 6a0a93f4de155e4d254d4d3a0ef75080727cad3d410e8be15e00ce856e65706d
batch_id = ahrb_d7888f3c58fdb6c749c9e7efcc10d050
catalog_operation_id = ahrop_031f039e3e0e011d181ac6cc5c00a80b
catalog = COMPLETED / VERIFY / resolved=13 / unresolved=0

native_multi_alpha:
  package_id = pkg_ma_8ec5e389fa2c5e484a1ac7e9
  top_k = 25
  candidate_count = 25
  included = 25
  excluded = 0
  source_revision_ref_count = 9
  candidate_artifact = candidate-artifacts/44a228046fcde1e0af3ddee2ca0dac7d75a38e42b04a2d2e4256e47614441331.json

single_alpha:
  package_id = pkg_378eb9c91e104c64935404e257e932ee
  top_k = 20
  candidate_count = 1200
  included = 20
  excluded = 1180
  source_revision_ref_count = 7
  candidate_artifact = candidate-artifacts/c02d0c54aacb2d709e9a1fddcfa84922041030b53747053712da43808218cd75.json
```

两个 Program 在同一 sealed request 中使用各自冻结的合法 `top_k`，均得到 `CANDIDATES_AVAILABLE` 和 candidate artifact v2。集成工作区的 6 个修复文件与 rebase 后 PR `#2545/#2549/#2557/#2558` 对应文件逐一 SHA-256 相同；四个 PR 的 GitHub CI 均通过。

隔离回执显示 17 个受保护关系前后计数完全一致，包括 StrategyPackage、普通 Selection artifact/DSE、`trading.rdagent_signal`、当前 Advisory、Paper v2 和模拟盘关系。Phase 1R 仅新增 1 个 batch、2 个 operation、3 个 operation attempt、1 个 request key 和 2 个 range run；candidate/day/list/episode/outcome/summary 表仍为 0，未伪造 R3 日成功状态。

## 5. DESIGN-COMPLIANCE-001

| 检查项 | 结论 | 说明 |
|---|---|---|
| 禁止简化版 | PASS_DEV | catalog DISCOVER/VERIFY/checkpoint/resume/seal、真实 WSL builder、四阶段候选和 CAS 已由单/原生多 Alpha DEV 正向链路证明，不使用 mock production path |
| 禁止静默错误 | PASS_DEV | 零输入、source drift、HMM 不一致、stage conflict、CAS/readback conflict 均显式失败；真实 v4 未出现 warning-success 或空结果降级 |
| 禁止业务语义偏移 | PASS_DEV | 两个 Program 使用独立冻结 top-k/window；current save/ensure/top-k/exclusions 默认行为通过共享依赖回归 |
| 禁止额外门禁审批 | PASS | 无 role、approval、authorization、backup、package health/re-admission 或 latest-day gate |
| 模块隔离 | PASS_DEV | 17 个受保护关系前后计数一致；R2-B 只写 planning rows 和显式 Phase 1R CAS，不触碰 Selection/Paper/模拟盘/QE/QMT 业务写路径 |

## 6. 剩余交付状态

R2-B 的真实 DEV 功能验收已闭合，但源码合入、production DDL 和后续 R3-R5 仍是独立状态：

| Design ID | 状态 | 原因 |
|---|---|---|
| F-977 单 Alpha 真实 DEV E2E | passed_dev_merged | `2026-07-03` 生成 1200 个完整候选事实、20 INCLUDED 与可读回 candidate artifact v2 |
| F-978 原生多 Alpha 真实 DEV E2E | passed_dev_merged | 同日生成 25 个候选、25 INCLUDED 与 9 个 source revision refs；两腿独立 window lineage 保留 |
| F-982 DEV/production release | separated | DEV migration 已 apply/verify/exact-reapply；production DDL 待具体目标授权，服务未重启、runtime 未激活 |
| F-987 planning 到 candidate 正向回执 | passed_dev_merged | planning 自动完成 13/13 requirements 并依次生成两个 Program candidate CAS，无审批或 latest-day gate |
| source PR merge | completed | PR `#2545/#2549/#2557/#2558` 已分别合入为 `73eb3599`、`7cd2c2dc`、`6b700eb9`、`c410b8c2` |
| R3-R5/API/UI/模型预测 | not_in_r2b | 仍按父设计后续阶段实施，不由本回执提前申报 |

## 7. 交付状态

```text
source_code = implemented_and_locally_reviewed
design_compliance = pass_dev
dev_ddl_dml = migration_applied_verified_exact_reapplied; planning_rows_only
production_ddl_dml = not_executed
real_dev_single_alpha_e2e = passed
real_dev_multi_alpha_e2e = passed
service_restart = not_requested
runtime_activation = none
merge_state = bug_prs_merged
bug_prs = 2545,2549,2557,2558
```

因此当前结论是：F-977、F-978 和 F-987 已在真实 DEV 集成源码闭包上通过，四个修复 PR 已合入主线，R2-B source delivery 完成。production DDL 未授权、未执行，服务未重启，R3 日执行器与完整 Phase 1R 仍未完成。
