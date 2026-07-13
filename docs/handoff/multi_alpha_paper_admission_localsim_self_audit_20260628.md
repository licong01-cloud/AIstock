# Multi-Alpha LocalSim Paper Admission 自审

日期：2026-06-28
分支：feature/multi-alpha-localsim-paper-admission-20260628
worktree：F:\Dev\AIstock_worktrees\multi-alpha-localsim-paper-admission-20260628

## 范围与边界

- 已实现 C1-C5：新增 manifest 外部准入表迁移、LocalSim dry-run validator、API 端点、venue-aware eligibility、paper create broker_backend 透传。
- 未改 `PaperPortfolio.package_id` 单 package 主契约；MULTI_ALPHA 仍作为单个 parent package 进入 Paper v2。
- SINGLE_ALPHA 路径通过 `alpha_mode != multi_alpha` 早退；测试覆盖 single-alpha paper create 不需要 admission。
- 未触碰 `research-assistant`、前端、MiniQMT 执行层、scheduler、broker、qrun、外部 qlib。
- 未启动/重启服务；未连接/写入生产 DB；未执行 DDL/DML。

## Ground Truth

- `SelectionCenterService.list_selectable_packages()` 使用 `asset_eligibility_service.summarize(record)` 并过滤 `eligible=False`。
- `SelectionCenterService._prepare_package_runtime_configs()` 使用 `asset_eligibility_service.require_eligible(record)`。
- `PaperTradingV2PortfolioService.create_portfolio()` 使用 `require_eligible(record, broker_backend=broker_backend)`。
- 因此 dry-run admission 命中 `local_sim` 后同时放行 Selection + LocalSim Paper；`minqmt_sim` 因 key 不命中继续 fail-closed。

## 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/asset_eligibility.py`; `backend/services/selection_center/service.py`; `backend/services/paper_trading_v2/service.py` | ground-truth 读码；`test_local_sim_dry_run_writes_admission_and_is_deterministic_for_topk_variants` 验证 Selection selectable + LocalSim eligibility | verified | - |
| F-002 | `backend/migrations/strategy_pkg_multi_alpha_paper_admission_20260628.sql`; rollback; `backend/services/strategy_package/multi_alpha_paper_admission.py` | F2 validate PASS；DDL 含唯一键、CHECK、index、table/column comments | verified | 生产 DDL 未执行，进入 gate |
| F-003 | `backend/services/strategy_package/multi_alpha_paper_dry_run.py` | dry-run 测试走真实 `StrategyPackageSelectionArtifactService` / `StrategyPackageRuntime` / `TargetPositionEngine` / `RebalanceEngine` 链路 | verified | - |
| F-004 | `backend/services/strategy_package/multi_alpha_paper_dry_run.py` | 同 manifest/runtime_config/trade_date 重跑 `dry_run_run_id`、admission_id、artifact_shas 一致 | verified | - |
| F-005 | `backend/routers/strategy_packages.py` | router success + not-applicable loud error 测试 | verified | - |
| F-006 | `backend/services/strategy_package/asset_eligibility.py` | `local_sim` admission PASS；`minqmt_sim` 仍含 `multi_alpha_runtime_not_validated_until_dry_run` | verified | - |
| F-007 | `backend/services/paper_trading_v2/service.py` | `create_portfolio(broker_backend=local_sim, data_source=DB_HISTORICAL)` 成功；MiniQMT 失败 | verified | - |
| F-008 | `asset_eligibility.py`; tests | `test_single_alpha_paper_create_still_passes_without_admission_reader`; 既有 paper/selection/strategy_package 回归全绿 | verified | - |
| F-009 | dry-run/eligibility/router typed errors | missing seed / child sha mismatch / label window insufficient / single-alpha / unsupported broker 均断言 reason_code；失败不写 admission | verified | - |
| F-010 | migration files only | 未执行生产 DDL；`production_ddl_gate=pending` | verified | - |

## 验证记录

- `rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_paper_admission_localsim_f2_design_20260628.md --tier F2` → PASS。
- `rtk python -m py_compile backend/services/strategy_package/multi_alpha_paper_admission.py backend/services/strategy_package/multi_alpha_paper_dry_run.py backend/services/strategy_package/asset_eligibility.py backend/services/paper_trading_v2/service.py backend/routers/strategy_packages.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py` → PASS。
- `rtk python -m ruff check backend/services/strategy_package/multi_alpha_paper_admission.py backend/services/strategy_package/multi_alpha_paper_dry_run.py backend/services/strategy_package/asset_eligibility.py backend/services/paper_trading_v2/service.py backend/routers/strategy_packages.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py` → PASS。
- `rtk python -m pytest -q backend/tests/strategy_package/test_multi_alpha_paper_admission.py backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_live_selection.py backend/tests/selection_center/test_runtime_selection.py backend/tests/paper_trading_v2/test_portfolio_broker_backend.py backend/tests/strategy_package/test_enable_paper_router_409.py` → 99 passed。
- `rtk python -m nox -s paper_v2_backend` → 717 passed, 1 skipped, 2 xfailed。
- `rtk python -m nox -s l0` → PASS。
- `rtk git diff --check` → PASS。

## 生产门禁

- `production_ddl_gate=pending`：新增 `strategy_pkg.multi_alpha_paper_admission`，只提交迁移文件，未执行生产 DDL。
- `production_frontend_dependency_gate=noop`：未改前端/前端依赖。
- `production_backend_dependency_gate=noop`：未改 Python 依赖。
- 需要用户合并后执行 DDL gate 并重启后端，runtime 才会加载新 endpoint/service。
