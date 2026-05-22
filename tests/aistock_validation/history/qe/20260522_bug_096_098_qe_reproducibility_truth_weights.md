# BUG-096/097/098 QE 可复现性、执行真值与因子权重入仓验证记录

日期：2026-05-22
工作目录：`F:\Dev\AIstock_worktrees\bug-096-098-qe-reproducibility-truth-weights-20260522`
分支：`bug/BUG-096-098-qe-reproducibility-truth-weights-20260522`
关联 GitHub Issues：`#141`、`#142`、`#143`
治理规则：`DESIGN-COMPLIANCE-001`、`PROD-DDL-001`

## 1. 本次修复范围

本次修复只覆盖 QE / QE Archive / QE Template / QE MCP / QE 相关前端入口：

- `#141`：训练型 QE loop 必须有显式固定 seed；缺失 seed 的历史记录在归档侧标记为不可复现审计数据。
- `#142`：custom_evo / 统一执行路径生成 canonical execution manifest，并在请求配置与生成 artifact 不一致时 fail-fast。
- `#143`：QE Archive 结构化沉淀 factor importance / factor weight percentage，并提供 API 与 MCP 查询入口，查询结果带 seed、HMM/no-HMM、收益风险和 raw artifact context。

未触碰范围：

- 未修改 Paper v2、MiniQMT、`vn.py`、`trading_core` 运行边界。
- 未启动、重启或连接生产 `8001` / `3000`。
- 未写生产 DB，未执行任何生产 DDL。

## 2. DESIGN-COMPLIANCE-001 矩阵

| Issue / 设计项 | 实现位置 | 验证证据 | 状态 | 缺口或例外 |
|---|---|---|---|---|
| `#141` 训练型 loop 在调度前必须有固定 seed | `backend/services/quantevolver/seed_contract.py`；`backend/routers/quantevolver_evolution.py`；`backend/services/quantevolver/qe_evolution_service.py`；`backend/routers/quantevolver.py`；`backend/services/dispatch_service.py` | `backend/tests/unified_engine/test_backtest_executor.py::test_full_train_strict_seed_contract_rejects_missing_seed`；`backend/tests/unified_engine/test_qe_config_truth.py::test_strategy_params_runtime_metadata_is_hoisted_to_runtime_flags`；`backend/tests/qe_templates/test_template_validator.py` | 完成 | 无 |
| `#141` seed 写入请求、任务配置、生成配置、runner 和归档元数据 | `backend/services/quantevolver/experiment_config.py`；`backend/services/quantevolver/experiment_config_builders.py`；`backend/services/quantevolver/config_composer.py`；`scripts/qrun_limit.py`；`scripts/qrun_limit_minute.py`；`backend/services/qe_archive/payload_extractor.py` | `backend/tests/unified_engine/test_qe_config_truth.py::test_score_weighted_v2_filters_archive_seed_metadata_from_strategy_kwargs`；`backend/tests/test_qe_archive_repository_static.py::test_payload_extractor_builds_archive_payload` | 完成 | GPU 低层仍可能有硬件/库非确定性，已通过 `deterministic_flags`、版本信息和 `verification_status` 记录，不伪装为已验证完全可复现。 |
| `#141` custom_evo create / append / rerun / retry / template / MCP 入口缺 seed fail-fast | `backend/routers/quantevolver_evolution.py`；`backend/services/qe_templates/validator.py`；`backend/services/qe_templates/materializer.py`；`scripts/aistock_qe_experiment_mcp_server.py` | `backend/tests/qe_templates/test_template_validator.py`；`backend/tests/test_aistock_qe_mcp_servers.py`；目标 pytest 通过 | 完成 | 无 |
| `#141` 历史 seedless loop 不得当作 fixed seed | `backend/services/qe_archive/payload_extractor.py` | `backend/tests/test_qe_archive_repository_static.py::test_payload_extractor_marks_seedless_payload_audit_only` | 完成 | 历史记录不回填假 seed；只标记 `seed_policy=unset_legacy`、`reproducibility_level=audit_only`、`verification_status=not_reproducible`。 |
| `#142` 生成 canonical execution manifest | `backend/services/quantevolver/execution_manifest.py`；`backend/services/quantevolver/executors/backtest.py`；`backend/services/quantevolver/qe_evolution_service.py` | `backend/tests/unified_engine/test_backtest_executor.py`；`backend/tests/unified_engine/test_qe_config_truth.py`；compileall | 完成 | 无 |
| `#142` 请求配置与生成 artifact 必须校验 seed、factor、strategy、capacity、label horizon、execution context | `backend/services/quantevolver/execution_manifest.py`；`backend/services/quantevolver/config_composer.py` | 目标 pytest 131 passed；execution manifest mismatch 逻辑 fail-fast；`git diff --check` 通过 | 完成 | `conf.yaml` 无标准 Qlib task/port_analysis 结构时，只有无 seed 的非训练审计场景允许 `not_applicable`；训练型 fixed-seed 场景会 fail-fast。 |
| `#142` mismatch details 可通过执行结果/loop config/API/UI 透传 | `backend/services/quantevolver/executors/backtest.py`；`backend/services/quantevolver/qe_evolution_service.py`；`frontend/src/app/quantevolver/evolution/page.tsx` | compileall；Next build；后端失败路径会把异常写入 `agent_analysis` | 完成 | 未启动真实远端 QE worker 做端到端长训练；本次为配置/生成/归档契约修复，真实 worker 长任务应在后续 issue 验证阶段执行。 |
| `#143` 新 loop 自动提取并写入结构化 factor importance | `backend/services/qe_archive/payload_extractor.py`；`backend/services/qe_archive/archive_service.py`；`backend/services/qe_archive/repository.py`；既有 `backend/db/init_qe_archive_schema.py` | `backend/tests/test_qe_archive_repository_static.py::test_archive_service_writes_factor_importance_records`；`test_payload_extractor_reads_enhanced_feature_importance_gain_pct` | 完成 | `qe_archive.run_factor_importance` 表已存在于 `origin/main` 的 schema bootstrap，本次未新增 DDL。 |
| `#143` 支持 `pytorch_correlation`、`feature_importance`、`weight_pct`、`gain_pct` 等来源 | `backend/services/qe_archive/payload_extractor.py` | `backend/tests/test_qe_archive_repository_static.py::test_payload_extractor_builds_archive_payload`；`test_payload_extractor_reads_enhanced_feature_importance_gain_pct` | 完成 | 无 |
| `#143` 查询入口返回 seed / HMM / no-HMM / 收益风险 / raw artifact context | `backend/routers/qe_archive.py`；`backend/services/qe_archive/repository.py`；`scripts/aistock_qe_archive_mcp_server.py` | `backend/tests/test_qe_archive_repository_static.py::test_factor_importance_query_includes_repro_config_source_and_return_context`；`test_factor_importance_stability_query_includes_seed_hmm_and_return_risk_aggregation` | 完成 | 无 |
| `#143` 支持跨 seed 稳定性聚合 | `backend/services/qe_archive/repository.py`；`backend/routers/qe_archive.py`；`scripts/aistock_qe_archive_mcp_server.py` | `backend/tests/test_qe_archive_repository_static.py::test_factor_importance_stability_query_includes_seed_hmm_and_return_risk_aggregation` | 完成 | 当前聚合为 read-model 查询，不执行自动因子替换决策。 |
| 前端入口必须要求 seed，而不是让后端才报错 | `frontend/src/app/quantevolver/compose/page.tsx`；`frontend/src/app/quantevolver/evolution/page.tsx`；`frontend/src/app/rdagent/dispatch/components/TaskCreatePanel.tsx` | `cd frontend && npm ci`；`cd frontend && rtk npm build` | 完成 | build 存在历史 `react-hooks/exhaustive-deps` warning，不是本次新增阻塞。 |

## 3. 自动化验证

已执行并通过：

```powershell
python -m compileall backend/routers/dispatch.py backend/routers/qe_archive.py backend/routers/qe_templates.py backend/routers/quantevolver.py backend/routers/quantevolver_evolution.py backend/services/dispatch_service.py backend/services/qe_archive backend/services/qe_templates backend/services/quantevolver scripts/aistock_qe_archive_mcp_server.py scripts/aistock_qe_experiment_mcp_server.py scripts/qrun_limit.py scripts/qrun_limit_minute.py
rtk pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_backtest_executor.py backend/tests/qe_templates/test_template_validator.py backend/tests/test_qe_archive_repository_static.py -q -p no:cacheprovider
rtk pytest backend/tests/unified_engine/test_label_horizon.py backend/tests/test_aistock_qe_mcp_servers.py -q -p no:cacheprovider
cd frontend; rtk npm ci
cd frontend; rtk npm build
python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
rtk git diff --check
```

结果：

- compileall：通过。
- 后端目标回归：`131 passed`。
- label horizon + QE MCP 补充回归：`30 passed`。
- 前端生产构建：通过；只输出历史 `react-hooks/exhaustive-deps` warning。
- guardrail changed-only：`blocking=0`；保留 3 个 P2 `ALGO-COMPLEXITY-001` 提示，位置为 `backend/services/qe_archive/repository.py:1555`、`backend/services/qe_archive/repository.py:1659`、`backend/services/qe_archive/repository.py:1906`。这些查询/写入均有 filter、limit 或 page size 约束，未扫描 raw workspace artifact，作为非阻塞复杂度复查项记录。
- `git diff --check`：通过。

## 4. 生产影响与 DDL Gate

- 生产后端 `8001`：未触碰、未重启。
- 生产前端 `3000`：未触碰、未重启。
- 生产 DB：未连接、未写入、未执行 DDL。
- 本次运行了专用 worktree 内 `frontend/npm ci`，生成的 `frontend/node_modules/` 与 `frontend/.next/` 均为 gitignored 本地验证产物，不进入提交。
- `production_ddl_gate=noop`。原因：本次 runtime code 依赖的 `qe_archive.run_factor_importance` 已存在于 `origin/main` 的 `backend/db/init_qe_archive_schema.py`，本次没有新增或修改 schema / migration / DB bootstrap。

## 5. 合入 main 条件判断

当前分支具备进入代码评审的基本条件：

1. 根目录 `F:\Dev\AIstock` 未作为开发目录使用；修复在专用 worktree + 专用分支内完成。
2. 已覆盖 `#141/#142/#143` 的主要验收点，没有将 seed 缺失、manifest mismatch、factor importance 查询降级为 POC 或 mock-only。
3. 后端目标测试、前端 build、guardrail P1、diff check 均通过。
4. 没有生产端口和生产 DB 影响。

仍需用户明确确认后才允许执行后续动作：

- 是否将本分支 push 到远端。
- 是否创建 PR。
- 是否合入 `main`。
- 是否关闭 GitHub Issues `#141/#142/#143`。

在未获得用户确认前，本分支只提交到独立 worktree，不合入 `main`，不污染项目根目录。
