# BUG-211 Phase 1 HMM / SQL Data Guard 验证记录

- 日期：2026-06-02
- BUG：BUG-211 / GitHub #568
- 分支：bug/BUG-211-paper-v2-miniqmt-unified-phase-1-hmm-sql-data-gu-20260602
- 目标集成分支：feature/paper-v2-miniqmt-unified-integration-20260602
- 生产门禁：production_ddl_gate=noop；production_backend_dependency_gate=noop；production_frontend_dependency_gate=noop
- 生产影响：未重启 8001/3000/19080；未写生产 DB；未提交 MiniQMT 委托；未切换 auto-run

## 设计方案与验收更新

- 设计文档 `docs/architecture/paper_v2_miniqmt_unified_autorun_design_20260602.md` 已包含 F2M-01 至 F2M-12 全功能验收矩阵、Phase 1-7 阶段 gate、DESIGN-COMPLIANCE-001 索引和禁止简化交付约束。
- 本次在 BUG-211 JSON 中补齐 Phase 1 closure requirements，把 F2M-03、HMM fail-fast、SQL/data guard、prepare-window reuse、BUG-181/193/199/202 继承证据、required verification 和生产 noop 门禁列为硬验收。
- 当前阶段只交付完整的 F2M-03 安全底座；F2M-01/02/04-12 不在本 issue 实现范围，但验收要求已经在设计方案中定义，后续阶段不得以最小闭环或 POC 替代。

## DESIGN-COMPLIANCE-001 阶段矩阵

| 设计项 | 实现位置 | 验证证据 | 状态 | 缺口/例外 |
|---|---|---|---|---|
| F2M-03 HMM readiness fail-fast | `backend/services/hmm_training_service.py`; `backend/services/selection_center/hmm_runtime.py` | `test_hmm_daily_coefficients.py`; `test_hmm_runtime.py`; `paper_v2_backend` | implemented/tested | UI 中文诊断属于后续 UI 阶段，不在 BUG-211 scope。 |
| F2M-03 malformed ts_code SQL 前拦截 | `backend/services/market_data/instrument_validator.py`; `qe_data_service.py`; `realtime_factor_data_loader.py`; `selection_artifact.py` | `test_bulk_kline_query_guard.py`; `test_qe_data_service_instrument_validation.py`; `test_realtime_factor_data_loader_validation.py` | implemented/tested | 无。 |
| F2M-03 大范围 SQL 分块与 source/correlation 日志 | `instrument_validator.py`; `qe_data_service.py`; `realtime_factor_data_loader.py`; `selection_artifact.py` | chunk tests cover `kline_daily_raw`、`adj_factor`、reference price | implemented/tested | 真实 DB Broken pipe 只能通过运行日志观察；本阶段保证 AIstock 侧有 source/correlation。 |
| F2M-03 prepare-window reuse / duplicated fanout 不重复 HMM/SQL | `backend/services/simulation_runtime/scheduler.py` | `test_selection_artifact_hmm_preflight.py`; lifecycle scheduler targeted tests | implemented/tested | account-group slot fanout 属 Phase 3。 |
| DC-06 HMM coefficient/artifact 两路径一致 | same as F2M-03 HMM | focused HMM + paper_v2_backend | implemented/tested | UI 展示待 Phase 6。 |
| DC-07 malformed ts_code 与大 SQL 统一 guard | same as SQL/data guard | focused data_service + paper_v2_l3 | implemented/tested | 无。 |
| DC-17 今天已修复问题不得回退 | BUG-211 closure + validation record | 本记录 11.4 继承矩阵 | evidence_recorded | 非本阶段功能保留后续重验。 |
| DC-18 完整开发验证前不合入 main | issue workflow + branch policy | 仅在 BUG 分支；目标为 integration branch | enforced | 本记录不是 main 合入授权。 |
| DC-19 所有 F2M 功能项有验收要求 | design doc 11.0 + BUG-211 closure | 本记录列出 `not_in_phase` 项 | acceptance_defined | F2M-01/02/04-12 后续阶段逐项实现。 |

## F2M 全功能阶段状态

| 功能项 | BUG-211 阶段状态 | 验收状态 |
|---|---|---|
| F2M-01 account group | not_in_phase | acceptance_defined_in_design |
| F2M-02 release/binding/plan | not_in_phase | acceptance_defined_in_design |
| F2M-03 selection readiness | in_phase | implemented/tested |
| F2M-04 unified vn.py execution | not_in_phase | acceptance_defined_in_design |
| F2M-05 order/preflight | not_in_phase | acceptance_defined_in_design |
| F2M-06 restart idempotency | not_in_phase | acceptance_defined_in_design |
| F2M-07 diagnostics | not_in_phase | acceptance_defined_in_design |
| F2M-08 holdings/cost | not_in_phase | acceptance_defined_in_design |
| F2M-09 operator UI | not_in_phase | acceptance_defined_in_design |
| F2M-10 Selection Center UI | not_in_phase | acceptance_defined_in_design |
| F2M-11 MiniQMT tables | not_in_phase | acceptance_defined_in_design |
| F2M-12 legacy migration | not_in_phase | acceptance_defined_in_design |

## 2026-06-02 已修复问题继承验收

| 已修复项 | BUG-211 继承方式 | 证据 | 状态 |
|---|---|---|---|
| BUG-181 Python 3.10 WSL live inference import | 本阶段未改 WSL worker；selection/live inference imports smoke 通过；未引入 `datetime.UTC` 到 touched runtime files。 | `python -c "from backend.services.strategy_package import live_inference; ..."` -> `imports_ok` | evidence_recorded |
| BUG-193 HMM preset/coefficient fail-fast | metadata-only preset 不再落默认 coefficient；preflight 覆盖 empty/missing/non-numeric/no coverage/mapping mismatch。 | `test_hmm_daily_coefficients.py`; `test_hmm_runtime.py`; `paper_v2_backend` | implemented/tested |
| BUG-199 deployable StrategyPackage release | 本阶段不改 release model；scheduler reuse 按 release/package/date/data_source key；paper_v2_backend 覆盖 strategy_package suite。 | `paper_v2_backend` -> 578 passed | preserved/tested_by_suite |
| BUG-202 RD-Agent/GPU env import | 本阶段不调用 live RD-Agent 节点；HMM/selection 失败保留 structured DataUnavailable/ArtifactGeneration context，不空 selection 成功。 | HMM failure tests; `qe_read_l3` -> 14 passed | preserved/evidence_recorded |
| workflow aftercare issues | 使用 BUG-211 worktree、task-card、finish gate；未从 root/main 开发；验证记录写入 issue-scoped history。 | `doctor` warning only; `resume` OK; final finish pending | in_progress |

## 验证命令结果

| 命令 | 结果 |
|---|---|
| `python scripts/aistock_issue_workflow.py doctor` | workflow_gate=warning；codegraph ready；client current。 |
| `python scripts/aistock_issue_workflow.py resume --bug-id BUG-211` | resumed active BUG-211 worktree。 |
| `pytest backend/tests/data_service/test_bulk_kline_query_guard.py backend/tests/selection_center/test_hmm_runtime.py backend/tests/simulation_runtime/test_selection_artifact_hmm_preflight.py -q -p no:cacheprovider` | 16 passed。 |
| `pytest backend/tests/test_hmm_daily_coefficients.py backend/tests/data_service/test_bulk_kline_query_guard.py backend/tests/data_service/test_qe_data_service_instrument_validation.py backend/tests/data_service/test_realtime_factor_data_loader_validation.py backend/tests/selection_center/test_hmm_runtime.py backend/tests/simulation_runtime/test_selection_artifact_hmm_preflight.py -q -p no:cacheprovider` | 32 passed。 |
| `python -m compileall ... selection/HMM/data/scheduler files` | passed。 |
| `pytest backend/tests/selection_center/test_runtime_selection.py backend/tests/selection_center/test_live_inference_preflight_wiring.py -q -p no:cacheprovider` | 53 passed。 |
| `pytest backend/tests/data_service -q -p no:cacheprovider` | 9 passed。 |
| scheduler targeted pytest | 6 passed。 |
| `python -m nox -s l0 paper_v2_backend` | l0 success；paper_v2_backend 578 passed, 1 skipped, 2 xfailed。 |
| `PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3` | success；data quality PASS with non-blocking legacy ledger consistency WARN；data_quality_deep 10 passed, 21 skipped。 |
| `python -m nox -s validation_module_registry_l0` | 8 passed；ownership scan mapped=12/unmapped=0/ambiguous=0。 |
| `QE_READ_L3_SKIP_UI=1 python -m nox -s qe_read_l3` | qe_read_backend 14 passed。 |
| `python -m nox -s validation_center_backend` | 330 passed；coverage line=79.9 branch=62.11 passed。 |
| `pytest backend/tests/simulation_runtime -q -p no:cacheprovider` | 61 passed, 4 failed；failures match known baseline outside BUG-211 scope (`test_strategy_runtime_release_*`, `test_tail_policy_*`, `test_execution_plan_compiler_*`). |
| `git diff --check` | passed after code changes. |

## 业务结果

- HMM `preset_A` metadata-only 不会再被内置默认 coefficient 掩盖；缺 date、空 coefficient、非 numeric、sector mapping 缺失均 fail-fast。
- `603819.S2026-06-01T...` 这类 timestamp-mixed 股票代码会在 SQL 前被拦截，不进入 `market.kline_daily_raw` 或 reference price 查询。
- 大股票池查询不再单次发送完整 symbol list；`kline_daily_raw`、`adj_factor`、reference price 均按 chunk 执行，并带 source/correlation 日志。
- 同一 release/package/date/data_source 同时 fanout 到 LocalSim + MiniQMT 时，scheduler 在一次 `run_once` 内只运行一次 selection；失败也缓存并复用，避免重复 HMM/SQL 工作。
- 已存在 authoritative selection artifact 时，prepare-window snapshot 复用 artifact，不重新触发 live preflight/regeneration。

## 残余风险

- `backend/tests/simulation_runtime` 全目录仍有 4 个历史/baseline 失败，未由 BUG-211 引入；本 issue 的 scheduler/selection 目标测试均通过。
- UI 中文诊断、MiniQMT 单/多策略统一 submit、account group、vn.py shared adapter、资金/持仓/成交 UI 属后续 Phase，不得因 BUG-211 通过而声称最终 unified 分支完成。
- 本记录不是生产 readiness 或 main 合入授权；最终 main 合入必须等 Phase 7 全量验收和用户确认。
