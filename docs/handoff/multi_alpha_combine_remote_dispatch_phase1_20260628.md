# multi-alpha combine-backtest 远端派发 Phase 1 自审与验收矩阵

- 日期: 2026-06-28
- AIstock worktree: `F:\Dev\AIstock_worktrees\combine-remote-dispatch-phase1-20260628`
- AIstock branch: `feature/combine-remote-dispatch-phase1-20260628`
- RDAgent worktree: `F:\Dev\RD-Agent_worktrees\qe-workspace-artifact-store-20260628`
- RDAgent branch: `feature/qe-workspace-artifact-store-20260628`
- 权威设计: `docs/architecture/multi_alpha_combine_backtest_remote_dispatch_design_20260627.md`
- 本轮范围: 仅 Phase 1；未做 Phase 2/3。

## 范围自审

- 未改 AIstock `qe_evolution_*` 表/服务/路由、`qe_experiments`、QE loop 执行端逻辑。
- 未改执行层、Paper v2、MiniQMT、RA、前端、DB schema。
- 本地 subprocess 路径保留 `ShellPredBacktestExecutor`，仅在 `node_id` 解析为远端时分流到 `RemotePredBacktestExecutor`。
- RDAgent 仅新增 WAS 大文件 artifact 端点并在 `results_api_server.py` 挂载；未改既有 loop 执行逻辑。
- `production_ddl_gate=noop`; `production_frontend_dependency_gate=noop`; `production_backend_dependency_gate=noop`。
- 未启动/重启服务，未写生产 DB，未执行 DDL/DML。

## 设计验收矩阵回填

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 node_id→host 解析复用 `infra.compute_nodes` | `backend/services/multi_alpha/remote_dispatch.py` `ComputeNodeInfo` / `get_compute_node_info` / `is_remote_compute_node`; `backend/services/multi_alpha/combine_backtest.py` `_executor_for_node` | `backend/tests/test_multi_alpha_remote_dispatch.py::test_is_remote_compute_node_uses_compute_node_registry`; `test_service_uses_local_shell_executor_for_local_node`; `test_service_uses_remote_executor_for_remote_node` | verified | - |
| F-002 `RemotePredBacktestExecutor` 新增 | `backend/services/multi_alpha/remote_dispatch.py` `RemotePredBacktestExecutor` | `backend/tests/test_multi_alpha_remote_dispatch.py::test_remote_pred_backtest_executor_posts_loop_and_ingests_metrics` | verified | - |
| F-003 复用远端 RDAgent loop 端点 | `backend/services/multi_alpha/remote_dispatch.py` `_run_remote_loop`; `QEWorkspaceClient.create_and_run_loop` 接缝 | mock loop L2: `test_remote_pred_backtest_executor_posts_loop_and_ingests_metrics`; L4 live remote task `macb_f007_7738e811_ic_weighted_20260628` Loop1/Loop2 completed on `rdagent-node1` | verified | - |
| F-004 L1 常驻 / L2 WAS / L3 小文件通道 + 路径按节点解析 | `backend/services/multi_alpha/remote_dispatch.py` `_resolve_l2_artifact_path`, `_remote_paths`, `_sync_small_files`, `_remote_wsl_command` | `test_remote_pred_backtest_executor_requires_remote_paths`; `test_remote_pred_backtest_executor_rejects_local_paths`; `test_remote_wsl_command_uses_remote_paths_not_local_paths`; `test_remote_small_file_sync_posts_loop_scoped_files` | verified | - |
| F-005 capacity guard cross-source unification | Phase 2 per design section 4B; no Phase 1 code path | User-approved Phase 1 scope excludes capacity guard; verified no qe_evolution_* / qe_experiments runtime changes in this branch | not_applicable approved_by_user | approved_by_user: capacity guard remains Phase 2 scope per section 4B; not implemented in Phase 1. |
| F-006 本地路径零回归 | `backend/services/multi_alpha/combine_backtest.py` `_executor_for_node` local branch returns existing `ShellPredBacktestExecutor` | `backend/tests/test_multi_alpha_combine_backtest.py` + `backend/tests/test_multi_alpha_remote_dispatch.py`: 57 passed; local executor selection test passed | verified | - |
| F-007 结果回传一致性 | `backend/services/multi_alpha/remote_dispatch.py` writes returned enhanced JSON then reuses `ingest_enhanced_metrics` | L4 true remote run: baseline `macb_7738e811293948eb_20250601_20260310_20260627T191255096216Z/combined_ic_weighted` vs remote `macb_f007_7738e811_ic_weighted_20260628/Loop1`; 7 metrics all diff 0.0 within abs/rel `1e-6` | verified | - |
| F-008 失败显式不静默 | `backend/services/multi_alpha/remote_dispatch.py` reason codes for node/path/artifact/sync/remote failure/timeout/result invalid | `test_remote_loop_failure_includes_run_log_tail`; `test_remote_loop_timeout_is_loud`; `test_remote_small_file_sync_failure_is_loud`; WAS sha mismatch rejects | verified | - |
| F-009 WAS 远端端点 | `rdagent/app/api_endpoints/qe_workspace_artifacts_api.py`; `rdagent/app/results_api_server.py` router mount | `test/app/test_qe_workspace_artifacts_api.py` passed; live 215 `GET /api/v1/qe_workspace/artifacts/{64*0}` returns 200; 952333757-byte parquet sha `96a81665ee0c1f02247d36813d1e14f8f88ab94ce0c71d9e4501578e26c8ecbf` uploaded and verified | verified | - |
| F-010 WAS 本机 sync client | `backend/services/multi_alpha/remote_dispatch.py` `WorkspaceArtifactSyncClient` | `test_workspace_artifact_sync_head_hit_skips_upload`; `test_workspace_artifact_sync_uploads_and_verifies`; `test_workspace_artifact_sync_rejects_remote_size_mismatch`; `test_workspace_artifact_sync_rejects_invalid_upload_response` | verified | - |
| F-011 WAS 通用性 + 零拷贝装配 | RDAgent content-addressed `/artifacts/{sha256}`; AIstock `_remote_wsl_command` `ln -sfn artifact_path combined_factors_df.parquet` | unit tests + L4 `run.log` symlink command into loop cwd; Loop2 artifact manifest `uploaded=false` proves HEAD-hit de-dup | verified | - |

## WAS 跨仓库契约

- `HEAD /api/v1/qe_workspace/artifacts/{sha256}`: 通过 `X-Artifact-Exists`, `X-Artifact-Size`, `X-Artifact-Sha256`, `X-Artifact-Store-Root` 返回状态。HEAD 无响应体，因此补充 `GET` JSON 状态端点。
- `POST /api/v1/qe_workspace/artifacts/{sha256}`: 流式写入临时文件，服务端重算 sha256；不一致则返回 400 并删除临时文件。
- AIstock `WorkspaceArtifactSyncClient`: 分块算本地 sha256，先 HEAD 命中则跳过，未命中则流式 POST，上传后再次 HEAD 验证。
- 远端装配: L2 parquet 以 `artifact_store/{sha256}` 为权威路径，loop workspace 内用 symlink `combined_factors_df.parquet` 指向该 artifact。

## 验证命令

### AIstock

- `rtk python -m py_compile backend/services/multi_alpha/remote_dispatch.py backend/services/multi_alpha/combine_backtest.py backend/tests/test_multi_alpha_remote_dispatch.py` -> PASS
- `rtk python -m compileall -q backend/services/multi_alpha/remote_dispatch.py backend/services/multi_alpha/combine_backtest.py backend/tests/test_multi_alpha_remote_dispatch.py` -> PASS
- `rtk python -m ruff check backend/services/multi_alpha/combine_backtest.py backend/services/multi_alpha/remote_dispatch.py backend/tests/test_multi_alpha_remote_dispatch.py` -> PASS
- `rtk python -m pytest -q backend/tests/test_multi_alpha_remote_dispatch.py --cov=backend.services.multi_alpha.remote_dispatch --cov-report=term-missing --cov-branch --cov-fail-under=70` -> 21 passed, coverage 77.88%
- `rtk python -m pytest -q backend/tests/test_multi_alpha_combine_backtest.py backend/tests/test_multi_alpha_remote_dispatch.py` -> 57 passed
- `rtk python scripts/aistock_guardrail_scan.py --changed-only --fail-new-only --baseline-json tests/aistock_validation/guardrails_baseline_20260511.json --fail-on-severity P1 --output-json tmp/validation/guardrails/combine_remote_dispatch_phase1_changed.json` -> blocking=0
- `rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_combine_backtest_remote_dispatch_design_20260627.md --tier F1` -> PASS, design_items=11 matrix_rows=11
- `rtk python -m nox -s l0` -> PASS
- `rtk git diff --check` -> PASS

### RDAgent

- `rtk python -m py_compile rdagent/app/api_endpoints/qe_workspace_artifacts_api.py rdagent/app/results_api_server.py test/app/test_qe_workspace_artifacts_api.py` -> PASS
- `rtk python -m compileall -q rdagent/app/api_endpoints/qe_workspace_artifacts_api.py rdagent/app/results_api_server.py test/app/test_qe_workspace_artifacts_api.py` -> PASS
- `rtk python -m ruff check rdagent/app/api_endpoints/qe_workspace_artifacts_api.py test/app/test_qe_workspace_artifacts_api.py` -> PASS
- `rtk python -m pytest -q test/app/test_qe_workspace_artifacts_api.py` -> 3 passed
- `rtk python -m pytest -q test/app/test_qe_workspace_artifacts_api.py --cov=qe_workspace_artifacts_api_under_test --cov-report=term-missing --cov-branch --cov-fail-under=70` -> 3 passed, coverage 83.65%
- `rtk git diff --check` -> PASS

## L4 远端部署与真实 run 证据

### 远端 WAS 前置就绪(2026-06-28,阻塞解除)

- 按本轮前置条件,远端 RDAgent(192.168.50.215) WAS 已部署且 9000 已重启；本实现窗口未启动/重启远端服务。
- 验证(HTTP from 本机):`/health` 200, `/config` 200, **GET `/api/v1/qe_workspace/artifacts/{64*0}` → 200**(此前 404), `artifact_store_root=/home/lc999/projects/RD-Agent-main/qe_workspace_artifact_store`。
- **R-1 风险已化解**:运行 env 仅设 QE_WORKSPACE_WSL=/home/lc999/projects/RD-Agent-main/qe_workspace;QE_WORKSPACE_ROOT 未设→默认同为 repo/qe_workspace;两者一致,小文件与 loop cwd 同目录。
- 结论:F-007 远端前置(WAS 上线)已满足。

### R-1/R-2/R-3 验证

- R-1 workspace 根: 小文件经 `qe_file_sync` 上传到 `macb_probe_r1_nocd_20260628/Loop1/probe.txt`; 远端 loop 默认 cwd 实测为 `/home/lc999/projects/RD-Agent-main/qe_workspace/macb_probe_r1_nocd_20260628/Loop1`, 可直接读取 `probe.txt`。代码仍在 `_remote_wsl_command` 显式 `cd <workspace_base>/<task_id>/LoopN` 加固。
- R-2 symlink 落点: L4 run `run.log` 显示在 loop cwd 内执行 `ln -sfn /home/lc999/projects/RD-Agent-main/qe_workspace_artifact_store/96a81665ee0c1f02247d36813d1e14f8f88ab94ce0c71d9e4501578e26c8ecbf combined_factors_df.parquet` 后 `test -f combined_factors_df.parquet` 通过并完成回测。
- R-3 容差: F-007 对账采用 absolute tolerance `1e-6` 和 relative tolerance `1e-6`; 本次 7 项核心指标 diff 全为 `0.0`, 无需放宽容差。

### F-007 数值对账

- 本地基线: `macb_7738e811293948eb_20250601_20260310_20260627T191255096216Z/combined_ic_weighted`。
- 远端真实 run: node `rdagent-node1`, task `macb_f007_7738e811_ic_weighted_20260628`, loop `Loop1`; 幂等复跑 loop `Loop2`。
- WAS artifact: `combined_factors_df.parquet`, size `952333757`, sha256 `96a81665ee0c1f02247d36813d1e14f8f88ab94ce0c71d9e4501578e26c8ecbf`。首次 sync 上传成功; Loop1/Loop2 dispatch 的 `artifact_manifest.uploaded=false`, 证明 HEAD 命中跳过重复 1GB 上传。

| metric | local_baseline | remote_loop1 | diff | status |
|---|---:|---:|---:|---|
| cagr | 0.53318 | 0.53318 | 0.0 | PASS |
| max_drawdown | -0.086708 | -0.086708 | 0.0 | PASS |
| sharpe | 3.1063 | 3.1063 | 0.0 | PASS |
| calmar | 6.149144254278729 | 6.149144254278729 | 0.0 | PASS |
| topk_return_20 | 0.045095 | 0.045095 | 0.0 | PASS |
| topk_hit_rate_20 | 0.608634 | 0.608634 | 0.0 | PASS |
| turnover | 9.5448 | 9.5448 | 0.0 | PASS |

结论: F-007 真实远端 run 数值一致性已 verified; Phase 1 可以开 PR 请求 Tier2 终审,但不自行合并 main。
