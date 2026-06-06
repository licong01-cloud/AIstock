# MCP Gateway Phase 6a 资源监控与无后台 token 防护完成报告

## 1. 范围与结论

- 分支：`codex/mcp-gateway-phase6-resource-monitor-20260604`
- Worktree：`F:\Dev\AIstock_worktrees\mcp-gateway-phase6-resource-monitor-20260604`
- 范围：Phase 6a 后端/脚本切片；不包含 Phase 6b UI 仪表盘。
- 结论：已实现结构化 startup summary、doctor 用户级 client config drift 扫描、process inventory 分类、fail-on-client-drift / fail-on-token-risk 开关、后端 dependency fail-fast 测试，并接入 Validation Center plan；G1 run_id `platform-mcp-gateway_20260604_150915_l2_mcp-gateway-phase6-resource-monitor_fa2da1e7_runner-validation__f9e1e23019`。
- 生产端口：未启动/停止/重启 `8001/3000/19080`。
- 生产 DB：本 PR 代码无 DB/DDL；本轮在用户确认后已单独执行 RA legacy cache cleanup，证据见 §7。

## 2. 改动清单

| 模块 | 文件 | 关键变化 |
| --- | --- | --- |
| gateway startup | `backend/mcp/gateway.py` | 新增 `manifest_version()`、`startup_summary_payload()`；`run_gateway()` 启动前输出结构化摘要；`self_check_payload(check_backend=True)` 对 backend dependency fail-fast。 |
| gateway CLI | `scripts/aistock_mcp_gateway.py` | 新增 `--startup-summary` 与 `--no-startup-summary`；运行 MCP transport 前默认向 stderr 输出 startup summary。 |
| doctor | `scripts/aistock_mcp_gateway_doctor.py` | 新增用户级 Codex config scan、legacy/full/modules drift 检查、process inventory 分类、`--fail-on-client-drift`、`--process-inventory`、`--fail-on-token-risk`。 |
| tests | `tests/mcp/test_mcp_gateway_cli.py` | 覆盖 startup summary、backend dependency failure、legacy user config fixture、full profile drift fail、process inventory 分类与 `bun` 子串误报回归。 |
| validation catalog | `noxfile.py`、`backend/services/validation/plan_catalog.py`、`tests/aistock_validation/catalog/test_plans.yaml`、`tests/aistock_validation/catalog/module_registry.yaml` | 新增 `mcp_gateway_phase6_resource_monitor` nox session、command allowlist、test plan 与 module registry 推荐项。 |
| docs | `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md` | 回填 §10 Phase 6a 验收矩阵行。 |

## 3. Phase 6 验收映射

| Phase 6 要求 | 实现引用 | 验证证据 | 状态 |
| --- | --- | --- | --- |
| gateway 启动输出结构化摘要 | `startup_summary_payload()`、`--startup-summary`、`run_gateway()` stderr summary | `test_gateway_cli_startup_summary_is_structured`；`python scripts/aistock_mcp_gateway.py --startup-summary --profile=lite` | done=true |
| doctor/smoke 检查 loopback、backend、manifest、client drift、full 默认风险 | `run_doctor()`、`_scan_client_configs()`、`self_check_payload()` | `python scripts/aistock_mcp_gateway_doctor.py --json --fail-on-client-drift` PASS；`client_config_drift.status=pass` | done=true |
| 静态检查禁止后台 LLM/daemon | 既有 static_no_llm + doctor guardrail | `static_no_llm.findings=[]`；`tests/mcp/test_mcp_gateway_cli.py` | done=true |
| 进程资源诊断说明和结构化输出 | `process_inventory_payload()`、`classify_process_record()`、`--process-inventory` | `F:\Dev\AIstock_artifacts\mcp_phase6_doctor_process_inventory_fixed_20260604_230557.json` | done=true |
| 无后端运行时明确 dependency failure | `self_check_payload(check_backend=True)` errors | `test_self_check_fails_fast_on_backend_dependency_failure` | done=true |

## 4. 测试结果关键行

```text
python -m pytest tests/mcp/test_mcp_gateway_cli.py -q -p no:cacheprovider
......... 9 passed

python -m nox -s mcp_gateway_phase6_resource_monitor
Session mcp_gateway_phase6_resource_monitor was successful

python -m pytest tests/mcp -q -p no:cacheprovider
.............................. 30 passed

python scripts/aistock_mcp_gateway.py --self-check --profile=lite --check-backend
status=pass; backend.dependency_status=healthy; manifest_tool_count=209; tool_count=6

python scripts/aistock_mcp_gateway_doctor.py --json --fail-on-client-drift
status=pass; client_config_drift.status=pass; static_no_llm.findings=[]
```

## 5. Live process inventory 解释

- 证据：`F:\Dev\AIstock_artifacts\mcp_phase6_doctor_process_inventory_fixed_20260604_230557.json`
- 当前 live inventory：`status=pass`，`process_inventory.status=warn`，relevant=69，findings=49。
- 分类摘要：`llm_or_daemon_token_risk=28`、`legacy_standalone_mcp=21`、`gateway_mcp=20`。
- 解释：用户级 `C:\Users\lc999\.codex\config.toml` 已收敛为 unified gateway 且 `client_config_drift.status=pass`；live legacy 进程来自修改前已启动的旧 Codex/Claude 客户端会话。未按本任务杀进程，需用户重启旧客户端窗口后自然释放。

## 6. Codex 启动错误处理说明

- 报错：`MCP client for aistock-validation failed to start ... initialize response`。
- 处理方式：本机用户级配置热修，不是正式 BUG/Issue 流程。
- 已备份：`C:\Users\lc999\.codex\config.toml.bak_aistock_mcp_20260604_225719`。
- 已切换：`aistock-validation -> scripts/aistock_mcp_gateway.py --profile=validation`，`aistock-qe -> --profile=qe`，`aistock-research -> --profile=research`，新增 `aistock-gateway-lite -> --profile=lite`。
- 验证：TOML parse pass；`validation/qe/research/lite` self-check pass；`aistock-validation` MCP initialize/list_tools pass。

## 7. RA legacy cache cleanup aftercare

- 用户确认后执行，属于 production DB cache data cleanup，不属于本 PR 代码 DDL。
- apply 证据：`F:\Dev\AIstock_artifacts\ra_mcp_legacy_cache_cleanup_apply_20260604_225126.json`
- 删除：legacy `assistant_mcp_tools` 104 行、legacy `assistant_mcp_servers` 8 行。
- after_commit：servers=9、tools=209、canonical_tool_rows=209、legacy candidates=0、missing_expected_tools=0。
- post-smoke：`F:\Dev\AIstock_artifacts\ra_mcp_legacy_cache_cleanup_post_smoke_20260604_225153.json`，RA readiness ready，MCP tools total=209，legacy alias canonicalize 到 `aistock-qe`。

## 8. G1/G2/G3

| Gate | 证据 | 状态 |
| --- | --- | --- |
| G1 Validation Runner | run_id `platform-mcp-gateway_20260604_150915_l2_mcp-gateway-phase6-resource-monitor_fa2da1e7_runner-validation__f9e1e23019`; job_id `valjob_20260604_150903_fa2da1e7`; return_code=0 | done=true |
aljob_20260604_150903_fa2da1e7; return_code=0 | done=true |
| G2 DESIGN-COMPLIANCE-001 | Phase 6 startup summary / doctor / static no LLM / process inventory / backend dependency failure 均映射到测试 | done=true |
| G3 docs 回填 | gateway doc §10 新增 `Phase 6 resource monitor / client drift` 行 | done=true |

## 9. Production gates

- `production_ddl_gate=noop`（本 PR 无 DDL；cleanup 是用户确认后的 cache DELETE，已单独留证）。
- `production_frontend_dependency_gate=noop`。
- `production_backend_dependency_gate=noop`。
- `frontend=noop`（Phase 6b UI 未纳入本 PR）。
- `backend runtime restart=not_required_for_tests`；合入 main 后若要让生产 runtime 载入新脚本，需要新 Codex/Claude 客户端会话或按用户确认进行后续重启/重载。

## 10. 遗留项

- Phase 6b：RA UI/API 展示 profile health、进程资源、最近 smoke、token-risk flags。
- 旧客户端会话：需要用户关闭/重开旧 Codex/Claude 窗口，释放修改前启动的 legacy standalone MCP 进程。
- 若需要将本机配置热修纳入正式流程，应单独提交 BUG/Issue；本轮已运行 issue workflow doctor，但未登记 BUG。

