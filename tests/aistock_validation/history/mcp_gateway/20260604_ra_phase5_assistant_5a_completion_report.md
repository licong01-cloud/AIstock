# Research Assistant Phase 5a 后端切片完成报告

- 日期：2026-06-04
- Worktree：`F:\Dev\AIstock_worktrees\ra-phase5-assistant-integration-20260604`
- Branch：`codex/ra-phase5-assistant-integration-20260604`
- Base：`origin/main` @ `f88a0c76`
- 实现提交：`d8b55ed6 feat(research-assistant): consume gateway manifest catalog`
- Validation Runner：`valjob_20260604_093414_0985cde6`
- G1 run_id：`research-assistant_20260604_093524_l4_mcp-gateway-phase5-assistant_0985cde6_runner-validation__2d64d1d1ba`

> Phase 5 后端切片，非完整 Phase 5；UI/审计/E2E 在 5b 完成。

## 1. 5a 范围与结论

本轮只交付 Phase 5a 后端切片：A1 目录源收敛、`.mcp.json` canonical server_key 对齐、A2 只读 override、L2.5 external retrieval 自动只读、ReAct/preflight 后端闸门联动、后端测试与 Validation Center 后端断言。未交付 UI、Playwright B-08、任务审计 UI 展示和完整 Phase 5 completion，这些全部保留到 5b。

结论：5a 后端 G1/G2/G3 均通过；无生产端口、无 DB DDL、无生产 runtime 启停。

## 2. 改动清单与关键 diff

| 分组 | 文件 | 关键改动 |
|---|---|---|
| M1/A2 manifest 校准 | `backend/mcp/tool_manifest.py` | 新增 external L2.5 三个检索工具 read_only/direct override；新增 15 个 A2 只读 list/query/get override；保留 `external_research_save_evidence` 为 external_network/preflight；强化 read-only override 豁免必须有证据理由。 |
| A1 catalog adapter | `backend/services/research_assistant/mcp_catalog_sync.py` | `default_mcp_servers()`、`default_mcp_tools()`、`load_catalog()` 改为从 `.mcp.json` + `profiles.resolve_modules()` + `TOOL_MANIFEST` 派生；新增 canonical server catalog、legacy alias canonicalize、manifest->RA tool 映射。 |
| A1/A2 service 消费 | `backend/services/research_assistant/service.py` | `seed_catalogs()` 写 manifest-derived cache；`list_mcp_servers()`/`list_mcp_tools()`/`_react_tool_catalog_entries()`/`preflight_mcp_tool()` 统一走 manifest-derived resolver；DB overlay 只能收紧 status，不能降低 manifest 风险。 |
| 闸门联动 | `backend/services/research_assistant/execution.py` | action proposal preflight 使用 manifest tool policy；若 capability 本身要求 approval，即使 tool 是只读也保留 action-level approval gate；route candidate legacy server_key canonicalize。 |
| API 输出 | `backend/routers/research_assistant.py` | `/mcp/servers`、`/mcp/tools` 返回统一 catalog 视图，补充 profile/module/manifest risk/backend_endpoint/migration_state/response_budget 等字段。 |
| route 规范化 | `backend/services/research_assistant/domain_ontology.py` | DomainSpec 从 legacy server_key 迁到 `.mcp.json` canonical server_key，旧 key 仅通过 adapter alias 输入兼容。 |
| 验证接入 | `noxfile.py`、`backend/services/validation/plan_catalog.py`、`tests/aistock_validation/catalog/test_plans.yaml`、`tests/aistock_validation/catalog/module_registry.yaml` | 新增 `mcp_gateway_phase5_assistant` runner-enabled 后端切片计划；L4 plan 配置 workspace_scoped_design_gate、runtime/evidence/resource policy；模块推荐该 gate。 |
| 测试 | `backend/tests/research_assistant/test_ra_manifest_catalog_consumption.py` 等 | 新增 manifest catalog consumption 断言；更新 API/service/execution 测试到 canonical server 与 manifest risk/preflight 行为；扩展 MCP manifest 安全断言。 |

## 3. 字段映射表

| Gateway manifest | RA catalog / ToolCatalogEntry | 5a 映射规则 |
|---|---|---|
| `tool_name` | `tool_name` | 原样，全局唯一。 |
| `module` | `server_key` / `module` | `server_key` 由 `.mcp.json` profile 与 `profiles.resolve_modules()` 推导；旧 key 仅 alias canonicalize。 |
| `risk_level in {catalog, read_only}` | `risk_level=low`、`side_effect_level=read_only` | `assistant_usable=direct_or_catalog` 时 `requires_approval=False`，允许 ReAct `execute_read_only`。 |
| `risk_level=write_confirmed` | `risk_level=production_sensitive`、`side_effect_level=production_sensitive` | 必须 preflight/approval，不自动执行。 |
| `risk_level=long_running` | `risk_level=high`、`side_effect_level=high_cost_compute` | 必须 preflight/approval，不自动执行。 |
| `risk_level=production_adjacent` | `risk_level=high`、`side_effect_level=write_nonprod` | 必须 preflight/approval，不自动执行。 |
| `risk_level=external_network` | `risk_level=high`、`side_effect_level=draft_only` | 默认 preflight；例外仅 external L2.5 三个检索工具通过 manifest override 降为 read_only/direct。 |
| `assistant_usable=preflight_required` | `requires_approval=True` | 硬约束；RA direct preflight 和 action proposal 均不得降级自动执行。 |
| `requires_confirmation=True` | `required_confirmations` | 从 RA confirmation map 补确认文本；只读 direct 工具不带 action confirmation。 |
| `backend_endpoint`、`migration_state`、`response_budget`、`profile_tags` | API enrichment / preflight metadata | 不进入 RA core；作为 pure data 提供给 API/preflight/UI。 |

## 4. server_key canonical 映射与 route 规范化

| canonical server_key | profile | modules | legacy alias |
|---|---|---|---|
| `aistock-gateway-lite` | `lite` | `catalog` | - |
| `research-assistant` | `assistant` | `catalog`, `research_assistant` | - |
| `aistock-research` | `research` | `research` | - |
| `aistock-local-data` | `data` | `local_data` | - |
| `aistock-validation` | `validation` | `validation` | - |
| `aistock-qe` | `qe` | `qe_experiment`, `qe_archive`, `model_registry` | `aistock-qe-experiment`、`aistock-qe-archive`、`aistock-model-registry` |
| `aistock-factor` | `factor` | `factor_library`, `factor_metrics`, `factor_correlation` | `aistock-factor-library`、`aistock-factor-metrics`、`aistock-factor-correlation` |
| `aistock-trading-ops` | `trading_ops` | `strategy_governance`, `execution_policy` | `aistock-strategy-governance`、`aistock-execution-policy` |
| `aistock-external-research` | `external_research` | `external_research` | - |

Route 规范化：`mcp_catalog_sync.canonicalize_server_key()` 接受 legacy key；`service._resolve_mcp_catalog_tool()` 返回 `requested_server_key`、`canonical_server_key`、`legacy_server_alias`；`execution._route_candidates_from_payload()` 对 action proposal route payload 做 canonicalize，确保 persisted/LLM route 旧 key 不绕过目录。

## 5. A2 + external override 清单与理由

### 5.1 用户点名 A2 只读工具

| tool | override | 证据理由 |
|---|---|---|
| `qe_archive_list_runs` | read_only/direct | `backend/mcp/modules/qe_archive.py:93` GET `/runs`，repository 只返回 archived runs。 |
| `qe_archive_get_run_quality` | read_only/direct | `backend/mcp/modules/qe_archive.py:97` GET `/runs/{run_id}/quality`，仅返回 row-count quality checks。 |
| `qe_archive_query_run_leaderboard` | read_only/direct | `backend/mcp/modules/qe_archive.py:274` GET `/analytics/run-leaderboard`，repository 只查询 leaderboard rows。 |
| `local_data_list_sync_targets` | read_only/direct | `backend/mcp/modules/local_data.py:209` GET `/targets`，service 返回 `risk_level=read_only`。 |
| `local_data_list_schedules` | read_only/direct | `backend/mcp/modules/local_data.py:427` GET `/schedules`，service list_schedules 使用 read_only source。 |
| `list_validation_runs` | read_only/direct | `backend/mcp/modules/validation.py:58` GET `/runs`，validation router 只列 history runs。 |
| `get_validation_run` | read_only/direct | `backend/mcp/modules/validation.py:71` GET `/runs/{run_id}`，validation router 返回 run detail。 |

### 5.2 同轮修复的只读候选

| tool | override | 证据理由 |
|---|---|---|
| `local_data_get_sync_target` | read_only/direct | GET `/targets/{target_id}`，service 返回 read_only。 |
| `local_data_list_sync_attempts` | read_only/direct | GET `/sync-attempts`，service 返回 read_only。 |
| `local_data_get_schedule_defaults` | read_only/direct | GET `/schedules/defaults`，service 返回 read_only。 |
| `local_data_list_source_test_runs` | read_only/direct | GET `/testing/runs`，service list_source_test_runs 使用 read_only。 |
| `local_data_list_source_test_schedules` | read_only/direct | GET `/testing/schedules`，service list_source_test_schedules 使用 read_only。 |
| `local_data_get_repair_status` | read_only/direct | service `get_repair_status` 只汇总 overview/jobs/targets，返回 read_only。 |
| `qe_archive_list_backfill_runs` | read_only/direct | GET `/backfill/runs`，router 暴露 GET list endpoint。 |
| `qe_archive_get_backfill_run` | read_only/direct | GET `/backfill/runs/{backfill_run_id}`，router 暴露 GET detail endpoint。 |

### 5.3 L2.5 external 调整

| tool | 5a 分类 | 理由 |
|---|---|---|
| `external_research_search_web` | read_only/direct | L2.5 evidence-first read-only retrieval; results enter external.*/personal.topic.* as candidates, never direct conclusions。 |
| `external_research_search_papers` | read_only/direct | L2.5 evidence-first read-only retrieval; results enter external.*/personal.topic.* as candidates, never direct conclusions。 |
| `external_research_fetch_extract` | read_only/direct | L2.5 evidence-first read-only retrieval; results enter external.*/personal.topic.* as candidates, never direct conclusions。 |
| `external_research_save_evidence` | external_network/preflight | 写 evidence draft，保持 preflight，不自动执行。 |

## 6. 测试结果关键行

### 6.1 本地 nox gate

```text
python -m nox -s mcp_gateway_phase5_assistant
nox > git diff --check
nox > python -m compileall backend/mcp backend/services/research_assistant backend/routers/research_assistant.py scripts/aistock_mcp_gateway.py scripts/aistock_mcp_gateway_doctor.py
nox > python scripts/aistock_mcp_gateway.py --self-check --profile=lite
"status": "pass"
"profile": "lite"
"tool_count": 6
"manifest_tool_count": 209
nox > python scripts/aistock_mcp_gateway_doctor.py --json
"status": "pass"
"static_no_llm": {"findings": [], "status": "pass", ...}
nox > python -m pytest ... -q -p no:cacheprovider
119 passed in 64.59s (0:01:04)
nox > scripts/aistock_validation_catalog_integrity.py --fail-on-warning
"state": "passed", "error_count": 0, "warning_count": 0, "finding_count": 0
nox > scripts/aistock_module_ownership_scan.py --fail-on-unmapped --fail-on-ambiguous
Module ownership scan completed: files=18, mapped=18, unmapped=0, ambiguous=0
nox > Session mcp_gateway_phase5_assistant was successful in a minute.
```

### 6.2 Validation Center / controlled runner

```text
job_id=valjob_20260604_093414_0985cde6
status=passed
return_code=0
workspace_commit=d8b55ed6
run_id=research-assistant_20260604_093524_l4_mcp-gateway-phase5-assistant_0985cde6_runner-validation__2d64d1d1ba
runner_log=tmp/validation/runner/jobs/history/research-assistant/20260604_093524_l4_mcp-gateway-phase5-assistant_0985cde6_runner-runner-log.txt
```

## 7. G1/G2/G3 三闸门

### G1：Validation Center 绿灯

| 项 | 结果 |
|---|---|
| plan_key | `mcp_gateway_phase5_assistant` |
| expected_branch | `codex/ra-phase5-assistant-integration-20260604` |
| expected_commit | `d8b55ed6` |
| job_id | `valjob_20260604_093414_0985cde6` |
| exit_code | 0 |
| run_id | `research-assistant_20260604_093524_l4_mcp-gateway-phase5-assistant_0985cde6_runner-validation__2d64d1d1ba` |
| status | PASS |

### G2：DESIGN-COMPLIANCE-001 矩阵（5a 后端切片）

| design_item | implementation_refs | test_or_evidence | done | gap_or_exception |
|---|---|---|---|---|
| A1 单一事实源为 `TOOL_MANIFEST` | `mcp_catalog_sync.py`、`service.py` | `test_ra_catalog_matches_gateway_manifest_without_legacy_drift` | true | DB 表保留为 cache/overlay，不退役、无 DDL。 |
| server_key 从 `.mcp.json` + profiles 推导 | `mcp_catalog_sync.py` | `test_default_catalog_contains_manifest_tools_on_canonical_gateway_servers` | true | legacy key 仅作为 alias 输入兼容。 |
| manifest taxonomy 映射到 RA catalog/core pure data | `mcp_catalog_sync.py`、`service.py` | `test_a2_and_external_read_only_tools_map_to_auto_executable_entries` | true | RA core 不 import backend.mcp。 |
| A2 只读误标修复 | `tool_manifest.py` | `test_manifest_risk_no_write_as_readonly` | true | 每条 override 均有后端语义证据。 |
| L2.5 external retrieval 自动只读 | `tool_manifest.py`、`test_ra_manifest_catalog_consumption.py` | `test_external_research_l25_read_only_retrieval_stays_direct`、`test_read_only_tools_enter_execute_read_only_path` | true | `external_research_save_evidence` 仍 preflight。 |
| 写/确认/长任务/外网写不自动执行 | `tool_manifest.py`、`service.py`、`execution.py` | manifest write-token guard、RA preflight/action proposal tests | true | `assistant_usable=preflight_required` 不降级。 |
| list/preflight/ReAct 同源 | `service.py` | `test_ra_catalog_matches_gateway_manifest_without_legacy_drift`、API tests | true | 5b 再补前端展示。 |
| worker/chat 不自动 full profile 或 LLM CLI | `service.py`、doctor | `test_chat_turn_does_not_spawn_cli_or_full_profile`、doctor `static_no_llm.findings=[]` | true | 仅后端断言，UI/E2E 不在 5a。 |
| Validation Center plan 接入 | `test_plans.yaml`、`plan_catalog.py`、`noxfile.py` | G1 run_id | true | 后端切片 runner，无 live backend/frontend。 |
| Phase 5 范围不夸大 | 本报告、网关 doc §7/§10 | PR body 顶部标注 5a 后端切片 | true | 完整 Phase 5 completion 仅 5b 可声明。 |

### G3：文档回填

| 文档 | 行/内容 | commit |
|---|---|---|
| `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md` §7 Phase 5 | 新增 5a 后端切片说明，明确 UI/审计/E2E 留给 5b，5a 不声明完整 Phase 5 完成。 | `d8b55ed6` implementation row；本报告提交包含 doc 回填。 |
| `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md` §10 | 更新“智能助手读取统一 catalog”和“高风险工具 preflight/approval”两行到 `PASS_5A_BACKEND (d8b55ed6)`，记录 G1 run_id。 | `d8b55ed6` implementation row；本报告提交包含 doc 回填。 |

## 8. Production gates

| gate | status | evidence |
|---|---|---|
| `production_ddl_gate` | noop | 未修改 `backend/migrations/`、DB init schema 或生产 DB；Validation Runner `production_db_touched=false`。 |
| `production_frontend_dependency_gate` | noop | 5a 不改 frontend、不启动 3000/3011/3012。 |
| `production_backend_dependency_gate` | noop | 5a 不启动/停止/重启 8001；只跑无服务 backend tests、compileall、self-check/doctor（未 check backend）。 |

## 9. 停止条件记录

| 停止条件 | 状态 |
|---|---|
| external gating 与 L2.5 冲突无法判定 | 未触发；三检索工具已按 L2.5 恢复 read_only/direct。 |
| server_key canonicalize 无法兼容 legacy route/persisted 数据 | 未触发；legacy aliases 测试通过。 |
| 三源收敛冲突 | 未触发；DB 保留 cache/overlay，只能收紧风险。 |
| core 解耦被破坏 | 未触发；`test_core_no_adapter_import.py` 通过。 |
| 生产端口/DDL/runtime 红线 | 未触发；无 8001/3000/19080 启停，无 DB DDL。 |
| DESIGN-COMPLIANCE-001 任一项无法 done=true | 未触发；5a 后端切片矩阵全部 true。 |

## 10. 对 5b / 后续影响

- 5b 必须在 5a 合并后实现 UI + 任务审计增强 + Playwright B-08，并重新跑完整 `mcp_gateway_phase5_assistant` plan。
- 5b completion report 才能声明完整 Phase 5 完成；5a PR 只能声明后端切片完成。
- Phase 5 消费入口现在已基于 9 个 canonical server / 209 manifest tools，5b 前端应直接消费 `/api/v1/research-assistant/mcp/tools` 的 `catalog_source=gateway_manifest_derived_catalog`、`risk_distribution`、`profile_distribution`、`backend_health`、`recent_smoke` 字段。
- A3 stock_analysis 仍为后续方案，本轮未实现。