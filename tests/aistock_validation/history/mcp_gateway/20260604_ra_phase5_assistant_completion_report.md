# Research Assistant Phase 5 统一 MCP manifest/catalog 集成完成报告（5b + full）

- 日期：2026-06-04
- Worktree：`F:\Dev\AIstock_worktrees\ra-phase5b-assistant-ui-audit-20260604`
- Branch：`codex/ra-phase5b-assistant-ui-audit-20260604`
- 实现提交：`f37ba7b8 feat(research-assistant): add phase5 mcp ui audit`
- 自审加固提交：`43da6fbc test(research-assistant): harden phase5 mcp audit gate`
- 最终代码 HEAD：`43da6fbcf6f403c3d64f693448615ab00456b701`
- Validation Runner：`valjob_20260604_111245_1096055f`
- G1 run_id：`research-assistant_20260604_111424_l4_mcp-gateway-phase5-assistant_1096055f_runner-validation__cf8153448f`

> 结论：Phase 5a 后端收敛已由 `24021029` 完成；本轮 5b 完成 UI、任务审计展示、Playwright B-08 与完整 `mcp_gateway_phase5_assistant` gate。至此可声明 MCP Gateway Phase 5（Research Assistant 消费统一 MCP manifest/catalog）完整完成。A3 `stock_analysis` 仍为已声明后续方案，不纳入本轮。

## 1. 范围与结论

本轮 5b 在 5a 合并后的主线之上补齐：

- UI：`/research-assistant/mcp-tools` 读取统一 catalog，展示 catalog source、manifest 工具数、profile/risk 分布、backend health/recent smoke 显式状态、工具搜索/过滤、profile recommendation、preflight 和 approval pending。
- 审计：`assistant_preflight_mcp_tool` 写入 `assistant_mcp_tool_events` 的 `response_json.audit`、`result_card_json`、`artifact_refs`，任务事件 payload 增加 `mcp_preflight_audit`。
- E2E：Playwright B-08 覆盖工具搜索、profile recommendation、preflight、approval pending、evidence refs、audit ledger，并禁止触碰 `8001/3000/19080`。
- 验证：`mcp_gateway_phase5_assistant` 从 5a backend-only 升级为完整 Phase 5 gate，包含 backend pytest、self-check/doctor、catalog integrity、ownership、frontend lint/build、Playwright。

结论：G1/G2/G3 均通过；无 DB DDL、无生产端口启停、无生产 runtime 改动、无依赖变更。

## 2. 改动清单与关键 diff

| 分组 | 文件 | 关键改动 |
|---|---|---|
| 5b preflight 审计 | `backend/services/research_assistant/service.py` | `preflight_mcp_tool()` 生成 `audit_payload`，记录 catalog_source/profile/module/server/tool/preflight/approval/evidence；写 `result_card_json` 和 `artifact_refs`；任务事件 payload 增加 `mcp_preflight_audit`。自审加固把 backend health/recent smoke 理由从 5a 文案改为完整 Phase 5 文案。 |
| 5b 审计回归 | `backend/tests/research_assistant/test_phase5_mcp_audit.py` | 覆盖 `external_research_save_evidence` approval_required 审计事件，以及 `qe_archive_query_run_leaderboard` read_only passed 审计事件。 |
| API 类型 | `frontend/src/lib/research-assistant/api.ts` | 新增 `AssistantMcpToolPage`、`AssistantMcpToolEvent`，`mcpTools()` 返回 page-level manifest/risk/profile/health/smoke 元数据，新增 `mcpToolEvents()`。 |
| UI | `frontend/src/app/research-assistant/mcp-tools/page.tsx`、`research-assistant.css` | 重构 MCP tools 页面，展示统一 catalog summary、risk/profile distribution、canonical server map、工具搜索/过滤、preflight card、evidence refs、audit event ledger 和 local-data capability 视图。 |
| Playwright B-08 | `frontend/tests/research-assistant/phase5-mcp-gateway-ui.spec.ts` | mock API 只用于前端交互隔离；断言 catalog source、manifest count、profile/risk、工具搜索、preflight、approval pending、evidence/audit 展示；自审加固 forbiddenRequests 包含 `:8001`、`:3000`、`:19080`。 |
| Validation gate | `noxfile.py` | `mcp_gateway_phase5_assistant` 升级为完整 gate，新增 frontend lint/build 和 Playwright，固定 dev env 到 `8012/3011`。 |
| Validation catalog | `tests/aistock_validation/catalog/test_plans.yaml` | plan title/scope 从 5a backend slice 更新为 full Phase 5；`requires_frontend=true`、允许 `3011/3012`、禁止 `8001/3000/19080`、required evidence 增加 completion report 与 Playwright B-08。 |

## 3. 字段映射与收敛策略最终选择

| Gateway manifest / catalog | Research Assistant 字段 | 最终规则 |
|---|---|---|
| `TOOL_MANIFEST` | RA catalog/cache/tool page | 单一事实源；`assistant_mcp_tools` 仅作派生 cache/runtime overlay，不退役、无 DDL。 |
| `.mcp.json` + `profiles.resolve_modules()` | `server_key` / `profile` / `module` | canonical server_key 从 `.mcp.json` 推导；旧 key 只作为 alias canonicalize，不再作为主目录源。 |
| `risk_level in {catalog, read_only}` + `assistant_usable=direct_or_catalog` | `risk_level=low`、`side_effect_level=read_only`、`requires_approval=False` | 可进入 ReAct `execute_read_only`，用于 grounding。 |
| `assistant_usable=preflight_required` | `requires_approval=True` | 硬约束；无论 UI/API/DB overlay 均不得降级为自动执行。 |
| `requires_confirmation=True` | `required_confirmations` / `missing_confirmations` | 写/确认/长任务/生产邻近工具只产 preflight/approval pending，不能自动执行。 |
| `backend_endpoint` / `migration_state` / `response_budget` / `profile_tags` | API enrichment / UI detail / audit evidence | 作为 pure data 经 adapter/service 层喂给 UI 和 preflight；core 继续不 import `backend.mcp`。 |
| `assistant_mcp_tool_events` | audit ledger | 记录 preflight/result card/artifact refs，UI 读取 `/mcp/tool-events` 展示最近事件。 |

## 4. server_key canonical 映射

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

Route 兼容策略延续 5a：旧 server_key 可 canonicalize 到主目录；persisted/LLM route 旧 key 不会绕过 manifest risk/preflight。

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

| tool | 分类 | 理由 |
|---|---|---|
| `external_research_search_web` | read_only/direct | L2.5 evidence-first read-only retrieval; results enter `external.*` / `personal.topic.*` as candidates, never direct conclusions。 |
| `external_research_search_papers` | read_only/direct | L2.5 evidence-first read-only retrieval; results enter `external.*` / `personal.topic.*` as candidates, never direct conclusions。 |
| `external_research_fetch_extract` | read_only/direct | L2.5 evidence-first read-only retrieval; results enter `external.*` / `personal.topic.*` as candidates, never direct conclusions。 |
| `external_research_save_evidence` | external_network/preflight | 写 evidence draft，保持 preflight，不自动执行；5b UI 展示 approval pending 与 evidence refs。 |

## 6. 测试结果关键行

### 6.1 本地 nox gate（最终代码 HEAD `43da6fbc`）

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
"static_no_llm": {"findings": [], "status": "pass", ...}
nox > python -m pytest ... -q -p no:cacheprovider
121 passed in 58.09s
nox > scripts/aistock_validation_catalog_integrity.py --fail-on-warning
"state": "passed", "error_count": 0, "warning_count": 0, "finding_count": 0, "production_8001_touched": false, "production_db_touched": false
nox > scripts/aistock_module_ownership_scan.py --fail-on-unmapped --fail-on-ambiguous
Module ownership scan completed: files=25, mapped=25, unmapped=0, ambiguous=0
nox > npm run lint
completed with existing react-hooks warnings outside this Phase 5 patch
nox > npm run build
✓ Compiled successfully
nox > npx playwright test tests/research-assistant/phase5-mcp-gateway-ui.spec.ts --project chromium
1 passed (7.8s)
nox > Session mcp_gateway_phase5_assistant was successful in 2 minutes.
```

### 6.2 Validation Center / controlled runner

```text
job_id=valjob_20260604_111245_1096055f
status=passed
return_code=0
workspace=F:\Dev\AIstock_worktrees\ra-phase5b-assistant-ui-audit-20260604
branch=codex/ra-phase5b-assistant-ui-audit-20260604
commit=43da6fbc
full_commit=43da6fbcf6f403c3d64f693448615ab00456b701
dirty_files=0
production_8001_touched=False
arbitrary_shell_allowed=False
run_id=research-assistant_20260604_111424_l4_mcp-gateway-phase5-assistant_1096055f_runner-validation__cf8153448f
runner_log=tmp/validation/runner/jobs/history/research-assistant/20260604_111424_l4_mcp-gateway-phase5-assistant_1096055f_runner-runner-log.txt
```

## 7. G1/G2/G3 三闸门

### G1：Validation Center 绿灯

| 项 | 结果 |
|---|---|
| plan_key | `mcp_gateway_phase5_assistant` |
| expected_branch | `codex/ra-phase5b-assistant-ui-audit-20260604` |
| expected_commit | `43da6fbc` |
| job_id | `valjob_20260604_111245_1096055f` |
| exit_code | 0 |
| run_id | `research-assistant_20260604_111424_l4_mcp-gateway-phase5-assistant_1096055f_runner-validation__cf8153448f` |
| status | PASS |

### G2：DESIGN-COMPLIANCE-001 矩阵

| design_item | implementation_refs | test_or_evidence | done | gap_or_exception |
|---|---|---|---|---|
| A1 单一事实源为 `TOOL_MANIFEST` | `mcp_catalog_sync.py`、`service.py` | 5a `test_ra_catalog_matches_gateway_manifest_without_legacy_drift` + full gate 121 passed | true | DB 表保留为 cache/overlay，不退役、无 DDL。 |
| server_key 从 `.mcp.json` + profiles 推导 | `mcp_catalog_sync.py`、`domain_ontology.py` | 5a canonical tests + full gate | true | legacy key 仅 alias 输入兼容。 |
| manifest taxonomy 映射到 RA catalog/core pure data | `mcp_catalog_sync.py`、`service.py` | 5a mapping tests、`test_core_no_adapter_import.py` | true | RA core 不 import `backend.mcp`。 |
| A2 只读误标修复 | `tool_manifest.py` | `test_manifest_risk_no_write_as_readonly`、5a override 清单 | true | 写/确认工具未放松。 |
| L2.5 external retrieval 自动只读 | `tool_manifest.py`、RA catalog consumption tests | `test_external_research_l25_read_only_retrieval_stays_direct` | true | `external_research_save_evidence` 仍 preflight。 |
| 写/确认/长任务/外网写不自动执行 | `tool_manifest.py`、`service.py`、`execution.py` | manifest guard、preflight/action proposal tests | true | `assistant_usable=preflight_required` 不降级。 |
| list/preflight/ReAct 同源 | `service.py` | RA catalog/API tests、G1 full gate | true | 5b UI 使用同一 `/mcp/tools` 与 `/mcp/preflight`。 |
| preflight 任务审计记录 profile/tool/preflight/approval/evidence | `service.py`、`test_phase5_mcp_audit.py` | `test_mcp_preflight_event_records_profile_approval_and_evidence_audit` | true | 写入 `mcp_preflight_audit` 与 `assistant_mcp_tool_events`。 |
| UI 展示 profile/tool count/risk/backend health/recent smoke | `mcp-tools/page.tsx` | Playwright B-08 | true | health/smoke 为显式 `not_checked/not_run`，不伪造 pass。 |
| UI 工具搜索/filter/profile recommendation | `mcp-tools/page.tsx` | Playwright B-08 search/filter/profile 断言 | true | compact list `include_schema=false`。 |
| UI preflight/approval pending/evidence 展示 | `mcp-tools/page.tsx` | Playwright B-08 preflight/evidence/audit 断言 | true | approval pending 不触发执行。 |
| worker/chat 不自动 full profile 或 LLM CLI | doctor + worker isolation tests | doctor `static_no_llm.findings=[]`、`test_worker_tool_isolation.py` | true | gateway 不后台启动 Claude/Codex/Bun/LLM CLI。 |
| Validation Center plan 接入完整 Phase 5 | `test_plans.yaml`、`noxfile.py` | G1 run_id、catalog integrity、ownership | true | requires_frontend=true，允许 3011/3012，禁止 8001/3000/19080。 |
| G3 文档与完成报告回填 | gateway doc §7/§10、RA blueprint §12、本报告 | doc diff + 本报告 | true | A3 stock_analysis 保持后续方案。 |

### G3：文档回填

| 文档 | 行/内容 | commit |
|---|---|---|
| `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md` §7 Phase 5 | 增加 5b/full Phase 5 完成说明：UI、任务审计、Playwright B-08 与 G1 run_id。 | `43da6fbc` |
| `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md` §10 | “智能助手读取统一 catalog”和“高风险工具 preflight/approval”更新为 `PASS_PHASE5_FULL`，记录 nox/runner/Playwright 证据。 | `43da6fbc` |
| `docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md` §12 | 新增 “MCP Gateway Phase 5 manifest/catalog + UI/audit” 可追溯行。 | `43da6fbc` |

## 8. Production gates

| gate | status | evidence |
|---|---|---|
| `production_ddl_gate` | noop | 未修改 `backend/migrations/`、`backend/db/init_*.py` 或生产 DB；Validation Runner `production_db_touched=false`。 |
| `production_frontend_dependency_gate` | noop | 未修改 `frontend/package.json` / lockfile；未启动/停止生产 `3000`。 |
| `production_backend_dependency_gate` | noop | 未修改 Python/Node 依赖；未启动/停止/重启生产 `8001`，self-check/doctor 未连接 live backend。 |

## 9. 停止条件与自审记录

| 停止条件 | 状态 |
|---|---|
| external gating 与 L2.5 冲突无法判定 | 未触发；三检索工具保持 read_only/direct，`save_evidence` 保持 preflight。 |
| server_key canonicalize 导致 route/persisted 数据无法兼容 | 未触发；legacy alias 兼容由 5a 测试覆盖。 |
| 三源收敛、core 解耦、生产端口/DDL 红线触发 | 未触发；core import gate 通过，无 DDL，无 8001/3000/19080。 |
| DESIGN-COMPLIANCE-001 任一项无法 done=true | 未触发；矩阵全部 true。 |
| UI 只做 mock、不接真实 API | 未触发；页面通过真实 `researchAssistantApi` 调 `/mcp/tools`、`/mcp/preflight`、`/mcp/tool-events`；Playwright mock 仅隔离 E2E，不替代 backend pytest/runner。 |

自审发现并修复一处验收加固点：Playwright forbidden request 扫描原本只覆盖 `3000/19080`，已在 `43da6fbc` 加入 `8001`；同时把 service 返回的 health/smoke 文案从 “5a backend slice” 改成完整 Phase 5 口径。

## 10. 后续影响

- A3 `stock_analysis` 仍为后续方案，本轮未实现也不宣称完成。
- standalone MCP 完整退役仍按 gateway doc Phase 7/§11 后续执行；本轮只完成 RA 对统一 catalog 的消费、UI 和审计链路。
- 生产激活仍需要用户按正常部署流程重启/部署前后端；本轮没有触碰生产 runtime。
