# 本地数据管理 MCP Gateway 开发验收记录（2026-05-24）

> 分支：`feature/local-data-mcp-gateway-20260524`  
> Worktree：`F:\Dev\AIstock_worktrees\local-data-mcp-gateway-20260524`  
> 对应设计方案：`docs/architecture/local_data_management_mcp_gateway_design_20260523.md`  
> 验收范围：`LDM-MCP-001` 至 `LDM-MCP-028`  
> 生产影响：未启动、停止或重启生产 backend `8001` / frontend `3000`；未写生产数据库。  
> DDL 结论：本次实现未新增数据库表、字段、索引、约束或 migration，`production_ddl_gate=noop`。

## 1. 交付内容

| 类别 | 文件 | 说明 |
| --- | --- | --- |
| 后端 facade | `backend/services/local_data_management.py` | 统一封装本地数据状态、任务、计划、告警、同步目标、修复计划和确认执行。 |
| 后端路由 | `backend/routers/local_data.py`、`backend/main.py` | 新增 `/api/v1/local-data/*` 稳定 facade，并挂载到主应用。 |
| MCP Gateway | `backend/mcp/modules/local_data.py`、`backend/mcp/profiles.py` | 新增 `local_data` Gateway module 和 profile；工具只调用 facade，不直接连 DB 或调度器。 |
| 助手能力 | `backend/services/research_assistant/service.py` | 注册 `local_data_management` capability、MCP 工具目录、Prompt Tree、长期记忆和轻量图谱 seed。 |
| 前端 UI | `frontend/src/app/research-assistant/chat/page.tsx`、`frontend/src/app/research-assistant/workbench/page.tsx`、`frontend/src/app/research-assistant/mcp-tools/page.tsx`、`frontend/src/lib/research-assistant/api.ts` | 对话页、工作台和 MCP 工具页增加人类可读的本地数据检查、计划、确认、执行、复查卡片。 |
| 测试 | `backend/tests/test_local_data_management_facade.py`、`backend/tests/mcp/test_local_data_module.py`、相关 Research Assistant 与 Playwright 测试 | 覆盖 facade、MCP 工具目录、确认口令、Prompt/Memory/Graph、前端无 raw JSON 主视图。 |

## 2. 验证命令与结果

| 序号 | 命令 | 结果 |
| --- | --- | --- |
| 1 | `python -m pytest backend/tests/test_local_data_management_facade.py backend/tests/mcp/test_local_data_module.py backend/tests/mcp/test_profiles_registry_gateway.py backend/tests/research_assistant/test_service.py backend/tests/research_assistant/test_api.py -q -p no:cacheprovider` | 通过，`82 passed in 8.43s`。 |
| 2 | `python -m compileall backend/services/local_data_management.py backend/routers/local_data.py backend/mcp/modules/local_data.py backend/services/research_assistant` | 通过。 |
| 3 | `npm run lint`（frontend） | 通过；仅保留既有其他模块 `react-hooks/exhaustive-deps` warnings。 |
| 4 | `npm exec tsc -- --noEmit --incremental false`（frontend） | 通过。 |
| 5 | `npx playwright test tests/research-assistant/research-assistant.spec.ts --project=chromium`（frontend） | 通过，`4 passed`；Playwright 临时 3012 服务已结束，未占用 3000。 |
| 6 | `npm run build`（frontend） | 通过；仅保留既有 lint warnings。 |
| 7 | `rg -n "get_conn|psycopg|subprocess|scheduler|backend\.ingestion|backend\.routers|backend\.services|requests" backend/mcp/modules/local_data.py` | 无输出，MCP module 没有直接 DB、调度器、脚本或后端 service import。 |
| 8 | `git diff --check` | 通过，无空白错误。 |

## 3. 逐项验收矩阵

| 编号 | 结论 | 代码位置 | 测试命令/证据 | 备注 |
| --- | --- | --- | --- | --- |
| LDM-MCP-001 | 通过 | `backend/mcp/modules/local_data.py`、`backend/mcp/profiles.py` | 后端测试命令 1；`test_local_data_module_registers_design_tool_catalog`、`test_gateway_loads_local_data_tools` | `local_data` profile 可加载，注册 47 个工具。 |
| LDM-MCP-002 | 通过 | `backend/services/research_assistant/service.py` | 后端测试命令 1；`test_local_data_management_catalog_prompt_memory_and_cards` | `local_data_management` capability 包含风险、入口、提示词和 MCP 工具引用。 |
| LDM-MCP-003 | 通过 | `backend/services/local_data_management.py` | 后端测试命令 1；`test_overview_returns_human_summary_and_business_impact` | overview 返回中文摘要、状态、影响模块和待处理项。 |
| LDM-MCP-004 | 通过 | `backend/services/local_data_management.py`、`backend/routers/local_data.py` | 后端测试命令 1；MCP 工具路径覆盖 `local_data_get_dataset_status` | dataset 状态包含 audit/cache/physical/last_job 结构。 |
| LDM-MCP-005 | 通过 | `backend/services/local_data_management.py`、`backend/mcp/modules/local_data.py` | 后端测试命令 1；MCP 路径覆盖 `/gaps`，facade 调用既有 gaps API | 按 `data_kind` 检查缺口，不直接启动修复。 |
| LDM-MCP-006 | 通过 | `backend/services/local_data_management.py` | 后端测试命令 1；`/targets`、`/sync-attempts` facade 测试 | 支持 target 与 attempt 查询。 |
| LDM-MCP-007 | 通过 | `backend/services/local_data_management.py`、`backend/routers/local_data.py` | 后端测试命令 1；MCP 覆盖 `local_data_list_jobs`、`local_data_get_job`、`local_data_get_job_logs` | 任务和日志通过 facade 读取。 |
| LDM-MCP-008 | 通过 | `backend/services/local_data_management.py`、`backend/routers/local_data.py`、`backend/mcp/modules/local_data.py` | 后端测试命令 1；`test_confirmed_write_refuses_to_call_source_without_confirmation`、`test_confirmed_run_calls_source_after_confirmation`、router `/run` 测试 | 缺确认时拒绝，确认后创建受控 fake job；未触碰生产 job。 |
| LDM-MCP-009 | 通过 | `backend/mcp/modules/local_data.py`、`backend/services/local_data_management.py` | 后端测试命令 1；MCP 工具目录覆盖 `/schedules/{id}/run` | 确认型计划运行工具已映射 facade。 |
| LDM-MCP-010 | 通过 | `backend/services/local_data_management.py`、`backend/mcp/modules/local_data.py` | 后端测试命令 1；MCP 工具目录覆盖 `/stats/refresh` | 刷新后复查 `data_stats`，缺确认不执行。 |
| LDM-MCP-011 | 通过 | `backend/services/local_data_management.py`、`backend/routers/local_data.py` | 后端测试命令 1；MCP 工具目录覆盖 `/schedules` | 计划任务列表只读返回。 |
| LDM-MCP-012 | 通过 | `backend/services/local_data_management.py`、`backend/routers/local_data.py` | 后端测试命令 1；MCP 工具目录覆盖 upsert/batch/toggle/delete | 更新、启停和删除均为确认型工具。 |
| LDM-MCP-013 | 通过 | `backend/services/local_data_management.py` | 后端测试命令 1；`test_schedule_reset_plan_is_plan_only_and_does_not_write`、`test_schedule_reset_apply_requires_confirmation` | reset plan 只生成 diff，apply 需确认并复查。 |
| LDM-MCP-014 | 通过 | `backend/services/local_data_management.py`、`backend/mcp/modules/local_data.py` | 后端测试命令 1；MCP 确认型工具覆盖 cancel | 取消任务使用 `confirm_change`，错误不被吞掉。 |
| LDM-MCP-015 | 通过 | `backend/services/local_data_management.py`、`backend/mcp/modules/local_data.py` | 后端测试命令 1；MCP 确认型工具覆盖 clear queued | 清理 queued jobs 使用 destructive 确认口令。 |
| LDM-MCP-016 | 通过 | `backend/services/local_data_management.py`、`backend/routers/local_data.py` | 后端测试命令 1；MCP 工具目录覆盖 alert ack | 告警确认只 ack 告警，不改 readiness 事实。 |
| LDM-MCP-017 | 通过 | `backend/services/local_data_management.py` | 后端测试命令 1；`test_repair_apply_stops_on_first_failure_and_records_error` 前置 plan 流程覆盖 | repair plan 为 plan_only，不执行写操作。 |
| LDM-MCP-018 | 通过 | `backend/services/local_data_management.py` | 后端测试命令 1；`test_repair_apply_stops_on_first_failure_and_records_error` | repair apply 任一步失败即停止并记录真实错误。 |
| LDM-MCP-019 | 通过 | `backend/services/research_assistant/service.py`、`frontend/src/app/research-assistant/chat/page.tsx` | 后端测试命令 1；Playwright 命令 5 | 本地数据自然语言意图先生成检查/计划/确认卡，不直接执行写操作。 |
| LDM-MCP-020 | 通过 | `backend/services/research_assistant/service.py` | 后端测试命令 1；Research Assistant API memory/entity/relation 测试 | 新增长期记忆和轻量图谱 seed，可被 catalog API 读取。 |
| LDM-MCP-021 | 通过 | `frontend/src/app/research-assistant/chat/page.tsx`、`frontend/src/app/research-assistant/workbench/page.tsx`、`frontend/src/app/research-assistant/mcp-tools/page.tsx` | Playwright 命令 5 | 主视图使用中文卡片；JSON 仅保留在审计详情展开区。 |
| LDM-MCP-022 | 通过 | `backend/mcp/modules/local_data.py` | 静态检查命令 7；后端测试命令 1 | MCP module 仅使用 loopback client，不直接 DB、调度器或脚本。 |
| LDM-MCP-023 | 通过 | `backend/mcp/modules/local_data.py` | 后端测试命令 1；工具目录断言不包含 factor、xtquant、miniqmt、paper | 首批工具排除因子独立指标、Xtquant/miniQMT 与实盘路径。 |
| LDM-MCP-024 | 通过 | `backend/services/local_data_management.py`、`backend/mcp/common.py` | 后端测试命令 1；`test_repair_apply_stops_on_first_failure_and_records_error` | 后端错误不转成功，MCP HTTP 4xx/5xx 会抛出真实错误摘要。 |
| LDM-MCP-025 | 通过 | 本记录 | 本记录第 1、2 节 | 无 DB DDL 变更，`production_ddl_gate=noop`。 |
| LDM-MCP-026 | 通过 | 本记录 | 本矩阵 | 已逐条记录设计一致性、代码位置、测试证据和风险边界。 |
| LDM-MCP-027 | 通过 | 本记录 | 本记录第 2 节 | 后端、MCP、助手、前端、构建、静态检查均有命令和结果。 |
| LDM-MCP-028 | 通过 | `backend/services/local_data_management.py`、`backend/mcp/modules/local_data.py`、前端三页 | 后端测试命令 1；Playwright 命令 5 | 写操作闭环为只读检查、计划生成、用户确认、执行、复查；缺确认时拒绝。 |

## 4. 合入前结论

1. `LDM-MCP-001` 至 `LDM-MCP-028` 均已按设计方案完成并通过聚焦验证。
2. 本次没有 production DDL，合入后无需执行生产迁移；仍需由用户按规范重启后端使新增 router/MCP profile 生效。
3. 本次没有触碰生产服务和生产数据库；验证使用单元测试、MCP MockTransport、FastAPI TestClient 和 Playwright mock API。
4. 分支具备创建 PR 和进入用户确认合入环节的条件；未获得用户确认前不得合入 `main`。

## 5. 2026-05-27 同步 `origin/main` 后复验

> 同步背景：PR #180 原始实现 commit `98d4511634623b9a54f39518e3dd4407a0a89ca8` 已落后最新 `origin/main`；本轮将 `origin/main` 合入 `feature/local-data-mcp-gateway-20260524`，以主线 Research Assistant prompt pack、runtime config、dialogue mode、capability sync、MCP catalog grounding 和 action proposal 架构为基底，重新接入 Local Data MCP 能力。

| 序号 | 命令 | 结果 |
| --- | --- | --- |
| 1 | `python -m compileall backend/services/local_data_management.py backend/routers/local_data.py backend/mcp/modules/local_data.py backend/services/research_assistant` | 通过。 |
| 2 | `python -m pytest -q backend/tests/test_local_data_management_facade.py backend/tests/mcp/test_local_data_module.py backend/tests/mcp/test_profiles_registry_gateway.py backend/tests/research_assistant/test_service.py backend/tests/research_assistant/test_api.py -p no:cacheprovider` | 通过，`101 passed in 34.06s`。 |
| 3 | `npm run lint`（frontend） | 通过；仅有既有跨模块 `react-hooks/exhaustive-deps` warnings，无 error。 |
| 4 | `npx tsc --noEmit --incremental false`（frontend） | 通过，`TypeScript: No errors found`。 |
| 5 | `npx playwright test tests/research-assistant/research-assistant.spec.ts --project=chromium`（frontend） | 通过，`PASS (4) FAIL (0)`；使用 Playwright 临时 `3012`，未触碰生产 `3000`。 |
| 6 | `npm run build`（frontend） | 通过，Next.js production build completed；仅有既有 lint warnings。 |
| 7 | `git diff --check` | 通过，无空白错误。 |
| 8 | `$files = @(git diff --name-only origin/main...HEAD -- '*.py'); python -m ruff check --force-exclude $files` | 通过，覆盖 PR 的 `11` 个 Python 改动文件。 |

### 5.1 同步后保留/迁移点

- `backend/services/research_assistant/service.py` 保留主线 dialogue mode、capability sync、runtime MCP catalog grounding，同时新增 `local_data_management_request` 意图、Local Data MCP skill/server/tool catalog、Prompt Tree 选择、Memory/Graph seed 与安全卡片。
- `configs/research_assistant/runtime_context.yaml` 新增 `local_data.health_overview`、`local_data.plan_repair`、`local_data.apply_repair_confirmed` workflow capability；不改变 QE workflow capability 语义。
- `prompt_packs/research_assistant/main/pack.yaml` 新增 `prompt.local_data_management`、`workflow.local_data_check_repair`、`tool_guard.mcp_local_data`，并落到 prompt-pack 节点文件，避免回退到旧版硬编码 Prompt Tree。
- 前端 `chat`、`workbench`、`mcp-tools` 保留主线无 raw JSON 主视图和 capability inquiry 行为，仅在 Local Data 语境或真实目录命中时展示本地数据检查、计划、确认、执行、复查卡。
- `production_ddl_gate=noop`；`production_backend_dependency_gate=noop`；`production_frontend_dependency_gate=noop`；未启动、停止或重启生产 backend `8001` / frontend `3000`，未写生产数据库。

### 5.2 PR Quality 门禁跟进

- PR #180 同步主线后的首次 `Context, scope, and open-source tooling dry-run` 失败原因为 `backend/main.py` 的既有启动顺序：该文件必须先设置项目路径并加载 `.env`，再导入内部 router；本 PR 新增 `local_data` router 使该文件进入 changed-file Ruff 检测范围，从而触发 `E402`。
- `backend/main.py` 已增加文件级 `# ruff: noqa: E402`，显式保留上述启动约束，不改变 Local Data router 的已验证挂载或任何生产运行态。
- 本地复验已按 PR Quality 的 changed-Python 范围运行 Ruff 并通过；后续以 GitHub Actions 重新触发的结果作为合入门禁。
- Ruff 修复 push 后，第二次 PR Quality 在构建 summary 时报告 `origin/main...HEAD: no merge base`；RCA 表明 workflow 虽使用 `fetch-depth: 0`，却又以 `git fetch origin "${BASE_REF}" --depth=1` 将最新 base 置为 shallow 边界，无法处理已合并最新 `main` 的长生命周期 PR。
- `.github/workflows/pr-quality.yml` 已改为保留 base 历史的普通 fetch，使 `issue_flow.py pr-check` 与后续 three-dot diff 能在同步主线后的分支上正常计算 merge base；此变更仅修复 PR 门禁路径，不影响产品运行态。
