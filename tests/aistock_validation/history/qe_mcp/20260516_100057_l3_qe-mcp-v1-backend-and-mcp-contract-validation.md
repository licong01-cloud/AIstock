# QE MCP v1 与待执行实验 UI 管理台验证

- 模块：qe_mcp
- 级别：L3
- 日期：2026-05-16T10:00:57+08:00
- Git commit：0b00983（执行验证时的基线；最终提交见本分支 HEAD）
- 分支：codex/qe-mcp-template-archive-20260516
- Operator：Codex

## 范围

- 补齐 QE 待执行实验管理台：`/quantevolver/templates` 与 `/quantevolver/templates/[templateId]`。
- 支持 MCP 创建的 QE 单次实验与自定义演进模板列表、详情、配置编辑、保存、校验和人工确认执行。
- 后端补强模板更新状态机：配置修改后重置旧校验、旧审批、旧物化结果；未审批模板不能物化；已物化或已执行模板不能原地修改。
- UI 执行路径继续调用 `/api/v1/qe-templates`，再由后端复用现有 `run_experiment` 或 `run_custom_evo_task`。
- 不包含：多 alpha 架构调度、自动演进 LLM 决策调度、生产端口切换、真实长耗时 QE 训练。

## 环境

- 工作区：`F:\Dev\AIstock_worktrees\qe-mcp-template-archive-20260516`
- 后端验证端口：`8011`，只做 dev/test 端口检查，验证时已被占用且允许。
- 前端验证端口：`3011`，Playwright 由测试流程临时启动 Next dev server。
- 生产端口：未触碰、未重启 `8001/3000`。
- Conda/env：`C:/Users/lc999/miniconda3/envs/AIstock/python.exe`
- 数据库：`qe_archive_data_quality` 通过 root `.env` 只读加载 TDX/AISTOCK PG 连接变量执行 schema/data quality smoke。
- 浏览器/headless：Playwright Chromium headless，mock-first API route 验证 UI 业务链路。

## 矩阵

| 用例 | 预期业务结果 | 证据 | 结果 |
|---|---|---|---|
| L0/L1 guardrail | 新增后端、MCP、UI、测试无 HIGH 级 secret、硬编码路径、静默空成功、禁止 fallback | `qe_mcp_l3` guardrail 仅 6 个 MEDIUM RAW_JSON_UI 审阅项，无 HIGH | PASS |
| 模板后端状态机 | 配置修改只保存数据库并重置审查/物化状态；已物化模板不可改；未审批模板不可物化 | `pytest backend/tests/qe_templates` 9 passed；`qe_mcp_backend` 23 passed | PASS |
| QE 单次实验 UI | UI 可展示 MCP 单次实验模板，修改模型/因子/策略等配置，保存不执行，确认后按保存、校验、审批、物化、执行顺序调用 | `qe_template_ui` Playwright 第 2 个用例通过 | PASS |
| 自定义演进 UI | UI 可展示自定义演进 loop 列表，确认执行使用 `QE_CUSTOM_EVO_RUN`，物化后再运行 | `qe_template_ui` Playwright 第 3 个用例通过 | PASS |
| 既有 MCP/Archive 回归 | QE MCP 与 QE Archive 后端合约不破坏 | `qe_mcp_backend` 23 passed；`qe_archive_backend` 98 passed | PASS |
| custom_evo 既有能力回归 | retry/rerun/append 等既有自定义演进路由不破坏 | `pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py` 13 passed | PASS |
| Archive data quality | archive v2 schema/table/column/comment 完整，pending outbox 只作为 informational warning | `qe_archive_data_quality` PASS；32/32 tables；546/546 columns commented；pending_outbox_count=1692 | PASS |
| 生产隔离 | 验证仅使用 dev 端口，不停止/重启生产 8001/3000 | `scripts/aistock_validate.py ports --allow-occupied 8011 3011`；本次无生产操作 | PASS |

## 命令

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/qe_templates -q -p no:cacheprovider
cd frontend; npm ci
cd frontend; npm exec tsc -- --noEmit --incremental false
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_template_ui -- 8011 3011
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_mcp_l3 -- 8011 3011
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q -p no:cacheprovider
cd frontend; npm run lint
git diff --check
# 加载 root .env 中 TDX_DB_* / AISTOCK_PG_* / PG* 后执行：
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality
```

## 证据

- `qe_mcp_l3`：PASS；包含 guardrail、`qe_mcp_backend`、`qe_archive_backend`、`qe_template_ui`。
- `qe_mcp_backend`：23 passed。
- `qe_archive_backend`：98 passed。
- `qe_template_ui`：TypeScript `--noEmit` PASS；Playwright 3 passed。
- custom_evo 回归：13 passed。
- `frontend npm run lint`：PASS；仅报告既有模块 hook dependency warnings，未出现新增错误。
- `git diff --check`：PASS；仅提示两个 Markdown 文件后续 Git 触碰时可能从 LF 转 CRLF，不存在空白错误。
- `qe_archive_data_quality`：schema version `qe_archive_v2_20260516`；32/32 tables；546/546 columns commented；`run_count=16`；`pending_outbox_count=1692` 为 informational warning。
- 临时输出：`tmp/qe_archive_data_quality_smoke.json`、`frontend/tmp/playwright-*` 为 ignored 验证产物，不提交。

## 失败与修复

| 失败 | 根因 | 修复 | 复测证据 |
|---|---|---|---|
| `pytest backend/tests/qe_templates` 初次失败 | 测试使用 `pytest.mark.asyncio`，当前环境未安装 async pytest 插件 | 改为 `asyncio.run(...)` 执行 materializer async 方法 | `pytest backend/tests/qe_templates` 9 passed |
| `npm exec tsc` 初次失败 | worktree 未安装 frontend `node_modules` | 在 `frontend` 执行 `npm ci` 安装锁定依赖 | `npm exec tsc -- --noEmit --incremental false` PASS |
| `qe_template_ui` 初次失败 | Playwright `getByText("Loop A")` 同时命中表格和 JSON 编辑区，strict mode 冲突 | 改为按 table cell 定位 `getByRole("cell", { name: "Loop A" })` | `qe_template_ui` 3 passed |
| `qe_archive_data_quality` 初次失败 | worktree 未带 `.env`，DB 连接缺少密码 | 只读加载 root `.env` 的 DB 环境变量后重跑 | `qe_archive_data_quality` PASS |

## 结果

- 最终状态：PASS。
- 业务结果：MCP 创建的 QE 单次实验和自定义演进模板可以进入统一 UI 管理台，人工可查看、修改、保存；保存不会执行；确认执行后复用现有 QE 执行层并跳转现有实验/演进页面。
- 未验证能力：未运行真实长耗时 QE 训练；未接入 multi-alpha 和自动演进 LLM 决策；这两项为本期明确不做范围。
- 需要生产 backend restart：否。
- 需要生产 frontend restart：否。
- 需要 dev service restart：若要在当前已运行的 `8011/3011` 体验新增页面/API，需要重启对应 dev 服务加载本分支代码。
- 资产安全：未修改 QE/RD-Agent 产物、模型权重、Qlib 数据、HMM snapshot 或 StrategyPackage 资产。
