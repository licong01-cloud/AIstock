# BUG-079 QE Archive 手动入仓设计符合性复核矩阵

日期：2026-05-20  
分支：`bug/BUG-079-qe-archive-design-compliance`  
设计文档：`docs/architecture/qe_archive_manual_ingestion_selection_design_20260519.md`  
治理规则：`DESIGN-COMPLIANCE-001`，禁止简化版、子集版、POC 版或仅后端占位交付。

## 1. 设计项到实现证据

| 设计项 | 实现位置 | 验证证据 | 状态 |
|---|---|---|---|
| 实验级与 loop 级必须同时支持入仓 | `backend/routers/qe_archive.py`；`backend/services/qe_archive/backfill_service.py`；`frontend/src/lib/qe-archive/api.ts` | `backend/tests/qe_archive/test_manual_ingestion_selection.py`；`backend/tests/test_aistock_qe_mcp_servers.py` | 完成 |
| `task_id + loop_indices` 支持多 loop 精确选择，缺失 loop 不静默忽略 | `backend/services/qe_archive/backfill_service.py`；`backend/tests/qe_archive/test_manual_ingestion_selection.py` | `test_build_candidates_returns_missing_loop_indices_in_preview_order` | 完成 |
| `/api/v1/qe-archive/backfill` 作为 UI/MCP 统一写入口，写入必须使用 `QE_ARCHIVE_WRITE` | `backend/routers/qe_archive.py`；`frontend/src/lib/qe-archive/api.ts`；`scripts/aistock_qe_archive_mcp_server.py` | `backend/tests/test_aistock_qe_mcp_servers.py`；Playwright archive flow | 完成 |
| `/api/v1/qe-archive/source-status` 展示 experiment/task/loop 入仓状态和推荐语义 | `backend/services/qe_archive/source_assembler.py`；`frontend/src/lib/qe-archive/api.ts` | `test_archive_status_from_policy_maps_design_states`；`test_loop_row_archive_status_reads_archive_policy_from_config_json` | 完成 |
| 状态语义覆盖 `not_archived`、`eligible`、`recommended`、`not_recommended`、`partially_archived`、`fully_archived`、`manual_only`、`skipped` | `backend/services/qe_archive/source_assembler.py`；`frontend/src/app/qe-archive/page.tsx`；QE 页面 badge | 后端单测 + 前端 TypeScript + Playwright | 完成 |
| QE Archive MCP 支持明确 ID preview/execute/source-status，不依赖扫描式 limit 误入仓 | `scripts/aistock_qe_archive_mcp_server.py` | `backend/tests/test_aistock_qe_mcp_servers.py` | 完成 |
| QE 自动演进主列表页显示任务级入仓状态并支持预览/写入全部待入仓 loop | `frontend/src/app/quantevolver/evolution/page.tsx` | `frontend/tests/qe/qe-candidate-strategy-actions.spec.ts` 中 task-level archive preview | 完成 |
| QE 自动演进任务详情页显示 task 汇总、推荐数、loop badge、checkbox、单 loop/批量 preview 和 confirmed write | `frontend/src/app/quantevolver/evolution/[taskId]/page.tsx` | TypeScript、lint、现有候选策略动作回归；source-status mock 覆盖 | 完成 |
| QE 实验历史页显示单实验、parent task、child loop 入仓状态，并支持单实验/loop/批量 preview 和 confirmed write | `frontend/src/app/quantevolver/experiments/page.tsx` | `frontend/tests/qe/qe-candidate-strategy-actions.spec.ts` 中 child loop archive preview | 完成 |
| `/qe-archive` 集中治理页支持 task 展开 loop、选推荐 loop、选全部有效 loop、手动选择 loop 入仓 | `frontend/src/app/qe-archive/page.tsx` | `frontend/tests/qe-archive/qe-archive-flows.spec.ts` 中 loop selection payload 断言 | 完成 |
| 页面不得因为入仓筛选隐藏 QE 源实验或 loop | `frontend/src/app/quantevolver/evolution/page.tsx`；`frontend/src/app/quantevolver/evolution/[taskId]/page.tsx`；`frontend/src/app/quantevolver/experiments/page.tsx` | 页面只根据源 API 列表渲染；archive 状态仅影响按钮和选择禁用 | 完成 |
| RP 自动记录不等于 QE Archive 自动入仓 | 本次未启用任何自动入仓开关；写入仍需 `QE_ARCHIVE_WRITE` | 后端/API/MCP 均保留 confirmed write | 完成 |

## 2. 本次修复的关键差距

1. 前端缺少自动演进主列表页的 task 级入仓状态和操作入口，本次补齐。
2. QE Archive 集中治理页原先只能选 task/experiment，不能展开到 loop 级精确选择，本次补齐。
3. source-status 原先对未入仓对象语义过粗，只能表达 `archived/not_archived`，本次补齐推荐、人工判断、跳过、不建议等设计语义。
4. 实验历史页和任务详情页已有部分能力，但缺少 `recommended` 状态展示和单对象快捷 preview/execute，本次补齐。
5. 测试补上了后端状态语义、缺失 loop、MCP 明确 ID、QE 页面按钮和 QE Archive loop 选择的回归覆盖。

## 3. 验证清单

已执行：

- `python -m compileall backend/services/qe_archive/source_assembler.py backend/services/qe_archive/backfill_service.py backend/routers/qe_archive.py scripts/aistock_qe_archive_mcp_server.py scripts/aistock_qe_experiment_mcp_server.py`
- `python -m pytest backend/tests/qe_archive/test_manual_ingestion_selection.py backend/tests/test_aistock_qe_mcp_servers.py -q -p no:cacheprovider`
- `python -m pytest backend/tests/qe_archive -q -p no:cacheprovider`
- `cd frontend && npm ci`
- `cd frontend && npm exec tsc -- --noEmit --incremental false`
- `cd frontend && npm run lint -- --file src/app/qe-archive/page.tsx --file src/app/quantevolver/evolution/page.tsx --file src/app/quantevolver/evolution/[taskId]/page.tsx --file src/app/quantevolver/experiments/page.tsx --file src/lib/qe-archive/api.ts`
- `cd frontend && npm run test:e2e -- tests/qe-archive/qe-archive-flows.spec.ts tests/qe/qe-candidate-strategy-actions.spec.ts`

待最终门禁执行：

- `python -m nox -s qe_archive_backend`
- `python -m nox -s qe_mcp_backend`
- `python -m nox -s qe_mcp_l3`
- `python -m nox -s guardrail_changed_files -- --changed-only`
- `python -m nox -s validation_module_registry_l0`
- `python -m nox -s l0 -- <changed files>`
- `git diff --check`

## 4. 结论

当前实现按设计文档覆盖了后端、MCP、前端四个页面和验证证据，没有保留已知简化版或 POC 交付项。若后续最终门禁发现失败，必须先修复再提交，不允许以“后续补齐”的方式合入。
