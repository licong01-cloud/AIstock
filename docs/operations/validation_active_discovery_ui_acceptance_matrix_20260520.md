# 主动发现流水线 UI 设计验收矩阵（2026-05-20）

关联设计文档：`docs/architecture/validation_active_bug_discovery_platform_design_20260520.md`

实现分支：`feature/active-discovery-ui-20260520`

## 1. 验收结论

本次实现按设计文档第 14.0、14.5、21.1-21.4 逐项对照。当前状态为 **passed**，具备提交 PR 并进入合入前人工验收的条件；未执行合入 `main`。

安全边界：

- 未触碰生产端口 `8001` / `3000`。
- 未写生产数据库。
- LLM API key、GitHub token、DB 密码未进入前端页面、测试快照、候选 Issue 或证据文本。
- Playwright 使用路由 mock 验证前端交互，不写业务状态。

## 2. 组件级验收

| 设计组件 | 状态 | 实现位置 | 验收证据 |
|---|---|---|---|
| `ValidationDiscoveryShell` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | `/validation/*` 页面统一顶部 tab；保留 AIstock 全局左侧导航；`npm run build` 产出 5 个新路由 |
| `MetricSummaryCard` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | 夜间汇报页顶部卡片绑定 `/validation/discovery/nightly-reports/{report_id}` 的 `summary_cards`，支持点击跳转 |
| `NightlyRunHeader` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | 支持 run selector、run_id/commit 复制、刷新；Playwright 覆盖夜间汇报页 |
| `ModuleResultCard` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | 模块卡片按模块展示覆盖率、候选、P0/P1、Issue，可展开详情 |
| `IssueCandidateTable` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | TanStack Table 支持搜索、排序、筛选、分页、详情抽屉、审核/晋级动作 |
| `EvidenceDrawer` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | 展示日志、API 响应、MCP 响应、截图/trace、artifact、复现命令 |
| `ExecutionTimeline` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | baseline/change/manual 三类执行树绑定 report `execution_tree`，失败/状态节点可打开证据 |
| `BusinessProbeFlow` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | React Flow 展示 QE -> Archive -> StrategyPackage -> Selection -> Paper v2 -> DW，节点状态绑定 report module |
| `LlmReportPanel` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | 展示 provider/model/prompt/context pack/补证据状态；不提供直接创建正式 Issue 按钮 |
| `AgentTaskPanel` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | 展示 task_id、agent_runtime、workspace/branch、claim/run/cancel、结果和证据入口 |
| `CleanupRiskPanel` | passed | `frontend/src/components/validation/discovery/ActiveDiscoveryComponents.tsx` | 仅展示 validation namespace 资源、TTL/cleanup 状态、失败风险 |

## 3. 页面级验收

### 3.1 夜间汇报页 `/validation/nightly-reports`

| 功能 | 状态 | 实现位置 | 证据 |
|---|---|---|---|
| Run 选择器 | passed | `frontend/src/app/validation/nightly-reports/page.tsx` | 调用 `/validation/discovery/nightly-reports`，至少展示最近 7 次 run |
| 顶部状态卡 | passed | `MetricSummaryCard` | 状态色、tooltip、点击跳转候选页 |
| 模块结果卡 | passed | `ModuleResultCard` | 支持 validation、qe、strategy_package、selection、paper_v2；后端缺失时自动补 fallback module card |
| 执行链路 | passed | `ExecutionTimeline` | baseline/change/manual 三组任务可打开 evidence |
| LLM 报告 | passed | `LlmReportPanel` | 显示 provider/model/prompt/context pack/补证据状态 |
| 候选 Issue 汇总 | passed | `CandidateGroupLinks` | 分组跳转 `/validation/discovery-candidates?review_status=...` |
| 证据包入口 | passed | `EvidenceDrawer` | 5 类证据统一展示 |
| Cleanup 风险 | passed | `CleanupRiskPanel` | 只展示 validation namespace |

### 3.2 候选 Issue 页 `/validation/discovery-candidates`

| 功能 | 状态 | 实现位置 | 证据 |
|---|---|---|---|
| 候选表格 | passed | `IssueCandidateTable` | TanStack Table 搜索、排序、筛选、分页、详情抽屉 |
| 去重状态 | passed | `backend/services/validation/active_discovery.py` | 候选携带 `github_issue_url` / `github_issue_number`；已有链接显示而非默认新建 |
| 审核动作 | passed | `frontend/src/app/validation/discovery-candidates/page.tsx` | reviewer + evidence checklist；晋级要求确认 candidate_id |
| GitHub 同步 | passed | `promoteDiscoveryCandidate` API | 有链接显示 GitHub；无链接返回 `requires_github_sync_mcp`，不直接提交 BUG JSON |
| 证据详情 | passed | `EvidenceDrawer` | 设计规则/日志/API/MCP/截图/复现命令统一展示 |

### 3.3 探测任务页 `/validation/discovery-tasks`

| 功能 | 状态 | 实现位置 | 证据 |
|---|---|---|---|
| 任务列表 | passed | `AgentTaskPanel` | nightly_baseline/change_driven/manual_mcp 可过滤 |
| 手工部署 | passed | `frontend/src/app/validation/discovery-tasks/page.tsx` | 创建专项任务，选择 detector/module/risk/resource；L4/L5 要确认 |
| Agent claim 状态 | passed | `claimDiscoveryAgentTask` API + UI | 显示 agent_runtime、workspace、branch、结果摘要 |
| 任务结果 | passed | `runDiscoveryTask` API + EvidenceDrawer | dry-run 生成 evidence_manifest_id；失败显示错误和复现入口 |
| 取消/重跑 | passed | `cancelDiscoveryTask` / `runDiscoveryTask` | 取消不删除证据；重跑通过 API 生成新结果 |

### 3.4 业务探针页 `/validation/business-probes`

| 功能 | 状态 | 实现位置 | 证据 |
|---|---|---|---|
| 链路流程图 | passed | `BusinessProbeFlow` | React Flow 节点颜色绑定真实 report module 状态，节点可点击 |
| 探针分层 | passed | `frontend/src/app/validation/business-probes/page.tsx` | UI 展示 L3/L4/L5 风险规则 |
| 步骤证据 | passed | `ExecutionTimeline` + `EvidenceDrawer` | 每步可按 evidence_manifest_id 打开证据 |
| Cleanup | passed | `CleanupRiskPanel` | validation resource、TTL、清理状态可展示 |

### 3.5 LLM 配置引用页 `/validation/discovery-llm-profiles`

| 功能 | 状态 | 实现位置 | 证据 |
|---|---|---|---|
| Profile 列表 | passed | `frontend/src/app/validation/discovery-llm-profiles/page.tsx` | 表格展示 agent_role、provider、model、prompt、nightly/manual；不展示 token |
| Prompt 跳转 | passed | `LlmReportPanel` / Profile 表格 | 跳转 `/quantevolver/prompts?agent_type=validation_discovery` |
| 模型跳转 | passed | `LlmReportPanel` / Profile 表格 | 跳转 `/config/rdagent-llm` |
| 运行质量 | passed | `LlmReportPanel` | 展示 `last_7_runs` 成功率、命中率、误报率、成本估算 |
| Eval 结果 | passed | `runDiscoveryLlmEval` API + UI | 支持 promptfoo 风格 dry-run，显示结果 JSON |

## 4. 后端 API 与安全验收

| 能力 | 状态 | 实现位置 | 证据 |
|---|---|---|---|
| 夜间汇报 API | passed | `backend/services/validation/active_discovery.py` + `backend/routers/validation.py` | `GET /api/v1/validation/discovery/nightly-reports` |
| 候选 Issue API | passed | 同上 | list/detail/review/promote，P0/P1 晋级要求 reviewer 和 confirm |
| 任务 API | passed | 同上 | schedule/run/cancel/agent claim/context/result/evidence/complete |
| LLM profile API | passed | 同上 | DeepSeek env 仅检测 configured/missing_env，不返回 token |
| Tool adapter API | passed | 同上 | Semgrep-like、Schemathesis-like、Playwright-like、contract、LLM eval dry-run |
| Evidence manifest API | passed | 同上 | trace/evidence 查询统一返回日志、API/MCP、截图、复现命令 |
| GitHub 同步门禁 | passed | `promote_candidate` | 无 GitHub 链接时只返回 `requires_github_sync_mcp`，避免本地 BUG JSON 与 GitHub 不一致 |

## 5. 验证命令

| 命令 | 状态 | 结果 |
|---|---|---|
| `python -m py_compile backend/services/validation/active_discovery.py backend/routers/validation.py backend/tests/test_validation_active_discovery.py` | passed | 语法检查通过 |
| `python -m pytest backend/tests/test_validation_active_discovery.py backend/tests/test_validation_pipeline_center_phase1.py backend/tests/test_validation_platform_health.py -q -p no:cacheprovider` | passed | `14 passed in 10.62s` |
| `cd frontend; npx tsc --noEmit` | passed | TypeScript 通过 |
| `cd frontend; npm run lint` | passed | 仅存量 warnings，无新增 error |
| `cd frontend; npm run build` | passed | 5 个 `/validation/*` 新路由构建成功 |
| `cd frontend; npx playwright test tests/validation-discovery/active-discovery.spec.ts --project=chromium` | passed | `3 passed (23.0s)` |

## 6. 合入前人工验收建议

1. 在独立测试端口启动后端和前端，不使用生产 `8001` / `3000`。
2. 打开 `/validation/nightly-reports`，确认 5 个顶部 tab、run selector、模块卡、LLM 报告和证据抽屉可读。
3. 在 `/validation/discovery-candidates` 对一个 P2 候选执行“追加证据”审核 dry-run；不要在未确认 GitHub 同步 MCP 的情况下晋级真实 Issue。
4. 在 `/validation/discovery-tasks` 创建 L2 手工任务并 dry-run，确认取消不删除证据。
5. 在 `/validation/business-probes` 点击 QE/Selection/Paper v2 节点，确认颜色与模块状态一致。
6. 在 `/validation/discovery-llm-profiles` 确认 DeepSeek provider 显示 configured/missing_env，但不显示任何 key。
