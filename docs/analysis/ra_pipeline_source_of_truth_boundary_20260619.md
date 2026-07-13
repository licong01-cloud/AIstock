# BUG-418 设计：Research Assistant 消费流水线事实源边界

- 版本/日期：v0 / 2026-06-19
- 关联：BUG-418 / GitHub Issue #1270
- 阶段：plan-first 设计稿，仅供 Tier2 审核；本阶段不改产品代码、不启停服务、不触碰生产 DB/DDL。
- 核心结论：Validation Center / Nightly / AIstock issue workflow 是候选 Issue、discovery report、GitHub sync、BUG JSON 与候选质量门的唯一事实源；Research Assistant 只能消费、解释、注释这些事实，并通过受控 MCP/Validation workflow 代理触发标准流程，不能自建第二套权威态。

## 1. 边界原则

1. **读源唯一**：RA 页面、API、晨报、对话工具卡读取候选/发现流时，统一消费 `ValidationPipelineCenterService.issue_candidates()` / `issue_candidate_summary()` 及 Nightly/Validation 产物；优先进程内依赖注入，不做 HTTP 自调，不要求启动 `8001/3000/19080`。
2. **写入不越权**：RA 不直接写 `tests/aistock_validation/bugs`，不执行 `gh issue create`，不导入或复制 `scripts/aistock_issue_workflow.py` / `scripts/nightly_bug_candidate_queue.py` 的写入逻辑。
3. **草稿显式降级**：`assistant_issue_candidates` 与 `assistant_validation_discovery_reports` 仅保留为对话草稿、人工备注、解释缓存；它们不是正式候选 Issue/发现报告事实源。
4. **触发走代理**：若用户要求提交/同步/关闭 Issue，RA 只能通过 `aistock-validation` MCP/Validation API 的标准工具和既有审批门禁触发；RA 自身不创建 GitHub Issue、不自写 `github_sync_*` 权威状态。
5. **LLM/Nightly 调度隔离**：RA 保留理解和解释发现流的能力，但不接入 DeepSeek/Nightly discovery 调度，不运行候选质量门；`llm_discovery` 种子状态继续保持 `not_started`。

## 2. 受影响文件与改造方案

| 文件 | 当前问题 | 设计改造 |
|---|---|---|
| `backend/routers/research_assistant.py` | `GET /research-assistant/issue-candidates` 直接读 RA `issue_candidates` 表；`POST .../github-sync` 调 RA 自有 sync gate。 | `GET` 改调 `ResearchAssistantService.list_pipeline_issue_candidates(...)`，返回 Validation 候选集合的助手视图；`POST .../github-sync` 保持 block-only，不写 GitHub、不写权威 sync 状态，只返回“请走 AIstock issue workflow / Validation MCP”的结构化提示。 |
| `backend/services/research_assistant/service.py` | `create_issue_candidate` / `validation_discovery_summary` / `github_sync_issue_candidate` 使用 RA 自有表，容易被理解为正式候选与同步流。 | 注入 `ValidationPipelineCenterService` 或 `IssueFactSource` 接口；候选列表与 discovery summary 从该接口读取；`create_issue_candidate` 改为“conversation draft/manual note”语义；`github_sync_issue_candidate` 改为明确阻断或仅记录非权威说明，不再更新正式 `github_sync_*` 语义。 |
| `backend/services/research_assistant/proactive_reports.py` | `collect_issue_validation_section` 从 `context.repository.issue_candidates` 读取 RA 草稿表。 | 扩展 `ProactiveReportContext` 注入候选事实源；晨报 issue section 改读 Validation 候选 API/服务，并使用 `validation_issue_candidates:<candidate_id>` 等 source refs。 |
| `frontend/src/app/research-assistant/issue-candidates/page.tsx` | 页面展示 RA 自有候选队列，并提供 GitHub dry-run 按钮。 | 页面定位改为“Validation 候选队列的助手视图”；展示 `source_type/module_id/quality_gate/issue_payload_ready/github_issue_url` 等规范字段；按钮改为“标准 workflow 操作提示/跳转”，不触发 RA 自建 GitHub dry-run。 |
| `frontend/src/app/research-assistant/streams/page.tsx` | “发现报告”与候选 Issue 文案指向 `assistant_validation_discovery_reports`。 | 页面定位改为“流水线发现流助手视图”；数据来自 Validation/Nightly 候选事实源与 summary；RA 草稿/备注仅在可折叠诊断或备注区显示，默认不作为事实源。 |
| `frontend/src/lib/research-assistant/api.ts` | RA 候选类型偏向 `AssistantIssueCandidate` 表结构。 | 保留 RA endpoint 客户端，但类型兼容/映射 Validation `ValidationIssueCandidateItem`；避免前端把 RA 草稿字段当正式候选字段。 |
| `backend/services/research_assistant/repository.py` | `issue_candidates`、`validation_discovery_reports` 映射名称缺少“草稿/缓存”边界。 | 代码注释与 schema 描述改为 draft/cache 语义；不删除表，不改变生产 DDL。 |
| `backend/db/init_research_assistant_schema_20260521.py` | 初始化脚本的表说明仍称 Candidate issue queue。 | 仅更新源码内描述/注释为“assistant draft/cache only”；如果需要真实 DB comment 或废表，放入 Phase 2 DDL 跟进。 |
| `backend/services/research_assistant/execution.py` | loopback `assistant_create_issue_candidate` 会创建 RA 自有候选。 | 工具结果卡改称“对话草稿已记录”；下一步明确必须走 `report_bug` / `mcp_github_issue_create` / `promote-nightly-candidate` / `mcp_github_issue_sync_bug` 等规范工具。 |
| `backend/services/research_assistant/domain_ontology.py`、`tool_router.py`、`mcp_catalog_sync.py` | VALIDATION_ISSUE 域可路由到标准 MCP 工具，但 RA 自有候选工具文案仍易混淆。 | 保留 `aistock-validation` 标准工具为唯一 confirmed path；RA 自有工具只允许 draft/preflight；文案与 preflight checks 加 `standard_workflow_required`。 |
| `backend/mcp/modules/validation.py` / `backend/mcp/tool_manifest.py` | 标准工具已有 `report_bug`、`mcp_github_issue_create`、`mcp_github_issue_sync_bug`；若需要 Nightly 候选提升，RA 不应直接导入脚本。 | 如需 `promote-nightly-candidate` 对话触发，应在 Validation MCP 模块提供受控适配器，复用 issue workflow gate；RA 只调用该工具，不 import/exec workflow 脚本。 |
| 测试文件 | 现有测试只证明 RA 不直接 create GitHub，不锁定读源唯一性。 | 新增/更新边界锁测试，详见第 6 节；全部使用依赖注入/假事实源，离线运行。 |

> 实现前需要把 allowed_write_scope 扩到上述后端、前端、测试文件；本设计阶段仅新增本设计文档并在 BUG JSON 中记录设计文档 scope。

## 3. 读源切换设计

### 3.1 RA `/issue-candidates`

- 新增轻量接口，例如 `IssueCandidateFactSource`：
  - `issue_candidates(module=None, severity=None, status=None, page=1, page_size=...) -> dict`
  - `issue_candidate_summary() -> dict`
- 默认实现包装 `ValidationPipelineCenterService`，由 `ResearchAssistantService` 构造函数注入；测试使用 fake provider。
- `GET /research-assistant/issue-candidates` 参数从 `limit/offset` 映射到 Validation 的 `page/page_size`；返回字段以 Validation candidate 为准，RA 可附加 `assistant_view=true`、`source_of_truth="validation_pipeline"`、`draft_storage_authoritative=false`。
- RA 自有 `assistant_issue_candidates` 不参与该列表；只可在单独“草稿/备注”视图或诊断卡中查看。

### 3.2 `validation_discovery_summary`

- `latest_reports` 不再读取 `assistant_validation_discovery_reports` 作为正式报告；改为从 Validation/Nightly 候选 summary 与候选 items 汇总：
  - `candidate_summary = issue_candidate_summary()`
  - `candidate_issues_needing_review = issue_candidates(status=<open/draft/ready filters>)`
  - 可选 `discovery_stream` 由 Nightly candidate `source_type/source_plan_key/active_discovery_reason/source_paths` 派生。
- 返回体写明：
  - `source_of_truth="validation_pipeline"`
  - `assistant_draft_tables=["assistant_issue_candidates","assistant_validation_discovery_reports"]`
  - `draft_tables_authoritative=false`
- 仅当事实源为空时返回空集合与 `candidate_queue_empty`，不得回退到 RA 表冒充事实。

### 3.3 `/research-assistant/streams`

- 定位改为“流水线发现流的助手视图”。
- 主面板展示 Validation/Nightly 候选摘要、活跃 discovery reason、quality gate 与 source paths。
- RA 草稿 discovery report 只作为“助手备注/解释缓存”折叠区，默认不显示为正式发现报告。

### 3.4 晨报 `collect_issue_validation_section`

- `ProactiveReportContext` 增加 `issue_fact_source` 或 `pipeline_center` 注入。
- 默认 registry 的 `collect_issue_validation_section` 使用 `issue_fact_source.issue_candidates(...)`。
- `source_refs` 改为 `validation_issue_candidates:<candidate_id>` 或候选 `source_path`，不再使用 `assistant_issue_candidates:<id>`。
- 如果事实源不可用，晨报 section 显式 degraded，并带 `validation_issue_fact_source_unavailable`，不静默回退 RA 表。

## 4. 写入与代理边界

### 4.1 RA 草稿表降级

- `assistant_issue_candidates`：
  - 新语义：对话草稿、人工备注、候选解释缓存。
  - 不再命名为正式候选队列；UI/卡片文案写明“正式提交必须走 AIstock issue workflow”。
  - `github_sync_status/github_sync_json` 仅保留为非权威对话草稿/解释缓存，待 Phase 2 退场，不代表正式 GitHub sync 状态。
- `assistant_validation_discovery_reports`：
  - 新语义：助手对 discovery 事实的备注/解释缓存。
  - `summary_json.llm_discovery` 继续固定为 `not_started`，不得接 DeepSeek/Nightly 调度。
- 不 DROP 表，不新增迁移；真实废表、DB comment、数据迁移列入 Phase 2。

### 4.2 GitHub sync 入口

P1 建议采用 **block-only**：

- `github_sync_issue_candidate(...)` 对 `dry_run/formal` 均返回结构化阻断：
  - `github_sync_status="blocked"`
  - `direct_github_create_performed=false`
  - `blocked_reason="Research Assistant does not own GitHub sync; use AIstock issue workflow / Validation MCP"`
  - `recommended_tools=["report_bug","mcp_github_issue_create","mcp_github_issue_sync_bug"]`
- 不执行 `gh`，不调用 subprocess，不写 BUG JSON，不写权威 `github_sync_*`。
- 前端 dry-run 按钮替换为说明或标准 workflow 入口提示；如果未来要 dry-run 代理，只能对已有 `bug_id` 调 `mcp_github_issue_sync_bug(apply=False)`，并由 Validation MCP 负责 gate。

### 4.3 VALIDATION_ISSUE MCP 工具

- RA 的 `McpDomain.VALIDATION_ISSUE` 只允许路由到 `aistock-validation` 规范工具：
  - `report_bug`
  - `mcp_github_issue_create`
  - `mcp_github_issue_sync_bug`
  - 后续如需 Nightly 候选提升，新增/使用 Validation MCP 适配器 `promote-nightly-candidate`，不能在 RA 中 import/exec `scripts/aistock_issue_workflow.py`。
- 写/确认类仍走既有 `ActionProposal` / approval gate；RA agent loop 不直接执行写入。
- `assistant_create_issue_candidate` 保留为 draft-only loopback 工具，不能被描述为正式 issue candidate creation。

## 5. LLM/Nightly discovery 隔离

- RA 不实现新的 DeepSeek/Nightly 调度，不构建候选质量门，不复制 Nightly rotation/queue 逻辑。
- `validation_discovery_reports` 的种子记录保留 `{"llm_discovery":"not_started","issue_gate":"candidate_only"}`，但文案改成“assistant draft/cache only”。
- RA 可以解释 Validation/Nightly 事实源里的 `llm_hypothesis`、`quality_gate`、`active_discovery_reason`，但不能产生正式候选或自动提升。

## 6. 测试清单

全部测试使用依赖注入或 fake provider，离线运行；不启动服务、不访问生产 DB。

1. **GitHub 创建边界**
   - 更新 `backend/tests/research_assistant/test_api.py` 与 `backend/tests/research_assistant/test_service.py` 现有断言。
   - 断言 `direct_github_create_performed is False`。
   - 断言 RA `github_sync_issue_candidate` 不调用 `gh`、不调用 direct GitHub create；P1 block-only 下断言返回 `blocked` 和标准 workflow 提示。
2. **BUG JSON 写入边界**
   - 新增静态/运行时测试：RA 模块路径不写 `BUGS_ROOT`，不出现 `tests/aistock_validation/bugs` 写入路径。
   - 对 `backend/services/research_assistant/**`、`backend/routers/research_assistant.py` 做 AST/text boundary lock。
3. **读源契约**
   - fake `IssueCandidateFactSource` 返回集合 X。
   - 断言 `/research-assistant/issue-candidates`、`validation_discovery_summary`、`streams` 数据结构、晨报 issue section 均展示 X。
   - 断言 RA 自有表中不同集合 Y 不会被主视图读取为事实源。
4. **VALIDATION_ISSUE confirmed 工具代理**
   - fake validation adapter/MCP client，断言调用的是 `report_bug` / `mcp_github_issue_create` / `mcp_github_issue_sync_bug` 或 Validation-owned promotion adapter。
   - 断言 RA 未直接 import/exec `aistock_issue_workflow.py`、`nightly_bug_candidate_queue.py`、`gh`。
5. **静态导入边界锁**
   - CI 测试扫描 RA 模块，禁止：
     - `import scripts.aistock_issue_workflow`
     - `from scripts import aistock_issue_workflow`
     - `import scripts.nightly_bug_candidate_queue`
     - `subprocess` 调用 `gh issue create`
     - 对 `tests/aistock_validation/bugs` 的直接写入。
6. **UI 文案与视图**
   - 前端单测或 Playwright：候选页/streams 明确显示 `source_of_truth=Validation/Nightly/Issue Workflow` 或等价文案。
   - 断言 dry-run button 不再触发 RA 自建 GitHub sync；页面提示标准 workflow。

实现后完整回归：

- `rtk proxy python -m nox -s l0`
- `rtk proxy python -m nox -s research_assistant_backend`
- `rtk proxy python -m nox -s research_assistant_mcp_contract`
- `rtk proxy python -m nox -s ra_phase7_full_accept`
- `rtk proxy python -m nox -s validation_module_registry_l0`
- `rtk proxy git diff --check`

## 7. Phase 2 跟进项

单独建 follow-up issue，需 DDL/生产授权后处理：

- 废弃或迁移 `assistant_issue_candidates`、`assistant_validation_discovery_reports` 的正式候选/发现用途。
- 评估是否 DROP 表、改名为 `assistant_issue_drafts` / `assistant_discovery_notes`，或增加 DB comment/迁移脚本。
- 清理历史 `github_sync_status/github_sync_json` 字段的权威语义，保留审计只读映射。
- 对已有数据做只读迁移审计：哪些是用户草稿、哪些可丢弃、哪些需转为备注缓存。

## 8. 红线确认

- 不在 RA 实现 DeepSeek/Nightly 调度。
- 不直接写 `tests/aistock_validation/bugs`。
- 不执行 `gh issue create`。
- 不复制 `nightly_bug_candidate_queue` / `aistock_issue_workflow` 逻辑。
- Validation Center / Nightly / issue workflow 继续作为唯一事实源。
