# BUG-423 Phase 2：废弃 RA 自有候选/发现草稿表迁移设计

- 版本/日期：v0 / 2026-06-19
- 关联：BUG-423 / GitHub Issue #1289
- 状态：plan-first，供 Tier2 审核；本阶段不执行 DDL、不改产品行为、不连接生产 DB、不启停 8001/3000/19080。
- DDL 授权：用户已于 2026-06-19 明确授权 BUG-423 Phase 2 DDL，方案为直接 DROP `assistant_issue_candidates` 与 `assistant_validation_discovery_reports`；`production_ddl_gate=authorized` 已记录到 BUG JSON。
- 目标：Research Assistant 不再保留第二套候选 Issue / discovery report 草稿表；Validation Center / Nightly / AIstock issue workflow 继续作为候选、发现、GitHub sync、BUG JSON 的唯一事实源。

## 1. 当前结论

完整引用扫描显示：当前 main 仍有多处 schema、repository、service、router、MCP、frontend、tests 引用两张 RA 自有表；如果直接执行 DROP，会留下运行时孤儿引用。实现阶段必须先完成本设计列出的所有引用清理与测试更新，再在受控迁移中 DROP。数据库层面未发现跨表 FK，依赖对象集中在 `idx_aic_status_updated` 索引、`ck_aic_status` check 约束、`uq_aic_dedupe` 唯一约束、PK 与 table comments；按无 `CASCADE` 的显式 DROP 可暴露未来新增依赖。

## 2. 完整引用扫描

扫描命令（已执行，仅读取）：

```powershell
rtk proxy rg -n "assistant_issue_candidates|assistant_validation_discovery_reports|idx_aic_status_updated|create_issue_candidate|github_sync_issue_candidate|assistant_create_issue_candidate|validation_discovery_summary|IssueCandidateFactSource|issue-candidates|github-sync|validation-discovery|_check_in_values" backend frontend tests docs scripts -g "!**/node_modules/**"
rtk proxy rg -n "\bissue_candidates\b|\bvalidation_discovery_reports\b|github_sync_issue_candidate|create_issue_candidate|assistant_create_issue_candidate|IssueCandidateCreate|IssueCandidateGithubSyncRequest|IssueCandidateFactSource|idx_aic_status_updated" backend frontend tests --glob "!**/node_modules/**" --glob "!tests/aistock_validation/history/**"
```

### 2.1 Schema / DB bootstrap

| 文件 | 当前引用 | Phase 2 处置 |
|---|---|---|
| `backend/db/init_research_assistant_schema_20260521.py:546` | 创建 `assistant_issue_candidates`，字段包括 `candidate_id/title/severity/module/problem_statement/reproduce_command/evidence_refs/dedupe_key/status/github_issue_number/github_issue_url/github_sync_status/github_sync_json/proposed_by/reviewed_by/reviewed_at/created_at/updated_at`。 | 从 bootstrap DDL 删除该表定义；新库不再创建草稿表。 |
| `backend/db/init_research_assistant_schema_20260521.py:565` | `ck_aic_status`：`draft/needs_review/approved_for_github/rejected/synced_to_github/duplicate`。 | 随表删除，不单独保留。 |
| `backend/db/init_research_assistant_schema_20260521.py:566` | `uq_aic_dedupe UNIQUE(dedupe_key)`。 | 随表删除，不单独保留。 |
| `backend/db/init_research_assistant_schema_20260521.py:953` | 创建 `assistant_validation_discovery_reports`，字段包括 `discovery_report_id/run_date/title/status/summary_json/candidate_issue_refs/validation_run_refs/evidence_refs/created_at/updated_at`。 | 从 bootstrap DDL 删除该表定义；discovery summary 已由 Validation 候选派生。 |
| `backend/db/init_research_assistant_schema_20260521.py:981` | `idx_aic_status_updated ON assistant_issue_candidates(status, updated_at DESC)`。 | forward migration 先 `DROP INDEX IF EXISTS idx_aic_status_updated`，再 DROP 表。 |
| `backend/db/init_research_assistant_schema_20260521.py:1006-1007` | 两表 TABLE_COMMENTS 仍说明为“非权威对话草稿/解释缓存，待 Phase 2 退场”。 | 删除两项 comments；新语义改为“已退场”，不再存在 DB table comment。 |
| `backend/db/init_research_assistant_schema_20260521.py:55` | `_check_in_values` 是通用 check helper；本次两表没有通过该 helper 生成 status check。 | 无需调整 helper；只删除两表相关 DDL/comment/index。 |

约束/FK 结论：`assistant_issue_candidates` 有 PK、`ck_aic_status`、`uq_aic_dedupe`、`idx_aic_status_updated`；`assistant_validation_discovery_reports` 有 PK。扫描未发现其他表通过 FK 引用这两表；JSON 字段里的 `candidate_issue_refs` 是非约束文本/JSON，不构成 FK。

### 2.2 Repository / Service

| 文件 | 当前引用 | Phase 2 处置 |
|---|---|---|
| `backend/services/research_assistant/repository.py:175-180` | `TABLES["issue_candidates"] -> assistant_issue_candidates`。 | 删除 mapping；任何 `repository.*("issue_candidates")` 调用必须先退役。 |
| `backend/services/research_assistant/repository.py:307-313` | `TABLES["validation_discovery_reports"] -> assistant_validation_discovery_reports`。 | 删除 mapping；任何 seed/list/create 调用必须先退役。 |
| `backend/services/research_assistant/service.py:564-588` | `IssueCandidateFactSource` 包装 `ValidationPipelineCenterService`，当前 eager init。 | 保留为规范读源适配器，但改为 lazy init；新增 `search` 参数透传。 |
| `backend/services/research_assistant/service.py:1308` | `ResearchAssistantService.__init__` 默认立即创建 `IssueCandidateFactSource()`。 | 改为惰性工厂或 lazy adapter，避免普通 RA 实例化就初始化 Validation pipeline。 |
| `backend/services/research_assistant/service.py:1804` | `overview()` 对 `repository.counts("issue_candidates", "status")` 计数。 | 改为从 `issue_fact_source.issue_candidate_summary()` 派生，失败时显式 degraded；不读 RA 草稿表。 |
| `backend/services/research_assistant/service.py:2213-2253` | `list_pipeline_issue_candidates()` 已读 Validation 事实源，但 `search` 在 RA 侧对当前页做本地过滤。 | 将 `search` 下推到 Validation API / `IssueCandidateFactSource`，在分页前过滤，避免跨页漏搜。 |
| `backend/services/research_assistant/service.py:2262,6829,6852` | 返回 `assistant_draft_tables=[...]` 作为“待退场”提示。 | DROP 后改为 `retired_draft_tables=[...]` 或文案“已退场”；不得暗示可回退读取。 |
| `backend/services/research_assistant/service.py:6553-6592` | `create_issue_candidate()` dedupe + `repository.create_record("issue_candidates", ...)` 写草稿表。 | 退役为 no-storage 响应：`status="retired"/"standard_workflow_required"`、`storage_performed=false`、`direct_github_create_performed=false`、推荐 `report_bug/mcp_github_issue_create/mcp_github_issue_sync_bug`；不生成/写入 RA candidate。 |
| `backend/services/research_assistant/service.py:6644-6669` | `github_sync_issue_candidate()` 读取并更新 `issue_candidates.github_sync_*`。 | 改为无表 lookup 的 block-only 响应：任何 `candidate_id` 均提示走标准 workflow；不写 `github_sync_*`。 |
| `backend/services/research_assistant/service.py:6806-6856` | `validation_discovery_summary()` 已由 Validation 候选派生，失败时显式 degraded。 | 保持读链；删除/更新两表存在性文案，确保不可用时仍显式 degraded，不回退 RA 表。 |
| `backend/services/research_assistant/service.py:7224-7238` | seed `validation_discovery_reports` 初始草稿。 | 删除 seed 分支；`llm_discovery=not_started` 隔离要求转为测试/文档断言，不再落 DB。 |

### 2.3 API / MCP / Tool catalog

| 文件 | 当前引用 | Phase 2 处置 |
|---|---|---|
| `backend/routers/research_assistant.py:845-850` | `POST /research-assistant/issue-candidates` 调 `service.create_issue_candidate()`。 | 保留 endpoint 兼容但改为 no-storage “请走标准 workflow”响应，或返回明确 410/retired 语义；不写表。 |
| `backend/routers/research_assistant.py:853-864` | `GET /research-assistant/issue-candidates` 调 Validation 事实源视图。 | 保留；新增/透传 `search` 到 Validation API。 |
| `backend/routers/research_assistant.py:868-873` | `POST /issue-candidates/{candidate_id}/github-sync` 调 `service.github_sync_issue_candidate()`。 | 保留为 block-only/no-storage；不查表、不写 GitHub、不写 BUG JSON。 |
| `backend/routers/research_assistant.py:1025-1030` | `GET /validation-discovery/summary` 调派生 summary。 | 保留；仅去除已删除表的可回退文案。 |
| `backend/mcp/modules/research_assistant.py:22,121-125` | 注册 `assistant_create_issue_candidate`，HTTP POST `/issue-candidates`。 | 退役该 loopback 工具：从注册表与 export 移除；若保留兼容 stub，必须只返回 no-storage retired 提示，且不在 manifest 里暴露。 |
| `backend/mcp/tool_manifest.py:320` | `research_assistant` profile 暴露 `assistant_create_issue_candidate`。 | 从 manifest/profile 移除，避免 LLM/agent 继续选择 RA 自建候选工具。 |
| `backend/services/research_assistant/execution.py:682-698` | loopback `_execute_loopback_tool` 调 `create_issue_candidate()` 并返回“草稿已记录”。 | 删除该分支或改为 retired/no-storage；不调用 `IssueCandidateCreate` 写表。 |
| `backend/services/research_assistant/mcp_catalog_sync.py:142,160` | catalog sync 仍定义 `assistant_create_issue_candidate` 必填/摘要字段。 | 删除该工具的 schema hint/summary hint，或改为 retired 但不进入可选工具集合。 |
| `backend/services/research_assistant/models.py:456-489` | `IssueCandidateGithubSyncRequest` / `IssueCandidateCreate` request model。 | endpoint 兼容保留时可保留 request model；语义改为 no-storage request，不再映射 DB。若移除 POST，则同步删除未用 model。 |

### 2.4 Frontend

| 文件 | 当前引用 | Phase 2 处置 |
|---|---|---|
| `frontend/src/lib/research-assistant/api.ts:902` | `listIssueCandidates()` 调 RA GET endpoint。 | 保留，参数增加 `search` 后端下推；类型以 Validation candidate view 为准。 |
| `frontend/src/lib/research-assistant/api.ts:905` | `createIssueCandidate()` 调 POST `/issue-candidates`。 | UI 不再调用；client 若保留则返回 retired/no-storage 类型，不能被主流程误用。 |
| `frontend/src/lib/research-assistant/api.ts:908` | `githubSyncIssueCandidate()` 调 POST `.../github-sync`。 | UI 不再触发实际 sync；按钮只提示标准 workflow，client 兼容 block-only。 |
| `frontend/src/lib/research-assistant/api.ts:944` | `validationDiscoverySummary()` 调派生 summary。 | 保留。 |
| `frontend/src/app/research-assistant/issue-candidates/page.tsx:12-14,59,83` | 页面显示“非权威对话草稿/解释缓存，待 Phase 2 退场”；仍提及 `assistant_issue_candidates`。 | 改为“RA 自有候选草稿表已退场；正式候选来自 Validation/Nightly/issue workflow”，不再暗示表存在或可回退。 |
| `frontend/src/app/research-assistant/streams/page.tsx:13,50,67` | streams 页面仍提及 `assistant_validation_discovery_reports` “待 Phase 2 退场”。 | 改为“发现报告为 Validation 候选字段派生视图；RA discovery 草稿表已退场”。 |
| `frontend/tests/research-assistant/research-assistant.spec.ts:427` | fixture 仍包含 `assistant_create_issue_candidate`。 | 更新 fixture/断言：该工具不再出现在可选工具；若覆盖兼容 stub，断言 retired/no-storage。 |

### 2.5 Tests / static locks

| 文件 | 当前引用 | Phase 2 处置 |
|---|---|---|
| `backend/tests/research_assistant/test_schema_contract.py:66,86,355` | schema-contract 期望两表存在，并校验 `issue_candidates` 写列。 | 删除两表存在期望和写列校验；新增“schema 不包含两表/索引/comment”的断言。 |
| `backend/tests/research_assistant/test_api.py:143,179,259-264` | API 测试覆盖 `assistant_create_issue_candidate`、POST candidate、github-sync block。 | 改为 endpoint/tool retired/no-storage 断言；GitHub sync 仍必须 `direct_github_create_performed=false`。 |
| `backend/tests/research_assistant/test_service.py:618-663,770-803,1131` | service 测试依赖 create/dedupe/github-sync 存储和工具 catalog。 | 改为 no-storage/retired 断言；删除 dedupe 存储断言；新增工具不暴露或 stub 不写表断言。 |
| `backend/tests/research_assistant/test_pipeline_source_boundary.py:85,112,198-206,215-249` | 已有 read-source 与 static lock；仍有 RA 草稿 fixture 和 github-sync block 测试。 | 扩展为“读允许/写禁止”：允许 `IssueCandidateFactSource`、`ValidationPipelineCenterService`、finding_store 读；禁止 workflow/nightly/BUG JSON/GitHub 写；新增两表不存在与端点/工具退役。 |
| `backend/tests/research_assistant/test_pipeline_source_ui_static.py:8-26` | UI 静态测试仍要求表名/退场文案。 | 改为要求“已退场/正式事实源”文案，且主 UI 不把表当事实源。 |
| `backend/tests/research_assistant/test_proactive_report_generation.py:182` | 晨报 source_refs 不以 `assistant_issue_candidates:` 开头。 | 保留并新增新 source_ref 可被证据/grounding 识别。 |
| `backend/tests/mcp/test_research_assistant_module.py:73,124-129` | 期望 `assistant_create_issue_candidate` 注册并 POST。 | 改为不注册该工具，或兼容 stub 返回 retired/no-storage；推荐不注册。 |

## 3. DDL 与迁移机制

### 3.1 机制

- 使用现有 Research Assistant 迁移命名空间：`backend/db/migrations/ra_upgrade/`。该目录 README 要求迁移幂等、带 comment 覆盖、验证 DB 可重复运行，并把生产 DDL gate 单独报告。
- 新增 forward/rollback 文件：
  - `backend/db/migrations/ra_upgrade/009_retire_candidate_discovery_draft_tables.sql`
  - `backend/db/migrations/ra_upgrade/009_retire_candidate_discovery_draft_tables.rollback.sql`
- `backend/db/init_research_assistant_schema_20260521.py` 是新库 bootstrap 期望；实现阶段要从 DDL 列表中删除两表创建、索引和 comments，但不得把非幂等生产 DROP 混入 bootstrap。
- forward migration 使用 `DROP ... IF EXISTS` 且不使用 `CASCADE`；如未来出现未扫描到的依赖，应 fail fast，停止并补引用扫描，而不是静默级联删除。
- rollback 只重建空表结构与 comments/index，不恢复数据；这符合“直接 DROP”语义。DROP 前审计只保留证据摘要，不建立备份表、不导出新表。

### 3.2 Forward SQL 摘要

```sql
-- backend/db/migrations/ra_upgrade/009_retire_candidate_discovery_draft_tables.sql
-- BUG-423 Phase 2: RA no longer owns candidate/discovery draft storage.
-- Run with psql --single-transaction -v ON_ERROR_STOP=1.

DROP INDEX IF EXISTS idx_aic_status_updated;
DROP TABLE IF EXISTS assistant_validation_discovery_reports;
DROP TABLE IF EXISTS assistant_issue_candidates;
```

### 3.3 Rollback SQL 摘要

```sql
-- backend/db/migrations/ra_upgrade/009_retire_candidate_discovery_draft_tables.rollback.sql
-- Emergency compatibility rollback: recreates empty non-authoritative draft/cache tables only.
-- It does not restore dropped rows. Run with psql --single-transaction -v ON_ERROR_STOP=1.

CREATE TABLE IF NOT EXISTS assistant_issue_candidates (
    candidate_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    severity TEXT NOT NULL,
    module TEXT NOT NULL,
    problem_statement TEXT NOT NULL,
    reproduce_command TEXT,
    evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    dedupe_key TEXT,
    status TEXT NOT NULL DEFAULT 'needs_review',
    github_issue_number INTEGER,
    github_issue_url TEXT,
    github_sync_status TEXT NOT NULL DEFAULT 'not_requested',
    github_sync_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_by TEXT NOT NULL DEFAULT 'assistant',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_aic_status CHECK (status IN ('draft','needs_review','approved_for_github','rejected','synced_to_github','duplicate')),
    CONSTRAINT uq_aic_dedupe UNIQUE (dedupe_key)
);

CREATE TABLE IF NOT EXISTS assistant_validation_discovery_reports (
    discovery_report_id TEXT PRIMARY KEY,
    run_date DATE NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    candidate_issue_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    validation_run_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aic_status_updated
    ON assistant_issue_candidates(status, updated_at DESC);

COMMENT ON TABLE assistant_issue_candidates IS 'Non-authoritative conversation draft / explanation cache; retired by BUG-423 Phase 2. Formal submission must use AIstock issue workflow / Validation MCP.';
COMMENT ON TABLE assistant_validation_discovery_reports IS 'Non-authoritative conversation draft / explanation cache; retired by BUG-423 Phase 2. Discovery facts come from Validation/Nightly candidate sources.';
```

## 4. DROP 前只读审计

实现/上线前必须执行只读审计并把结果摘要写入 BUG JSON events 与 PR body。审计是证据，不是保留；不得创建备份表、不得导出成新表、不得将表继续作为回退事实源。

```sql
SELECT to_regclass('public.assistant_issue_candidates') AS assistant_issue_candidates;
SELECT to_regclass('public.assistant_validation_discovery_reports') AS assistant_validation_discovery_reports;

SELECT COUNT(*) AS assistant_issue_candidates_count FROM assistant_issue_candidates;
SELECT candidate_id, title, severity, module, status, github_sync_status, github_issue_number, created_at, updated_at
FROM assistant_issue_candidates
ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
LIMIT 20;

SELECT COUNT(*) AS assistant_validation_discovery_reports_count FROM assistant_validation_discovery_reports;
SELECT discovery_report_id, run_date, title, status, created_at, updated_at
FROM assistant_validation_discovery_reports
ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
LIMIT 20;

SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename IN ('assistant_issue_candidates','assistant_validation_discovery_reports')
ORDER BY tablename, indexname;

SELECT conname, contype, conrelid::regclass::text AS table_name
FROM pg_constraint
WHERE conrelid IN ('assistant_issue_candidates'::regclass, 'assistant_validation_discovery_reports'::regclass)
ORDER BY table_name, conname;
```

若任一表不存在，审计记录为 `table_absent_before_forward_migration`，forward migration 仍可幂等通过。

## 5. 草稿写入面退役方案

| 写入面 | 处置 |
|---|---|
| `create_issue_candidate()` | 不再写 `issue_candidates`；返回结构化 no-storage/retired 响应，明确 `standard_workflow_required=true`、`storage_performed=false`、`direct_github_create_performed=false`、`recommended_tools=["report_bug","mcp_github_issue_create","mcp_github_issue_sync_bug"]`。 |
| `github_sync_issue_candidate()` | 不再读取/更新 `issue_candidates.github_sync_*`；对 dry_run/formal 都 block-only，返回具体 reason：`ra_github_sync_retired_use_standard_workflow`。 |
| loopback `assistant_create_issue_candidate` | 推荐从 `backend/mcp/modules/research_assistant.py`、`backend/mcp/tool_manifest.py`、`mcp_catalog_sync.py`、执行器分支中移除，不再暴露给 LLM/agent；如保留兼容 stub，也只能返回 no-storage retired。 |
| `POST /research-assistant/issue-candidates` | 保留兼容入口但不存储；用户可见提示“正式提交必须走 AIstock issue workflow / Validation MCP”。 |
| `POST /research-assistant/issue-candidates/{candidate_id}/github-sync` | 保留兼容入口但不查表、不写表、不调用 gh；只返回 block-only 标准 workflow 提示。 |
| `validation_discovery_summary()` | 保持读取 Validation 候选派生；删除“两表待退场”表述，改为“两表已退场/不可作为事实源”。 |
| `overview()` | 不再统计 RA `issue_candidates`；改从 Validation summary 派生候选状态，失败时显式 degraded。 |
| seed catalog | 删除 `validation_discovery_reports` seed；不得创建替代表。 |

读源（BUG-418 已切换）：`GET /research-assistant/issue-candidates`、`validation_discovery_summary()`、streams、晨报继续消费 Validation/Nightly 事实源，不因两表删除而改变 source-of-truth。

## 6. 三条非 DDL 遗留的落地方式

### 6.1 VALIDATION_ISSUE confirmed 动作正向代理测试

- 现有规范路径：`backend/services/research_assistant/domain_ontology.py` 将 `VALIDATION_ISSUE` 指向 `aistock-validation`，plan/read/confirmed 工具包含 `report_bug`、`mcp_github_issue_create`、`mcp_github_issue_sync_bug`；`backend/services/research_assistant/tool_router.py` 对 sync/close/finish 命中 `mcp_github_issue_sync_bug`；`backend/mcp/modules/validation.py` 注册这三个 Validation-owned 工具。
- 新增正向测试：模拟用户请求“同步/关闭 BUG-423 GitHub issue”或 confirmed action，断言 route/preflight/action proposal 命中 `aistock-validation/mcp_github_issue_sync_bug` 或 `mcp_github_issue_create`，且没有调用/暴露 `assistant_create_issue_candidate`。
- 静态锁补充：RA 模块不得 import/exec `scripts.aistock_issue_workflow`、`scripts.nightly_bug_candidate_queue`、`scripts.aistock_mcp_server` 写函数，不得直接写 `tests/aistock_validation/bugs`，不得出现 `gh issue create`。

### 6.2 候选页 search 下推

- `IssueCandidateFactSource.issue_candidates(..., search=None)` 新增 `search` 参数。
- `ValidationPipelineCenterService.issue_candidates()` 与 `/api/v1/validation/issues/candidates` 新增 `search` query，在 pagination 前按 `candidate_id/title/module_id/severity/status/summary/actual/expected/fingerprint/source_path` 过滤。
- `ResearchAssistantService.list_pipeline_issue_candidates(search=...)` 只做参数传递，不再对当前页本地过滤。
- 测试构造超过一页的 fake candidates：目标项在第 2 页时，RA candidate page search 仍返回目标项，证明无跨页漏搜。

### 6.3 IssueCandidateFactSource 惰性初始化

- 当前 `IssueCandidateFactSource.__init__` eager 构造 `ValidationPipelineCenterService()`；改为保存 `pipeline_center_factory` 与 `_pipeline_center=None`，首次调用 `issue_candidates()` / `issue_candidate_summary()` 时再创建。
- `ResearchAssistantService.__init__` 默认注入 lazy adapter；测试仍可传 fake fact source，不 monkeypatch 生产模块。
- 新增测试：构造 `ResearchAssistantService(repository=fake)` 不触发 Validation pipeline 初始化；调用候选读取时才触发；传入 fake fact source 时完全不创建默认 pipeline。

## 7. 测试清单

计划全部离线可跑，使用依赖注入/fake provider，不启服务、不连生产 DB。

1. `backend/tests/research_assistant/test_schema_contract.py`
   - 断言 init schema 不再包含 `assistant_issue_candidates`、`assistant_validation_discovery_reports`、`idx_aic_status_updated`、`ck_aic_status`、两表 comment。
   - 删除 `issue_candidates` 写列校验。
2. 新增/扩展 RA DDL migration static tests
   - forward SQL 包含 `DROP INDEX IF EXISTS idx_aic_status_updated` 与两个 `DROP TABLE IF EXISTS`，不包含 `CASCADE`。
   - rollback SQL 可重建两张空表、约束、索引、comments。
   - 若框架支持，使用临时/dev transaction 执行 forward + rollback 并 rollback，不接生产 DB。
3. `backend/tests/research_assistant/test_api.py`
   - POST candidate 与 github-sync endpoint 返回 no-storage/block-only；`direct_github_create_performed=false`；不依赖 candidate row。
4. `backend/tests/research_assistant/test_service.py`
   - `create_issue_candidate()` 不写 repository `issue_candidates`；`github_sync_issue_candidate()` 不查/更新表；`overview()` 不读草稿表。
   - `assistant_create_issue_candidate` 不再出现在可选工具 catalog，或返回 retired/no-storage。
5. `backend/tests/mcp/test_research_assistant_module.py`
   - research-assistant MCP module 不注册 `assistant_create_issue_candidate`；如保留兼容 stub，断言它不 POST 存储。
6. `backend/tests/research_assistant/test_pipeline_source_boundary.py`
   - 两表已不存在 + 草稿写入端点/工具已退役。
   - VALIDATION_ISSUE confirmed action 命中 `aistock-validation` 标准工具。
   - 静态导入边界锁：允许 `IssueCandidateFactSource`、`ValidationPipelineCenterService`、finding_store 读；禁止 `aistock_issue_workflow`、`nightly_bug_candidate_queue`、`scripts.aistock_mcp_server` 写函数、BUG JSON 直接写、`gh issue create`。
7. `backend/tests/research_assistant/test_pipeline_source_ui_static.py`
   - 候选页/streams 文案改为“表已退场；正式事实源是 Validation/Nightly/issue workflow”。
8. `backend/tests/research_assistant/test_proactive_report_generation.py`
   - 晨报继续使用 `validation_issue_candidates:<candidate_id>`，证据卡/grounding 不误伤。
9. Validation search pushdown tests
   - `backend/tests/test_validation_pipeline_center_phase1.py` 或 RA boundary 测试新增 `search` 下推：分页前搜索。
10. 最终回归
   - `rtk proxy python -m nox -s l0`
   - `rtk proxy python -m nox -s research_assistant_backend`
   - `rtk proxy python -m nox -s research_assistant_mcp_contract`
   - `rtk proxy python -m nox -s ra_phase7_full_accept`
   - `rtk proxy python -m nox -s validation_module_registry_l0`
   - `rtk proxy git diff --check`
   - `rtk proxy ruff check <changed python files>`

## 8. CI / 测试 DB / 生产应用边界

### 8.1 CI / 测试 DB

- CI 只验证迁移 SQL 静态契约与测试 DB/dev transaction；不得连接生产。
- 若 nox 提供 dev DB transaction 环境，可执行：

```powershell
rtk proxy psql "%AISTOCK_DEV_DATABASE_URL%" -v ON_ERROR_STOP=1 --single-transaction -f backend/db/migrations/ra_upgrade/009_retire_candidate_discovery_draft_tables.sql
rtk proxy psql "%AISTOCK_DEV_DATABASE_URL%" -v ON_ERROR_STOP=1 --single-transaction -f backend/db/migrations/ra_upgrade/009_retire_candidate_discovery_draft_tables.rollback.sql
```

以上仅限明确标记的 dev/test DB；生产库由用户或既定迁移作业执行。

### 8.2 生产只读审计（由用户/迁移作业执行）

```powershell
psql "%AISTOCK_DATABASE_URL%" -v ON_ERROR_STOP=1 -f docs/operations/bug423_pre_drop_readonly_audit.sql
```

如果没有单独审计脚本，则使用第 4 节 SQL 逐条只读执行，并把计数/样本摘要写入 BUG JSON events 与 PR body。

### 8.3 生产 apply（Codex 不执行）

```powershell
psql "%AISTOCK_DATABASE_URL%" -v ON_ERROR_STOP=1 --single-transaction -f backend/db/migrations/ra_upgrade/009_retire_candidate_discovery_draft_tables.sql
```

执行前条件：PR 已合并、CI 绿、只读审计已记录、用户或既定迁移作业确认目标库不是测试替身。

### 8.4 生产 rollback（Codex 不执行）

```powershell
psql "%AISTOCK_DATABASE_URL%" -v ON_ERROR_STOP=1 --single-transaction -f backend/db/migrations/ra_upgrade/009_retire_candidate_discovery_draft_tables.rollback.sql
```

rollback 仅恢复空表结构以兼容紧急回滚代码；不恢复已 DROP 数据。如需要历史数据，只能依赖数据库级备份/PITR，而不是 RA 表继续保留。

## 9. 红线确认

- 本设计不在 RA 实现 DeepSeek/Nightly 调度。
- 本设计不允许 RA 直接写 `tests/aistock_validation/bugs`。
- 本设计不允许 RA 执行 `gh issue create`。
- 本设计不复制 `aistock_issue_workflow.py` / `nightly_bug_candidate_queue.py` 写入逻辑。
- Validation Center / Nightly / issue workflow 继续是唯一事实源。
- 除本次已授权、且仍待 Tier2 审核后实施的两表 DROP 外，不碰生产 backend/frontend 运行态、不启停服务、不自行连接生产 DB。
