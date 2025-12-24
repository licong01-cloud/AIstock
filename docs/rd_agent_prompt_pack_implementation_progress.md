# RD-Agent 提示词模板管理（AIstock Phase 0）实施步骤与进度更新

> 目标：在 AIstock 侧落地 Phase 0（管理闭环）：**导入 -> 版本化 -> diff -> publish -> set-active**。
>
> 约束：本期不修改 RD-Agent 代码；RD-Agent 仍独立运行（WSL/conda）。

---

## 0. 当前范围与里程碑

### Milestone A：后端（FastAPI + DB）最小闭环

- **A1 DB 表结构**：`prompt_pack` / `prompt_pack_file` / `prompt_global_active`
- **A2 可选但建议**：`prompt_pack_event` / `prompt_pack_validation_run`
- **A3 API**（统一 `/api/v1`）：
  - `POST /api/v1/prompt-packs/validate-import-dir`
  - `POST /api/v1/prompt-packs/import-from-dir`
  - `GET /api/v1/prompt-packs`
  - `GET /api/v1/prompt-packs/{pack_id}`
  - `GET /api/v1/prompt-packs/active`
  - `GET /api/v1/prompt-packs/diff?from=A&to=B`
  - `POST /api/v1/prompt-packs/{pack_id}/publish`
  - `POST /api/v1/prompt-packs/{pack_id}/set-active`

### Milestone B：前端（导航 + 页面）最小闭环

- **B1 左侧导航**：新增一级 `RD-Agent管理`，二级 `RD-agent提示词模板管理`
- **B2 页面能力**：列表/详情/导入/diff/publish/set-active

---

## 1. 实施步骤（建议按顺序执行）

### Step 1：后端 DB schema

- [x] 新增建表脚本（`backend/db/init_prompt_schema.py`）
- [x] 确认与现有 `psycopg2` 连接池兼容（复用 `backend/db/pg_pool.py`）

### Step 2：后端模块划分

- [x] Router：`backend/routers/prompt_packs.py`
- [ ] Repository/Service：暂未拆分（Phase 0 先集中在 Router，后续如需扩展再抽离）
- [ ] Pydantic models：暂未单独建模（Phase 0 先返回 Dict）

### Step 3：API 接口实现（Phase 0）

- [x] validate-import-dir
- [x] import-from-dir
- [x] list/get/active
- [x] diff
- [x] publish/set-active

### Step 4：前端导航与页面

- [x] 左侧导航新增目录（`frontend/src/app/layout.tsx`）
- [x] pack 列表页（`/rd-agent/prompt-packs`）
- [x] pack 详情页（`/rd-agent/prompt-packs/[pack_id]`）
- [ ] 导入弹窗（当前为页面内输入框 + 按钮，后续可升级为弹窗）
- [x] diff 视图（与 active 对比，页面内展示 unified diff）
- [ ] publish / set-active（含二次确认，待完善交互）

---

## 2. 进度更新（日志）

### 2025-12-21

- **完成**：设计方案文档已更新为本期仅做 AIstock Phase 0，并明确导航结构。
- **完成**：创建本进度文档。
- **完成**：新增 DB 建表脚本 `backend/db/init_prompt_schema.py`（幂等建表）。
- **完成**：新增后端路由 `backend/routers/prompt_packs.py`（active/list/get 最小接口）。
- **完成**：将 `prompt_packs` 路由挂载到 FastAPI（`/api/v1/prompt-packs/*`）。
- **完成**：新增 allowlist：`backend/schema_registry/prompt_pack_allowlist.py`（必需 prompts 文件清单）。
- **完成**：实现接口 `POST /api/v1/prompt-packs/validate-import-dir`（目录预校验：meta + YAML parse + 缺失/多余统计）。
- **完成**：实现接口 `POST /api/v1/prompt-packs/import-from-dir`（导入落库为 draft，并写入审计事件）。
- **完成**：实现接口 `GET /api/v1/prompt-packs/diff?from=A&to=B`（文件级 + unified diff）。
- **完成**：实现接口 `POST /api/v1/prompt-packs/{pack_id}/publish`（publish gate：allowlist 全量性 + YAML parse，并落库 validation_run + event）。
- **完成**：实现接口 `POST /api/v1/prompt-packs/{pack_id}/set-active`（仅允许 published，并写入 set_active event）。
- **完成**：修复 JSONB 入库适配：使用 `psycopg2.extras.Json` 写入 `tags/report/metadata`，避免 psycopg2 类型转换错误。
- **完成**：前端左侧导航新增分组 `RD-Agent管理`，并挂载入口 `/rd-agent/prompt-packs`。
- **完成**：新增页面 `frontend/src/app/rd-agent/prompt-packs/page.tsx`（Phase 0 最小可用：列表/导入预校验/导入/publish/set-active/diff vs active）。
- **完成**：后端导入链路完善：统一错误返回结构（`detail.error.{code,message,details}`），meta 字段更完整落库（usage_scene/requirements/limitations/created_at 等），overwrite 策略改为不删除 pack（保留历史事件/校验记录），清空并重写 files，并记录 `overwrite_import` 事件。
- **完成**：overwrite 策略 A1：禁止覆盖当前 Active Pack（返回 `ACTIVE_PACK_LOCKED`），允许覆盖非 Active 的 published pack（覆盖后回到 draft，需要重新 publish）。
- **完成**：严格模式 meta 校验：`tags` 必须为 `list/dict/null`、`source` 必须为字符串；不满足则 `validate-import-dir` 给出 meta_error，`import-from-dir` 返回 `INVALID_META`。
- **完成**：`import-from-dir` 返回值补充 `meta` 摘要与 `active_pack_id`，便于前端导入后直接展示与判断覆盖策略。
- **完成**：`validate-import-dir` 返回值补充 `meta` 摘要（含解析后的 `created_at`），与导入返回结构对齐。
- **完成**：Meta 必填策略 S2：导入/预校验均要求 `usage_scene/requirements/limitations` 必填，不满足则提示缺失字段并拒绝导入。
- **进行中**：后端导入链路收尾（必要字段约束策略确认、import/validate 返回结构对齐）。

---

## 2.1 已开发内容总览（按模块）

### 后端（FastAPI）

- **已完成**：路由 `backend/routers/prompt_packs.py`
  - `GET /api/v1/prompt-packs/active`
  - `GET /api/v1/prompt-packs`
  - `GET /api/v1/prompt-packs/{pack_id}`
  - `GET /api/v1/prompt-packs/diff?from=A&to=B`
  - `POST /api/v1/prompt-packs/validate-import-dir`
  - `POST /api/v1/prompt-packs/import-from-dir`
  - `POST /api/v1/prompt-packs/{pack_id}/publish`
  - `POST /api/v1/prompt-packs/{pack_id}/set-active`
- **已完成**：导入/发布校验与策略
  - allowlist 全量性校验 + YAML parse 校验
  - 错误返回结构统一：`detail.error.{code,message,details}`
  - overwrite 策略 A1：禁止覆盖 active；允许覆盖 published（覆盖后回 draft）
  - JSONB 适配：`psycopg2.extras.Json`

### DB（PostgreSQL）

- **已完成**：`backend/db/init_prompt_schema.py`
  - `app.prompt_pack`
  - `app.prompt_pack_file`
  - `app.prompt_global_active`
  - `app.prompt_pack_event`
  - `app.prompt_pack_validation_run`

### 后端 schema_registry

- **已完成**：`backend/schema_registry/prompt_pack_allowlist.py`

### 前端（Next.js）

- **已完成**：左侧导航新增 `RD-Agent管理` -> `RD-agent提示词模板管理`
- **已完成**：页面 `frontend/src/app/rd-agent/prompt-packs/page.tsx`（Phase 0 MVP）
- **已完成**：页面交互完善：publish/set-active 二次确认、导入 overwrite 开关、后端统一错误结构解析、成功/错误提示分离。
- **已完成**：新增 pack 详情页 `frontend/src/app/rd-agent/prompt-packs/[pack_id]/page.tsx`，并从列表页跳转。
- **已完成**：diff 展示优化（P2）：支持选择 from/to（含 active）、changed-only 筛选、按文件折叠展示 unified diff。

### 文档

- **已完成**：本实施步骤与进度文档持续更新：`AIstock/docs/rd_agent_prompt_pack_implementation_progress.md`

---

## 3. 风险与阻塞

- **allowlist 维护**：初期建议人工维护 allowlist 文件清单；后续再做自动扫描。
- **权限/审计**：若系统后续要支持多用户，需要补齐 `created_by/updated_by` 的来源与鉴权。

---

## 4. 约定（便于后续 Phase 1）

- Pack 文件键建议使用 RD-Agent 逻辑路径映射后的相对文件路径（如 `scenarios/qlib/experiment/prompts.yaml`），以便未来无缝生成 cache 包。
