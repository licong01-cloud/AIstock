# AIstock Bug Registry Workflow (2026-05-10)

物理 bug 注册表的目录约定、状态机、AI agent 接入路径、cross-tool review 入库流程
和修复责任 + verify 双轮 review 协议。

## 1. 目录与命名

- **物理目录**: `tests/aistock_validation/bugs/`
- **schema**: `aistock_validation_bug_v1` (必填 `schema_version` 字段)
- **文件命名**: `<YYYYMMDD>_BUG-<NNN>-<short-slug>.json`
  - `<YYYYMMDD>` 为 *发现* 日期，`<NNN>` 为 registry 全局递增编号
  - slug 用 kebab-case，描述受影响模块/场景

示例：

```
20260510_BUG-001-archive-handler-subclass-crash.json
20260415_BUG-012-hmm-coefficient-coverage-keyerror.json
```

## 2. Schema 字段详解

完整字段定义见 `tests/aistock_validation/bugs/README.md`。Validation Center backend
的 `backend/services/validation/finding_store.py::_normalize_bug` 是 source of truth。

关键字段语义提示：

| 字段 | 用途 |
|------|------|
| `severity` | P0=立即阻塞主线 / P1=本周修 / P2=本月修 / P3=有空再修 |
| `risk_area` | runtime_crash / data_correctness / observability / performance / feature_gap / design_clarity |
| `status` | 状态机见 §3 |
| `assigned_agent` | 修复责任方 (claude_code / codex_app / human) |
| `evidence_uris[]` | drawer ID / commit SHA / file path / log path 任意 URI |
| `allowed_write_scope[]` | 白名单：修复 agent 只能改这些路径 |
| `required_verification[]` | 必须执行的 verify 命令 |
| `closure_requirements[]` | 关闭前必须满足的条件清单 |
| `events[]` | 状态变更日志 (timestamp/actor/action/note) |

## 3. 状态机

```
open ─────► in_progress ─────► fixed ─────► verified
   │                                │
   └────────────────────────────────┴────► wontfix
```

| 状态 | 必填字段 | 进入条件 |
|------|----------|----------|
| `open` | severity, module, title, description, fingerprint | 入库即 open |
| `in_progress` | + assigned_agent, fix_branch | agent 接单 |
| `fixed` | + fix_commit, fixed_at | 修复 commit 已 push |
| `verified` | + verification_run_id | 独立 reviewer 验证通过 |
| `wontfix` | + events 末条记录决策原因 | 决定不修 |

**重要**：`fixed` 不等于 `closed`。`closed_at` 仅在 `verified` / `wontfix` 时填写。

## 4. Discover → Close 工作流

### 4.1 Discover

bug 来源（任一）：
- Codex App / Claude Code 在 cross-tool review 中发现
- nox L0/L1/L2 流水线失败
- pre-commit hook / Semgrep guardrail 命中
- 监控告警 (生产/dev DB 异常)
- 用户报告 / 内部测试

### 4.2 Register

在本 worktree 写入 `tests/aistock_validation/bugs/<file>.json`，提交到 main。
当前阶段（Stage 1）由人工/AI agent 直接 Write + git commit。

Stage 3 后由 MCP `report_bug` 工具自动入库。

### 4.3 Assign

- 显式 PR / drawer 派发：派发方在 events 添加一条 `assign` 记录，更新 `assigned_agent`
  + `status=in_progress`
- 自助接单：agent 主动选 open 项，在 events 添加 `claim`，更新两字段

### 4.4 Fix

修复 commit push 到 `fix_branch` 后，更新：
- `status=fixed`
- `fix_commit=<SHA>`
- `fixed_at=<ISO>`
- `events[]` 追加 `fixed` 事件

### 4.5 Verify

第二方独立 reviewer 跑 `required_verification` 全部通过后：
- `status=verified`
- `verification_run_id=<run_id|drawer_id|test_run_id>`
- `events[]` 追加 `verified` 事件

### 4.6 Close

`verified` 或 `wontfix` 后，填 `closed_at`，bug 进入只读归档状态。

## 5. AI Agent 接入

Validation Center backend 已暴露：

```
GET  /api/v1/validation/bugs                              # 列表 (severity/status/module/agent filter)
GET  /api/v1/validation/bugs/{bug_id}                     # 详情 + agent_context (字段)
GET  /api/v1/validation/bugs/{bug_id}/agent-context       # 修复任务上下文 (kebab-case path)
GET  /api/v1/validation/bugs/summary                      # 严重度/状态/模块分布
```

注意：FastAPI 路由用 kebab-case `agent-context`，但响应 JSON 字段 / Python
函数名仍是 snake_case `agent_context` / `get_bug_agent_context`。

`bug_agent_context` 返回 ：
- `problem_statement`
- `reproduce_command`
- `evidence_uris[]`
- `allowed_write_scope[]`
- `suspected_modules[]`
- `required_verification[]`
- `closure_requirements[]`

可直接喂给 Claude Code 或 Codex App 作为修复任务的输入。

Stage 3 MCP server 上线后，agent 还可通过 MCP 工具：
- `list_bugs` / `get_bug` / `bug_agent_context` (read-only)
- `report_bug` / `update_bug_status` / `assign_bug` (write，需权限)

## 6. Cross-Tool Review 入库流程

当前阶段（Stage 1）：

1. Codex 在 review drawer 中提出 finding
2. Claude Code 战略 session 读取 drawer，决定哪些进入 bug registry
3. 战略 session 派发到对应 worktree (本次是 pipeline-foundation)
4. worktree lead 写入 bugs/*.json，git commit + push
5. 战略 session 在 cross-tool drawer 通知入库结果

Stage 3+ 自动化：

1. Codex 通过 MCP `report_bug` 直接入库
2. Validation Center backend 触发 webhook 通知 Claude Code 战略 session
3. 战略 session 决定 assign / wontfix / merge_with_existing
4. 后续状态变更（fix / verify）双方都通过 MCP 同步

## 7. 修复责任 + Verify 双轮 Review

**核心原则**：修复者不能自验。

- `assigned_agent` 是修复责任方 (Claude / Codex / 人)
- `verification_run_id` 必须由 `assigned_agent` 之外的主体产生
  - Codex 修的 → Claude / 人验
  - Claude 修的 → Codex / 人验
  - 人修的 → 任一 agent 自动化验证 + 人 review
- Verify 通过前不允许 `closed_at`
- 双轮 review 失败时：状态回到 `in_progress` + events 记录失败原因 + reviewer 反馈

## 8. 与现有 Validation 流水线的关系

- `tests/aistock_validation/catalog/`: 测试矩阵 (yaml)
- `tests/aistock_validation/runs/`: 测试运行记录
- `tests/aistock_validation/history/`: 历史证据 (markdown)
- `tests/aistock_validation/modules/`: 模块特定测试
- `tests/aistock_validation/templates/`: 报告模板
- **`tests/aistock_validation/bugs/`** (本目录): 缺陷注册表

`finding_store.py` 同时读取本目录的 bug 文件和 guardrail / legacy_inventory tmp 输出，
统一暴露为 Validation Center API。

## 9. 后续 Stage 衔接

| Stage | 范围 | 与 bug registry 关系 |
|-------|------|-----------------------|
| Stage 2 | nox/catalog 注册新模块 | 新模块的 nox 失败自动开 BUG |
| Stage 3 | MCP server 暴露 Validation Center | `report_bug` / `update_bug_status` 工具 |
| Stage 4 | CI/CD GitHub Actions | PR 触发的失败转 BUG 入库 |
| Stage 5 | DR snapshot | (与本 registry 无直接交互) |
| Stage 6 | 整体回归 + Phase 3 联调 | 全 bug verified 才允许 release |

---

**Source of truth**: 本文档约束 *流程*。schema 字段 source of truth 在
`backend/services/validation/finding_store.py`。两者变化时必须同步 PR。
