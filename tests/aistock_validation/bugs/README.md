# AIstock Validation Bug Registry

物理 bug 注册表。Validation Center backend 已就位的 schema (`aistock_validation_bug_v1`)
和 list_bugs / get_bug / bug_agent_context API 在 `backend/services/validation/finding_store.py`
读取本目录下所有 `*.json` 文件 (要求 `schema_version=aistock_validation_bug_v1`)。

## File Naming

```
<YYYYMMDD>_BUG-<NNN>-<short-slug>.json
```

例如:

- `20260510_BUG-001-archive-handler-subclass-crash.json`
- `20260510_BUG-006-cash-ledger-schema-divergence.json`

`<NNN>` 为整个 registry 全局递增编号，跨日期不重置。`<YYYYMMDD>` 是 *发现* 日期，不是修复日期。

## Schema (aistock_validation_bug_v1)

参考 `backend/services/validation/finding_store.py::_normalize_bug` 实际读取字段。
注意 API 路径用 kebab-case `agent-context`（FastAPI router 在 `backend/routers/validation.py`
注册的 path 是 `/bugs/{bug_id}/agent-context`），但 Python 函数名仍是 snake_case
`get_bug_agent_context`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `schema_version` | string | 必须 `aistock_validation_bug_v1` |
| `bug_id` | string | `BUG-<NNN>` |
| `title` | string | 一句话标题 |
| `description` | string | 完整描述 (可多行) |
| `module` | string | 受影响模块名 (e.g. `qe_archive`, `rl_execution`) |
| `severity` | enum | `P0` / `P1` / `P2` / `P3` |
| `risk_area` | string | 风险面 (e.g. `data_correctness`, `runtime_crash`) |
| `status` | enum | `open` / `in_progress` / `fixed` / `verified` / `wontfix` |
| `trigger_condition` | object | 触发条件 (e.g. `{"runs": "subclass_definition"}`) |
| `reproduce_command` | string | 一行复现命令 (pytest / python -c …) |
| `failing_run_id` | string\|null | 失败运行 ID (qe_run_id / pytest run id) |
| `evidence_uris` | string[] | 证据指针 (drawer ID / file path / commit hash) |
| `fingerprint` | string | 去重指纹 |
| `assigned_agent` | string | `claude_code` / `codex_app` / `human` |
| `fix_branch` | string\|null | 修复分支 |
| `fix_commit` | string\|null | 修复 commit SHA |
| `verification_run_id` | string\|null | verify 运行 ID |
| `created_at` | ISO datetime | 入库时间 |
| `first_seen_at` | ISO datetime | 首次观察时间 |
| `last_seen_at` | ISO datetime | 最近观察时间 |
| `fixed_at` | ISO datetime\|null | 修复时间 |
| `closed_at` | ISO datetime\|null | 关闭时间 |
| `allowed_write_scope` | string[] | 允许 agent 修改的文件路径 |
| `suspected_modules` | string[] | 涉及模块/文件清单 |
| `required_verification` | string[] | 关闭前必须执行的验证命令 |
| `closure_requirements` | string[] | 关闭前必须满足的条件 |
| `events` | object[] | 状态变更日志 (timestamp/actor/action/note) |

> 没有列在上表的字段会被 `_normalize_bug` 忽略。如果需要扩展，请提 PR 同步 schema +
> finding_store.py + 本 README。

## Status Machine

```
open -> in_progress -> fixed -> verified
                              |
                              +-> wontfix (closed)
```

- `open`: 已发现并入库，未分配或未开始修复
- `in_progress`: 修复中 (`assigned_agent` 必填)
- `fixed`: 修复 commit 已 push (`fix_commit` 必填)，但尚未独立 verify
- `verified`: 第二方独立验证通过 (`verification_run_id` 必填)，可结案
- `wontfix`: 决定不修 (events 必须记录决策原因)

## Discover → Close Workflow

1. **Discover** — 任何来源 (Codex review / nox 失败 / 用户报告 / 监控告警 / cross-tool drawer)
2. **Register** — 写入本目录 `*.json` (`status=open`)，必要时 `git add` 提交到 main
3. **Assign** — `assigned_agent` 写入 + `status=in_progress`
4. **Fix** — 修复 commit 推送 → 状态 `fixed`，`fix_commit` 必填
5. **Verify** — 独立的 reviewer (Codex / Claude / 人) 跑 `required_verification` 通过
   → `status=verified`，`verification_run_id` 必填
6. **Close** — `closed_at` 填写。`verified` / `wontfix` 都视为关闭态

## AI Agent Integration

Validation Center backend 已暴露：

```
GET  /api/v1/validation/bugs                    # 列表 (filter by severity/status/module/agent)
GET  /api/v1/validation/bugs/{bug_id}           # 详情 + agent_context (字段)
GET  /api/v1/validation/bugs/{bug_id}/agent-context  # 修复任务上下文 (kebab-case path)
GET  /api/v1/validation/bugs/summary            # 严重度/状态/模块分布
```

`agent_context` 字段（Python 字典 key 用下划线）包含 reproduce_command / allowed_write_scope / required_verification /
closure_requirements，可直接喂给 Claude Code 或 Codex App 作为修复任务的输入。

## Cross-Tool Review 入库流程

(占位 — Stage 3 MCP server 上线后填充)

Codex review 发现的 finding 通过 MCP `report_bug` 入库 → 自动写入本目录 →
mempalace cross-tool drawer 通知。

当前阶段 (Stage 1)：人工/AI 直接 Write 文件 + git commit 入库。

## 修复责任 + Verify 双轮 Review

- `assigned_agent` 是修复责任方 (Claude / Codex / 人)
- Verify 必须由不同主体执行 (Codex 修的由 Claude 验，反之亦然；人修的可以由任一 agent 验)
- Verify 通过前不允许 `closed_at`
