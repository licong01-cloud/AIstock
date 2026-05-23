# AIstock BUG 编号与 Issue 创建统一入口设计方案（2026-05-23）

## 1. 目标

本方案用于解决 AIstock 多窗口、多 worktree 并行开发时 BUG 编号重复、BUG JSON 与 GitHub Issue 不一致、Issue 创建和修复流程混淆的问题。

设计目标保持精简：

1. 正式 BUG 只能通过 Validation MCP 创建。
2. BUG 编号由流水线统一分配，所有窗口共享同一个编号源。
3. 创建 BUG 不创建修复 worktree；只有认领修复时才创建 worktree 或 batch worktree。
4. BUG JSON 继续作为正式 registry 镜像提交到 Git，但由 MCP 生成和同步。
5. 同模块多个 BUG 可以 batch 修复和统一验证，但每个 BUG 保留独立编号、GitHub Issue、BUG JSON、commit 和 closure。

## 2. 范围

本方案覆盖：

- Validation MCP BUG 创建工具。
- BUG ID 分配逻辑。
- BUG JSON 生成和 GitHub Issue 同步。
- 现有开发规范中 BUG 创建和 batch 修复规则的精简更新。
- 当前历史重号 BUG 的后续整改阶段规划。

本方案不覆盖：

- 业务 BUG 的代码修复。
- 大规模重构 Validation Center。
- 新增复杂审批系统或过多 CI 门禁。

## 3. 当前问题

当前 `scripts/aistock_mcp_server.py` 中 BUG 编号由本地目录扫描后 `max + 1` 生成。该方式在单窗口场景可用，但在 AIstock 当前并行模式下不可靠：

- 不同 worktree 的 BUG JSON 视图可能不同。
- stale MCP root 可能落后于 `origin/main`。
- 其他窗口可能已有未提交或刚提交的 BUG JSON。
- GitHub Issue 可能已有 BUG 标记，但本地工作区尚未同步。

因此出现过 `BUG-102` 同时指向两个问题的情况，影响 MCP `get_bug_agent_context` 的准确性。

## 4. 总体方案

### 4.1 创建 BUG 的唯一入口

正式 BUG 创建统一走 Validation MCP：

```text
mcp_github_issue_create(create_github=true, ...)
```

或后续同义工具：

```text
validation_bug_create(...)
```

创建者不传入 `bug_id`，也不手工修改 `bug_id`。MCP 返回正式编号。

### 4.2 BUG 编号分配

采用轻量中心编号文件，避免引入复杂数据库和过度工程化。

新增文件：

```text
tests/aistock_validation/bugs/.bug_id_allocator.json
```

建议内容：

```json
{
  "schema_version": "aistock_bug_id_allocator_v1",
  "last_allocated": 105,
  "updated_at": "2026-05-23T00:00:00Z",
  "updated_by": "validation_mcp"
}
```

分配规则：

1. 创建前读取 allocator 文件和当前 BUG JSON。
2. `next_id = max(allocator.last_allocated, registry_max_bug_id) + 1`。
3. 在同一次写入中更新 allocator 和新 BUG JSON。
4. MCP 工具返回 `BUG-NNN`。

该方案不需要新 DB 表，适合当前本地唯一开发环境和少量并行窗口。后续如需要再升级为数据库 sequence。

### 4.3 GitHub Issue 同步

MCP 创建正式 BUG 时同步创建 GitHub Issue，并写入机器可读标记：

```md
<!-- aistock-bug-id: BUG-106 -->
<!-- aistock-registry-path: tests/aistock_validation/bugs/20260523_BUG-106-xxx.json -->
```

GitHub title 格式：

```text
BUG-106 P1: <title>
```

标签按现有规则设置：

- `bug`
- `aistock:bug`
- `severity:<p0|p1|p2|p3>`
- `status:open`
- `module:<module>`

### 4.4 BUG JSON 管理

正式 BUG JSON 继续提交到：

```text
tests/aistock_validation/bugs/*.json
```

规则：

1. BUG JSON 是正式 registry 镜像，必须 Git 跟踪。
2. MCP 创建 BUG 时生成 BUG JSON。
3. 创建 BUG 只提交 registry 变更，不创建修复 worktree。
4. 开始修复时，修复分支可更新 BUG JSON 的 `assigned_agent`、`fix_branch`、`fix_commit`、`verification_run_id`、`status`。

草稿问题不进入该目录。未确认问题可放到 `tmp/` 或 `debug_tools/`。

### 4.5 创建 BUG 与修复 BUG 分离

创建 BUG：

```text
发现问题 -> MCP 创建 BUG -> 分配 BUG ID -> 创建 GitHub Issue -> 写 BUG JSON -> registry-only 提交 main
```

修复 BUG：

```text
已登记 BUG -> assign_bug -> 创建 worktree/branch -> 修复 -> 验证 -> PR/合入
```

创建 BUG 不要求提前分配修复 worktree 或分支。

### 4.6 同模块 batch 修复

同模块、同风险域、同验证链路的多个 BUG 可以共享一个 batch worktree / branch：

```text
batch/<module>-<yyyymmdd>-bugs-106-107
```

要求：

1. 每个 BUG 已先通过 MCP 独立创建。
2. 每个 BUG 有独立 GitHub Issue 和 BUG JSON。
3. 每个 BUG 独立 commit。
4. batch PR 列出每个 BUG 的 commit 和验证结果。
5. 模块级验证可以统一执行，避免重复验证。

## 5. 流水线程序更新

### 5.1 MCP 编号分配函数

改造 `scripts/aistock_mcp_server.py`：

- `_next_bug_id()` 不再只依赖本地 `BUG_ROOT` 扫描。
- 新增 allocator 文件读写。
- 分配时同时考虑 allocator 与现有 BUG JSON 中最大编号。
- 写入 BUG JSON 后更新 allocator。

### 5.2 MCP 创建工具

更新：

- `report_bug()`
- `mcp_github_issue_create()`

创建正式 BUG 时：

1. 调用统一编号分配函数。
2. 生成 BUG JSON。
3. 若要求 GitHub 创建，则创建 GitHub Issue 并回填链接。
4. 返回 `bug_id`、`github_issue_number`、`github_issue_url`、`path`。

### 5.3 轻量一致性验证

新增或补充测试即可，不新增复杂门禁：

- 连续创建 BUG 时编号递增且不重复。
- allocator 文件存在时从 allocator 继续编号。
- registry 中存在更大编号时以 registry 最大编号为准。
- `mcp_github_issue_create(create_github=true)` 返回的 BUG JSON 与 GitHub marker 一致。

## 6. 规范更新

### 6.1 开发规范补充

在 `docs/standards/aistock_development_standard_v1.5_20260523.md` 中精简更新：

1. 正式 BUG 必须通过 Validation MCP 创建。
2. BUG 编号由流水线统一分配。
3. BUG JSON 是正式 registry 镜像，必须 Git 跟踪。
4. 创建 BUG 不创建修复 worktree。
5. 修复 BUG 或 batch 修复时再创建 worktree/branch。

### 6.2 Issue 流程规范补充

在 `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md` 中精简更新：

1. Issue 创建阶段只登记 BUG，不分配修复 worktree。
2. 认领修复阶段才写入 `assigned_agent`、`fix_branch`、必要时 `batch_id`。
3. 同模块 batch 继续保留，但 batch 只用于修复和验证，不用于共享 BUG 编号。

## 7. 历史重号整改阶段

历史重号不放在本次第一阶段实施中立即整改，原因是历史 BUG 已关联多个 GitHub Issue、closed/fixed/wontfix 状态和旧修复记录，直接改动容易影响追溯。

整改阶段安排：

- 阶段 1：先实现新 BUG 创建唯一编号，阻止新增重号。
- 阶段 2：单独做历史 registry cleanup，清理已知重复 BUG ID。
- 阶段 3：将历史 cleanup 结果同步 GitHub 标题、body 和 BUG JSON。

阶段 2 当前候选重复编号：

- `BUG-074`
- `BUG-085`
- `BUG-089`
- `BUG-090`
- `BUG-097`

这些历史整改必须走独立 registry cleanup 分支，不与业务 BUG 修复混合。

## 8. 验收矩阵

| 编号 | 验收项 | 验收方式 |
| --- | --- | --- |
| A1 | MCP 创建 BUG 不需要外部传入 `bug_id` | 单元测试 |
| A2 | 连续创建 BUG 编号递增且不重复 | 单元测试 |
| A3 | allocator 小于 registry 最大编号时，以下一个 registry 最大编号为准 | 单元测试 |
| A4 | 新 BUG JSON 文件名与 JSON 内 `bug_id` 一致 | 单元测试 |
| A5 | GitHub 创建路径回填 `github_issue_number` 和 `github_issue_url` | mock 单元测试 |
| A6 | GitHub body 包含 `aistock-bug-id` 和 `aistock-registry-path` marker | mock 单元测试 |
| A7 | 创建 BUG 不创建修复 worktree/branch | 代码检查和流程检查 |
| A8 | 规范文档明确 MCP 创建 BUG 与修复 worktree 分离 | 文档检查 |
| A9 | 同模块 batch 规则保留并强调每个 BUG 独立编号 | 文档检查 |

## 9. 合入前验证

实施分支合入 main 前必须完成：

1. `python -m pytest backend/tests/test_aistock_mcp_server.py backend/tests/scripts/test_aistock_mcp_github_issue_tools.py -q -p no:cacheprovider`
2. `git diff --check`
3. 手工检查本次改动不触碰生产后端、生产 DB 或前端运行服务。
4. 报告 `production_ddl_gate=noop`。

## 10. 预期结果

实施后，新 BUG 创建路径将形成单一入口：

```text
Validation MCP -> 流水线编号 -> GitHub Issue -> BUG JSON
```

所有窗口只要通过 MCP 创建 BUG，就会共享同一编号源，不再依赖各自 worktree 的本地 `max + 1`。同模块修复仍可 batch，以减少 worktree、上下文和验证成本。
