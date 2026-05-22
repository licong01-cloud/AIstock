# AIstock Issue 修复与并行开发隔离规范

> 版本：v1.3
> 更新日期：2026-05-23
> 状态：生效
> 适用范围：AIstock 所有 BUG / GitHub Issue / MCP issue / 并行 Codex 或 Claude Code 开发窗口
> 规范位置：`docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md`
> v1.2 变更：移除跨工具强制通讯(改为 opt-in)、删除与 v1.5 标准重复的规则(改为引用)、新增同模块批量执行

## 1. 目的

定义 issue 从发现到关闭的标准流程，以及多窗口并行开发时的隔离规则。

与其他规范的关系：
- **P0/P1 红线、fail-fast、生产端口、DB comment、文档归属、DESIGN-COMPLIANCE** → 参见 `aistock_development_standard_v1.5_20260521.md`
- **Worktree/分支隔离、生产同步规则** → 参见 `docs/codex_project_memory.md`
- 本文仅定义 **issue 特有的** 生命周期、角色、allowed_write_scope、批量执行和 MCP 持久化

## 2. 基本原则

1. **一 issue 一分支一 worktree**：修复必须在独立 worktree 中完成，不得在脏的 `F:\Dev\AIstock` 生产/同步目录直接开发。（同模块批量执行见 §6）
2. **先 scope 后编码**：任何修复前必须声明 `allowed_write_scope`，未声明文件默认不可改。
3. **同文件不并行写**：多个窗口需要改同一文件时，必须指定唯一实现者；其他窗口只做 review、测试或非重叠文件。
4. **禁止 sweeping commit**：只 stage 当前 issue 的文件；不得把无关 Paper、QE、frontend build cache、test-results 等混入。
5. **GitHub Issue 同步**：参见 v1.5 §5.18, §6.16 [ISSUE-GITHUB-SYNC-001]。
6. **禁止简化交付**：参见 v1.5 §15.3 [DESIGN-COMPLIANCE-001]。
7. **跨工具通讯为 opt-in**：仅在用户明确要求时通过 MemPalace 协调；不主动 poll channel，不发 `[DECISION]`/`[REVIEW]`/`[INFO]`/`[ACK]`。


## 2.1 Issue 处理分级与上下文预算

Triage 阶段必须写入或口头声明 `process_level` / `task_tier`，用于决定上下文注入、验证深度和是否批量处理：

| 层级 | 适用范围 | 默认流程 | 上下文预算 |
|---|---|---|---|
| S / T0 | 小 UI、文案、明显依赖缺失、小测试修正、非核心 P2/P3 | 快速修复、针对性验证、简短 handoff | 不加载完整规范/设计/历史，只读相关文件片段和最小规则摘要 |
| M / T1 | 单模块 P1/P2 bug、普通业务逻辑问题 | 标准 issue 流程、closure 验证、GitHub/BUG 同步 | 读取 issue agent context、allowed_write_scope、相关模块片段 |
| B / T2 | 同模块多个 issue，文件和验证链路重叠 | batch worktree、每 issue 独立 commit、统一模块验证 | 使用 Batch Context Pack，不重复加载同一模块背景 |
| L / T3 | 跨模块、设计驱动、架构调整、生产 DDL/依赖/资产风险 | 完整设计验收矩阵和分阶段验证 | 只加载当前阶段设计验收索引和相关章节，不反复注入全文 |

禁止把所有 issue 都按 L/T3 处理；也禁止用 S/T0 绕过 P0/P1 红线、GitHub 同步、allowed_write_scope 或必须执行的业务验证。

## 3. 角色定义

| 角色 | 职责 | 是否可写代码 |
|---|---|---|
| Reporter | 发现问题，创建 BUG JSON / GitHub Issue，提供复现和证据 | 否，除非另行认领 |
| Triage Owner | 做 RCA，确认严重级别、影响模块、写入范围、验收标准、是否可批量 | 默认否 |
| Fix Owner | 在独立 worktree / branch 中实现修复 | 是，仅限 `allowed_write_scope` |
| Validator | 运行测试、平台验证、业务路径验证，记录 evidence | 默认否，可写验证记录 |
| Production Operator | 同步生产 checkout、重启服务、观察运行时 | 仅用户明确授权后执行 |

一个窗口可以承担多个角色，但必须在 BUG JSON 或 GitHub Issue 中明确。

## 4. Issue 生命周期

### 4.1 Open

创建 BUG JSON 和 GitHub Issue 必须同步完成（参见 v1.5 §6.16）。

BUG JSON 必须包含：`bug_id`、`title`、`module`、`severity`、`risk_area`、`status=open`、`description`、`reproduce_command`、`suspected_modules`、`required_verification`、`closure_requirements`、`allowed_write_scope`、`non_goals`。

若 `allowed_write_scope` 还不能确定，issue 只能进入 RCA / triage，不能进入代码修复。

### 4.2 Triaged

Triage 完成后补齐：根因判断、影响模块、最小修复范围、非目标、是否需要拆分子 issue、是否可与其他 issue 批量执行。

### 4.3 In Progress

认领前必须：
1. 从最新 `origin/main` 创建独立 worktree
2. 创建 issue 专属 branch
3. 写入 `assigned_agent`、`fix_branch`、`worktree_path`
4. 确认 `allowed_write_scope` 非空
5. 确认 GitHub Issue 链接仍存在

推荐命名：
```text
worktree: F:\Dev\AIstock_worktrees\bug-039-qe-data-freshness
branch:   bug/BUG-039-qe-data-freshness
```

### 4.4 Review Ready

提交 PR 前必须：
- `git status --short --branch` 只显示当前 issue 相关改动
- `git diff --check` 通过
- 所有改动文件都在 `allowed_write_scope` 内
- 已运行 issue 要求的测试
- 若来源于设计文档，已提交验收矩阵（参见 DESIGN-COMPLIANCE-001）
- PR title/body 引用 `BUG-NNN` 和 GitHub Issue
- 明确声明是否触碰生产 `8001/3000`、DB 写入、migration、QMT、Paper live runtime

### 4.5 Fixed

PR 合入后标记 `fixed`，记录：fix commit、PR/GitHub Issue 链接、测试结果、合入分支、是否需要用户同步生产。

### 4.6 Verified

平台验证通过后标记 `verified`。证据可来自自动化测试、Validation Center run record、临时 dev port smoke、E2E、人工验收。

### 4.7 Closed

关闭前：closure requirements 全部完成、已复核 DESIGN-COMPLIANCE-001、GitHub Issue 与 BUG JSON 状态同步、无未提交的 source-of-truth JSON 修改。

## 5. allowed_write_scope

`allowed_write_scope` 是 issue 修复的写入合同：

```json
{
  "allowed_write_scope": [
    "backend/services/quantevolver/factor_universe_mask_service.py",
    "backend/tests/test_stock_universe_pit_service.py"
  ]
}
```

规则：
- 精确文件优先于目录；新增测试目录可声明为目录 scope
- 若发现必须修改 scope 外文件，必须先更新 issue，再继续编码
- 以下高冲突文件/目录需谨慎，涉及时应评估是否需要串行排队：
  `backend/main.py`、`backend/db/`、`backend/migrations/`、`backend/services/quantevolver/config_composer.py`、`backend/services/strategy_package/`、`backend/services/paper_trading_v2/` live/runtime 路径、`frontend/src/app/*/layout.tsx`、`frontend/next.config.mjs`、`noxfile.py`、`.github/workflows/`、`docs/standards/`

## 6. 同模块批量执行

规则依据见 v1.5 §23 [ISSUE-BATCH-CONTEXT-001]。

同一模块的多个 issue 可通过 `batch_id` 合并执行：

```json
{
  "batch_id": "BATCH-<MODULE>-<YYYYMMDD>",
  "batch_issues": ["BUG-NNN", "BUG-NNN"],
  "batch_strategy": "shared_worktree"
}
```

合并规则：
- 同 `batch_id` 共享一个 worktree、一次上下文加载、一次回归测试
- 每个 issue 仍独立 commit、独立 GitHub Issue、独立 closure
- batch 内按 issue 顺序逐个修复，每个 fix commit 引用对应 BUG-NNN
- 不可合并：GitHub Issue 创建、commit 粒度、closure requirements 逐项满足

## 7. Git 与提交规范

### 7.1 开始前

```bash
git fetch origin
git status --short --branch
git branch --show-current
git log --oneline -5
```

若当前目录是 `F:\Dev\AIstock` 且有脏改动，不得直接修复 issue。

### 7.2 Commit message

```text
fix(qe): isolate backtest freshness policy

Refs BUG-039
Fixes #23
```

### 7.3 PR checklist

PR 必须说明：Linked BUG/Issue、文件是否全部在 scope 内、测试结果、是否改 DB schema、是否触碰生产 8001/3000、是否需要用户重启、是否有后续 issue。若修改依赖清单，必须声明合入后 `production_frontend_dependency_gate` / `production_backend_dependency_gate` 是否 required。

## 8. MCP Server 持久化要求

### 8.1 创建 issue

`mcp_github_issue_create` 应支持：`github_issue_number`、`github_issue_url`、`github_sync_state`、`allowed_write_scope`、`non_goals`、`required_verification`、`closure_requirements`、`batch_id`（可选）。

若缺失 `allowed_write_scope`，标记 `workflow_gate: triage_only_until_allowed_write_scope_is_set`。

若 GitHub Issue 创建或同步失败，工具必须 fail-fast 返回错误。

### 8.2 认领 issue

`assign_bug` 应支持：`worktree_path`、`fix_branch`、`assigned_agent`。

若 issue 没有 `allowed_write_scope`，MCP 应拒绝进入 `in_progress`。

### 8.3 修复前校验

提供 MCP 工具或脚本检查：当前 branch 是否匹配、diff 文件是否在 scope 内、是否触碰高冲突文件、是否有未声明的 DB migration。

### 8.4 关闭前校验

检查：`fix_commit` 非空、验证记录非空、`closure_requirements` 已逐项满足、GitHub Issue 与 BUG JSON 同步、生产同步状态。

## 9. CI / 流水线校验

### 9.1 Warning 阶段
- PR 未引用 `BUG-NNN`、BUG JSON 没有 `allowed_write_scope`、diff 文件超出 scope、修改高冲突文件

### 9.2 Blocking 阶段
- P0/P1 issue 修复无 linked BUG、diff 超出 `allowed_write_scope`、改 DB schema 缺 comment/migration evidence、改 Paper live/trading/QE runtime 无对应测试、关闭 issue 前缺验证记录、新建 BUG JSON 缺 `github_issue_number`

## 10. BUG-039 类跨模块问题执行模板

1. 创建 BUG，声明 risk area
2. Triage 确认模块边界
3. 拆分写入范围：QE backend / factor cache / Paper readiness / frontend / tests
4. 同文件不并行，不重叠文件可并行
5. `config_composer.py` 等共享文件由唯一 Fix Owner 写
6. 验证时同时证明：QE 历史窗口可命中缓存、Paper 最新数据 fail-fast 不被放宽
7. 合入后先 `fixed`，平台验证后再 `verified`

## 11. Agent 执行提示

1. 先判定 `process_level/task_tier`、模块、风险等级和阶段；只读取 BUG JSON / GitHub Issue / 相关 docs 的必要片段
2. 确认当前 worktree 和 branch，不在脏生产 checkout 中开发
3. 不扩大 scope，不 revert 其他窗口的改动
4. 不重启 `8001/3000` 除非用户明确要求
5. 同模块多 issue 检查 batch_id，默认优先合并执行；使用 Batch Context Pack，避免重复提示词和重复验证

## 12. 后续落地

1. **MCP 字段增强**：扩展 issue create/assign/sync 工具字段
2. **Scope guardrail**：新增 diff-vs-allowed-write-scope 检查脚本
3. **PR template / GitHub Action**：要求 PR 填写 BUG、scope、验证、生产触碰声明
