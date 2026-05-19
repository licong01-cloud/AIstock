# AIstock Issue 修复与并行开发隔离规范

> 版本：v1.1
> 更新日期：2026-05-19
> 状态：规范草案；先作为人工执行规范落地，不创建治理 issue
> 适用范围：AIstock 所有 BUG / GitHub Issue / MCP issue / 并行 Codex 或 Claude Code 开发窗口
> 规范位置：`docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md`

## 1. 目的

AIstock 经常有多个窗口同时开发不同模块。为了避免 issue 修复与新功能开发互相覆盖、抢改同一文件、混入生产目录脏改动，所有 issue 修复必须按本文执行。

本文定义：

- issue 从发现、认领、修复、验证到关闭的标准流程。
- 多窗口并行开发时的 worktree、branch、写入范围和 integrator 规则。
- MCP bug registry / GitHub Issues / Validation Center / CI 流水线应持久化和校验的字段。
- 代码合入、平台验证、生产同步和运行时重启的边界。

## 2. 基本原则

1. **GitHub Issue 与 BUG JSON 必须同源同步**：新增 issue 必须在同一流程创建/同步 GitHub Issue；BUG JSON 是本地可读索引和 Validation Center 缓存，不允许把未链接 GitHub 的 BUG JSON 提交进主线。
2. **一 issue 一分支一 worktree**：修复必须在独立 worktree 中完成，不得在脏的 `F:\Dev\AIstock` 生产/同步目录直接开发。
3. **先 scope 后编码**：任何修复前必须声明 `allowed_write_scope`，未声明文件默认不可改。
4. **同文件不并行写**：多个窗口需要改同一文件时，必须指定唯一实现者；其他窗口只做 review、测试或非重叠文件。
5. **共享文件由 integrator 控制**：全局入口、迁移、配置、schema、核心 composer 等高冲突文件只能由一个集成窗口写。
6. **代码合入与运行时激活分离**：PR 合入不等于生产生效；生产 checkout 同步、`8001/3000` 重启必须另行确认。
7. **验证不能依赖生产服务**：默认使用测试命令或临时 dev port，例如 backend `8013`，不把生产 `8001` 当作唯一证明路径。
8. **禁止 sweeping commit**：只 stage 当前 issue 的文件；不得把无关 Paper、QE、frontend build cache、test-results 等混入。

## 3. 角色定义

| 角色 | 职责 | 是否可写代码 |
|---|---|---|
| Reporter | 发现问题，创建 BUG JSON / GitHub Issue，提供复现和证据 | 否，除非另行认领 |
| Triage Owner | 做 RCA，确认严重级别、影响模块、写入范围和验收标准 | 默认否 |
| Fix Owner | 在独立 worktree / branch 中实现修复 | 是，仅限 `allowed_write_scope` |
| Validator | 运行测试、平台验证、业务路径验证，记录 evidence | 默认否，可写验证记录 |
| Integrator | 控制共享文件、跨 PR 合并顺序、最终 main 合入 | 是，负责冲突处理 |
| Production Operator | 同步生产 checkout、重启服务、观察运行时 | 仅用户明确授权后执行 |

一个窗口可以承担多个角色，但必须在 BUG JSON 或 GitHub Issue 中明确当前角色。

## 4. Issue 生命周期

### 4.1 Open

创建 BUG JSON 和 GitHub Issue 时必须同步完成。推荐入口为 `mcp_github_issue_create`，或 `report_bug` 后立即执行 `mcp_github_issue_sync_bug(apply=true)` / 等价 Validation API 同步；禁止只写本地 `tests/aistock_validation/bugs/*.json` 后提交。

提交仓库前，BUG JSON 必须包含并通过只读校验：

- `github_issue_number`
- `github_issue_url`
- 与 GitHub label/status 一致的 `severity`、`status`、`module`
- 至少一条 `events` 记录说明创建、同步或状态更新来源

如果 GitHub 不可用，只能保留为未提交的临时 triage 草稿；不得把本地-only BUG JSON 合入 main。历史遗留本地-only BUG 只能通过专门的 backfill/cleanup 分支补链或关闭，不得作为新模式延续。

创建 BUG JSON 和 GitHub Issue 时必须包含：

- `bug_id`
- `title`
- `module`
- `severity`
- `risk_area`
- `status = open`
- `description`
- `reproduce_command`
- `suspected_modules`
- `required_verification`
- `closure_requirements`
- `allowed_write_scope`
- `non_goals`

若 `allowed_write_scope` 还不能确定，issue 只能进入 RCA / triage，不能进入代码修复。

### 4.2 Triaged

Triage 完成后必须补齐：

- 根因判断：code bug、配置 bug、数据问题、环境问题、架构边界问题、历史债务。
- 影响模块：backend / frontend / data pipeline / QE / Paper / Selection / HMM / infra 等。
- 最小修复范围。
- 非目标：哪些行为不能改变。
- 是否需要拆分子 issue。
- 是否涉及高冲突共享文件。

### 4.3 In Progress

认领 issue 前必须：

1. 从最新 `origin/main` 创建独立 worktree。
2. 创建 issue 专属 branch。
3. 写入 `assigned_agent`、`fix_branch`、`worktree_path`、`integration_owner`，并确认 GitHub Issue 链接仍存在。
4. 确认 `allowed_write_scope` 非空。
5. 在 cross-tool channel 发 `[INFO]` 或 `[ACK]`，说明认领范围和不触碰范围。

推荐命名：

```text
worktree: F:\Dev\AIstock_worktrees\bug-039-qe-data-freshness
branch:   bug/BUG-039-qe-data-freshness
```

### 4.4 Review Ready

提交 PR 前必须：

- `git status --short --branch` 只显示当前 issue 相关改动。
- `git diff --check` 通过。
- 所有改动文件都在 `allowed_write_scope` 内，或 issue 已更新 scope 并记录原因。
- 已运行 issue 要求的测试。
- PR title/body 引用 `BUG-NNN` 和 GitHub Issue。
- BUG JSON 中的 `github_issue_number` / `github_issue_url` 与 PR 引用的 GitHub Issue 一致。
- 明确声明是否触碰生产 `8001/3000`、DB 写入、migration、QMT、Paper live runtime。

### 4.5 Fixed

PR 合入后，BUG 可以标记为 `fixed`，但不能直接视为 `verified`。必须记录：

- fix commit
- PR / GitHub Issue 链接
- 测试命令和结果
- 合入分支
- 是否需要用户同步生产 checkout
- 是否需要运行时重启

### 4.6 Verified

平台验证通过后才能标记为 `verified`。验证证据可以来自：

- 自动化测试记录。
- Validation Center run record。
- 临时 dev port smoke。
- 业务路径 E2E。
- 数据/缓存覆盖审计。
- 明确的人工验收记录。

### 4.7 Closed

关闭前必须满足：

- closure requirements 全部完成。
- GitHub Issue 与 BUG JSON 状态同步；若两边不一致，先修复同步状态再关闭。
- 无未提交的 source-of-truth JSON 修改。
- 如果需要生产同步，已明确由谁执行、何时执行、是否完成。

## 5. 并行开发冲突防护

### 5.1 Worktree 规则

| 场景 | 必须做法 |
|---|---|
| 新 issue 修复 | 从 `origin/main` 创建独立 worktree |
| 新功能开发 | 独立 feature worktree，不与 bugfix 混用 |
| 生产 checkout `F:\Dev\AIstock` 脏 | 不在其中开发；只用于同步、救援或用户指定验证 |
| 多窗口并行 | 每个窗口有独立 branch/worktree |
| 需要合并多个窗口结果 | 由 integrator 建集成 branch，按顺序 cherry-pick 或 merge |

禁止在脏的生产/同步目录中直接开始新 issue 修复。

### 5.2 allowed_write_scope

`allowed_write_scope` 是 issue 修复的写入合同。示例：

```json
{
  "allowed_write_scope": [
    "backend/services/quantevolver/factor_universe_mask_service.py",
    "backend/services/quantevolver/config_composer.py",
    "scripts/backfill_factor_cache.py",
    "backend/tests/test_stock_universe_pit_service.py",
    "backend/tests/quantevolver/"
  ]
}
```

规则：

- 精确文件优先于目录。
- 新增测试目录可以声明为目录 scope。
- 文档或验证记录必须单独声明。
- 若发现必须修改 scope 外文件，必须先更新 issue，再继续编码。
- 若 scope 外文件属于高冲突文件，必须交给 integrator。

### 5.3 高冲突文件

以下文件或目录默认需要 integrator 或显式 `[DECISION]`：

- `backend/main.py`
- `backend/db/`
- `backend/migrations/`
- `backend/services/quantevolver/config_composer.py`
- `backend/services/strategy_package/`
- `backend/services/paper_trading_v2/` live/runtime 路径
- `frontend/src/app/*/layout.tsx`
- `frontend/next.config.mjs`
- `noxfile.py`
- `.github/workflows/`
- `docs/standards/`
- 全局配置、schema、manifest、migration、runtime profile

这类文件不是禁止修改，而是必须由一个窗口统一写入和合并。

### 5.4 子 issue 拆分

一个 issue 如果需要跨多个模块修改，应拆分为子任务或明确阶段：

| 子任务类型 | 示例 | 是否可并行 |
|---|---|---|
| 后端核心逻辑 | QE freshness profile | 可，但独占相关服务文件 |
| 前端默认值 | QE compose / factor cache UI | 可，与后端文件不重叠时 |
| Paper/Paper v2 guard | live readiness strictness tests | 可，但不得与 Paper runtime 开发窗口抢同文件 |
| 测试验证 | regression tests / smoke scripts | 可，但要避免改同一测试文件 |
| 文档 | standards / analysis / operations | 可，但 docs/standards 默认需 review |

若两个子任务需要同一文件，应改为串行。

## 6. Git 与提交规范

### 6.1 开始前

必须执行：

```bash
git fetch origin
git status --short --branch
git branch --show-current
git log --oneline -5
```

若当前目录是 `F:\Dev\AIstock` 且有脏改动，不得直接修复 issue。

### 6.2 提交前

必须执行：

```bash
git status --short
git diff --check
```

按需执行对应测试。不得 stage 不相关文件。

### 6.3 Commit message

推荐格式：

```text
fix(qe): isolate backtest freshness policy

Refs BUG-039
Fixes #23
```

如果只是文档：

```text
docs(standards): define issue fix parallel workflow
```

### 6.4 PR checklist

PR 必须说明：

- Linked BUG / Issue。
- Changed files 是否全部在 `allowed_write_scope` 内。
- Tests / validation run。
- 是否改 DB schema / migration。
- 是否触碰 production `8001/3000`。
- 是否需要用户重启服务。
- 是否有后续 issue 或 deferred work。

## 7. MCP Server 持久化要求

MCP server 应逐步支持以下字段和校验。本文先定义目标规范；具体实现可分阶段加入 `scripts/aistock_mcp_server.py`。

### 7.1 创建 issue

`mcp_github_issue_create` 应支持并持久化：

- `github_issue_number`
- `github_issue_url`
- `github_sync_state`
- `allowed_write_scope`
- `non_goals`
- `required_verification`
- `closure_requirements`
- `conflict_sensitive_files`
- `integration_owner`

若缺失 `allowed_write_scope`，默认写入空数组，并标记：

```json
{
  "workflow_gate": "triage_only_until_allowed_write_scope_is_set"
}
```

若 GitHub Issue 创建或同步失败，工具必须 fail-fast 返回错误；不得把未链接的 BUG JSON 当作已登记 issue 提交。

### 7.2 认领 issue

`assign_bug` 应支持：

- `worktree_path`
- `fix_branch`
- `assigned_agent`
- `integration_owner`
- `sync_github`

若 issue 没有 `allowed_write_scope`，MCP 应拒绝进入 `in_progress`，除非 `fix_branch` 为空且明确是 RCA / triage。

### 7.3 修复前校验

应提供一个 MCP 工具或脚本检查：

- 当前 git branch 是否匹配 `fix_branch`。
- 当前 worktree 是否匹配 `worktree_path`。
- diff 文件是否都在 `allowed_write_scope` 内。
- 是否触碰高冲突文件。
- 是否存在未声明的 DB migration 或 workflow 修改。

### 7.4 关闭前校验

关闭 issue 前必须检查：

- `fix_commit` 非空。
- `verification_run_id` 或验证记录非空。
- `closure_requirements` 已逐项满足。
- GitHub Issue 与 BUG JSON 同步。
- 如果生产同步尚未完成，状态不能超过 `fixed`。

## 8. CI / 流水线校验目标

### 8.1 Warning 阶段

先以 warning 模式落地，避免阻塞既有开发：

- PR 未引用 `BUG-NNN`。
- BUG JSON 没有 `allowed_write_scope`。
- diff 文件超出 scope。
- 修改高冲突文件但未声明 integrator。
- PR 声明未触碰生产服务，但验证证据来自生产 `8001/3000`。

### 8.2 Blocking 阶段

稳定后升级为 blocking：

- P0/P1 issue 修复无 linked BUG。
- P0/P1 修复 diff 超出 `allowed_write_scope`。
- 修改 DB schema 但缺少 comments 或 migration evidence。
- 修改 Paper live / trading / QE runtime 但无对应测试。
- 关闭 issue 前缺少验证记录。
- 新建 BUG JSON 缺少 `github_issue_number` / `github_issue_url`，或 GitHub Issue 与本地状态不一致。

## 9. 生产同步边界

以下动作不属于普通 issue 修复的一部分：

- 合并 `main` 到生产 checkout。
- 重启 backend `8001`。
- 重启 frontend `3000`。
- 写生产 DB。
- 触发 Paper live session。
- 修改 QMT / miniQMT live 配置。

这些动作必须由用户明确授权，并在独立操作记录中报告。

## 10. BUG-039 类问题的执行模板

对于“QE 和 Paper/Paper-v2 数据实时性策略隔离”这类跨模块问题，应按以下模板执行：

1. 创建 BUG，声明 risk area 为 `data_freshness_policy`。
2. Triage 阶段确认 QE 与 Paper/Paper-v2 的策略边界。
3. 拆分写入范围：
   - QE backend profile。
   - factor cache / prepare_factors cache policy。
   - Paper/Paper-v2 strict readiness tests。
   - frontend defaults/config display。
   - validation tests。
4. 若多个窗口并行，只允许不重叠文件并行。
5. `config_composer.py` 这类共享文件由 integrator 或唯一 Fix Owner 写。
6. 验证时必须同时证明：
   - QE 历史窗口可命中缓存。
   - Paper/Paper-v2 最新数据 fail-fast 不被放宽。
7. 合入后状态先设为 `fixed`，平台验证后再设为 `verified`。

## 11. Agent 执行提示

所有 Codex / Claude Code 窗口在处理 issue 修复时应遵循：

1. 先读取 BUG JSON / GitHub Issue / 相关 docs。
2. 先确认当前 worktree 和 branch。
3. 不在脏生产 checkout 中开发。
4. 不扩大 scope。
5. 不把用户或其他窗口的改动 revert。
6. 不合并到 `main`，除非用户明确要求。
7. 不重启 `8001/3000`，除非用户明确要求。
8. 需要跨模块或共享文件时，先发 cross-tool `[DECISION]` 或 `[REVIEW]`。

## 12. 后续落地建议

本文档先作为规范源。后续可分三个独立变更落地，不应在本规范文档生成时创建治理 issue：

1. **MCP 字段增强**：扩展 issue create / assign / sync 工具字段。
2. **Scope guardrail**：新增 diff-vs-allowed-write-scope 检查脚本和 nox session。
3. **PR template / GitHub Action**：要求 PR 填写 BUG、scope、验证、生产触碰声明。

在这些实现完成前，所有窗口应人工按本文执行。
