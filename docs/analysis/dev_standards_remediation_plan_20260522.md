# AIstock 开发规范整改方案

> 日期：2026-05-22
> 触发：Issue 修复耗时过长（数小时/issue），token 消耗过大
> 状态：分析完成，待用户审批后执行

## 1. 诊断结论

### 1.1 核心问题

当前规范体系存在 **三个结构性瓶颈**，叠加导致 issue 修复耗时数小时：

| 瓶颈 | 根因 | 影响 |
|------|------|------|
| **全量上下文加载** | 每个 session 需读取 ~8000 行规范/记忆文档后才开始工作，其中 80% 与当前 issue 无关 | 前置 token 消耗大，AI agent 无法按需过滤 |
| **重复规范副本** | 同一规则在 v1.4标准、issue流程、codex_memory 三处独立成文 | Agent 重复处理相同规则三次，认知负载 3x |
| **无差别完整流程** | typo fix 和跨模块重构走同一套六阶段流程 + 五列验收矩阵 | 简单修复的流程开销超过编码本身 |

### 1.2 冗余量化

9 份文件分析后的重复矩阵（仅列高严重度）：

| 重复对 | 重复主题 | 可删减量 |
|--------|---------|---------|
| issue流程(414行) vs v1.4标准(556行) | GitHub同步、DESIGN-COMPLIANCE、生产端口、Agent规则、文档归属、禁止降级 | issue流程 **-56%** (414→180行) |
| codex_memory(~5000行) vs v1.4标准 | 工程规则段(76-100行)大量重述，8001规则出现50+次，worktree规则描述3次 | codex_memory **-15%** (~5000→~4200行) |
| AGENTS.override(42行) vs codex_memory | "读codex_memory"指令、禁止修改AGENTS.md、DESIGN-COMPLIANCE | AGENTS.override **-30%** (42→30行) |
| Claude记忆(35行) vs 全套规范 | worktree、端口、文档归属、禁止降级 | **-40%** (35→15行) |

### 1.3 跨工具通讯要求清单

当前强制通讯分布在 2 个位置：

1. **`C:\Users\lc999\.codex\AGENTS.md`** 第 1-4 条 Mandatory behavior — 每次 session 开始 poll channel、回复前再查、必须加 `[DECISION]`/`[REVIEW]`/`[INFO]`/`[ACK]` 标签
2. **issue 流程标准** §4.3、§5.3、§11 — 认领 issue 时发 `[INFO]`/`[ACK]`，改共享文件前发 `[DECISION]`/`[REVIEW]`

v1.4 标准本身不引用 cross-tool channel。Claude Code 的 CLAUDE.md 为空，对协议完全无感知。

## 2. 整改方案

### 原则

1. **v1.4 标准是唯一 rule source** — 所有其他文档只引用，不重述规则
2. **跨工具通讯改为 opt-in** — 仅在用户明确要求时启用
3. **Issue 按 S/M/L 分级** — 不同级别走不同深度的流程
4. **同级模块 issue 支持批量执行** — 共享上下文、worktree、验证

### 2.1 Issue 分级制度（新增）

在 BUG JSON 中增加 `process_level` 字段，Triage 阶段确定：

| 级别 | 判定条件 | 流程深度 | 预计耗时 |
|------|---------|---------|---------|
| **S (simple)** | 单文件、无 DB 变更、无共享文件、无设计文档依赖 | worktree + fix + test + commit。**跳过** cross-tool 协调、完整 DESIGN-COMPLIANCE 矩阵 | 10-30 min |
| **M (moderate)** | 多文件、单模块、可能触 DB、有 closure_requirements | 标准流程，但 DESIGN-COMPLIANCE 只检查 closure_requirements（3 列矩阵） | 30-90 min |
| **L (large)** | 跨模块、涉及共享文件/高冲突文件、基于已批准设计文档 | 完整六阶段流程 + 完整 5 列验收矩阵 | 2-4 h |

实现方式：在 `aistock_issue_fix_parallel_workflow_standard` 中增加 §2.0 "Issue 分级"，在 BUG JSON schema 中增加 `process_level` 字段。

### 2.2 同模块批量执行（新增）

在 BUG JSON 中增加批量字段：

```json
{
  "batch_id": "BATCH-<MODULE>-<YYYYMMDD>",
  "batch_issues": ["BUG-NNN", "BUG-NNN"],
  "batch_strategy": "shared_worktree"
}
```

合并规则：
- 同 `batch_id` 的 issues 共享一个 worktree、一次上下文加载、一次回归测试
- 每个 issue 仍独立 commit、独立 GitHub Issue、独立 closure
- batch 内按 issue 顺序逐个修复，每个 fix commit 引用对应 BUG-NNN

### 2.3 规范精简（改现有文件）

#### 2.3.1 `aistock_issue_fix_parallel_workflow_standard` — 目标 414→180 行 (-56%)

删减明细：

| 节 | 当前行数 | 目标行数 | 操作 |
|----|---------|---------|------|
| §1 目的 | ~10 | ~8 | 保留 |
| §2 基本原则 | ~15 | ~10 | 删 6 条与 v1.4 重复的规则（生产端口、GitHub同步、禁止简化交付等），改为 `参见 v1.4 §X` |
| §3 角色定义 | ~12 | ~10 | 保留（issue 特有），删 Integrator 角色（合并到 Fix Owner） |
| §4 Issue 生命周期 | ~55 | ~40 | 保留状态机，删 GitHub 同步细节（引用 v1.4 §6.16），删 cross-tool [INFO]/[ACK] 要求 |
| §5 并行开发冲突 | ~35 | ~10 | **大幅删除**。worktree 规则与 codex_memory 完全重复，改为一行引用。仅保留 `allowed_write_scope` 机制 |
| §6 Git/提交规范 | ~30 | ~12 | 删除通用 git 规范（不属于 issue 流程），仅保留 PR checklist |
| §7 MCP 持久化 | ~40 | ~30 | 保留（issue 特有），增加 `process_level` 和 `batch_id` 字段定义 |
| §8 CI 流水线 | ~25 | ~15 | 保留 |
| §9 生产同步边界 | ~15 | ~0 | **全部删除**，改为引用 v1.4 §5.1, §6.2 |
| §10 BUG-039 模板 | ~20 | ~15 | 保留（实用） |
| §11 Agent 提示 | ~15 | ~5 | 删 5 条重复，仅保留 issue 特有 3 条 |
| §12 后续落地 | ~15 | ~10 | 保留 |
| **新增** §2.0 Issue 分级 | 0 | ~15 | S/M/L 三级定义和判定条件 |

#### 2.3.2 `C:\Users\lc999\.codex\AGENTS.md` — 目标 17→8 行 (-53%)

将 4 条 Mandatory behavior 全部移除，改为：

```markdown
# Codex App — AIstock Project Instructions

## Startup
Read `F:\Dev\AIstock\docs\codex_project_memory.md` before any architecture,
backend, frontend, data pipeline, or trading-related changes.

## Standards
Active standards (read when relevant to the task):
- `docs/standards/aistock_development_standard_v1.4_20260521.md` — all P0/P1 rules
- `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md` — issue lifecycle

## Cross-tool coordination (OPT-IN)
A MemPalace channel exists at wing="cross-tool" room="codex-claude-coord" for
coordination with Claude Code. Use it ONLY when the user explicitly requests
cross-tool coordination. Do not poll it proactively.

@C:\Users\lc999\.codex\RTK.md
```

变更要点：
- 删除 `Mempalace` 拼写错误（改为 MemPalace）
- 删除 4 条 Mandatory behavior
- 将 cross-tool 改为 opt-in

#### 2.3.3 `Claude Code` 记忆 `feedback_aistock_codex_alignment.md` — 目标 35→15 行 (-57%)

删除重复的规则描述（worktree、端口、文档归属、禁止降级），只保留指针：

```markdown
在 AIstock 项目下工作时，遵守与 Codex 同一套规则。
**Source of truth**：`F:\Dev\AIstock\docs\codex_project_memory.md`
和 `F:\Dev\AIstock\docs\standards\aistock_development_standard_v1.4_20260521.md`。

关键边界（Codex vs Claude Code）：
1. 生产端口 8001 绝不动。验证用 8011/8012 + 3011/3012。
2. 不修改 Codex 领地文件：codex_project_memory.md、AGENTS.override.md、AGENTS.md。
3. 分支命名前缀 claude/ 区分于 Codex 的 codex/。
```

#### 2.3.4 `codex_project_memory.md` — 目标 ~5000→~4200 行 (-15%)

删减策略（后续阶段执行）：
- 删除第 76-100 行 "Engineering Rules for Codex" 中与 v1.4 重复的 8 条规则，改为一行引用
- 合并 "Multi-Codex Parallel Development Guardrails" 和 "Production Root Sync Rule" 中重复的 worktree 描述
- 删除各 update log 中重复出现的 "production backend 8001 was not restarted" 约 50 处（只保留首次出现）

### 2.4 不做的事

- 不修改 v1.4 标准本身的规则定义（它是唯一 rule source，只加 applicability 元数据）
- 不删除 codex_project_memory.md 的历史 update log（有审计价值）
- 不修改 Claude Code 的 CLAUDE.md（当前为空，不影响冗余）

## 3. 实施顺序

### Phase 1（立即，1 小时内）

| 序号 | 操作 | 文件 | 影响 |
|------|------|------|------|
| 1 | 删除强制 cross-tool 通讯 | `.codex/AGENTS.md` | Codex 行为变更 |
| 2 | 精简 issue 流程标准 | `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md` | 所有 agent |
| 3 | 增加 Issue 分级 + 批量执行 | 同上文件 §2.0（新增） | 所有 agent |
| 4 | 更新 Claude Code 记忆 | `memory/feedback_aistock_codex_alignment.md` | Claude Code |

### Phase 2（本周）

| 序号 | 操作 | 文件 | 影响 |
|------|------|------|------|
| 5 | 精简重复规则段 | `docs/codex_project_memory.md` | Codex |
| 6 | 精简 AGENTS.override | `AGENTS.override.md` | Codex |

### Phase 3（后续）

| 序号 | 操作 | 说明 |
|------|------|------|
| 7 | v1.4 规则增加 applicability 元数据 | 每条规则声明适用模块，agent 按需加载 |
| 8 | 规则引擎化 | rules.json 作为机器主源，提供过滤 API |
| 9 | CI 集成 | scope 校验、DESIGN-COMPLIANCE 从 agent prompt 移到 CI |

## 4. 预期效果

| 指标 | 当前 | 整改后 | 节省 |
|------|------|--------|------|
| Session 启动前置阅读 | ~8000 行 | ~2000 行 (仅 v1.4 + 当前模块相关段) | **-75%** |
| S 级 issue 修复时间 | 1-2h (走完整流程) | 10-30 min | **-75%** |
| 同模块 3 个 M 级 issue | 3×1.5h = 4.5h | 2h + 3×20min = 3h | **-33%** |
| 规范文档总行数（核心 4 份） | ~6000 行 | ~4800 行 | **-20%** |
| 规则重复副本 | 3 份 (v1.4/issue流程/codex_memory) | 1 份 (v1.4) + 引用 | 消除 2 份副本 |
| 跨工具强制通讯点 | 6 处 | 0 处 | 消除全部 |

## 5. 待用户决策

1. Phase 1 四个改动是否批准？是否一次全部执行？
2. Issue 分级的 S/M/L 定义是否接受，还是需要调整？
3. codex_project_memory.md 精简（Phase 2）是否需要先备份当前版本？
4. 整改文档是否需要作为独立 PR 合入 main，还是暂存在分析目录等待审批？
