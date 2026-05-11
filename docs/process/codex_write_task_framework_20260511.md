# Codex 后续写任务框架

**Author**: Claude Code 战略 session 2026-05-11
**Status**: AUTHORITATIVE
**Purpose**: 定义 Codex 后续直接修改/写代码任务的边界 + 调度模式

## §1 Codex 写任务分类

### 类型 A: Codex 自驱写（不需要战略 session 派发）

Codex 在自己工作面写代码，**不需要战略 session 协调**：

| 工作面 | 范围 | 调度 |
|---|---|---|
| **codex/qe-governance-integration-20260509** | governance Phase 4-7 自驱开发 | Codex 自决 + 用户直接授权 |
| **codex/hmm-sector-regime-20260509** | HMM regime 独立工作 | Codex 自决 |
| **codex/financial-distress-rerank-20260508** | financial distress 信号筛选 | Codex 自决 |
| Codex governance branch 内 BUG 修复 | 修自己代码 audit 发现的 P1/P2 | Codex 自决, 通知战略 session |
| `scripts/governance_*_smoke.py` | Codex 自己的 smoke 工具 | Codex 自决 |
| `backend/tests/strategy_package/` | Codex 自己的测试 | Codex 自决 |

### 类型 B: 战略 session 协调的写任务

涉及 cross-tool 协调或共享资源：

| 工作 | 触发 | 调度 |
|---|---|---|
| **4 packages stability evidence backfill** | R6 阶段前置 | 战略 session 派发，Codex 写 backfill script，用户授权 prod 写 |
| **governance migrations apply 到 prod DB** | R6 phase | 战略 session 协调 timing，用户授权门 |
| **governance live smoke against prod backend 8001** | R6 后 | 用户授权，Codex 配合 monitoring |

### 类型 C: 仅 Claude review 触发的修复

Claude side audit 发现 Codex 代码 P0/P1 bug：

```
Claude audit → 入 BUG 注册表 → 战略 session 通知 Codex 
→ Codex 自驱修自己代码 → push → Claude/战略 verify
```

不需要战略 session 派发完整 dispatch doc，Codex 直接读 BUG 注册表 (via MCP get_bug_agent_context) 修复。

## §2 Codex 写任务的 boundary

无论类型，Codex 写代码必须遵守：

- ✅ 仅在 codex/* 分支内
- ✅ 不动 claude/* 分支
- ✅ 不动 origin/main（merge 由战略 session 协调）
- ✅ 不写 prod DB（除非用户授权 + 战略协调）
- ✅ 不启动 prod backend 8001 / frontend 3000
- ✅ 文档化在 docs/cross_tool/ (v3 协议)

## §3 通知模式

### Codex 写任务前
- 类型 A: 通知战略 session via drawer (短，<800字)
- 类型 B: 等待战略 session 派发或用户授权
- 类型 C: 直接 fix + 通知战略 session

### Codex 写任务后
- v3 协议: commit + push + status doc + drawer notify
- drawer 含: branch + commit SHA + 测试结果 + boundary

## §4 当前 Codex 写任务 queue（2026-05-11）

| 任务 | 类型 | 状态 |
|---|---|---|
| Codex governance Phase 4-7 self-driven | A | ⏸️ 等用户授权 |
| 修 Claude audit 发现的 Codex bug | C | ⏸️ 等 paper-v2 audit 完成 |
| 4 packages evidence backfill script | B | ⏸️ 等 R6 phase |
| governance prod DB migrations apply script | B | ⏸️ 等 R6 phase |
| HMM regime / financial-distress 独立工作 | A | ⏸️ 等用户授权 |

## §5 与战略 session 的协作

战略 session 不调度 Codex 类型 A 工作，但需要知情：
- Codex push 类型 A commit → 通过 drawer 通知（短）
- 战略 session 不 review 类型 A 工作（除非用户要求）
- Codex 类型 A 进度可见于 codex/* 分支 git log

战略 session 调度 Codex 类型 B 工作 + 用户授权 + Codex 实施

战略 session 仲裁 Codex 类型 C 修复（用户决策）

## §6 升级路径

如未来 Codex 工作需要修改 claude/* 分支代码（如 cross-branch refactor），必须：
1. 战略 session 决策 + 用户授权
2. 临时给 Codex worktree 边界权限
3. 修改后 strict review by Claude team

这种跨边界写**不在当前 Sprint 范围**。

## §7 References

- protocol v3: cross_tool_communication_protocol_v3_20260511.md
- rollout v2: production_rollout_playbook_v2_20260511.md
- branch convergence: branch_convergence_strategy_20260511.md
