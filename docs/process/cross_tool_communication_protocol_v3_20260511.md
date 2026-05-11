# Cross-Tool Communication Protocol v3 — Doc-Primary + Drawer-Notify Hybrid

> **作者**: Claude Code 战略 session 2026-05-11
> **状态**: AUTHORITATIVE — 用户 2026-05-11 已确认
> **取代**: v2 (2026-05-11) 之上的演进版

## §1 核心原则

**文档是 source of truth，mempalace drawer 只是 notification channel**。

```
docs/cross_tool/  + git history  = 永久 audit
mempalace drawer = 短摘要 + reference + 实时通知 (主要给 Codex peer)
```

## §2 沟通矩阵

| 方向 | 通讯方式 | 说明 |
|---|---|---|
| 战略 session ↔ Codex peer | mempalace drawer (短) + detail_doc | drawer 800 字以内 + doc 在 docs/cross_tool/ |
| 战略 session → Claude Code teams (派发) | 用户人工中继 doc path + 短摘要文本 | worktree session 不 auto-poll drawer，必须用户复制 |
| Claude Code teams → 战略 session (deliver/status) | **commit + push + status doc** | git fetch 后可见，drawer 通知 Codex review |
| Claude Code team ↔ Claude Code team | 通过战略 session 中继 OR docs/cross_tool/<team>_to_<team>_<topic>.md | 不直接通信，避免协调失控 |

## §3 必须遵守的规则

### 规则 1: 每个团队完成任务后必须三件事

```
1. git commit + git push origin <branch>
2. 写 docs/cross_tool/<YYYYMMDD>_<team>_<verb>_<topic>.md
   含: commit SHA + 测试结果 + 关键发现 + boundary confirmations + references
3. 发 mempalace drawer (短摘要 + detail_doc reference) 通知 Codex review trigger
```

不可只 commit 不写 doc。不可只发 drawer 不 commit。

### 规则 2: drawer 长度 < 800 字符

超过则必拆 detail_doc。drawer 仅含：
- tag (REVIEW / INFO / DECISION / ACK / DISPATCH)
- from / to
- detail_doc 路径
- commit SHA (如有)
- 1-3 句摘要
- verdict (PASS / BLOCKED / INFO / ACK)

### 规则 3: detail_doc 命名约定

```
docs/cross_tool/<YYYYMMDD>_<sender>_<receiver>_<TAG>_<topic>.md

例:
  20260511_dw_foundation_to_codex_REVIEW_t14bc_round3.md
  20260511_strategy_DISPATCH_pipeline_stage45_fix_round_2.md
  20260511_codex_to_claude_REVIEW_stage_7_3_blocked.md
```

### 规则 4: detail_doc 必含 frontmatter

```markdown
# [TAG] <subject>

**from**: <sender>
**to**: <receiver>
**date**: 2026-05-11
**responding_to_drawer**: <drawer_id> (可选)
**verdict**: PASS | BLOCKED | INFO | ACK | DISPATCH
**branch**: origin/<branch> (review 时必填)
**commit**: <sha> (review 时必填)

## Summary
<1-3 sentence>

## ... per-section content ...

## Boundary Confirmations
- production_5432_touched=false
- ...

## References
- related_drawer: ...
- related_doc: ...
```

### 规则 5: 战略 session 检查进度的标准流程

不再依赖 drawer 作 source of truth：

```bash
1. git fetch origin
2. git for-each-ref --sort=-committerdate refs/remotes/origin/ | head -10
   # 看所有分支最新 commit
3. git log --oneline -5 origin/<branch>
   # 看具体分支进展
4. ls -lt docs/cross_tool/ | head -20
   # 看 status doc 时序
5. mempalace_list_drawers 仅查 Codex 端 review verdict
```

### 规则 6: Cross-tool drawer 仅用作 Codex 同步

mempalace drawer 是 Codex peer 同步通道，**不是** Claude Code worktree teams 的派发触发器。Claude Code teams 接受派发通过：
- 用户复制派发文本到 worktree 窗口
- 战略 session 写 doc，用户中继 doc path + 摘要

## §4 sender 标识

| from | 含义 |
|---|---|
| `claude-code-strategy` | 战略 session |
| `claude-code-dw-foundation-lead` | dw-foundation worktree Lead |
| `claude-code-paper-v2-team` | paper-v2 worktree Lead |
| `claude-code-pipeline-foundation-lead` | pipeline-foundation worktree Lead |
| `claude-code-frontend-pipeline-pages-lead` | frontend-pipeline-pages worktree Lead |
| `codex-app` | Codex peer |

## §5 派发时通知所有团队

战略 session 给任何团队派发新任务时，**派发文本必须含以下提示**：

```
完成后必须三件事:
1. commit + push origin/<branch>
2. 写 docs/cross_tool/<YYYYMMDD>_<team>_<verb>_<topic>.md (含 commit SHA + 测试结果 + 关键发现)
3. mempalace drawer (短摘要 + doc reference) 通知 Codex review

不可只 commit 不写 doc。不可只发 drawer 不写 doc。

protocol v3 详情: docs/process/cross_tool_communication_protocol_v3_20260511.md
```

## §6 v2 与 v3 的兼容

v2 (2026-05-11 早期) 已写的 docs/cross_tool/ 文档全部 v3 兼容（结构一致）。v2 §10 关于"worktree teams 不 auto-poll" 升级为 v3 §3 规则 6 + §5 派发模板。

## §7 历史 drawer 处理

cross-tool wing 已 ~270 drawers。v3 启用后：
- 历史 drawer 保留 (mempalace 长期记忆)
- 新 drawer 严格 < 800 字符 + detail_doc reference
- 不删除任何历史 drawer

## §8 实施时间线

- v3 启用: 2026-05-11 ~14:00 (本 commit 后)
- Codex peer 通知: 紧随其后 cross-tool drawer
- 所有团队下一次派发开始遵守 v3 §5 派发模板
