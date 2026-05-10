# Cross-Tool Communication Protocol v2 — 短 drawer + 文档详情

> **Author**: Claude Code 战略 session 2026-05-11
> **Status**: PROPOSAL → adopted upon Codex ACK
> **Replaces**: 2026-05-10 protocol where full content lived in drawers

## §1 问题背景

mempalace v1 协议把全部内容塞 drawer，已暴露 5 类痛点（per `docs/process/...mempalace_limits...`）：
- MCP -32000 长内容失败（>~1.5KB）
- 特殊字符（em-dash、长串中文）触发 MCP error
- HNSW 索引在 100+ drawer / wing 变慢
- PostHog 遥测网络超时干扰
- 长内容 drawer 不可富 markdown

实际数据：cross-tool wing 已 110+ drawer，每天新增 30-50 条。

## §2 v2 协议核心

**drawer = metadata + 摘要 + doc 引用**

```
[TAG] <short subject>

from=<sender>
to=<receiver>
responding_to=<drawer_id> (optional)
detail_doc=<repo-relative path or absolute path>
commit=<sha> (optional)
branch=<branch> (optional)
verdict=PASS|BLOCKED|INFO|ACK (optional)

<1-3 sentence summary>
<1-3 bullet key findings if applicable>

-- <sender_id> <date>
```

drawer 体长目标: < 800 字符（约 200 字英文 / 100 字中文）。
全部详情写到 detail_doc 中。

## §3 详情文档约定

### 路径约定

```
docs/cross_tool/
  └─ <YYYYMMDD>_<sender>_to_<receiver>_<tag>_<topic>.md

例:
  docs/cross_tool/20260511_codex_to_claude_REVIEW_t14bc_round2_verdict.md
  docs/cross_tool/20260511_claude_to_codex_ACK_q1_q2_q3_decisions.md
```

### 详情文档结构

```markdown
# [TAG] <Subject>

**from**: codex_app | claude_code_strategy | claude_code_<team>_lead
**to**: codex_app | claude_code_strategy
**date**: 2026-05-11T00:43Z
**drawer_id**: <mempalace drawer id>
**responding_to_drawer**: <other drawer id, optional>
**branch_reviewed**: origin/claude/...
**commit_reviewed**: <sha>

## Summary
<1-3 sentence overview>

## Verdict
PASS | BLOCKED | INFO | ACK | DECISION

## Findings (for REVIEW)
### P1 BLOCKER
- ...
### P2 follow-up
- ...
### P3 nit
- ...

## Validation Performed
- ...

## Recommended Action
- ...

## Boundary Confirmations
- production_5432_touched=false
- ...

## References
- related_drawer: <id>
- related_doc: <path>
- related_bug: BUG-NNN
```

### Schema 字段

| 字段 | 必填 | 说明 |
|---|---|---|
| from / to / date / drawer_id | ✅ | metadata |
| responding_to_drawer | 视情况 | reply 时填 |
| branch_reviewed / commit_reviewed | review 时必填 | |
| Summary / Verdict | ✅ | 与 drawer 一致 |
| Findings | review 时必填 | |
| Validation Performed | review 时必填 | |
| Recommended Action | review 时必填 | |
| Boundary Confirmations | ✅ | 安全约束证据 |
| References | 视情况 | 关联其他记录 |

## §4 工作流

### 发消息侧（reviewer / informer）

1. 写详情文档 `docs/cross_tool/<YYYYMMDD>_..._.md`
2. git add + commit + push（详情文档跟代码同 commit 流转）
3. 发 drawer 含摘要 + `detail_doc=docs/cross_tool/...`
4. drawer 长度 < 800 字符

### 收消息侧（reviewee / receiver）

1. 读 drawer 拿 metadata + 决定是否需要详情
2. `Read F:/Dev/AIstock/<detail_doc_path>` 拉详情
3. 处理 / 回复

### AI Agent 视角

- **Claude / Codex 无需读 drawer 全文** — 只看 summary + verdict + detail_doc
- 需要详情时用 Read tool 拉 doc
- 写 review 时先写 doc 再发 drawer

## §5 兼容性

### v1 → v2 迁移
- v1 长 drawer 不删，留作历史
- v2 起新 drawer 走新协议
- 历史 drawer 引用：用 drawer_id 仍可 mempalace_get_drawer

### Fallback
- 如果详情文档体量也超大（>20k 行），拆多个 doc，drawer 列主入口 doc
- 主入口 doc 用 markdown TOC 链接子 doc

## §6 何时仍用纯 drawer (无 doc)

- ACK 类（< 200 字符）
- 状态广播（如"started X"、"X done"）
- 决策选项请求（< 800 字符）

## §7 何时必须用 doc

- review 报告（> 1KB）
- 派发指令（>1KB 步骤）
- 多 commit summary
- 含 code snippets / 多 list 的内容
- 含特殊字符（em-dash / Unicode 标点）

## §8 文档存档

- `docs/cross_tool/` 目录每月归档：超过 30 天的 doc 移到 `docs/cross_tool/archive/<YYYY-MM>/`
- 归档不删除，git 历史保留
- 主目录保持当月活跃文档 < 100 个

## §9 实施时间线

- T0 (now): 战略 session 写本协议 + 发 ACK drawer 给 Codex 提议
- T0+30 min: Codex ACK 后正式启用 v2
- T0+1d: 所有 review/dispatch 都走 v2
- T0+1 week: 评估效果（drawer 体长 / MCP 错误率 / HNSW 性能）
