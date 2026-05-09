# Cross-Tool 协调通道协议（2026-05-10）

> **作者**：Claude Code Opus 4.7（战略 session）
> **目的**：规范 Claude Code 与 Codex 通过 mempalace 共享 drawer 实现的跨工具异步通信
> **状态**：✅ 通道已验证（drawer 1+2+3 双向写读）；✅ Notifier 已部署（Windows toast 桌面通知）

---

## 1. 通道基本信息

| 字段 | 值 |
| --- | --- |
| **共享后端** | mempalace 3.3.0（pip 装在 `C:\Users\lc999\miniconda3`）|
| **数据库** | `C:\Users\lc999\.mempalace\palace\` + `knowledge_graph.sqlite3` |
| **Wing** | `cross-tool` |
| **Room** | `codex-claude-coord` |
| **当前消息数** | 3（截至 2026-05-09T10:47）|
| **Notifier 脚本** | `C:\Users\lc999\agent_teams_cross_tool_notifier.py` |
| **启动器** | `C:\Users\lc999\start_cross_tool_notifier.bat` |

## 2. 双方 added_by 命名约定

| Agent | added_by 值 | 用途 |
| --- | --- | --- |
| Claude Code 战略 / 分析 session | `claude-code-strategy` | 长期战略讨论、跨工具协调 |
| Claude Code 实施 session（Agent Teams Lead） | `claude-code-impl-lead` | Phase 实施进度、teammate 任务派单状态 |
| Claude Code teammate（具体 5 个） | `teammate-<name>`（如 `teammate-engine-design`） | 罕见情况（teammate 通常不直接跨工具沟通） |
| Codex App | `codex-app` | Codex 主体设计、Phase 0-7 进展、跨工具回复 |

**严禁**写到其他 wing（如 `aistock` / `wing_claude_code` 等）—— 那些是项目专属，会污染各自上下文。

## 3. 写入 drawer 的标签约定

drawer 内容**前 100 字符**应该包含明确的语义标签，方便对方 search 时优先级判断：

| 标签 | 含义 | 优先级 |
| --- | --- | --- |
| `[URGENT]` | 阻塞性问题需立即响应（如生产 8001 异常） | 立即（toast 高音量警报）|
| `[DECISION]` | 决策请求（D1 / D3 等） | 高（数小时内回） |
| `[REVIEW]` | PR / 文档 review 请求 | 中（半天内回） |
| `[INFO]` | 进度报告 / 状态同步 | 低（次日 OK） |
| `[ACK]` | 确认收到 / 已处理 | 立即但不需深度回 |

例：
```
[DECISION] D1 strategy_package/live_inference.py 工作面归属
...
```

## 4. 双方阅读义务（必读）

### Claude Code 端

- 战略 session：每次用户提出问题前 **search 一次** 通道（看 Codex 是否留新消息）
- 实施 session：每次 lead 收到 idle_notification + 派任务前 search 一次
- Notifier 弹 toast 时立即看（用户会基于 toast 通知 prompt 我）

### Codex 端（**配合指令见 §6**）

- 每次新会话开始时 **search 通道**
- 每次 task 之间（idle 时）search 一次
- 收到 `[URGENT]` 桌面 toast 时立即响应

## 5. Notifier 使用方法

### 启动 notifier

**双击启动**：
```
C:\Users\lc999\start_cross_tool_notifier.bat
```

或 PowerShell：
```powershell
C:\Users\lc999\start_cross_tool_notifier.bat
```

**预期效果**：
- 启动时弹一个 toast 确认 baseline（"已记录 N drawer 基线"）
- 之后每 5 秒轮询 mempalace，发现新 drawer 时弹 toast
- 自己（`claude-code-strategy`）写入的 drawer 不弹（避免噪音）
- 关窗口即停止

### 停止 notifier

关掉控制台窗口，或 Ctrl+C。

### Toast 内容格式

| 类型 | 标题 | 正文 |
| --- | --- | --- |
| 普通通知 | `💬 [Cross-tool] codex-app` | drawer 内容前 200 字符 |
| 紧急通知 | `🚨 [URGENT] codex-app` | 同上，但音效是警报 |

## 6. Codex 端配合指令（用户复制 → 粘贴给 Codex）

### 6.1 Codex 一次性约定（设置阶段）

```
请记住以下跨工具协调约定：

【共享通道】
- mempalace MCP 通道 wing="cross-tool" room="codex-claude-coord"
- Claude Code 与你（Codex）通过此通道异步通信
- 数据库共享于 ~/.mempalace/palace/，121+ 当前 drawer

【你的标识】
- 你写 drawer 时 added_by="codex-app"
- Claude Code 战略 session 用 added_by="claude-code-strategy"
- Claude Code 实施 session 用 added_by="claude-code-impl-lead"

【阅读义务】
1. 每次新会话开始时，先 list/search wing="cross-tool" room="codex-claude-coord" 看是否有新消息
2. 收到任务派单或决策请求前，再 search 一次
3. 看到 [URGENT] 标签的 drawer 立即响应

【写入约定】
- 内容前加语义标签：[URGENT] / [DECISION] / [REVIEW] / [INFO] / [ACK]
- 长消息可分多 drawer（标 [part 1/3] 等）
- 严禁写到其他 wing

【参考文档】（在 origin/main）
- docs/discussion/cross_tool_channel_protocol_20260510.md（本文档）
- docs/discussion/user_decisions_for_morning_review_20260510.md（D1-D4 决策详解）

收到请回 [ACK] drawer 确认。
```

### 6.2 立即触发：让 Codex 检查最新 drawer

```
请用 mempalace_list_drawers 工具查 wing="cross-tool" room="codex-claude-coord"，
按 filed_at 降序，告诉我最近 5 条的：
- drawer_id（截短）
- added_by
- 内容前 200 字符
```

### 6.3 让 Codex 回复 D1 决策

把以下 prompt 复制粘贴给 Codex（**通道打通后，未来这种 relay 你只需要做一次"读 mempalace"提示，不必逐字复制内容**）：

```
请读 mempalace cross-tool/codex-claude-coord 通道的最新 drawer。
里面有 Claude Code 提的 D1 决策请求（strategy_package/live_inference.py 工作面归属）。

请回复：
1. 你（Codex）认可"backend/services/strategy_package/ 是 Claude Code 工作面"吗？
2. 你的 Phase 0-7 中是否预期需要修改 strategy_package/live_inference.py？
3. 简短理由

通过 mempalace_add_drawer(wing="cross-tool", room="codex-claude-coord",
  content="[DECISION] D1 reply: ...", added_by="codex-app") 写回。
```

## 7. 协作工作流示例

### 场景 A：Claude Code 提决策给 Codex

```
1. Claude Code 战略 session: add_drawer 留 [DECISION] 消息
2. Notifier toast 弹到桌面（用户看到"💬 from claude-code-strategy"）
3. 用户切到 Codex App，说"读 mempalace cross-tool 最新"
4. Codex search → 读到 [DECISION] → 回复 add_drawer [DECISION] reply
5. Notifier 又弹 toast（"💬 from codex-app"）
6. 用户回 Claude Code 说"看 codex 回复"
7. Claude Code search → 读到 reply → 继续
```

**单轮往返时间**：1-3 分钟（用户响应 + 双方 search）

### 场景 B：Codex 主动通知进度

```
1. Codex 完成 Phase 5.3 → add_drawer 留 [INFO] "Phase 5.3 complete, see commit XXX"
2. Notifier 弹 toast
3. 用户在 Claude Code 这边说"看 codex 进度"
4. Claude Code search 后调整工作计划
```

### 场景 C：[URGENT] 紧急情况

```
1. 任一方发现紧急问题（如生产 8001 异常）→ add_drawer 加 [URGENT] 标签
2. Notifier 弹高音量警报 toast
3. 用户立即 prompt 双方处理
```

## 8. 容错与限制

| 问题 | 处理 |
| --- | --- |
| Notifier 进程死掉 | 重启 .bat；之前的 drawer 不会被错过（state 文件持久化） |
| toast 无声 | Windows 通知设置 → 启用 toast 音效 |
| 长消息（含特殊字符）写入失败 | 改纯 ASCII + 简单标点（中文括号 `【】` 触发过 MCP error，普通中文 OK）|
| 双方同时写造成冲突 | mempalace 是 ChromaDB，自动序列化；不会冲突 |
| Codex 不会自动 poll | 通过用户 prompt 触发（toast 通知 → 用户提示 Codex）|

## 9. 推迟到后续的扩展

- **L2 真正实时**（每轮 < 5 秒）：需要 AgentBridge（Bun + Research Preview）
- **MCP server 共享 TaskList**：未来如有 cross-tool task 需求可加
- **GitHub Actions 触发**：push to main → 自动留 cross-tool drawer

当前 L2 异步 + 通知方案对 D1/D3 决策已足够。
