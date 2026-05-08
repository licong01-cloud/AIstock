# 战略讨论 Session 补充交接（2026-05-09）

> **作者**：Claude Code（Opus 4.7） — 战略 / 分析 session（与 Agent Teams Day 1+2 实施 session 平行）
> **状态**：本 session 即将重启，本文档持久化战略讨论的最新状态
> **配套文档**：
> - 完整 Day 1+2 实施状态：`F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\docs\discussion\agent_teams_session_handoff_20260509.md`
> - Day 1 启动交接：`F:\Dev\AIstock\docs\discussion\agent_teams_session_handoff_20260508.md`
> - 完整审计：`F:\Dev\AIstock\docs\analysis\paper_v2_user_requirement_audit_20260507.md`（34 节）
> - Codex 主体设计 + 附录 A/B：`F:\Dev\AIstock\docs\architecture\qe_sota_strategy_package_asset_governance_design_20260508.md`

## 1. 本 session 角色定位

**这是战略 / 分析 session，不是实施 session**：
- 实施工作（21 task 完成 + #27 in-flight + #10 pending）由**另一个 Agent Teams Claude Code session** 在 worktree 上完成
- 本 session 长期保持开启，用于：架构分析、文档写作、用户战略问询
- 实施 session 的进度 / 暂停状态 / teammate 列表见 `agent_teams_session_handoff_20260509.md`

## 2. 本 session 最近完成的工作（2026-05-09）

### 2.1 Agent Teams 监控工具
- 写了 `C:\Users\lc999\agent_teams_monitor.bat` 监控脚本
- 修复两个 bug：
  - wt 把 PowerShell 内部 `;` 误解析为子命令分隔（用 `\;` 转义修复）
  - PowerShell 默认编码非 UTF-8 导致中文乱码（加 `[Console]::OutputEncoding=UTF8` + `Get-Content -Encoding UTF8`）
- 当前状态：4 pane 2x2 布局可用，UTF-8 中文显示正常

### 2.2 Windows split-pane 现实评估
深度调研 Claude Code Agent Teams Windows 终端支持现状（见后文"参考"）：
- **WezTerm / Windows Terminal / Zellij / Ghostty 都不原生支持** Claude Code 的 split-pane backend
- 全部是 OPEN feature request（Issue #23574 / #24384 / #24122 / #31901 / #26572）
- Windows 上"原生看每个 agent 完整工作"唯一可行路径：**WSL+tmux**
- 不切 WSL 的话，最佳方案是"mailbox monitor + 关键产出文件 tail 组合"

### 2.3 测试流水线优先级提前提案（未决）
**用户提议**：等 Codex 开发期间，并行完成测试流水线，独立分支，验证后合并。

我的分析结论：**合理且现在更合适**（比 §22 原计划"Tier 2 穿插"时机更佳），理由：
- Day 1+2 主线第一波已结束，6 teammate idle
- Codex 在独立分支工作，不需要 Lead 高频协调
- Codex Phase 0-1 PR 1-2 周内开始，**那时正好需要自动化 cross-test**
- §22 当时反对的理由（"会抢主线资源"）现在不成立

**未决 4 个决策点**（见 §4）。

## 3. 当前未提交的本 session 内容

无。本 session 写过的：
- 流水线提前提案的分析（口头交付，未写入文档——本 supplement 是首次落地）
- WezTerm / split-pane 调研结论（口头交付，未写入文档）
- agent_teams_monitor.bat（已落地到 `C:\Users\lc999\`，非 git 跟踪）

本 supplement 是**本 session 首个需要 commit 的产出**。

## 4. 用户必须拍板的 4 项决策

### 决策 1：是否启动测试流水线提前提案？
- 选 A：是，按 §5 节奏 2-3 周完成 50% 自动 cross-test 后合 main
- 选 B：否，按 §22 原计划 Tier 2 穿插
- 选 C：先处理 Block 1+2（commit Day 1+2 worktree 内容、拍板 §6 待办），再决定 A/B
  - **我的倾向：C**——避免在已暂停状态上再开新支线

### 决策 2（如启动）：分支命名
- 推荐：`claude/test-pipeline-enhancement-20260509`
- Teammate 来源：从 6 个 idle teammate 转岗（cross-test + impl-paper-v2 最匹配）vs 新 spawn

### 决策 3（如启动）：是否更新 §22 优先级表
- 把 #4 从 Tier 2/3 提到"Tier 1B 早期"作为正式记录
- 写入 `paper_v2_user_requirement_audit_20260507.md`

### 决策 4（沿袭 Day 2 handoff §6）：用户操作待办
- DB migration（broker_backend 字段）
- 8001 重启
- 浏览器手测（Day 2 UI 简化）
- §8.1/§8.2/§8.3/§8.4 audit 决策
- OPEN-EXT-1（Mode G 双 PR）/ OPEN-EXT-2（on_event schema）/ OPEN-EXT-3（broker_compatible 字段双 PR）
- Day 2 worktree 全套是否合 main（按 4 PR 拆分计划）

## 5. 流水线提前方案的具体执行节奏（决策 1 = A 时）

### 5.1 §21.4 列出的 35-40% 缺口

| 子任务 | 工作量 | 备注 |
| --- | --- | --- |
| finding/bug 双 agent 字段 | 0.5-1 天 | 已有 `assigned_agent` 单字段，扩展为 `developer_agent` + `tester_agent` |
| Cross-test 自动路由（按分支前缀） | 2-3 天 | 新增 `cross_test_router.py` 服务（~200 行） |
| Bug 状态机（NEW → ... → CLOSED + REOPEN） | 2-3 天 | 状态字段 + transition API |
| Re-test 自动触发 | 1-2 天 | git_activity_provider + cross_test_router 联动 |
| **MCP server for Claude Code 接入** | 1-2 天 | 参考 mempalace MCP 实现 |
| Validation Center UI 加 agent 列 | 2-3 天 | 前端 layout.tsx + page.tsx |
| 自动 trigger（push → run） | 1-2 天 | GitHub Actions / git hook |
| Tester 权限隔离 hook | 1-2 天 | settings.json + PreToolUse 拦截 |
| **总计** | **10-18 工作日（2-4 周日历）** | 用 6 idle teammate 并行可压缩到 1.5-2.5 周 |

### 5.2 合 main 前必须满足的 6 条 Gate

| # | 条件 |
| --- | --- |
| 1 | 双 agent 字段 schema 兼容老 finding（不破坏现有 7 个模块测试历史） |
| 2 | Cross-test 路由对 codex/* / claude/* 分支前缀都正确路由 |
| 3 | Bug 状态机至少跑通 1 个 happy path + 1 个 REOPEN 路径 |
| 4 | MCP server 在 Claude Code 端可用（手工冒烟） |
| 5 | UI 改造无破坏现有 Validation Center 4059 行功能 |
| 6 | **关键真实验证**：至少 1 个真实 PR（Codex Phase 0 或 Day 1+2 worktree）走通完整自动化 cross-test 流程 |

## 6. 重启后新 session 的执行顺序

### Step 1：读必读文档
1. 本 supplement（`agent_teams_session_handoff_20260509.md` 同目录）
2. `agent_teams_session_handoff_20260509.md`（Day 2 暂停时刻全状态）
3. `agent_teams_session_handoff_20260508.md`（Day 1 启动 + 授权清单）
4. `paper_v2_user_requirement_audit_20260507.md` §22-§34（计划核心）
5. `qe_sota_strategy_package_asset_governance_design_20260508.md` 附录 A+B（协作规范）

### Step 2：询问用户 4 项决策（§4）
按优先级问：
- 决策 1：流水线提前？
- 决策 4：Day 1+2 worktree 内容是否合 main / §6 待办处理？

### Step 3：根据决策启动相应工作流
- 决策 1=C：先解决 Block 1+2，按 Day 2 handoff §7 顺序执行
- 决策 1=A：新建 `claude/test-pipeline-enhancement-20260509` 分支，启动 §5.1 子任务
- 决策 1=B：按 Day 2 handoff §7 顺序，流水线推迟到后续 Tier 2 穿插

## 7. 不要做的事（边界提醒）

- ❌ 不要继续 Day 2 实施 session 的 #27（in-flight）—— 那是另一个 session 的任务
- ❌ 不要重新 spawn 新 teammate —— 6 个 teammate idle 可唤醒，重 spawn 会丢历史
- ❌ 不要直接合 Day 2 worktree 到 main —— 必须等用户决策后按 4 PR 拆分计划
- ❌ 不要重启生产 8001 —— Codex memory line 314 硬约束
- ❌ 不要修改 main 业务代码 —— 仅文档可直接 commit

## 8. 可用资源

- 6 个 idle teammate（含 lead）：team_name=`paper-v2-vnpy-mvp`，可通过 SendMessage 唤醒
- worktree：`F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508`（claude/paper-v2-vnpy-mvp-20260508 分支）
- 监控工具：`C:\Users\lc999\agent_teams_monitor.bat`（4 pane 2x2 布局，UTF-8）
- Codex 集成分支：`codex/qe-governance-integration-20260508`（独立工作，不污染）

## 9. 一句话核心

**本 session 是战略讨论 session（与 Agent Teams 实施 session 平行）**；最近完成监控工具修复 + Windows split-pane 调研 + 流水线提前提案分析。**4 个未决决策**等用户拍板（§4）。重启新 session 后按 §6 顺序执行：先读必读文档 → 询问 4 项决策 → 按决策启动相应工作流。

## 参考：Windows split-pane 调研结论（用于 §4 决策 2 的 teammate spawn 后视）

| 终端 | Claude Code 原生 split-pane 支持 |
| --- | --- |
| tmux | ✅ 唯一原生支持（v2.1.32+） |
| iTerm2 | ✅ 原生支持（macOS） |
| WezTerm | ❌ Issue #23574 OPEN |
| Windows Terminal | ❌ Issue #24384 OPEN |
| Zellij | ❌ Issue #24122 + #31901 OPEN |
| Ghostty | ❌ Blocked on upstream API |
| 元提案 CustomPaneBackend | ❌ Issue #26572 OPEN |

**Windows 上"原生 split-pane 看每个 agent 完整工作"唯一可行 = WSL+tmux**；不切 WSL 的话用 mailbox monitor + 文件 tail 组合是 Windows 原生最佳。

不要因为想要 split-pane 切 WSL —— 跨 WSL→Windows 连 miniQMT 的不确定性远大于 split-pane 收益。
