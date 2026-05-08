# Agent Teams 会话交接文档

> **生成时间**：2026-05-08
> **交接来源**：Claude Code (Opus 4.7) — 单 session 模式
> **交接目标**：Claude Code (Opus 4.7) — Agent Teams 模式（用户启用 `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` 后的 lead session）
> **目的**：确保 Agent Teams 启用后新 session 能无缝接续本次会话所有上下文、授权、约定、进行中工作

---

## 1. 启动新 session 时的第一步必读文档（按顺序）

新 lead session 启动后**必须先读完这些**才能正确理解上下文：

| 顺序 | 文档 | 用途 |
| --- | --- | --- |
| 1 | **本文档**（`docs/discussion/agent_teams_session_handoff_20260508.md`） | 会话交接 + 待办 + 授权清单 |
| 2 | `docs/analysis/paper_v2_user_requirement_audit_20260507.md`（34 节，~2700 行） | 全方案推导背景；§22-§34 是当前执行计划核心 |
| 3 | `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`（含附录 A、B） | Codex 治理设计 + Claude Code 补充建议 |
| 4 | `docs/codex_project_memory.md` | Codex 维护规则（生产 8001 / 模块边界 / Git 提交） |
| 5 | `~/.claude/projects/C--Users-lc999/memory/feedback_aistock_codex_alignment.md` | Claude Code 与 Codex 协调 13 条约定 |

读完后即可开始正式工作。

---

## 2. 用户在本次会话中的所有授权（不需要再次确认）

| 编号 | 授权内容 | 时间 | 边界条件 |
| --- | --- | --- | --- |
| A1 | 启动 §A.3 Strategy Engine 接口纸面设计 | 2026-05-08 | 仅纸面设计；实际实现等 Codex Phase 4 完成 |
| A2 | **立即**启动 vn.py + miniQMT PoC | 2026-05-08 | 含 `pip install vnpy vnpy_xt` 等依赖安装 |
| A3 | 访问 miniQMT 仿真账户（sim 服务已启动） | 2026-05-08 | 配置在 AIstock `.env` 文件中（位置见 §4） |
| A4 | Paper v2 阻断点分析（§0/§7 P0-4） | 2026-05-08 | **仅分析不改代码** |
| A5 | Cross-test 框架准备 | 2026-05-08 | 写 checklist 模板，**不写 Codex 模块的具体测试矩阵** |
| A6 | 使用 Agent Teams 模式 | 2026-05-08 | 用户负责设置 `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` 环境变量 |

**未授权的事项**（明确不能做）：
- 重启生产 FastAPI 8001（按 codex_project_memory line 314）
- 修改 main 上的代码（仅文档可直接 commit/push 到 main，按用户 2026-05-08 决定）
- 修改 Codex 维护范围的代码（QE / Model Registry / config_composer 等）
- 触动 §0/§7 阻断点的实际代码修复（仅分析，等用户后续授权）

---

## 3. 当前工作并行原则与隔离要求

**用户在 2026-05-08 明确**："需要确保目前所有开始执行的开发都是与 codex 侧并行，需要做好隔离，确保不会冲突，工作没有互相的依赖。"

### 3.1 隔离边界

| 层 | Codex 工作面 | Claude Code 工作面 |
| --- | --- | --- |
| Git 分支前缀 | `codex/qe-governance-integration-20260508` 及其子 worktree | `claude/paper-v2-vnpy-mvp-20260508`（待创建）+ 子 worktree |
| 文件目录 | `backend/services/quantevolver/` / `qe_strategies/` / `model_registry`（新 schema） / 主体设计文档 | `backend/services/paper_trading_v2/` / `backend/services/strategy_package/runtime.py` / `backend/services/trading_core/`（新建）/ `frontend/src/app/paper-v2/` |
| DB schema | `model_registry.*`（新）/ `strategy_pkg.package` 加新可空字段 / `strategy_pkg.seed_fragility_score`（新）/ `strategy_pkg.promotion_review`（新）等 | 不改 DB schema（PoC + Engine 设计阶段不需要 DB 改动）|
| ID 命名空间 | 测试用 `pkg_dev_*` / `mfst_dev_*` / `qe_dev_*` 等前缀 | 同样用 `pkg_dev_*` 前缀（避免与生产冲突） |
| 资产路径 | `rdagent_assets/strategy_package_runtime_dev/` 等 dev 后缀 | 同样用 dev 后缀路径 |

### 3.2 不依赖 Codex 工作的任务（可立即开始）

| 任务 | 不依赖原因 |
| --- | --- |
| **vn.py + miniQMT PoC** | 完全独立的 OEMS 层验证；不依赖 Codex 任何 Phase |
| **Strategy Engine 接口纸面设计** | 仅参考 Codex 主体设计文档（已合 main）；不需要 Codex 代码 |
| **Paper v2 阻断点分析** | 读现有 main 代码 + 分析；不修改任何代码 |
| **Cross-test 框架准备** | 通用模板设计；不涉及 Codex 具体模块 |

### 3.3 必须等 Codex 才能启动的任务（已识别，本 session 不开工）

- §A.3 Strategy Engine **实施**——等 Codex Phase 4（Master Seed Contract）合入集成分支
- §A.3 QE Adapter 实施——等 Codex Phase 5（Model Library）
- §A.3 Paper Adapter 实施——等 vn.py PoC + trading_core daemon

---

## 4. 关键技术信息（启动前必查）

### 4.1 miniQMT 配置位置

用户明确："QMT 模拟盘的配置在 AIstock 的 env 文件中有"

新 session 第一步应：
```bash
cd /f/Dev/AIstock
# 找 .env 文件
ls -la .env* 2>&1
# 找 miniQMT / xtquant 相关变量
grep -i "qmt\|xtquant" .env* backend/config* 2>&1 | head -30
```

**已知信息**：
- miniQMT 仿真服务已经启动（用户 2026-05-08 确认）
- 当前是模拟盘服务
- AIstock 已有自研 QMT 客户端 `backend/infra/qmt_client.py`（1199 行），可作为 vnpy_xt 集成时的"哪些 API 能用"参考——**但不复用其代码，仅作环境验证参考**

### 4.2 当前在 main 上的关键文件状态

最近 commit：`cb3eb13 docs(qe-governance): add Paper v2 audit + appendix A/B for Codex implementation`（已 push 到 origin/main）

| 文件 | 状态 | 大小 |
| --- | --- | --- |
| `docs/analysis/paper_v2_user_requirement_audit_20260507.md` | 已合 main | 34 节 ~2700 行 |
| `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` | 已合 main | 主体 + 附录 A + B = ~2200 行 |
| `docs/discussion/agent_teams_session_handoff_20260508.md` | **本文档**——待 commit | - |

### 4.3 Codex 计划启动的工作

按 §31.5 / 附录 B.10：Codex 立即启动 Phase 0 + Phase 1 + Phase 4（最高优先级）。

**Codex 用 `codex/qe-governance-integration-20260508` 长期集成分支**——不直接合 main，所有 Phase PR 先合入集成分支，整体验证后再合 main（按附录 B.4 的 7 条 merge gate）。

**用户 2026-05-08 明确**："QE 大的架构改动时，使用全新分支"——指 Codex 的代码改动；文档修改可直接 commit/push 到 main。

---

## 5. 进行中的任务清单（新 session 应继续）

按 §31.5 优先级 + 用户 2026-05-08 授权：

| # | 任务 | 状态 | 优先级 | 估算工作量 | 依赖 |
| --- | --- | --- | --- | --- | --- |
| 1 | **建立隔离工作环境**（worktree + 分支） | 待启动（重启后立即做） | 最高 | 30 分钟 | 无 |
| 2 | **核查 QMT 配置 + vnpy_xt 社区情报** | 待启动 | 高 | 0.5-1 天 | 任务 1 完成 |
| 3 | **Strategy Engine 接口纸面设计** | 待启动 | 高 | 0.5-1 周 | 任务 1 完成 |
| 4 | **vn.py + miniQMT PoC**（已授权） | 待启动 | 高 | 3-5 天连通性验证 | 任务 1 + 2 完成 |
| 5 | **Paper v2 阻断点分析**（§0/§7 P0-4） | 待启动 | 中 | 1-2 天 | 任务 1 完成 |
| 6 | **Cross-test 框架准备** | 待启动 | 中 | 1-2 天 | 任务 1 完成 |

**新 session 重启后的第一动作**：
1. 用 `TaskCreate` 重建任务 #1-#6（按上表）
2. 立即启动任务 #1（建立 worktree）
3. 用 Agent Teams `Teammate` 工具 spawn teammate 并行任务 #2 和 #3

---

## 6. Agent Teams 团队结构建议

新 lead session 用 Agent Teams 时建议的团队：

```
[Lead session]（用户 attach 的窗口）
  └── 角色：架构决策、用户交互、集成 review、关键拍板
  └── 处理：任务 #1（建工作环境）、任务 #5（阻断点分析）

[Teammate: env-poc]
  └── 名字：env-poc
  └── 角色：QMT 配置核查 + vn.py PoC
  └── 处理：任务 #2、任务 #4

[Teammate: engine-design]
  └── 名字：engine-design
  └── 角色：Strategy Engine 接口纸面设计
  └── 处理：任务 #3
  └── 输出：docs/architecture/strategy_engine_design_20260508.md

[Teammate: cross-test]
  └── 名字：cross-test
  └── 角色：Cross-test 框架模板准备
  └── 处理：任务 #6
```

**协调方式**：
- Lead 用 `SendMessage` 给 teammate 发指令
- Teammate 完成阶段性进度通过 SendMessage 报告 Lead
- 关键架构决策（如 Strategy Engine 字段是否扩展）必须 Lead 拍板
- 跨 teammate 信息（如 PoC 发现影响 Engine 设计的问题）通过 SendMessage 直接转发

---

## 7. 启动新 session 后建议的执行顺序

```
Step 1（5 分钟）: 读本交接文档 + auto-memory（已自动加载）

Step 2（10 分钟）: 用 TaskCreate 重建任务 #1-#6

Step 3（30 分钟）: 启动任务 #1 — 建立 worktree
   cd /f/Dev/AIstock  # 必须在 git 仓库内
   git checkout main
   git pull origin main
   # 创建 worktree
   EnterWorktree --name paper-v2-vnpy-mvp-20260508

Step 4（30 分钟）: 用 Teammate 工具 spawn 三个 teammate
   - env-poc：去做任务 #2（QMT 配置 + vnpy_xt 情报）
   - engine-design：去做任务 #3（Strategy Engine 设计）
   - cross-test：去做任务 #6（Cross-test 框架）

Step 5（持续）: Lead 在主 session 做任务 #5（Paper v2 阻断点分析）
   + 接收 teammate 的 SendMessage 报告
   + 任务 #2 完成后给 env-poc 派任务 #4（vn.py PoC）

Step 6（每天）: Lead 做 daily review
   + 收集 teammate 进度
   + 处理跨 teammate 协调
   + 与用户同步关键决策
```

---

## 8. 必须注意的边界（防止误操作）

### 8.1 不要做的事

- ❌ 重启生产 FastAPI 8001（codex_project_memory line 314）
- ❌ 修改 main 上的代码（除文档外）
- ❌ 修改 Codex 工作面：`backend/services/quantevolver/` / `qe_strategies/` / Codex 主体设计文档
- ❌ 修改 DB schema（PoC + 设计阶段不需要）
- ❌ 在生产 ID（如 `pkg_xxx` 不带 dev）下创建测试数据
- ❌ 不写 Codex 模块的具体测试矩阵（如 `qe_governance.md`）——那是 Codex 的活

### 8.2 可以做的事

- ✅ pip install vnpy + vnpy_xt（用户已授权）
- ✅ 用 miniQMT 仿真账户跑 PoC（用户已授权）
- ✅ 创建 `claude/*` 分支 + worktree
- ✅ 修改 Paper v2 / strategy_package/runtime.py / trading_core（新建）等 Claude Code 工作面
- ✅ 写 Strategy Engine 设计文档 / Cross-test 框架模板等新文档
- ✅ 运行 PoC 测试脚本
- ✅ 如发现 Codex 模块 bug，按 §A.4.5 / §20-§21 走 Validation Center / GitHub Issue 流程，**不直接改 Codex 代码**

### 8.3 文档归档强制路径（codex memory line 493/498）

- 分析类 → `docs/analysis/`
- 设计/架构类 → `docs/architecture/`
- 标准 → `docs/standards/`
- 讨论/交接类 → `docs/discussion/`（本文档所在位置）

---

## 9. Day 1 结束时（向用户的进度报告模板）

新 session 应在 Day 1 结束时给用户一个简短报告，建议格式：

```
Day 1 进度（2026-05-XX）

✓ 完成：
  - 建立 claude/paper-v2-vnpy-mvp-20260508 worktree
  - 任务 #X 完成（具体内容）
  - 任务 #Y 进展 N%（具体进度）

⚠ 阻塞：
  - （如有）需要用户决策的问题

→ 明日计划：
  - 任务 #X 继续
  - 启动任务 #Y

teammate 状态：
  - env-poc: 进展 X%（关键发现）
  - engine-design: 进展 Y%
  - cross-test: 进展 Z%
```

---

## 10. 一句话核心

**Agent Teams 启用后，新 lead session 读完本文档 + main 上 5 份必读文档，即可立即用 `Teammate` + `SendMessage` 启动 4 条并行任务（#1-#6 中除 #4 外）；任务 #4 vn.py PoC 等 #2 完成后由 env-poc teammate 接手；与 Codex `codex/qe-governance-integration-20260508` 集成分支完全隔离；用户授权清单见 §2，禁止边界见 §8.1。**

整体并行节奏与 §31.5 / 附录 B.10 一致。

---

**交接到此结束**。新 session 启动后请按 §7 顺序执行。如本文档与最新 main 上其他文档冲突，以最新文档为准；如有疑问以与用户最近一轮交互为准。
