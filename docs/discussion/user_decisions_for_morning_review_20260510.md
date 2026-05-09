# 用户次日决策详细说明（2026-05-10）

> **来源**：Claude Code Opus 4.7 战略 / 分析 session
> **目的**：帮你逐项确认 4 项未决决策；含选项分析、风险/收益、Codex 进展、推荐方案
> **配套文档**：
> - 实施 session overnight 报告：`docs/discussion/morning_status_20260510.md`（304 行）
> - P0-F 根因分析（D1 决策辅助）：`docs/analysis/p0_f_live_inference_root_cause_and_fix_menu_20260509.md`（384 行）
> - 战略 supplement：`docs/discussion/strategy_session_supplement_20260509.md`

---

## §1 Codex 进展速览（来自 git 真源）

通过 `git fetch origin` + 查 `codex/qe-governance-integration-20260509` 集成分支提交历史，**Codex 已经把附录 A.7 表格里的 7 项立即启动工作中的 Phase 0-7 全部完成**。

### 1.1 Codex 已完成的 Phase（按集成分支 commit 顺序）

| Phase | Commit | 时间 | 内容 |
| --- | --- | --- | --- |
| Phase 0 | `b07f346` | 2026-05-09 | governance terminology（术语对齐） |
| Phase 1 | `493bcdf` | 2026-05-09 | manual SOTA promotion review gate（手工 SOTA 流程） |
| Phase 2 | （asset-ledger 分支） | 2026-05-09 | 资产 ledger（已合 governance review） |
| Phase 3 | `dcbe0d0` | 2026-05-09 | paper retest gate（强制原始配置 retest） |
| **Phase 4** | `e138614` + `0d57a19` | 2026-05-09 | **Master Seed Contract 基础（核心 gate）** |
| Phase 5 | `bfa75c6` + `0cdaa60` | 2026-05-09 | Model Registry 4 层架构 foundation |
| Phase 5.1 | `8cbec83` + `b47bcf0` | 2026-05-09 | Model Registry migration smoke |
| Phase 5.2 | `8a7bc18` + `21ef17e` | 2026-05-09 | Model Registry bridge read API（兼容老 catalog） |
| Phase 6 | `d9ce84f` + `63bcc31` | 2026-05-09 | Runtime Variants foundation |
| Phase 6.1 | `a86fe21` + `e31732a` | 2026-05-09 | Governance integration fixes（review 反馈修复） |
| Phase 7（modes） | `46bcdda` + `f95d31c` | 2026-05-09 | Validation Modes A-F foundation |
| Phase 7（scoring） | `1a17bca` + `f593498` | 2026-05-09 | Stability Scoring（seed fragility） |

### 1.2 Codex 已通过的 Review

- `c1308d7` fix(qe): address governance review blockers
- `e31732a` fix(qe): address governance integration review gaps
- `4abec51` merge(main): sync governance integration with Claude docs handoff（Codex 已经吸收 main 上的 docs handoff 同步）
- `bf2ee16` fix(qe): relocate same-node mlruns source symlink（基础设施小修）

### 1.3 Codex 没动的（边界严守）

通过 git 比对 `origin/main..origin/codex/qe-governance-integration-20260509` 的差异：
- ✅ 没动 `backend/services/paper_trading_v2/`（Claude Code 工作面）
- ✅ 没动 `backend/services/strategy_package/runtime.py`（Claude Code 工作面）
- ✅ 没动 `frontend/src/app/paper-v2/`（Claude Code 工作面）
- ⚠ 可能动 `backend/services/strategy_package/` 部分（Phase 5.2 Model Registry bridge 可能涉及） —— 需要细查

### 1.4 Codex 集成分支整体状态

**Codex 主体设计 Phase 0-7 全部完成 ✅**——这意味着按附录 B.4 7 条 merge gate 评估，**Codex 集成分支理论上已具备合 main 资格**，只剩用户最终签字 + DB migration + 8001 重启的"用户操作"步骤。

实际什么时候合 main 由 Codex 自己决定（按附录 A.4.1，主体设计是 Codex 维护范围）。**但 Codex 可能也在等：**
1. finding_store 双 agent 字段协调（与 Claude Code 工作流耦合）
2. 第三方 review（你或 Claude Code cross-test）
3. 用户授权重启 8001

### 1.5 这意味着什么

**整体计划比 §27 时间表快了 1-2 周**。原计划 Codex Phase 0-7 是 Month 1-1.5 → Codex 2 天就跑完（Phase 0-7 全部）。这给了我们更多空间：
- 流水线增强 #3 可以更从容
- §A.3 Strategy Engine 实施可以提前启动（不再等 Codex Phase 4-5 完成 —— 已经完成了）
- 多 alpha (#7) 提前可能性增加

---

## §2 决策 D1：live_inference.py 改动归属（最优先）

### 2.1 背景速览

**问题**：T1 impl-paper-v2 在 `81b1370` commit 中提交了 `backend/services/strategy_package/live_inference.py` 的 preflight 5 项检查实施代码 + 14 测试，**但**：
- `paper_v2_blockers.md` §5 line 76 把 P0-F 列为"待与 Codex 协调"边界
- impl-paper-v2 漏读了 Lead 的 (C) 仲裁（Lead 让它转纯分析，但它按原始派单写了实施代码）
- 实施代码已 push 到 `claude/paper-v2-vnpy-mvp-20260508` feature 分支

**已有决策辅助文档**：`docs/analysis/p0_f_live_inference_root_cause_and_fix_menu_20260509.md`（commit `5515b74`，384 行，含 7 节）

### 2.2 工作面归属冲突分析

| 边界文档 | 归属判断 | 矛盾点 |
| --- | --- | --- |
| `audit §8.5` | `backend/services/strategy_package/` = Claude Code 工作面 | ✅ 支持 keep |
| `paper_v2_blockers.md §5 line 76` | live_inference.py 改动需"与 Codex 协调" | ⚠ 与 §8.5 矛盾 |
| 附录 A.4.1 | Claude Code 工作面包含 strategy_package/runtime.py 等 | ✅ 支持 keep |
| Codex 实际行为 | git 显示 Codex 集成分支 0 触 strategy_package/live_inference.py | ✅ 与 keep 兼容 |

**实际看，4 份证据中 3 份支持 keep，1 份矛盾**——blockers §5 line 76 那句话更可能是 Day 2 时编写者一时谨慎的边界保守化，**不是经过架构决策的硬边界**。

### 2.3 P0-F 修复实质内容

impl-paper-v2 的 preflight 5 项检查（在 `5515b74` 文档 §1 详述）：
1. **QE source 存在性**：检查 manifest 的 source identity
2. **节点可达性**：节点 API ping
3. **conf.yaml 可读**：从节点拉取 conf 验证
4. **factor files 可读**：因子源代码完整性
5. **model params 可读**：模型权重 + 预处理器存在

每项失败立即返回**结构化 typed error**（不再 30 分钟超时后报通用 DataUnavailableError）。

### 2.4 三个选项详细对比

#### 🟢 D1.a — Keep（保留 81b1370 backend 改动）⭐ Lead 推荐

**做什么**：
- 保留 `81b1370` 中的 backend 改动
- 更新 `paper_v2_blockers.md §5 line 76` 边界文字（明确 strategy_package/ 是 Claude Code 工作面）
- impl-paper-v2 后续完善 preflight + 在 UI 接入 typed error 展示

**收益**：
- ✅ 14 测试 + preflight 实施 + P0-F 根因分析全部保留（沉没成本不浪费）
- ✅ Paper v2 阻断点 P0-F 30+ 历史失败的 70-80%（H1/H3/H4/H5）立即缓解
- ✅ 与 Codex 0 冲突（Codex 没动该文件）
- ✅ 工作量最小：只需更新 1 行 blockers §5 边界文字 + 后续完善 2-3h

**风险**：
- ⚠ 边界一致性：blockers §5 与 audit §8.5 短期内有不一致历史；解决方式是更新 blockers §5 文字
- ⚠ 假设错误的可能：万一 audit §8.5 才是错的、blockers §5 是对的（Codex 长期想要 strategy_package/live_inference.py 工作面），事后 revert 成本会比现在 revert 高（已合并的代码被依赖）
  - **缓解**：可以在合 main 前先开 GitHub Issue 给 Codex 公开 review，让 Codex 5 分钟扫一眼确认"我们不要这个工作面"；如果 Codex 没异议，keep 风险接近 0

**总评**：风险低 + 收益高 + 工作量小 + 与 Codex 0 冲突。**强烈推荐**。

#### 🟡 D1.b — Revert（回退 backend 改动，仅保留 frontend）

**做什么**：
- 在 feature 分支上 revert `81b1370` 中的 backend 5 文件改动
- 保留 frontend 12 文件（WorkflowStepper / ErrorListCard 等）
- impl-paper-v2 重写：仅作纯分析报告（不动 backend 代码）
- 之后让 Codex 主导 P0-F 修复

**收益**：
- ✅ 严守"待 Codex 协调"边界（blockers §5 line 76 字面）
- ✅ 避免任何后续可能的"Codex 反对"摩擦

**风险**：
- 🔴 浪费 14 测试 + preflight 代码 + 4-6 小时实施工作
- 🔴 P0-F 30+ 历史失败需要重新等 Codex 排期实施（Codex 主线还在 Phase 0-7 收尾，P0-F 不在他们路线图）
- 🔴 Codex 的 quantevolver 焦点和 strategy_package 焦点不同，他们做 P0-F 反而更慢（他们要先理解 strategy_package 内部约束）
- 🔴 P0-F 是 Paper v2 / Selection Center 用户体验关键阻断点，延后修复影响 Paper v2 演示

**总评**：风险中等 + 收益负 + 工作量浪费。**不推荐**。

#### 🟡 D1.c — 协调 Codex（Issue + PR review）

**做什么**：
- 把 backend 改动作为 PR 给 Codex review
- Codex 决定是否合 main 或 rewrite
- 同时开一个 GitHub Issue 在 Codex 工作面公开讨论 strategy_package/live_inference.py 工作面归属

**收益**：
- ✅ 完整透明的协调流程，避免后续"Codex 反对"的可能
- ✅ 走标准跨工具协作流程（附录 A.4 双 PR 模式的精神）

**风险**：
- ⚠ 等待时间：Codex 可能 review 几小时到几天（不知道 Codex session 节奏）
- ⚠ Codex 可能反对：如果 Codex 认为 strategy_package/live_inference.py 是他们工作面，会要求 revert
- ⚠ 4-6h 协调 + 等待时间，比 keep 慢

**总评**：风险中等 + 收益中等 + 工作量中等。**仅在你想"绝对避免任何边界摩擦"时选**。

### 2.5 Lead 推荐 + 我的进一步建议

**Lead 推荐 D1.a Keep**，理由如 §2.4 所述。

**我（战略 session）的进一步建议**：D1.a + 一个轻量 cross-check，组合方案：

```
D1.a + 轻量协调:
  1. 立即更新 blockers §5 line 76 边界文字（5 分钟）
  2. 在 Codex 集成分支的 GitHub PR description 里加一段 note：
     "本 PR 不动 backend/services/strategy_package/ —— 该工作面属 Claude Code 范围（per audit §8.5）"
  3. Claude Code 把 81b1370 backend 改动作为单独 PR 推到 main
     PR description 引用 audit §8.5 + 注明"已与 Codex 集成分支无冲突"
  4. 你 review 后合 main
```

这个组合：
- 保留 D1.a 的全部收益
- 增加一个轻量 cross-check（不阻塞，但留一份公开记录）
- 如果 Codex 有异议，他们在 review 时能看到，可以及时讨论

---

## §3 决策 D2：Phase 1+2 全套是否合 main（与 D1 强相关）

### 3.1 当前 commit 状态

| Commit | 内容 | 在哪 |
| --- | --- | --- |
| `b5f9e13` | PR-A 后端基础 schema | feature 分支 |
| `7500194` | PR-B LocalSim broker | feature 分支 |
| `f253a6b` | PR-C 前端 UI 简化 B 前 3 项 | feature 分支 |
| `b4177d1` | PR-D 设计稿 + 文档增量 | feature 分支 |
| `81b1370` | T1 backend + T2 frontend 混合 | feature 分支（D1 影响） |
| `e212460` | T5 vn.py MVP 端到端 sim | feature 分支 |
| `3d856f4` + `290455f` | T3 双纸面设计 | **已合 main** ✅ |
| `a814161` | T4 5 份测试矩阵 | **已合 main** ✅ |
| `df758e9` + `8ca58bb` + `7d9a328` | Phase 1 状态 + morning status + addendum | **已合 main** ✅ |

也就是说：**文档类已合 main（T3 + T4 + 状态报告）**；代码类（PR-A/B/C/D + T1+T2 + T5）全部在 feature 分支待审。

### 3.2 三个选项详细对比

#### 🟡 D2.a — 全合 main（D1 拍板后立即创建 5-6 个 PR 合）

**做什么**：
- D1=keep 后，把 `b5f9e13`/`7500194`/`f253a6b`/`b4177d1`/`81b1370`/`e212460` 6 个 commit 全部按 PR 拆分合 main
- 5-6 个 PR，分别有独立 commit message + 描述

**收益**：
- ✅ Codex 端 cross-tester 能 review 完整 Claude Code 端工作
- ✅ 主线快速推进，下一阶段（流水线增强 / vnpy_xt 真接入）可以从 main 拉
- ✅ Day 1+2 的实证（vn.py MVP 端到端 sim 跑通）成为 main 上的稳定参考点

**风险**：
- 🔴 DB migration（PR-A 的 `add_paper_v2_portfolio_broker_backend_20260509.sql`）**未跑过生产 DB**——合 main 后需要立即执行 + 8001 重启
- 🔴 浏览器手测（PR-C frontend + 81b1370 frontend）**未做**——合 main 后用户在 8001 上看 UI 可能踩到 bug
- 🔴 一次合 5-6 个 PR 难以分别回滚

#### 🟢 D2.b — 仅文档 + T3 + T4 已合 main，其他等 D4 后逐 PR 合 ⭐ Lead 推荐

**做什么**：
- 当前状态保持（T3 + T4 已合 main + Phase 1 状态文档已合 main）
- D4（DB migration / 8001 重启 / 浏览器手测）做完后，**分阶段**合代码 PR：
  1. 先合 PR-A（schema + DB migration）→ 跑 migration → 重启 8001 验证
  2. 再合 PR-B（LocalSim broker）→ 跑 sim test 验证
  3. 再合 PR-C（前端 UI 简化）→ 浏览器验证
  4. D1 决策后处理 81b1370（D1=keep 则单独合 backend + frontend）
  5. 最后合 e212460（vn.py MVP）—— 取决于是否真要走 vn.py 路径

**收益**：
- ✅ 风险最小：每次合 1 个 PR + 立即验证 + 出问题立即回滚
- ✅ 用户可以分阶段决策（每个 PR 都可以独立 abort）
- ✅ 与 Codex 集成分支合 main 时的优先级协调更灵活

**风险**：
- ⚠ 时间成本：5-6 次合 main + 每次验证，需要几天
- ⚠ feature 分支与 main 长时间分离，rebase 维护成本

**总评**：风险最低 + 工作量中等 + 灵活性最高。**强烈推荐**。

#### 🟡 D2.c — 等 DB migration + 8001 重启 + 浏览器手测全部完成才合

**做什么**：
- 完全等 D4 全部完成才开始合 main
- 然后一次性合 5-6 个 PR

**收益**：
- ✅ 风险最低（已经手测过）

**风险**：
- 🔴 与 D2.b 比，本质相同但减少了"分阶段验证"的灵活性
- 🔴 一次合 5-6 个 PR 难以分别回滚（与 D2.a 同问题）

**总评**：本质是 D2.a + D4 前置的混合，**不如 D2.b 灵活**。

### 3.3 推荐顺序

**D1.a → D4（按 §5 推荐）→ D2.b** 的链式决策。

D1=keep 后才能讨论 D2；D2.b 中"分阶段合"需要 D4 完成验证。所以执行顺序：
1. 拍 D1=keep
2. 启动 D4（按 §5 节奏）
3. D4 进度推进时按 D2.b 分阶段合 PR

---

## §4 决策 D3：finding_store 双 agent 字段（与 Codex 协商）

### 4.1 背景

测试流水线 §21.4 缺口列表里：
- finding/bug 双 agent 字段（`developer_agent` + `tester_agent`）—— 是 schema 改动，**与 Codex 共用 finding_store**
- 后续依赖：Cross-test 自动路由 / Bug 状态机 / UI agent 列 等都需要这个字段

按附录 A.4.4 双 PR 模式：
1. PR 1（产出端）：在产出 finding 的代码加 schema v2 字段定义（optional / nullable）
2. PR 2（消费端）：消费 finding 的代码加 v2 reader（兼容 v1 默认值）
3. 都合后 → PR 3：切默认产出 v2

**问题**：Codex 也用 finding_store（Phase 4 Seed Fragility Scoring + Phase 7 Validation Run 都写 finding）—— 谁先做 schema v2 + 谁是产出端？

### 4.2 三个路径详细对比

#### 🟢 D3.a — Lead 写 GitHub Issue 给 Codex（**推荐**）

**做什么**：
- 我（战略 session）写一个详细 GitHub Issue：
  - 描述需求：Cross-testing 自动路由需要 finding 区分 developer_agent vs tester_agent
  - 提议 schema v2 字段定义（precise schema with field types / nullable / default）
  - 提议谁做 PR 1 / PR 2（建议 Codex 做 PR 1 因为 Codex 主体设计 Phase 7 已涉及 finding）
  - 提议 timeline
- 用户 review Issue → 给 Codex
- Codex 回复 → 启动双 PR 流程

**收益**：
- ✅ 标准协作流程，留下完整审计追溯
- ✅ 让 Codex 决定 schema 细节（避免你或 Claude Code 一厢情愿）
- ✅ Issue 作为持久参考 —— 未来其他 Cross-testing 决策都可以参考
- ✅ 不阻塞当前工作（Issue 异步推进）

**风险**：
- ⚠ Codex 响应延迟：可能几小时到几天
- ⚠ 多轮反复：schema 细节可能要 review 1-2 轮

**估算**：开 Issue 30 分钟 + Codex 响应 + 双 PR 实施总计 1-2 周

**总评**：标准 + 透明 + 风险低。**推荐**。

#### 🟡 D3.b — 用户开 Issue 协调

**做什么**：
- 你直接开 Issue 给 Codex
- 你描述需求和路径选择

**收益**：
- ✅ 用户身份的 Issue 优先级可能更高
- ✅ 你可以加入额外的产品判断（如对优先级的看法）

**风险**：
- ⚠ 需要你写完整 schema 设计（可能花更多时间）
- ⚠ Codex 可能问你后续问题（你被卷入低层细节）

**估算**：开 Issue 1 小时 + 协调时间

**总评**：和 D3.a 收益相近，但你被卷入 schema 细节。**不如 D3.a**。

#### 🟡 D3.c — 今天暂不开 Issue，等下批工作时再决定

**做什么**：
- 不开 Issue
- 等 Cross-testing 自动路由真正要启动时再讨论

**收益**：
- ✅ 当下不分散注意力

**风险**：
- 🔴 拖延 Cross-testing 自动路由的启动 1-2 周
- 🔴 流水线增强 #3 缺这一步会卡住后续：Bug 状态机 / UI agent 列 等都依赖
- 🔴 错失"Codex 刚完成 Phase 7 还有空"的窗口（Codex 后续可能转其他工作）

**总评**：短期省心 + 长期成本高。**不推荐**。

### 4.3 我的推荐 + 提议的 Issue 内容

**推荐 D3.a**——下面是我**已经准备好的 GitHub Issue 草稿**（你只需复制粘贴到 GitHub）：

---

**Issue 标题**：[RFC] Cross-testing finding/bug schema v2: add developer_agent + tester_agent fields

**Issue Body**：

```markdown
## Background

Per `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §20-§21 and Codex
governance design appendix A.4.5, Cross-testing requires distinguishing the
agent who developed the code (developer_agent) from the agent who tested it
(tester_agent) in finding_store records.

Currently `backend/services/validation/finding_store.py` has a single
`assigned_agent` field. We need to extend the schema to support both fields
without breaking existing 7-module test history.

## Proposed Schema v2

Add to finding_store records:

```yaml
developer_agent:
  type: string
  enum: ["claude-code", "codex", "user"]
  required: false
  default: derived from commit author (codex/* prefix → codex; claude/* → claude-code)
  description: "Agent that authored the code being tested"

tester_agent:
  type: string
  enum: ["claude-code", "codex", "user"]
  required: false
  default: same as creator of the finding (existing assigned_agent fallback)
  description: "Agent that ran the test and recorded the finding"
```

Additional support fields (optional, for completeness):

```yaml
cross_test_routed:
  type: boolean
  default: false
  description: "True if finding originated from auto-routed cross-test"

routing_decision:
  type: object
  optional fields: { source_branch, target_agent, routing_rule }
  description: "Audit trail of cross-test routing logic"
```

## Backward Compatibility

- Old `assigned_agent` field stays (deprecated but functional for 6 months)
- v1 findings without new fields default to:
  - `developer_agent` = derived from commit author
  - `tester_agent` = `assigned_agent` value (fallback)
- All 7 existing module test matrices continue to work unchanged

## Proposed Dual-PR Plan (per appendix A.4.4)

| PR | Owner | Branch | Content |
|----|-------|--------|---------|
| PR 1 (producer) | Codex | codex/qe-finding-store-v2-schema | Add v2 fields to ORM/Pydantic; v2 not enabled by default |
| PR 2 (consumer) | Claude Code | claude/finding-store-v2-reader | Add v2 reader in tools / UI; backward compat |
| PR 3 (cutover) | Whoever | main | Switch default writer to v2 |

## Why Codex as Producer

Codex's recently merged Phase 7 stability scoring (`1a17bca`) and validation
modes foundation (`46bcdda`) write findings; it makes sense for Codex to
extend the schema as the primary producer of these new finding types.

## Timeline Suggestion

- Issue review: 1-2 days
- PR 1 (Codex producer): 2-3 days
- PR 2 (Claude Code consumer): 1-2 days
- PR 3 (cutover): 0.5-1 day
- Total: 1-2 weeks

## Related Documents

- `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §20-§21 / §A.4.4
- `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` 附录 A.4
- `docs/discussion/strategy_session_supplement_20260509.md` §3 测试流水线提前提案

## Action Required

@codex please review and confirm:
1. Is Codex willing to be PR 1 producer?
2. Are proposed v2 fields acceptable?
3. Is dual-PR timeline workable?

@user will coordinate cross-team timing.
```

---

**你只需要复制上面 Issue body 到 `https://github.com/<repo>/issues/new` 即可**。或者授权我自己用 `gh issue create` 提交。

---

## §5 决策 D4：DB migration / 8001 重启 / 浏览器手测

### 5.1 D4 包含的具体待办（按风险/重要性排序）

#### 5.1.1 DB migration（必须先做）

**SQL 文件**：`add_paper_v2_portfolio_broker_backend_20260509.sql`（在 worktree）

**执行命令**：
```bash
# 先 dump（强烈建议）
pg_dump -h localhost -p 5432 -U <user> -d aistock > F:\Dev\AIstock_backups\aistock_pre_broker_backend_20260510.sql

# 跑 migration
psql -h localhost -p 5432 -U <user> -d aistock -f /path/to/add_paper_v2_portfolio_broker_backend_20260509.sql

# 验证
psql -c "\\d paper_v2.portfolio" -d aistock
# 应该看到新字段 broker_backend
```

**风险**：
- 🟢 改动是 additive（仅加字段，不删/不改）—— 按附录 B.5 第 2 条规则
- 🟢 现有 paper_v2.portfolio 行 broker_backend 默认 NULL（不影响现有 portfolio）
- 🟡 万一 SQL 语法 / 类型有 bug，pg_dump 可恢复

**估算**：5-10 分钟

#### 5.1.2 8001 重启（Codex memory line 314 要求）

**为什么需要**：FastAPI 当前进程加载的是老代码（不知 broker_backend 字段）。Migration 后需要重启 8001 以让代码识别新 schema。

**执行**：
```powershell
# 找到 8001 进程
Get-Process -Name "python" | Where-Object {$_.MainWindowTitle -like "*8001*" -or ...}
# 杀进程
# 重启 (per AIstock 启动方式，可能是 start_all_ai_stock.bat 的子集)
```

**风险**：
- 🟡 重启期间 1-2 分钟服务不可用
- 🟡 万一新代码 bug，8001 起不来 —— 立即 revert migration + 回退代码

**估算**：3-5 分钟

#### 5.1.3 浏览器手测（用户操作）

**测试范围**：
- Day 1+2 UI 简化全部（PR-C 前 3 项）
- WorkflowStepper（首页 / 流程引导）
- ErrorListCard（结构化错误展示）
- CopyChip（哈希复制功能）

**手测路径**（建议 8 路）：
1. 首页 → 看 WorkflowStepper 是否正确显示 5 步
2. 任意 portfolio 详情 → 看 broker_backend 字段（是否显示）
3. 触发一个 readiness 失败场景 → 看 ErrorListCard 是否结构化
4. SHA256 哈希点击 → 看 CopyChip 复制功能
5. dataSourceLabel → 看中文标签
6. Selection Center 流程引导
7. Paper v2 portfolio 列表
8. 任意 readiness 报告页

**风险**：
- 🟡 可能发现 frontend bug（CSS / 交互问题）
- 🟡 可能发现 backend 不返回新字段（说明 PR-A schema 改动不完整）

**估算**：30-60 分钟

#### 5.1.4 node_modules 软链清理 + eslint 配置（次要）

**为什么需要**：实施 session morning status §4 D4 提到 node_modules 软链清理 + eslint 配置可能影响前端构建。

**执行**：
```bash
# 清理 node_modules 软链
rm -rf frontend/node_modules
cd frontend && npm install
# 检查 eslint
npm run lint
```

**风险**：🟢 极低

**估算**：10-20 分钟

### 5.2 推荐执行顺序（你来做这些）

```
Step 1（5 min）: pg_dump 生产 DB（备份）
Step 2（5 min）: 跑 DB migration
Step 3（5 min）: 验证 schema（\d paper_v2.portfolio）
Step 4（3 min）: 重启 8001
Step 5（5 min）: 验证 8001 起来 + 老 portfolio 仍可读
Step 6（30-60 min）: 浏览器手测 8 路 + 记录任何 bug
Step 7（10-20 min）: node_modules + eslint 修复（如发现 bug）
```

**总计 60-110 分钟**。

### 5.3 D4 与 D2 的依赖

D4 完成后才能按 D2.b 分阶段合 main。具体：
- D4 Step 1-3 跑 migration → 才能合 PR-A
- D4 Step 4-5 8001 起来 → 才能合 PR-B（LocalSim 需要 portfolio.broker_backend 字段）
- D4 Step 6 浏览器手测 → 才能合 PR-C / 81b1370 frontend

---

## §6 决策依赖关系图

```
                  D1（live_inference 归属）
                         │
                         ▼
         D1.a Keep ──┬──→ 立即开 Issue 让 Codex 知会（轻量协调）
                     │
                     ▼
                D2 节奏选 D2.b（分阶段合 main）
                     │
                     ▼
        D4（用户操作：DB migration / 8001 重启 / 手测）
        │
        ├─ Step 1-3 done → 合 PR-A
        ├─ Step 4-5 done → 合 PR-B
        ├─ Step 6 done → 合 PR-C / 81b1370
        └─ 全部 done → 合 e212460（T5 vn.py MVP）

           D3 完全独立于 D1/D2/D4（可任何时机做）
                     │
                     ▼
        D3.a 开 Issue 给 Codex 协调 finding_store 双 agent
                     │
                     ▼
        Codex 响应 → 双 PR 流程 → main 合并
                     │
                     ▼
        Cross-testing 自动路由 + Bug 状态机 + UI agent 列 解锁
```

---

## §7 推荐执行顺序（按时间）

| 时机 | 你做的事 | 预期工作量 |
| --- | --- | --- |
| **第 1 步（最优先）** | 拍板 D1=Keep（**Lead 推荐 + 我推荐**）；如果有疑虑，读 `5515b74` 384 行根因文档 | 5-15 分钟 |
| **第 2 步** | 拍板 D2=D2.b 分阶段合（**Lead 推荐 + 我推荐**） | 1-2 分钟（与 D1 配套） |
| **第 3 步（独立轨道）** | 拍板 D3=D3.a 开 Issue（**我推荐**）；可以授权我用 `gh issue create` 提交 §4.3 草稿 | 30 秒 - 5 分钟 |
| **第 4 步（用户操作）** | 启动 D4 顺序：pg_dump → DB migration → 8001 重启 → 浏览器手测 | **60-110 分钟** |
| **第 5 步** | 按 D2.b 分阶段合 main（每合一个 PR 验证一次） | 几天逐步 |

**今天可以一气呵成做完 第 1-3 步（决策 5-20 分钟）+ 第 4 步（60-110 分钟）**——**最快 1.5-2 小时完成全部 4 项决策 + 启动后续 PR 合并流程**。

---

## §8 一句话现实

**Codex Phase 0-7 全部完成，比 §27 时间表快 1-2 周；Claude Code overnight 也跑完 5 thread；现在唯一卡住的是 4 项用户决策**。其中 D1（live_inference.py 归属）最紧迫；D2/D4 是 D1 的下游；D3（finding_store Codex 协商）独立可异步推进。

**推荐组合：D1.a + D2.b + D3.a + D4 顺序执行**——风险最低 + 收益最高 + 灵活性最大。

逐项确认时直接告诉我"D1 = a"等即可，我可以帮你启动后续动作（开 Issue / 写 PR description / 协调实施 session 等）。
