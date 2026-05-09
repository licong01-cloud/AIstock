> 状态：草稿（待 D2 完成后激活）。本文档仅起草、不发起 cross-tool [DECISION]，避免 D2 合并节奏被 D3 抢跑。
> Created: 2026-05-10. Activation gate: D2.b 全部 PR 合 main。

# D3 finding_store schema v2 双 PR 提案（草稿）

本文档基于 `F:\Dev\AIstock\docs\discussion\user_decisions_for_morning_review_20260510.md` §4.3
中已起草的 GitHub Issue 草稿展开，落地为 worktree 内的工程提案，作为后续 cross-tool
[DECISION] D3 抽屉的主要引用依据。

---

## §1 背景与动机

### 1.1 缺口来源

`docs/analysis/paper_v2_user_requirement_audit_20260507.md` §20-§21（Cross-testing pipeline 增强提案）
中明确指出：finding/bug 记录必须能区分 **代码作者** 与 **测试执行者**，否则无法支撑：

- §21 Cross-test 自动路由（developer_agent ≠ tester_agent 才能触发跨测）
- §21.4 Bug 状态机的 owner 转移（assigned developer vs assigned tester）
- Paper v2 / Selection Center UI 的 agent 列展示（区分谁开发 / 谁测试）

### 1.2 Codex 现状（为何 PR 1 由 Codex 主导）

Codex 已合入两个写 finding_store 的提交：

- `1a17bca` — Phase 7 stability_scoring（向 finding_store 写 stability 类 finding）
- `46bcdda` — validation modes foundation（validation run 写 finding）

这意味着 **Codex 现在是 finding_store 的主要 producer**。schema 演进由 producer 主导是
governance design 附录 A.4.4 双 PR 模式的一贯原则，能把"写入兼容性风险"留在 producer 自己
的回归测试里，最早暴露问题。

### 1.3 现状字段（v1）

`backend/services/validation/finding_store.py` 当前仅有单一 `assigned_agent`，无法承载
"developer ↔ tester"二元关系。

### 1.4 引用

- `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §20-§21
- `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` 附录 A.4.4 / A.4.5
- `F:\Dev\AIstock\docs\discussion\user_decisions_for_morning_review_20260510.md` §4

---

## §2 Schema 字段差异：v1 现状 vs v2 目标

### 2.1 v1 现有字段（节选 finding_store 关键列）

| 字段 | 类型 | Nullable | 默认 | 说明 |
|------|------|----------|------|------|
| `finding_id` | string (UUID) | no | gen | 主键 |
| `module` | string | no | — | 7 模块测试矩阵中的模块名 |
| `severity` | enum | no | — | critical / major / minor |
| `assigned_agent` | string | yes | null | **当前唯一 agent 字段，二义性严重** |
| `created_at` | timestamp | no | now() | — |
| `status` | enum | no | open | open / fixed / wontfix |

### 2.2 v2 新增字段（直接采用 user_decisions §4.3 的 yaml）

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

cross_test_routed:
  type: boolean
  required: false
  default: false
  description: "True if finding originated from auto-routed cross-test"

routing_decision:
  type: object
  required: false
  default: null
  fields (optional):
    source_branch: string
    target_agent: string
    routing_rule: string
  description: "Audit trail of cross-test routing logic"
```

### 2.3 v1 → v2 兼容映射（fallback 规则）

| v2 字段 | v1 缺省时的派生规则 |
|---------|---------------------|
| `developer_agent` | 由 commit author 派生：分支前缀 `codex/*` → `codex`；`claude/*` → `claude-code`；其他 → `user` |
| `tester_agent` | fallback 至 v1 的 `assigned_agent`；若也为空，则 = finding 的 creator |
| `cross_test_routed` | 默认 `false`（保持 v1 行为） |
| `routing_decision` | 默认 `null` |

兼容期：v1 `assigned_agent` 字段保留 6 个月（标 deprecated，但仍可读写），到期后由独立的
PR 4 删除。

---

## §3 PR 1（Codex producer 角色，建议）

### 3.1 分支与 owner

- **建议分支**：`codex/qe-finding-store-v2-schema`
- **建议 owner**：Codex（理由见 §1.2）

### 3.2 内容范围

- 在 ORM / Pydantic 模型中加入 §2.2 的 4 个 v2 字段
- v2 字段全部 optional / nullable，**不启用默认产出 v2**（即默认 writer 仍走 v1 字段）
- 保留 v1 `assigned_agent` 字段（标 deprecated，6 个月兼容期）
- 提供 `_derive_developer_agent_from_branch(branch_name)` 工具函数

### 3.3 测试

- Codex 端 finding_store unit 测试覆盖：
  - v2 字段写入 / 读出
  - v1 字段不变（回归）
  - fallback 规则（commit author 派生）
- Phase 7 stability_scoring smoke：写一条 v2 字段 finding，确认不破坏现有回归

### 3.4 不做的事

- 不切默认产出 v2（留给 PR 3）
- 不动 cross-test 自动路由代码（属于 PR 2 / 后续 epic）

---

## §4 PR 2（Claude Code consumer 角色）

### 4.1 分支与 owner

- **建议分支**：`claude/finding-store-v2-reader`
- **建议 owner**：Claude Code

### 4.2 内容范围

- Cross-test 工具读取端：识别并使用 `developer_agent` / `tester_agent` / `cross_test_routed`
- Paper v2 UI：finding 列表加 agent 列，区分 developer vs tester
- Selection Center API：返回 finding 时透出 v2 字段
- 向后兼容：v1 finding 通过 §2.3 的 fallback 规则填补 v2 字段

### 4.3 测试（5 类读取场景）

| # | 场景 | 输入 | 期望 |
|---|------|------|------|
| 1 | v1 only | finding 仅含 `assigned_agent` | UI 通过 fallback 显示 developer/tester |
| 2 | v2 only | finding 仅含 v2 字段，无 `assigned_agent` | UI 直接读 v2 字段 |
| 3 | mixed | v1 + v2 字段都有 | v2 字段优先，v1 仅作 audit |
| 4 | null fallback | v2 字段全 null | 通过 commit author 派生 developer_agent，tester_agent fallback assigned_agent |
| 5 | cross_test_routed=true | v2 + `routing_decision` | UI 显示路由徽章 + audit trail |

---

## §5 顺序与依赖

### 5.1 PR 顺序

1. **PR 1（Codex producer）先合 main**
2. 在 main 上跑一晚 smoke / 回归（间隔 1-2 天，让 Phase 7 现有写入路径在 v2 schema 下稳定）
3. **PR 2（Claude Code consumer）再合 main**
4. 可选 **PR 3（cutover）**：把默认 writer 切成产出 v2 字段（owner 任意，但建议在 §6 测试矩阵
   全绿后再做）

### 5.2 与 D2.b 的依赖

**硬性前置**：D2.b 的 5-6 个 PR 必须**全部合入 main** 后，才允许在 cross-tool wing/room 留
[DECISION] D3 抽屉激活本提案。原因：

- D2.b 涉及 Selection Center / Paper v2 UI 等会被 PR 2 直接修改的区域
- 若 D2.b 未合，PR 2 与 D2.b 会出现 merge conflict 风暴
- D3 schema 演进对 D2.b 没有反向依赖，因此天然应排在 D2.b 之后

### 5.3 时间预估（沿用 user_decisions §4.3）

- Issue / 提案 review：1-2 天
- PR 1（Codex producer）：2-3 天
- PR 2（Claude Code consumer）：1-2 天
- PR 3（cutover，可选）：0.5-1 天
- 合计：1-2 周

---

## §6 测试矩阵（前向 / 后向兼容）

横轴 = consumer，纵轴 = producer。"mixed-time-series consumer"特指在同一查询窗口内同时
返回 v1 历史 finding 与 v2 新 finding 的场景（典型例子：跨周期回测报告聚合）。

| | v1 consumer | v2 consumer | mixed-time-series consumer |
|---|---|---|---|
| **v1 producer** | ✅ 兼容（无变化） | ✅ 兼容（v2 consumer 走 fallback） | ✅ 兼容（全部走 fallback） |
| **v2 producer** | ⚠ 降级（v1 consumer 看不到 v2 字段，但不报错） | ✅ 完整功能 | ✅ 完整功能（v2 字段缺失项走 fallback） |

### 6.1 失败时的 typed error

- v1 consumer 收到 v2 finding 但试图严格校验 schema 时：抛出 `FindingSchemaVersionMismatch`
  （而非静默忽略）—— 与 `feedback_no_silent_errors.md` 原则一致
- v2 consumer 收到 fallback 推导失败（commit author 解析不出 agent）：抛出
  `DeveloperAgentDerivationFailed`，由调用方决定是否降级到 `user`

---

## §7 Cross-tool 协调点（激活条件）

### 7.1 本文档当前不触发 [DECISION]

明确：本草稿**不**写入 cross-tool wing/room 的 [DECISION] D3 drawer。原因详见文档头部 status
说明 —— 避免 D2 合并节奏被 D3 抢跑。

### 7.2 激活条件

同时满足以下两条：

1. D2.b 全部 5-6 个 PR 合入 main
2. 用户拍板启动 D3（Issue 通道 or Codex App 通道二选一）

### 7.3 激活动作

1. Lead 在 cross-tool wing/room 新增 [DECISION] D3 drawer，引用本文档绝对路径：
   `F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\docs\discussion\d3_finding_store_dual_pr_proposal_20260510.md`
2. 同步 user_decisions §4.3 的 GitHub Issue 草稿（提交方式由届时通道决定）

### 7.4 GitHub Issue vs 本文档的关系

`user_decisions_for_morning_review_20260510.md` §4.3 已经准备了一份完整的 GitHub Issue 正文。
本文档与该 Issue **可二选一或互补**，取决于届时哪个通道更顺手：

- **Codex App 通道顺**：本文档作为提案主体，Issue 仅作为登记/追踪入口
- **GitHub Issue 通道顺**：直接复制 §4.3 的 Issue body，本文档作为 Issue 的"详细附录"链接

---

## §8 关联文档

- `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §20-§21
- `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` 附录 A.4
  （A.4.4 双 PR 模式 / A.4.5 finding_store 演进规约）
- `docs/discussion/strategy_session_supplement_20260509.md` §3
- `F:\Dev\AIstock\docs\discussion\user_decisions_for_morning_review_20260510.md` §4
- `F:\Dev\AIstock\docs\discussion\strategy_session_handoff_20260510_evening.md` §5 P7
