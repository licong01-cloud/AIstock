# GitHub Issues 集成 — 与现有流水线增量叠加设计

**作者**: Strategy session (Claude Code 战略)
**日期**: 2026-05-12
**状态**: APPROVED by user, awaiting Codex Task 18 cherry-pick to main → 启动 Task 119 实施

## §1 设计目标

为现有 AIstock 流水线添加 issue tracker 能力, 必须满足:

- **不推倒重来**: 现有流水线 (nox + Validation Center + bugs/ JSON + cross-tool drawers) 100% 保留
- **零破坏性改动**: 仅新增文件 / schema 增量字段, 不修改现有 code/config
- **增量启用**: 6 个新文件覆盖完整功能
- **bugs/ JSON 仍是 source of truth**: git versioned, 完整 audit trail
- **GitHub Issues 是 UI/workflow layer**: 移动友好, Kanban, mobile push, PR auto-link
- **现有"基础代码问题"被加速解决** (不是延误)

## §2 当前流水线 inventory

5 大模块, ~20 子组件:

### 2.1 验证数据层 (`tests/aistock_validation/`)
- `bugs/` 41+ JSON entries (source of truth, 保留)
- `catalog/` (module_registry / test_plans / file_ownership / nox_session_registry, 全保留)
- `guardrails/` rules (保留)
- `dry_runs/` outputs (保留)
- `templates/` (保留)

### 2.2 后端服务层 (`backend/services/validation/`)
- `plan_catalog.py` (保留)
- `runner.py` (保留)
- `results.py` (保留)

### 2.3 前端 UI (`frontend/src/app/validation/`)
- `catalog` / `plans` / `runs` / `reports` 页面 (保留, 加 link button)

### 2.4 CI/CD
- `noxfile.py` ~22 sessions (保留)
- 现有 GitHub Actions yml (保留)
- guardrail scan (保留)
- Stage 6 baseline + Stage 7 完整性 (保留)

### 2.5 协作 + 工具
- MCP server (~12 tools 保留, 增量加 issue tools)
- Cross-tool drawers (mempalace) (保留, 与 issues 互补)
- R6 cutover scripts (保留)

## §3 集成策略

### 3.1 双向同步架构

```
                        git audit trail
  tests/aistock_validation/bugs/ JSON (source of truth)
                      ↕ webhook 双向同步
              scripts/bug_github_sync.py
                      ↕
              GitHub Issues (UI/workflow/mobile/notifications)
                      ↕
                .github/workflows/auto-link.yml
                      ↕
                 PR ↔ commit ↔ baseline runs
                 (现有, 不动)
```

**关键约束**: 任一组件 fail 另一方仍 functional:
- GitHub Issues 服务故障 → bugs/ JSON 完整可用, sync 后追
- bugs/ JSON corrupt → Issues 仍可创建 (sync 修复后重 import)

### 3.2 集成方式 (per 组件)

| 现有组件 | 集成方式 | 改动类型 | 工作量 |
|---|---|---|---|
| `bugs/` JSON | 新增 `scripts/bug_github_sync.py` 双向同步 | **新增文件** | 1.5 天 |
| nox sessions | 不动, 新增 workflow yml fail→file | **新增 yml** | 0.5 天 |
| 现有 GitHub Actions | 复用, 新增 3 个独立 workflow | **新增 yml** | 1 天 |
| MCP server | 新增 `mcp_github_issue_*` tools | **新增 tools** | 1 天 |
| Validation Center UI | 新增 React component (link button + badge) | **新增 component** | 0.5 天 |
| `module_registry.yaml` | schema 增量加 `github_label` 字段 (可选) | **schema 向后兼容** | 0 天 |
| `file_ownership.yaml` | read-only 复用作 issue auto-assign | **不改** | 0 天 |
| Cross-tool drawers | 不动, 分工: drawer=实时, issues=持久 | **不改** | 0 天 |
| Stage 6/7 baseline | 不动, fail trigger auto-file (workflow yml) | **新增 step in yml** | 0.5 天 |
| R6 cutover scripts | 不动 (one-off ops) | **不改** | 0 天 |

**总改动: 6 个新文件 + 1 个 schema 增量字段, 0 破坏性**

总工作量: **~4-5 天 (Codex 主导)**

### 3.3 分工

**现有流水线 (100% 保留)**:
- 测试执行 + baseline (nox + Stage 6)
- 代码 verify (guardrail / lint / mypy)
- Validation Center UI 现有页面
- bugs/ JSON 持久化
- R5/R6 merge gate
- mempalace cross-tool 实时沟通

**GitHub Issues (新增叠加)**:
- Issue lifecycle UI (Kanban / mobile push)
- Auto-discovery 入口 (CI fail → issue)
- 用户报 bug 入口 (Issue Form)
- PR ↔ Issue 自动 link
- Cross-Sprint 跨时间 backlog
- 移动端审批

## §4 现有"基础代码问题"被加速

| 现有基础代码问题 | GitHub Issues 怎么帮 |
|---|---|
| ST PIT event loop 阻塞 (Task 18) | auto-file P0 issue + 链接 fix PR + 不会被遗忘 |
| Selection Center health BLOCKED | 自动追踪 evidence pipeline 缺口 |
| 41 历史 BUG entries 大部分未 cherry-pick to main | 一次性 import 为 Issues, 可视化哪些 verified 未 merged |
| R7 Sprint 5-track 架构修复 | 拆为 epic Issues + sub-issues, 看板 Kanban view |
| pkg_5a5c synthetic evidence 待 rollback | 单独 issue tracking, 自动 link rollback PR |
| Codex N in-flight tasks | 每个 task 对应 issue, mobile 看进度 |

→ Issues 不是"另一个待办系统", 而是给现有问题**找到归宿** + 加速优先级排序

## §5 风险评估

| 风险 | 概率 | 影响 | 缓解 |
|---|---|---|---|
| sync script bug 导致 JSON ↔ Issues 不一致 | 中 | 低 (JSON 是 source) | sync 单独 test; fail 仅 warn 不阻止现有 |
| GitHub Issues 太多噪声 | 中 | 低 | label 严格, auto-file 仅 P0/P1, bulk close |
| Codex PR 自动 link 错乱 | 低 | 低 | PR template + lint check |
| GitHub Actions yml 错误破坏 CI | 低 | 中 | 新 yml 单独文件, fail 不阻止现有 nox |
| Issues 平台故障 | 极低 | 低 | bugs/ JSON 完整可用, sync 后补 |
| 学习曲线 | 0 | 0 | Issues 直观 |

## §6 实施 Plan (3 Phase)

### Phase 0: 准备 (Codex Task 18 cherry-pick to main 后立即启动)

1. 战略 session review 此 design doc
2. 用户最终确认所有 6 个决策点 (见 §7)

### Phase 1: Codex 主导实施 (~4-5 天)

#### 1.1 Repository 配置 (~30 min)
- 启用 GitHub Issues
- 启用 GitHub Projects (Kanban view)
- Issue Templates (4 个):
  - `bug_report.yml`
  - `feature_request.yml`
  - `architecture_rfc.yml`
  - `regression_report.yml`

#### 1.2 Sync 基础设施 (~2 天, Codex 写)
- `scripts/bug_github_sync.py`:
  - bugs/ JSON → GitHub Issue (新建 / 更新)
  - GitHub Issue → bugs/ JSON (新建 / 状态变更)
  - 双向 idempotent
  - 含 dry-run 模式
- `tests/scripts/test_bug_github_sync.py` ≥15 tests
- Webhook receiver (轻量 Python or GitHub Action)

#### 1.3 GitHub Actions yml (~1 天, Codex 写)
- `.github/workflows/issue-auto-link.yml` (PR Fixes #N auto-link)
- `.github/workflows/issue-on-test-fail.yml` (nox fail auto-file)
- `.github/workflows/issue-on-guardrail-fail.yml` (P1 finding auto-file)

#### 1.4 历史数据 import (~0.5 天, Codex 写)
- 41 个现有 bugs/ JSON 一次性 sync 到 Issues
- 加 `import:historical` label
- 保留原 BUG-XXX ID 作为 issue title prefix

#### 1.5 Validation Center UI 集成 (~0.5 天, Codex 写)
- 现有 page 增量加:
  - "View on GitHub Issues" button
  - Open issue count badge (per module)
- 不修改现有 component, 用新 component 包裹

#### 1.6 MCP Server 集成 (~1 天, Codex 写)
- 加 3 个 tools:
  - `mcp_github_issue_list`
  - `mcp_github_issue_create`
  - `mcp_github_issue_search`
- 不动现有 ~12 tools

#### Phase 1 Deliverable
- 6 新文件 (零修改现有)
- bugs/ JSON ↔ GitHub Issues 完全双向同步
- 41 历史 bugs 已 import
- Validation Center 增量 link
- MCP 增量工具

#### Phase 1 流水线验证
- 现有 Stage 6 baseline 仍 GREEN (因 0 现有改动)
- 新增 GitHub Actions workflow 在 staging branch 跑通
- paper-v2 跑 baseline 验证
- main 分支 cherry-pick (R0 风格 + code 部分流水线验证)

### Phase 2: R7 Sprint 中后期协同 (~1-2 周)

1. R7 5 tracks (T1-T5) 作为 5 个 epic Issues
2. Sub-issues 拆分 (每 track ~5-10)
3. Codex 按 sub-issue 派工
4. PR 自动 link sub-issue
5. Milestones: "R7 Phase 1 (T3)", "R7 Phase 2 (T1+T2)" 等
6. 移动 dashboard 看进度

### Phase 3: R8+ (optional, 远期)

- Production observability (Prometheus / Grafana) → auto-file P0 alerts
- AI-driven label 优化 (Codex auto-categorize)
- 跨 Sprint 智能 backlog
- 评估迁移到 Linear (仅当 Issues 不够)

## §7 关键决策点 (User APPROVED 2026-05-12)

| # | 决策 | 选项 | 用户决策 |
|---|---|---|---|
| 1 | 启用 GitHub Issues? | YES / NO | ✅ YES |
| 2 | sync 方向 | 双向 / 单向 | ✅ 双向 |
| 3 | 历史 41 bugs 是否 import | YES / partial / NO | ✅ YES + `import:historical` label |
| 4 | Auto-file 触发范围 | P0/P1 / all | ✅ P0/P1 only |
| 5 | Phase 1 启动时机 | 立即 / R7 Phase 0 / R7 Phase X | ✅ Task 18 cherry-pick to main 后立即 |
| 6 | Validation Center UI 改动范围 | link button / inline list / full | ✅ link button (R8 再扩) |

## §8 与现有 Codex 任务的关系

| Codex 任务 | 与本 design 关系 |
|---|---|
| Task 22 (R7 roadmap + bug tracker automation) | **应纳入本 design** — 不写 from-scratch |
| Task 24 (41 bug entries audit + automation) | **应衔接** — audit 后的 entries 就是 Phase 1.4 import 的输入 |
| Task 20 (5-缺陷 RCA + 重构 design) | **平行不冲突** — R7 5 tracks 是 epic, Issues 是承载工具 |
| **Task 18 (P0 hotfix QE event loop)** | **前置条件** — cherry-pick to main 后才启动 Task 119 |
| Task 119 (本 design 实施) | **本 design 是执行依据** |

## §9 启动条件 (Task 119 Trigger)

**All must PASS**:
1. ✅ Codex Task 18 production-grade hotfix deliver
2. ✅ paper-v2 baseline 流水线验证 PASS
3. ✅ 战略 cherry-pick Task 18 code to main
4. ✅ Production validate (QE 实验启动不再阻塞 UI)

满足后, Codex 立即启动 Task 119 (本 design 实施)。

## §10 总结

| 维度 | 答案 |
|---|---|
| 兼容现有流水线? | ✅ **100% 兼容**, 增量叠加 |
| 推倒重来? | ❌ **不需要** |
| 工作量? | **~4-5 天 Codex 主导** |
| 风险? | **低**, backward compatible |
| 现有"基础代码问题" 加速? | ✅ **是** |
| 启动时机? | Task 18 cherry-pick 后 |
| 长期 ROI? | 跨时间 / 跨设备 / 跨 AI session 协作 |
