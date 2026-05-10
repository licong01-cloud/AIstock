# 实施 Session 下一批任务分配（2026-05-09 晚 → 2026-05-10 早）

> **来源**：战略 / 分析 session（Claude Code Opus 4.7）
> **目标 session**：Agent Teams 实施 session（team_name=`paper-v2-vnpy-mvp`）
> **执行窗口**：2026-05-09 晚启动 → 2026-05-10 早用户 review 进展
> **代理**：用户当 relay；本文档是实施 session 的执行说明书

## 0. 启动前必读（实施 session lead）

按以下顺序读完 + 确认理解：

1. **本文档**（你正在读）—— 任务分配与执行规范
2. `F:\Dev\AIstock\docs\discussion\strategy_session_supplement_20260509.md` —— 战略 session 的最新分析（特别是测试流水线提前提案 + 4 项未决决策的当前用户答复）
3. `F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\docs\discussion\agent_teams_session_handoff_20260509.md` —— 你自己 Day 2 暂停时刻的全状态快照（应该已读过，确认仍然准确）

**用户在 2026-05-09 已答复的关键决策**：
- ✅ 测试流水线提前提案 = 可启动，**但**：
- ⚠ **finding_store 双 agent 字段（流水线 §21.4 缺口 #1）今晚暂不做** —— 需要先与 Codex 协商，明天再决定
- ✅ 路径 A+B 协作（用户当 relay + 文档作为 task spec）
- ✅ 严格分工 + 独立分支 + 全面验证前不合 main + 文档可直接 main

## 1. Phase 1：Day 1+2 worktree 整理（最高优先级，必须先做）

**目标**：把 Day 2 全部未 commit 增量按 4 PR 拆分计划合到 `claude/paper-v2-vnpy-mvp-20260508` 分支，**不合 main**。

### 1.1 工作内容

按 Day 2 handoff §3 内的 4 PR 拆分计划提交：

| PR # | 范围 | 涉及文件类别 |
| --- | --- | --- |
| PR-A | 后端基础（MarketDataSource.MINIQMT_REALTIME 枚举 + portfolio.broker_backend 字段） | `backend/services/paper_trading_v2/` 后端 |
| PR-B | LocalSim BrokerBackend Protocol 完整实施 | `backend/services/trading_core/` 新建 |
| PR-C | 前端 UI 简化 B 前 3 项 | `frontend/src/app/paper-v2/` |
| PR-D | 设计稿 5 份 + cross-test v0.5 增量 + 测试代码（45 新测试） | `docs/architecture/` + `tests/` |

每个 PR：
- ✅ commit message 清晰（含 scope 标签）
- ✅ 推送到 `claude/paper-v2-vnpy-mvp-20260508`（**不开 PR 到 main**）
- ✅ 116 全套测试无回归（已确认）

### 1.2 完成后 lead 做的事

- 在 `docs/discussion/` 新写一份 `day12_4pr_split_status_20260509.md` 记录 4 PR 状态（commit hash / 大小 / 测试覆盖）
- 等 Phase 2 启动

**Phase 1 估算**：1-2 小时（已暂停，整理性质工作）

## 2. Phase 2：5 teammate 并行下一批任务（overnight）

**核心约束**：
- 严格不动 Codex 范围（`backend/services/quantevolver/` / `qe_strategies/` / `model_registry`（新 schema）/ `aistock_strategy_catalog`）
- **不动 `backend/services/validation/finding_store.py`**（finding_store schema 改动 = 与 Codex 协商点，今晚禁动）
- 5 teammate × 5 thread 并行；lead 协调
- 所有代码工作在 `claude/paper-v2-vnpy-mvp-20260508` 分支（commit 时 message 含 thread 标签 `[T1-blocker]` `[T2-ui]` 等）
- 文档类（thread 3 + 4 部分）可直接 commit 到 main（沿用至今规则）

### 2.1 Thread T1：impl-paper-v2 → Paper v2 阻断点修复

**任务**：修 §0/§7 P0-4 live inference 冷启动 30+ 失败的根因（preflight 缺失）。

**输入参考**：
- `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §0 P0-4
- `docs/analysis/paper_v2_blockers_20260508.md`（你 Day 2 自己写的阻断点分析，含修复方向）

**输出**：
- `backend/services/strategy_package/live_inference.py` 加 preflight 函数（QE source / node / conf.yaml / factor / model params 5 项检查）
- `backend/services/selection_center/` 调用点接入 preflight，失败立即结构化 fail-fast（不进入正式 selection run）
- 至少 5 个新测试 case（preflight 各分支）
- 不动 quantevolver / qe_strategies / model_registry

**完成判定**：
- 选一个 ST PIT manifest 跑 selection，验证 preflight 失败时返回结构化错误（不再 30 分钟超时）
- L0/L1 通过

**估算**：5-8 小时

### 2.2 Thread T2：ui-simplify → UI 简化 §1 B 后续项

**任务**：继续 Day 1+2 已做的 §1 B 前 3 项之后的 UI 简化项。

**输入参考**：
- `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §1（UI 复杂度审计）
- 你 Day 2 已交付的 B 前 3 项（在 worktree）

**优先做的几项**：
- §1.1 命名问题：把 SHA256 截短哈希、英文枚举（`BACKTEST_APPROVED` 等）替换成中文友好显示
- §1.5 错误展示：把 `JsonPanel` dump 改为结构化错误卡（至少 2-3 处典型）
- §1.4 流程引导：在首页或顶部 nav 加"步骤 1→2→3"流程示意（最简版）

**输出**：
- `frontend/src/app/paper-v2/` 内的 UI 改动
- 不破坏现有功能（手测 happy path）

**完成判定**：
- frontend 构建通过 + ts 类型检查通过
- 现有 Playwright 测试（如有）不退化

**估算**：5-8 小时

### 2.3 Thread T3：engine-design → 双纸面设计

**任务**：写两份纸面设计文档（不写代码）。

**输出 #1**：`docs/architecture/mcp_server_for_validation_center_design_20260509.md`
- MCP server 让 Claude Code 自然 read findings / bugs / runs / module test matrices
- 参考已有 `mempalace` MCP 实现模式
- **不涉及 finding_store schema 改动**（当前 read-only API 对接已有字段即可）
- 含：API 列表、参数 schema、错误处理、与 Cross-testing 流程衔接

**输出 #2**：`docs/architecture/shadow_run_consistency_infrastructure_design_20260509.md`
- §11.3 / §15.6 提到的 "QE backtest vs Paper v2 重放 vs 未来实盘" 输出对账框架
- 含：触发机制（夜跑 / 手动）、对比维度（NAV / 持仓 / 换手 / 费用）、容忍度阈值、告警机制、与 Codex Validation Mode A-F / 附录 A.3.4 Mode G 整合
- 不写实施代码（设计阶段）

**完成判定**：
- 两份文档完整可 review
- 直接 commit + push 到 main（文档类）

**估算**：4-6 小时

### 2.4 Thread T4：cross-test → 模块测试矩阵补齐

**任务**：补 5 个新模块的测试矩阵（写测试 plan 而不是测试代码；纯文档）。

**输入参考**：
- `tests/aistock_validation/catalog/test_levels.md` L0-L5 定义
- 已有 7 个模块矩阵作为模板（`tests/aistock_validation/modules/`）

**新建 5 份**：
1. `tests/aistock_validation/modules/strategy_engine.md`（§A.3 Strategy Engine 层）
2. `tests/aistock_validation/modules/qe_paper_consistency.md`（QE backtest vs Paper v2 重放对账）
3. `tests/aistock_validation/modules/trading_core.md`（vn.py 接入 + OEMS）
4. `tests/aistock_validation/modules/paper_v2_blockers.md`（§0/§7 阻断点系列）
5. `tests/aistock_validation/modules/ui_simplification.md`（§1 UI 简化）

每份含：
- 模块 ID + 描述 + 风险等级
- L0 / L1 / L2 / L3 测试 case 清单（每级至少 3 case）
- pass criteria（明确量化）
- 失败处理预期
- 与 Codex 模块的边界声明（哪些不属于本模块）

**严格不要做**：
- ❌ 写 Codex 模块的测试矩阵（如 `qe_governance.md` / `model_registry.md` / `qe_reproducibility.md` / `strategy_package_v2.md` / `qe_validation_modes.md`）—— **那 5 份是 Codex 的活**（按附录 A.5.1 分配）
- ❌ 修改 `backend/services/validation/finding_store.py` 任何 schema

**完成判定**：
- 5 份矩阵符合现有 7 份模板格式
- 直接 commit + push 到 main（文档类）

**估算**：4-6 小时

### 2.5 Thread T5：env-poc → vn.py + Paper v2 MVP 第 2 周

**任务**：vn.py + miniQMT MVP 端到端 sim 跑通（接续 Day 1+2 完成的 PoC + LocalSim BrokerBackend）。

**输入参考**：
- Day 1+2 已交付：MarketDataSource.MINIQMT_REALTIME 枚举 + portfolio.broker_backend 字段 + LocalSim BrokerBackend Protocol（在 worktree）
- `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §16（4 周 MVP 计划）

**目标**：实现"用 main 上现有 ST PIT manifest 跑通端到端 sim"：
- StrategyPackage manifest → trading_core → SimGateway 模拟成交
- 一笔订单的完整生命周期（submit → fill 事件 → position 更新）走通
- daemon_event_log 表写入新事件

**输出**：
- `backend/services/trading_core/` 端到端 sim 跑通的代码
- 至少 1 个真实 manifest 的 demo 跑通记录（写到 `tests/aistock_validation/history/paper_v2_vnpy_mvp/20260509_sim_endtoend.md`）
- 至少 5 个新集成测试

**严格约束**：
- ❌ 不动 Codex 范围（quantevolver / qe_strategies）
- ❌ 不修改 finding_store schema
- ❌ 不重启生产 8001
- ❌ 不真实下单（仅 SimGateway）

**完成判定**：
- 端到端 sim 跑通 + 集成测试通过
- 116 全套无回归

**估算**：6-10 小时（最长的 thread）

## 3. 协作规则

### 3.1 各 thread 的分支策略

- **代码 thread**（T1 / T2 / T5）：**所有代码 commit 到 `claude/paper-v2-vnpy-mvp-20260508`**（不再分子分支，避免管理负担）；commit message 加 `[T1-blocker]` `[T2-ui]` `[T5-vnpy]` 等 scope 标签
- **文档 thread**（T3 + T4）：**直接 commit + push 到 main**（文档类；沿用既定规则）

### 3.2 跨 thread 通信

- 同步信号：通过 `SendMessage` 在 teammate 间传（如 T5 发现 trading_core 接口需求 → SendMessage T1 协调）
- 阻塞：teammate SendMessage 给 lead，lead 决策

### 3.3 Lead 角色

- 启动时：派任务（5 个 SendMessage）+ 做 Phase 1 整理
- 中途：每 1-2 小时收 idle_notification + 跨 thread 协调
- 完成时：写 `docs/discussion/morning_status_20260510.md` 汇总进展

### 3.4 Status 报告时机

每个 teammate 每完成一个明显里程碑，SendMessage 给 lead 报告。Lead 累积进展，写到 morning_status 文档。

## 4. 严格不要做的事（边界）

| 禁项 | 原因 |
| --- | --- |
| ❌ 修改 `backend/services/validation/finding_store.py` 任何 schema | 与 Codex 协商点，明天再做 |
| ❌ 修改 `backend/services/quantevolver/` 任何代码 | Codex 工作面 |
| ❌ 修改 `qe_strategies/` 任何代码 | 同上 |
| ❌ 创建 `model_registry.*` schema | Codex Phase 5 范围 |
| ❌ 写 Codex 范围的测试矩阵（qe_governance / model_registry / qe_reproducibility / strategy_package_v2 / qe_validation_modes） | 附录 A.5.1 分配为 Codex 范围 |
| ❌ 重启生产 8001 / 真实下单 | 安全硬约束 |
| ❌ 合并任何代码分支到 main | 用户审阅前不合 main |
| ❌ 解决 §6 用户操作待办（DB migration / 8001 重启 / 浏览器手测 / §8.x audit / OPEN-EXT-1/2/3）| 等用户决策 |

## 5. 早晨 review checklist（用户次日来看）

实施 session lead 在 morning_status 文档中按以下 checklist 总结：

- [ ] Phase 1 完成（4 PR 已 commit 到 feature 分支，hash 列出）
- [ ] T1 阻断点修复进展（preflight 实施情况）
- [ ] T2 UI 简化 B 后续项进展
- [ ] T3 两份纸面设计完成（已 commit main 的 commit hash）
- [ ] T4 5 份测试矩阵完成（已 commit main 的 commit hash）
- [ ] T5 vn.py + Paper v2 MVP 端到端 sim 跑通 / 阻塞情况
- [ ] 任何意外阻塞（teammate 报告需要用户决策的）
- [ ] 总测试覆盖：116 全套是否仍无回归
- [ ] 与 Codex 边界检查：没有越界改动

## 6. 一句话核心

**今晚 5 thread 并行 overnight 工作 + 早晨写 morning_status 等用户 review；严格避开 finding_store schema 改动（明天与 Codex 协商）+ 严格隔离 Codex 工作面 + 代码分支不合 main + 文档类直接 main**。

## 7. 启动顺序（实施 session lead 按以下顺序执行）

```
Step 0（30 sec）: 读本文档全文
Step 1（5 min）: 读 strategy_session_supplement_20260509.md
Step 2（30 min）: Phase 1 启动 — 派 6 teammate 整理 Day 2 worktree → 4 PR commit + push 到 feature 分支
Step 3（10 min）: Phase 1 完成检查 + 写 day12_4pr_split_status_20260509.md
Step 4（5 min）: Phase 2 启动 — 5 SendMessage 并行派任务给 5 teammate（按本文档 §2.1-§2.5）
Step 5（持续）: Lead 接收 idle_notification + 跨 thread 协调 + 累积 morning_status
Step 6（早晨 5-7 AM）: 写 morning_status_20260510.md 汇总
Step 7: 等用户来看 review
```
