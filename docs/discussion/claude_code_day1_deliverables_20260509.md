# Claude Code Day 1 交付清单 + Codex 协调事项

> **生成时间**：2026-05-09（基于 2026-05-08 Day 1 工作）
> **来源**：Claude Code (Opus 4.7) Agent Teams `paper-v2-vnpy-mvp` 团队
> **目的**：把本团队 Day 1 工作 + 待 Codex 配合的 3 个外部决策项 + 后续 Phase 4-6 衔接点整理成单一锚点文档，方便 Codex / 用户随时查阅

---

## 1. Day 1 已交付清单（13 个 task）

| # | Task | 状态 | 产出位置 |
| --- | --- | --- | --- |
| 1 | Paper v2 阻断点分析（P0×8 + P1×7 + 决策清单） | ✅ | `docs/analysis/paper_v2_blockers_20260508.md` |
| 2 | QMT/vnpy_xt 情报核查 | ✅ | `docs/analysis/qmt_vnpy_xt_recon_20260508.md` |
| 3 | vn.py + miniQMT PoC（阶段 1+2） | ✅ | `docs/analysis/vnpy_poc_result_20260508.md` + `vnpy_integration_feasibility_20260508.md` v1.1 |
| 4 | worktree 建立 | ✅ | `claude/paper-v2-vnpy-mvp-20260508` (base: main@2a1b8cb) |
| 5 | Cross-test 框架模板 v0.1 | ✅ | `docs/standards/cross_test_framework_template_20260508.md` |
| 6 | Strategy Engine 接口纸面设计 | ✅ | `docs/architecture/strategy_engine_design_20260508.md` 主体 |
| 7 | Engine 设计 §17 Lead 决议节（Q1-Q8） | ✅ | 同上 §17 |
| 8 | Cross-test 模板 v0.2（§2.4 具体示例） | ✅ | 同 #5 v0.2 |
| 9 | 主 .env userdata_path 修复（生产 bug） | ✅ | `F:\Dev\AIstock\.env` 第 52 行 |
| 10 | vn.py PoC 盘中复测 | 🟡 周一盘中 | `poc/step4_intraday_revalidate.py` |
| 11 | Engine §3.6 BrokerBackend + SimMode 二分（R-Q9） | ✅ | 同 #6 §3.6 / §10.1 / §11 / §17.1 / §17.4 |
| 12 | Cross-test 模板 v0.3（broker_backend 维度） | ✅ | 同 #5 v0.3 |
| 13 | 阻断点清单增 P0-H 两种模拟盘 + §7 R-Q9 落地 | ✅ | 同 #1 §7 |

**main 已合并**：commit 含 6 份核心文档（2872 行新增）→ `origin/main`。
**待 commit**：PoC 源码（`backend/services/paper_trading_v2/poc/`），Task #15 进行中。

---

## 2. 待 Codex 配合事项（OPEN-EXT 三项）

详见 `strategy_engine_design_20260508.md` §17.4。三项均待用户单独授权后启动跨工作面协调。

### OPEN-EXT-1：Mode G 推 Codex 主体 §6 正式纳入（双 PR）

- **当前状态**：Engine 设计文档 §11 单方面声明 Mode G 是合 main 硬 gate
- **需 Codex 配合**：在 `qe_sota_strategy_package_asset_governance_design_20260508.md` 主体 §6 正式 mode 列表纳入 Mode G
- **风险若不做**：类别 C 漂移监控失去 Codex 主体的合规背书

### OPEN-EXT-2：on_event schema 对齐 announcement_event_risk_signal

- **当前状态**：Engine §3.1 on_event docstring 采纳"独立触发 OrderIntent 调整"语义，但字段 schema 未与 announcement_event_risk_signal 设计对齐
- **需 Codex 配合**：拉通 `announcement_event_risk_signal_top_level_design.md`（Codex 维护）的字段定义
- **风险若不做**：Engine 实施期 on_event 输入字段需返工

### OPEN-EXT-3：StrategyPackage manifest 加 `broker_compatibility` 字段（双 PR）

- **当前状态**：Engine §3.6.5 定义字段语义（`Literal["LocalSim_only", "MiniQMTSim_only", "both"]`，默认 `"both"`，LEGACY 默认 `"LocalSim_only"`）
- **需 Codex 配合**：
  1. `strategy_pkg.package` 表加字段 + alembic migration（Codex schema 边界）
  2. qe_source_resolver 的 `_build_manifest()` 写入字段
  3. 切换默认值逻辑
  4. 重 freeze 现有 4 个 LEGACY_NON_ST_PIT 包（标 `LocalSim_only`）
- **过渡方案**：Engine 实施期可借 `custom_extension.broker_compatibility` 占位（audit-only，与 R-Q2 一致）；待 Codex 字段落地后切换
- **范围收窄说明**：`MarketDataSource.MINIQMT_REALTIME` 枚举扩展属于 `paper_trading_v2/market_data.py` = Claude 工作面（按 codex_project_memory line 944），**不在 OPEN-EXT-3 范围内**，由 impl-paper-v2 独立推进

---

## 3. R-Q9 决策摘要（用户 2026-05-08 已授权）

| # | 决策点 | 选定 | 落地位置 |
| --- | --- | --- | --- |
| Q9.1 | 引入 `BrokerBackend` 抽象（LocalSim / MiniQMTSim） | A 引入 | Engine §3.6.1 |
| Q9.2 | 多策略包并行属性绑定 LocalSim | A 是（LocalSim N 包；MiniQMTSim 单例） | Engine §3.6.3 |
| Q9.3 | 行情通道强绑定撮合端 | A 强绑定（fail-fast） | Engine §3.6.4 + market_data.py 扩展 |
| Q9.4 | StrategyPackage 加 `broker_compatibility` | 是 | Engine §3.6.5 + OPEN-EXT-3 |

---

## 4. 后续 Phase 4-6 衔接点（Codex 主体集成分支）

### Codex Phase 4（Master Seed Contract）

| Engine 设计衔接点 | 等待事项 |
| --- | --- |
| Engine §3.2 SeedBundle 字段 | Codex Phase 4 Master Seed Contract schema |
| Engine §3.5 DecisionTrace 加 `seed_bundle_digest` | Phase 4 写入流程 |
| Engine §7.3a SeedBundle 强制写入（R-Q5） | Phase 4 L4 byte-equal gate 验证 |

### Codex Phase 5（Model Library）

| Engine 设计衔接点 | 等待事项 |
| --- | --- |
| Engine §3.4 Model Registry 选择 | Phase 5 Model Library API |
| Engine §17.4 OPEN-EXT-3 manifest 字段 | Phase 5 strategy_pkg.package schema 扩展 |

### Codex Phase 6（RuntimeOverlay schema）

| Engine 设计衔接点 | 等待事项 |
| --- | --- |
| Engine §3.2 RuntimeOverlay allow-list | Phase 6 `package_runtime_variant` schema（R-Q6 派生源） |

### Claude Code 工作面独立推进项（不依赖 Codex）

- A#3 `MarketDataSource.MINIQMT_REALTIME` 枚举（impl-paper-v2，进行中）
- A#4 `portfolio.broker_backend` 字段（impl-paper-v2）
- A#1 LocalSim BrokerBackend 实施（impl-paper-v2 + engine-design 协作）
- B 前 3 项 UI 简化（ui-simplify，进行中）
- vnpy connect 层 dry-run 实证（env-poc，待 engine-design Task #17.3 设计稿）

---

## 5. 当前未决决策（用户决策清单）

| 来源 | 决策点 | 影响 |
| --- | --- | --- |
| audit §8.1 | 配置冻结边界（A 保留 / B 极简 / C 软合约） | 决定 backtest_contract 改动范围 |
| audit §8.2 | "统一引擎"含义（A 字段一致 / B Qlib 统一 / C 明确分工） | 决定 Strategy Engine 实施目标 |
| audit §8.3 | UI 简化方向（A 任务向导 / B 角色拆分 / C 重命名+折叠） | 决定 P0-C / P1-G UI 改造方向 |
| audit §8.4 | 日频/尾盘策略（A 暂不支持 / B 优先日频 / C 同时补） | 与 vn.py PoC 解耦 |
| OPEN-EXT-1 | Mode G 主体 §6 双 PR 推进 | 跨 Codex 协调 |
| OPEN-EXT-2 | on_event 对齐 announcement 设计 | 跨 Codex 协调 |
| OPEN-EXT-3 | broker_compatibility 字段双 PR | 跨 Codex 协调 |

---

## 6. 团队 Day 2+ 工作分配（2026-05-09 启动）

| Teammate | 任务 | 状态 |
| --- | --- | --- |
| impl-paper-v2 | Task #16 → #19 → #20（MarketDataSource 枚举 / portfolio.broker_backend 字段 / LocalSim BrokerBackend Protocol） | 启动中 |
| ui-simplify | Task #18（B 前 3 项 UI 简化） | 启动中 |
| engine-design | Task #17（C 设计 4 份） | 进行中 |
| env-poc | Task #15（PoC 目录整理）+ Task #10（周一盘中） | 进行中 |
| cross-test | Task #21（模板 v0.4 + LocalSim/MiniQMTSim 矩阵草稿） | 进行中 |
| team-lead | Task #14（本文档）+ 监督 / 集成 review | 完成本文档 |

---

**结束**。本文档随团队进度 / Codex 反馈 / 用户决策更新。下次重大变化触发 v2 版本。
