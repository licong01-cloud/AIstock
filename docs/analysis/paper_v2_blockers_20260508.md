# Paper v2 阻断点清单（基于 audit §0 / §7 提炼）

> **生成时间**：2026-05-08
> **来源**：`docs/analysis/paper_v2_user_requirement_audit_20260507.md` §0 + §7
> **范围**：仅分析（授权 A4），不修改任何代码
> **作者**：Claude Code Lead session（worktree `claude/paper-v2-vnpy-mvp-20260508`）
> **目的**：把审计报告中分散的 P0/P1 阻断条目压缩成一张可执行优先级表，便于后续按需推进

---

## 1. P0 阻断点（架构 / 核心数据 / 推理）

| # | 阻断点 | 类别 | 证据（文件:行） | 当前状态 |
| --- | --- | --- | --- | --- |
| P0-A | `backtest_contract` 把 portfolio_policy / minute_execution / risk_policy / HMM / industry_blacklist / tradability / stock_pool 6 项强制锁定与 QE 一致 | 架构 | `backend/services/strategy_package/backtest_contract.py:59-93,125,234,406-556` | 与"策略包仅冻结模型+因子"产品意图相反；UI 上"可改"实际改不动 |
| P0-B | "统一引擎"实际只是字段一致性，三套独立执行栈（QE Qlib / Selection Python / Paper Python）算法实现不同 | 架构 | `quantevolver/executors/backtest.py:27` vs `selection_center/service.py:79` vs `paper_trading_v2/day_runner.py:77` | 同一份 manifest 在三处可能产出不同持仓/净值；contract 无法覆盖动态 n_drop / hold_thresh / 数据窗口语义 |
| P0-C | 缺少"从 QE source 一键到选股结果"原子化入口 | 流程 | 用户路径需 ≥8 次交互跨 2-3 页面（packages→selection） | 阻断 UI 简化目标 |
| P0-D | ST PIT universe spans 仅到 2026-04-30，落后于交易日 | 数据 | Codex 文档 P0-2 | 选股配置阶段不暴露，运行后才失败 |
| P0-E | 4 个可选策略包全部 LEGACY_NON_ST_PIT | 数据 | Codex 文档 P0-1；UI 警告条 `selection/page.tsx:367` | 当前无任何 RUNNABLE 包可入选股 |
| P0-F | live inference 冷启动失败 30+ 次 | 推理 | Codex 文档 P0-4 | 信号路径不稳定 |
| P0-G | strict feature coverage 可能为 0 | 推理 | Codex 文档 P0-5 | 关联 P0-D / P0-E |
| P0-H | **两种模拟盘形态从未规划**：现有 Paper v2 = 纯本地撮合 + TDX 行情；miniQMT 通道仅以独立 client 存在未接入 paper_trading_v2；audit / Codex 主体设计 / Engine 设计均无"本地多策略包并行 vs miniQMT 单策略包"产品形态二分 | 架构 | `paper_trading_v2/` 0 处触碰 miniQMT；`backend/infra/qmt_client.py` 1199 行未被 paper 使用；audit §6 仅讲 session mode 不讲 broker backend | P0（已用户授权 4 项决策落地，详见 §7） |

## 2. P1 功能 / UX 缺口

| # | 阻断点 | 类别 | 证据 | 备注 |
| --- | --- | --- | --- | --- |
| P1-A | 日频策略路径完全缺失 | 功能 | `qe_source_resolver.py:526-540` 仅允许 `1min`/`5min` | QE 实验合约层就限死，去掉 contract 锁也无法跑日频 |
| P1-B | 尾盘处理策略概念不存在 | 功能 | 全库无 `pre_close` / `tail_period` / `close_handling` 字段 | 属于产品功能缺失，非"配置不可调" |
| P1-C | UI 单页交互元素过多 | UX | `paper-v2/page.tsx`(174 行 ~18 元素) / `selection/page.tsx`(468 行 ~25+) / `run-console/page.tsx`(722 行 ~45+) | 无步骤式引导 |
| P1-D | 内部哈希、英文枚举直接展示 | UX | `packages/page.tsx:322-323` / `paper-v2/page.tsx:158` / `selection/page.tsx:391` | `STATUS_LABELS` 仅在 StatusBadge 内生效，列/下拉/错误条仍英文 |
| P1-E | 错误诊断大量 JsonPanel dump | UX | `run-console/page.tsx:566,643` | 给开发者格式不是用户格式 |
| P1-F | 冻结字段 vs 可调字段无视觉区分 | UX | UI 表单全部允许填写但提交侧驳回 | 加深 P0-A 的混淆 |
| P1-G | 7 个平级 tab 无主从引导 | UX | `paper-v2/layout.tsx:8-16` | 总览页"流程看板"仅是 4 个 MetricCard |

## 3. 不在阻断列表的项（已达标 / 风险低）

| 需求 | 状态 | 来源 |
| --- | --- | --- |
| 强制实盘数据，禁用回测产物 | 基本达标（仅余 readiness 主动告知 TDX 可用性的提前提示） | audit §4 |
| 回放 / 实盘平滑转换（CATCHUP_THEN_LIVE） | 达标 | audit §6；`live_session.py:111-192` |

## 4. 阻断点之间的依赖关系

```
P0-A (backtest_contract 锁) ──► P0-C (一键流程) ◄── P1-D/E/F/G (UI)
        │
        ├──► P1-A (日频)：即便 P0-A 解开，P1-A 仍需 QE 合约层先放开 freq
        └──► P1-B (尾盘)：与 P0-A 解耦，但需先决策 §8.4

P0-B (执行栈一致性) ──┬──► §8.2 用户决策（A 字段一致 / B Qlib 统一 / C 明确分工）
                     └──► 决定 Strategy Engine 设计目标（参 engine-design teammate）

P0-D / P0-E / P0-F / P0-G ── 数据/推理基础设施 ── 不解就没有可用策略包

P0-H (两种模拟盘形态) ──┬──► R-Q9.1-Q9.4（已用户授权 2026-05-08，详见 §7）
                       ├──► Engine §3.6 BrokerBackend 抽象（待 engine-design 落地）
                       ├──► MarketDataSource 枚举扩展（OPEN-EXT-3 候选）
                       └──► 与 P0-A / P0-B 协同：BrokerBackend 让"统一引擎"语义清晰

P0-D ◄── ST PIT universe 续期任务（Codex 工作面）
P0-E ◄── 资产准入审计正向状态机重设计（与 P0-A 联动）
P0-F ◄── live inference 冷启动鲁棒性（推理路径排查）
```

## 5. 与本 worktree 工作的衔接

按本会话授权 §A4：**仅分析，不改代码**。后续修复需用户单独授权。

| 阻断点 | 与本 worktree 的关系 |
| --- | --- |
| P0-A | 等用户决策 §8.1（A/B/C），决策后再决定 backtest_contract 改动归属 |
| P0-B | engine-design teammate 的纸面设计输出会触及；不实现 |
| P0-C / P1-C-G | UI 简化方向待用户决策 §8.3；不在本次 vn.py PoC scope |
| P0-D / P0-E | 数据基础设施由 Codex 工作面负责（universe / strategy package 状态），本 worktree 不动 |
| P0-F / P0-G | live inference 路径由 Codex 主导；本 worktree 不改 |
| P1-A / P1-B | 日频 / 尾盘属于功能缺失，与 vn.py PoC 解耦；待用户决策 §8.4 |

**结论**：本 worktree 当前 4 周 MVP 工作（vn.py + miniQMT PoC、Strategy Engine 设计、Cross-test 框架）**不直接触发** P0 阻断点的修复，但 Strategy Engine 设计需要把 P0-A / P0-B 的解耦边界纳入字段语义分歧清单（详见 engine-design teammate 输出）。

## 6. 待用户决策清单（来自 audit §8）

| 决策点 | 选项 | 与本 worktree 的关联 |
| --- | --- | --- |
| §8.1 配置冻结边界 | A 保留 / B 极简策略包 / C 软合约 | 影响 Strategy Engine 的 contract 字段范围 |
| §8.2 "统一引擎"含义 | A 字段一致 / B Qlib 统一 / C 明确分工 | 直接决定 Strategy Engine 设计目标 |
| §8.3 UI 简化方向 | A 任务向导 / B 角色拆分 / C 重命名+折叠 | 不在本 worktree scope |
| §8.4 日频/尾盘 | A 暂不支持 / B 优先补日频 / C 同时补 | 不影响 vn.py PoC（vnpy 本身支持日级与分钟级） |
| §8.5 模块边界 | 已商定（见 audit §8.5 列表） | 本 worktree 在 strategy_package / selection_center / paper_trading_v2 / frontend/paper-v2 内，与 Codex QE 工作面隔离 |

---

## 7. P0-H 用户已授权决策（2026-05-08，R-Q9 入 Engine §17）

PoC 阶段 1 + 阶段 2 实证 + 数据源对照触发新决策。**用户 2026-05-08 全部按 A 选项授权**：

| # | 决策点 | 选定 | 落地位置 |
| --- | --- | --- | --- |
| Q9.1 | 引入 `BrokerBackend` 抽象（LocalSim / MiniQMTSim 两实现） | A 引入 | Engine §3.6.1 接口定义 |
| Q9.2 | 多策略包并行属性绑定 LocalSim | A 是（LocalSim 多包；MiniQMTSim 单例） | Engine §3.6.4 多包并行模型 |
| Q9.3 | 行情通道强绑定撮合端 | A 强绑定（LocalSim→TDX/DB；MiniQMTSim→MINIQMT_REALTIME；跨配 fail-fast） | Engine §3.6.3；触发 `MarketDataSource` 枚举增 `MINIQMT_REALTIME` |
| Q9.4 | StrategyPackage manifest 加 `broker_compatible` 字段 | 是（`LocalSim_only` / `MiniQMTSim_only` / `both`，默认 both） | Engine §3.6.5；不阻断 runtime 自由配置（与 §8.1 软合约决策正交） |

### 7.1 衍生 schema / 代码改动项（待 Codex 协调或本工作面实施）

| 改动 | 工作面归属 | 状态 |
| --- | --- | --- |
| `MarketDataSource` 枚举增 `MINIQMT_REALTIME` | 待 engine-design 判断（market_data.py 是否 Codex 边界）；否则进入 OPEN-EXT-3 | 待评估 |
| StrategyPackage manifest 增 `broker_compatible` 字段 | 由 §8.5 已商定 strategy_package/ 模块边界决定（按 audit §8.5 当前归属 Claude Code 工作面） | 等用户授权实施 |
| paper_trading_v2 portfolio 增 `broker_backend` 字段 + 创建/激活校验 | Claude Code 工作面（按 audit §8.5） | 等用户授权实施 |
| `BrokerBackend` 抽象层与 LocalSim/MiniQMTSim 两实现 | Claude Code 工作面 | 等 Codex Phase 4（Master Seed Contract）合入集成分支后才能实施 |

### 7.2 与已有决策的关系

- **与 §8.1（配置冻结边界）正交**：`broker_compatible` 是策略包**能力声明**（运行环境兼容性），不是运行时配置；不论 §8.1 选 A/B/C，broker_compatible 都必须冻结
- **与 §8.2（统一引擎含义）协同**：BrokerBackend 抽象本质上是 §8.2 选项 C "明确分工"的延伸——承认本地撮合与 miniQMT 撮合是两种不同后端，但都共享同一 Engine + 同一 OrderIntent schema
- **与 P0-A 协同**：解开 backtest_contract 6 项锁后，broker_compatible 字段可作为新的最小冻结集合的一部分
- **与 P0-B 协同**：BrokerBackend 把 Selection / Paper 与 QE 的执行栈差异显式化，让"统一引擎"= Engine + 多 Backend 的语义清晰

### 7.3 衔接 vn.py PoC 实证

- 方案 A（xtquant 直调）→ MiniQMTSim 实施推荐路径，36ms 下单已实证
- 方案 B（vnpy_xt + PYTHONPATH hack）→ MiniQMTSim 备选路径，import + Gateway 加载层已实证（v1.1 §2.5）
- LocalSim 实施 → 复用现有 `MinuteExecutionEngine` + TDX，零额外 PoC 成本

---

**结束**。本清单为后续修复优先级的输入；具体修复方案与时间节点等用户授权后再展开。
