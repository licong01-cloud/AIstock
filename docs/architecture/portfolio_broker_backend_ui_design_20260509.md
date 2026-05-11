# Portfolio UI 设计：LocalSim 多包 vs MiniQMTSim 单包

> **作者**：engine-design teammate
> **日期**：2026-05-09
> **任务**：Task #17 (1)
> **范围**：纸面 UI 设计；不写实际 React/Tailwind 代码
> **依赖**：`strategy_engine_design_20260508.md` §3.6（BrokerBackend / R-Q9）、`paper_v2_blockers_20260508.md` P0-H、`vnpy_poc_result_20260508.md`
>
> **核心约束**：UI 必须把 R-Q9 的两条硬不变量物理化呈现：
> 1. **D2 容量差异**：LocalSim 一个进程绑定 N 个 portfolio（账本互相隔离）；MiniQMTSim 进程内单例 — 同时只能有 1 个 portfolio
> 2. **D3 行情绑定**：撮合端与行情通道一一对应，无法解耦切换
>
> UI 不是单纯的"换枚举值"，必须把不变量翻译成用户可理解的视觉语言（容量徽章 / 互斥提示 / 行情绑定锁）。

---

## 1. 设计目标

| # | 目标 | 验收 |
| --- | --- | --- |
| G1 | 用户在创建 portfolio 时一眼看出"我在创建哪种 SimMode 的 portfolio" | backend 切换时 UI 视觉差异 ≥ 3 处（badge / 颜色 / 容量提示） |
| G2 | 多包并行（LocalSim）与单包独占（MiniQMTSim）的限制写在用户面前，不是错误后才弹 | LocalSim 已绑 1 个时仍可继续；MiniQMTSim 已绑 1 个时新建按钮直接 disabled + 解释 |
| G3 | 行情通道选择被 backend 决定，不是用户独立选 | backend 切换 → 行情下拉自动联动 + 显式锁定标记 |
| G4 | broker_compatible 不兼容的策略包在选择阶段就被过滤，不是提交后报错 | StrategyPackage selector 内显示 broker_compatible 徽章 + 不兼容包灰显 |
| G5 | 错误来源可追溯到 `strategy_engine_design_20260508.md` §10.1 错误类 | 所有 fail-fast 路径附错误类名 + 文档链接 |

---

## 2. 现状参考

`frontend/src/app/paper-v2/portfolios/[portfolioId]/page.tsx` + `portfolios/page.tsx` 当前结构：
- portfolio 列表 + 详情页
- 不区分 broker_backend
- TDX_REALTIME / DB_HISTORICAL 选择是独立 control（与 backend 解耦）

R-Q9 落地后必须改造为 **broker_backend-first** 的页面层级。

---

## 3. 整体架构：portfolio 列表 + 创建流 + 详情

### 3.1 列表页（`/paper-v2/portfolios`）布局

```
┌──────────────────────────────────────────────────────────────────────┐
│ 模拟组合                                                  [+ 新建组合] │
├──────────────────────────────────────────────────────────────────────┤
│ 视图筛选: ● 全部  ○ LocalSim 组合  ○ MiniQMTSim 组合                   │
├──────────────────────────────────────────────────────────────────────┤
│ ┌─ Section: LocalSim 组合（多包并行，当前 N 个）─────────────────┐  │
│ │  PortfolioCard │ PortfolioCard │ PortfolioCard │ ...           │  │
│ │  本地撮合       │ 本地撮合       │ 本地撮合                       │  │
│ │  [TDX 实时]    │ [DB 历史回放]  │ [TDX 实时]                     │  │
│ │  pkg_xxx_a     │ pkg_xxx_b     │ pkg_xxx_c                       │  │
│ └─────────────────────────────────────────────────────────────────┘  │
│                                                                       │
│ ┌─ Section: MiniQMTSim 组合（单例，当前 1/1）─────────────────────┐  │
│ │  PortfolioCard                                                    │  │
│ │  miniQMT 仿真                                                     │  │
│ │  [MINIQMT_REALTIME 锁定]                                          │  │
│ │  pkg_xxx_d  ●运行中                                               │  │
│ │                                                                   │  │
│ │  ⓘ 单进程内仅允许 1 个 MiniQMTSim 组合（R-Q9 D2）                │  │
│ │  ⓘ 如需切换策略包，先停用当前组合（详见切换流程文档）              │  │
│ └─────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

**关键视觉差异**：
- **分 Section**：两类后端必须分组，不混在一个列表里（防止用户误以为可以"互换"）
- **Section 头部容量计数**：
  - LocalSim：`(多包并行，当前 N 个)` — 数字表达式
  - MiniQMTSim：`(单例，当前 1/1)` — 分母固定 1
- **新建按钮位置**：右上角统一入口，但创建流第一步选 backend → MiniQMTSim 已满则该选项 disabled

### 3.2 PortfolioCard 视觉规范

```
┌────────────────────────────────────────────────┐
│ ●运行中 / ○已停用                              │
│ {portfolio_name}                                │
│ ─────────────────────────────────────────────  │
│ Backend │ ⌂ LocalSim   |   ⛁ MiniQMTSim         │
│ 行情    │ TDX_REALTIME |   MINIQMT_REALTIME (锁) │
│ 策略包  │ pkg_xxx [LocalSim_only/both]           │
│ 持仓    │ N 个 / NAV ¥X                          │
│ ─────────────────────────────────────────────  │
│ [详情] [停用]                                    │
└────────────────────────────────────────────────┘
```

**视觉编码**：
- LocalSim 卡片：左边 cyan border + ⌂ 图标（"家"=本地）
- MiniQMTSim 卡片：左边 amber border + ⛁ 图标（数据库/外部）+ "锁"图标在行情字段右侧
- broker_compatible 徽章：紧贴策略包名右侧（`[LocalSim_only]` 紫底 / `[MiniQMTSim_only]` 橙底 / `[both]` 灰底）

---

## 4. 新建 portfolio 流程（关键页面）

### 4.1 Step 1 — 选 Backend（必须第一步）

```
┌─────────────────────────────────────────────────────────────┐
│ 新建模拟组合 — 第 1/4 步：选择撮合后端                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────────┐    ┌─────────────────────┐         │
│  │ ⌂ LocalSim          │    │ ⛁ MiniQMTSim       │         │
│  │ 本地撮合             │    │ miniQMT 仿真        │         │
│  │                      │    │                      │         │
│  │ ✓ 多策略包并行       │    │ ✗ 同进程仅 1 个      │         │
│  │ ✓ 历史回放支持       │    │ ✓ 接近实盘撮合       │         │
│  │ ✓ 启动成本低         │    │ ✗ 不支持历史回放     │         │
│  │                      │    │                      │         │
│  │ 行情：TDX 实时 / DB │    │ 行情：MINIQMT_REALTIME│         │
│  │       历史回放       │    │       (强绑定)       │         │
│  │                      │    │                      │         │
│  │ 当前已运行：3 个      │    │ 当前已运行：1/1 ⚠   │         │
│  │ [选择 LocalSim]      │    │ [已满，需先停用]     │         │
│  └─────────────────────┘    └─────────────────────┘         │
│                                                              │
│  ⓘ 撮合后端选定后不可更改；如需切换需新建 portfolio          │
│  ⓘ 详细对比 → strategy_engine_design §3.6.2                  │
└─────────────────────────────────────────────────────────────┘
```

**互斥逻辑**（前端态 + 后端校验双保险）：

| 状态 | LocalSim 卡 | MiniQMTSim 卡 |
| --- | --- | --- |
| MiniQMTSim 当前 0 个 | enabled | enabled |
| MiniQMTSim 当前 1 个（运行中） | enabled | **disabled**，按钮显示"已满，需先停用"，hover 提示链接到当前 MiniQMTSim portfolio |
| MiniQMTSim 当前 1 个（已停用但未释放进程） | enabled | **disabled**，按钮显示"释放中..."（adapter 端等 singleton release） |

### 4.2 Step 2 — 选策略包（broker_compatible 过滤）

```
┌─────────────────────────────────────────────────────────────┐
│ 新建模拟组合 — 第 2/4 步：选择策略包                          │
│ (Backend: ⛁ MiniQMTSim)                                      │
├─────────────────────────────────────────────────────────────┤
│  筛选: 仅显示与 MiniQMTSim 兼容的策略包 ✓                    │
│                                                              │
│  ✓ pkg_qe_001  [both]                                        │
│     LGB 单 alpha · 已 ORIGINAL_RETEST_PASSED                 │
│                                                              │
│  ✓ pkg_qe_002  [MiniQMTSim_only]                             │
│     CatBoost 单 alpha · 已 PAPER_CANDIDATE                   │
│     ⓘ 此策略包仅在 MiniQMTSim 下验证                          │
│                                                              │
│  ⊘ pkg_legacy_004  [LocalSim_only]  (灰显，不可选)           │
│     LEGACY_NON_ST_PIT · 仅 LocalSim 兼容                     │
│     [说明] 该包未在 MiniQMTSim 下验证 broker_compatibility    │
│                                                              │
│  [上一步]                                          [下一步]   │
└─────────────────────────────────────────────────────────────┘
```

**关键规则**：
- broker_compatible 徽章紧贴包名右侧
- 不兼容包**仍显示**（不是隐藏），但灰显 + 禁用 + 给出原因（避免用户疑惑"我的包去哪儿了？"）
- 鼠标悬停灰显项给说明 popup：
  ```
  此策略包 broker_compatible = "LocalSim_only"
  当前 backend = MiniQMTSim
  → 不兼容
  如需在 MiniQMTSim 下使用，需先在 Mode G 下验证
  此 backend 的 OrderIntent 等价性，并升级到 "both"。
  详见 strategy_engine_design §3.6.5
  ```

### 4.3 Step 3 — 行情通道（被 backend 决定，不可独立选）

LocalSim 分支：

```
┌─────────────────────────────────────────────────────────────┐
│ 新建模拟组合 — 第 3/4 步：行情通道                            │
│ (Backend: ⌂ LocalSim)                                        │
├─────────────────────────────────────────────────────────────┤
│  ◉ TDX_REALTIME（推荐）                                      │
│     实时盘中跟单；适合盘中模拟                                │
│                                                              │
│  ○ DB_HISTORICAL                                             │
│     历史回放；适合 CATCHUP_THEN_LIVE 启动                     │
│                                                              │
│  ⓘ LocalSim 仅支持 TDX_REALTIME 或 DB_HISTORICAL              │
│     （详见 strategy_engine_design §3.6.4 ALLOWED_MARKET_SOURCES│
└─────────────────────────────────────────────────────────────┘
```

MiniQMTSim 分支：

```
┌─────────────────────────────────────────────────────────────┐
│ 新建模拟组合 — 第 3/4 步：行情通道                            │
│ (Backend: ⛁ MiniQMTSim)                                      │
├─────────────────────────────────────────────────────────────┤
│  🔒 MINIQMT_REALTIME（锁定）                                 │
│     由 miniQMT 行情通道（xtquant xtdata）提供                 │
│                                                              │
│  ⓘ MiniQMTSim 强绑定 MINIQMT_REALTIME，不可切换为其他通道     │
│     原因：撮合源与行情源必须同源（详见 §3.6.4 理由 1-3）       │
│                                                              │
│  [上一步]                                          [下一步]   │
└─────────────────────────────────────────────────────────────┘
```

**关键设计**：MiniQMTSim 分支**不显示其他选项**（不是 disabled 单选，而是只显示一个锁定项）— 物理上消除"用户以为可换"的认知。

### 4.4 Step 4 — 资金 / 命名 / 确认

通用 step；不展开。但确认页必须再次回显：

```
┌─────────────────────────────────────────────────────────────┐
│ 新建模拟组合 — 确认                                          │
├─────────────────────────────────────────────────────────────┤
│ 名称        │ {user_name}                                    │
│ Backend     │ ⛁ MiniQMTSim （单例，与 1 个进程绑定）         │
│ 策略包      │ pkg_qe_001 [both]                              │
│ 行情通道    │ MINIQMT_REALTIME 🔒                            │
│ 初始资金    │ ¥100,000                                       │
│                                                              │
│ ⚠ 创建后 Backend 不可变更；切换需新建 portfolio              │
│ ⚠ 创建时将占用进程内 MiniQMTSim 唯一槽位                     │
│                                                              │
│ [取消]  [上一步]                              [创建并启动]   │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. 详情页（`/paper-v2/portfolios/[id]`）增量

在现有详情页头部加 **Backend 信息卡**：

```
┌──────────────────────────────────────────────────────────────┐
│ pkg_qe_001 / portfolio_xxx                                    │
│ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│ Backend 信息                                                  │
│   后端类型 ⌂ LocalSim                                         │
│   行情通道 TDX_REALTIME                                       │
│   占用槽位 LocalSim 实例 #2 / 共 N 个                          │
│   broker_compatible 包字段值 = "both" ✓ 与当前 backend 兼容   │
│                                                              │
│   [查看 BrokerBackend 状态]  [启停撮合后端]                   │
└──────────────────────────────────────────────────────────────┘
```

---

## 6. 错误 UI（关键 fail-fast 落地）

R-Q9 引入的 4 个错误类（`strategy_engine_design §10.1`），UI 必须分别有对应的视觉处理。**禁止把错误折叠成单一 toast**。

| 错误类 | 触发场景 | UI 呈现 |
| --- | --- | --- |
| `BrokerCompatibilityMismatchError` | 创建 step 2 选了 LocalSim_only 包 + MiniQMTSim backend，绕过前端校验直 POST | 全屏错误页 + 显示包 broker_compatible 值 + backend_id + 链接到 §3.6.5 |
| `BrokerBindCapacityExceededError` | 创建 step 1 时 MiniQMTSim 槽已满但用户用旧链接绕过 | 红色 banner + 当前占用 portfolio 名 + "去停用占用" 按钮 |
| `MiniQMTSingletonViolation` | 后端进程态：第二个 MiniQMTSimBroker 实例尝试构造 | 详情页 banner + 提示"adapter 进程内已有 MiniQMTSim 实例 — 联系运维" + log link |
| `BrokerMarketSourceMismatchError` | 创建/启动时配置了不允许的 MinuteDataSource | 行情步骤回退 + 显示 ALLOWED_MARKET_SOURCES 当前 backend 的允许集 |

**错误 banner 通用模板**：

```
┌──────────────────────────────────────────────────────────────┐
│ ⚠ 创建失败 — BrokerCompatibilityMismatchError                │
│                                                              │
│ 策略包 pkg_legacy_004 broker_compatible = "LocalSim_only"   │
│ 当前选择的 backend = MiniQMTSim                              │
│                                                              │
│ 这两个组合不兼容（参 strategy_engine_design §3.6.5）          │
│                                                              │
│ 解决方法：                                                    │
│   ① 改选 LocalSim backend（推荐）                            │
│   ② 选另一个 broker_compatible ∈ {"MiniQMTSim_only","both"} 包│
│                                                              │
│ [回到 backend 选择]    [回到包选择]                          │
└──────────────────────────────────────────────────────────────┘
```

---

## 7. 多包并行的视觉细节（LocalSim 专属）

LocalSim section 的 Header 加进度条：

```
┌──────────────────────────────────────────────────────────────┐
│ LocalSim 组合（多包并行，当前 3 / 上限 N）                    │
│ ▰▰▰░░░░░░░  3/10  ⓘ 上限由系统资源决定                       │
└──────────────────────────────────────────────────────────────┘
```

**资源使用提示**（hover 显示）：
- 每个 LocalSim portfolio ≈ X MB 内存 + Y CPU
- 当前已用 / 系统总量
- 接近上限时（≥80%）颜色转黄

---

## 8. 与 Mode G 用例的对应（可观测性）

设计文档 `strategy_engine_design §3.6.6` 列出 4 条 Mode G 新增用例。UI 应在"运行监控"页加只读视图：

```
┌──────────────────────────────────────────────────────────────┐
│ 当前 portfolio 的 Mode G 验证状态                            │
├──────────────────────────────────────────────────────────────┤
│ engine_modeg_localsim_vs_minqmtsim_orderintents      ✓ Pass  │
│   该包在 LocalSim/MiniQMTSim 下 OrderIntent byte-equal       │
│                                                              │
│ engine_modeg_multi_package_localsim_isolation        ✓ Pass  │
│ engine_modeg_minqmt_capacity_reject                  ✓ Pass  │
│ engine_modeg_broker_compat_reject                    ✓ Pass  │
└──────────────────────────────────────────────────────────────┘
```

未跑过 Mode G 的包：在创建 step 2 的徽章上加 ⚠ 角标，提示"未验证 cross-backend 等价性"。

---

## 9. 实现拆分（不在本设计实施，仅指引）

按本文档 UI 结构，未来 PR 拆分建议（与 task #17 (4) Paper v2 双 BrokerBackend MVP PR 计划对齐）：

| PR | 范围 |
| --- | --- |
| PR-UI-1 | portfolios 列表分 section + Backend badge |
| PR-UI-2 | 创建 wizard step 1（backend 选择 + 互斥逻辑） |
| PR-UI-3 | 创建 wizard step 2（broker_compatible 过滤 + 灰显） |
| PR-UI-4 | 创建 wizard step 3-4（行情锁定 + 确认页） |
| PR-UI-5 | 详情页 Backend 信息卡 + Mode G 状态视图 |
| PR-UI-6 | 错误 banner 4 类（fail-fast UI） |

**依赖**：
- 后端 R-Q9 落地（task #11 设计 + 实施期开发）
- `broker_compatible` manifest 字段落地（OPEN-EXT-3 等用户授权）
- `MinuteDataSource.MINIQMT_REALTIME` 枚举落地（Claude 工作面，可独立推进）

---

## 10. 不在本设计范围

- 单个 portfolio 的内部 tab（订单 / 成交 / 持仓 / 账本 / 历史回放）— 沿用现有设计
- HMM / Risk Policy 等运行时配置 — 不受 R-Q9 影响
- 选股中心（`/selection`）— 不直接绑 backend
- 实盘 backend `minqmt_live` 的 UI（待主体 §11 准入流程定义）

---

**End of Portfolio UI design**.
