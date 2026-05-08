# LocalSim ↔ MiniQMTSim 切换流程设计（含 broker_compatibility 失败 UI）

> **作者**：engine-design teammate
> **日期**：2026-05-09
> **任务**：Task #17 (2)
> **范围**：纸面流程设计；不写代码
> **依赖**：`strategy_engine_design_20260508.md` §3.6 / §10.1，`portfolio_ui_localsim_minqmtsim_design_20260509.md`
>
> **核心定位**：本文档**不**讨论"在同一个 portfolio 内热切 backend"——这违反 R-Q9 D2/D3 的物理不变量。本文档讨论的是用户**意图层**的切换："我想把策略 X 从 LocalSim 跑到 MiniQMTSim"——这等价于"新建一个 MiniQMTSim portfolio + 停用旧的 LocalSim portfolio"。文档定义这个意图的端到端 UI/后端流程 + 失败路径。

---

## 1. 设计原则

| # | 原则 | 来源 |
| --- | --- | --- |
| P1 | Backend 不可在同一 portfolio 内热切 | R-Q9 D3（行情通道与撮合端强绑定） |
| P2 | broker_compatibility 校验 fail-fast，禁止静默 fallback | R-Q9 D4 + feedback_no_silent_errors |
| P3 | 切换不丢失账本审计（旧 portfolio 保留 retired 状态，可回查） | 与 Paper v2 现有 portfolio lifecycle 对齐 |
| P4 | MiniQMTSim 单例释放必须显式确认（不允许 race） | R-Q9 D2 |
| P5 | 用户每一步都看到"正在做什么 + 下一步是什么"（无意外副作用） | UI 一致性 |

---

## 2. 三种切换意图（用户视角）

| 意图 | 物理实现 | 数据保留 |
| --- | --- | --- |
| **I1 LocalSim → MiniQMTSim**（同一策略包，提升到 miniQMT 仿真） | 新建 MiniQMTSim portfolio + 旧 LocalSim portfolio 转 retired | 旧账本只读保留；新 portfolio 从干净状态开始 |
| **I2 MiniQMTSim → LocalSim**（回退到本地撮合） | 新建 LocalSim portfolio + 旧 MiniQMTSim portfolio 转 retired + 释放 singleton 槽 | 同上 |
| **I3 MiniQMTSim 内换包**（同 backend，换策略包） | 新建 MiniQMTSim portfolio + 停用旧的 + singleton 切换序列化 | 同上；singleton 释放与重占需要原子化 |

**为什么不做 in-place 切换**：
- backend 决定行情通道（D3）→ 切 backend = 切行情；意味着 score 来源不一致
- backend 决定单例约束（D2）→ MiniQMTSim 槽位约束跨 portfolio
- broker_compatible 字段绑死包 → 同包跨 backend 必走 Mode G 等价性验证
- 账本物理割裂（LocalSim 进程内 ledger / MiniQMTSim 远端 xtquant 账户）→ 无法迁移

**因此切换总是"新建 + 停用"**。in-place 改 backend 在 UI 层就被禁止（详情页 Backend 字段只读）。

---

## 3. I1 LocalSim → MiniQMTSim 流程

### 3.1 用户起点

旧 portfolio 详情页右上角 **[切换到 MiniQMTSim]** 按钮（仅当 portfolio 当前 backend 为 LocalSim 时显示）。

### 3.2 检查阶段（client + server 双重）

点击按钮 → 弹出 modal：

```
┌──────────────────────────────────────────────────────────────┐
│ 切换到 MiniQMTSim — 检查项                                    │
├──────────────────────────────────────────────────────────────┤
│ 当前组合：portfolio_xxx（LocalSim）                          │
│ 策略包  ：pkg_qe_001  broker_compatible="both"               │
│                                                              │
│ ✓ 策略包兼容 MiniQMTSim                                      │
│ ✓ Mode G 用例 engine_modeg_localsim_vs_minqmtsim 已通过      │
│ ⚠ MiniQMTSim 当前已被 portfolio_yyy 占用（pkg_qe_002）        │
│   → 需先停用 portfolio_yyy                                   │
│                                                              │
│ [取消]                       [前往停用 portfolio_yyy]        │
└──────────────────────────────────────────────────────────────┘
```

**3 类检查**：

| # | 项 | 失败 UI |
| --- | --- | --- |
| C1 | 策略包 `broker_compatible ∈ {"MiniQMTSim_only", "both"}` | 失败 → 见 §6.1（broker_compatibility 失败专章） |
| C2 | Mode G 用例 `engine_modeg_localsim_vs_minqmtsim_orderintents` 在该包上通过 | 失败 → modal 显示"未验证等价性"+ 链接到 Mode G 报告 + 阻塞继续 |
| C3 | MiniQMTSim 单例槽空闲 | 占用 → modal 显示占用方 + "前往停用" 按钮（非阻塞，但需用户先去停） |

### 3.3 槽位释放（如 C3 占用）

用户点 **[前往停用 portfolio_yyy]** → 跳到 portfolio_yyy 详情页 → 用户停用 → 后端 adapter：

```
1. portfolio_yyy.lifecycle: ACTIVE → STOPPING
2. adapter 调 broker.cancel(all open orders)
3. adapter 等所有挂单 cancelled or rejected
4. broker.close() → MiniQMTSimBroker 实例销毁
5. 进程内 singleton flag 复位（MINIQMTSIM_SINGLETON_HELD = False）
6. portfolio_yyy.lifecycle: STOPPING → RETIRED
7. UI 推送 SSE: "MiniQMTSim slot released"
```

**关键约束**（与 R-Q9 D2 一致）：
- step 4-5 必须原子化（adapter 实现期用 lock + try/finally）
- 失败必须显式抛 `BrokerConnectivityError` / `MiniQMTSingletonViolation`，不允许"假装成功"
- 释放未完成时新建 MiniQMTSim portfolio 必须抛 `BrokerBindCapacityExceededError`（不能 silent retry）

### 3.4 新建阶段

旧 portfolio_yyy 释放后，回到原 portfolio_xxx 详情页 → **[切换到 MiniQMTSim]** 按钮重新可点 → 进入新建 portfolio wizard（参 portfolio UI 设计 §4）：

- Step 1 backend 已预填 MiniQMTSim（但仍允许用户改）
- Step 2 策略包 **预填 pkg_qe_001**（旧 portfolio 的同包）
- Step 3 行情自动锁 MINIQMT_REALTIME
- Step 4 资金 **可选"延续旧 portfolio NAV"或"重置初始资金"**
  - 选延续：新 portfolio 初始 cash + position 从旧 portfolio.snapshot 复制（但物理上是新账户：xtquant 仿真账户的实际持仓 + 现金为准；UI 显示"导入 LocalSim 历史持仓为参考，实际以 miniQMT 账户为准"）
  - 选重置：标准初始资金流程

### 3.5 停用旧 portfolio

新 portfolio 创建成功后弹出确认：

```
┌──────────────────────────────────────────────────────────────┐
│ 切换完成 — 旧 LocalSim 组合处置                              │
├──────────────────────────────────────────────────────────────┤
│ 新组合：portfolio_zzz (MiniQMTSim) ✓ 已创建并启动             │
│ 旧组合：portfolio_xxx (LocalSim)                             │
│                                                              │
│ 旧组合处置：                                                  │
│   ◉ 立即停用 + 转为 RETIRED（推荐）                          │
│   ○ 保留运行（双 backend 并行 — 仅 LocalSim 多包场景允许）   │
│   ○ 保留运行（Mode G 双跑等价性验证）                        │
│                                                              │
│ ⓘ 选保留运行将占用一个 LocalSim 槽位                         │
│ [取消切换]                            [应用]                  │
└──────────────────────────────────────────────────────────────┘
```

**双跑场景**：当用户选 "Mode G 双跑等价性验证" 时，前端把两个 portfolio 同时显示在 portfolios 列表，并自动加 cross-test 比对监控视图（详见 cross-test 设计 §2.4）。

---

## 4. I2 MiniQMTSim → LocalSim 流程

镜像 I1，但 C3 检查改为：
- "MiniQMTSim 当前 portfolio 是否安全停用"（含挂单 / 持仓的处置）

差异点：

```
1. 检查阶段：弹 modal 询问 LocalSim 资源（此场景下 LocalSim 永远有空，不会 reject）
2. 释放阶段：MiniQMTSim singleton 释放（同 §3.3 step 1-6）
3. 新建阶段：standard wizard，backend 预填 LocalSim
4. 行情可选 TDX_REALTIME 或 DB_HISTORICAL（不再锁定单值）
5. 处置旧 portfolio：同 §3.5
```

**特殊提示**（仅 I2）：

```
⚠ 您即将从 MiniQMTSim 切回 LocalSim
   - 不再有真实 miniQMT 撮合参与
   - 持仓 / 现金从 LocalSim ledger 重新跟踪（非来自 miniQMT 账户实时拉取）
   - 适用于：策略调试 / 多包并行研究 / 不需要接近实盘的场景
```

---

## 5. I3 MiniQMTSim 内换包流程

最简单的场景，但 singleton 切换序列化最严格。

### 5.1 用户起点

详情页 **[切换策略包]** 按钮（仅当 backend = MiniQMTSim 时显示）。

### 5.2 流程

```
1. 用户选新策略包 pkg_qe_010 → 检查 broker_compatible
2. 检查 Mode G 在新包上的覆盖（engine_modeg_smoke_lgb / 同 backend 内部用例）
3. 旧 portfolio 停用 → singleton 释放（§3.3 step 1-6）
4. 新 portfolio 创建 + singleton 占用
5. 整体过程显示进度条："停用旧组合 → 释放 miniQMT 槽 → 创建新组合 → 启动"
```

**3-4 步原子化**（adapter 端）：

```python
# 伪代码（实施期参考；本文档不实现）
with miniqmt_singleton_lock:
    deactivate_old_portfolio(old)
    wait_for_broker_close(old, timeout=30s)
    assert MINIQMTSIM_SINGLETON_HELD is False
    new_broker = MiniQMTSimBroker(account=...)  # 此处独占
    create_new_portfolio(new_pkg, new_broker)
    activate_new_portfolio(new)
```

锁超时 → `BrokerBindCapacityExceededError` + UI 显示重试按钮。

### 5.3 进度条 UI

```
┌──────────────────────────────────────────────────────────────┐
│ 切换 MiniQMTSim 策略包 — 进度                                 │
├──────────────────────────────────────────────────────────────┤
│ ✓ 1/5 检查兼容性                                             │
│ ✓ 2/5 停用旧组合                                             │
│ ⏳ 3/5 等待 miniQMT 槽位释放（已用 12s / 上限 30s）          │
│ ⏸ 4/5 创建新组合                                             │
│ ⏸ 5/5 启动                                                   │
│                                                              │
│ ⓘ 进程内仅 1 个 MiniQMTSim 槽位；切换需序列化                │
│ [中止切换]                                                    │
└──────────────────────────────────────────────────────────────┘
```

---

## 6. broker_compatibility 失败 UI（专章）

R-Q9 D4 引入的 `broker_compatible: Literal["LocalSim_only", "MiniQMTSim_only", "both"]` 在切换流程的多个点都可能 fail-fast。**不允许把这些失败折叠成单一 toast 或自动 fallback**。

### 6.1 主失败 UI（页面级）

```
┌──────────────────────────────────────────────────────────────┐
│ ⚠ 不能切换 — BrokerCompatibilityMismatchError                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   策略包 pkg_legacy_004                                      │
│     broker_compatible = "LocalSim_only"                      │
│     来源：StrategyPackage v2 manifest                        │
│                                                              │
│   目标 backend = MiniQMTSim                                  │
│                                                              │
│   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                              │
│   该包未在 MiniQMTSim 下验证等价性。强制切换会导致：          │
│   • Mode G byte-equal 不成立（结果不可信）                    │
│   • 决策侧 OrderIntent 可能与 LocalSim 不一致                 │
│   • 撮合后 NAV 漂移路径不可控                                 │
│                                                              │
│   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                              │
│   推荐操作：                                                  │
│                                                              │
│   ① 选择另一个 broker_compatible ∈ {"MiniQMTSim_only","both"} │
│      的策略包                                                 │
│                                                              │
│   ② 在 LocalSim 上保留旧 portfolio 不变                      │
│                                                              │
│   ③ （高级）请求把该包升级到 broker_compatible="both"         │
│      → 需通过 Mode G 用例：                                  │
│        engine_modeg_localsim_vs_minqmtsim_orderintents       │
│      → 流程参考 strategy_engine_design §3.6.5                │
│                                                              │
│   [选其他包]   [保留 LocalSim]   [升级流程文档]              │
└──────────────────────────────────────────────────────────────┘
```

### 6.2 失败 UI 的 5 个细分场景

| 场景 | 错误类 | 视觉关键 |
| --- | --- | --- |
| S1 包 `LocalSim_only` 切到 MiniQMTSim | `BrokerCompatibilityMismatchError` | 页面级 §6.1 主 UI |
| S2 包 `MiniQMTSim_only` 切到 LocalSim | 同上 | 同上（文案对调） |
| S3 包 `both` 但 Mode G 用例未跑过 | `BrokerCompatibilityMismatchError`（context 含 mode_g_status="not_run"） | banner + "去跑 Mode G" 按钮（链接到 cross-test 工具） |
| S4 包 `both` Mode G 用例失败 | 同 S3（context 含 mode_g_status="failed", report_uri=...） | banner + "查看失败报告" 按钮 |
| S5 LEGACY 包未声明 `broker_compatible` | `BrokerCompatibilityMismatchError`（context 含 reason="legacy_default_localsim_only"） | banner + 提示 LEGACY 默认值 + 链接迁移流程 |

### 6.3 4 个 typed error 的中文用户向 UI 映射（cross-test 取材点）

**适用范围**：所有切换流程 / 创建 wizard / portfolio 启动路径中可能抛出的 R-Q9 typed error 必须按本表映射为用户可读中文 UI。**禁止**让用户看到原始英文异常类名 / Python traceback。

| 错误类（英文，开发态） | 中文标题 | 中文内文（用户向） | 推荐操作（按钮顺序） | 视觉级别 |
| --- | --- | --- | --- | --- |
| `BrokerCompatibilityMismatchError` | 策略包与撮合后端不兼容 | 策略包【{package_id}】声明仅在 {broker_compatible_value} 下经过验证，不能用于当前选择的 {target_backend_id} 撮合后端。强制切换会破坏 OrderIntent 等价性，运行结果不可信。 | ① 选择其他兼容的策略包 ② 改用兼容的撮合后端 ③ 查看升级该策略包的流程 | 页面级（占据主内容区） |
| `BrokerBindCapacityExceededError` | MiniQMTSim 槽位已被占用 | 当前进程内的 MiniQMTSim 撮合后端只能绑定一个组合，已被【{occupying_portfolio_name}】占用。请先停用占用方再创建新组合。 | ① 前往停用 {occupying_portfolio_name} ② 改用 LocalSim 后端 ③ 取消 | banner（顶部红色横条）+ 按钮内联 |
| `MiniQMTSingletonViolation` | 系统错误：MiniQMTSim 实例冲突 | 后端进程内已存在一个 MiniQMTSim 实例，但代码尝试构造第二个 — 这是软件 bug，不是用户操作问题。请截图本提示和日志 ID【{error_id}】联系运维或开发人员。 | ① 复制错误信息 ② 打开日志位置 ③ 取消（不要重试） | 系统错误模态（不允许 dismiss-and-retry） |
| `BrokerMarketSourceMismatchError` | 行情通道与撮合后端不匹配 | 当前选择的撮合后端【{backend_id}】只允许使用以下行情通道：{allowed_set}；但本次配置的是【{given_source}】。撮合源与行情源必须同源，否则会导致价格幻觉。 | ① 自动切换为允许的行情通道 ② 改用其他撮合后端 ③ 取消 | 行情步骤页内嵌（局部 banner） |

**字段从 typed error.context 取值**：

| context 字段 | 来源 | 用于 |
| --- | --- | --- |
| `package_id` | StrategySpec | 中文内文展示策略包 ID |
| `broker_compatible_value` | StrategySpec.broker_compatible | 解释为何不兼容 |
| `target_backend_id` | portfolio.broker_backend_id | 解释当前选择 |
| `occupying_portfolio_name` | runtime singleton state | 给出"前往停用"目标 |
| `error_id` | logger session id | 让用户能复现到日志 |
| `allowed_set` | `ALLOWED_MARKET_SOURCES[backend_id]` | 列举允许集 |
| `given_source` | 用户提交的 MinuteDataSource | 解释当前不允许 |

**前端实现规范**（cross-test 模板 v0.4 §2.4 取材）：

```typescript
// 伪代码（不实施）
const ERROR_UI_MAP: Record<BrokerErrorClass, ErrorUIRenderer> = {
  BrokerCompatibilityMismatchError: pageLevelRenderer({
    titleKey: "broker_error.compatibility_mismatch.title",
    bodyKey: "broker_error.compatibility_mismatch.body",
    actionsKey: "broker_error.compatibility_mismatch.actions",
    docsLink: "strategy_engine_design#§3.6.5",
    forbidOverride: true,            // 不允许"强行继续"按钮
  }),
  BrokerBindCapacityExceededError: bannerRenderer({...}),
  MiniQMTSingletonViolation: systemModalRenderer({
    forbidRetry: true,
    showLogLink: true,
  }),
  BrokerMarketSourceMismatchError: inlineBannerRenderer({
    autoFixOption: true,             // "自动切换为允许的行情通道"
  }),
};
```

**i18n key 规范**（统一前缀 `broker_error.*`）：

```yaml
broker_error.compatibility_mismatch.title: "策略包与撮合后端不兼容"
broker_error.compatibility_mismatch.body: |
  策略包【{package_id}】声明仅在 {broker_compatible_value} 下经过验证，
  不能用于当前选择的 {target_backend_id} 撮合后端。
  强制切换会破坏 OrderIntent 等价性，运行结果不可信。
broker_error.compatibility_mismatch.actions:
  - { label: "选择其他兼容的策略包", action: "back_to_package_selection" }
  - { label: "改用兼容的撮合后端", action: "back_to_backend_selection" }
  - { label: "查看升级流程", action: "open_docs", target: "engine_design_3_6_5" }

broker_error.bind_capacity_exceeded.title: "MiniQMTSim 槽位已被占用"
# ... (同上模式，4 个错误类各 4 行)
```

**禁止做法**（与 §6.3 不变量配套强制）：

- ❌ 把 4 个错误折叠成一个通用 toast（"操作失败"）
- ❌ 翻译时丢弃 context 字段（如不提及具体 package_id）
- ❌ 任意错误页加"强行继续"按钮（违反 fail-fast）
- ❌ 用 alert() / window.confirm() 等浏览器原生 dialog（无法承载 actions[]）

**给 cross-test 的取材索引**：

| Cross-test 模板 v0.4 §2.4 用途 | 取材 |
| --- | --- |
| typed error 中文 UI 渲染 fixture | 本节 4 行映射表 |
| context 字段 fixture | 本节"字段从 typed error.context 取值"子表 |
| forbidOverride / forbidRetry / autoFixOption fixture flag | 本节伪代码 ERROR_UI_MAP |
| i18n key 命名规范 fixture | 本节 i18n 子段 |

### 6.4 失败 UI 的不变量

| 不变量 | 落地 |
| --- | --- |
| 不允许 "Override and proceed" 按钮 | 任何强制绕过的入口都禁止；必须从根因解决（换包 / 升级包 / 不切换） |
| 错误必带 context（含包/包字段值/目标 backend/原因码） | 后端 `StrategyEngineError.context` 字段 → 前端反序列化展示 |
| 必带文档链接（§3.6.5） | 错误页底部固定有"详细规则"链接 |
| 必带 actionable 选项 ≥ 2 个 | 不能只显示"取消"；必须给出至少 2 条解决路径 |
| Toast / banner 不可替代页面级 UI | broker_compatibility 失败必须用页面级；toast 只用于网络错误等瞬时问题 |

---

## 7. 单例释放故障的 UI（边角）

`MiniQMTSingletonViolation` 通常是 adapter 内部 bug（不应触发到用户）。但若发生：

```
┌──────────────────────────────────────────────────────────────┐
│ ⚠ 系统错误 — MiniQMTSingletonViolation                       │
├──────────────────────────────────────────────────────────────┤
│ adapter 进程内已存在 MiniQMTSimBroker 实例                    │
│ 但当前请求尝试构造第二个 — 这是软件 bug                       │
│                                                              │
│ 临时处置：                                                    │
│   • 不要重试（重试不能解决）                                   │
│   • 截图本对话框 + 日志 ID: {error_id}                        │
│   • 联系运维或开发                                             │
│                                                              │
│ 系统日志位置: backend/logs/paper_v2_{date}.log              │
│ Issue 模板：strategy_engine_modeg_minqmt_singleton            │
│                                                              │
│ [复制错误信息]   [打开日志]                                    │
└──────────────────────────────────────────────────────────────┘
```

---

## 8. 后端流程（伪步骤，与 UI 对齐）

I1 (LocalSim → MiniQMTSim) 后端流程：

```
HTTP POST /paper-v2/api/portfolios/switch
  body: {
    source_portfolio_id: "p_xxx",
    target_backend: "minqmt_sim",
    target_market_source: "MINIQMT_REALTIME",  # 必填，与 backend 强绑定校验
    new_strategy_package_id: "pkg_qe_001",     # 可与旧不同（I3）
    initial_capital: { mode: "carry_over" | "reset", value: ... },
    old_portfolio_disposition: "retire" | "keep_active" | "keep_modeg",
  }

Backend 步骤：
  1. validate inputs (Pydantic)
  2. fetch source_portfolio + StrategyPackage
  3. assert StrategyPackage.broker_compatible compatible with target_backend
     (或 raise BrokerCompatibilityMismatchError)
  4. assert target_market_source in ALLOWED_MARKET_SOURCES[target_backend]
     (或 raise BrokerMarketSourceMismatchError)
  5. if target_backend == "minqmt_sim":
        acquire_miniqmt_singleton_lock(timeout=30s)
        if locked by other portfolio:
            raise BrokerBindCapacityExceededError
        else:
            wait until released
  6. create_new_portfolio(target_backend, target_market_source, pkg, capital)
  7. start_new_portfolio(new_id)
  8. apply old_portfolio_disposition
  9. emit SSE: { type: "switch_complete", new_id, old_id, disposition }
```

错误传播：所有 raise 必须含 context，UI 直接展示 context 字段。**禁止 except: pass / 默认 fallback**。

---

## 9. 与 task #17 其他设计的衔接

| 关联文档 | 衔接点 |
| --- | --- |
| `portfolio_ui_localsim_minqmtsim_design_20260509.md` | 列表/创建 wizard / 详情页头部 — 本文档复用其视觉规范 |
| `vnpy_connect_dryrun_design_20260509.md`（task #17 (3)） | 切换流程的 connect 检查点（步 5 槽位 acquire 时如何判断 miniQMT 服务可达） |
| `paper_v2_dual_brokerbackend_mvp_pr_plan_20260509.md`（task #17 (4)） | 切换流程实施依赖该 PR 计划中的 PR 序号 |
| `cross-test 模板 v0.4`（task #21） | 切换前的 Mode G 检查（§3.2 C2）通过 cross-test 触发 |

---

## 10. 不在本文档范围

- in-place backend 切换（违反 R-Q9，物理禁止）
- 跨进程的 MiniQMTSim portfolio 迁移（不支持；每进程独立 singleton）
- 实盘 (`minqmt_live`) 切换（待主体 §11 准入流程）
- portfolio 内的 strategy_package version 升级（与 backend 切换正交，独立流程）

---

**End of Switch Flow design**.
