# Paper v2 双 BrokerBackend MVP — PR 拆分计划（3-5 PR 版）

> **作者**：engine-design teammate
> **日期**：2026-05-09
> **任务**：Task #17 (4) 修订版（按 Lead 2026-05-09 派单具体规格重写）
> **范围**：纸面 PR 拆分计划；不写代码、不开 PR
> **依赖**：
> - `strategy_engine_design_20260508.md` §3.6（R-Q9 / BrokerBackend / broker_compatibility / MinuteDataSource）
> - `portfolio_broker_backend_ui_design_20260509.md`
> - `broker_backend_switch_flow_20260509.md`
> - `vnpy_connect_dry_run_design_20260509.md`
> - `vnpy_poc_result_20260508.md`（PoC 阶段 1 PASS）
> - `paper_v2_blockers_20260508.md` P0-H

---

## 0. 计划定位与本轮口径

**MVP 目标**（不变）：让 Paper v2 同时支持两种 BrokerBackend（LocalSim + MiniQMTSim），完整跑通"创建 portfolio → 启动 → 撮合 → 持仓更新 → 停用"，满足 R-Q9 D1/D2/D3/D4 全部不变量。

**本轮 Lead 派单口径**（与前版不同）：
- **PR 数量约束**：最少 3、最多 5（前版 13 PR 太碎）
- **必须自包含**：每 PR 可独立 review + 独立部署（不依赖另一 PR 同时 merge）
- **5 个实施任务必须分配完毕**：
  - A#3 `MinuteDataSource.MINIQMT_REALTIME` 枚举扩展（task #16，已 completed）
  - A#4 `portfolio.broker_backend` 字段（task #19）
  - A#1 LocalSim BrokerBackend Protocol 实施（task #20 / #23）
  - 后续 MiniQMTSim BrokerBackend 实施
  - Day Runner 路由（既有 day_runner.py 改造为调用 BrokerBackend Protocol）
- **每 PR 必须标注**：schema 改动 / 接口契约 / 单测覆盖 / 与 Codex Phase 4-6 衔接点

---

## 1. PR 拆分原则（Lead 本轮口径）

| # | 原则 | 落地 |
| --- | --- | --- |
| P1 | **每 PR 自包含**：可独立 review，独立合 main 不破坏其他 PR 的进度 | 每 PR 内部完成"接口 + 实现 + 测试" 3 件套 |
| P2 | **测试随 PR 走**：每 PR 必须含对应单元测试 + Mode G 用例（如改决策路径） | 与 strategy_engine_design §A.5.1 一致 |
| P3 | **schema 改动走 additive only** | 不修改既有字段；新字段 NULL or DEFAULT |
| P4 | **既有 LocalSim 行为零回归** | 每 PR 完成后既有 paper_trading_v2 测试全 PASS |
| P5 | **不依赖 Codex Phase 同时 merge**：Codex Phase 4-6 是衔接点不是阻断点 | broker_compatible 用 `custom_extension` 占位（R-Q2 audit-only），等 OPEN-EXT-3 落地再切一等公民 |
| P6 | **错误传播零妥协**：禁止 silent fallback / except: pass | feedback_no_silent_errors |

---

## 2. 推荐拆分：4 个 PR（最优）

总规模 ~3 周；多人并行 ~1.5 周。

```
PR-1  Foundation        ┐
                        │ 自包含；独立合 main
PR-2  LocalSim Migrate  ┘

PR-3  MiniQMTSim        ─ 自包含；依赖 PR-1 接口；不依赖 PR-2 撮合栈
PR-4  Frontend MVP      ─ 自包含；mock fetcher → 后端就绪后切真实 API
```

也可压缩到 3 个（合并 PR-1 + PR-2）或扩到 5 个（拆 PR-3）— 见 §4。

---

## 3. 各 PR 详细规格

### PR-1 — Foundation：BrokerBackend Protocol + MinuteDataSource 扩展 + portfolio.broker_backend 字段

**承接任务**：A#3（task #16，已 completed） + A#4（task #19）

**归属**：Claude 工作面

**Schema 改动（DB additive only）**：
- `paper_v2_portfolio` 表加 `broker_backend_id TEXT NOT NULL DEFAULT 'local_sim'`
- 不修改任何既有字段

**接口契约**：
- 定义 `BrokerBackend` Protocol（按 strategy_engine_design §3.6.1 — `submit_order_intent` / `cancel` / `query_status` / `subscribe_fill_callback` / `query_account` / `query_positions` / `market_data_channel` / `bind_capacity`）
- 定义共享数据类型（Pydantic）：`OrderHandle / OrderStatus / FillEvent / AccountSnapshot / CancelAck / BrokerBindCapacity / SubscriptionHandle`
- 定义 R-Q9 错误类（按 §10.1）：`BrokerCompatibilityMismatchError / BrokerBindCapacityExceededError / MiniQMTSingletonViolation / BrokerMarketSourceMismatchError / BrokerSubmitError / BrokerRejectedError / BrokerConnectivityError`
- 定义 `MinuteDataSource.MINIQMT_REALTIME` 枚举值（A#3 已落，本 PR 引用）
- 定义 `ALLOWED_MARKET_SOURCES` 字典 + `assert_broker_market_source_match()` 函数

**单测覆盖**：
- Pydantic 模型 shape + 序列化测试
- 错误类继承链测试（每个都是 `StrategyEngineError` 子类）
- DB migration test：旧记录读取后 `broker_backend_id="local_sim"`
- `assert_broker_market_source_match` 表驱动测试（local_sim+TDX_REALTIME PASS / local_sim+MINIQMT_REALTIME FAIL / etc）
- 既有 `paper_trading_v2/test_*.py` 全部 PASS（零回归）

**与 Codex Phase 4-6 衔接**：
- Phase 4 (Master Seed Contract)：本 PR **不依赖** Phase 4；BrokerBackend 不消费 master_seed
- Phase 5 (Model Library)：本 PR **不依赖**；BrokerBackend 不直连 model registry
- Phase 6 (Runtime Variants)：`broker_backend_id` 字段在 portfolio 维度，不在 manifest 维度，与 runtime variant schema 正交

**预估**：3-4 天（schema migration + Protocol 定义 + 错误类 + 单测）

**风险**：低 — 纯接口定义 + DB additive

---

### PR-2 — LocalSim Migrate：现有 day_runner 撮合栈改造为 BrokerBackend Protocol

**承接任务**：A#1（task #20 / #23） + Day Runner 路由

**归属**：Claude 工作面

**Schema 改动**：无（PR-1 已落字段；本 PR 仅消费）

**接口契约**：
- 实现 `LocalSimBroker` 类，遵循 `BrokerBackend` Protocol
- 复用现有 `day_runner.py` 撮合栈 + ledger（重构为 `LocalSimBroker._submit_internal()` 等私有方法）
- `bind_capacity()` 返回 `max_concurrent_packages = N`（N 来自配置；默认 ≥ 2）
- `market_data_channel()` 返回 TDX_REALTIME 或 DB_HISTORICAL 对应通道
- 改造 `day_runner.py` 主路径：从直接调撮合改为调 `broker.submit_order_intent(intent)`
- `paper_trading_v2/service.py` portfolio 创建路径据 `broker_backend_id` 实例化 `LocalSimBroker`
- **完全行为兼容**：现有 portfolio 在 LocalSim 下行为不变

**单测覆盖**：
- 既有 `test_day_runner.py` / `test_session.py` / `test_runtime_profile.py` 全 PASS（核心兼容性）
- 新增 `test_local_sim_broker.py`：单元测试每个 Protocol 方法
- 多 portfolio 并发测试：2 个 portfolio 各持自己的 LocalSimBroker，互不干扰（验证 R-Q9 D2 LocalSim 多包）
- Mode G 用例 `engine_modeg_multi_package_localsim_isolation`（参 §3.6.6）— 至少跑过 1 次

**与 Codex Phase 4-6 衔接**：
- Phase 4：不依赖
- Phase 5：不依赖（LocalSim 不连 model registry，仅消费 StrategySpec.frozen_alpha_core 提供的 model_artifact_pointer）
- Phase 6：本 PR 在 portfolio 维度引入 broker_backend_id，**不**触碰 manifest schema；与 runtime variant 完全正交。Phase 6 落地后，runtime variant 可叠加 broker_backend 维度，但本 PR 不预设

**预估**：5-7 天（涉及 day_runner 重构 + 多 portfolio 并发测试）

**风险**：中 — 重构现有代码路径；回归风险通过 P4（既有测试零回归）兜底

---

### PR-3 — MiniQMTSim：xtquant 直调实现 + singleton + broker_compatibility 校验

**承接任务**：MiniQMTSim BrokerBackend 实施

**归属**：Claude 工作面

**前置依赖**：
- vnpy_connect_dry_run（task #17 (3) → step5 by env-poc）报告 PASS 或 FAIL → 决定本 PR 走方案 A 还是 A+B
- 默认走方案 A（xtquant 直调，对齐 PoC 阶段 1 PASS 事实）

**Schema 改动**：无

**接口契约**：
- 实现 `MiniQMTSimBroker` 类（基于 PoC 阶段 1 验证的 xtquant 直调代码模式）
- `bind_capacity()` 返回 `max_concurrent_packages = 1`
- 进程内 `MINIQMTSIM_SINGLETON_HELD` flag + `threading.Lock`
- `__init__` 检查 + 占用；`close()` 释放；第二个实例化抛 `MiniQMTSingletonViolation`
- `market_data_channel()` 返回 MINIQMT_REALTIME 通道
- StrategySpec `custom_extension.broker_compatible` reader（占位实现，对应 OPEN-EXT-3 等用户授权）
- portfolio 启动 / Engine init 校验 broker_compatible 与 broker_backend_id 兼容性，违反抛 `BrokerCompatibilityMismatchError`
- xtquant 错误码 → typed errors 映射（rc=-1 → `BrokerConnectivityError`，等）

**单测覆盖**：
- mock xtquant 的单元测试（不触真 SIM 账户）：submit / cancel / query / 错误码映射
- 集成测试（mark `@pytest.mark.integration_minqmt`）：连真 SIM 账户跑闭环

**集成测试 CI 策略**（Lead 2026-05-09 批准）：
- **CI 跳过**：标 `@pytest.mark.integration_minqmt` 的测试在 CI 上默认 skip。理由：CI 环境无法稳定提供 miniQMT 客户端 + SIM 撮合服务（state-ful，非可重复纯函数）；强求 CI 跑会让 PR check 长期红，反而稀释信号
- **本地必跑**：开发者在 PoC 环境（`.venv-vnpy-poc/` + miniQMT SIM 服务在跑）执行 `pytest -m integration_minqmt` 必须 PASS
- **PR description 强制贴运行截图**：包含通过的 marker、用例名、PASS 计数、运行机器（标 PoC env）。截图缺失 → reviewer 拒绝 PR
- **CI 配置**：默认 `pytest -m "not integration_minqmt"`；不依赖此 marker 的其他测试不受影响
- 单例测试：连续构造两个 MiniQMTSimBroker → 第二个抛 `MiniQMTSingletonViolation`
- 释放协议测试：close() 后 flag 复位，新构造 OK
- broker_compatibility 5 类失败场景（参 broker_backend_switch_flow §6.2 S1-S5）
- Mode G 用例 4 条全部 PASS：
  - `engine_modeg_localsim_vs_minqmtsim_orderintents`
  - `engine_modeg_minqmt_capacity_reject`
  - `engine_modeg_broker_compat_reject`
  - （`...multi_package_localsim_isolation` 已在 PR-2 跑过）

**与 Codex Phase 4-6 衔接**：
- Phase 4：不依赖
- Phase 5：**有衔接但不阻断** — 等 Phase 5 model_registry 落地后，MiniQMTSimBroker 校验 ModelArtifact `broker_compatibility_filter`（如 artifact 显式排除 minqmt_sim）；本 PR 内仅消费 StrategySpec.broker_compatible，artifact 级 filter 留给 Phase 5 之后的小补丁
- **Phase 6 (Runtime Variants) 关键衔接**：本 PR 实现 broker_compatibility 在 `custom_extension` 的占位 reader；Phase 6 落地后 manifest schema 加一等公民 `broker_compatible` 字段（OPEN-EXT-3 双 PR 模式），切 reader 仅改一处函数。本 PR 不阻塞 Phase 6；Phase 6 落地后追加 1 个迁移 PR 切 reader。

**预估**：5-7 天（实现 + mock 单测 + SIM 集成测试 + Mode G）

**风险**：中-高 — 涉及外部 miniQMT 仿真依赖；但 PoC 阶段 1 已 PASS 大幅降低未知

---

### PR-4 — Frontend MVP：portfolios 列表 / 创建 wizard / 切换流程 / 错误页

**承接任务**：UI 实施（对应 portfolio_broker_backend_ui_design + broker_backend_switch_flow 设计）

**归属**：Claude 工作面（`frontend/src/app/paper-v2/`）

**Schema 改动**：无

**接口契约**（前端→后端）：
- portfolio 列表 API 返回 broker_backend_id 字段（PR-1 已暴露）
- portfolio 创建 API 接受 broker_backend_id + market_data_source（PR-1 已就位）
- portfolio 切换 API（POST /paper-v2/api/portfolios/switch，参 broker_backend_switch_flow §8）— 需 PR-2/3 同时合
- 错误响应序列化：typed error.context 透传到前端

**前端实现**：
- 列表页分 section（LocalSim 多包 + MiniQMTSim 单例 1/1）
- 创建 wizard 4 步：backend 选择 + broker_compatible 过滤 + 行情锁定 + 确认
- 切换流程 modal（I1 LocalSim→MiniQMTSim / I2 镜像 / I3 单 backend 内换包）
- 4 类 typed error 中文 UI 映射（参 broker_backend_switch_flow §6.3）：
  - `BrokerCompatibilityMismatchError` → 页面级错误页
  - `BrokerBindCapacityExceededError` → banner + "前往停用" 按钮
  - `MiniQMTSingletonViolation` → 系统错误 modal（不允许 retry）
  - `BrokerMarketSourceMismatchError` → 行情步骤内嵌 banner
- i18n key 规范统一前缀 `broker_error.*`

**单测覆盖**：
- e2e（playwright）：列表分 section / 创建 wizard 4 步 + 互斥逻辑 / 切换 3 种意图 / 4 类错误页
- 单元测试：i18n key 完整性 + ERROR_UI_MAP 渲染器配置正确
- 视觉 regression（如有）：PortfolioCard 视觉规范

**与 Codex Phase 4-6 衔接**：
- Phase 4-5：不依赖（UI 不消费 master_seed / model_registry 直连）
- Phase 6：UI 显示的 `broker_compatible` 徽章值来自后端（`custom_extension` 占位或 Phase 6 一等公民字段）；后端切字段时前端 fetcher 自动跟随，无需改 UI 代码

**预估**：8-10 天（多页面 + e2e 全覆盖）

**风险**：中-高 — UI 状态机复杂；多步骤交互；可分前后两半合（仅列表 + 创建为先合 PR-4a / 切换 + 错误页为 PR-4b — 这是 5 PR 选项的来源）

---

## 4. 备选拆分

### 4.1 最少 3 PR 版（PR-1 + PR-2 合并）

```
PR-A  Foundation + LocalSim Migrate  （PR-1+2 合并）
PR-B  MiniQMTSim                      （= PR-3）
PR-C  Frontend MVP                    （= PR-4）
```

**何时选**：
- 单人执行，希望减少切换成本
- DB additive 改动 + LocalSim 重构信心高（Lead review 接受较大 PR）

**风险**：合并后 PR 较大（约 800-1200 行 diff），review 难度上升

### 4.2 最多 5 PR 版（拆 PR-4）

```
PR-1  Foundation                       （= 上文 PR-1）
PR-2  LocalSim Migrate                 （= 上文 PR-2）
PR-3  MiniQMTSim                       （= 上文 PR-3）
PR-4  Frontend Lists+Create            （UI 列表 + 创建 wizard）
PR-5  Frontend Switch+ErrorMap         （切换流程 + 4 类 error UI）
```

**何时选**：
- 多人前端并行
- 切换流程语义复杂，单独 review 价值高
- 4 类 error UI 是 cross-test 取材的关键，单独 PR 便于 cross-test 模板 v0.4 引用 commit

**默认推荐**：4 PR 版（§3）— 在 review 成本与并行度之间最优

---

## 5. 合并顺序（impl-paper-v2 必须遵循）

```
PR-1 Foundation       ────► merge       (Day 1-4)
   │
   ├──► PR-2 LocalSim Migrate ────► merge   (Day 5-11)
   │
   ├──► PR-3 MiniQMTSim       ────► merge   (Day 5-11，可与 PR-2 并行)
   │
   └──► PR-4 Frontend MVP     ────► merge   (Day 4-14，前端可早启动用 mock)
```

**强制约束**：
- PR-1 必须先合（其他 3 个的接口源）
- PR-2 与 PR-3 之间无强依赖（不同 backend）
- PR-4 前端可与 PR-2/3 并行；mock fetcher → 后端就绪后切真实 API
- **任一 PR 合 main 后，既有 paper_trading_v2 LocalSim 行为必须 100% 不变**（P4 强制）

**多人并行**：单人后端 + 单人前端 ≈ 1.5 周完成 4 PR

---

## 6. 风险与对策

| # | 风险 | 概率 | 对策 |
| --- | --- | --- | --- |
| R1 | PR-2 day_runner 重构引入回归 | 中 | 既有 paper_trading_v2 测试全保留 + LocalSim Protocol 单元测试覆盖；分小 commit 内审 |
| R2 | PR-3 miniQMT 集成测试 CI 跳过，本地 PASS 但 CI 漏问题 | 中 | mark `requires_miniqmt_sim`；env-poc 在 PoC 环境跑全集；本地 commit 前必跑；docs 写明 |
| R3 | broker_compatible 在 custom_extension 占位时的 reader 与 OPEN-EXT-3 落地后字段不一致 | 低 | reader 抽象成函数（PR-3 一处实现）；OPEN-EXT-3 落地后补 1 个 PR 切 reader |
| R4 | Codex Phase 6 manifest 重 freeze 时本 MVP 已落 portfolio 用旧 manifest | 低 | broker_backend_id 在 portfolio 维度，与 manifest 解耦；不受 manifest 重 freeze 影响 |
| R5 | PR-4 前端切换流程状态机复杂，review 难度高 | 中-高 | 选 5 PR 版（§4.2）单独拆切换 + 错误页；状态机提取为独立 hook + 单元测试覆盖 |
| R6 | vnpy_connect_dry_run（task #17 (3)）报告 FAIL → 方案 B 不可行 | 低（不影响 MVP） | MVP 默认方案 A；方案 B 仅在 PR-014 触发，不阻塞 PR-3 |

---

## 7. 验收清单（4 PR 全合后 = MVP 完工）

| # | 验收项 | 来源 PR |
| --- | --- | --- |
| V1 | LocalSim 同时跑 ≥ 2 个 portfolio，账本互不干扰 | PR-2 |
| V2 | MiniQMTSim 单进程仅允许 1 个 portfolio；尝试第二个抛 `MiniQMTSingletonViolation` / `BrokerBindCapacityExceededError` | PR-3 |
| V3 | 行情通道 `MinuteDataSource` 与 backend 强绑定；跨配抛 `BrokerMarketSourceMismatchError` | PR-1 + PR-3 |
| V4 | broker_compatible 不兼容包不能创建 / 切换；抛 `BrokerCompatibilityMismatchError` | PR-3 |
| V5 | UI 创建 wizard 4 步 + 切换流程 + 4 类错误页全部物化 + i18n 完整 | PR-4 |
| V6 | Mode G 4 条新增用例 PASS（§3.6.6） | PR-2 + PR-3 |
| V7 | 既有 paper_trading_v2 LocalSim 行为 100% 兼容（既有测试零回归） | PR-1 / PR-2 / PR-3 / PR-4 全部 |
| V8 | 5 个实施任务（A#3/A#4/A#1/MiniQMTSim/Day Runner 路由）全部承接 | A#3 → PR-1 / A#4 → PR-1 / A#1 → PR-2 / MiniQMTSim → PR-3 / Day Runner 路由 → PR-2 |

---

## 8. 与外部决策项的关系

| 外部项 | 关系 |
| --- | --- |
| **OPEN-EXT-1**（Mode G 推 Codex 主体 §6 双 PR） | MVP 不依赖；Engine 文档单方面声明 Mode G 为 gate |
| **OPEN-EXT-2**（on_event 与 announcement_event_risk_signal 字段对齐） | MVP 不依赖；on_event 默认 no-op |
| **OPEN-EXT-3**（broker_compatible manifest schema 双 PR） | MVP 借 `custom_extension` 占位；OPEN-EXT-3 落地后追加 1 个迁移 PR（不在本 4 PR 内） |
| **vnpy_connect_dry_run 报告**（task #17 (3) 后续） | MVP 默认方案 A；方案 B 为可选 PR-014（不在本 4 PR 内）。详见 §9 "vnpy_xt 方案 B 接入"的双触发条件 |
| **Codex Phase 4 (Master Seed Contract)** | MVP 不依赖（broker 抽象不消费 master_seed） |
| **Codex Phase 5 (Model Library)** | MVP 不依赖；落地后 1 个补丁 PR 加 artifact 级 broker filter |
| **Codex Phase 6 (Runtime Variants)** | MVP 不依赖；落地后 OPEN-EXT-3 跟进迁移 PR |

---

## 9. 不在 MVP 范围（明确推迟）

- 实盘 backend `minqmt_live`（待主体 §11 准入流程 + 用户授权）
- 跨进程 portfolio 协调
- Strategy Engine 决策内核完整实施（依赖 Codex Phase 4 master_seed_contract）
- vn.py 完整 main_engine
- broker_compatible 字段升级为 manifest 一等公民（OPEN-EXT-3 单独迁移 PR）
- 多 alpha 组合（Engine 内 `compute_score_combination`）
- vnpy_xt 方案 B 接入（PR-014）— **双触发条件**（满足任一即可启动；Lead 2026-05-09 批准）：
  - **触发 1（前置主因）**：`vnpy_connect_dry_run`（task #17 (3) 由 env-poc 接力）报告推荐方案 B（决策矩阵 PASS + callback shape 一致性 ≥ 80% + close 干净）
  - **触发 2（后置补丁）**：MVP 4 PR 合 main 后，LocalSim/MiniQMTSim 在生产运行期发现 xtquant 直调系统性缺陷（如 callback 顺序不可靠 / 心跳重连难做 / `on_stock_order` 丢帧 等可重现的接口层不可靠现象），PR-014 也可启动用 vnpy_xt 包装层规避
  - 实施时 `MiniQMTSimBrokerVnpy` 与现有 `MiniQMTSimBroker` 并列（不替换），通过 `broker_backend_id` 配置切换；Mode G 增 `engine_modeg_xtquant_vs_vnpy_xt_orderintents` 用例验证两实现等价

---

## 10. 一句话总结

**MVP = 4 个 PR（备选 3 或 5）3 周（多人并行 1.5 周）**：PR-1 Foundation（A#3+A#4+Protocol）/ PR-2 LocalSim Migrate（A#1+Day Runner 路由）/ PR-3 MiniQMTSim（singleton + broker_compatibility）/ PR-4 Frontend MVP。每 PR 自包含、可独立合 main、零回归既有 LocalSim；与 Codex Phase 4-6 解耦（不阻塞、不被阻塞）；OPEN-EXT-3 / Phase 5 落地后追加迁移 PR。

---

**End of MVP PR plan (3-5 PR 修订版)**.
