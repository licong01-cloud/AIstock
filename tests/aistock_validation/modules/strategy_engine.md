# Strategy Engine 测试矩阵

日期：2026-05-09
归属：Claude Code 工作面（paper-v2-vnpy-mvp 团队）
设计依据：`docs/architecture/strategy_engine_design_20260508.md`（含 §3.6 BrokerBackend / §10.1 错误层级 / §17 Lead 裁决）

## 模块定位

Strategy Engine 是 plain Python 决策内核，把 (frozen StrategyPackage v2 manifest, ScoreFrame, PortfolioState, SeedBundle, ModelArtifactHandle, RuntimeOverlay) 装配为 `OrderIntentBatch + DecisionTrace`。Engine **不感知**执行端、**不连**DB、**不做**模型推理。

| 维度 | 取值 |
| --- | --- |
| 模块 ID | `strategy_engine` |
| 风险等级 | high（决策一致性是 paper / live / QE 三 adapter 等价性的物理基础） |
| 工作面 | Claude Code（设计 + 接口）；具体实施依赖 Codex Phase 4 / 5 |
| 是否触动 main | 仅文档；实施代码合 main 走 PR 流程 |

## L0 静态守卫

L0 trigger：Engine 设计文档变更 / 新加 Engine 接口字段 / 新加 typed error 类。

- L0-G1：`docs/architecture/strategy_engine_design_20260508.md` 文档术语扫描通过（无 v0 / v1 旧术语残留）
- L0-G2：Engine 接口签名（伪 Pydantic）与 §3.1 / §3.2 / §3.5 / §3.6 一致；`init / decide_eod / on_bar / on_event / close` 五个 lifecycle 入口签名不变
- L0-G3：`§10.1` 错误类层级树完整；任何新加 typed error 必须挂在 `StrategyEngineError` 下
- L0-G4：禁止 `except: pass` / 默认值 fallback / silent ignore 出现在 Engine 实施代码（参考 `feedback_no_silent_errors.md`）

pass criteria：
- 文档 lint 通过（已有 L0 nox session）
- grep 旧术语 0 命中
- typed error 类层级一致性脚本通过（实施期补）

## L1 单能力

L1 trigger：单个 Engine 模块 / 单个 typed error 类 / 单个 BrokerBackend 方法变更。

### L1-C1：StrategySpec 字段完整性
- 构造合法 `StrategySpec(package_id, manifest_sha256, alpha_mode, frozen_alpha_core, baseline_runtime, seed_contract, model_artifact_pointer, ...)` → init() 通过
- 字段缺失 / 类型错误 → `StrategySpecValidationError`，含字段名 + 期望类型
- pass：所有字段缺失负 case 抛 typed error；error.context 含字段名

### L1-C2：SeedBundle 校验路径（4 种 SeedContractError 触发）
- `seed_policy="lottery_random"`（非法枚举）→ `SeedContractError`，message 含 "seed_policy" + 实际值
- `frozen_alpha_core` 要求 fixed seed 但 `seed_policy="unset_legacy"` → `SeedContractError`
- LGB model family 但 seed_sequence 缺 "lgb" key → `SeedContractError`
- `library_versions["numpy"]` 与 frozen_alpha_core 期望差异 → `SeedContractError`，含两侧版本
- pass：4 类负 case 全部抛 typed error；不静默 fallback；不允许 `random.SystemRandom()` 填补

### L1-C3：RuntimeOverlay allow-list（来源 Codex schema）
- overlay 含合法字段（topk / n_drop / threshold_overlay / minute_execution_algo / cost_overrides / hmm_toggle / sector_blacklist / capital_capacity）→ 通过
- overlay 含 `factor_set` / `model_weights` / `preprocessor` 任一 → `RuntimeOverlayValidationError`，rejected_field=X
- overlay 含 `runtime_variant_id` 但缺 `runtime_variant_hash` → 拒收
- pass：allow-list 与 Codex `package_runtime_variant` schema 派生一致；不硬编码字段

### L1-C4：ModelArtifactHandle 校验
- artifact.status ∈ {promoted_artifact, paper_enabled} → 通过
- artifact.spec_id != spec.frozen_alpha_core.model_spec_id → `ModelArtifactMismatchError`
- artifact.weight_sha256 == "" 或文件不存在 → `ModelArtifactMismatchError`
- artifact.feature_order_sha256 != spec.frozen_alpha_core.feature_schema_sha256 → 抛错
- pass：4 类不一致负 case 全部抛 `ModelArtifactMismatchError`，不允许 fallback 到 next candidate

### L1-C5：BrokerBackend ABC 接口完整性（R-Q9.5 D1/D2/D3 + R-Q9.6）
- `query_account()` 返回 `BrokerAccountSnapshot`（broker 维度，与 `trading_core.AccountSnapshot` portfolio 维度区别）
- `query_positions()` 返回 `dict[str, trading_core.PositionLot]`（复用，不重定义）
- `subscribe_fill_callback(cb)` 返回 `SubscriptionHandle`；`unsubscribe_fill_callback(handle)` 释放回调
- `unsubscribe` 幂等：同一 handle 第二次调用不抛错；unknown / released / shutdown 期 silent noop
- 真实 unsubscribe 错误（底层连接故障）→ `BrokerConnectivityError`
- `market_data_channel()` 返回 `MarketDataChannel(channel_kind ∈ {"in_process_tdx", "in_process_db", "minqmt_xtdata"})`
- `bind_capacity()` 返回 `BrokerBindCapacity(backend_id, max_concurrent_packages>=1, rejection_reason_if_exceeded)`
- pass：现有 `backend/tests/paper_trading_v2/test_localsim_backend.py` 20 个 test_* 函数全绿

## L2 组件 / API / DB 流

L2 trigger：Engine ↔ BrokerBackend ↔ adapter 三方协作；DecisionTrace 写入；Engine.decide_eod 全链路。

### L2-F1：Engine.init 全路径
- 输入：合法 StrategySpec + RuntimeOverlay + SeedBundle + ModelArtifactHandle
- 路径：所有 §3.6.5 broker_compatibility 校验 + §7 SeedBundle 校验 + §8 ModelArtifact 校验
- 输出：`EngineSession` 对象 + DecisionTrace 初始化
- pass：合法路径不抛错；任一字段不一致按 L1-C* 类型抛 typed error

### L2-F2：Engine.decide_eod 同输入 byte-equal（Phase 4 L4 gate 子集）
- fix master_seed + 完整 SeedBundle + 同 (spec, scores, portfolio)
- 跑两次 `Engine.decide_eod()`
- assert OrderIntentBatch.intents byte-equal（含字段顺序）
- assert DecisionTrace.pipeline_steps 每步 input_digest / output_digest 一致
- pass：byte-equal 严格；任何差异定位到 dict 迭代 / set 序列化 / float 精度三类

### L2-F3：DecisionTrace.inputs_digest 折入 broker_compatible（R-Q9 D4）
- 同 (scores, portfolio, seed) 用 `broker_compatible="LocalSim_only"` → digest_1
- 同 (scores, portfolio, seed) 用 `broker_compatible="both"` → digest_2
- assert digest_1 != digest_2
- pass：`broker_compatible` 折入 inputs_digest；不允许"按需写入"

### L2-F4：BrokerCompatibility 校验在 init 阶段
- spec.broker_compatible="LocalSim_only" + portfolio.broker.backend_id="minqmt_sim"
- 调 Engine.init() → 抛 `BrokerCompatibilityMismatchError`，error.context 含 package_id / broker_compatible_value / target_backend_id
- pass：典型不兼容矩阵（LocalSim_only ↔ minqmt_sim / MiniQMTSim_only ↔ local_sim）全部拒收

### L2-F5：custom_extension audit-only 语义（R-Q2）
- spec.custom_extension={"foo": "bar"} → init() 通过；DecisionTrace 含 custom_extension 摘要
- Engine 内部代码不以 custom_extension 字段做决策路径分支
- pass：custom_extension 仅 audit；不存在 extension_handler 注册机制

## L3 模块 UI/API 回归

> Engine 本身**无独立 UI**；L3 通过 Paper v2 / QE Adapter / Live Adapter 间接验证。本节定义"Engine 与上层 adapter 集成 L3"。

### L3-I1：Engine ↔ Paper v2 LocalSim adapter 集成
- 走 paper_v2 创建 portfolio → 触发 day_runner → 调 Engine.decide_eod → LocalSimBackend.submit_order_intent
- pass：portfolio 创建 + 一日完整 OrderIntent → fill → ledger 更新链通；UI 不显示原始 typed error class 名（按 §6.3 中文 UI 映射）

### L3-I2：Engine ↔ QE backtest adapter 等价性（Mode G smoke）
- 同 (manifest, scores, portfolio, seed)，QE adapter 与 Paper LocalSim adapter 各跑一次
- assert OrderIntent 序列 byte-equal（不比 NAV）
- pass：等价性通过即可标"Mode G smoke 已跑过"，broker_compatible 可保留 "both" 默认值

### L3-I3：BrokerCompatibilityMismatchError UI 映射（§6.3）
- 触发 portfolio 切换到 minqmt_sim 但 spec="LocalSim_only" → 后端抛 typed error
- 前端必须显示页面级 UI（标题"策略包与撮合后端不兼容"+ ≥2 actionable + §3.6.5 文档链接）
- assert 不出现"强行继续"按钮（forbidOverride: true）
- pass：UI 渲染符合 §6.3 不变量；console / pageerror 不显示 Python traceback

## Pass Criteria 汇总

| 等级 | 必须项 |
| --- | --- |
| L0 | 文档 lint + 术语扫描 + 错误类层级一致性 |
| L1 | 所有 5 类单能力测试 case 全绿；error.context 字段完整 |
| L2 | byte-equal Phase 4 L4 子集通过；典型不兼容矩阵全部拒收；custom_extension audit-only |
| L3 | Engine ↔ paper v2 LocalSim 集成走通；Mode G smoke byte-equal；UI 映射符合 §6.3 |

每级失败 = 该级 + 上级回归 fail；不允许跳级宣称。

## 失败处理预期

- L0 失败 → 阻断 PR 合 main；先修文档 / 错误类层级
- L1 失败 → 阻断该子能力实施；先修单元测试
- L2 byte-equal 失败 → Engine PR 不合 main（Phase 4 L4 gate 严格）；定位到 dict / set / float 三类后修复
- L3 失败 → 阻断 paper v2 / QE adapter 集成；不允许 "Override and proceed"

## 与 Codex 模块的边界

| 不属于本模块（Codex 范围） | 落地位置 |
| --- | --- |
| QE 治理 / Validation Modes A-F / Phase 0-7 实施 | `qe_governance.md`（Codex 维护） |
| Model Registry 4 层（Template / Spec / Trial / Artifact）schema 与 CRUD | `model_registry.md`（Codex 维护） |
| StrategyPackage v2 manifest schema 修订 | `strategy_package_v2.md`（Codex 维护） |
| Master Seed Contract Phase 4 L4 gate 完整执行 | `qe_reproducibility.md`（Codex 维护） |
| Mode G 主体 §6 正式纳入决策（OPEN-EXT-1） | Codex 主体设计修订 |
| `broker_compatible` 字段进 manifest schema（OPEN-EXT-3） | Codex 主体附录 A.4.4 双 PR |

本模块仅覆盖 Engine 层；上述跨模块关注点必须 cross-tester 与对应模块负责人协调。

## 取材源

- `docs/architecture/strategy_engine_design_20260508.md` §3 / §6 / §7 / §8 / §10 / §11 / §17
- `docs/standards/cross_test_framework_template_20260508.md` §2.4 / §3.5.1（v0.5）
- `backend/services/paper_trading_v2/broker/{base,localsim}.py`
- `backend/tests/paper_trading_v2/test_localsim_backend.py`（20 个 test_* 函数）
- `feedback_no_silent_errors.md`

## Deferred Scope

- MiniQMTSim adapter 层 L1-L3 case：等 PR-005 实施，落到独立 `minqmtsim_adapter.md` 或本模块 v1.1 增量
- Live adapter（minqmt_live）准入 case：等用户授权 + 主体 §11 流程
- 多 alpha / ensemble 推理路径：本期单 alpha + frozen weight，跳过
- on_event 事件信号集成：等 OPEN-EXT-2 授权 + announcement_event_risk_signal 对齐
