# Strategy Engine 接口纸面设计

> **作者**：Claude Code (Opus 4.7) — engine-design teammate
> **日期**：2026-05-08
> **范围**：纸面设计；不写代码、不改 Codex 主体设计、不动 main
> **工作面**：worktree `F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508`
> **必读上下文**：
> - `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`（主体 + 附录 A、B；本设计是附录 A.3 的展开）
> - `docs/discussion/agent_teams_session_handoff_20260508.md`（授权 §2、隔离 §3、边界 §8.1）
> - `docs/codex_project_memory.md`（生产 8001 / 模块边界 / Git）
>
> **核心约束**：本设计**仅在 StrategyPackage v2（Codex 主体设计）之上叠加**。Engine 不修改 Codex Phase 0-7 的任何契约，仅消费已 frozen 的 manifest + master_seed + ModelArtifact pointer。

---

## 0. 一句话总结

Strategy Engine 是一个 **plain Python 决策内核**，把 (frozen StrategyPackage v2 manifest, score series, current positions, master seed contract, runtime variant overrides) 装配为一组可被 QE / Paper / Live 三个 adapter 复用的 `OrderIntent`。它**不感知**执行端（Qlib / SimGateway / vnpy_xt），不做撮合、不做行情访问、不做模型推理；它只做"持仓决策"。

---

## 1. 设计目标与非目标

### 1.1 目标

| # | 目标 | 验收口径 |
| --- | --- | --- |
| G1 | **决策一致**：QE Adapter / Paper Adapter / Live Adapter 给同一 `(manifest, scores, positions, seed)` 必产出 100% 相同 `List[OrderIntent]` | 主体附录 A.3.4 Mode G Cross-Adapter Equivalence |
| G2 | **可复现**：同一 `(manifest, master_seed, inputs)` 跑两次，输出 byte-equal | Codex 主体 §A.5.2 Phase 4 L4 gate |
| G3 | **零 silent fallback**：任何缺字段、不可选模型、风险策略冲突 → 显式抛错；不允许 except: pass / 默认值 | 用户 feedback `feedback_no_silent_errors` |
| G4 | **类别 C 修改单点扩展**：新加权重算法 / 多 alpha 组合 rule / 公告信号合成时，**只在 Engine 改一次** | 主体附录 A.6.1 / A.6.2 |
| G5 | **不依赖 Qlib / vn.py / xtquant**：纯 stdlib + numpy + pydantic | 主体附录 A.3.2 |

### 1.2 非目标（明确不在 Engine 内）

- 因子计算 / 模型推理 → `selection_center/inference_engine.py`（已存在）
- 撮合 / fill 模拟 → adapter 各自处理（QE 用 Qlib backtest exchange / Paper-Live 用 vnpy `BacktestingEngine` 或 `SimGateway` / 实盘 `vnpy_xt`）
- 行情数据访问 / 加载 score → adapter 上游
- 订单状态机 / 对账 / OEMS → vnpy `OmsEngine`（trading_core daemon 内）
- 持仓持久化 / portfolio 状态 → trading_core / Paper v2 DB
- 风险后置实时熔断 → 实时风控（不在本 Engine 范畴；Engine 只做决策时的硬过滤）

---

## 2. 三层架构定位

复制主体附录 A.2 图，标注 Engine 的输入输出边界：

```
┌──────────────────────────────────────────────────────────┐
│ 治理层（Codex 主体 Phase 0-7，本设计仅消费，不修改）       │
│  StrategyPackage v2 manifest + master_seed + Frozen Core │
│  Model Registry (Template/Spec/Trial/Artifact)           │
│  Validation Modes A-F                                    │
└──────────────────────────────────────────────────────────┘
                        │ (frozen manifest pointer)
                        ▼
┌──────────────────────────────────────────────────────────┐
│ Strategy Engine（本文档）                                  │
│  Inputs:                                                 │
│   - StrategySpec（Pydantic 包装 manifest）                │
│   - SeedBundle（来自 SeedContract）                       │
│   - ModelArtifactHandle（来自 Model Registry）            │
│   - ScoreFrame（adapter 上游产出）                        │
│   - PortfolioState（current positions + cash）            │
│   - RuntimeOverlay（runtime variant 内可调字段）          │
│   - ClockContext（trade_date, now_ts, mode）              │
│  Outputs:                                                │
│   - OrderIntentBatch（List[OrderIntent] + DecisionTrace）│
└──────────────────────────────────────────────────────────┘
                        │
            ┌───────────┼────────────┐
            ▼           ▼            ▼
       QE Adapter  Paper Adapter  Live Adapter
       (Qlib YAML) (trading_core) (trading_core)
            │           │            │
            ▼           ▼            ▼
       Qlib backtest   vnpy SimGw   vnpy_xt → miniQMT
```

**核心边界**：Engine 只产 intent，不下单；adapter 把 intent 翻译为各自执行环境的语义（Qlib `Order` / vnpy `OrderRequest` / xtquant trade API）。

---

## 3. 接口契约

### 3.1 顶层 Engine 接口

```python
# 仅纸面伪代码 — 实施留待 Codex Phase 4 完成后

class StrategyEngine(Protocol):
    """
    Stateless decision kernel.

    A new Engine instance is constructed per (StrategyPackage v2 manifest, runtime
    variant). The engine itself caches no per-trade-date state; portfolio /
    seed / clock context are passed in at each on_bar / decide call by the
    adapter.

    Engine does NOT load score data; the adapter feeds ScoreFrame in.
    """

    # ----- Lifecycle -----
    def init(
        self,
        spec: StrategySpec,                  # frozen Alpha Core + baseline runtime
        overlay: RuntimeOverlay | None,      # variant overrides (allowed fields only)
        seed_bundle: SeedBundle,             # SeedContract decoded
        model_handle: ModelArtifactHandle,   # selected ModelArtifact pointer
    ) -> EngineSession:
        """
        Validate spec + overlay + seed + model compatibility.
        Raise StrategySpecValidationError / SeedContractError /
        ModelArtifactMismatchError on any inconsistency. NO silent fallback.
        """

    # ----- Per-trade-date decision -----
    def decide_eod(
        self,
        session: EngineSession,
        clock: ClockContext,                 # trade_date + mode (close / preopen)
        scores: ScoreFrame,                  # symbol → score, with provenance
        portfolio: PortfolioState,           # positions + cash + lots
    ) -> OrderIntentBatch:
        """
        End-of-day rebalance decision. The canonical entry for QE backtest +
        Paper v2 daily rebalance. Returns OrderIntentBatch with full
        DecisionTrace for audit + Mode G equivalence.
        """

    # ----- Optional intra-day hook (Live / future use) -----
    def on_bar(
        self,
        session: EngineSession,
        clock: ClockContext,
        bar: BarFrame,
        portfolio: PortfolioState,
    ) -> OrderIntentBatch | None:
        """
        For minute-execution intent refinement.

        Lead 2026-05-08 R-Q3: signature defined this round; default base
        Engine returns None (decisions made at decide_eod). v24/v25/v26
        minute-execution algos implement this hook in dedicated subclasses
        when their respective integration phases land. Live adapter only.
        """

    # ----- Optional event hook -----
    def on_event(
        self,
        session: EngineSession,
        clock: ClockContext,
        event: EventSignal,                  # announcement / earnings (future §A.6.2)
        portfolio: PortfolioState,
    ) -> OrderIntentBatch | None:
        """
        Reserved for event-driven adjustment; default no-op.

        Engine-side semantics (Lead 2026-05-08 R-Q4): on_event triggers
        INDEPENDENT OrderIntent adjustments (NOT score-merging — score-side
        composition stays in adapter upstream).

        EventSignal input schema is a PLACEHOLDER pending alignment with
        announcement_event_risk_signal_top_level_design.md (external decision
        item OPEN-EXT-2). Until alignment, default implementation MUST return
        None.
        """

    # ----- Lifecycle close -----
    def close(self, session: EngineSession) -> EngineCloseReport:
        """
        Emit decision telemetry summary. Stateless engines may return empty
        report. Adapter logs the report under (package_id, portfolio_id,
        run_id).
        """
```

### 3.2 输入数据结构（伪 Pydantic）

```python
class StrategySpec(BaseModel):
    """Pydantic projection of frozen StrategyPackage v2 manifest."""
    package_id: str
    manifest_sha256: str                    # MUST be set; reject empty
    alpha_mode: Literal["single_alpha", "multi_alpha"]
    frozen_alpha_core: FrozenAlphaCore       # see §3.3
    baseline_runtime: BaselineRuntime        # see §3.4
    seed_contract: SeedContractRef           # pointer to seed contract
    model_artifact_pointer: ModelArtifactRef # selection rule + filters
    custom_extension: dict[str, Any] | None  # main §A.6.5 escape hatch — Engine treats as audit-only (R-Q2); never executed

class RuntimeOverlay(BaseModel):
    """Runtime variant overrides — main §5.3.

    The allow-list MUST be derived from the Codex `package_runtime_variant`
    schema (source of truth) — Engine does NOT hardcode the field set
    (Lead 2026-05-08 R-Q6). Implementation depends on Codex Phase 6 schema
    landing in the integration branch. The fields below are illustrative
    placeholders; the actual list comes from the schema descriptor.

    Any field NOT in the derived allow-list MUST be rejected at init() time
    (no silent ignore).
    """
    runtime_variant_id: str
    runtime_variant_hash: str
    topk: int | None = None
    n_drop: int | None = None
    threshold_overlay: ThresholdOverlay | None = None
    minute_execution_algo: str | None = None    # v24 / v25.1 / v26
    cost_overrides: CostOverrides | None = None
    hmm_toggle: HMMToggle | None = None
    sector_blacklist: list[str] | None = None
    capital_capacity: CapitalCapacity | None = None
    # Forbidden (raise if attempted): factor set / model weights / preprocessor

class SeedBundle(BaseModel):
    """Decoded SeedContract — main §7.3."""
    seed_policy: Literal["fixed", "multi_seed", "random_logged", "unset_legacy"]
    master_seed: int | None
    seed_sequence: dict[str, int]            # per sub-seed (numpy/torch/lgb/...)
    deterministic_algorithms: bool
    cudnn_deterministic: bool
    library_versions: dict[str, str]
    # Engine usage: most paths only consume master_seed + seed_sequence;
    # adapter passes deterministic flags down to its inference layer.

class ModelArtifactHandle(BaseModel):
    """Pointer + metadata for selected ModelArtifact — main §8.2."""
    artifact_id: str
    spec_id: str
    template_id: str
    weight_uri: str                          # protected asset path
    weight_sha256: str
    feature_order_sha256: str
    preprocessor_uri: str | None
    prediction_schema_sha256: str
    selection_reason: str                    # which selection rule fired

class ScoreFrame(BaseModel):
    """Score input from adapter (NOT loaded by Engine itself)."""
    package_id: str
    manifest_sha256: str                     # must match spec.manifest_sha256
    trade_date: date
    rows: list[ScoreRow]                     # symbol, score, rank, tradability_flags
    source: Literal["qe_artifact", "live_inference", "backtest_replay"]
    source_uri: str | None
    no_candidate_reason: str | None          # adapter declares; engine validates

class PortfolioState(BaseModel):
    portfolio_id: str
    as_of: datetime
    cash: Decimal
    positions: dict[str, PositionLot]        # symbol → lot
    nav: Decimal
    capacity_used: Decimal | None

class ClockContext(BaseModel):
    trade_date: date
    now_ts: datetime
    mode: Literal["eod_close", "pre_open", "intra_bar", "event"]
    timezone: str = "Asia/Shanghai"
```

### 3.3 FrozenAlphaCore（mirrors 主体 §5.1，Engine 仅读）

```python
class FrozenAlphaCore(BaseModel):
    factor_set: list[FactorRef]              # ordered
    factor_set_sha256: str
    model_spec_id: str
    model_code_sha256: str
    model_arch_params_sha256: str
    training_recipe_sha256: str
    seed_policy_ref: str
    weight_artifact_id: str
    feature_schema_sha256: str
    label_config_sha256: str
    split_identity_sha256: str
    source_data_snapshot_id: str
```

Engine **绝不修改**任何 frozen core 字段；overlay 改动 frozen core 任意字段必须在 init() 显式拒绝。

### 3.4 BaselineRuntime（mirrors 主体 §5.2）

```python
class BaselineRuntime(BaseModel):
    strategy_params: StrategyParams           # topk/n_drop/threshold defaults
    portfolio_params: PortfolioParams         # capital / max_position_ratio
    risk_policy: RiskPolicy                   # tradability / blacklist baseline
    minute_execution_policy: MinuteExecPolicy
    hmm_settings: HMMSettings | None
    cost_model: CostModel
    rebalance_policy: RebalancePolicy
    selection_runtime_defaults: dict
```

### 3.5 输出数据结构

```python
class OrderIntent(BaseModel):
    """Re-uses backend.services.trading_core.models.OrderIntent shape.

    The adapter is responsible for translating intent into Qlib Order /
    vnpy OrderRequest. Engine never sets venue / broker fields.
    """
    intent_id: str
    package_id: str
    portfolio_id: str
    trade_date: date
    symbol: str
    side: Literal["buy", "sell"]
    target_quantity: int                      # shares (A-shares 100 lot)
    target_weight: Decimal
    reference_price: Decimal | None           # close-of-prev-day or last bar
    reason: str                               # rebalance / dynamic_ndrop / hold_thresh_release
    parent_intent_id: str | None              # for child intents from on_bar
    decision_trace_id: str

class OrderIntentBatch(BaseModel):
    package_id: str
    portfolio_id: str
    trade_date: date
    intents: list[OrderIntent]
    decision_trace: DecisionTrace
    engine_run_id: str
    engine_version: str

class DecisionTrace(BaseModel):
    """Mode G equivalence audit payload — main §A.3.4.

    Granularity policy v0 (Lead 2026-05-08 R-Q7): fixed full granularity —
    every pipeline step writes input_digest / output_digest / params_digest.
    v1 may introduce baseline/debug tiers based on implementation-phase
    performance data; not in scope for this design.

    SeedBundle digest MUST be folded into inputs_digest unconditionally
    (Lead 2026-05-08 R-Q5) — no opt-in policy. This guarantees Phase 4 L4
    gate byte-comparison covers seed state.

    custom_extension (StrategySpec.custom_extension) MUST be folded into
    inputs_digest as audit only (Lead 2026-05-08 R-Q2). The Engine does NOT
    interpret or execute custom_extension content.
    """
    inputs_digest: str                        # sha256(spec + scores + portfolio + seed_bundle + custom_extension)
    pipeline_steps: list[PipelineStepRecord]
    final_targets: list[TargetPosition]
    diagnostics: dict[str, Any]               # per-step measurements
    seed_bundle_digest: str                   # explicit field (always populated)
    custom_extension_digest: str | None       # audit-only echo of spec.custom_extension
```

### 3.6 BrokerBackend 抽象 + 两种 SimMode 对照（Lead 2026-05-08 R-Q9）

> **背景**：`docs/analysis/paper_v2_blockers_20260508.md` P0-H 揭示 — 现有 Paper v2 = 纯本地撮合 + TDX 行情；miniQMT 通道仅以独立 client 存在，从未接入 paper_trading_v2；audit / Codex 主体设计 / 本 Engine 设计 v1 均无"本地多策略包并行 vs miniQMT 单策略包"产品形态二分。Lead 2026-05-08 已用户授权 4 项决策落地（详见 §17 R-Q9）。
>
> **本节范围**：纸面定义 BrokerBackend 抽象 + 两种 SimMode 的对照 + StrategyPackage 兼容性字段 + 行情通道强绑定规则。Engine 自身（决策内核）不感知具体 broker；本节定义 **adapter 必须遵守的 broker 抽象**，使 Engine 输出 `OrderIntent` 能被两种 SimMode 一致消费。

#### 3.6.1 BrokerBackend 抽象

```python
class BrokerBackend(Protocol):
    """
    Adapter-side broker abstraction. Engine itself never imports this Protocol;
    each adapter (QE / Paper / Live) selects a concrete BrokerBackend per
    portfolio binding. Engine's OrderIntent contract MUST be backend-agnostic.

    Two concrete backends in scope this round (Lead 2026-05-08 R-Q9 D1):
      - LocalSimBroker:    in-process matching against TDX bars (today's
                            paper_trading_v2 default). Supports parallel
                            multi-package binding.
      - MiniQMTSimBroker:  routes OrderIntent to miniQMT 仿真账户 via
                            xtquant. Single-package binding only (miniQMT
                            account = single trading session per process).
    """

    backend_id: Literal["local_sim", "minqmt_sim", "minqmt_live"]
    backend_version: str

    # ----- Order lifecycle (Lead 2026-05-08 R-Q9 派单规范) -----
    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        """
        Translate Engine OrderIntent into backend-native order; return handle
        for later cancel / status query. NEVER mutate intent in place.

        Errors are typed (BrokerSubmitError / BrokerRejectedError /
        BrokerConnectivityError) and propagated to adapter. NO silent
        fallback (feedback_no_silent_errors). Adapter MUST NOT catch and
        retry blindly; failed submit propagates up to trading_core errors.
        """

    def cancel(self, handle: OrderHandle) -> CancelAck: ...
    def query_status(self, handle: OrderHandle) -> OrderStatus: ...
    def subscribe_fill_callback(self, cb: Callable[[FillEvent], None]) -> SubscriptionHandle: ...
    def query_account(self) -> AccountSnapshot: ...
    def query_positions(self) -> dict[str, PositionLot]: ...

    # ----- Channel + capacity introspection -----
    def market_data_channel(self) -> MarketDataChannel:
        """Returns the bound market-data channel (R-Q9 D3 — see §3.6.4)."""

    def bind_capacity(self) -> BrokerBindCapacity:
        """
        Declares whether this backend instance accepts MULTIPLE concurrent
        StrategyPackage bindings or only ONE. Used by adapter at portfolio
        bootstrap to enforce R-Q9 D2.
        """


class OrderHandle(BaseModel):
    handle_id: str
    backend_id: str
    submitted_at: datetime
    intent_id: str                            # echoes OrderIntent.intent_id


class OrderStatus(BaseModel):
    handle_id: str
    state: Literal["pending", "partial_filled", "filled", "cancelled", "rejected"]
    filled_quantity: int
    avg_fill_price: Decimal | None
    last_event_at: datetime
    rejection_reason: str | None              # populated iff state == "rejected"


class FillEvent(BaseModel):
    handle_id: str
    intent_id: str
    fill_quantity: int
    fill_price: Decimal
    fill_ts: datetime
    venue: str                                # "local_sim" / "minqmt_sim" / ...


class AccountSnapshot(BaseModel):
    backend_id: str
    cash: Decimal
    nav: Decimal
    margin_used: Decimal | None
    as_of: datetime


class CancelAck(BaseModel):
    handle_id: str
    accepted: bool
    reason: str | None



class BrokerBindCapacity(BaseModel):
    backend_id: str
    max_concurrent_packages: int          # LocalSim: ≥1 (parallel); MiniQMTSim: 1
    rejection_reason_if_exceeded: str
```

Engine 输出的 `OrderIntent` schema **不变**；adapter 在 `submit()` 中翻译为各 backend 的下单 API（LocalSim 的内部 ledger / vnpy_xt 的 `xtquant.xttrade.order_stock`）。

#### 3.6.2 两种 SimMode 完整对照表

| 维度 | LocalSim（本地撮合） | MiniQMTSim（miniQMT 仿真） |
| --- | --- | --- |
| **backend_id** | `local_sim` | `minqmt_sim` |
| **撮合发生地** | 进程内 ledger（沿用现 paper_trading_v2 撮合栈） | miniQMT 仿真服务进程外（xtquant `xttrade`） |
| **行情源（强绑定，R-Q9 D3）** | `MinuteDataSource ∈ {TDX_REALTIME, DB_HISTORICAL}` | `MinuteDataSource = MINIQMT_REALTIME`（**新增枚举值**，见 §3.6.4） |
| **多策略包并行（R-Q9 D2）** | ✅ 每个 portfolio 独立 BrokerBackend 实例；N 个 portfolio 互相隔离（账本 / 资金 / 持仓） | ❌ 进程内**单例**；尝试启动第二个抛 `MiniQMTSingletonViolation` |
| **接近实盘** | 中（撮合简化模型；时序同步） | 高（真实 miniQMT 撮合通道；与实盘账户共用 trade 协议） |
| **历史回放** | ✅ 支持（`MinuteDataSource.DB_HISTORICAL` + CATCHUP_THEN_LIVE） | ❌ 不支持（miniQMT 仿真账户只接受实时单） |
| **资金上限** | 配置决定（无外部约束） | miniQMT 仿真账户配置决定；超额抛 `BrokerRejectedError` |
| **启动成本** | 低（in-process，进程启动即可） | 高（依赖 miniQMT 仿真服务进程在运行 + xtquant 已 attach） |
| **Engine 端差异** | 无（Engine 输出同一 OrderIntent） | 无（Engine 输出同一 OrderIntent） |
| **Adapter 翻译层** | LocalSim adapter（沿用现 paper_trading_v2 day_runner 撮合分支） | MiniQMTSim adapter（task #3 vn.py PoC 已验证 — 详见下文"对接现有代码"列） |
| **PortfolioState 来源** | adapter 内存 ledger 镜像 | xtquant `query_stock_positions` 实时拉取 |
| **故障语义** | 进程崩溃 = ledger 丢失（除非接持久化层） | miniQMT 仿真服务崩溃 = session 中断；adapter 必须显式抛 `BrokerConnectivityError`（feedback_no_silent_errors） |
| **对接现有代码** | 复用 `backend/services/paper_trading_v2/day_runner.py` `MinuteExecutionEngine` + `paper_trading_v2/market_data.py::MinuteDataSource.{TDX_REALTIME, DB_HISTORICAL}` | 走 PoC 验证的 xtquant 直调（**方案 A 推荐**：直接 import xtquant）或 vnpy_xt + PYTHONPATH hack（**方案 B**：env-poc 验证后选定）；最终方案待 task #10 盘中复测后由 env-poc 给出选型 |
| **未来 MiniQMTLive 关系** | N/A | 实盘 backend `minqmt_live` 共用本 adapter 翻译层；仅切换 miniQMT 账户类型（仿真→实盘）；`broker_compatibility` 字段必须额外标 `minqmt_live` 才允许 |

**关键不变量**：两种 backend 给同一 (manifest, scores, portfolio, seed) 必须产出相同 OrderIntent 序列（Mode G 已覆盖）；NAV 差异由撮合层差异产生，**不在 Engine 层 cover**（同主体附录 A.3.4 的口径）。

#### 3.6.3 多策略包并行绑定规则（R-Q9 D2）

```
LocalSim：
   adapter 在 portfolio 启动时调 broker.bind_capacity() →
   返回 max_concurrent_packages=N（N 由 LocalSim 配置决定，>=1）
   adapter 维护 (broker_instance_id, package_id, portfolio_id) 三元映射
   Engine 实例数 = 绑定的 package 数（一个 EngineSession 对应一个 package）

MiniQMTSim：
   adapter 在 portfolio 启动时调 broker.bind_capacity() →
   返回 max_concurrent_packages=1
   若已存在绑定 → 抛 BrokerBindCapacityExceededError（不静默替换）
   一个 miniQMT 仿真账户进程 ↔ 一个 EngineSession ↔ 一个 StrategyPackage
```

**LocalSim 多包细则**（R-Q9 D2 落地）：
- 每个 portfolio 创建独立 `LocalSimBroker` 实例（不共享 ledger / cash / positions）
- BrokerBackend 实例间无跨实例依赖；可在同进程内安全并发跑 N 个 portfolio
- `OrderIntent.portfolio_id` 严格区分账本边界；adapter 不允许跨 portfolio 共享 ledger 切片

**MiniQMTSim 单例细则**（R-Q9 D2 落地）：
- 进程内 `MiniQMTSimBroker` 实现 process-wide singleton：构造时检测既有实例 → 抛 `MiniQMTSingletonViolation`
- 一个 miniQMT 仿真账户进程 ↔ 一个 BrokerBackend 实例 ↔ 一个 EngineSession ↔ 一个 StrategyPackage
- 多 portfolio 共用一个 miniQMT 账户的需求**不在本期范围**（如出现，需用户单独决策；可能引入 portfolio-level allocation rule）

**portfolio 创建/激活时的兼容性校验**（与 §3.6.5 配合）：
- portfolio 绑定 BrokerBackend 时，校验 `portfolio.broker_backend.backend_id ∈ strategy_package.broker_compatibility`
- 不相容 → 抛 `BrokerCompatibilityMismatchError`（已在 §10.1）

**禁止做法**（adapter 必须显式拒绝；feedback_no_silent_errors 一致）：
- ❌ 在已绑定 MiniQMTSim 的进程内构造第二个 `MiniQMTSimBroker`（`MiniQMTSingletonViolation`）
- ❌ 把 LocalSim 多包共享的 ledger 切片混入 OrderIntent.portfolio_id
- ❌ 用 LocalSim 的 PortfolioState 喂 MiniQMTSim 的 Engine（必须用 `broker.query_positions()` 实时拉）

#### 3.6.4 行情通道强绑定撮合端（R-Q9 D3）

行情通道**不可与撮合端解耦切换**。规则编码到 `MinuteDataSource` 枚举与 backend_id 的映射：

```python
# backend/services/paper_trading_v2/market_data.py（Claude 工作面，§3.6.7 已确认）
class MinuteDataSource(StrEnum):
    TDX_REALTIME = "TDX_REALTIME"
    DB_HISTORICAL = "DB_HISTORICAL"
    MINIQMT_REALTIME = "MINIQMT_REALTIME"      # 新增枚举值（R-Q9 D3 落地）
```

| backend_id | 唯一允许的 `MinuteDataSource` 集合 |
| --- | --- |
| `local_sim` | `{TDX_REALTIME, DB_HISTORICAL}` |
| `minqmt_sim` | `{MINIQMT_REALTIME}` |
| `minqmt_live`（未来实盘） | `{MINIQMT_REALTIME}` |

**Adapter 层 init 阶段的强制校验**（R-Q9 D3 fail-fast）：

```python
ALLOWED_MARKET_SOURCES: dict[str, set[MinuteDataSource]] = {
    "local_sim":   {MinuteDataSource.TDX_REALTIME, MinuteDataSource.DB_HISTORICAL},
    "minqmt_sim":  {MinuteDataSource.MINIQMT_REALTIME},
    "minqmt_live": {MinuteDataSource.MINIQMT_REALTIME},
}

def assert_broker_market_source_match(backend: BrokerBackend, source: MinuteDataSource) -> None:
    allowed = ALLOWED_MARKET_SOURCES.get(backend.backend_id)
    if allowed is None:
        raise BrokerMarketSourceMismatchError(
            f"unknown backend_id {backend.backend_id}",
            context={"backend_id": backend.backend_id},
        )
    if source not in allowed:
        raise BrokerMarketSourceMismatchError(
            f"{backend.backend_id} requires market source in {allowed}; got {source}",
            context={"backend_id": backend.backend_id, "given_source": source.value, "allowed": [s.value for s in allowed]},
        )
```

校验时机：portfolio 启动 / live_session bootstrap / Engine `init()` — 三处都必须校验，违反即抛 `BrokerMarketSourceMismatchError`，不静默 fallback（feedback_no_silent_errors）。

**理由**（保留 v1 三条）：
1. 撮合源 + 行情源不一致会引入"价格幻觉"（信号基于行情 A、撮合按行情 B），破坏 Mode G 等价性的物理基础
2. 实盘切换路径要求 MiniQMTSim → MiniQMTLive 仅切换 trading 层，不动行情层；解耦行情会让仿真→实盘语义不连续
3. 故障可观测性：行情中断 / 撮合中断的归因清晰（同源 = 同根因）

**Adapter 实现约束**：
- BrokerBackend 实例化时**绑定**对应行情通道；`market_data_channel()` 返回的 channel 不接受 hot-swap
- 若需改用其他行情源（研究用）→ 必须新建独立 SimMode 类型 + 独立 backend_id + 独立 BROKER_COMPATIBLE 标签（见 §3.6.5），不允许在现有 backend 内偷换

**Engine 端不变**：Engine 不直接消费行情通道；行情仅在 adapter 上游产 ScoreFrame + adapter 撮合层使用。Engine 仅在 DecisionTrace.diagnostics 中记录 backend_id 以便 audit。

**MinuteDataSource 枚举扩展归属**：`backend/services/paper_trading_v2/market_data.py` 在 Claude Code 工作面（per `docs/codex_project_memory.md` line 944：paper_trading_v2 plus Paper v2 tests/docs，no QE shared implementation files）。**因此本字段扩展不开 OPEN-EXT-3**（详见 §17.4 修订）。

#### 3.6.5 StrategyPackage 加 BROKER_COMPATIBLE 字段（R-Q9 D4）

主体设计 v1 的 StrategyPackage v2 manifest 没有"哪些 broker backend 兼容"的声明；现 LocalSim / MiniQMTSim 形态二分后，需显式标注。

**新增 manifest 字段**（schema additive，按主体附录 A.4.4 双 PR 模式）：

```yaml
broker_compatible:
  type: string
  enum: ["LocalSim_only", "MiniQMTSim_only", "both"]
  default: "both"
  description: |
    Capability declaration of this StrategyPackage with respect to
    BrokerBackend kinds. Set to *_only when the package depends on
    backend-specific properties:
      - "LocalSim_only":   relies on synchronous in-process matching timing,
                           or DB_HISTORICAL replay (CATCHUP_THEN_LIVE)
      - "MiniQMTSim_only": relies on real miniQMT slippage / fill semantics,
                           or production-account-like account constraints
      - "both":            validated under both sim modes (default for new
                           packages once Mode G localsim-vs-minqmtsim case
                           passes)
    LEGACY packages default to "LocalSim_only" on migration (validated only
    against current paper_trading_v2 stack; not auto-granted MiniQMTSim
    compatibility).
```

**字段语义**：
- 默认 `"both"`（新包必须验证 Mode G 的 `engine_modeg_localsim_vs_minqmtsim_orderintents` 用例后才能保留默认）
- 标 `LocalSim_only` 或 `MiniQMTSim_only` 必须 PR 描述说明依赖的具体后端特性，并在 Mode G 测试中**仅跑对应分支**（不能跑跨 backend 等价性）
- LEGACY_NON_ST_PIT 4 个老包迁移时默认 `"LocalSim_only"`，不自动获得 MiniQMTSim 兼容性
- adapter 在 EngineSession.init 前校验：portfolio 绑定的 backend_id 与 `broker_compatible` 兼容；违反抛 `BrokerCompatibilityMismatchError`
  - 兼容性矩阵：`LocalSim_only` → 仅 `local_sim`；`MiniQMTSim_only` → 仅 `minqmt_sim` / `minqmt_live`；`both` → 任一 backend
- 实盘 `minqmt_live` 准入由主体 §11 流程定义；Engine 设计仅定义字段与校验，不定义晋级规则

**与 audit §8.1（配置冻结边界）的正交关系**：

audit §8.1 讨论的是 **运行时配置（topk / n_drop / risk_policy 等）** 的冻结边界（A 保留 / B 极简 / C 软合约）。本字段属于完全不同维度：

| 维度 | audit §8.1 | broker_compatible |
| --- | --- | --- |
| 关注对象 | 运行时配置（用户可改的旋钮） | 包能力声明（运行环境兼容性） |
| 是否 runtime 可调 | 视 §8.1 选项决定 | **永不可调**（与模型权重等同保护） |
| 写入时机 | 包创建 / runtime variant 时 | 包 freeze 时（audit 跑通后） |
| 冻结性质 | 硬冻结（A）/ 软合约（C）二选一 | 永远硬冻结（运行时不可改） |

**结论**：`broker_compatible` 是**能力声明类字段，必冻结但不阻断 runtime 自由配置**。无论 §8.1 选 A/B/C，本字段都属于"模型/因子之外但仍必须冻结"的封闭子集（与 `frozen_alpha_core` 一类，但语义维度不同）。本字段引入**不依赖** §8.1 决策。

**Engine 内消费点**：
1. init() 阶段做 broker_compatible 与 portfolio.broker.backend_id 校验（Engine 直接做，不留给 adapter — 这是决策侧 invariant）
2. DecisionTrace.inputs_digest 折入 `spec.broker_compatible`（影响等价性比对）
3. 不允许 runtime overlay 修改 broker_compatible（与 frozen alpha core 等同保护）

**Schema 升级路径**（与 Codex 主体协调，OPEN-EXT-3 范围）：
- 本 Engine 设计**仅声明字段需求**；实际 manifest schema 修订由 Codex 主体设计在 §5.4 / §A.4.4 双 PR 模式中加
- 在 Codex schema 升级落地前，Engine 实施期可在 `StrategySpec` 中先用 `custom_extension.broker_compatible` 占位（仅 audit，与 R-Q2 一致）；schema 正式 promote 后切到一等公民字段
- 双 PR 流程：Codex 端先加 optional 默认 `"both"` → Engine 端加 reader（LEGACY 默认 `"LocalSim_only"`）→ 切默认产出 → 全包重 freeze

#### 3.6.6 与 Mode G 的整合

新增等价性测试用例（追加到 §11 Mode G 自测最小集）：

- `engine_modeg_localsim_vs_minqmtsim_orderintents` — 同 (manifest, scores, portfolio, seed)，LocalSim adapter 与 MiniQMTSim adapter 输出 OrderIntent **byte-equal**（仅决策侧；NAV 不比）
- `engine_modeg_multi_package_localsim_isolation` — LocalSim 同时绑定 2 个 package，每包 OrderIntent 与单独跑该包结果一致（验证 portfolio_id 切片正确）
- `engine_modeg_minqmt_capacity_reject` — 已绑定 MiniQMTSim 的 portfolio 注入第二包 → adapter 抛 BrokerBindCapacityExceededError（验证 D2 强制）
- `engine_modeg_broker_compat_reject` — 包 broker_compatibility=["local_sim"] 但 portfolio 绑 minqmt_sim → engine.init() 抛 BrokerCompatibilityMismatchError

#### 3.6.7 不在本节范围

- LocalSim / MiniQMTSim 撮合算法实现细节（adapter 实施期）
- vnpy_xt 集成具体代码（task #4 vn.py PoC）
- miniQMT 仿真账户配置（env-poc teammate task #2 范围）
- 实盘 backend MiniQMTLive 的准入流程（主体 §11 + 用户后续授权）
- 多策略包之间的资金 / 仓位组合（属于 portfolio 层；不在 Engine 决策内核）

---

## 4. 内部决策流水线（Engine 内部模块）

承接主体附录 A.3.2 的 6 个模块，明确顺序与责任边界：

```
ScoreFrame
   │
   ▼
[1] score_to_candidates              <- selection_center.risk_policy + tradability
   │   - apply tradability filter (suspend_d / limit / pre_close)
   │   - apply sector_blacklist + risk_policy
   │   - sort by score desc, take topk
   │   - emit SelectionCandidate[]
   ▼
[2] (multi_alpha only) compute_score_combination   <- §A.6.1 future
   │   - rule ∈ {weighted_sum, rank_aggregation, meta_learner}
   │   - default Engine raises UnsupportedFeatureError until enabled
   ▼
[3] compute_weights                  <- runtime.py:602-664 重构
   │   - method ∈ {softmax, equal, rank, linear}
   │   - apply min_weight / max_weight clamp w/ redistribute (10 iters)
   │   - apply max_position_ratio
   ▼
[4] apply_dynamic_ndrop              <- runtime.py:551-600 重构
   │   - threshold_method ∈ {fixed, percentile, adaptive}
   │   - bound by [min_n_drop, max_n_drop]
   ▼
[5] apply_hold_thresh                <- runtime.py:667-679 重构
   │   - block sell if (trade_date - lot.trade_date).days < hold_thresh
   │   - re-flow blocked sells back; downstream may keep position
   ▼
[6] targets_to_intents               <- day_runner diff logic
   │   - diff target_positions vs portfolio.positions
   │   - emit OrderIntent[] (buy / sell)
   │   - attach DecisionTrace
   ▼
OrderIntentBatch
```

每个 step 在 `DecisionTrace.pipeline_steps` 写入：input_digest / output_digest / params_digest / wall_time_us。Mode G 等价性比对的依据是这条链。

---

## 5. 与 strategy_package/runtime.py 现有边界的对接

### 5.1 现状

`backend/services/strategy_package/runtime.py` 当前承担两件事：
1. `StrategyPackageRuntime.build_signal_snapshot` — 加载 score artifact + HMM 调整 → `SignalSnapshot`
2. `StrategyPackageRuntime` 内部 `_compute_score_weighted_weights` / `_filter_dynamic_ndrop` / `_can_sell_under_hold_thresh` — 实际决策逻辑（A.3.2 中点名重构的部分）

另外 `RebalanceEngine.build_order_intents`（runtime.py 之 line 682+）做 diff → OrderIntent。

### 5.2 改造路径（不在本文档实施，仅描述对接）

| 现有位置 | Engine 内归属 | 改造动作（实施阶段） |
| --- | --- | --- |
| `build_signal_snapshot` 上半段（score 加载 + HMM） | 留在 `runtime.py`，作为 **adapter 上游** 提供 `ScoreFrame` 给 Engine | 不动现有签名；HMM 仍属 selection_center |
| `_compute_score_weighted_weights` | Engine `compute_weights` 模块 | 函数体逐字搬入 Engine；保持算法等价 |
| `_filter_dynamic_ndrop` + `_compute_threshold` | Engine `apply_dynamic_ndrop` 模块 | 同上 |
| `_can_sell_under_hold_thresh` | Engine `apply_hold_thresh` 模块 | 同上 |
| `RebalanceEngine.build_order_intents` | Engine `targets_to_intents` 模块 | 同上 |

**对接契约**：
- Adapter 用 `StrategyPackageRuntime.build_signal_snapshot()` 产出 `SignalSnapshot` → 转 `ScoreFrame` 喂 Engine
- Engine 输出 `OrderIntentBatch` → adapter 翻译为对应执行端的下单 API

### 5.3 不做的事（避免范围蔓延）

- 不重构 `StrategyPackageRuntime.build_signal_snapshot` 本体（HMM 保持原位）
- 不动 `selection_center/inference_engine.py`
- 不改 `selection_artifact.py` / `validators.py` / `models.py` 的字段
- 不影响现有 `paper_trading_v2/` 调用方（迁移在 A.3.3 #6 单独立项）

---

## 6. 与 QE Adapter / Paper Adapter 的解耦设计

### 6.1 解耦原则

Engine 输出 `OrderIntent`（语义：我想买/卖某 symbol 多少股）；adapter 翻译为执行端语义。Engine **不知道**自己在 Qlib 还是 vnpy 中跑。

### 6.2 各 adapter 边界

| Adapter | 职责 | 与 Engine 的对接点 |
| --- | --- | --- |
| **QE Adapter** | Qlib YAML 内 `strategy.delegate` 指向 Engine；day-by-day 喂入 score → 取 intent → 提交给 Qlib `Exchange` | `decide_eod()` only；不调 `on_bar` |
| **Paper Adapter** | trading_core daemon 拉 score → 调 Engine → intent 入队 → vnpy SimGateway 撮合 | 主要 `decide_eod()`；分钟算法时调 `on_bar()` |
| **Live Adapter** | trading_core daemon 拉实时 score → Engine → intent → vnpy_xt → miniQMT | `decide_eod()` + `on_bar()` + `on_event()` |

### 6.3 Adapter 不可做的事

- 不可修改 Engine 输出的 OrderIntent（只能拒绝并报错，不能 silent rewrite）
- 不可绕过 Engine 直接生成下单（违反 G1 / Mode G）
- 不可往 Engine 注入未 frozen 的 manifest（违反 G2）

---

## 7. Master Seed 注入

### 7.1 Seed 在 Engine 中的角色

**关键认识**：Engine 本身的决策代码 **几乎不需要随机性**——`compute_weights` / `apply_dynamic_ndrop` / `apply_hold_thresh` 全部是确定性函数（对相同输入输出唯一）。Seed 主要影响**模型推理**（在 Engine 之外），而推理在 frozen-weight 模式下也已经 deterministic。

**那么 Engine 为何还要持有 SeedBundle？**

1. **审计**：DecisionTrace 必须把 seed_bundle 摘要写进 `inputs_digest`，便于 Mode G 比对 + Phase 4 L4 gate
2. **未来扩展**：若 `meta_learner` 组合规则引入 sampling、或 dropout-at-inference 引入随机性，必须从 SeedBundle 取子 seed
3. **Multi-seed ensemble**（主体 §7.2 candidate 阶段）：Engine 在 multi_alpha 启动后可能需要依据 seed_sequence 跑多次推理取均值——此时 seed 直接进入决策路径

### 7.2 注入路径

```
Codex Phase 4 产出 SeedContract 表（strategy_pkg.package.master_seed + seed_sequence JSONB）
   │
   │ Adapter 在初始化 EngineSession 时
   ▼
Adapter 从 manifest.seed_contract_ref 读取记录 → 构建 SeedBundle → 传给 engine.init()
   │
   ▼
Engine 校验：
   - seed_policy 必须 ∈ {fixed, multi_seed, random_logged, unset_legacy}
   - 若 spec.frozen_alpha_core 要求 fixed 但 SeedBundle.policy=unset_legacy → 抛 SeedContractError
   - seed_sequence 缺关键子 seed（numpy / torch / lgb 视模型 family 必备）→ 抛 SeedContractError
   - library_versions 与 frozen_alpha_core 期望差异 → 抛 SeedContractError（**不静默**）
```

### 7.3 Seed 与 ModelArtifact 的责任划分

| 关注点 | 谁负责 |
| --- | --- |
| 训练时 seed 固定 → frozen weights | Codex QE / RD-Agent |
| frozen weights 加载 + deterministic inference | adapter 上游（inference_engine.py） |
| Engine 决策路径的随机性（未来 ensemble / sampling） | Engine 取 SeedBundle.seed_sequence 中的子 seed |
| Mode G 等价性 audit 摘要 | Engine 写 DecisionTrace.inputs_digest |

### 7.3a SeedBundle 强制写入 DecisionTrace（Lead 2026-05-08 R-Q5）

无条件强制：每次 `decide_eod` / `on_bar` / `on_event` 产出 `OrderIntentBatch` 时，`DecisionTrace.seed_bundle_digest` 必填，且 `inputs_digest` 必须把 `seed_bundle_digest` 折入计算。

不允许"按需写入"或基于 `spec.seed_dependent_pipeline` 的开关——按需路径会让 Mode G 漂移监控漏帧；Phase 4 L4 gate 反正逐字节比对，多写一字段成本可忽略。

### 7.4 与 Phase 4 L4 gate 的关系

Phase 4 gate（同 manifest 同 master_seed 跑两次 → NAV diff < 0.01bp + 持仓 100% 相同）的 **决策侧** 由 Engine 负责保证。具体：

- Engine 决策路径必须 byte-deterministic given (spec, scores, portfolio, seed_bundle)
- DecisionTrace 必须 byte-equal between 两次 run
- 任何 dict 迭代顺序 / set 序列化 / float 精度差异都必须避免（用 sorted、Decimal、显式 ordering）

**Engine 自测 L4 gate**：在 Engine 单元测试矩阵中加 `engine_l4_decision_determinism_strict`——独立于 Phase 4 整体 gate。

---

## 8. Model Registry 选择

### 8.1 选择规则来源

主体 §8.2 / §8.4 / §8.5：StrategyPackage v2 的 `model_artifact_pointer` 字段不是单一 artifact_id，而是 **selection rule + filters**。Engine 在 init 时按 rule 解析为单一 `ModelArtifactHandle`。

```python
class ModelArtifactRef(BaseModel):
    """Stored in StrategyPackage v2 manifest."""
    selection_rule: Literal[
        "pinned_artifact",          # 单一 artifact_id（最常见，frozen 默认）
        "latest_promoted_in_spec",  # spec_id 下最新 promoted_artifact
        "rolling_train_pointer",    # 滚动训练自动晋级（主体 §A.6.4）
        "ensemble_set",             # 多 artifact ensemble（未来扩展）
    ]
    artifact_id: str | None         # for pinned_artifact
    spec_id: str | None             # for latest_promoted_in_spec / rolling_train_pointer
    filters: ModelArtifactFilters   # status filter / version range
```

### 8.2 Engine 内的解析步骤

```
init():
   1. 读 spec.model_artifact_pointer (= ModelArtifactRef)
   2. 调 ModelRegistryReader 接口（adapter 注入；Engine 不直接连 DB）：
        reader.resolve(ref) -> ModelArtifactHandle
   3. 校验：
      - artifact.status 必须 ∈ {promoted_artifact, paper_enabled}（除非 spec 显式允许）
      - artifact.spec_id == spec.frozen_alpha_core.model_spec_id（一致性）
      - artifact.weight_sha256 != "" 且文件存在（adapter 校验文件存在性）
      - artifact.feature_order_sha256 与 spec.frozen_alpha_core.feature_schema_sha256 一致
   4. 任一不满足 → raise ModelArtifactMismatchError（**不 fallback 到 next candidate**）
   5. 多 artifact 命中（latest_promoted_in_spec 应只返回 1 个）→ 抛错；不静默选第一个
```

### 8.3 Engine 不做模型推理

Engine 持有 `ModelArtifactHandle` 仅用于：
- 把 handle.artifact_id 写入 DecisionTrace（audit）
- 校验 score 来源一致性（ScoreFrame 应注明产出 score 的 artifact_id；与 handle 不一致 → 抛错）

**实际推理**（factor → score）由 adapter 上游 `inference_engine.py` / QE artifact 负责。Engine 收到的 `ScoreFrame` 已经是模型输出。

### 8.4 多模型 / ensemble 的未来扩展

主体 §A.6.4 滚动训练 + §A.6.1 多 alpha 启动后，可能出现 `selection_rule = "ensemble_set"`：

- Engine 在 init 阶段拿到 list[ModelArtifactHandle]
- Engine 调 adapter 注入的 inference callable，分别取每个 artifact 的 score → 按 ensemble rule 合成 → 进入 score_to_candidates
- 这是**唯一**需要 Engine 内部触发推理的场景（且仍由 adapter 注入推理 callable，Engine 不直连模型权重）

本期（单 alpha + frozen weight）**不实现** ensemble 路径；占位接口预留即可。

---

## 9. Versioning 与回退

### 9.1 三层版本号

| 版本对象 | 标识 | 谁分配 |
| --- | --- | --- |
| StrategyPackage v2 manifest | `manifest_sha256` | Codex（Phase 2 资产冻结时） |
| Runtime variant | `runtime_variant_id` + `runtime_variant_hash` | Codex（主体 §5.3 / §12） |
| Engine 自身代码 | `engine_version` (semver: e.g. `1.0.0`) | Engine 仓库（claude/* 分支） |

`OrderIntentBatch.engine_version` + `decision_trace.inputs_digest` 共同唯一确定一次决策。

### 9.2 Engine 与 manifest 版本兼容性矩阵

```
Engine 1.x ↔ Manifest v2.x（Codex Phase 2-7 后的稳定 schema）
   - 兼容契约：Engine 1.x 必须能读所有 manifest v2.minor（向后兼容）
   - manifest 加新字段（schema 升级）→ 走主体 §A.4.4 双 PR 模式
       - PR 1: Codex 加 v2.next 字段（optional）
       - PR 2: Engine 加 v2.next reader（默认 fallback 到 v2.cur 行为）
   - 不允许 Engine 1.x 拒绝读 v2.cur manifest

Engine 2.x（major bump）= breaking change
   - 触发条件：决策语义不兼容修改（如 weights 算法语义变更）
   - 必须经过 Mode G 验证 + 主体附录 A.4.3 类别 C 硬约束
```

### 9.3 回退策略

| 故障类型 | 回退动作 |
| --- | --- |
| Engine 升级后 Mode G 等价性失败 | adapter 端切回上一版 Engine（adapter 配置 `engine_version_pin`） |
| manifest schema v2.next 出问题 | Codex 端 `seed_policy=unset_legacy` 标记；Engine 拒绝读 v2.next，退回 v2.cur reader |
| ModelArtifact 选择器错选了未验证的 artifact | Engine init 阶段抛 `ModelArtifactMismatchError`；adapter 不允许 retry 替换 artifact，必须人工介入 |
| 决策路径 nondeterminism 暴露（Phase 4 L4 失败） | Engine PR 不合 main；先在 claude/* 分支修复至 byte-equal |

### 9.4 不允许的回退动作

- **不允许** Engine 内 silent fallback 到默认权重算法（违反 G3）
- **不允许** ModelArtifact 找不到时 Engine 自动选 spec 内"最近一个"——必须显式抛错
- **不允许** SeedContract 缺字段时 Engine 用 random.SystemRandom() 填补——必须抛错

---

## 10. 错误传播与禁止 silent fallback

按 user `feedback_no_silent_errors`：错误必须传播。

### 10.1 Engine 错误层级

```python
class StrategyEngineError(Exception): ...

class StrategySpecValidationError(StrategyEngineError):
    """spec 字段缺失 / 不一致 / overlay 越权"""

class SeedContractError(StrategyEngineError):
    """seed_policy 不匹配 / 子 seed 缺失 / library_versions 漂移"""

class ModelArtifactMismatchError(StrategyEngineError):
    """artifact status 不允许 / spec_id 不一致 / hash 不一致"""

class ScoreFrameMismatchError(StrategyEngineError):
    """ScoreFrame.manifest_sha256 != spec.manifest_sha256
       / source artifact_id 不在 spec 允许列表"""

class DecisionPipelineError(StrategyEngineError):
    """pipeline 内部数学异常（NaN / inf / 不可解算）"""

class UnsupportedFeatureError(StrategyEngineError):
    """multi_alpha / ensemble / on_event 未启用时被调用"""

class BrokerCompatibilityMismatchError(StrategyEngineError):
    """portfolio.broker.backend_id incompatible with spec.broker_compatible (R-Q9 D4)"""

class BrokerBindCapacityExceededError(StrategyEngineError):
    """attempt to bind extra package on a single-binding backend
       (e.g. MiniQMTSim already bound; R-Q9 D2)"""

class MiniQMTSingletonViolation(StrategyEngineError):
    """attempt to construct a second MiniQMTSimBroker in the same process
       (R-Q9 D2 — process-wide singleton invariant)"""

class BrokerMarketSourceMismatchError(StrategyEngineError):
    """MinuteDataSource not in ALLOWED_MARKET_SOURCES[backend_id]
       (R-Q9 D3 — market channel strongly bound to broker)"""

# Adapter-side broker errors (NOT raised by Engine; raised by BrokerBackend
# implementations and propagated up through adapter; documented here for
# completeness so adapter authors do not invent new shapes)
class BrokerSubmitError(StrategyEngineError):
    """submit_order_intent() failed before reaching backend (validation)"""

class BrokerRejectedError(StrategyEngineError):
    """backend (LocalSim ledger / miniQMT) rejected the order
       — e.g. capital limit / suspended / limit-up violation"""

class BrokerConnectivityError(StrategyEngineError):
    """backend session lost (miniQMT service crash / xtquant disconnect)
       — adapter MUST surface; never silently retry"""
```

### 10.2 禁止做法

- ❌ `try: ... except: pass`
- ❌ `weights = [0] * n if compute_failed else weights`
- ❌ overlay 不识别字段时 `setattr` 静默忽略
- ❌ ScoreFrame 缺 symbol 时用 0 / NaN 填补
- ❌ ModelArtifact 不存在时选"最像的那个"

### 10.3 强制做法

- ✅ 任何 validation 失败 → 立即 raise，附 context dict（package_id / trade_date / 字段名 / 期望 vs 实际）
- ✅ DecisionTrace 必须含完整 step 失败点（最后一个成功 step + 失败 step name）
- ✅ adapter 调 Engine 抛错时不允许 catch 后 fallback 决策；必须把错误向上传到 trading_core 错误系统

---

## 11. 与 Validation Modes A-F + Mode G 的整合

主体 §6.2 + 附录 A.3.4：

| Mode | Engine 角色 |
| --- | --- |
| **Mode A** Original-config retest | Engine 跑一次 → 跟 QE archive 的 NAV 比 |
| **Mode B-F** 各 retest 模式 | Engine 行为一致；只是 adapter 喂的 score / clock / overlay 不同 |
| **Mode G** Cross-Adapter Equivalence（**Engine 层硬 gate** — 由本文档单方面声明；待 Codex 主体 §6 正式纳入，外部决策项 OPEN-EXT-1） | 同 (spec, scores, portfolio, seed)，三 adapter 必产 byte-equal OrderIntent |

**Mode G 自测最小集**（实施时写入 `tests/aistock_validation/modules/strategy_engine_modeg.md`）：

1. `engine_modeg_smoke_lgb_single_alpha` — 1 个 LGB single_alpha package + 1 日固定 score → QE adapter / Paper adapter 输出 OrderIntent 列表 byte-equal
2. `engine_modeg_with_dynamic_ndrop` — 含 dynamic_ndrop + threshold_method=adaptive 的 package
3. `engine_modeg_with_hold_thresh` — 含 hold_thresh=5 的 package + portfolio 含未达 thresh 的 lots
4. `engine_modeg_overlay_topk` — 同 manifest 不同 runtime variant（topk 改动）→ 三 adapter 仍 byte-equal
5. `engine_l4_decision_determinism_strict` — 同输入跑两次（同 process / 跨 process）→ DecisionTrace + OrderIntent byte-equal
6. `engine_modeg_localsim_vs_minqmtsim_orderintents` — 同 (manifest, scores, portfolio, seed)，LocalSim adapter 与 MiniQMTSim adapter 输出 OrderIntent byte-equal（决策侧；NAV 不比；R-Q9 §3.6.6）
7. `engine_modeg_multi_package_localsim_isolation` — LocalSim 同时绑定 2 个 package，每包 OrderIntent 与单独跑该包结果一致（R-Q9 D2）
8. `engine_modeg_minqmt_capacity_reject` — 已绑定 MiniQMTSim 的 portfolio 注入第二包 → BrokerBindCapacityExceededError（R-Q9 D2 强制）
9. `engine_modeg_broker_compat_reject` — 包 broker_compatibility=["local_sim"] 但 portfolio 绑 minqmt_sim → BrokerCompatibilityMismatchError（R-Q9 D4 强制）

测试矩阵详细设计**不在本文档**（按 task #6 / #11 范围）；占位以便实施阶段填入。

---

## 12. 实施依赖与优先级（仅引用，不本期做）

主体附录 A.3.3 七个交付物，与主体 Phase 编号衔接：

| # | 交付物 | 依赖 |
| --- | --- | --- |
| 1 | StrategySpec 接口定义（本文档 §3） | 主体 Phase 5 完成 |
| 2 | Engine 核心实现 | 主体 Phase 4（Master Seed Contract）完成 — **解锁条件** |
| 3 | QE Adapter | 主体 Phase 5（Model Library） |
| 4 | Paper Adapter | vn.py PoC（task #4） |
| 5 | Live Adapter | 实盘 PoC |
| 6 | runtime.py 迁移 | #2 + #3 + #4 完成 |
| 7 | Equivalence 测试矩阵 Mode G | #6 完成 |

**本文档（task #6）= 交付物 #1 的纸面版**。实施代码不动。

---

## 13. 与 Codex 主体设计的引用 / 不冲突点

| Codex 主体章节 | 本文档对接方式 | 是否扩展 |
| --- | --- | --- |
| §5.1 不可变 Alpha Core | Engine 仅读 `FrozenAlphaCore`，绝不修改 | 否 |
| §5.2 BaselineRuntime | Engine `init` 接受 `BaselineRuntime` + overlay | 否 |
| §5.3 Variant 规则 | Engine 强制 overlay 仅含允许字段；越权抛错 | 否 |
| §6 Validation Modes A-F | Engine 行为对所有 mode 一致；新增 Mode G 由 Engine 提供 | **新增 Mode G**（仅作建议，待 Lead 确认） |
| §7.3 Seed Contract | Engine 通过 `SeedBundle` 消费；不增字段 | 否 |
| §8.2 四层模型身份 | Engine 通过 `ModelArtifactHandle` 消费 spec / artifact 关联 | 否 |
| §A.6.5 custom_extension escape hatch | Engine **仅 audit**（写入 DecisionTrace），**禁止** extension_handler 注册机制（Lead 2026-05-08 R-Q2）。如未来证明需要执行式 extension_handler，必须走 **Codex 主体设计修订 + 全套 Mode A-G 回归** | 否 |
| §5.4 建议增强字段 | 本文档 §3.6.5 提议在 manifest 加 `broker_compatibility: list[str]` 字段（R-Q9 D4）。schema 落地走主体附录 A.4.4 双 PR 模式：Codex 端先加 optional → Engine 端加 reader（LEGACY 默认 `["local_sim"]`）→ 切默认产出 → 全包重 freeze。 | **是**（R-Q9 D4） |
| §11 Paper / 实盘准入门槛 | 本文档 §3.6 引入 BrokerBackend 抽象 + 两种 SimMode（R-Q9 D1）；行情通道强绑定撮合端（R-Q9 D3）；MiniQMTLive backend 准入由主体 §11 定义流程。Engine 仅定义字段与 init 校验。 | **是**（R-Q9 D1/D3） |

**冲突评估**：本设计未发现与 Codex 主体设计的字段语义冲突；Engine 是 **消费方**，所有契约定义在 Codex 端。

---

## 14. 边界与禁止事项（自我约束）

为避免本设计后续实施时蔓延：

- ❌ Engine **不**直连 DB（model_registry / strategy_pkg.package）；adapter 注入 reader 接口
- ❌ Engine **不**含 IO（除 logging）；不读文件、不发 HTTP
- ❌ Engine **不**发起任何撮合 / 订单状态变更
- ❌ Engine **不**触发模型训练 / artifact 创建
- ❌ Engine **不**修改 manifest / package 记录
- ❌ Engine **不**复用 Qlib / vnpy 类型（输入输出全部 Pydantic dataclass）
- ✅ Engine 可通过 adapter 注入的 `inference_callable`（未来 ensemble 用）调推理；该 callable 的实现在 adapter 侧

---

## 15. 开放问题清单（已由 Lead 在 2026-05-08 裁决，详见 §17）

> **注**：本节保留原始问题陈述以便审计。每条问题的最终裁决见 §17。

按重要性排序：

### Q1（高）：是否新增 Mode G 作为 Engine 层硬 gate？

- **背景**：主体附录 A.3.4 把 Mode G 作为"建议"，未在主体 §6 测试模式中正式列出
- **影响**：若不强制 Mode G，则 Engine + 3 adapter 漂移监控失去自动化抓手；类别 C 修改仍需双工
- **建议**：作为 Engine 实施 PR 合 main 的硬 gate 写入 `tests/aistock_validation/modules/strategy_engine_modeg.md`
- **需 Lead 决策**：是 / 否；若是，是否需要 Codex 在主体 §6 加正式 Mode G 章节（双 PR 模式）

### Q2（高）：custom_extension 字段在 Engine 内的语义

- **背景**：主体 §A.6.5 提议 manifest 加 `custom_extension`（escape hatch），但未规定 Engine 如何消费
- **分歧**：
  - 选 A：Engine 完全忽略 custom_extension（仅 audit）；任何使用必须升级为 first-class 字段
  - 选 B：Engine 提供 `extension_handler` 注册机制，allow-list 模式
- **建议**：选 A（保守），避免 Engine 成为 hidden config 传染源
- **需 Lead 决策**

### Q3（中）：on_bar 接口本期是否要定义

- **背景**：本文档为分钟级执行（v24/v25/v26）预留 `on_bar` hook，但短期内 Paper v2 daily rebalance 用不到
- **分歧**：
  - 选 A：本期定义占位接口（默认 None），实施时再填充
  - 选 B：本期不定义，等 minute execution 整合时再加（避免过度设计）
- **建议**：选 A（接口稳定优于后期 schema 改动）；on_bar 在 Engine 1.0 默认返回 None
- **需 Lead 决策**

### Q4（中）：on_event 与 §A.6.2 公告信号的关系

- **背景**：用户路线图 #5 公告 / 财报独立信号；主体 §A.6.2 提议 EventSignalComponent
- **分歧**：on_event 是"Engine 决策时合入事件 score" vs "事件触发独立 OrderIntent"
- **建议**：本期 on_event 定义为后者（事件触发独立调整）；前者通过 ScoreFrame 在 adapter 上游合成 score 后喂 Engine
- **需 Lead 决策 + 与 announcement_event_risk_signal_top_level_design.md 协调**

### Q5（中）：SeedBundle 在 Engine 决策路径的最小消费量

- **背景**：§7.1 指出 Engine 决策本身几乎不需要 seed；主要为 audit + 未来扩展
- **分歧**：
  - 选 A：Engine 必须把 seed_bundle.master_seed 写入 DecisionTrace（强制 audit）
  - 选 B：仅当 spec 显式声明 seed_dependent_pipeline=true 时才写入
- **建议**：选 A（Phase 4 L4 gate 反正要逐字节比对，多写一个字段成本低）
- **需 Lead 决策**

### Q6（低）：runtime_variant 哪些字段必须由 Engine 强制 overlay 校验

- **背景**：§3.2 的 RuntimeOverlay 列了允许字段；主体 §5.3 列出 variant 规则但字段集略不同
- **分歧**：是否 Engine 内 hardcode allow-list，还是从主体设计的 `package_runtime_variant` schema 自动派生
- **建议**：从 schema 派生（Codex 端是 source of truth）；Engine 在 init 阶段读 schema descriptor
- **需 Lead 决策 + 与 Codex Phase 6 衔接**

### Q7（低）：DecisionTrace 详细程度

- **背景**：每个 pipeline step 的 `params_digest` / `output_digest` 详细到何种粒度，影响 Mode G 比对精度与存储
- **分歧**：
  - 选 A：每 step 全 hash（最精细，但 trace 大）
  - 选 B：仅顶层 inputs/outputs hash + final intents（trace 小，调试时不够细）
  - 选 C：可配置（baseline 选 B，调试时切 A）
- **建议**：选 C
- **需 Lead 决策**

### Q8（低）：Engine 单元测试的 fixture 数据来源

- **背景**：Engine 实施期需要可重复 fixture（manifest + scores + portfolio）
- **分歧**：用现有 4 个 LEGACY_NON_ST_PIT manifest 还是 Codex Phase 1 后的新 dev manifest
- **建议**：等 Codex Phase 4 完成后用 dev manifest（带 seed_contract）；本期不实施，无紧迫性
- **需 Lead 决策（可推迟）**

---

## 16. 一句话核心

**Strategy Engine = plain Python 决策内核**，输入 (frozen StrategyPackage v2 spec, ScoreFrame, PortfolioState, SeedBundle, ModelArtifactHandle, RuntimeOverlay)，输出 (OrderIntentBatch + DecisionTrace)；不感知执行端、不连 DB、不做推理；**绝不静默 fallback**；**Mode G byte-equal** 由本文档单方面声明为 Engine 合 main 硬 gate（待 Codex 主体 §6 正式纳入，外部决策项）；**实施依赖 Codex Phase 4 完成**。

---

## 17. Lead 拍板与外部决策项（2026-05-08）

本节固化 §15 的 8 个开放问题在 2026-05-08 的裁决结果。后续 Engine 实施 PR 必须以本节结论为准；§15 仅作历史审计保留。

### 17.1 已采纳（Engine 设计内部生效，无需外部协调）

| 编号 | 议题 | 裁决 | 文档落地点 |
| --- | --- | --- | --- |
| **R-Q2** | custom_extension 在 Engine 内的语义 | **保守方案**：Engine 仅 audit（写入 DecisionTrace），**禁止** extension_handler 注册机制。理由：与 `feedback_no_silent_errors` 一致；hidden config 传染风险高于 escape hatch 价值。 | §3.2 / §13 |
| **R-Q3** | on_bar 接口本期是否定义 | **批准占位**：本期定义签名 + 默认返回 None；v24/v25/v26 minute algo 实施时由对应 algo 子类实现。 | §3.1 |
| **R-Q5** | SeedBundle 是否强制写入 DecisionTrace | **强制**：Phase 4 L4 gate 逐字节比对反正要这份数据；按需写入会让 Mode G 漂移监控漏帧。 | §7.1 / §7.4 |
| **R-Q6** | RuntimeOverlay allow-list 来源 | **派生**：allow-list 派生自 Codex `package_runtime_variant` schema（source of truth），Engine 不硬编码字段。实施依赖 Codex Phase 6 合入集成分支。 | §3.2 / §13 |
| **R-Q9** | BrokerBackend 抽象 + 两种 SimMode 二分（来源：`paper_v2_blockers_20260508.md` P0-H） | **4 项决策全采纳**（A/A/A/是，用户授权 2026-05-08）：D1 引入 `BrokerBackend` 抽象（`submit_order_intent` / `cancel` / `query_status` / `subscribe_fill_callback` / `query_account` / `query_positions`），落地 `LocalSimBroker` 与 `MiniQMTSimBroker` 两个 backend；D2 LocalSim 每 portfolio 独立 BrokerBackend 实例支持多包并行，MiniQMTSim 进程内单例（违反抛 `MiniQMTSingletonViolation`）；D3 行情通道**强绑定**撮合端 — `MinuteDataSource` 枚举新增 `MINIQMT_REALTIME`，跨配抛 `BrokerMarketSourceMismatchError` fail-fast；D4 StrategyPackage v2 manifest 加 `broker_compatible: Literal["LocalSim_only", "MiniQMTSim_only", "both"]` 字段（默认 `"both"`，LEGACY 默认 `"LocalSim_only"`），schema additive 走双 PR 模式协调 Codex（OPEN-EXT-3）。`broker_compatible` 与 audit §8.1 配置冻结边界**正交**——属能力声明类（必冻结）而非运行时配置。 | §3.6.1-3.6.7 / §10.1 / §11 / §13 |

### 17.2 已采纳但推迟到实施期（无紧迫性）

| 编号 | 议题 | 裁决 | 占位说明 |
| --- | --- | --- | --- |
| **R-Q7** | DecisionTrace 粒度是否可配置 | **粒度策略 v0**：固定全粒度（每 step 全 hash）；**v1** 视实施期性能数据决定是否引入 baseline/调试两档。 | §3.5 / §11 |
| **R-Q8** | Engine 单元测试 fixture 数据来源 | **推迟**：等 Mode A 实施时由 cross-test 框架与 fixture 库共同确定。 | §11 / §12 |

### 17.3 半采纳（Engine 单方面声明 + 标记为外部决策项）

| 编号 | 议题 | Engine 端裁决 | 外部协调项 |
| --- | --- | --- | --- |
| **R-Q1** | Mode G 作为合 main 硬 gate | **本文档单方面声明** Mode G 为 Engine 合 main 硬 gate（Class C 漂移自动化抓手不能放弃）。 | **是否推 Codex 在主体 §6 正式纳入 Mode G**（双 PR 模式）→ 待用户后续单独授权；本文档**不**替 Codex 做主体 §6 修订决定。 |
| **R-Q4** | on_event 与公告信号设计的语义对齐 | **本文档采纳**：on_event 语义为"独立触发 OrderIntent 调整"（不混入 score）。 | **on_event 输入 schema 字段对齐 `announcement_event_risk_signal_top_level_design.md`** → 跨工作面拉通；待用户后续单独授权。 |

### 17.4 外部决策项汇总（需用户后续授权才能跨文档协调）

下列两项**必须**由用户单独授权后才能跨工作面推进；本 session / 本文档不代为决定：

1. **OPEN-EXT-1**（来源 R-Q1）：是否走双 PR 模式推 Codex 主体设计 §6 正式纳入 Mode G（Codex 端文档修订 + Engine 端 reader 协调）。在用户授权前，Engine 文档单方面声明仍然生效（不影响 Engine 合 main gate 强制力）。
2. **OPEN-EXT-2**（来源 R-Q4）：on_event 输入 schema 与 `announcement_event_risk_signal_top_level_design.md` 的字段对齐方案。在对齐前，Engine 实施期保持 on_event 默认 no-op（与 R-Q3 占位策略对应）。
3. **OPEN-EXT-3**（来源 R-Q9 D4）：StrategyPackage v2 manifest 加 `broker_compatible: Literal["LocalSim_only", "MiniQMTSim_only", "both"]` 字段 — 必须走 Codex 主体附录 A.4.4 双 PR 模式（Codex 端 schema additive 默认 `"both"` + Engine 端 reader（LEGACY 默认 `"LocalSim_only"`） + 切默认产出 + 全包重 freeze）。在 Codex schema 落地前，Engine 实施期可在 `StrategySpec.custom_extension.broker_compatible` 占位（与 R-Q2 audit-only 语义一致）；正式 promote 后切一等公民字段。
   **范围说明**：`MinuteDataSource` 枚举扩展（`MINIQMT_REALTIME`）位于 `backend/services/paper_trading_v2/market_data.py`，属 Claude Code 工作面（codex_project_memory line 944），**不在 OPEN-EXT-3 内**；仅 manifest schema 字段需要跨工作面协调。

### 17.5 与本文档其他节的一致性更新清单

实施时按下列清单核对（防止 §17 裁决与文档其他节不一致）：

- §3.1 `on_bar` / `on_event` docstring 已为占位（与 R-Q3 / R-Q4 一致）✓
- §3.2 RuntimeOverlay 字段集声明为"来自 Codex schema 派生"占位（与 R-Q6 一致）✓
- §3.5 DecisionTrace 注明"v0 固定全粒度"（与 R-Q7 一致，本节落地）✓
- §7.1 / §7.4 SeedBundle 强制写入 DecisionTrace（与 R-Q5 一致）✓
- §11 Mode G 为合 main 硬 gate 写法保持，附注"待 Codex 主体 §6 正式纳入"（与 R-Q1 一致）✓
- §13 custom_extension 行明确"Engine 仅 audit；如需 extension_handler 走 Codex 主体设计修订 + 全套 Mode A-G 回归"（与 R-Q2 一致）✓
- §3.6 BrokerBackend 抽象 + 两种 SimMode 对照（R-Q9 D1/D2/D3/D4 全采纳）✓
- §10.1 新增 `BrokerCompatibilityMismatchError` / `BrokerBindCapacityExceededError`（R-Q9 D2/D4 错误传播）✓
- §11 Mode G 自测最小集追加 4 条 broker 维度用例（R-Q9 §3.6.6）✓
- §13 加两行：§5.4 加字段 / §11 准入门槛对接（R-Q9 D4 / D1+D3）✓
- `broker_compatibility` 字段引入需走主体附录 A.4.4 双 PR 模式 → 标记为外部协调项 OPEN-EXT-3（见下）

---

## 附录 A：与 main 上现有 runtime.py 函数级映射

| runtime.py 函数 | 行号 | Engine 模块 | 改造类型 |
| --- | --- | --- | --- |
| `StrategyPackageRuntime.build_signal_snapshot` | 49-107 | adapter 上游（**留在 runtime.py**） | 保留 |
| `_load_score_rows` | 109+ | adapter 上游 | 保留 |
| `_filter_dynamic_ndrop` | 551-584 | Engine `apply_dynamic_ndrop` | 搬入 |
| `_compute_threshold` | 586-600 | Engine `apply_dynamic_ndrop` 内嵌 | 搬入 |
| `_compute_score_weighted_weights` | 602-664 | Engine `compute_weights` | 搬入 |
| `_can_sell_under_hold_thresh` | 666-679 | Engine `apply_hold_thresh` | 搬入 |
| `RebalanceEngine.build_order_intents` | 682+ | Engine `targets_to_intents` | 搬入 |

迁移在交付物 #6（runtime.py 改造为调用 Engine）一次性完成。

---

## 附录 B：本文档不涵盖的事

- 实际 Python 代码（仅伪代码 / 类型签名）
- 测试矩阵的详细 case 设计（仅列出 Mode G 自测最小集名）
- DB schema（Engine 不连 DB）
- vn.py / vnpy_xt 集成（adapter 实施期 task #4）
- Paper v2 阻断点（task #5 范围）
- Cross-test 自动路由（task #6 框架范围）

**本文档仅交付**：Engine 接口契约 + 内部模块顺序 + 与 Codex 主体的对接边界 + 开放问题清单。

---

**End of design document**.
