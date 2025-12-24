# 数据服务层实现设计（AIstock 本地 · 方案 C 混合模式）

> 本文基于《2025-12-24_DataServiceLayer_Detail_Design_RD-Agent_AIstock.md》的对上接口与视图设计，
> 结合 AIstock 现有 TimescaleDB / xtquant / miniQMT 能力，给出当前阶段在 AIstock 仓库中的实现规划。
> 重点聚焦“本地策略运行 + 模拟盘/实盘”场景，暂不替换现有 TimescaleDB → HDF5/qlib 导出链路。

---

## 1. 范围与前提

### 1.1 范围

- **仅股票资产**，不考虑期货/期权等多资产扩展。
- 不修改、不替换现有：
  - TimescaleDB 作为 AIstock DB 的底层实现；
  - 从 TimescaleDB 导出 HDF5 / qlib bin 供 RD-Agent/Qlib 使用的离线链路。
- 新增一个 **数据服务层内部模块**，只在 AIstock 仓库内使用：
  - 短期不接入现有消费方（旧策略/模拟盘/实盘），不改变现有行为；
  - 优先实现“最终想要”的形态与接口，作为未来接入的基础。

### 1.2 技术前提

- 底层数据源：
  - **xtquant（miniQMT 客户端）**：
    - 提供历史 K 线、实时 snapshot、盘口、订阅推送等；
    - 已有文档：`xtquant_dataset_catalog.md` / `xtquant_realtime_quote_guide.md` / `xtquant_miniqmt_integration_memo.md`。
  - **miniQMT**：
    - 作为交易通道，提供下单、账户/持仓、订单/成交等能力；
    - 与 xtquant 一起构成本地交易与行情环境。
  - **TimescaleDB**：
    - 作为 AIstock 全量历史数据的统一存储；
    - 当前导出链路已覆盖 RD-Agent/Qlib 研究所需，无需在本设计中重做。

- 当前阶段只在 **AIstock 进程内以库模块形式使用数据服务层**：
  - 允许未来在此基础上再包装为内部 HTTP/gRPC 服务，但不是当前必选项。

---

## 2. 整体架构（方案 C：核心库 + 可选服务）

### 2.1 模块划分

在 AIstock 仓库中新建内部包（命名暂定）：

- `aistock.data_service`（Python 包，真正的“内核”）
  - `aistock.data_service.xtquant_adapter`
    - 封装 xtquant 历史/实时/订阅接口；
    - 提供统一的 DataFrame 形态给上层视图层。
  - `aistock.data_service.miniqmt_adapter`
    - 封装 miniQMT 下单、账户、持仓、订单/成交接口；
    - 对上暴露 `PortfolioState` / `Order` / `Trade` 结构。
  - `aistock.data_service.timescaledb_adapter`（可选）
    - 如有需要，从 TimescaleDB 直接查询历史窗口（例如更大跨度历史）；
    - 可作为 `get_history_window` 的其中一种后端实现策略。
  - `aistock.data_service.api`
    - 对上暴露统一的 Python 接口：
      - `get_realtime_snapshot(...)`
      - `stream_quotes(...)`
      - `get_history_window(...)`
      - `get_portfolio_state()` / `get_open_orders()` / `get_trades(...)`
    - 对调用方屏蔽 xtquant/miniQMT/TimescaleDB 的差异。

- （可选）`aistock.data_service.http`
  - 使用 FastAPI 对 `api` 做一层极薄包装，仅供：
    - 内部工具/监控/可视化使用；
    - 未来如需给 Web 前端暴露只读查询。
  - 策略运行 **不依赖** 这一层，仍推荐直接 import Python 包。

### 2.2 策略运行时的集成视角

策略运行栈示意：

```text
xtquant / miniQMT / TimescaleDB
          │
          ▼
 aistock.data_service (xtquant_adapter / miniqmt_adapter / timescaledb_adapter)
          │
          ▼
   aistock.strategy_engine (策略宿主 / Runner)
          │
          ▼
   用户策略函数 (RD-Agent 导出的在线策略 / AIstock 自有策略)
```

- **策略函数**只关心“我每次被调用时拿到什么数据结构”：
  - 历史窗口 DataFrame、实时 snapshot DataFrame、`PortfolioState` 等；
  - 不直接依赖 xtquant/miniQMT 具体调用细节。
- **策略宿主/Runner**：
  - 负责根据策略配置和调度模式，从 `aistock.data_service.api` 拉/订阅数据；
  - 然后将数据打包成参数调用策略函数；
  - 处理策略输出（目标权重/订单）并通过 miniQMT 下单。

---

## 3. 对上接口的具体实现规划

本节在《数据服务层详细设计》中的接口定义基础上，结合 xtquant/miniqmt 能力给出 AIstock 实现侧的细化约定。

### 3.1 get_realtime_snapshot

```python
# aistock.data_service.api

def get_realtime_snapshot(
    universe: list[str],
    *,
    fields: list[str] | None = None,
    level: str = "stock",
    freq: str = "1d",  # 当前阶段主要支持 1d，可预留 "1m"
) -> pd.DataFrame:
    """返回 index=instrument 的 DataFrame，columns 为标准行情字段。

    - 数据源优先级：
      1) xtquant 实时 snapshot / 行情接口；
      2) 如 xtquant 不可用，可回退到 TimescaleDB 近一条记录（仅用于模拟）。
    - 字段对齐：字段名与含义应尽量与 offline daily_pv.h5 保持一致。
    """
```

**与 xtquant 的映射**：

- 使用 xtquant 的“当前行情”接口（例如 get_market_data / get_snapshots，一以真实 API 名为准）：
  - 对 `universe` 批量请求；
  - 将返回结果转换为 DataFrame，列名映射到标准字段（open/high/low/close/volume/amount 等）。
- 若 `freq="1m"`：
  - 可使用 xtquant 的分钟级 snapshot 或从最新 1m K 线推导（需在实现阶段确认 xtquant 实际能力）。

### 3.2 stream_quotes（推送模式）

```python
# aistock.data_service.api

@dataclass
class QuoteBatch:
    timestamp: datetime
    data: pd.DataFrame  # index: instrument, columns: price/volume/... 标准字段


def stream_quotes(
    universe: list[str],
    *,
    fields: list[str] | None = None,
    level: str = "stock",
    freq: str = "tick",  # 或 "1s" / "1m"，取决于 xtquant 支持
) -> Iterator[QuoteBatch]:
    """基于 xtquant 订阅行情，生成连续的 QuoteBatch。

    - 内部统一管理 xtquant 订阅与回调，将推送数据写入线程安全队列；
    - 本函数作为生成器，从队列中按批次读出数据。
    """
```

**推送实现要点**：

- `xtquant_adapter`：
  - 统一调用 xtquant 的订阅 API（subscribe/unsubscribe）；
  - 在回调中将原始数据转换为 DataFrame，写入一个 `queue.Queue[QuoteBatch]`；
  - 管理订阅生命周期（引用计数：多个策略订阅相同标的时只建立一条底层订阅）。
- `stream_quotes`：
  - 在一个 while 循环中从队列 `get()` 出最新一个或合并后的 QuoteBatch；
  - 作为生成器对外 yield，供策略宿主使用：

    ```python
    for batch in stream_quotes(universe, freq="1m"):
        on_new_quote(batch)
    ```

### 3.3 get_history_window

```python
# aistock.data_service.api

def get_history_window(
    universe: list[str],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    bars: int | None = None,
    fields: list[str] | None = None,
    freq: str = "1d",
) -> pd.DataFrame:
    """返回 MultiIndex(datetime, instrument) 的历史行情窗口。

    - 当前阶段优先支持 freq="1d"；
    - bars 优先，若提供则由数据服务层根据 end 计算 start；
    - 数据源策略：
      1) 日线：可直接从 TimescaleDB 查询（推荐），或使用 xtquant 日线历史接口；
      2) 分钟线：使用 xtquant 历史 K 线接口；
    - 字段对齐：与 offline daily_pv.h5 / xtquant_dataset_catalog 中的映射保持一致。
    """
```

**数据源选择策略（可在实现时进一步细化配置）**：

- 日线：
  - 优先 TimescaleDB（统一、完整，便于做更长历史）；
  - 若无法访问 TimescaleDB，可 fallback 到 xtquant 日线历史接口（本地场景）。
- 分钟线：
  - 使用 xtquant 的分钟 K 线接口，统一在 Data Service 中做字段映射。

---

## 4. 账户与交易视图实现规划

### 4.1 账户与持仓：get_portfolio_state

```python
# aistock.data_service.api

@dataclass
class Position:
    instrument: str
    volume: float
    available: float
    avg_price: float
    market_value: float


@dataclass
class PortfolioState:
    cash: float
    equity: float
    positions: list[Position]
    timestamp: datetime


def get_portfolio_state() -> PortfolioState:
    """基于 miniQMT 查询当前账户与持仓状态，转换为统一结构。"""
```

- `miniqmt_adapter`：
  - 封装 miniQMT 的账户/持仓查询接口；
  - 做必要的字段单位/精度转换；
  - 确保 `equity≈cash+Σmarket_value`。

### 4.2 订单与成交

```python
# aistock.data_service.api

@dataclass
class Order:
    order_id: str
    instrument: str
    side: str
    volume: float
    price: float | None
    status: str
    created_at: datetime


@dataclass
class Trade:
    trade_id: str
    order_id: str
    instrument: str
    side: str
    volume: float
    price: float
    traded_at: datetime


def get_open_orders() -> list[Order]:
    ...


def get_trades(start: datetime | None = None, end: datetime | None = None) -> list[Trade]:
    ...
```

- 实现上，`miniqmt_adapter` 负责从 miniQMT 拉订单/成交并转换为上述 dataclass。
- Data Service 不决定是否下单，只提供统一视图给策略引擎与风控模块使用。

---

## 5. 策略集成与多策略并行

### 5.1 策略函数形态与解耦

推荐将策略本身限定为“纯函数风格”，仅依赖输入参数，不直接访问 Data Service：

```python
def strategy_long_topk_with_risk(
    *,
    factors: pd.DataFrame,       # index: instrument, columns: 因子名
    prices: pd.DataFrame,        # index: instrument, columns: 当前价/盘口等
    portfolio: PortfolioState,
    context: dict | None = None,
) -> dict[str, float]:           # instrument -> target_weight
    ...
```

- 策略 **不直接 import xtquant / miniQMT / Data Service**。
- Data Service 的调用集中在“策略宿主/Runner”中实现。

### 5.2 策略运行配置

为方便多策略并行运行，可以在 AIstock 中维护一套策略运行时配置（示例）：

```json
{
  "strategy_id": "...",
  "entry_point": "aistock.strategies.sample_strategy.run_step",
  "universe": ["000001.SZ", "000002.SZ", ...],
  "freq": "1d",
  "window_bars": 60,
  "fields": ["open", "high", "low", "close", "volume"],
  "snapshot_fields": ["close", "limit_up", "limit_down"],
  "risk_params": {
    "max_position": 0.95,
    "max_weight_per_stock": 0.05
  }
}
```

- UI 只需提供：选择策略、填写/加载配置、点击“启动/停止”。
- 此配置不涉及 Data Service 细节，只是描述“策略运行需要什么数据”。

### 5.3 多策略并行 Runner（示意）

```python
# aistock.strategy_engine.runner

from aistock.data_service.api import (
    get_history_window,
    get_realtime_snapshot,
    get_portfolio_state,
    stream_quotes,
)


def run_strategy_polling(cfg):
    """轮询模式：适用于日线/低频策略。"""
    strategy_fn = load_entry_point(cfg.entry_point)
    while not should_stop():
        hist = get_history_window(
            universe=cfg.universe,
            bars=cfg.window_bars,
            fields=cfg.fields,
            freq=cfg.freq,
        )
        snap = get_realtime_snapshot(
            universe=cfg.universe,
            fields=cfg.snapshot_fields,
            freq=cfg.freq,
        )
        portfolio = get_portfolio_state()

        factors = build_factors_from_hist(hist)  # 使用离线/在线统一的因子函数
        target_weights = strategy_fn(
            factors=factors,
            prices=snap,
            portfolio=portfolio,
            context={"mode": "sim"},
        )
        apply_target_weights_via_miniqmt(target_weights, portfolio, cfg)

        sleep_until_next_bar(cfg.freq)


def run_strategy_push(cfg):
    """推送驱动模式：适用于盘中/需快速响应策略。"""
    strategy_fn = load_entry_point(cfg.entry_point)
    for batch in stream_quotes(cfg.universe, fields=cfg.snapshot_fields, freq=cfg.freq):
        snap = batch.data
        hist = get_history_window(
            universe=cfg.universe,
            bars=cfg.window_bars,
            fields=cfg.fields,
            freq=cfg.freq,
        )
        portfolio = get_portfolio_state()
        factors = build_factors_from_hist(hist)
        target_weights = strategy_fn(
            factors=factors,
            prices=snap,
            portfolio=portfolio,
            context={"mode": "sim"},
        )
        apply_target_weights_via_miniqmt(target_weights, portfolio, cfg)
```

- **多策略并行**：
  - UI 启动多个 Runner 进程/线程，每个 Runner 读取自身的策略配置；
  - 所有 Runner 通过 `aistock.data_service.api` 共享 xtquant/miniQMT/TimescaleDB 适配层；
  - 不需要为 Data Service 做 per-strategy 单独配置。
- 策略开发者视角：
  - 只需实现符合接口的策略函数；
  - 在 UI 中选择策略 ID 并启动即可。

---

## 6. 开发计划与渐进接入

### 6.1 开发顺序（建议）

1. **实现 xtquant_adapter**：
   - 历史日线/分钟线查询；
   - 实时 snapshot 查询；
   - 行情订阅 & 推送封装（队列 + QuoteBatch）。
2. **实现 miniqmt_adapter**：
   - 账户/持仓查询；
   - 订单/成交查询；
   - 下单封装（与现有 AIstock 交易模块对齐）。
3. **实现 data_service.api**：
   - `get_history_window` / `get_realtime_snapshot` / `stream_quotes`；
   - `get_portfolio_state` / `get_open_orders` / `get_trades`。
4. **实现示例 Runner**：
   - 先做一个最小样例策略（例如简单动量/均值回归），验证日线 polling 模式；
   - 再验证推送驱动模式。
5. **评估与接入计划**：
   - 在不改现有模拟盘/实盘逻辑的前提下，评估如何逐步引入 Data Service 提供的数据视图；
   - 制定“单策略 → 多策略”的接入路线与回滚方案。

### 6.2 当前阶段的限制与后续扩展

- 当前阶段：
  - 只在本地环境使用，不考虑集群/多机部署；
  - 不对外暴露 HTTP/gRPC 服务；
  - 不改变现有 TimescaleDB → HDF5/qlib 导出与 RD-Agent 离线研究流程。

- 后续可扩展方向：
  - 将 `aistock.data_service.api` 包装为内部 HTTP/gRPC 服务，支持：
    - 多语言策略；
    - Web 可视化/监控直接查询；
  - 扩展多资产、多市场、多时区；
  - 引入缓存层（Redis 等）优化高频读写场景。

---

> 本文档当前为实现设计的初稿，后续可根据 xtquant/miniqmt 具体接口与 AIstock 现有策略引擎结构进一步补充：
> - xtquant 接口与 Data Service API 的一一映射表；
> - 具体错误处理与重试策略；
> - 与现有回测/模拟盘模块的集成计划。
