# xtquant 数据源总结与 AIstock 数据服务接入说明

> 本文基于 xtquant 自带文档 `xtdata.md` / `xttrader.md` 以及仓库中的
> `xtquant_dataset_catalog.md` / `xtquant_realtime_quote_guide.md` 进行归纳，
> 只关注 **AIstock 数据服务层需要消费的行情相关能力**：历史 K 线、实时
> 快照、分笔/Level2 概览以及时间/周期、字段、精度与返回格式。

---

## 1. xtdata 行情能力总览

- **连接对象**：MiniQMT 客户端（Windows，本机进程）。
- **Python 接口模块**：`xtquant.xtdata`（本仓库 vendor 在 `xtquant/` 目录）。
- **主要数据类型**：
- K 线（多周期 Level1：`1m/5m/15m/30m/1h/1d/1w/1mon/1q/1hy/1y`）
- 分笔 `tick`
- Level2 深度：`l2quote/l2order/l2transaction/...`
- **时间范围语义**：`[start_time, end_time]` 闭区间内，按时间增序返回不多于
  `count` 条。
- **复权方式**（K 线）：`none/front/back/front_ratio/back_ratio` 通过
  `dividend_type` 控制。

### 1.1 主要接口一览（与 Data Service 相关）

- **订阅类**
- `subscribe_quote(stock_code, period, start_time, end_time, count, callback)`
- `subscribe_quote2(..., dividend_type, callback)`
- `subscribe_whole_quote(code_list, callback)`
- `unsubscribe_quote(seq)`
- `run()`（阻塞当前线程，驱动回调循环）

- **主动获取类**
- `get_market_data(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)`
- `get_local_data(...)`（本地文件，批量历史）
- `get_full_tick(stock_list)`（最新分笔快照）

- **辅助类**（当前阶段仅参考，不直接接入）
- 财务数据：`get_financial_data` / `download_financial_data` 系列。
- 合约信息：`get_instrument_detail`。
- 板块与行业：`get_sector_list` / `get_stock_list_in_sector` 等。

---

## 2. 历史 K 线数据

### 2.1 `get_market_data` - K 线

- **函数签名**（简化）：

```python
xtdata.get_market_data(
    field_list: list[str] = [],
    stock_list: list[str] = [],
    period: str = "1d",
    start_time: str = "",
    end_time: str = "",
    count: int = -1,
    dividend_type: str = "none",
    fill_data: bool = True,
) -> dict[str, pd.DataFrame] | dict[str, np.ndarray]
```

- **周期与返回类型**：
- period 为 `1m/5m/1d/...` 等 K 线：
  - 返回 `dict[field -> pd.DataFrame]`
  - 每个 DataFrame：`index = stock_list`，`columns = time_list`
  - 所有字段共享同一 index/columns。
- period 为 `tick`：
  - 返回 `dict[stock_code -> np.ndarray]`，数组按时间增序。

- **K 线字段（典型 Level1）**：

| 字段名       | 类型   | 精度     | 说明           |
| ------------ | ------ | -------- | -------------- |
| `time`       | int/ms | 毫秒或日 | 时间戳         |
| `open`       | float  | 4 位小数 | 开盘价         |
| `high`       | float  | 4 位小数 | 最高价         |
| `low`        | float  | 4 位小数 | 最低价         |
| `close`      | float  | 4 位小数 | 收盘价/最新价  |
| `volume`     | int    | 股       | 成交量         |
| `amount`     | float  | 2 位小数 | 成交额         |
| `pre_close`  | float  | 4 位小数 | 前收盘价       |
| `suspend_flag` | int  | -        | 停牌标记       |

- **时间范围参数**：
- `start_time`/`end_time`：字符串，常用 `YYYYMMDD` 或 `YYYYMMDDHHMMSS`。
- `count`：
  - `>0`：限制返回条数（以 end_time 为基准向前取）；
  - `0`：用于订阅时仅建立缓存，不主动返回历史；
  - `-1`：返回全部可用数据（需谨慎，量大时会较慢）。

### 2.2 数据覆盖与历史范围

- 历史 K 线由 MiniQMT 本地缓存 + 服务器增量补充组成：
- 一般能覆盖 **近数年~十几年** 的 A 股数据；
- 若本地无对应历史，可用 `download_history_data` / `download_history_data2`
  先拉取后再 `get_market_data`。
- 具体覆盖范围以券商/MiniQMT 配置为准，AIstock 不再额外限制。

### 2.3 Data Service 统一视图

在数据服务层中：

- 将 `get_market_data` 返回的
  `dict[field -> DataFrame(index=stock, columns=time)]` 转换为：

```text
MultiIndex(datetime, instrument)  DataFrame
列：open / high / low / close / volume / amount / ...
```

- 标准化规则：
- `instrument`：使用 `code.market` 形式（与 TimescaleDB / RD-Agent 一致）；
- `datetime`：转成 pandas `datetime64[ns]`，本地时区（不强制 UTC）；
- 字段名：全部小写，复用 TimescaleDB/qlib 的 `open/high/low/close/volume/amount`。

---

## 3. 实时行情与 Tick 数据

### 3.1 订阅与回调（推送模式）

- **单股订阅**：`subscribe_quote` / `subscribe_quote2`
  - `period`: `tick` / `1m` / `1d` 等；
  - `callback(datas)`：`datas` 为 `{stock_code: [data1, data2, ...]}`。
- **全推订阅**：`subscribe_whole_quote(code_list, callback)`
  - `code_list` 支持市场代码（`"SH"/"SZ"`）或个股列表；
  - 回调形态：`datas = {stock_code: quote_dict}`，`quote_dict` 结构与 tick 类似。
- 驱动循环：`xtdata.run()` 阻塞当前线程，维持连接并处理回调。

在数据服务层中：

- 推荐以 **统一 `QuoteBatch`** 结构暴露：

```python
@dataclass
class QuoteBatch:
    timestamp: datetime
    data: pd.DataFrame  # index: instrument, columns: lastPrice/volume/...
```

- 由 `xtquant_adapter` 将回调 `datas` 转换为 `QuoteBatch` 并送入队列；
- `api.stream_quotes` 再以生成器形式对外提供。

### 3.2 主动获取实时快照（轮询模式）

- 推荐两种方式：

1. `get_market_data(period='tick', count=1)` 从缓存读最新分笔；
2. `get_full_tick(stock_list)` 直接返回最新一条 tick 快照：
   - 返回 `{stock_code: {time, lastPrice, open, high, low, volume, amount, ...}}`。

- 数据字段（`get_full_tick` 与全推 tick 类似）：

| 字段           | 类型     | 说明           |
| -------------- | -------- | -------------- |
| `time`         | int/ms   | 时间戳         |
| `lastPrice`    | float    | 最新价         |
| `open`         | float    | 开盘价         |
| `high`         | float    | 最高价         |
| `low`          | float    | 最低价         |
| `lastClose`    | float    | 昨收价         |
| `amount`       | float    | 成交额         |
| `volume`       | int      | 成交量         |
| `stockStatus`  | int      | 股票状态       |
| `askPrice[]`   | float[]  | 卖盘价（5 档） |
| `bidPrice[]`   | float[]  | 买盘价（5 档） |
| `askVol[]`     | int[]    | 卖盘量（5 档） |
| `bidVol[]`     | int[]    | 买盘量（5 档） |

- Data Service 中的统一快照视图：

```text
index: instrument
columns: close / open / high / low / volume / amount / last_close / time / ...
```

---

## 4. Data Service 侧 xtquant 适配策略

### 4.1 历史窗口：`fetch_history_window_xt`

- 输入：
- `universe: list[str]` 统一使用 `code.market` 形式；
- `start/end/bars/fields/freq` 与 TimescaleDB 适配器保持一致；
- 当前阶段支持 `freq="1d"` 和 `freq="1m"` 两种。

- 实现要点：
- 通过 `xtquant.xtdata.get_market_data` 获取指定周期 K 线；
- 使用 `field_list = ["open","high","low","close","volume","amount"]`
 （如 `fields` 非空则取交集）；
- 将 `dict[field -> DataFrame(index=stock, columns=time)]` 压平成
  `MultiIndex(datetime, instrument)` 的 DataFrame；
- 如调用方给出 `bars`，在 DataFrame 级别按每个 `instrument` 截取最后
  `bars` 条；
- `start/end` 映射为 `YYYYMMDD` 或 `YYYYMMDDHHMMSS` 字符串。

### 4.2 实时快照：`fetch_realtime_snapshot_xt`

- 输入：
- `universe: list[str]`；
- `fields: list[str] | None`（可选字段过滤）；
- `freq: str = "1d"`（当前主要用于控制字段/含义，不改变实现）。

- 实现要点：
- 直接调用 `xtdata.get_full_tick(universe)`；
- 为每个 `stock_code` 构造一行记录：
  - `close = lastPrice`；
  - `open/high/low/volume/amount/last_close/time` 等按字段映射；
- 组装为 `index=instrument` 的 DataFrame 并按需要裁剪列。

### 4.3 推送流：`stream_quotes_xt`（后续阶段）

- 使用 `subscribe_whole_quote(code_list=universe, callback=...)`：
  - 在回调中转换为 `QuoteBatch` 并写入 `queue.Queue`；
  - `stream_quotes_xt` 从队列中 `get()` 批次并 yield。
- 也可与现有 `backend/infra/realtime_quote_subscriber.RealtimeQuoteSubscriber`
  对接，减少重复实现。

---

## 5. 与 TimescaleDB 视图的对齐

- **统一字段**：
- K 线：`open/high/low/close/volume/amount` 与
  `DBReader.load_daily` 输出保持一致；
- 快照：`close` 代表最新价，`last_close` 代表昨收。

- **统一索引**：
- 历史窗口：`MultiIndex(datetime, instrument)`；
- 快照：`index = instrument`，必要时可携带 `datetime` 列。

- **统一精度**：
- 价格类字段统一使用 `float`（4 位小数为主）；
- 成交量使用整数；
- 成交额使用 2 位小数浮点。

---

## 6. 数据服务层实现状态

- `backend/data_service/xtquant_adapter.py`：
- 将按本说明实现：
  - `fetch_history_window_xt`（历史 K 线）；
  - `fetch_realtime_snapshot_xt`（实时快照）；
- `backend/data_service/api.py`：
  - `get_history_window`：优先 TimescaleDB，必要时可回退到
    xtquant；
  - `get_realtime_snapshot`：在 TimescaleDB 不适用的场景下，可以
    直接依赖 xtquant 快照实现。

此文档作为 xtquant 相关模块（尤其是 Data Service 适配层）的
**权威参考**，后续若 xtquant 版本升级，只需在本文中补充差异再迭代实现即可。
