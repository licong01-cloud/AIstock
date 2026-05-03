# P0 价格口径统一验证：up_limit/down_limit/pre_close 与开盘涨幅

日期：2026-05-03
范围：AIstock Qlib 1min 权威导出、QE V25 分钟执行策略、Paper v2 分钟执行上下文。
结论等级：P0 / 已用代码与真实数据验证。

## 1. 结论

1. `market.stk_limit.up_limit`、`market.stk_limit.down_limit`、`market.stk_limit.pre_close` 是交易所口径的未复权原始人民币价格，不是前复权价格。
2. AIstock 日内执行、涨跌停判断、P0 判断、买入涨停阻塞、卖出跌停阻塞、开盘涨幅和 `gap_ratio` 的统一计算口径必须是未复权 raw。
3. Qlib bin 存储层为了符合 Qlib 因子/训练习惯，`$open/$high/$low/$close` 仍是前复权价格，`$factor` 是复权因子；执行层必须使用 `raw_price = adjusted_price / $factor` 转回未复权价格后，再与 raw `prev_close/up_limit/down_limit` 比较。
4. 不允许直接用 Qlib 前复权 `$open/$close` 与 raw `prev_close/up_limit/down_limit` 比较；这会让开盘涨幅和涨跌停判断系统性偏移。

## 2. 当前代码证据

- `backend/qlib_exporter/authoritative_bin_exporter.py`
  - 从 `market.stk_limit` 读取 `pre_close AS prev_close`、`up_limit AS up_limit_price`、`down_limit AS down_limit_price`。
  - 导出 `$open/$high/$low/$close = raw_ohlc * qfq_factor`。
  - 导出 `$prev_close/$up_limit_price/$down_limit_price` 为 raw 价格。
  - `limit_up/limit_down` 使用 raw close 与 raw limit 计算。
- `scripts/tail_twap_v25_strategy.py`
  - `_require_raw_price()` 读取 `$factor` 并计算 `raw_price = adjusted_price / factor`。
  - `_generate_plan_for_order()` 使用 raw open 与 raw prev_close 计算 `gap_pct/gap_ratio`。
  - P0、买入涨停阻塞、卖出跌停阻塞均使用 raw close 与 raw limit 价格比较。
- `backend/execution_algos/v25_core.py`
  - `classify_v25_minute_market_state()` 要求 `price_basis/limit_price_basis` 都是 raw，否则返回 `price_basis_mismatch_data_error`。
  - `generate_plan()` 明确要求 open price 与 prev_close 同基准，AIstock adapter 传入 raw。
- `backend/services/paper_trading_v2/market_data.py`
  - DB/Paper v2 分钟执行上下文明确写入 `price_basis=raw`、`limit_price_basis=raw`、`prev_close_basis=raw`。

## 3. 真实数据验证

验证命令使用 WSL `rdagent` conda 环境，直接读取 DB 与 `/home/lc999/data/qlib_minute_authoritative_full_20260428` 的 Qlib bin。

### 3.1 CSV/DB vs Qlib bin 字段一致性

验证范围：`000001.SZ, 000063.SZ, 000651.SZ, 300750.SZ, 688766.SH`，`2025-07-08` 至 `2025-07-10`。

```text
字段集合       open/high/low/close/volume/amount/factor/up_limit_price/down_limit_price/prev_close/limit_up/limit_down
basis_start    2024-01-01
basis_end      2026-04-28
股票数         5
股票-日期数    15
DB分钟行数     3600
校验字段值     43200
error_count    0
max_abs_diff   所有字段均为 0.0
```

说明：第一次不指定全量导出使用的 `basis_end=2026-04-28` 时，复权 OHLC/factor 会与 Qlib bin 不一致；指定同一 basis 后，所有字段完全一致。这证明复权基准本身也必须固定，不能每次用局部窗口重新计算。

### 3.2 开盘涨幅 raw 口径验证

```text
股票        时间                 factor      DB raw open  Qlib adj open  Qlib raw open  raw prev_close  DB gap%      Qlib raw gap%  错误adj gap%
----------  -------------------  ----------  -----------  -------------  -------------  --------------  -----------  -------------  ------------
000001.SZ   2025-07-08 09:31:00  0.97925687  12.750000    12.485525      12.750000      12.780000       -0.00234742  -0.00234739    -0.02304183
000651.SZ   2025-07-08 09:31:00  0.93443465  46.490000    43.441868      46.490001      46.490002        0.00000000  -0.00000002    -0.06556536
300750.SZ   2025-07-08 09:31:00  0.98102075  260.700000   255.752121     260.700012     260.890015      -0.00072828  -0.00072829    -0.01969372
688766.SH   2026-04-27 09:31:00  1.00000000  263.310000   263.309998     263.309998     261.579987       0.00661366   0.00661370     0.00661370
```

结论：

- `Qlib raw open = Qlib adjusted open / factor` 后，与 DB raw open 对齐，误差仅为 float32 精度。
- `Qlib raw gap%` 与 DB raw gap% 对齐。
- 如果错误地直接用 `Qlib adjusted open` 去除以 raw `prev_close`，会出现明显错误，例如 `000651.SZ` 的开盘涨幅会从约 `0%` 错成 `-6.56%`。

## 4. 必须执行的统一标准

```text
场景                         必须使用的价格口径     说明
---------------------------  ---------------------  ------------------------------------------------------------
Qlib bin OHLC 存储            前复权 adjusted         用于 Qlib 因子/训练兼容，不能直接用于执行状态比较
Qlib bin factor 存储          复权因子                执行层用 adjusted / factor 还原 raw
Qlib bin prev_close           未复权 raw              来源 market.stk_limit.pre_close
Qlib bin up/down limit        未复权 raw              来源 market.stk_limit.up_limit/down_limit
涨跌停 flag                  未复权 raw              raw close vs raw up/down limit
V25 P0 / 阻塞判断             未复权 raw              raw close vs raw up/down limit
V25 open gap / gap_ratio      未复权 raw              raw open vs raw prev_close
Paper v2 DB 分钟执行          未复权 raw              DB kline_minute_raw 本身为 raw
```

## 5. 后续约束

1. 任何新增 Qlib adapter 只要读取 `$open/$high/$low/$close` 做执行状态判断，必须同时读取 `$factor` 并先转换到 raw。
2. 任何 open gap / `gap_ratio` 都必须写明 `price_basis=raw`，严禁直接使用 adjusted OHLC 与 raw prev_close。
3. 任何新增导出或修复工具必须固定全量导出 `basis_end`，不能用局部窗口重新计算复权分母。
4. 如果 `$factor` 缺失、非有限或小于等于 0，且该分钟存在有效 bar，则必须 fail-fast；不能用 `factor=1` 静默兜底。
5. 如果 limit/pre_close 缺失，必须用 suspend/no-bar 证据解释；不能把缺失值填默认价格。

## 6. 验证命令

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/test_tail_twap_v25_market_state.py backend/tests/trading_core/test_v25_execution_contract.py -q -p no:cacheprovider
```

结果：`18 passed in 1.17s`。
