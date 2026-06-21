# BUG-466 L5 分析与修复方案

## 结论

- GitHub issue #1431 的三项指控与当前代码一致：LocalSim 的 TDX realtime pre-trade gate 读取报价字段但不校验报价时间戳；TDX_REALTIME 衍生涨跌停只按代码前缀判断，无法识别 ST/*ST 的 5% 涨跌停；pre-trade gate 只拦截空盘口，不按 BUY/SELL 方向拦截涨停买入和跌停卖出。
- 本修复限定在 LocalSim / TDX realtime 市场数据路径，不触碰 MiniQMT 文件或 MiniQMT 专属分支。
- 与 issue 无业务分歧；实现上为 `PreTradeTradabilityProvider.get_statuses()` 增加 `as_of_time` 和 `side_by_symbol` 可选入参，使上层在已有方向信息时得到方向化 `reason_code`。

## 根因

1. `quote_tradability_evidence()` 只读取 `K`、盘口、成交量/额等字段，没有解析 `ServerTime` / `timestamp` / `time` 等报价时间字段，也没有与调用时点 `as_of_time` 比较；冻结或跨日 TDX 报价会被当成新鲜行情。
2. `PaperV2MinuteMarketDataProvider._derived_realtime_limit_price_from_previous_close()` 使用 prefix-only 的涨跌停比例，普通主板固定 10%，没有查询 `market.stock_st` 的 point-in-time 状态；ST/*ST 主板股票会生成错误的 10% 限价。
3. `PreTradeTradabilityProvider` 只在 `book_empty` 且 OHLC/成交缺失时返回 `NO_TRADABLE_REALTIME_QUOTE`，没有基于 previous close + ST-aware limit pct 计算涨停/跌停，也没有在 `BUY@limit-up` / `SELL@limit-down` 时 fail-closed。

## 修复策略

- TDX 报价新鲜度：新增严格时间戳解析与 freshness gate。缺时间戳、格式无效、日期不匹配、超出 5 分钟 freshness bound、未来时间超过容忍窗口均抛出 `DataUnavailableError`，`context` 包含 `reason_code`、`symbol`、`trade_date`、`as_of_time`、`quote_source`、原始 timestamp 与差值信息。
- ST 状态：新增 `DailyStStatus` / `StStatusProvider` / `DbStStatusProvider`，默认查询 `market.stock_st`。查询失败、provider 缺失或 payload 非法均 loud fail；有有效 ST row 时使用 5%，非 ST 再按 BJ/科创/创业/普通板规则选择 30%/20%/10%。
- 限价拦截：pre-trade quote evidence 从 TDX `K.Close` 和 `K.Last` 得到 current price 与 previous close，计算 ST-aware `limit_up` / `limit_down`；请求方向为 BUY 且涨停时返回 `LIMIT_UP_BUY_BLOCKED`，方向为 SELL 且跌停时返回 `LIMIT_DOWN_SELL_BLOCKED`。若未提供方向但报价已处于涨跌停状态，返回 `REALTIME_QUOTE_LIMIT_STATE_REQUIRES_SIDE`，避免继续 `OK` 软化。

## 回归覆盖

- 缺失 TDX quote timestamp -> `DataUnavailableError` / `REALTIME_QUOTE_TIMESTAMP_MISSING`。
- 过期 TDX quote timestamp -> `DataUnavailableError` / `REALTIME_QUOTE_STALE`。
- ST/*ST 主板 TDX_REALTIME 衍生涨跌停 -> 5% `limit_up` / `limit_down`。
- ST 状态不可得 -> `DataUnavailableError` / `ST_STATUS_UNAVAILABLE`。
- `BUY@limit-up` 与 `SELL@limit-down` -> 方向化 fail-closed `reason_code`。

## 验证证据

- `rtk python -m pytest backend/tests/paper_trading_v2/test_market_data.py -q` -> 25 passed。
- `rtk python -m ruff check backend/services/paper_trading_v2/market_data.py backend/tests/paper_trading_v2/test_market_data.py` -> All checks passed。
- `rtk git diff --check` -> passed。
- `rtk python -m nox -s l0` -> passed。
- `rtk python -m nox -s validation_module_registry_l0` -> 8 passed。
- `rtk cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"` -> `paper_v2_backend` 639 passed, 1 skipped, 1 deselected；`data_quality_deep` 10 passed, 21 skipped。

## 生产门禁

- `production_ddl_gate=noop`。
- `production_frontend_dependency_gate=noop`。
- `production_backend_dependency_gate=noop`。
- 未启动、重启或停止任何服务；未触碰生产 DB。合并后如需在线生效，需要用户自行重启相关运行时。
