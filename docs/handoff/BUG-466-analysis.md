# BUG-466 L5 分析与返工结论

## 结论

- GitHub issue #1431 的原始指控与当前代码一致：LocalSim 的 TDX realtime pre-trade gate 原先没有校验报价时间戳；ST/*ST 5% 涨跌停没有使用可审计 ST 源；BUY 涨停 / SELL 跌停没有按方向 fail-closed。
- 本 PR 只修改 LocalSim / TDX realtime 市场数据路径，不触碰 MiniQMT 文件或 MiniQMT 专属分支。
- 本次 Tier2 条件核验确认：运行库 `market.stock_st` 表存在且有当日快照；该表当前实际语义是“每日 ST 快照”，不是 `start_date/end_date` 有效期区间表。
- 因此代码必须按 `ann_date` 最新快照判定 ST。不能把 `start_date/end_date IS NULL` 的历史行当成无限期有效，否则历史 ST 股票会长期误判为 ST。

## 运行库核验证据

- `market.stock_st` 表存在。
- 总行数：327417。
- 最新 `ann_date`：2026-06-22。
- `ann_date=2026-06-22`：221 行 / 221 个 symbol。
- 近期快照：2026-06-22 有 221 个 symbol，2026-06-18 有 225 个，2026-06-17 有 227 个。
- `start_date` / `end_date` 当前全部为 NULL，不能作为有效期区间使用。

## 根因

1. `quote_tradability_evidence()` 原先只读取 `K`、盘口、成交量/额等字段，没有解析 `ServerTime` / `timestamp` / `time` 等报价时间戳，也没有与调用点 `as_of_time` 比较；冻结或跨日 TDX 报价会被当成新鲜行情。
2. `PaperV2MinuteMarketDataProvider._derived_realtime_limit_price_from_previous_close()` 原先只按代码前缀选择涨跌停比例；普通主板固定 10%，无法识别 ST/*ST 5%。
3. `PreTradeTradabilityProvider` 原先只在盘口完全为空或 OHLC/成交缺失时返回不可交易，没有基于 previous close + ST-aware limit pct 阻断 `BUY@limit-up` / `SELL@limit-down`。
4. 原 PR 首版 `DbStStatusProvider` 会把 `start_date/end_date IS NULL` 的历史行当成有效期无限长；与生产数据的每日快照语义不一致。

## 修复策略

- TDX 报价新鲜度：新增严格时间戳解析与 freshness gate。缺时间戳、格式无效、日期不匹配、超过 5 分钟 freshness bound、未来时间超过容忍窗口均抛出 `DataUnavailableError`，`context.reason_code` 明确说明原因。
- ST 状态：新增 `DailyStStatus` / `StStatusProvider` / `DbStStatusProvider`，默认查询 `market.stock_st`。
- ST 快照语义：`DbStStatusProvider` 先取 `latest_ann_date <= trade_date`；当 `start_date/end_date` 都为 NULL 时，只把 `ann_date = latest_ann_date` 的行视为有效快照；没有任何快照时 loud fail `ST_STATUS_SOURCE_EMPTY`。
- ST 源错误：查询失败继续 loud fail `ST_STATUS_QUERY_FAILED`，不会静默降级为非 ST，也不会升级成全市场不可交易的假状态。
- 限价阻断：pre-trade quote evidence 使用 TDX `K.Close` 与 `K.Last` 得到 current price 和 previous close，计算 ST-aware `limit_up` / `limit_down`；BUY 涨停返回 `LIMIT_UP_BUY_BLOCKED`，SELL 跌停返回 `LIMIT_DOWN_SELL_BLOCKED`。

## 回归覆盖

- 缺失 TDX quote timestamp -> `DataUnavailableError` / `REALTIME_QUOTE_TIMESTAMP_MISSING`。
- 过期 TDX quote timestamp -> `DataUnavailableError` / `REALTIME_QUOTE_STALE`。
- ST/*ST 主板 TDX_REALTIME 衍生涨跌停 -> 5% `limit_up` / `limit_down`。
- ST 状态不可得 -> `DataUnavailableError` / `ST_STATUS_UNAVAILABLE`。
- ST 源快照为空 -> `DataUnavailableError` / `ST_STATUS_SOURCE_EMPTY`。
- ST 源查询失败 -> `DataUnavailableError` / `ST_STATUS_QUERY_FAILED`。
- 每日快照语义：历史 NULL 区间行不能被当成无限期 ST；只使用 `latest_ann_date` 快照。
- `BUY@limit-up` 与 `SELL@limit-down` -> 方向化 fail-closed reason_code。

## 验证证据

- `rtk python -m pytest backend/tests/paper_trading_v2/test_market_data.py -q` -> 29 passed。
- `rtk python -m ruff check backend/services/paper_trading_v2/market_data.py backend/tests/paper_trading_v2/test_market_data.py` -> All checks passed。
- `rtk git diff --check` -> passed。
- `rtk python -m nox -s l0` -> passed。
- `rtk python -m nox -s validation_module_registry_l0` -> passed。
- `rtk cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"` -> passed；`paper_v2_backend` 643 passed, 1 skipped, 1 deselected；data quality smoke 仅保留既有 legacy ledger consistency WARN。

## 生产门禁

- `production_ddl_gate=noop`。
- `production_frontend_dependency_gate=noop`。
- `production_backend_dependency_gate=noop`。
- 未启动、重启或停止任何服务；未写生产 DB；本次只是读取运行库确认 `market.stock_st` 可用性和语义。
