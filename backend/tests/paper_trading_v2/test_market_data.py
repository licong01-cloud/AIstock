from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.paper_trading_v2.market_data import (
    DailySuspendStatus,
    DailyStStatus,
    DbStStatusProvider,
    DbSuspendStatusProvider,
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
    PreTradeTradabilityProvider,
    PreviousClose,
    fetch_tdx_realtime_quotes,
    quote_tradability_evidence,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.limit_price_provider import DailyLimitPrice


class FakeLimitProvider:
    def __init__(self, *, pre_close: float | None = 10.0) -> None:
        self.pre_close = pre_close
        self.calls: list[tuple[str, date]] = []

    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        self.calls.append((symbol, trade_date))
        return DailyLimitPrice(
            symbol=symbol,
            trade_date=trade_date,
            pre_close=self.pre_close,
            up_limit=11.0,
            down_limit=9.0,
        )


class MissingLimitProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, date]] = []

    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        self.calls.append((symbol, trade_date))
        raise DataUnavailableError(
            "missing limit price rows in market.stk_limit",
            context={"symbol": symbol, "trade_date": trade_date.isoformat()},
        )


class FakeSuspendProvider:
    def __init__(self, *, suspended: bool = False) -> None:
        self.suspended = suspended

    def get_suspend_status(self, symbol: str, trade_date: date) -> DailySuspendStatus:
        return DailySuspendStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_suspended=self.suspended,
            suspend_type="S" if self.suspended else None,
        )


class FakePreviousCloseProvider:
    def __init__(self, *, pre_close: float | None = 10.0) -> None:
        self.pre_close = pre_close

    def get_previous_close(self, symbol: str, trade_date: date) -> PreviousClose:
        if self.pre_close is None:
            raise DataUnavailableError("pre_close is required for minute execution context")
        return PreviousClose(
            symbol=symbol,
            trade_date=trade_date,
            previous_trade_date=trade_date - timedelta(days=1),
            pre_close=self.pre_close,
            source="test.previous_close_provider",
        )


class FakeStStatusProvider:
    def __init__(self, *, is_st: bool = False) -> None:
        self.is_st = is_st

    def get_st_status(self, symbol: str, trade_date: date) -> DailyStStatus:
        return DailyStStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_st=self.is_st,
            source="test.stock_st",
        )


class MissingStStatusProvider:
    def get_st_status(self, symbol: str, trade_date: date) -> DailyStStatus:
        raise DataUnavailableError(
            "ST status is unavailable",
            context={
                "reason_code": "ST_STATUS_UNAVAILABLE",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "source": "test.stock_st",
            },
        )


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.params_sql = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params):
        self.params_sql = sql
        self.params = params

    def fetchone(self):
        return self.row


class FakeConn:
    def __init__(self, row):
        self.cursor_obj = FakeCursor(row)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def cursor(self):
        return self.cursor_obj


def make_raw_bars(count: int = 31, *, trade_date: date = date(2024, 1, 2)) -> list[dict]:
    start = datetime(trade_date.year, trade_date.month, trade_date.day, 9, 31)
    return [
        {
            "time": start + timedelta(minutes=i),
            "open": 10.0 + i * 0.01,
            "high": 10.2 + i * 0.01,
            "low": 9.9 + i * 0.01,
            "close": 10.1 + i * 0.01,
            "volume": 1000,
            "amount": 1_000_000.0,
        }
        for i in range(count)
    ]


def make_tdx_quote(
    *,
    server_time: str | None,
    close: float = 10_000,
    pre_close: float = 10_000,
    open_price: float = 10_000,
    high_price: float = 10_000,
    low_price: float = 10_000,
    total_hand: float = 100,
    bid_price: float = 9_990,
    bid_volume: float = 1_000,
    ask_price: float = 10_000,
    ask_volume: float = 1_000,
) -> dict:
    quote = {
        "K": {
            "Close": close,
            "Last": pre_close,
            "Open": open_price,
            "High": high_price,
            "Low": low_price,
        },
        "TotalHand": total_hand,
        "Amount": 1_000_000.0,
        "BuyLevel": [{"Price": bid_price, "Number": bid_volume}],
        "SellLevel": [{"Price": ask_price, "Number": ask_volume}],
    }
    if server_time is not None:
        quote["ServerTime"] = server_time
    return quote


def pre_trade_provider(quote: dict, *, is_st: bool = False) -> PreTradeTradabilityProvider:
    return PreTradeTradabilityProvider(
        suspend_status_provider=FakeSuspendProvider(),
        realtime_quote_fetcher=lambda _symbols: {"000001.SZ": quote},
        realtime_quote_source="TDX_REALTIME.batch_quote",
        st_status_provider=FakeStStatusProvider(is_st=is_st),
    )


def pre_trade_provider_with_st_source(quote: dict, st_status_provider) -> PreTradeTradabilityProvider:
    return PreTradeTradabilityProvider(
        suspend_status_provider=FakeSuspendProvider(),
        realtime_quote_fetcher=lambda _symbols: {"000001.SZ": quote},
        realtime_quote_source="TDX_REALTIME.batch_quote",
        st_status_provider=st_status_provider,
    )


def test_tdx_market_data_provider_builds_minute_input_with_observed_context() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(31),
    )

    result = provider.load_symbol_input(
        symbol="000001.SZ",
        trade_date=date(2024, 1, 2),
        source=MinuteDataSource.TDX_REALTIME,
        min_bars=31,
    )

    assert len(result.minute_bars) == 31
    assert result.minute_bars[0].limit_up == 11.0
    assert result.minute_bars[0].volume == 100_000
    assert result.market_context["prev_close"] == 10.0
    assert result.market_context["price_basis"] == "raw"
    assert result.market_context["limit_price_basis"] == "raw"
    assert result.market_context["limit_price_source"].startswith(
        "derived_from_previous_close.injected_limit_provider.pre_close.a_share_board_limit_pct_0.10"
    )
    assert result.market_context["prev_close_basis"] == "raw"
    assert result.market_context["observed_only"] is True
    assert len(result.market_context["full_day_close"]) == 31


def test_market_data_provider_uses_explicit_suspend_provider_when_required() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        suspend_status_provider=FakeSuspendProvider(suspended=True),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(31),
    )

    result = provider.load_symbol_input(
        symbol="000001.SZ",
        trade_date=date(2024, 1, 2),
        source=MinuteDataSource.TDX_REALTIME,
        min_bars=31,
        require_suspend_status=True,
    )

    assert all(bar.is_suspended for bar in result.minute_bars)
    assert result.market_context["suspend_status"]["is_suspended"] is True


def test_db_suspend_status_provider_reads_suspend_d_rows() -> None:
    provider = DbSuspendStatusProvider(
        conn_factory=lambda: FakeConn(("S", "09:30-10:00")),
    )

    status = provider.get_suspend_status("000001.SZ", date(2024, 1, 2))

    assert status.is_suspended is True
    assert status.suspend_type == "S"
    assert status.suspend_timing == "09:30-10:00"


def test_db_suspend_status_provider_treats_no_suspend_row_as_active() -> None:
    provider = DbSuspendStatusProvider(conn_factory=lambda: FakeConn(None))

    status = provider.get_suspend_status("000001.SZ", date(2024, 1, 2))

    assert status.is_suspended is False
    assert status.suspend_type is None


def test_db_st_status_provider_uses_latest_daily_snapshot_not_old_rows() -> None:
    conn = FakeConn((None, None, None, date(2026, 6, 22)))
    provider = DbStStatusProvider(conn_factory=lambda: conn)

    status = provider.get_st_status("000001.SZ", date(2026, 6, 22))

    assert status.is_st is False
    assert "latest_stock_st_snapshot" in conn.cursor_obj.params_sql
    assert "ann_date = latest.latest_ann_date" in conn.cursor_obj.params_sql
    assert conn.cursor_obj.params == (
        date(2026, 6, 22),
        "000001.SZ",
        date(2026, 6, 22),
        date(2026, 6, 22),
    )


def test_db_st_status_provider_fails_loud_when_source_snapshot_is_empty() -> None:
    provider = DbStStatusProvider(conn_factory=lambda: FakeConn((None, None, None, None)))

    with pytest.raises(DataUnavailableError) as exc_info:
        provider.get_st_status("000001.SZ", date(2026, 6, 22))

    assert exc_info.value.context["reason_code"] == "ST_STATUS_SOURCE_EMPTY"
    assert exc_info.value.context["table"] == "market.stock_st"


def test_db_st_status_provider_fails_loud_when_source_query_fails() -> None:
    def failing_conn_factory():
        raise RuntimeError("undefined_table market.stock_st")

    provider = DbStStatusProvider(conn_factory=failing_conn_factory)

    with pytest.raises(DataUnavailableError) as exc_info:
        provider.get_st_status("000001.SZ", date(2026, 6, 22))

    assert exc_info.value.context["reason_code"] == "ST_STATUS_QUERY_FAILED"
    assert exc_info.value.context["table"] == "market.stock_st"
    assert "undefined_table" in str(exc_info.value.__cause__)


def test_tdx_market_data_provider_fails_when_31_bars_are_required_but_missing() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(30),
    )

    with pytest.raises(DataUnavailableError, match="insufficient minute bars"):
        provider.load_symbol_input(
            symbol="000001.SZ",
            trade_date=date(2024, 1, 2),
            source=MinuteDataSource.TDX_REALTIME,
            min_bars=31,
        )


def test_tdx_market_data_provider_fails_when_prev_close_is_missing() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(pre_close=None),
        previous_close_provider=FakePreviousCloseProvider(pre_close=None),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(31),
    )

    with pytest.raises(DataUnavailableError, match="pre_close is required"):
        provider.load_symbol_input(
            symbol="000001.SZ",
            trade_date=date(2024, 1, 2),
            source=MinuteDataSource.TDX_REALTIME,
            min_bars=31,
        )


def test_realtime_market_data_uses_explicit_previous_close_provider_for_derived_limits() -> None:
    limit_provider = FakeLimitProvider(pre_close=None)
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=limit_provider,
        previous_close_provider=FakePreviousCloseProvider(pre_close=9.8),
        st_status_provider=FakeStStatusProvider(is_st=False),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(31),
    )

    result = provider.load_symbol_input(
        symbol="000001.SZ",
        trade_date=date(2024, 1, 2),
        source=MinuteDataSource.TDX_REALTIME,
        min_bars=31,
    )

    assert limit_provider.calls == []
    assert result.market_context["prev_close"] == 9.8
    assert result.market_context["prev_close_source"] == "test.previous_close_provider"
    assert result.market_context["limit_up"] == pytest.approx(10.78)
    assert result.market_context["limit_down"] == pytest.approx(8.82)
    assert all(bar.limit_up == pytest.approx(10.78) and bar.limit_down == pytest.approx(8.82) for bar in result.minute_bars)


def test_realtime_market_data_derives_limit_prices_from_previous_close_without_stk_limit() -> None:
    limit_provider = MissingLimitProvider()
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=limit_provider,  # type: ignore[arg-type]
        previous_close_provider=FakePreviousCloseProvider(pre_close=10.0),
        st_status_provider=FakeStStatusProvider(is_st=False),
        tdx_fetcher=lambda _symbol, trade_date: make_raw_bars(31, trade_date=trade_date),
    )

    result = provider.load_symbol_input(
        symbol="001210.SZ",
        trade_date=date(2026, 6, 16),
        source=MinuteDataSource.TDX_REALTIME,
        min_bars=31,
    )

    assert limit_provider.calls == []
    assert result.market_context["prev_close"] == 10.0
    assert result.market_context["prev_close_source"] == "test.previous_close_provider"
    assert result.market_context["limit_price_source"].startswith(
        "derived_from_previous_close.test.previous_close_provider.a_share_board_limit_pct_0.10"
    )
    assert result.market_context["limit_up"] == pytest.approx(11.0)
    assert result.market_context["limit_down"] == pytest.approx(9.0)
    assert all(bar.limit_up == pytest.approx(11.0) and bar.limit_down == pytest.approx(9.0) for bar in result.minute_bars)


def test_realtime_market_data_uses_st_five_percent_limit_when_deriving_limits() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=MissingLimitProvider(),  # type: ignore[arg-type]
        previous_close_provider=FakePreviousCloseProvider(pre_close=10.0),
        st_status_provider=FakeStStatusProvider(is_st=True),
        tdx_fetcher=lambda _symbol, trade_date: make_raw_bars(31, trade_date=trade_date),
    )

    result = provider.load_symbol_input(
        symbol="001210.SZ",
        trade_date=date(2026, 6, 16),
        source=MinuteDataSource.TDX_REALTIME,
        min_bars=31,
    )

    assert result.market_context["limit_up"] == pytest.approx(10.5)
    assert result.market_context["limit_down"] == pytest.approx(9.5)
    assert "a_share_board_limit_pct_0.05.test.stock_st" in result.market_context["limit_price_source"]
    assert all(bar.limit_up == pytest.approx(10.5) and bar.limit_down == pytest.approx(9.5) for bar in result.minute_bars)


def test_realtime_market_data_fails_closed_when_st_status_is_unavailable() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=MissingLimitProvider(),  # type: ignore[arg-type]
        previous_close_provider=FakePreviousCloseProvider(pre_close=10.0),
        st_status_provider=MissingStStatusProvider(),
        tdx_fetcher=lambda _symbol, trade_date: make_raw_bars(31, trade_date=trade_date),
    )

    with pytest.raises(DataUnavailableError) as exc_info:
        provider.load_symbol_input(
            symbol="001210.SZ",
            trade_date=date(2026, 6, 16),
            source=MinuteDataSource.TDX_REALTIME,
            min_bars=31,
        )

    assert exc_info.value.context["reason_code"] == "ST_STATUS_UNAVAILABLE"


def test_realtime_market_data_uses_chinext_limit_pct_when_deriving_limits() -> None:
    limit_provider = MissingLimitProvider()
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=limit_provider,  # type: ignore[arg-type]
        previous_close_provider=FakePreviousCloseProvider(pre_close=10.0),
        st_status_provider=FakeStStatusProvider(is_st=False),
        tdx_fetcher=lambda _symbol, trade_date: make_raw_bars(31, trade_date=trade_date),
    )

    result = provider.load_symbol_input(
        symbol="300001.SZ",
        trade_date=date(2026, 6, 16),
        source=MinuteDataSource.TDX_REALTIME,
        min_bars=31,
    )

    assert limit_provider.calls == []
    assert result.market_context["limit_up"] == pytest.approx(12.0)
    assert result.market_context["limit_down"] == pytest.approx(8.0)
    assert "a_share_board_limit_pct_0.20" in result.market_context["limit_price_source"]


def test_realtime_observed_intraday_derives_limit_prices_from_previous_close_without_stk_limit() -> None:
    limit_provider = MissingLimitProvider()
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=limit_provider,  # type: ignore[arg-type]
        previous_close_provider=FakePreviousCloseProvider(pre_close=10.0),
        st_status_provider=FakeStStatusProvider(is_st=False),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(5, trade_date=date(2026, 6, 16)),
    )

    result = provider.load_observed_intraday(
        symbol="002138.SZ",
        trade_date=date(2026, 6, 16),
        source=MinuteDataSource.TDX_REALTIME,
        until_time=datetime(2026, 6, 16, 9, 33),
    )

    assert limit_provider.calls == []
    assert result.market_context["feed_mode"] == "observed_intraday"
    assert result.market_context["limit_up"] == pytest.approx(11.0)
    assert result.market_context["limit_down"] == pytest.approx(9.0)
    assert all(bar.limit_up == pytest.approx(11.0) and bar.limit_down == pytest.approx(9.0) for bar in result.minute_bars)


def test_realtime_market_data_fails_fast_when_previous_close_is_missing_for_derived_limits() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=MissingLimitProvider(),  # type: ignore[arg-type]
        previous_close_provider=FakePreviousCloseProvider(pre_close=None),
        st_status_provider=FakeStStatusProvider(is_st=False),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(31),
    )

    with pytest.raises(DataUnavailableError, match="pre_close is required"):
        provider.load_symbol_input(
            symbol="001210.SZ",
            trade_date=date(2026, 6, 16),
            source=MinuteDataSource.TDX_REALTIME,
            min_bars=31,
        )


def test_db_historical_still_requires_stk_limit_rows_without_realtime_derivation() -> None:
    class EmptyDbConn:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def cursor(self):
            raise AssertionError("historical DB minute rows should not be queried before stk_limit")

    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=MissingLimitProvider(),  # type: ignore[arg-type]
        previous_close_provider=FakePreviousCloseProvider(pre_close=10.0),
        st_status_provider=FakeStStatusProvider(is_st=False),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(31),
        conn_factory=lambda: EmptyDbConn(),
    )

    with pytest.raises(DataUnavailableError, match="missing limit price rows"):
        provider.load_symbol_input(
            symbol="001210.SZ",
            trade_date=date(2026, 6, 16),
            source=MinuteDataSource.DB_HISTORICAL,
            min_bars=1,
        )


def test_tdx_market_data_provider_fails_on_invalid_bar_price() -> None:
    raw_bars = make_raw_bars(31)
    raw_bars[0]["close"] = 0
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: raw_bars,
    )

    with pytest.raises(DataUnavailableError, match="close must be positive"):
        provider.load_symbol_input(
            symbol="000001.SZ",
            trade_date=date(2024, 1, 2),
            source=MinuteDataSource.TDX_REALTIME,
            min_bars=31,
        )


def test_observed_intraday_filters_future_bars_without_fabrication() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(5),
    )

    result = provider.load_observed_intraday(
        symbol="000001.SZ",
        trade_date=date(2024, 1, 2),
        source=MinuteDataSource.TDX_REALTIME,
        until_time=datetime(2024, 1, 2, 9, 33),
    )

    assert [bar.bar_time for bar in result.minute_bars] == [
        datetime(2024, 1, 2, 9, 31),
        datetime(2024, 1, 2, 9, 32),
        datetime(2024, 1, 2, 9, 33),
    ]
    assert result.market_context["feed_mode"] == "observed_intraday"
    assert result.market_context["observed_bar_count"] == 3


def test_observed_intraday_can_return_empty_waiting_input() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: [],
    )

    result = provider.load_observed_intraday(
        symbol="000001.SZ",
        trade_date=date(2024, 1, 2),
        source=MinuteDataSource.TDX_REALTIME,
        until_time=datetime(2024, 1, 2, 9, 30),
    )

    assert result.minute_bars == []
    assert result.market_context["observed_bar_count"] == 0


def test_load_new_bars_uses_strict_cursor() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(5),
    )

    bars = provider.load_new_bars(
        symbol="000001.SZ",
        trade_date=date(2024, 1, 2),
        source=MinuteDataSource.TDX_REALTIME,
        after_time=datetime(2024, 1, 2, 9, 32),
        until_time=datetime(2024, 1, 2, 9, 34),
    )

    assert [bar.bar_time for bar in bars] == [
        datetime(2024, 1, 2, 9, 33),
        datetime(2024, 1, 2, 9, 34),
    ]


def test_latest_available_bar_time_uses_common_symbol_time() -> None:
    def fetcher(symbol: str, trade_date: date):
        return make_raw_bars(4 if symbol == "000001.SZ" else 3, trade_date=trade_date)

    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=fetcher,
    )

    latest = provider.latest_available_bar_time(
        symbols=["000001.SZ", "000002.SZ"],
        trade_date=date(2024, 1, 2),
        source=MinuteDataSource.TDX_REALTIME,
        as_of_time=datetime(2024, 1, 2, 9, 40),
    )

    assert latest == datetime(2024, 1, 2, 9, 33)


def test_live_feed_rejects_db_source_instead_of_fallback() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(5),
    )

    with pytest.raises(DataUnavailableError, match="TDX_REALTIME"):
        provider.load_observed_intraday(
            symbol="000001.SZ",
            trade_date=date(2024, 1, 2),
            source=MinuteDataSource.DB_HISTORICAL,
            until_time=datetime(2024, 1, 2, 9, 33),
        )


def test_completed_day_rejects_realtime_source_instead_of_fallback() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(5),
    )

    with pytest.raises(DataUnavailableError, match="historical DB"):
        provider.load_completed_day(
            symbol="000001.SZ",
            trade_date=date(2024, 1, 2),
            source=MinuteDataSource.TDX_REALTIME,
            expected_bars=5,
        )


def test_pre_trade_tdx_quote_fails_closed_when_timestamp_is_missing() -> None:
    provider = pre_trade_provider(make_tdx_quote(server_time=None))

    with pytest.raises(DataUnavailableError) as exc_info:
        provider.get_statuses(
            ["000001.SZ"],
            date(2026, 6, 16),
            require_realtime_quote=True,
            as_of_time=datetime(2026, 6, 16, 10, 0, 0),
            side_by_symbol={"000001.SZ": "BUY"},
        )

    assert exc_info.value.context["reason_code"] == "REALTIME_QUOTE_TIMESTAMP_MISSING"
    assert exc_info.value.context["quote_source"] == "TDX_REALTIME.batch_quote"


def test_pre_trade_tdx_quote_fails_closed_when_timestamp_is_stale() -> None:
    provider = pre_trade_provider(make_tdx_quote(server_time="2026-06-16 09:30:00"))

    with pytest.raises(DataUnavailableError) as exc_info:
        provider.get_statuses(
            ["000001.SZ"],
            date(2026, 6, 16),
            require_realtime_quote=True,
            as_of_time=datetime(2026, 6, 16, 9, 36, 0),
            side_by_symbol={"000001.SZ": "BUY"},
        )

    assert exc_info.value.context["reason_code"] == "REALTIME_QUOTE_STALE"
    assert exc_info.value.context["quote_age_seconds"] == pytest.approx(360.0)


@pytest.mark.parametrize(
    ("server_time", "trade_date", "as_of_time", "expected_timestamp"),
    [
        ("9594403", date(2026, 6, 16), datetime(2026, 6, 16, 9, 59, 45), "2026-06-16T09:59:44.030000"),
        ("10151103", date(2026, 6, 16), datetime(2026, 6, 16, 10, 15, 12), "2026-06-16T10:15:11.030000"),
        ("10158777", date(2026, 6, 16), datetime(2026, 6, 16, 10, 15, 30), "2026-06-16T10:15:00"),
        ("14999733", date(2026, 6, 16), datetime(2026, 6, 16, 15, 0, 0), "2026-06-16T14:59:00"),
        ("13990274", date(2026, 7, 2), datetime(2026, 7, 2, 14, 1, 0), "2026-07-02T13:59:00"),
        ("13984048", date(2026, 7, 2), datetime(2026, 7, 2, 14, 1, 0), "2026-07-02T13:59:00"),
        ("14993094", date(2026, 7, 2), datetime(2026, 7, 2, 15, 0, 0), "2026-07-02T14:59:00"),
        ("14993374", date(2026, 7, 2), datetime(2026, 7, 2, 15, 0, 0), "2026-07-02T14:59:00"),
    ],
)
def test_pre_trade_tdx_quote_accepts_compact_servertime_with_centiseconds(
    server_time: str,
    trade_date: date,
    as_of_time: datetime,
    expected_timestamp: str,
) -> None:
    provider = pre_trade_provider(make_tdx_quote(server_time=server_time))

    statuses = provider.get_statuses(
        ["000001.SZ"],
        trade_date,
        require_realtime_quote=True,
        as_of_time=as_of_time,
        side_by_symbol={"000001.SZ": "BUY"},
    )

    status = statuses["000001.SZ"]
    assert status["is_tradable"] is True
    assert status["reason_code"] == "OK"
    assert status["quote_evidence"]["quote_timestamp"] == expected_timestamp


@pytest.mark.parametrize("server_time", ["14608733", "14968733", "24999733", "20260702", "123456789"])
def test_pre_trade_tdx_quote_fails_closed_for_invalid_compact_servertime(server_time: str) -> None:
    provider = pre_trade_provider(make_tdx_quote(server_time=server_time))

    with pytest.raises(DataUnavailableError) as exc_info:
        provider.get_statuses(
            ["000001.SZ"],
            date(2026, 6, 16),
            require_realtime_quote=True,
            as_of_time=datetime(2026, 6, 16, 15, 0, 0),
            side_by_symbol={"000001.SZ": "BUY"},
        )

    assert exc_info.value.context["reason_code"] == "REALTIME_QUOTE_TIMESTAMP_INVALID"


def test_pre_trade_tdx_quote_stale_guard_still_applies_after_compact_sentinel_clamp() -> None:
    provider = pre_trade_provider(make_tdx_quote(server_time="13984048"))

    with pytest.raises(DataUnavailableError) as exc_info:
        provider.get_statuses(
            ["000001.SZ"],
            date(2026, 7, 2),
            require_realtime_quote=True,
            as_of_time=datetime(2026, 7, 2, 14, 5, 1),
            side_by_symbol={"000001.SZ": "BUY"},
        )

    assert exc_info.value.context["reason_code"] == "REALTIME_QUOTE_STALE"
    assert exc_info.value.context["raw_timestamp"] == "13984048"
    assert exc_info.value.context["quote_timestamp"] == "2026-07-02T13:59:00"
    assert exc_info.value.context["quote_age_seconds"] == pytest.approx(361.0)


def test_fetch_tdx_realtime_quotes_chunks_batch_quote_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    class FakeResponse:
        def __init__(self, codes: list[str]) -> None:
            self.codes = list(codes)

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "code": 0,
                    "data": [
                        {
                            "Code": code[-6:],
                            "Exchange": code[:2],
                            "ServerTime": "10151103",
                        }
                        for code in self.codes
                ],
            }

    def fake_post(_url: str, *, json: dict, timeout: int):
        assert timeout == 5
        codes = list(json["codes"])
        calls.append(codes)
        assert len(codes) <= 50
        return FakeResponse(codes)

    monkeypatch.setattr("backend.services.paper_trading_v2.market_data.requests.post", fake_post)
    symbols = [f"{index:06d}.SZ" for index in range(1, 86)]

    quotes = fetch_tdx_realtime_quotes(symbols)

    assert [len(call) for call in calls] == [50, 35]
    assert len(quotes) == 85
    assert quotes["000001.SZ"]["ServerTime"] == "10151103"


def test_pre_trade_tdx_quote_fails_closed_when_st_source_is_unavailable() -> None:
    provider = pre_trade_provider_with_st_source(
        make_tdx_quote(server_time="2026-06-16 09:34:00"),
        MissingStStatusProvider(),
    )

    with pytest.raises(DataUnavailableError) as exc_info:
        provider.get_statuses(
            ["000001.SZ"],
            date(2026, 6, 16),
            require_realtime_quote=True,
            as_of_time=datetime(2026, 6, 16, 9, 34, 30),
        )

    assert exc_info.value.context["reason_code"] == "ST_STATUS_UNAVAILABLE"
    assert exc_info.value.context["source"] == "test.stock_st"


def test_pre_trade_tdx_quote_blocks_buy_at_limit_up_with_reason_code() -> None:
    provider = pre_trade_provider(
        make_tdx_quote(
            server_time="2026-06-16 10:00:00",
            close=11_000,
            pre_close=10_000,
            high_price=11_000,
            ask_price=0,
            ask_volume=0,
        )
    )

    statuses = provider.get_statuses(
        ["000001.SZ"],
        date(2026, 6, 16),
        require_realtime_quote=True,
        as_of_time=datetime(2026, 6, 16, 10, 0, 0),
        side_by_symbol={"000001.SZ": "BUY"},
    )

    status = statuses["000001.SZ"]
    assert status["is_tradable"] is False
    assert status["reason_code"] == "LIMIT_UP_BUY_BLOCKED"
    assert status["quote_evidence"]["limit_up"] == pytest.approx(11_000)
    assert status["quote_evidence"]["blocked_sides"] == ["BUY"]


def test_pre_trade_limit_up_without_known_side_preserves_directional_evidence() -> None:
    provider = pre_trade_provider(
        make_tdx_quote(
            server_time="2026-06-16 10:00:00",
            close=11_000,
            pre_close=10_000,
            high_price=11_000,
            ask_price=0,
            ask_volume=0,
        )
    )

    status = provider.get_statuses(
        ["000001.SZ"],
        date(2026, 6, 16),
        require_realtime_quote=True,
        as_of_time=datetime(2026, 6, 16, 10, 0, 0),
    )["000001.SZ"]

    assert status["is_tradable"] is True
    assert status["reason_code"] == "OK"
    assert status["quote_evidence"]["blocked_sides"] == ["BUY"]
    assert status["quote_evidence"]["limit_state_reason_code"] == "REALTIME_QUOTE_LIMIT_STATE_REQUIRES_SIDE"


def test_pre_trade_tdx_quote_blocks_sell_at_limit_down_with_reason_code() -> None:
    provider = pre_trade_provider(
        make_tdx_quote(
            server_time="2026-06-16 10:00:00",
            close=9_000,
            pre_close=10_000,
            low_price=9_000,
            bid_price=0,
            bid_volume=0,
        )
    )

    statuses = provider.get_statuses(
        ["000001.SZ"],
        date(2026, 6, 16),
        require_realtime_quote=True,
        as_of_time=datetime(2026, 6, 16, 10, 0, 0),
        side_by_symbol={"000001.SZ": "SELL"},
    )

    status = statuses["000001.SZ"]
    assert status["is_tradable"] is False
    assert status["reason_code"] == "LIMIT_DOWN_SELL_BLOCKED"
    assert status["quote_evidence"]["limit_down"] == pytest.approx(9_000)
    assert status["quote_evidence"]["blocked_sides"] == ["SELL"]


def test_pre_trade_miniqmt_quote_uses_yuan_basis_for_limit_range() -> None:
    payload = quote_tradability_evidence(
        symbol="603303.SH",
        quote={
            "price_basis": "yuan",
            "lastPrice": 30.23,
            "pre_close": 30.14,
            "open": 30.27,
            "high": 31.8,
            "low": 29.01,
            "volume": 66974,
            "amount": 203373536,
            "bid_price_1": 30.23,
            "bid_volume_1": 108,
            "ask_price_1": 30.26,
            "ask_volume_1": 17,
            "time": "2026-06-23 14:05:00",
        },
        source="MINIQMT_REALTIME.broker_quote",
        trade_date=date(2026, 6, 23),
        as_of_time=datetime(2026, 6, 23, 14, 5, 30),
        st_status_provider=FakeStStatusProvider(is_st=False),
    )

    assert payload["quote_price_basis"] == "yuan"
    assert payload["limit_up"] == pytest.approx(33.15)
    assert payload["limit_down"] == pytest.approx(27.13)
    assert payload["no_tradable_market"] is False
    assert payload["blocked_sides"] == []


def test_pre_trade_miniqmt_quote_ignores_degenerate_raw_li_limit_metadata() -> None:
    payload = quote_tradability_evidence(
        symbol="000048.SZ",
        quote={
            "quote_price_basis": "raw_li",
            "lastPrice": 20.66,
            "pre_close": 20.75,
            "open": 20.7,
            "high": 20.88,
            "low": 20.3,
            "volume": 12345,
            "amount": 25432100,
            "bid_price_1": 20.65,
            "bid_volume_1": 100,
            "ask_price_1": 20.66,
            "ask_volume_1": 100,
            "limit_up": 20.0,
            "limit_down": 20.0,
            "time": "2026-06-23 14:05:00",
        },
        source="MINIQMT_REALTIME.broker_quote",
        trade_date=date(2026, 6, 23),
        as_of_time=datetime(2026, 6, 23, 14, 5, 30),
        st_status_provider=FakeStStatusProvider(is_st=False),
    )

    assert payload["quote_price_basis"] == "yuan"
    assert payload["limit_up"] == pytest.approx(22.83)
    assert payload["limit_down"] == pytest.approx(18.68)
    assert payload["limit_down"] < payload["pre_close"] < payload["limit_up"]
    assert payload["no_tradable_market"] is False


def test_pre_trade_miniqmt_quote_fails_with_miniqmt_source_label_when_pre_close_missing() -> None:
    with pytest.raises(DataUnavailableError) as exc_info:
        quote_tradability_evidence(
            symbol="603303.SH",
            quote={
                "lastPrice": 30.23,
                "open": 30.27,
                "high": 31.8,
                "low": 29.01,
                "volume": 66974,
                "amount": 203373536,
                "bid_price_1": 30.23,
                "bid_volume_1": 108,
                "ask_price_1": 30.26,
                "ask_volume_1": 17,
                "time": "2026-06-23 14:05:00",
            },
            source="MINIQMT_REALTIME.broker_quote",
            trade_date=date(2026, 6, 23),
            as_of_time=datetime(2026, 6, 23, 14, 5, 30),
            st_status_provider=FakeStStatusProvider(is_st=False),
        )

    assert exc_info.value.context["reason_code"] == "REALTIME_QUOTE_PRE_CLOSE_MISSING"
    assert exc_info.value.context["quote_source"] == "MINIQMT_REALTIME.broker_quote"
    assert str(exc_info.value).startswith("MiniQMT realtime quote previous close")
    assert "TDX" not in str(exc_info.value)


def test_pre_trade_miniqmt_st_quote_uses_five_percent_limit() -> None:
    payload = quote_tradability_evidence(
        symbol="000048.SZ",
        quote={
            "quote_price_basis": "raw_li",
            "lastPrice": 20.66,
            "pre_close": 20.75,
            "open": 20.7,
            "high": 20.88,
            "low": 20.3,
            "volume": 12345,
            "amount": 25432100,
            "bid_price_1": 20.65,
            "bid_volume_1": 100,
            "ask_price_1": 20.66,
            "ask_volume_1": 100,
            "time": "2026-06-23 14:05:00",
        },
        source="MINIQMT_REALTIME.broker_quote",
        trade_date=date(2026, 6, 23),
        as_of_time=datetime(2026, 6, 23, 14, 5, 30),
        st_status_provider=FakeStStatusProvider(is_st=True),
    )

    assert payload["quote_price_basis"] == "yuan"
    assert payload["limit_pct"] == pytest.approx(0.05)
    assert payload["limit_up"] == pytest.approx(21.79)
    assert payload["limit_down"] == pytest.approx(19.71)
