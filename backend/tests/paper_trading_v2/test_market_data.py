from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.paper_trading_v2.market_data import (
    DailySuspendStatus,
    DailyStStatus,
    DbSuspendStatusProvider,
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
    PreTradeTradabilityProvider,
    PreviousClose,
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
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, _sql, params):
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
