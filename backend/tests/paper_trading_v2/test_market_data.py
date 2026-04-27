from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.paper_trading_v2.market_data import (
    DailySuspendStatus,
    DbSuspendStatusProvider,
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.limit_price_provider import DailyLimitPrice


class FakeLimitProvider:
    def __init__(self, *, pre_close: float | None = 10.0) -> None:
        self.pre_close = pre_close

    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        return DailyLimitPrice(
            symbol=symbol,
            trade_date=trade_date,
            pre_close=self.pre_close,
            up_limit=11.0,
            down_limit=9.0,
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
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(31),
    )

    with pytest.raises(DataUnavailableError, match="pre_close is required"):
        provider.load_symbol_input(
            symbol="000001.SZ",
            trade_date=date(2024, 1, 2),
            source=MinuteDataSource.TDX_REALTIME,
            min_bars=31,
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
