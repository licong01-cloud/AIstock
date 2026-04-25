from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.paper_trading_v2.market_data import (
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
