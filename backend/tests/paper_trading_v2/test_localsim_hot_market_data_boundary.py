from __future__ import annotations

from datetime import date, datetime

import pytest

from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.simulation_runtime.models import canonical_json_sha256


TRADE_DATE = date(2026, 8, 21)


class PoisonProvider:
    def __getattr__(self, name: str):
        raise AssertionError(f"live path accessed forbidden provider: {name}")


def _frozen_reference() -> dict:
    row_hash = canonical_json_sha256(
        {
            "source": "market.stk_limit",
            "symbol": "000001.SZ",
            "trade_date": TRADE_DATE.isoformat(),
            "pre_close": 10.0,
            "up_limit": 10.77,
            "down_limit": 9.23,
            "price_basis": "raw",
        }
    )
    return {
        "schema_version": "daily_trading_context_reference_v1",
        "context_id": "dtc_0123456789abcdef",
        "context_hash": "1" * 64,
        "trade_date": TRADE_DATE.isoformat(),
        "symbol_set_hash": "2" * 64,
        "stk_limit_row_hash": row_hash,
        "source": "market.stk_limit",
        "symbol_fact": {
            "symbol": "000001.SZ",
            "trade_date": TRADE_DATE.isoformat(),
            "pre_close": 10.0,
            "up_limit": 10.77,
            "down_limit": 9.23,
            "price_basis": "raw",
            "stk_limit_row_hash": row_hash,
            "is_st": False,
            "st_source": "market.stock_st.latest_ann_date:2026-08-21",
            "st_evidence_hash": "4" * 64,
            "is_suspended": False,
            "suspend_type": None,
            "suspend_timing": None,
            "suspend_source": "market.suspend_d",
            "board": "MAIN",
            "lot_rule": {"min_quantity": 100, "increment": 100},
        },
    }


def test_live_observed_intraday_uses_frozen_stk_limit_and_tdx_only() -> None:
    raw_bars = [
        {
            "time": datetime(2026, 8, 21, 9, 31),
            "open": 10.1,
            "high": 10.2,
            "low": 10.0,
            "close": 10.15,
            "volume": 12,
            "amount": 1218.0,
        }
    ]
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=PoisonProvider(),  # type: ignore[arg-type]
        suspend_status_provider=PoisonProvider(),  # type: ignore[arg-type]
        previous_close_provider=PoisonProvider(),  # type: ignore[arg-type]
        st_status_provider=PoisonProvider(),  # type: ignore[arg-type]
        day_feature_provider=PoisonProvider(),  # type: ignore[arg-type]
        tdx_fetcher=lambda _symbol, _trade_date: raw_bars,
        conn_factory=lambda: (_ for _ in ()).throw(AssertionError("live path queried database")),
    )

    result = provider.load_observed_intraday(
        symbol="000001.SZ",
        trade_date=TRADE_DATE,
        source=MinuteDataSource.TDX_REALTIME,
        until_time=datetime(2026, 8, 21, 9, 31),
        require_suspend_status=True,
        frozen_daily_fact=_frozen_reference(),
    )

    assert result.market_context["limit_up"] == 10.77
    assert result.market_context["limit_down"] == 9.23
    assert result.market_context["limit_price_source"] == "market.stk_limit:frozen_daily_trading_context_v1"
    assert result.minute_bars[0].limit_up == 10.77


def test_live_observed_intraday_rejects_missing_frozen_daily_fact() -> None:
    provider = PaperV2MinuteMarketDataProvider(tdx_fetcher=lambda _symbol, _trade_date: [])

    with pytest.raises(DataUnavailableError) as error:
        provider.load_observed_intraday(
            symbol="000001.SZ",
            trade_date=TRADE_DATE,
            source=MinuteDataSource.TDX_REALTIME,
            until_time=datetime(2026, 8, 21, 9, 31),
        )

    assert error.value.context["reason_code"] == "LOCALSIM_DAILY_TRADING_FACT_MISSING"
