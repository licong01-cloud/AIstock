from __future__ import annotations

from datetime import date

import pytest

from backend.services.simulation_data.historical_minute import HistoricalMinuteProvider
from backend.services.trading_core.errors import DataUnavailableError


class _PoisonConnectionFactory:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self):
        self.calls += 1
        raise AssertionError("current-day request must be rejected before database access")


def test_historical_provider_rejects_current_day_before_database_access() -> None:
    factory = _PoisonConnectionFactory()
    provider = HistoricalMinuteProvider(conn_factory=factory)

    with pytest.raises(DataUnavailableError, match="current or future trading day"):
        provider.load_completed_day(
            symbol="000001.SZ",
            trade_date=date(2026, 8, 28),
            current_trading_date=date(2026, 8, 28),
            frozen_daily_fact={},
        )

    assert factory.calls == 0


def test_data_layer_contains_no_market_writer_sql() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "services" / "simulation_data"
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.py"))
    for statement in ("INSERT INTO market.", "UPDATE market.", "DELETE FROM market."):
        assert statement not in source


def test_historical_provider_has_no_tdx_or_http_fallback() -> None:
    from inspect import getsource

    source = getsource(HistoricalMinuteProvider)
    assert "fetch_minute_kline_tdx" not in source
    assert "requests." not in source


def test_current_day_tdx_provider_has_zero_database_dependency() -> None:
    from inspect import getsource

    from backend.services.simulation_data.tdx_causal_minute import TdxCausalMinuteProvider

    source = getsource(TdxCausalMinuteProvider)
    assert "get_conn" not in source
    assert "market." not in source
    assert "kline_minute_raw" not in source
