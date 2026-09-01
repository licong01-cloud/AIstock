from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.index_sources import (
    DatabaseTushareIndexSource,
    IndexProviderRateLimitTerminal,
    IndexProviderUnavailable,
)
from backend.services.dataset_release.profile import ResourcePolicy


class FakePool:
    policy = ResourcePolicy()

    def fetch_all_small(self, sql, params, *, max_rows):
        if "trading_calendar" in sql:
            return [(date(2026, 7, 30),), (date(2026, 7, 31),)]
        return [
            (
                params["ts_code"],
                date(2026, 7, 31),
                1.0,
                2.0,
                0.5,
                1.5,
                1.0,
                50.0,
                100.0,
                200.0,
            )
        ]


class FakeProvider:
    def __init__(self) -> None:
        self.calls = []

    def index_daily(self, **kwargs):
        self.calls.append(kwargs)
        return pd.DataFrame(
            [
                {
                    "ts_code": kwargs["ts_code"],
                    "trade_date": "20260730",
                    "open": 1,
                    "high": 2,
                    "low": 0.5,
                    "close": 1.5,
                    "pre_close": 1,
                    "pct_chg": 50,
                    "vol": 100,
                    "amount": 200,
                }
            ]
        )


def test_index_source_uses_independent_pool_and_tushare_contract() -> None:
    provider = FakeProvider()
    source = DatabaseTushareIndexSource(FakePool(), provider_factory=lambda: provider)
    definition = DOMESTIC_INDEX_DEFINITIONS[0]
    assert source.trading_dates(date(2026, 7, 30), date(2026, 7, 31)) == (
        date(2026, 7, 30),
        date(2026, 7, 31),
    )
    assert source.database_rows(definition, date(2026, 7, 30), date(2026, 7, 31))[0]["pre_close"] == 1.0
    rows = source.provider_rows(definition, date(2026, 7, 30), date(2026, 7, 31))
    assert rows[0]["trade_date"] == date(2026, 7, 30)
    assert provider.calls[0]["fields"].endswith("pct_chg,vol,amount")


def test_index_source_reports_provider_failure_without_fallback() -> None:
    source = DatabaseTushareIndexSource(
        FakePool(), provider_factory=lambda: (_ for _ in ()).throw(RuntimeError("offline"))
    )
    with pytest.raises(IndexProviderUnavailable, match="initialization failed"):
        source.provider_rows(DOMESTIC_INDEX_DEFINITIONS[0], date(2026, 7, 30), date(2026, 7, 31))


def test_index_source_rejects_non_tabular_provider() -> None:
    source = DatabaseTushareIndexSource(
        FakePool(), provider_factory=lambda: type("Provider", (), {"index_daily": lambda self, **_: []})()
    )
    with pytest.raises(IndexProviderUnavailable, match="not tabular"):
        source.provider_rows(DOMESTIC_INDEX_DEFINITIONS[0], date(2026, 7, 30), date(2026, 7, 31))


def test_index_source_treats_tushare_40203_as_retryable_waiting() -> None:
    class TerminalProvider:
        def index_daily(self, **_kwargs):
            error = RuntimeError("provider code=40203 request window exhausted")
            error.code = 40203
            raise error

    source = DatabaseTushareIndexSource(FakePool(), provider_factory=TerminalProvider)
    with pytest.raises(IndexProviderRateLimitTerminal) as raised:
        source.provider_rows(DOMESTIC_INDEX_DEFINITIONS[0], date(2026, 7, 30), date(2026, 7, 31))
    assert raised.value.code == "WAITING_PROVIDER_RATE_LIMIT_40203"
    assert raised.value.retryable is True
