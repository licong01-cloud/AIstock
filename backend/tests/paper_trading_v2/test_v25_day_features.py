from __future__ import annotations

import math
from datetime import UTC, date, datetime
from typing import Any

import pytest

from backend.services.data_refresh_audit import DatasetRefreshStatus
from backend.services.paper_trading_v2.day_features import (
    DbV25DayFeatureProvider,
    V25_DAY_FEATURE_FIELDS,
    V25_DAY_FEATURE_SCHEMA_VERSION,
    V25DayFeatures,
)
from backend.services.paper_trading_v2.market_data import MinuteDataSource, PaperV2MinuteMarketDataProvider
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.limit_price_provider import DailyLimitPrice
from backend.tests.paper_trading_v2.test_market_data import make_raw_bars


class FakeAudit:
    def __init__(self, *, missing_dataset: str | None = None) -> None:
        self.missing_dataset = missing_dataset
        self.calls: list[tuple[str, date]] = []

    def require_success(self, *, dataset: str, trade_date: date, data_source=None, max_age_minutes=None):
        self.calls.append((dataset, trade_date))
        if dataset == self.missing_dataset:
            raise DataUnavailableError(
                "required dataset refresh status is missing",
                context={"dataset": dataset, "trade_date": trade_date.isoformat()},
            )
        return DatasetRefreshStatus(
            dataset=dataset,
            trade_date=trade_date,
            data_source="unit_test",
            status="success",
            row_count=10,
            refreshed_at=datetime(2024, 1, 3, tzinfo=UTC),
        )


class StaticV25Provider(DbV25DayFeatureProvider):
    def _previous_trading_day(self, before_date: date) -> date:
        if before_date == date(2024, 1, 3):
            return date(2024, 1, 2)
        if before_date == date(2024, 1, 2):
            return date(2024, 1, 1)
        raise AssertionError(before_date)

    def _load_stock_daily_row(self, symbol: str, trade_date: date) -> dict[str, Any]:
        if trade_date == date(2024, 1, 1):
            return {"open_li": 7000, "high_li": 8500, "low_li": 6900, "close_li": 8000, "volume_hand": 80, "amount_li": 800_000}
        return {"open_li": 9000, "high_li": 11000, "low_li": 8000, "close_li": 10000, "volume_hand": 100, "amount_li": 1_000_000}

    def _load_daily_basic_row(self, symbol: str, trade_date: date) -> dict[str, Any]:
        return {"turnover_rate": 5.0, "volume_ratio": 1.2, "pb": 2.0}

    def _load_moneyflow_row(self, symbol: str, trade_date: date) -> dict[str, Any]:
        return {"net_mf_amount": 50.0}

    def _load_index_row(self, trade_date: date) -> dict[str, Any]:
        return {"pct_chg": 2.5}

    def _load_sector_row(self, symbol: str, trade_date: date) -> dict[str, Any]:
        return {"sw2_pct_change": -1.0}


class FakeLimitProvider:
    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        return DailyLimitPrice(symbol=symbol, trade_date=trade_date, pre_close=10.0, up_limit=11.0, down_limit=9.0)


class FakeDayProvider:
    def load_day_features(self, *, symbol: str, trade_date: date) -> V25DayFeatures:
        return V25DayFeatures(
            symbol=symbol,
            trade_date=trade_date,
            feature_date=date(2024, 1, 2),
            values=[0.1] * 10,
            audit=[{"dataset": "unit_test", "status": "success"}],
        )


def test_db_v25_day_feature_provider_builds_ten_finite_features() -> None:
    audit = FakeAudit()
    provider = StaticV25Provider(conn_factory=lambda: None, refresh_audit=audit)

    features = provider.load_day_features(symbol="000001.SZ", trade_date=date(2024, 1, 3))

    assert features.schema_version == V25_DAY_FEATURE_SCHEMA_VERSION
    assert features.feature_date == date(2024, 1, 2)
    assert features.fields == V25_DAY_FEATURE_FIELDS
    assert len(features.values) == 10
    assert all(math.isfinite(value) for value in features.values)
    assert features.values[0] == pytest.approx(0.25)
    assert features.values[4] == pytest.approx(0.05)
    assert features.values[7] == pytest.approx(0.025)
    assert features.values[8] == pytest.approx(-0.01)
    assert features.values[9] == pytest.approx(0.05)
    assert ("kline_daily_raw", date(2024, 1, 1)) in audit.calls


def test_db_v25_day_feature_provider_fails_when_required_audit_is_missing() -> None:
    provider = StaticV25Provider(conn_factory=lambda: None, refresh_audit=FakeAudit(missing_dataset="sector_data"))

    with pytest.raises(DataUnavailableError, match="refresh status"):
        provider.load_day_features(symbol="000001.SZ", trade_date=date(2024, 1, 3))


def test_market_data_includes_day_features_only_when_required() -> None:
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        day_feature_provider=FakeDayProvider(),
        tdx_fetcher=lambda _symbol, trade_date: make_raw_bars(31, trade_date=trade_date),
    )

    without_features = provider.load_symbol_input(
        symbol="000001.SZ",
        trade_date=date(2024, 1, 3),
        source=MinuteDataSource.TDX_REALTIME,
        min_bars=31,
    )
    with_features = provider.load_symbol_input(
        symbol="000001.SZ",
        trade_date=date(2024, 1, 3),
        source=MinuteDataSource.TDX_REALTIME,
        min_bars=31,
        require_day_features=True,
    )

    assert "day_features" not in without_features.market_context
    assert with_features.market_context["day_features"] == [0.1] * 10
    assert with_features.market_context["day_features_schema_version"] == V25_DAY_FEATURE_SCHEMA_VERSION
