from __future__ import annotations

from datetime import date, datetime

import pytest

from backend.models.analysis import StockQuote
from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.result_enrichment import (
    SelectionResultEnrichmentService,
    component_scores_with_display_fields,
    display_fields_from_component_scores,
)
from backend.services.trading_core.errors import DataUnavailableError


class _FakeNameResolver:
    def resolve(self, symbols):
        return {symbol: f"Name-{symbol}" for symbol in symbols}


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql, params) -> None:
        self.executed.append((sql, params))

    def fetchall(self):
        return [{"ts_code": "000001.SZ", "close_li": 12340, "volume_hand": 777}]


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self.cursor_obj = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


def _quote(symbol: str, *, price: float = 13.2) -> StockQuote:
    return StockQuote(
        symbol=symbol,
        name=f"TDX-{symbol}",
        current_price=price,
        pre_close=12.8,
        volume=123456,
        quote_source="TDX_REALTIME",
        quote_timestamp=datetime(2026, 5, 25, 10, 1, 2),
    )


def test_historical_selection_entry_price_uses_pit_reference_close_not_current_quote() -> None:
    cursor = _FakeCursor()
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: _FakeConn(cursor),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=_quote,
        today_provider=lambda: date(2026, 5, 25),
    )

    enriched = service.enrich_candidates(
        [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
        trade_date=date(2026, 5, 13),
        runtime_config={"point_in_time_context": {"reference_price_trade_date": "2026-05-12"}},
    )

    candidate = enriched[0]
    assert candidate.stock_name == "Name-000001.SZ"
    assert candidate.selection_entry_price == pytest.approx(12.34)
    assert candidate.reference_price == pytest.approx(12.34)
    assert candidate.selection_entry_price_source == "market.kline_daily_raw.close:2026-05-12"
    assert candidate.current_price == pytest.approx(13.2)
    assert candidate.current_price_source == "TDX_REALTIME"
    assert candidate.volume == pytest.approx(123456)
    assert cursor.executed[0][1][0] == date(2026, 5, 12)


def test_current_day_selection_entry_price_uses_tdx_quote_and_display_fields_persist() -> None:
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: pytest.fail("current-day enrichment must not query PIT daily rows"),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda symbol: _quote(symbol, price=15.6),
        today_provider=lambda: date(2026, 5, 25),
    )

    enriched = service.enrich_candidates(
        [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
        trade_date=date(2026, 5, 25),
        runtime_config={},
    )

    candidate = enriched[0]
    assert candidate.selection_entry_price == pytest.approx(15.6)
    assert candidate.reference_price == pytest.approx(15.6)
    assert candidate.current_price == pytest.approx(15.6)

    component_scores = component_scores_with_display_fields(candidate)
    display = display_fields_from_component_scores(component_scores)
    assert display["selection_entry_price"] == pytest.approx(15.6)
    assert display["current_price_display_only"] is True


def test_current_day_selection_fails_fast_when_tdx_quote_price_is_missing() -> None:
    service = SelectionResultEnrichmentService(
        conn_factory=lambda: pytest.fail("current-day enrichment must not query PIT daily rows"),
        symbol_name_resolver=_FakeNameResolver(),
        quote_fetcher=lambda symbol: StockQuote(symbol=symbol, name="missing-price", quote_source="TDX_REALTIME"),
        today_provider=lambda: date(2026, 5, 25),
    )

    with pytest.raises(DataUnavailableError, match="TDX quote price"):
        service.enrich_candidates(
            [SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1, reference_price=99.0)],
            trade_date=date(2026, 5, 25),
            runtime_config={},
        )
