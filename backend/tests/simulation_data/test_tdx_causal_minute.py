from __future__ import annotations

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pytest

from backend.services.simulation_data.contracts import MinuteDataSource, _canonical_json_sha256
from backend.services.simulation_data.daily_context import DailyTradingSymbolFactV1
from backend.services.simulation_data.tdx_causal_minute import TdxCausalMinuteProvider


TRADE_DATE = date(2026, 8, 28)


def _reference() -> dict[str, object]:
    row_payload = {
        "source": "market.stk_limit",
        "symbol": "000001.SZ",
        "trade_date": TRADE_DATE.isoformat(),
        "pre_close": 10.0,
        "up_limit": 11.0,
        "down_limit": 9.0,
        "price_basis": "raw",
    }
    fact = DailyTradingSymbolFactV1(
        symbol="000001.SZ",
        trade_date=TRADE_DATE,
        pre_close=10.0,
        up_limit=11.0,
        down_limit=9.0,
        stk_limit_row_hash=_canonical_json_sha256(row_payload),
        is_st=False,
        st_source="market.stock_st",
        st_evidence_hash="1" * 64,
        is_suspended=False,
        suspend_source="market.suspend_d",
        board="MAIN",
        lot_rule={"min_quantity": 100, "increment": 100},
    )
    return {
        "schema_version": "daily_trading_context_reference_v1",
        "context_id": "dtc_unit",
        "context_hash": "2" * 64,
        "trade_date": TRADE_DATE.isoformat(),
        "symbol_fact": fact.model_dump(mode="python"),
    }


def _row(minute: int) -> dict[str, object]:
    return {
        "time": datetime(2026, 8, 28, 9, minute),
        "open": 10.0,
        "high": 10.2,
        "low": 9.9,
        "close": 10.1,
        "volume": 12,
        "amount": 1212.0,
    }


def test_current_day_reads_only_tdx_and_filters_future_bars() -> None:
    calls: list[tuple[str, date]] = []

    def fetch(symbol: str, trade_date: date) -> list[dict[str, object]]:
        calls.append((symbol, trade_date))
        return [_row(31), _row(32), _row(33)]

    batch = TdxCausalMinuteProvider(fetcher=fetch).load(
        symbol="000001.SZ",
        trade_date=TRADE_DATE,
        observed_until=datetime(2026, 8, 28, 9, 32, tzinfo=ZoneInfo("Asia/Shanghai")),
        frozen_daily_fact=_reference(),
    )

    assert calls == [("000001.SZ", TRADE_DATE)]
    assert batch.source is MinuteDataSource.TDX_REALTIME
    assert [bar.bar_time.minute for bar in batch.bars] == [31, 32]
    assert all(bar.limit_up == 11.0 and bar.limit_down == 9.0 for bar in batch.bars)

    with pytest.raises(Exception, match="frozen"):
        batch.bars[0].close = 10.0

    tampered = batch.model_dump(mode="python")
    tampered["batch_hash"] = "0" * 64
    with pytest.raises(ValueError, match="batch_hash does not match content"):
        type(batch).model_validate(tampered)


def test_current_day_rejects_missing_frozen_daily_fact_before_fetch() -> None:
    provider = TdxCausalMinuteProvider(fetcher=lambda *_args: pytest.fail("TDX must not be called"))
    with pytest.raises(Exception, match="frozen daily symbol fact"):
        provider.load(
            symbol="000001.SZ",
            trade_date=TRADE_DATE,
            observed_until=datetime(2026, 8, 28, 9, 32, tzinfo=ZoneInfo("Asia/Shanghai")),
            frozen_daily_fact={},
        )


def test_causal_cutoff_normalizes_utc_to_asia_shanghai() -> None:
    batch = TdxCausalMinuteProvider(fetcher=lambda *_args: [_row(31), _row(32), _row(33)]).load(
        symbol="000001.SZ",
        trade_date=TRADE_DATE,
        observed_until=datetime(2026, 8, 28, 1, 32, tzinfo=UTC),
        frozen_daily_fact=_reference(),
    )

    assert [bar.bar_time.minute for bar in batch.bars] == [31, 32]


def test_current_day_rejects_naive_cutoff_before_fetch() -> None:
    provider = TdxCausalMinuteProvider(fetcher=lambda *_args: pytest.fail("TDX must not be called"))
    with pytest.raises(Exception, match="identity is invalid"):
        provider.load(
            symbol="000001.SZ",
            trade_date=TRADE_DATE,
            observed_until=datetime(2026, 8, 28, 9, 32),
            frozen_daily_fact=_reference(),
        )
