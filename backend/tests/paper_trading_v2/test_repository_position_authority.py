from contextlib import contextmanager
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository, PaperTradingV2Repository
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import AccountSnapshot, PositionLot


PORTFOLIO_ID = "paper_position_authority"


def database_position_reader(rows):
    """Exercise the production row decoder without a database write."""
    class Cursor:
        def execute(self, sql, params):
            assert "position_authority" in sql

        def fetchall(self):
            return rows

    @contextmanager
    def cursor(**_kwargs):
        yield Cursor()

    @contextmanager
    def connection():
        yield SimpleNamespace(cursor=cursor)

    return PaperTradingV2Repository(conn_factory=connection)


def test_database_position_restore_preserves_persisted_lot_date():
    lot_day, snapshot_day = date(2026, 7, 9), date(2026, 7, 10)
    expected = _position("000001.SZ", 100, lot_day)
    row = {**expected.model_dump(), "trade_date": snapshot_day,
           "metadata": {"position_trade_date": lot_day.isoformat()}}
    actual = database_position_reader([row]).load_latest_positions(PORTFOLIO_ID, snapshot_day)
    assert actual == {expected.symbol: expected}


@pytest.mark.parametrize("metadata", [None, {}, {"legacy": True}])
def test_database_legacy_position_without_lot_date_retains_snapshot_date(metadata):
    expected = _position("000001.SZ", 100, date(2026, 7, 10))
    row = {**expected.model_dump(), "metadata": metadata}
    assert database_position_reader([row]).load_latest_positions(PORTFOLIO_ID, expected.trade_date) == {
        expected.symbol: expected}


@pytest.mark.parametrize("raw_date", [None, "", "not-a-date", "2026-07-11", "20260709", 20260709, True])
def test_database_corrupt_lot_date_fails_closed(raw_date):
    expected = _position("000001.SZ", 100, date(2026, 7, 10))
    row = {**expected.model_dump(), "metadata": {"position_trade_date": raw_date}}
    with pytest.raises(DataUnavailableError) as exc:
        database_position_reader([row]).load_latest_positions(PORTFOLIO_ID, expected.trade_date)
    assert exc.value.context["reason_code"] == "PAPER_V2_POSITION_TRADE_DATE_INVALID"


@pytest.mark.parametrize("metadata", [[], "not-an-object", 42])
def test_database_corrupt_position_metadata_fails_closed(metadata):
    expected = _position("000001.SZ", 100, date(2026, 7, 10))
    row = {**expected.model_dump(), "metadata": metadata}
    with pytest.raises(DataUnavailableError) as exc:
        database_position_reader([row]).load_latest_positions(PORTFOLIO_ID, expected.trade_date)
    assert exc.value.context["reason_code"] == "PAPER_V2_POSITION_TRADE_DATE_INVALID"


def _position(symbol: str, quantity: int, trade_date: date) -> PositionLot:
    return PositionLot(
        portfolio_id=PORTFOLIO_ID,
        symbol=symbol,
        quantity=quantity,
        available_quantity=quantity,
        avg_cost=10.0,
        trade_date=trade_date,
    )


def _snapshot(trade_date: date, *, position_count: int) -> AccountSnapshot:
    return AccountSnapshot(
        portfolio_id=PORTFOLIO_ID,
        cash=100_000.0,
        market_value=float(position_count * 1_000),
        nav=100_000.0 + float(position_count * 1_000),
        snapshot_time=datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC),
    )


def test_latest_complete_snapshot_does_not_resurrect_symbols_from_older_runs() -> None:
    repo = InMemoryPaperTradingV2Repository()
    old_day = date(2026, 8, 27)
    anchor_day = date(2026, 8, 28)
    repo.save_positions(
        run_id="simrun_old",
        trade_date=old_day,
        positions=[_position("000001.SZ", 100, old_day), _position("000002.SZ", 200, old_day)],
        prices={"000001.SZ": 10.0, "000002.SZ": 10.0},
    )
    repo.save_daily_snapshot(
        run_id="simrun_old",
        trade_date=old_day,
        snapshot=_snapshot(old_day, position_count=2),
        metadata={"position_count": 2},
    )
    repo.save_positions(
        run_id="simrun_anchor",
        trade_date=anchor_day,
        positions=[_position("000002.SZ", 300, anchor_day)],
        prices={"000002.SZ": 10.0},
    )
    repo.save_daily_snapshot(
        run_id="simrun_anchor",
        trade_date=anchor_day,
        snapshot=_snapshot(anchor_day, position_count=1),
        metadata={"position_count": 1},
    )

    positions = repo.load_latest_positions(PORTFOLIO_ID, anchor_day)

    assert set(positions) == {"000002.SZ"}
    assert positions["000002.SZ"].quantity == 300


def test_latest_flat_snapshot_is_authoritative_over_older_positions() -> None:
    repo = InMemoryPaperTradingV2Repository()
    old_day = date(2026, 8, 27)
    flat_day = date(2026, 8, 28)
    repo.save_positions(
        run_id="simrun_old",
        trade_date=old_day,
        positions=[_position("000001.SZ", 100, old_day)],
        prices={"000001.SZ": 10.0},
    )
    repo.save_daily_snapshot(
        run_id="simrun_old",
        trade_date=old_day,
        snapshot=_snapshot(old_day, position_count=1),
        metadata={"position_count": 1},
    )
    repo.save_positions(run_id="simrun_flat", trade_date=flat_day, positions=[], prices={})
    repo.save_daily_snapshot(
        run_id="simrun_flat",
        trade_date=flat_day,
        snapshot=_snapshot(flat_day, position_count=0),
        metadata={"position_count": 0},
    )

    assert repo.load_latest_positions(PORTFOLIO_ID, flat_day) == {}


def test_legacy_position_rows_without_snapshot_use_one_latest_run() -> None:
    repo = InMemoryPaperTradingV2Repository()
    old_day = date(2026, 8, 27)
    latest_day = date(2026, 8, 28)
    repo.save_positions(
        run_id="legacy_old",
        trade_date=old_day,
        positions=[_position("000001.SZ", 100, old_day)],
        prices={"000001.SZ": 10.0},
    )
    repo.save_positions(
        run_id="legacy_latest",
        trade_date=latest_day,
        positions=[_position("000002.SZ", 200, latest_day)],
        prices={"000002.SZ": 10.0},
    )

    assert set(repo.load_latest_positions(PORTFOLIO_ID, latest_day)) == {"000002.SZ"}
