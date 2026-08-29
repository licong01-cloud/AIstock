from datetime import UTC, date, datetime

from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.trading_core.models import AccountSnapshot, PositionLot


PORTFOLIO_ID = "paper_position_authority"


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
