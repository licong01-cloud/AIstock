"""Tests for paper_v2 DW ETL capture fields (T5).

Covers the new columns added to ``paper_v2.fills``,
``paper_v2.positions`` and ``paper_v2.daily_snapshots`` per T3 + A2
BLOCKING analysis (commits f7c669d + d50d3c5):

  - fills: created_at, updated_at, intended_price, fill_market_context
  - positions: created_at, updated_at
  - daily_snapshots: created_at, updated_at

These tests target the InMemoryPaperTradingV2Repository because the
worktree test fixtures are not wired to a live PostgreSQL instance.
The PG repository uses the same call signatures and explicitly passes
now() for the timestamp fields, so InMemory parity covers contract
behavior the call sites rely on.

Real values for ``intended_price`` and ``fill_market_context`` will be
sourced from ``order_execution_state.algo_state_json`` in T6.1; T5 only
guarantees the columns exist and that the INSERT paths accept NULL or a
populated dict without raising.
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

from backend.services.paper_trading_v2.repository import (
    InMemoryPaperTradingV2Repository,
)
from backend.services.trading_core.models import (
    AccountSnapshot,
    Fill,
    OrderSide,
    PositionLot,
)


def _make_fill(*, fill_id: str = "fill_test_1") -> Fill:
    return Fill(
        fill_id=fill_id,
        order_id="order_test_1",
        symbol="600000.SH",
        side=OrderSide.BUY,
        quantity=100,
        price=10.5,
        trade_time=datetime(2026, 5, 10, 9, 31, tzinfo=UTC),
        bar_time=datetime(2026, 5, 10, 9, 31, tzinfo=UTC),
        reason="t5_capture_field_test",
        metadata={},
    )


def _make_position(*, portfolio_id: str = "pf_test", symbol: str = "600000.SH") -> PositionLot:
    return PositionLot(
        portfolio_id=portfolio_id,
        symbol=symbol,
        quantity=100,
        available_quantity=100,
        avg_cost=10.0,
        trade_date=date(2026, 5, 10),
    )


def _make_snapshot(*, portfolio_id: str = "pf_test") -> AccountSnapshot:
    return AccountSnapshot(
        portfolio_id=portfolio_id,
        cash=1000.0,
        market_value=1050.0,
        nav=2050.0,
        snapshot_time=datetime(2026, 5, 10, 15, 0, tzinfo=UTC),
    )


def test_fill_insert_populates_created_updated_at() -> None:
    """save_fill records created_at and updated_at within seconds of insert."""

    repo = InMemoryPaperTradingV2Repository()
    fill = _make_fill()
    before = datetime.now(UTC)
    repo.save_fill("run_test", fill)
    after = datetime.now(UTC)

    capture = repo.fill_capture[fill.fill_id]
    assert before - timedelta(seconds=1) <= capture["created_at"] <= after + timedelta(seconds=1)
    assert before - timedelta(seconds=1) <= capture["updated_at"] <= after + timedelta(seconds=1)
    # On INSERT both watermarks point at the same instant.
    assert capture["created_at"] == capture["updated_at"]


def test_position_insert_populates_timestamps() -> None:
    """save_positions records created_at and updated_at."""

    repo = InMemoryPaperTradingV2Repository()
    before = datetime.now(UTC)
    repo.save_positions(
        run_id="run_pos_test",
        trade_date=date(2026, 5, 10),
        positions=[_make_position()],
        prices={"600000.SH": 10.5},
    )
    after = datetime.now(UTC)

    capture = repo.position_capture["run_pos_test"]
    assert before - timedelta(seconds=1) <= capture["created_at"] <= after + timedelta(seconds=1)
    assert before - timedelta(seconds=1) <= capture["updated_at"] <= after + timedelta(seconds=1)


def test_daily_snapshot_insert_populates_timestamps() -> None:
    """save_daily_snapshot records created_at and updated_at on first insert."""

    repo = InMemoryPaperTradingV2Repository()
    snapshot = _make_snapshot()
    trade_date = date(2026, 5, 10)
    before = datetime.now(UTC)
    repo.save_daily_snapshot(
        run_id="run_snap_test",
        trade_date=trade_date,
        snapshot=snapshot,
        metadata={"position_count": 1},
    )
    after = datetime.now(UTC)

    capture = repo.snapshot_capture[(snapshot.portfolio_id, trade_date)]
    assert before - timedelta(seconds=1) <= capture["created_at"] <= after + timedelta(seconds=1)
    assert before - timedelta(seconds=1) <= capture["updated_at"] <= after + timedelta(seconds=1)
    assert capture["created_at"] == capture["updated_at"]


def test_fill_intended_price_nullable() -> None:
    """save_fill accepts intended_price=None (default) without error."""

    repo = InMemoryPaperTradingV2Repository()
    fill = _make_fill(fill_id="fill_null_intent")
    repo.save_fill("run_null_intent", fill)

    capture = repo.fill_capture[fill.fill_id]
    assert capture["intended_price"] is None
    assert capture["fill_market_context"] is None


def test_fill_market_context_jsonb_round_trip() -> None:
    """save_fill round-trips a dict for fill_market_context (JSONB on PG)."""

    repo = InMemoryPaperTradingV2Repository()
    fill = _make_fill(fill_id="fill_with_ctx")
    market_context: dict[str, Any] = {
        "bid": 10.49,
        "ask": 10.51,
        "best_volume": 1200,
        "spread": 0.02,
    }
    repo.save_fill(
        "run_with_ctx",
        fill,
        intended_price=10.50,
        fill_market_context=market_context,
    )

    capture = repo.fill_capture[fill.fill_id]
    assert capture["intended_price"] == 10.50
    assert capture["fill_market_context"] == market_context
    # Stored value must be an independent copy: mutating the source dict
    # afterwards does not corrupt the captured snapshot.
    market_context["bid"] = 999.0
    assert capture["fill_market_context"]["bid"] == 10.49


def test_daily_snapshot_upsert_bumps_updated_at_preserves_created_at() -> None:
    """Re-saving a snapshot for the same (portfolio_id, trade_date) bumps updated_at
    while preserving created_at — mirrors the PG ON CONFLICT branch."""

    repo = InMemoryPaperTradingV2Repository()
    snapshot = _make_snapshot()
    trade_date = date(2026, 5, 10)
    repo.save_daily_snapshot(
        run_id="run_snap_v1",
        trade_date=trade_date,
        snapshot=snapshot,
        metadata={"position_count": 1},
    )
    cap_v1 = dict(repo.snapshot_capture[(snapshot.portfolio_id, trade_date)])

    time.sleep(0.01)

    repo.save_daily_snapshot(
        run_id="run_snap_v2",
        trade_date=trade_date,
        snapshot=snapshot,
        metadata={"position_count": 2},
    )
    cap_v2 = repo.snapshot_capture[(snapshot.portfolio_id, trade_date)]

    assert cap_v2["created_at"] == cap_v1["created_at"]
    assert cap_v2["updated_at"] > cap_v1["updated_at"]


# Note on missing UPDATE coverage:
#   - paper_v2.fills has no UPDATE path in repository.py (fills are append-only
#     via INSERT ... ON CONFLICT(fill_id) DO NOTHING). updated_at on fills is
#     therefore reserved for future T6+ wiring; it is provisioned NOT NULL
#     DEFAULT NOW() so the column exists, equal to created_at on insert.
#   - paper_v2.positions is delete-and-insert per run, not row-level UPDATE,
#     so a separate update test would duplicate test_position_insert_populates_timestamps.
#   - paper_v2.daily_snapshots is the only table with a true UPDATE branch
#     (ON CONFLICT DO UPDATE), covered by
#     test_daily_snapshot_upsert_bumps_updated_at_preserves_created_at above.
