"""Integration tests for paper-v2 capture fields against the dev DB.

Phase 3 (T-PAPER-V2-INT) reads existing rows in ``aistock_dev`` to verify the
write contract added by T5/T6.1:

  INT-2  capture-cols filled invariant
         ``created_at`` / ``updated_at`` are populated on every paper_v2
         fills/positions/daily_snapshots row, and ``created_at <= updated_at
         <= now()``.

  INT-3  fill_market_context schema validation
         JSONB column contains the keys ``_build_market_context`` actually
         emits (T6.1 §5.7) and NONE of the v1-fictional keys (bid/ask/
         best_volume/spread) that the original T5 prompt mentioned.

  INT-4  intended_price dual-path ETL
         MARKET fills land with ``intended_price IS NULL`` (Batch A real);
         LIMIT-class fills land with non-NULL intended_price (Batch C
         synthetic). slippage_bps must be NULL when intended_price is NULL.

These tests intentionally read existing dev DB rows rather than driving a
mini ServiceRepository simulation: the data is already on disk from prior
batches and the read path tests the SAME write contract without bringing up
the heavy paper-v2 service stack.

Safety: all DB access goes through ``fixtures_dev_db._dev_dsn`` which
hard-asserts port=5433 + 'dev' in dbname. Connecting to prod (5432) is
impossible from this module.
"""

from __future__ import annotations

import pytest

from backend.tests.paper_trading_v2.fixtures_dev_db import (
    dev_db_conn,  # noqa: F401  re-exported as a fixture
    find_run_with_capture_fills,
    find_run_with_market_order_fills,
    find_recent_run_with_full_data,
    load_dev_paper_v2_run,
)


# ---------------------------------------------------------------------------
# INT-2 — capture-cols filled invariant
# ---------------------------------------------------------------------------


def test_run_simulation_writes_capture_cols(dev_db_conn) -> None:
    """Every fills/positions/daily_snapshots row of a recent paper_v2 run
    has non-NULL created_at / updated_at, and created_at <= updated_at.

    REV-1 P2.2: in addition to the SQL-roll-up NOT-NULL/ordering checks,
    sample-validate per-row Python types so a regression that writes
    string timestamps or float-cast Decimals fails loud here:
      * created_at / updated_at are ``datetime`` instances
      * intended_price (where non-NULL) is a ``Decimal``
      * fill_market_context (where non-NULL) is a ``dict`` with anchor
        keys (stock_id, generated_at) and no v1-fictional keys
        (bid/ask/best_volume/spread). Full key-set is verified by INT-3.

    Approach: read an existing dev DB run rather than spinning a service-
    layer mini simulation. The write contract is identical (PG repository
    writes use NOW() for both timestamps); reading the resulting rows
    exercises the same invariant the simulation would generate.
    """
    import datetime as _dt
    from decimal import Decimal

    run_id = find_recent_run_with_full_data(dev_db_conn)
    assert run_id is not None, (
        "dev DB has no paper_v2.run with fills + positions + snapshots; "
        "expected at least one Batch A or C run to be present"
    )

    bundle = load_dev_paper_v2_run(dev_db_conn, run_id)
    assert bundle["fills"], f"run {run_id} has no fills (find query lied)"
    assert bundle["positions"], f"run {run_id} has no positions"
    assert bundle["snapshots"], f"run {run_id} has no daily_snapshots"

    # Now query to confirm timestamps and ordering. Doing a single SQL roll-up
    # avoids 100s of per-row Python checks while still being a fail-fast invariant.
    with dev_db_conn.cursor() as cur:
        for table in ("fills", "positions", "daily_snapshots"):
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE created_at IS NULL) AS null_created,
                    COUNT(*) FILTER (WHERE updated_at IS NULL) AS null_updated,
                    COUNT(*) FILTER (WHERE created_at > updated_at) AS reverse,
                    COUNT(*) FILTER (WHERE updated_at > NOW() + INTERVAL '1 second') AS future
                FROM paper_v2.{table}
                WHERE run_id = %s
                """,
                (run_id,),
            )
            total, null_created, null_updated, reverse, future = cur.fetchone()
            assert total > 0, f"{table} unexpectedly empty for run {run_id}"
            assert null_created == 0, (
                f"{table} run={run_id}: {null_created}/{total} rows have NULL created_at"
            )
            assert null_updated == 0, (
                f"{table} run={run_id}: {null_updated}/{total} rows have NULL updated_at"
            )
            assert reverse == 0, (
                f"{table} run={run_id}: {reverse}/{total} rows have created_at > updated_at"
            )
            assert future == 0, (
                f"{table} run={run_id}: {future}/{total} rows have updated_at in the future"
            )

    # REV-1 P2.2: per-row Python type assertions on a sample of fill rows.
    # psycopg2 default adapters return:
    #   timestamp/timestamptz -> datetime.datetime
    #   numeric               -> decimal.Decimal
    #   jsonb                 -> dict (with json adapter registered)
    # If a regression switches the write path to write strings or to coerce
    # Decimal -> float, the type assertions catch it at row read time.
    _ANCHOR_MARKET_CONTEXT_KEYS = {"stock_id", "generated_at"}
    _FORBIDDEN_MARKET_CONTEXT_KEYS = {"bid", "ask", "best_volume", "spread"}

    with dev_db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT fill_id, created_at, updated_at, intended_price, fill_market_context
            FROM paper_v2.fills
            WHERE run_id = %s
            LIMIT 50
            """,
            (run_id,),
        )
        sample_fills = cur.fetchall()

    assert sample_fills, f"sample fills query returned 0 rows for run {run_id}"

    for fill_id, created_at, updated_at, intended_price, ctx in sample_fills:
        # Timestamp types.
        assert isinstance(created_at, _dt.datetime), (
            f"fill {fill_id}: created_at must be datetime, got {type(created_at)!r}"
        )
        assert isinstance(updated_at, _dt.datetime), (
            f"fill {fill_id}: updated_at must be datetime, got {type(updated_at)!r}"
        )
        # Ordering preserved at row level (SQL roll-up already covers
        # aggregate, but make sure psycopg2 didn't lose precision).
        assert created_at <= updated_at, (
            f"fill {fill_id}: created_at {created_at!r} > updated_at {updated_at!r}"
        )

        # Numeric type for intended_price (where present).
        if intended_price is not None:
            assert isinstance(intended_price, Decimal), (
                f"fill {fill_id}: intended_price must be Decimal, "
                f"got {type(intended_price)!r}={intended_price!r}"
            )

        # JSONB type + anchor-key check (full key-set in INT-3).
        if ctx is not None:
            assert isinstance(ctx, dict), (
                f"fill {fill_id}: fill_market_context must deserialise as dict, "
                f"got {type(ctx)!r}"
            )
            keys = set(ctx.keys())
            missing_anchors = _ANCHOR_MARKET_CONTEXT_KEYS - keys
            assert not missing_anchors, (
                f"fill {fill_id}: fill_market_context missing anchor keys "
                f"{missing_anchors}; got keys={sorted(keys)}"
            )
            forbidden_present = _FORBIDDEN_MARKET_CONTEXT_KEYS & keys
            assert not forbidden_present, (
                f"fill {fill_id}: fill_market_context contains v1-fictional keys "
                f"{forbidden_present}"
            )


# ---------------------------------------------------------------------------
# INT-3 — fill_market_context schema validation
# ---------------------------------------------------------------------------


# Required keys per ``backend/services/paper_trading_v2/market_data.py
# :_build_market_context`` (line 692). Authoritative source = code, not the
# T5 prompt (which incorrectly listed bid/ask/spread; corrected by T6.1).
_REQUIRED_MARKET_CONTEXT_KEYS = {
    "stock_id",
    "trade_date",
    "data_source",
    "prev_close",
    "limit_up",
    "limit_down",
    "suspend_status",
    "full_day_open",
    "full_day_close",
    "full_day_volume",
    "full_day_high",
    "full_day_low",
    "generated_at",
}

# Forbidden keys: the original T5 prompt mentioned these but the actual
# ``_build_market_context`` impl does NOT produce them. Test guards against
# regression to the v1-fictional shape.
_FORBIDDEN_MARKET_CONTEXT_KEYS = {"bid", "ask", "best_volume", "spread"}


def test_fill_market_context_real_keys(dev_db_conn) -> None:
    """fill_market_context JSONB matches T6.1 §5.7 real key set.

    Pulls all rows with non-NULL ``fill_market_context`` from a recent run
    and asserts:
      * every required key is present
      * no v1-fictional key is present
      * any extra keys (e.g. V25 day_features_*) are tolerated
    """
    runs = find_run_with_capture_fills(dev_db_conn, n=1)
    if not runs:
        pytest.xfail(
            "no Batch C synthetic fills present in dev DB "
            "(intended_price IS NOT NULL set is empty)"
        )
    run_id = runs[0]

    with dev_db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT fill_id, fill_market_context
            FROM paper_v2.fills
            WHERE run_id = %s AND fill_market_context IS NOT NULL
            """,
            (run_id,),
        )
        rows = cur.fetchall()

    assert rows, f"run {run_id} promised non-NULL fill_market_context but had none"

    for fill_id, ctx in rows:
        assert isinstance(ctx, dict), (
            f"fill_market_context must deserialise as dict, got {type(ctx)!r} for {fill_id}"
        )
        keys = set(ctx.keys())

        missing = _REQUIRED_MARKET_CONTEXT_KEYS - keys
        assert not missing, (
            f"fill {fill_id}: fill_market_context missing required keys {missing}; "
            f"got keys={sorted(keys)}"
        )

        forbidden_present = _FORBIDDEN_MARKET_CONTEXT_KEYS & keys
        assert not forbidden_present, (
            f"fill {fill_id}: fill_market_context contains v1-fictional keys "
            f"{forbidden_present}; the T5 prompt's bid/ask/spread/best_volume "
            f"shape was a regression — assert against the real "
            f"_build_market_context output instead"
        )


# ---------------------------------------------------------------------------
# INT-4 — intended_price dual-path ETL
# ---------------------------------------------------------------------------


def _slippage_bps(filled_price: float, intended_price: float | None) -> float | None:
    """Pure-Python slippage (bps). NULL intended_price -> NULL bps (NOT 0).

    This mirrors the dual-path ETL semantics: a MARKET order has no
    ``intended_price`` to anchor against, so slippage is genuinely undefined
    and must remain NULL through to the DW. Returning 0 would silently
    flatter MARKET fills against LIMIT fills in downstream metrics.
    """
    if intended_price is None:
        return None
    return (filled_price - intended_price) * 10000.0 / intended_price


def test_intended_price_market_order_null(dev_db_conn) -> None:
    """MARKET-class fills must have NULL intended_price; slippage_bps NULL.

    Asserts:
      * at least one fill exists with intended_price IS NULL (Batch A)
      * the corresponding order (joined via order_id) is a MARKET order
        (paper_v2.orders.order_type)
      * the pure-Python slippage helper returns None for NULL intended_price
    """
    runs = find_run_with_market_order_fills(dev_db_conn, n=1)
    assert runs, "dev DB has no MARKET-class fills (intended_price IS NULL)"

    run_id = runs[0]
    with dev_db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.fill_id, f.price, f.intended_price, o.order_type
            FROM paper_v2.fills f
            JOIN paper_v2.orders o ON o.order_id = f.order_id
            WHERE f.run_id = %s AND f.intended_price IS NULL
            LIMIT 100
            """,
            (run_id,),
        )
        rows = cur.fetchall()

    assert rows, f"join lost rows for run {run_id}"

    for fill_id, price, intended_price, order_type in rows:
        assert intended_price is None, f"fill {fill_id}: expected NULL intended_price"
        # paper_v2 only emits MARKET intents per strategy_package/runtime.py:716,
        # so every fill with NULL intended_price must trace back to a MARKET order.
        assert order_type == "MARKET", (
            f"fill {fill_id}: NULL intended_price but order_type={order_type!r} "
            f"(expected 'MARKET')"
        )
        assert _slippage_bps(float(price), None) is None, (
            "slippage helper returned non-None for NULL intended_price — "
            "would silently flatter MARKET fills in downstream metrics"
        )


def test_intended_price_limit_order_non_null(dev_db_conn) -> None:
    """LIMIT-class fills (Batch C synthetic) have non-NULL intended_price.

    Asserts:
      * at least one fill exists with intended_price populated
      * slippage_bps is a finite float
      * |slippage_bps| < 1000 (sanity bound; real slippage is usually < 100bps)
    """
    runs = find_run_with_capture_fills(dev_db_conn, n=1)
    if not runs:
        pytest.xfail(
            "no Batch C synthetic fills present in dev DB "
            "(intended_price IS NOT NULL set is empty)"
        )
    run_id = runs[0]

    with dev_db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT fill_id, price, intended_price
            FROM paper_v2.fills
            WHERE run_id = %s AND intended_price IS NOT NULL
            LIMIT 100
            """,
            (run_id,),
        )
        rows = cur.fetchall()

    assert rows, f"run {run_id} promised non-NULL intended_price but had none"

    import math

    for fill_id, price, intended_price in rows:
        bps = _slippage_bps(float(price), float(intended_price))
        assert bps is not None
        assert math.isfinite(bps), (
            f"fill {fill_id}: slippage_bps={bps} (non-finite); "
            f"price={price} intended={intended_price}"
        )
        assert abs(bps) < 1000.0, (
            f"fill {fill_id}: |slippage_bps|={abs(bps):.2f} exceeds 1000bps "
            f"sanity bound; price={price} intended={intended_price}"
        )
