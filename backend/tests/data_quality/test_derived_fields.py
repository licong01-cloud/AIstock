"""Derived-field correctness checks against the reference implementation.

Per Stage 7.3 §7.3.3:
- ``qe_archive.paper_v2_cash_ledger.entry_type`` must match the reference
  ``synthesize_cash_ledger_entry_type`` for every archived row (BUG-006).
- ``qe_archive.paper_v2_reset_audit.reset_type`` must match the reference
  ``synthesize_reset_audit_reset_type`` (BUG-007).
- ``qe_archive.paper_v2_session_day.data_quality`` must be one of the
  allowed enum values (BUG-008 partial fix).
- ``market.regime_label.regime`` for ``simple_quadrant`` must match the
  reference ``classify_simple_quadrant``.
- ``paper_v2.fills`` slippage (when intended_price is non-NULL) must match
  ``compute_slippage_bps``.

The reference impls live in backend/tests/data_quality/_reference.py — they
mirror the dw-foundation handler synthesize logic (commit bd098f8).
"""

from __future__ import annotations

import json

import pytest

from psycopg2.extras import RealDictCursor

from ._reference import (
    CASH_LEDGER_ENTRY_TYPES,
    RESET_AUDIT_RESET_TYPES,
    REGIME_VALUES,
    SESSION_DAY_QUALITY,
    classify_simple_quadrant,
    compute_slippage_bps,
    synthesize_cash_ledger_entry_type,
    synthesize_reset_audit_reset_type,
)


def test_module_collected_smoke():
    assert CASH_LEDGER_ENTRY_TYPES
    assert RESET_AUDIT_RESET_TYPES
    assert SESSION_DAY_QUALITY


def _coerce(value):
    if value is None or isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def test_cash_ledger_entry_type_matches_synth_reference(
    dev_conn, archive_tables_ready,
):
    """For each row in qe_archive.paper_v2_cash_ledger, derive the expected
    entry_type from the *source* paper_v2.cash_ledger row and assert
    equality."""
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT a.ledger_entry_id, a.entry_type,
                   s.side, s.notional, s.fee, s.cash_delta
            FROM qe_archive.paper_v2_cash_ledger a
            JOIN paper_v2.cash_ledger s ON s.cash_id::text = a.ledger_entry_id
            LIMIT 500
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip(
            "no joined cash_ledger rows between source and archive; "
            "PaperV2ArchiveHandler.handle has not produced archive rows yet."
        )
    drift = []
    for r in rows:
        expected = synthesize_cash_ledger_entry_type(
            side=r["side"], notional=r["notional"],
            fee=r["fee"], cash_delta=r["cash_delta"],
        )
        if r["entry_type"] != expected:
            drift.append((r["ledger_entry_id"], r["entry_type"], expected))
    assert not drift, (
        f"{len(drift)} cash_ledger rows have entry_type drift; first 5: {drift[:5]}"
    )


def test_cash_ledger_entry_type_in_allowed_enum(
    dev_conn, archive_tables_ready,
):
    """Every archived entry_type must be in CASH_LEDGER_ENTRY_TYPES."""
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT ledger_entry_id, entry_type FROM qe_archive.paper_v2_cash_ledger LIMIT 1000"
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("qe_archive.paper_v2_cash_ledger empty.")
    bad = [r for r in rows if r["entry_type"] not in CASH_LEDGER_ENTRY_TYPES]
    assert not bad, (
        f"{len(bad)} cash_ledger rows have illegal entry_type; first 5: "
        f"{[(r['ledger_entry_id'], r['entry_type']) for r in bad[:5]]}"
    )


def test_reset_audit_reset_type_matches_synth_reference(
    dev_conn, archive_tables_ready,
):
    """Per BUG-007: handler.synthesize_reset_audit_reset_type must produce
    the canonical mapping. Where the source table is empty this is a no-op
    pass (the synthesize function is unit-tested separately on dw-foundation)."""
    # qe_archive.paper_v2_reset_audit.audit_id is TEXT (per T12 P1.3 UUID->TEXT
    # round-2 fix BUG-003); paper_v2.reset_audit.audit_id is bigint. Cast at
    # the join boundary to bridge the two type domains.
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT a.reset_type, s.rerun_policy, s.deleted_counts
            FROM qe_archive.paper_v2_reset_audit a
            JOIN paper_v2.reset_audit s ON s.audit_id::text = a.audit_id
            LIMIT 500
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip(
            "paper_v2.reset_audit join is empty (Batch A had no reset events); "
            "this is the BUG-007 'integration test deferred' case."
        )
    drift = []
    for r in rows:
        expected = synthesize_reset_audit_reset_type(
            rerun_policy=r["rerun_policy"],
            deleted_counts=_coerce(r["deleted_counts"]),
        )
        if r["reset_type"] != expected:
            drift.append((r["reset_type"], expected, r["rerun_policy"]))
    assert not drift, f"{len(drift)} reset_type drift; first 5: {drift[:5]}"


def test_session_day_data_quality_in_allowed_enum(
    dev_conn, archive_tables_ready,
):
    """BUG-008 partial fix: data_quality is derived but may still
    under-report. The contract is the enum domain remains tight."""
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT trade_session_id, data_quality FROM qe_archive.paper_v2_session_day "
            "WHERE data_quality IS NOT NULL LIMIT 1000"
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("qe_archive.paper_v2_session_day has no data_quality rows.")
    bad = [r for r in rows if r["data_quality"] not in SESSION_DAY_QUALITY]
    assert not bad, (
        f"{len(bad)} session_day rows have illegal data_quality; first 5: "
        f"{[(r['trade_session_id'], r['data_quality']) for r in bad[:5]]}"
    )


def test_regime_label_simple_quadrant_classification(
    dev_conn,
):
    """``market.regime_label.regime`` (where source_method='simple_quadrant')
    must equal classify_simple_quadrant(signal['ret_pct_5y'],
    signal['vol_pct_5y'])."""
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname='market' AND tablename='regime_label'"
        )
        if cur.fetchone() is None:
            pytest.skip("market.regime_label absent.")
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT trade_date, regime, source_signal_json
            FROM market.regime_label
            WHERE source_method = 'simple_quadrant'
            LIMIT 500
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("no simple_quadrant rows in market.regime_label yet.")
    drift = []
    for r in rows:
        signal = _coerce(r["source_signal_json"]) or {}
        expected, _conf = classify_simple_quadrant(
            ret_pct_5y=signal.get("ret_pct_5y"),
            vol_pct_5y=signal.get("vol_pct_5y"),
        )
        if r["regime"] not in REGIME_VALUES:
            drift.append((r["trade_date"], r["regime"], "illegal enum"))
        elif r["regime"] != expected:
            drift.append((r["trade_date"], r["regime"], expected))
    assert not drift, f"{len(drift)} regime classifications drift; first 5: {drift[:5]}"


def test_slippage_bps_consistent_with_intended_price(
    dev_conn, source_tables_ready,
):
    """When intended_price is non-NULL, slippage_bps must equal
    compute_slippage_bps(intended_price, fill_price, side) within 0.5 bps
    rounding tolerance. MARKET orders (intended_price NULL) are skipped."""
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='paper_v2' AND table_name='fills' "
            "AND column_name IN ('intended_price', 'slippage_bps', 'fill_price', 'side')"
        )
        cols = {r[0] for r in cur.fetchall()}
    needed = {"intended_price", "slippage_bps", "fill_price", "side"}
    if not needed.issubset(cols):
        pytest.skip(
            f"paper_v2.fills missing {sorted(needed - cols)}; T6.1 capture cols "
            f"may not all be present on this dev DB."
        )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT fill_id, intended_price, fill_price, side, slippage_bps
            FROM paper_v2.fills
            WHERE intended_price IS NOT NULL AND slippage_bps IS NOT NULL
            LIMIT 200
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip(
            "no fills with both intended_price and slippage_bps populated "
            "(MARKET-only baseline per T6.1 §5.7)."
        )
    drift = []
    for r in rows:
        expected = compute_slippage_bps(
            intended_price=r["intended_price"],
            fill_price=r["fill_price"],
            side=r["side"],
        )
        if expected is None:
            continue
        actual = float(r["slippage_bps"])
        if abs(actual - expected) > 0.5:  # 0.5 bps tolerance for Decimal rounding
            drift.append((r["fill_id"], actual, expected))
    assert not drift, f"{len(drift)} slippage_bps drift; first 5: {drift[:5]}"
